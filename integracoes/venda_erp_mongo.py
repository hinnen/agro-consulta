import re
import unicodedata
from datetime import datetime, timedelta
from difflib import SequenceMatcher

from pymongo import MongoClient
from django.conf import settings
from django.core.cache import cache

from integracoes.texto import tokens


_mongo_client = None


def get_mongo_client():
    global _mongo_client

    if _mongo_client is None:
        _mongo_client = MongoClient(
            settings.VENDA_ERP_MONGO_URL,
            maxPoolSize=20,
            minPoolSize=1,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            socketTimeoutMS=10000,
            retryWrites=False,
        )

    return _mongo_client


class VendaERPMongoClient:
    DIAS_RANKING_VENDAS = 180

    def __init__(self):
        self.client = get_mongo_client()
        self.db = self.client[settings.VENDA_ERP_MONGO_DB]
        self.cache_ttl = getattr(settings, "CONSULTA_CACHE_TTL", 20)

    def _projection_produto(self):
        return {
            "Nome": 1,
            "Codigo": 1,
            "CodigoNFe": 1,
            "EAN_NFe": 1,
            "Marca": 1,
            "Categoria": 1,
            "SubCategoria": 1,
            "Subcategoria": 1,
            "Grupo": 1,
            "Genero": 1,
            "CategoriaPadrao": 1,
            "Categorias": 1,
            "CategoriasProduto": 1,
            "ListaCategorias": 1,
            "GruposProdutos": 1,
            "PrecoVenda": 1,
            "CadastroInativo": 1,
            "NomeNormalizado": 1,
        }

    def _normalizar_termo(self, termo):
        termo = (termo or "").strip()
        termo_numerico = re.sub(r"\D", "", termo)
        return termo, termo_numerico

    def _normalizar_texto_livre(self, texto):
        texto = str(texto or "").strip().lower()
        texto = unicodedata.normalize("NFKD", texto)
        texto = "".join(ch for ch in texto if not unicodedata.combining(ch))
        texto = texto.replace("ç", "c")
        texto = re.sub(r"[^a-z0-9\s\-]", " ", texto)
        texto = re.sub(r"\s+", " ", texto).strip()
        return texto

    def _tokens_busca(self, termo):
        base = tokens(termo)
        if base:
            return base

        termo_norm = self._normalizar_texto_livre(termo)
        return [t for t in termo_norm.split() if t]

    def _termo_parece_barcode(self, termo, termo_numerico):
        return (
            (termo.isdigit() and len(termo) >= 8)
            or (termo_numerico.isdigit() and len(termo_numerico) >= 8)
        )

    def _termo_parece_codigo(self, termo):
        termo = (termo or "").strip()

        if not termo:
            return False

        if " " in termo:
            return False

        tem_numero = any(c.isdigit() for c in termo)
        tem_letra = any(c.isalpha() for c in termo)

        if tem_numero and tem_letra:
            return True

        if tem_numero and "-" in termo:
            return True

        if tem_numero and len(termo) <= 20:
            return True

        return False

    def _buscar_barras_exato(self, termo, termo_numerico, projection):
        candidatos = []

        if termo and termo.isdigit() and len(termo) >= 8:
            candidatos.append(termo)

        if termo_numerico and len(termo_numerico) >= 8 and termo_numerico not in candidatos:
            candidatos.append(termo_numerico)

        for valor in candidatos:
            produto = self.db["DtoProduto"].find_one(
                {
                    "EAN_NFe": valor,
                    "CadastroInativo": False,
                },
                projection,
            )
            if produto:
                return [produto]

        return []

    def _buscar_por_codigo_prefixo(self, termo, projection):
        regex_prefixo = f"^{re.escape(termo)}"

        filtro = {
            "$and": [
                {"CadastroInativo": False},
                {
                    "$or": [
                        {"CodigoNFe": {"$regex": regex_prefixo, "$options": "i"}},
                        {"Codigo": {"$regex": regex_prefixo, "$options": "i"}},
                    ]
                },
            ]
        }

        return list(
            self.db["DtoProduto"].find(filtro, projection).limit(50)
        )

    def _buscar_por_barras_parcial(self, termo, termo_numerico, projection):
        if not termo_numerico:
            return []

        regex_numerico = re.escape(termo_numerico)

        filtro = {
            "$and": [
                {"CadastroInativo": False},
                {
                    "$or": [
                        {"EAN_NFe": {"$regex": regex_numerico, "$options": "i"}},
                        {"Codigo": {"$regex": regex_numerico, "$options": "i"}},
                        {"CodigoNFe": {"$regex": regex_numerico, "$options": "i"}},
                    ]
                },
            ]
        }

        return list(
            self.db["DtoProduto"].find(filtro, projection).limit(50)
        )

    def _buscar_por_nome(self, termo, projection):
        tks = self._tokens_busca(termo)

        if not tks:
            return []

        condicoes = []
        for t in tks:
            condicoes.append({
                "NomeNormalizado": {
                    "$regex": f".*{re.escape(t)}.*"
                }
            })

        filtro = {
            "$and": [
                {"CadastroInativo": False},
                *condicoes
            ]
        }

        return list(
            self.db["DtoProduto"].find(filtro, projection).limit(50)
        )

    def _buscar_por_nome_amplo(self, termo, projection):
        tks = self._tokens_busca(termo)

        if not tks:
            return []

        termos_aproveitaveis = [t for t in tks if len(t) >= 3][:3]
        if not termos_aproveitaveis:
            termos_aproveitaveis = tks[:2]

        if not termos_aproveitaveis:
            return []

        condicoes = []
        for t in termos_aproveitaveis:
            # prefixo curto para tolerar erro leve de escrita
            prefixo = t[: max(2, min(4, len(t)))]
            condicoes.append({
                "NomeNormalizado": {
                    "$regex": re.escape(prefixo)
                }
            })

        filtro = {
            "$and": [
                {"CadastroInativo": False},
                {
                    "$or": condicoes
                }
            ]
        }

        return list(
            self.db["DtoProduto"].find(filtro, projection).limit(120)
        )

    def _ranking_vendas_por_produto(self, produto_ids):
        if not produto_ids:
            return {}

        cache_key = "ranking_vendas::" + "|".join(sorted(produto_ids))
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        data_inicio = datetime.utcnow() - timedelta(days=self.DIAS_RANKING_VENDAS)

        pipeline = [
            {
                "$match": {
                    "Movimentacao": "Venda",
                    "Produto_ID": {"$in": produto_ids},
                    "Data": {"$gte": data_inicio},
                }
            },
            {
                "$group": {
                    "_id": "$Produto_ID",
                    "quantidade_vendida": {"$sum": "$Quantidade"},
                    "valor_total_vendido": {"$sum": "$ValorTotal"},
                    "ultima_venda": {"$max": "$Data"},
                }
            }
        ]

        agregados = list(
            self.db["ReportViewDtoEstoqueSaida"].aggregate(pipeline)
        )

        ranking = {}
        for item in agregados:
            ranking[str(item["_id"])] = {
                "quantidade_vendida": float(item.get("quantidade_vendida", 0) or 0),
                "valor_total_vendido": float(item.get("valor_total_vendido", 0) or 0),
                "ultima_venda": item.get("ultima_venda"),
            }

        cache.set(cache_key, ranking, self.cache_ttl)
        return ranking

    def _coletar_textos(self, valor):
        textos = []

        if valor is None:
            return textos

        if isinstance(valor, str):
            textos.append(valor)

        elif isinstance(valor, dict):
            for v in valor.values():
                textos.extend(self._coletar_textos(v))

        elif isinstance(valor, list):
            for item in valor:
                textos.extend(self._coletar_textos(item))

        return textos

    def _texto_produto(self, produto):
        campos = [
            produto.get("Nome"),
            produto.get("Categoria"),
            produto.get("SubCategoria"),
            produto.get("Subcategoria"),
            produto.get("Grupo"),
            produto.get("Genero"),
            produto.get("CategoriaPadrao"),
            produto.get("Categorias"),
            produto.get("CategoriasProduto"),
            produto.get("ListaCategorias"),
            produto.get("GruposProdutos"),
        ]

        textos = []
        for campo in campos:
            textos.extend(self._coletar_textos(campo))

        return " ".join(str(t or "") for t in textos).strip().lower()

    def _categoria_eh_granel(self, produto):
        texto = self._texto_produto(produto)
        return "granel" in texto

    def _pontuacao_relevancia_nome(self, produto, termo):
        nome = str(produto.get("Nome") or "")
        nome_norm = self._normalizar_texto_livre(nome)
        termo_norm = self._normalizar_texto_livre(termo)
        tks = self._tokens_busca(termo)

        score = 0

        if termo_norm and termo_norm in nome_norm:
            score += 120

        if termo_norm and nome_norm.startswith(termo_norm):
            score += 80

        tokens_encontrados = 0
        for tk in tks:
            if tk in nome_norm:
                score += 25
                tokens_encontrados += 1

            # tolerância a erro leve
            for palavra in nome_norm.split():
                ratio = SequenceMatcher(None, tk, palavra).ratio()
                if ratio >= 0.82:
                    score += 18
                    break

            # bônus para número importante, como 1, 15, 24, 47 etc.
            if tk.isdigit():
                if re.search(rf"(^|[^0-9]){re.escape(tk)}([^0-9]|$)", nome_norm):
                    score += 60

        if tks and tokens_encontrados == len(tks):
            score += 100

        # similaridade geral da frase toda
        if termo_norm and nome_norm:
            score += int(SequenceMatcher(None, termo_norm, nome_norm).ratio() * 100)

        return score

    def _ordenar_por_vendas(self, produtos, termo=""):
        if not produtos:
            return produtos

        produto_ids = [str(p.get("_id")) for p in produtos if p.get("_id")]
        ranking = self._ranking_vendas_por_produto(produto_ids)

        def chave_ordenacao(produto):
            produto_id = str(produto.get("_id"))
            dados = ranking.get(produto_id, {})

            quantidade_vendida = dados.get("quantidade_vendida", 0)
            valor_total_vendido = dados.get("valor_total_vendido", 0)
            nome = str(produto.get("Nome") or "")
            eh_granel = 1 if self._categoria_eh_granel(produto) else 0
            relevancia = self._pontuacao_relevancia_nome(produto, termo)

            return (
                eh_granel,
                -relevancia,
                -quantidade_vendida,
                -valor_total_vendido,
                nome.lower(),
            )

        produtos_ordenados = sorted(produtos, key=chave_ordenacao)

        for produto in produtos_ordenados:
            produto_id = str(produto.get("_id"))
            dados = ranking.get(produto_id, {})
            produto["QuantidadeVendidaRanking"] = dados.get("quantidade_vendida", 0)
            produto["ValorTotalVendidoRanking"] = dados.get("valor_total_vendido", 0)
            produto["EhGranelRanking"] = self._categoria_eh_granel(produto)

        return produtos_ordenados

    def _deduplicar_produtos(self, produtos):
        vistos = set()
        unicos = []

        for produto in produtos:
            pid = str(produto.get("_id"))
            if pid and pid not in vistos:
                vistos.add(pid)
                unicos.append(produto)

        return unicos

    def buscar_produtos(self, termo):
        termo, termo_numerico = self._normalizar_termo(termo)

        if not termo:
            return []

        cache_key = f"busca_produtos::{termo.lower()}::{termo_numerico}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        projection = self._projection_produto()

        resultados = []

        # 1. código de barras exato
        if self._termo_parece_barcode(termo, termo_numerico):
            resultados = self._buscar_barras_exato(termo, termo_numerico, projection)
            if resultados:
                resultados = self._ordenar_por_vendas(resultados, termo)
                cache.set(cache_key, resultados, self.cache_ttl)
                return resultados

        # 2. código puro
        if self._termo_parece_codigo(termo):
            resultados = self._buscar_por_codigo_prefixo(termo, projection)
            if resultados:
                resultados = self._ordenar_por_vendas(resultados, termo)
                cache.set(cache_key, resultados, self.cache_ttl)
                return resultados

        # 3. nome direto
        resultados_nome = self._buscar_por_nome(termo, projection)

        # 4. nome amplo/flexível para erro de escrita
        resultados_amplos = self._buscar_por_nome_amplo(termo, projection)

        resultados = self._deduplicar_produtos(resultados_nome + resultados_amplos)
        if resultados:
            resultados = self._ordenar_por_vendas(resultados, termo)
            cache.set(cache_key, resultados, self.cache_ttl)
            return resultados

        # 5. fallback parcial numérico
        resultados = self._buscar_por_barras_parcial(termo, termo_numerico, projection)
        if resultados:
            resultados = self._ordenar_por_vendas(resultados, termo)
            cache.set(cache_key, resultados, self.cache_ttl)
            return resultados

        # 6. última tentativa por código
        resultados = self._buscar_por_codigo_prefixo(termo, projection)
        resultados = self._ordenar_por_vendas(resultados, termo)

        cache.set(cache_key, resultados, self.cache_ttl)
        return resultados

    def buscar_estoques_por_produto_ids(self, produto_ids):
        if not produto_ids:
            return []

        produto_ids_str = [str(pid) for pid in produto_ids]

        cache_key = "estoques::" + "|".join(sorted(produto_ids_str))
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        resultados = list(
            self.db["DtoEstoqueDepositoProduto"].find(
                {"ProdutoID": {"$in": produto_ids_str}},
                {
                    "ProdutoID": 1,
                    "Produto": 1,
                    "Deposito": 1,
                    "DepositoID": 1,
                    "Saldo": 1,
                    "EstoqueMinimo": 1,
                }
            )
        )

        cache.set(cache_key, resultados, self.cache_ttl)
        return resultados