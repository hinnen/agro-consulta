import re
import unicodedata
from datetime import datetime, timedelta
from difflib import SequenceMatcher

from pymongo import MongoClient
from pymongo.errors import PyMongoError
from django.conf import settings

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

    # =========================
    # NORMALIZAÇÃO
    # =========================

    def _normalizar(self, texto):
        texto = str(texto or "").lower()
        texto = unicodedata.normalize("NFKD", texto)
        texto = "".join(c for c in texto if not unicodedata.combining(c))
        texto = texto.replace("ç", "c")
        texto = re.sub(r"[^a-z0-9\s]", " ", texto)
        return re.sub(r"\s+", " ", texto).strip()

    def _tokens(self, termo):
        base = tokens(termo)
        if base:
            return base
        return [t for t in self._normalizar(termo).split() if t]

    def _texto_busca_produto(self, produto):
        partes = [
            produto.get("Nome"),
            produto.get("BuscaTexto"),
            produto.get("Categoria"),
            produto.get("SubCategoria"),
            produto.get("Subcategoria"),
            produto.get("Marca"),
        ]
        return self._normalizar(" ".join(str(p or "") for p in partes))

    # =========================
    # REGRAS DE PRODUTO
    # =========================

    def _eh_granel(self, produto):
        texto = self._normalizar(
            f"{produto.get('Nome')} {produto.get('Categoria')} "
            f"{produto.get('SubCategoria')} {produto.get('Subcategoria')}"
        )

        if "granel" in texto:
            return True

        if any(x in texto for x in ["1kg", "1 kg"]):
            if any(
                t in texto
                for t in ["milho", "racao", "ração", "farelo", "quirera", "soja"]
            ):
                return True

        return False

    def _peso_produto(self, produto):
        nome = self._normalizar(produto.get("Nome"))
        match = re.search(r"(\d+(?:[.,]\d+)?)\s*(kg|g)\b", nome)

        if not match:
            return 0

        valor_str = match.group(1).replace(",", ".")
        unidade = match.group(2)

        try:
            valor = float(valor_str)
        except ValueError:
            return 0

        if unidade == "kg":
            return int(valor * 1000)

        return int(valor)

    def _peso_alvo(self, termo):
        termo_norm = self._normalizar(termo)

        if "turtle" in termo_norm:
            return 300

        if "racao" in termo_norm or "ração" in termo_norm:
            return 15000

        return None

    # =========================
    # BUSCA
    # =========================

    def _projection_busca(self):
     return {
        "Nome": 1,
        "Codigo": 1,
        "CodigoNFe": 1,
        "EAN_NFe": 1,
        "Categoria": 1,
        "SubCategoria": 1,
        "Subcategoria": 1,
        "PrecoVenda": 1,
        "BuscaTexto": 1,
        "NomeNormalizado": 1,
        "Marca": 1,
        "CadastroInativo": 1,
    }

    def _buscar_text_index(self, termo):
        return list(
            self.db["DtoProduto"].find(
                {
                    "$text": {"$search": termo},
                    "CadastroInativo": False,
                },
                {
                    **self._projection_busca(),
                    "score": {"$meta": "textScore"},
                },
            ).sort([("score", {"$meta": "textScore"})]).limit(200)
        )

    def _buscar_regex_fallback(self, termo):
        tks = self._tokens(termo)
        if not tks:
            return []

        filtro = {
            "$and": [
                {"CadastroInativo": False},
                {
                    "$and": [
                        {
                            "$or": [
                                {"BuscaTexto": {"$regex": re.escape(t), "$options": "i"}},
                                {"Nome": {"$regex": re.escape(t), "$options": "i"}},
                                {"NomeNormalizado": {"$regex": re.escape(t), "$options": "i"}},
                            ]
                        }
                        for t in tks
                    ]
                },
            ]
        }

        return list(self.db["DtoProduto"].find(filtro, self._projection_busca()).limit(200))

    def _buscar(self, termo):
        try:
            resultados = self._buscar_text_index(termo)
            if resultados:
                return resultados
        except PyMongoError:
            pass
        except Exception:
            pass

        try:
            return self._buscar_regex_fallback(termo)
        except Exception:
            return []

    # =========================
    # SCORE
    # =========================

    def _score(self, produto, termo):
        nome = self._normalizar(produto.get("Nome"))
        texto_produto = self._texto_busca_produto(produto)
        termo_norm = self._normalizar(termo)
        tks = self._tokens(termo)

        score = 0

        mongo_text_score = produto.get("score")
        try:
            if mongo_text_score is not None:
                score += int(float(mongo_text_score) * 100)
        except (TypeError, ValueError):
            pass

        if termo_norm and termo_norm in nome:
            score += 1000
        elif termo_norm and termo_norm in texto_produto:
            score += 700

        if termo_norm and nome.startswith(termo_norm):
            score += 500

        tokens_encontrados = 0

        for tk in tks:
            encontrou = False

            if tk in nome:
                score += 220
                encontrou = True
            elif tk in texto_produto:
                score += 150
                encontrou = True
            else:
                for palavra in nome.split():
                    if SequenceMatcher(None, tk, palavra).ratio() >= 0.82:
                        score += 100
                        encontrou = True
                        break

            if encontrou:
                tokens_encontrados += 1

        if tks and tokens_encontrados == len(tks):
            score += 900
        elif len(tks) >= 2 and tokens_encontrados >= len(tks) - 1:
            score += 300

        return score

    # =========================
    # VENDAS
    # =========================

    def _ranking_vendas(self, ids):
        if not ids:
            return {}

        data_inicio = datetime.utcnow() - timedelta(days=self.DIAS_RANKING_VENDAS)

        pipeline = [
            {
                "$match": {
                    "Movimentacao": "Venda",
                    "Produto_ID": {"$in": ids},
                    "Data": {"$gte": data_inicio},
                }
            },
            {
                "$group": {
                    "_id": "$Produto_ID",
                    "qtd": {"$sum": "$Quantidade"},
                }
            },
        ]

        try:
            dados = list(self.db["ReportViewDtoEstoqueSaida"].aggregate(pipeline))
            return {str(d["_id"]): float(d.get("qtd", 0) or 0) for d in dados}
        except Exception:
            return {}

    # =========================
    # ORDENAÇÃO
    # =========================

    def _ordenar(self, produtos, termo):
        ids = [str(p["_id"]) for p in produtos if p.get("_id")]
        ranking = self._ranking_vendas(ids)
        peso_alvo = self._peso_alvo(termo)

        def chave(p):
            pid = str(p.get("_id"))
            vendidos = ranking.get(pid, 0)
            peso = self._peso_produto(p)
            score = self._score(p, termo)

            distancia_peso = abs(peso - peso_alvo) if peso_alvo and peso > 0 else 999999

            return (
                self._eh_granel(p),
                -score,
                distancia_peso,
                -vendidos,
                self._normalizar(p.get("Nome")),
            )

        return sorted(produtos, key=chave)

    # =========================
    # PÚBLICO
    # =========================

    def buscar_produtos(self, termo):
        if not termo:
            return []

        produtos = self._buscar(termo)
        produtos = self._ordenar(produtos, termo)
        return produtos[:50]

    def buscar_estoques_por_produto_ids(self, produto_ids):
        if not produto_ids:
            return []

        try:
            return list(
                self.db["DtoEstoqueDepositoProduto"].find(
                    {"ProdutoID": {"$in": [str(pid) for pid in produto_ids]}},
                    {
                        "ProdutoID": 1,
                        "Produto": 1,
                        "Deposito": 1,
                        "DepositoID": 1,
                        "Saldo": 1,
                        "EstoqueMinimo": 1,
                    },
                )
            )
        except Exception:
            return []