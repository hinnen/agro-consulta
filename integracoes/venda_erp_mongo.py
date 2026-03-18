import re

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
            "PrecoVenda": 1,
            "CadastroInativo": 1,
            "NomeNormalizado": 1,
        }

    def _normalizar_termo(self, termo):
        termo = (termo or "").strip()
        termo_numerico = re.sub(r"\D", "", termo)
        return termo, termo_numerico

    def _buscar_barras_exato(self, termo, termo_numerico, projection):
        # Busca exata por código de barras só quando parece realmente código de barras
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
        # Ex.: GM0090 deve trazer GM0090-1, GM0090-5, GM0090-24...
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

        resultados = list(
            self.db["DtoProduto"].find(filtro, projection).limit(50)
        )

        return resultados

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
        tks = tokens(termo)

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
            self.db["DtoProduto"].find(filtro, projection).limit(20)
        )

    def buscar_produtos(self, termo):
        termo, termo_numerico = self._normalizar_termo(termo)

        if not termo:
            return []

        cache_key = f"busca_produtos::{termo.lower()}::{termo_numerico}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        projection = self._projection_produto()

        # 1. Código de barras exato
        resultados = self._buscar_barras_exato(termo, termo_numerico, projection)
        if resultados:
            cache.set(cache_key, resultados, self.cache_ttl)
            return resultados

        # 2. Código interno por prefixo
        resultados = self._buscar_por_codigo_prefixo(termo, projection)
        if resultados:
            cache.set(cache_key, resultados, self.cache_ttl)
            return resultados

        # 3. Busca numérica parcial
        resultados = self._buscar_por_barras_parcial(termo, termo_numerico, projection)
        if resultados:
            cache.set(cache_key, resultados, self.cache_ttl)
            return resultados

        # 4. Busca por nome
        resultados = self._buscar_por_nome(termo, projection)
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