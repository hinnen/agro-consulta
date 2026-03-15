import re

from pymongo import MongoClient
from django.conf import settings
from django.core.cache import cache


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
        }

    def buscar_produtos(self, termo):
        termo = (termo or "").strip()
        if not termo:
            return []

        cache_key = f"busca_produtos::{termo.lower()}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        projection = self._projection_produto()

        # 1. tenta busca exata por código de barras
        produto_exato = self.db["DtoProduto"].find_one(
            {
                "EAN_NFe": termo,
                "CadastroInativo": False,
            },
            projection,
        )
        if produto_exato:
            resultado = [produto_exato]
            cache.set(cache_key, resultado, self.cache_ttl)
            return resultado

        # 2. tenta busca exata por código interno
        produto_exato = self.db["DtoProduto"].find_one(
            {
                "$or": [
                    {"CodigoNFe": termo},
                    {"Codigo": termo},
                ],
                "CadastroInativo": False,
            },
            projection,
        )
        if produto_exato:
            resultado = [produto_exato]
            cache.set(cache_key, resultado, self.cache_ttl)
            return resultado

        # 3. fallback para busca parcial
        termo_regex = re.escape(termo)

        filtro = {
            "$and": [
                {"CadastroInativo": False},
                {
                    "$or": [
                        {"Nome": {"$regex": termo_regex, "$options": "i"}},
                        {"CodigoNFe": {"$regex": termo_regex, "$options": "i"}},
                        {"EAN_NFe": {"$regex": termo_regex, "$options": "i"}},
                        {"Marca": {"$regex": termo_regex, "$options": "i"}},
                    ]
                },
            ]
        }

        resultados = list(
            self.db["DtoProduto"]
            .find(filtro, projection)
            .limit(15)
        )

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