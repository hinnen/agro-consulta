import pymongo
from django.conf import settings
import re

class VendaERPMongoClient:
    def __init__(self):
        self.url = getattr(settings, "VENDA_ERP_MONGO_URL", "")
        self.db_name = getattr(settings, "VENDA_ERP_MONGO_DB", "")
        self.client = pymongo.MongoClient(self.url)
        self.db = self.client[self.db_name]
        
        # IDs Oficiais mapeados pela Manus
        self.DEPOSITO_VILA_ELIAS = "69960ed00a7abd17679e2ec7"
        self.DEPOSITO_CENTRO = "698e36e0d34f9b3013b16da6"

    def buscar_produtos(self, termo, limite=20):
        # Lógica de Tokens: Busca produtos que contenham TODAS as palavras digitadas
        palavras = termo.strip().split()
        regex_parts = [f"(?=.*{re.escape(p)})" for p in palavras]
        regex_final = "".join(regex_parts)
        
        filtro = {
            "$or": [
                {"Nome": {"$regex": regex_final, "$options": "i"}},
                {"CodigoNFe": {"$regex": termo, "$options": "i"}}
            ]
        }
        # Ordenação: Produtos normais primeiro, Granel depois
        return list(self.db["DtoProduto"].find(filtro).sort([("EhGranel", 1), ("Nome", 1)]).limit(limite))

    def buscar_estoques_por_produto_ids(self, ids):
        # Busca apenas nos depósitos oficiais da Agro Mais
        return list(self.db["DtoEstoqueDepositoProduto"].find({
            "ProdutoID": {"$in": ids},
            "DepositoID": {"$in": [self.DEPOSITO_CENTRO, self.DEPOSITO_VILA_ELIAS]}
        }))