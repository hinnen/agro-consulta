from pymongo import MongoClient
from django.conf import settings


class VendaERPMongoClient:
    def __init__(self):
        self.client = MongoClient(settings.VENDA_ERP_MONGO_URL)
        self.db = self.client[settings.VENDA_ERP_MONGO_DB]

    def buscar_produtos(self, termo):
        filtro = {
            "$or": [
                {"Nome": {"$regex": termo, "$options": "i"}},
                {"CodigoNFe": {"$regex": termo, "$options": "i"}},
                {"EAN_NFe": {"$regex": termo, "$options": "i"}},
                {"Marca": {"$regex": termo, "$options": "i"}},
            ],
            "CadastroInativo": False
        }

        return list(
            self.db["DtoProduto"].find(
                filtro,
                {
                    "Nome": 1,
                    "Codigo": 1,
                    "CodigoNFe": 1,
                    "EAN_NFe": 1,
                    "Marca": 1,
                    "Categoria": 1,
                    "PrecoVenda": 1,
                    "PrecoCusto": 1,
                    "CadastroInativo": 1,
                }
            ).limit(50)
        )

    def buscar_estoques_por_produto_ids(self, produto_ids):
        if not produto_ids:
            return []

        produto_ids_str = [str(pid) for pid in produto_ids]

        return list(
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