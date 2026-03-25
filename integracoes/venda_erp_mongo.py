from pymongo import MongoClient
from decouple import config


class VendaERPMongoClient:
    def __init__(self):
        self.uri = config("VENDA_ERP_MONGO_URL")

        self.client = MongoClient(
            self.uri,
            serverSelectionTimeoutMS=10000,
            connectTimeoutMS=10000,
            socketTimeoutMS=20000,
            retryWrites=False,
            tls=False,
            ssl=False,
        )

        self.db = self.client[config("VENDA_ERP_MONGO_DB")]

        self.col_p = "DtoProduto"
        self.col_e = "DtoEstoqueDepositoProduto"
        self.col_c = "DtoPessoa"

        self.DEPOSITO_CENTRO = "698e36e0d34f9b3013b16da6"
        self.DEPOSITO_VILA_ELIAS = "69960ed00a7abd17679e2ec7"