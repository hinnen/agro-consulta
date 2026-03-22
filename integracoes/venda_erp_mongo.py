from pymongo import MongoClient
from urllib.parse import quote_plus


class VendaERPMongoClient:
    def __init__(self):
        user = quote_plus("Teste Sisvale")
        password = quote_plus("Hinnen9973#")

        self.uri = (
            f"mongodb://{user}:{password}"
            f"@db3.wl6.aprendaerp.com.br:27017,"
            f"db4.wl6.aprendaerp.com.br:27017,"
            f"ab2.wl.aprendaerp.com.br:27025/"
            f"9c6f91fb-04e9-42be-aa5d-ec29b43c9a10"
            f"?tls=true"
            f"&tlsAllowInvalidCertificates=true"
            f"&authSource=admin"
        )

        self.client = MongoClient(
            self.uri,
            serverSelectionTimeoutMS=10000,
            connectTimeoutMS=10000,
            socketTimeoutMS=20000,
        )

        self.db = self.client["9c6f91fb-04e9-42be-aa5d-ec29b43c9a10"]

        self.col_p = "DtoProduto"
        self.col_e = "DtoEstoqueDepositoProduto"
        self.col_c = "DtoPessoa"

        self.DEPOSITO_CENTRO = "698e36e0d34f9b3013b16da6"
        self.DEPOSITO_VILA_ELIAS = "69960ed00a7abd17679e2ec7"