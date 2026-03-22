import pymongo
from pymongo import MongoClient
from urllib.parse import quote_plus

class VendaERPMongoClient:
    def __init__(self):
        user = quote_plus("Teste Sisvale")
        password = quote_plus("Hinnen9973#")
        
        # Coloquei os parâmetros de SSL direto na URI para não ter erro de interpretação
        self.uri = (
            f"mongodb+srv://{user}:{password}@wl6.aprendaerp.com.br/"
            "?retryWrites=true&w=majority&tls=true&tlsAllowInvalidCertificates=true"
        )
        
        # Conexão direta ignorando validação de SSL do Windows
        self.client = MongoClient(
            self.uri,
            serverSelectionTimeoutMS=5000,
            tlsAllowInvalidCertificates=True 
        )
        
        self.db = self.client['9c6f91fb-04e9-42be-aa5d-ec29b43c9a10']
        self.col_p = "DtoProduto"
        self.col_e = "DtoEstoqueDepositoProduto"
        self.col_c = "DtoPessoa"
        
        self.DEPOSITO_CENTRO = "698e36e0d34f9b3013b16da6"
        self.DEPOSITO_VILA_ELIAS = "8226b1e9-05a6-496f-8a21-751b345g0b21"