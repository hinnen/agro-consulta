import pymongo
from pymongo import MongoClient

class VendaERPMongoClient:
    def __init__(self):
        self.uri = "mongodb+srv://Teste%20Sisvale:Hinnen9973%23@wl6.aprendaerp.com.br/admin?readPreference=primaryPreferred&tls=false" 
        self.client = MongoClient(self.uri, serverSelectionTimeoutMS=10000)
        self.db = self.client['9c6f91fb-04e9-42be-aa5d-ec29b43c9a10']
        
        self.col_p = "DtoProduto"
        self.col_e = "DtoEstoqueDepositoProduto"
        self.col_c = "DtoPessoa"
        
        # IDs REAIS que o seu terminal mostrou:
        self.DEPOSITO_CENTRO = "698e36e0d34f9b3013b16da6" # Deposito Centro
        self.DEPOSITO_VILA_ELIAS = "8226b1e9-05a6-496f-8a21-751b345g0b21" # Ajustaremos se for outro

    def buscar_clientes(self, termo, limite=10):
        query = {"$or": [{"Nome": {"$regex": termo, "$options": "i"}}, {"CpfCnpj": {"$regex": termo, "$options": "i"}}]}
        return list(self.db[self.col_c].find(query).limit(limite))