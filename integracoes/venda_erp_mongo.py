import pymongo
from pymongo import MongoClient

class VendaERPMongoClient:
    def __init__(self):
        # URI com tratamento de caracteres especiais na senha e login
        self.uri = "mongodb+srv://Teste Sisvale:Hinnen9973%23@wl6.aprendaerp.com.br/admin?readPreference=primaryPreferred" 
        self.client = MongoClient(self.uri, serverSelectionTimeoutMS=10000)
        self.db = self.client['9c6f91fb-04e9-42be-aa5d-ec29b43c9a10']
        
        # Nomes das Coleções
        self.col_p = "DtoProduto"
        self.col_e = "DtoEstoqueDepositoProduto"
        self.col_c = "DtoPessoa"
        
        # IDs de Depósito (Conforme seu banco de dados)
        self.DEPOSITO_CENTRO = "698e36e0d34f9b3013b16da6"
        # Verifique se este ID da Vila é exatamente este no Mongo (uuid vs objectid)
        self.DEPOSITO_VILA_ELIAS = "8226b1e9-05a6-496f-8a21-751b345g0b21" 

    def buscar_clientes(self, termo, limite=10):
        if not termo: return []
        query = {
            "$or": [
                {"Nome": {"$regex": termo, "$options": "i"}}, 
                {"CpfCnpj": {"$regex": termo, "$options": "i"}}
            ]
        }
        return list(self.db[self.col_c].find(query).limit(limite))

    def buscar_estoque_por_ids(self, lista_ids):
        """ Busca estoque de vários produtos de uma vez (Performance) """
        return list(self.db[self.col_e].find({"ProdutoID": {"$in": lista_ids}}))