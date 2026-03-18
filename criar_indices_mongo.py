from pymongo import MongoClient
from decouple import config


MONGO_URL = config("VENDA_ERP_MONGO_URL")
DATABASE_NAME = config("VENDA_ERP_MONGO_DB")

client = MongoClient(MONGO_URL)
db = client[DATABASE_NAME]

print("Conectado ao MongoDB")
print("Criando índices...")

# Produtos
db["DtoProduto"].create_index("EAN_NFe")
print("OK - índice em DtoProduto.EAN_NFe")

db["DtoProduto"].create_index("CodigoNFe")
print("OK - índice em DtoProduto.CodigoNFe")

db["DtoProduto"].create_index("Codigo")
print("OK - índice em DtoProduto.Codigo")

db["DtoProduto"].create_index("CadastroInativo")
print("OK - índice em DtoProduto.CadastroInativo")

db["DtoProduto"].create_index("NomeNormalizado")
print("OK - índice em DtoProduto.NomeNormalizado")

db["DtoProduto"].create_index("NomeTokens")
print("OK - índice em DtoProduto.NomeTokens")

db["DtoProduto"].create_index("BuscaTexto")
print("OK - índice em DtoProduto.BuscaTexto")

db["DtoProduto"].create_index("EhGranel")
print("OK - índice em DtoProduto.EhGranel")

# Estoque
db["DtoEstoqueDepositoProduto"].create_index("ProdutoID")
print("OK - índice em DtoEstoqueDepositoProduto.ProdutoID")

db["DtoEstoqueDepositoProduto"].create_index("Deposito")
print("OK - índice em DtoEstoqueDepositoProduto.Deposito")

print("Índices criados com sucesso.")