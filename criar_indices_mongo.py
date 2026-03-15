from pymongo import MongoClient
from decouple import config

MONGO_URL = config("VENDA_ERP_MONGO_URL")
DATABASE_NAME = config("VENDA_ERP_MONGO_DB")

client = MongoClient(MONGO_URL)
db = client[DATABASE_NAME]

print("Conectado ao MongoDB")
print("Criando índices...")

# Produto
db["DtoProduto"].create_index("EAN_NFe")
print("OK - índice em DtoProduto.EAN_NFe")

db["DtoProduto"].create_index("CodigoNFe")
print("OK - índice em DtoProduto.CodigoNFe")

db["DtoProduto"].create_index("Nome")
print("OK - índice em DtoProduto.Nome")

db["DtoProduto"].create_index("Marca")
print("OK - índice em DtoProduto.Marca")

db["DtoProduto"].create_index("CadastroInativo")
print("OK - índice em DtoProduto.CadastroInativo")

# Estoque
db["DtoEstoqueDepositoProduto"].create_index("ProdutoID")
print("OK - índice em DtoEstoqueDepositoProduto.ProdutoID")

db["DtoEstoqueDepositoProduto"].create_index("Deposito")
print("OK - índice em DtoEstoqueDepositoProduto.Deposito")

print("Finalizado.")