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

db["DtoProduto"].create_index("CodigoBarras")
print("OK - índice em DtoProduto.CodigoBarras")

for _sku in ("Sku", "SKU", "Referencia", "CodigoReferencia", "CodigoInterno"):
    try:
        db["DtoProduto"].create_index(_sku)
        print(f"OK - índice em DtoProduto.{_sku}")
    except Exception as exc:
        print(f"Aviso - índice {_sku}: {exc}")

# Similares (multikey): acelera $elemMatch em listas vindas do ERP
for _path in (
    "Similares.CodigoBarras",
    "Similares.EAN",
    "ProdutosSimilares.CodigoBarras",
    "ProdutosSimilares.EAN",
    "ListaSimilares.CodigoBarras",
):
    try:
        db["DtoProduto"].create_index(_path)
        print(f"OK - índice em DtoProduto.{_path}")
    except Exception as exc:
        print(f"Aviso - índice {_path}: {exc}")

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

db["DtoProduto"].create_index("index_codigos")
print("OK - índice multikey em DtoProduto.index_codigos")

# Estoque
db["DtoEstoqueDepositoProduto"].create_index("ProdutoID")
print("OK - índice em DtoEstoqueDepositoProduto.ProdutoID")

db["DtoEstoqueDepositoProduto"].create_index("Deposito")
print("OK - índice em DtoEstoqueDepositoProduto.Deposito")

print("Índices criados com sucesso.")