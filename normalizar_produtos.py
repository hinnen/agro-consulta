from pymongo import MongoClient
from decouple import config
from integracoes.texto import normalizar

client = MongoClient(config("VENDA_ERP_MONGO_URL"))
db = client[config("VENDA_ERP_MONGO_DB")]

col = db["DtoProduto"]

total = col.count_documents({})
print(f"Total de produtos: {total}")

contador = 0

for doc in col.find({}, {"_id": 1, "Nome": 1}):
    nome = doc.get("Nome") or ""
    nome_norm = normalizar(nome)

    col.update_one(
        {"_id": doc["_id"]},
        {"$set": {"NomeNormalizado": nome_norm}}
    )

    contador += 1

    if contador % 500 == 0:
        print(f"{contador} produtos processados...")

print("Produtos normalizados com sucesso.")