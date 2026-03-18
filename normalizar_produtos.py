from pymongo import MongoClient
from decouple import config

from integracoes.texto import normalizar, tokens, montar_busca_texto, eh_granel


client = MongoClient(config("VENDA_ERP_MONGO_URL"))
db = client[config("VENDA_ERP_MONGO_DB")]
col = db["DtoProduto"]

total = col.count_documents({})
print(f"Total de produtos: {total}")

contador = 0

for doc in col.find(
    {},
    {
        "_id": 1,
        "Nome": 1,
        "Marca": 1,
        "Categoria": 1,
        "SubCategoria": 1,
    }
):
    nome = doc.get("Nome") or ""
    marca = doc.get("Marca") or ""
    categoria = doc.get("Categoria") or ""
    subcategoria = doc.get("SubCategoria") or ""

    nome_norm = normalizar(nome)
    nome_tokens = tokens(nome)
    busca_texto = montar_busca_texto(
        nome=nome,
        marca=marca,
        categoria=categoria,
        subcategoria=subcategoria,
    )
    produto_granel = eh_granel(
        categoria=categoria,
        subcategoria=subcategoria,
        nome=nome,
    )

    col.update_one(
        {"_id": doc["_id"]},
        {
            "$set": {
                "NomeNormalizado": nome_norm,
                "NomeTokens": nome_tokens,
                "BuscaTexto": busca_texto,
                "EhGranel": produto_granel,
            }
        }
    )

    contador += 1
    if contador % 500 == 0:
        print(f"{contador} produtos processados...")

print("Produtos normalizados com sucesso.")
