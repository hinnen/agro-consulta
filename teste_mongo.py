from pymongo import MongoClient
from pprint import pprint
from decouple import config

MONGO_URL = config("VENDA_ERP_MONGO_URL")
DATABASE_NAME = config("VENDA_ERP_MONGO_DB")

client = MongoClient(MONGO_URL, tls=False, ssl=False)
db = client[DATABASE_NAME]

termo = "milho grande 47 kg"

produto = db["DtoProduto"].find_one(
    {"Nome": {"$regex": termo, "$options": "i"}},
    {
        "Nome": 1,
        "Codigo": 1,
        "CodigoNFe": 1,
        "EAN_NFe": 1,
        "PrecoVenda": 1,
    }
)

print("=" * 80)
print("PRODUTO")
print("=" * 80)
pprint(produto)

if produto:
    print("\nID DO PRODUTO:", produto["_id"], type(produto["_id"]))

    print("\n" + "=" * 80)
    print("ESTOQUES PELO ProdutoID")
    print("=" * 80)

    estoques = list(
        db["DtoEstoqueDepositoProduto"].find(
            {"ProdutoID": produto["_id"]},
            {
                "Produto": 1,
                "ProdutoID": 1,
                "Deposito": 1,
                "DepositoID": 1,
                "Saldo": 1,
                "EstoqueMinimo": 1,
            }
        )
    )

    print("Quantidade de estoques encontrados:", len(estoques))
    for e in estoques:
        pprint(e)

    print("\n" + "=" * 80)
    print("ESTOQUES PELO NOME")
    print("=" * 80)

    estoques_nome = list(
        db["DtoEstoqueDepositoProduto"].find(
            {"Produto": {"$regex": termo, "$options": "i"}},
            {
                "Produto": 1,
                "ProdutoID": 1,
                "Deposito": 1,
                "DepositoID": 1,
                "Saldo": 1,
                "EstoqueMinimo": 1,
            }
        )
    )

    print("Quantidade de estoques por nome:", len(estoques_nome))
    for e in estoques_nome:
        pprint(e)