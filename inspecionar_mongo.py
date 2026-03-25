import os
import django
from pprint import pprint

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()

from pymongo import MongoClient
from django.conf import settings


client = MongoClient(settings.VENDA_ERP_MONGO_URL, tls=False, ssl=False)
db = client[settings.VENDA_ERP_MONGO_DB]

print("\n=== COLLECTIONS ===")
collections = db.list_collection_names()
for nome in sorted(collections):
    print(nome)

print("\n=== COLLECTIONS PARECIDAS COM VENDA / MOV / PEDIDO / ITEM ===")
palavras = ["venda", "mov", "pedido", "item", "finance", "nota", "saida"]
for nome in sorted(collections):
    nome_lower = nome.lower()
    if any(p in nome_lower for p in palavras):
        print(nome)

print("\n=== AMOSTRA DE DOCUMENTOS DAS COLLECTIONS SUSPEITAS ===")
for nome in sorted(collections):
    nome_lower = nome.lower()
    if any(p in nome_lower for p in palavras):
        print(f"\n--- {nome} ---")
        doc = db[nome].find_one()
        if doc:
            pprint(doc)
        else:
            print("Sem documentos")