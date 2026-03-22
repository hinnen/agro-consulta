import os
import django
import sys
import ssl

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "SEU_PROJETO.settings")
django.setup()

from integracoes.venda_erp_mongo import VendaERPMongoClient

print("PYTHON:", sys.executable)
print("OPENSSL:", ssl.OPENSSL_VERSION)

c = VendaERPMongoClient()
print(c.client.admin.command("ping"))