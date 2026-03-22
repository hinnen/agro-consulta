import sys
import ssl
from pymongo import MongoClient
from urllib.parse import quote_plus

print("PYTHON:", sys.executable)
print("OPENSSL:", ssl.OPENSSL_VERSION)
print("-" * 80)

user = quote_plus("Teste Sisvale")
password = quote_plus("SUA_SENHA_AQUI")

uris = {
    "URI_HOSTS": (
        f"mongodb://{user}:{password}"
        f"@db3.wl6.aprendaerp.com.br:27017,"
        f"db4.wl6.aprendaerp.com.br:27017,"
        f"ab2.wl.aprendaerp.com.br:27025/"
        f"9c6f91fb-04e9-42be-aa5d-ec29b43c9a10"
        f"?authSource=admin"
    ),
    "URI_SRV": (
        f"mongodb+srv://{user}:{password}"
        f"@wl6.aprendaerp.com.br/"
        f"?retryWrites=true&w=majority"
    ),
}

for nome, uri in uris.items():
    print(f"\nTESTANDO: {nome}")
    try:
        client = MongoClient(
            uri,
            tls=True,
            tlsAllowInvalidCertificates=True,
            serverSelectionTimeoutMS=10000,
            connectTimeoutMS=10000,
            socketTimeoutMS=20000,
            retryWrites=False,
        )
        print(client.admin.command("ping"))
        print(f"{nome} = OK")
    except Exception as e:
        print(f"{nome} = ERRO")
        print(repr(e))