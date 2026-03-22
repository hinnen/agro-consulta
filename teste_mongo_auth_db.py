from pymongo import MongoClient
from urllib.parse import quote_plus

user = quote_plus("Teste Sisvale")
password = quote_plus("Hinnen9973#")
db_name = "9c6f91fb-04e9-42be-aa5d-ec29b43c9a10"

uri = (
    f"mongodb://{user}:{password}"
    f"@db3.wl6.aprendaerp.com.br:27017,"
    f"db4.wl6.aprendaerp.com.br:27017,"
    f"ab2.wl.aprendaerp.com.br:27025/"
    f"{db_name}"
    f"?authSource={db_name}"
    f"&authMechanism=SCRAM-SHA-1"
)

client = MongoClient(
    uri,
    serverSelectionTimeoutMS=10000,
    connectTimeoutMS=10000,
    socketTimeoutMS=20000,
    retryWrites=False,
)

print(client.admin.command("ping"))