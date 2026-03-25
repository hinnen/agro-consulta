import requests
from decouple import config

API_URL = "https://api.vendaerp.com.br/produtos"
TOKEN = config("VENDA_ERP_API_TOKEN")

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

try:
    response = requests.get(API_URL, headers=headers)

    print("STATUS:", response.status_code)
    print("RESPOSTA:")
    print(response.text)

except Exception as e:
    print("Erro:", e)