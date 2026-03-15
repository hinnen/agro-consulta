import requests

API_URL = "https://api.vendaerp.com.br/produtos"
TOKEN = "SEU_TOKEN_AQUI"

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