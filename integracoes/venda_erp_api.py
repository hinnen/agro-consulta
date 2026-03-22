import requests
from django.conf import settings

class VendaERPAPIClient:
    def __init__(self):
        self.base_url = "https://cw.vendaerp.com.br"
        # Garante que o token esteja limpo
        self.token = str(getattr(settings, "VENDA_ERP_API_TOKEN", "")).strip()

    def testar_conexao(self):
        """Tenta apenas listar os pedidos para ver se o Token é válido"""
        url = f"{self.base_url}/api/request/Pedidos/GetTodosPedidos"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json"
        }
        try:
            res = requests.get(url, headers=headers, timeout=10)
            print(f"--- TESTE DE CONEXÃO ---")
            print(f"Status: {res.status_code}")
            print(f"Resposta: {res.text[:200]}") # Pega só o começo
            return res.status_code == 200
        except Exception as e:
            print(f"Erro no teste: {e}")
            return False

    def salvar_operacao_pdv(self, payload):
        url = f"{self.base_url}/api/request/Pedidos/Salvar"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        try:
            res = requests.post(url, json=payload, headers=headers, timeout=15)
            print(f"--- DEBUG SALVAR ---")
            print(f"Status: {res.status_code} | Resposta: {res.text}")
            if 200 <= res.status_code < 300:
                return True, res.status_code, res.json()
            return False, res.status_code, res.text
        except Exception as e:
            return False, 0, str(e)

    def buscar_produtos(self, termo):
        """Busca produtos na API do VendaERP"""
        url = "https://api.vendaerp.com.br/produtos/GetTodos"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json"
        }
        params = {
            "desc": termo,
            "somenteAtivos": True
        }
        try:
            res = requests.get(url, headers=headers, params=params, timeout=15)
            if res.status_code == 200:
                return True, res.json()
            return False, res.text
        except Exception as e:
            return False, str(e)