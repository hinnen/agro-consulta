"""
Cliente HTTP da API REST Venda ERP (white label).

Swagger da instância (troque o subdomínio): ``https://<wl>.vendaerp.com.br/api/swagger/index.html``
— use o mesmo host que ``VENDA_ERP_API_BASE_URL`` quando a API for nesse domínio.

Relatórios HTTP **v3** (dashboard gerencial): ``{base}/v3/PedidosItens/Report`` e
``{base}/v3/CondensadoVendasPorVendedor/Report`` — ex.: host SisVale
``https://sisvale.vendaerp.com.br`` sem sufixo ``/api``.
"""
import logging
import re
from collections import defaultdict

import requests
from decouple import config
from datetime import date

logger = logging.getLogger(__name__)


def _django_setting(name, default=""):
    try:
        from django.conf import settings as dj_settings

        return (getattr(dj_settings, name, None) or default or "").strip()
    except Exception:
        return (default or "").strip()


def _env_or_setting_path(key: str) -> str:
    """Sufixo após /api/request/ — .env (decouple) ou settings Django."""
    v = (config(key, default="") or "").strip().strip("/")
    if v:
        return v
    return (_django_setting(key) or "").strip().strip("/")


def _erp_json_tem_linhas_pessoa(data):
    if isinstance(data, list) and len(data) > 0:
        return True
    if not isinstance(data, dict):
        return False
    for k in ("Data", "data", "Pessoas", "pessoas", "Items", "items", "Result", "result"):
        v = data.get(k)
        if isinstance(v, list) and len(v) > 0:
            return True
    return False


class VendaERPAPIClient:
    def __init__(self, base_url=None, token=None, user=None, app=None):
        bu = (base_url or "").strip().rstrip("/") if base_url else ""
        if not bu:
            bu = (config("VENDA_ERP_API_URL", default="") or _django_setting("VENDA_ERP_API_BASE_URL")).strip().rstrip("/")
        if not bu:
            bu = "https://cw.vendaerp.com.br"
        self.base_url = bu

        tok = (token if token is not None else "") or config("VENDA_ERP_API_TOKEN", default="") or _django_setting(
            "VENDA_ERP_API_TOKEN"
        )
        self.token = str(tok).strip()
        self.user = (user or config("VENDA_ERP_API_USER", default="")).strip()
        self.app = (app or config("VENDA_ERP_API_APP", default="")).strip()

    def testar_conexao(self):
        """Tenta apenas listar os pedidos para ver se o Token é válido"""
        url = f"{self.base_url}/api/request/Pedidos/GetTodosPedidos"
        headers = {
            "Authorization-Token": self.token,
            "User": self.user,
            "App": self.app,
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
        if not self.token:
            return (
                False,
                0,
                "Configure VENDA_ERP_API_TOKEN no .env ou o token na Integração ERP (Admin). "
                "Se a API exigir usuário, defina também VENDA_ERP_API_USER.",
            )

        url = f"{self.base_url}/api/request/Pedidos/Salvar"
        # Corpo: JSON objeto completo (não JSON Patch). No Swagger, escolha ``application/json``, não json-patch+json.
        headers = {
            "Authorization-Token": self.token,
            "User": self.user,
            "App": self.app,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        try:
            res = requests.post(url, json=payload, headers=headers, timeout=30)
            texto = (res.text or "")[:4000]
            print(f"--- Pedidos/Salvar --- HTTP {res.status_code} --- {texto[:500]}")
            if 200 <= res.status_code < 300:
                try:
                    return True, res.status_code, res.json()
                except Exception:
                    return True, res.status_code, texto or "OK"
            try:
                return False, res.status_code, res.json()
            except Exception:
                return False, res.status_code, texto or res.reason
        except Exception as e:
            return False, 0, str(e)

    def buscar_produtos(self, termo):
        """Busca produtos na API do VendaERP"""
        url = "https://api.vendaerp.com.br/produtos/GetTodos"
        headers = {
            "Authorization-Token": self.token,
            "User": self.user,
            "App": self.app,
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

    def _headers_get(self):
        return {
            "Authorization-Token": self.token,
            "User": self.user,
            "App": self.app,
            "Accept": "application/json",
        }

    @staticmethod
    def _flatten_result_list(payload):
        if isinstance(payload, list):
            return payload
        if not isinstance(payload, dict):
            return []
        for key in ("Data", "data", "Items", "items", "Result", "result", "Pedidos", "pedidos"):
            v = payload.get(key)
            if isinstance(v, list):
                return v
        return []

    def pedidos_listar_periodo(self, data_ini: date, data_fim: date, page_size: int = 200, skip: int = 0):
        """
        Busca pedidos (vendas) no ERP para intervalo de datas.
        Tenta variações de parâmetros/casing por compatibilidade entre WLs.
        """
        if not self.token:
            return False, []
        url = f"{self.base_url}/api/request/Pedidos/GetTodosPedidos"
        di = (data_ini.isoformat() if hasattr(data_ini, "isoformat") else str(data_ini or ""))[:10]
        df = (data_fim.isoformat() if hasattr(data_fim, "isoformat") else str(data_fim or ""))[:10]
        ps = min(max(int(page_size or 200), 1), 500)
        sk = max(int(skip or 0), 0)
        param_sets = (
            {"dataInicial": di, "dataFinal": df, "pageSize": ps, "skip": sk},
            {"DataInicial": di, "DataFinal": df, "PageSize": ps, "Skip": sk},
            {"dataDe": di, "dataAte": df, "pageSize": ps, "skip": sk},
            {"DataDe": di, "DataAte": df, "PageSize": ps, "Skip": sk},
            {"dtInicial": di, "dtFinal": df, "pageSize": ps, "skip": sk},
            {"DtInicial": di, "DtFinal": df, "PageSize": ps, "Skip": sk},
            {"dataInicial": di, "dataFinal": df},
            {"DataInicial": di, "DataFinal": df},
        )
        try:
            for params in param_sets:
                res = requests.get(url, headers=self._headers_get(), params=params, timeout=45)
                if not (200 <= res.status_code < 300):
                    continue
                try:
                    payload = res.json()
                except Exception:
                    payload = []
                rows = self._flatten_result_list(payload)
                if rows or params is param_sets[-1]:
                    return True, rows
            return False, []
        except Exception as e:
            logger.warning("VendaERP pedidos_listar_periodo erro: %s", e)
            return False, []

    def pessoas_pesquisar(
        self,
        nomefantasia="",
        cpfcnpj="",
        page_size=80,
        skip=0,
        cliente=True,
        fornecedor=False,
    ):
        """
        GET /api/request/Pessoas/Pesquisar (Swagger: camelCase; alguns hosts usam PascalCase).
        """
        if not self.token:
            return False, []
        url = f"{self.base_url}/api/request/Pessoas/Pesquisar"
        ps = min(max(int(page_size or 80), 1), 500)
        sk = max(int(skip or 0), 0)
        nf = (nomefantasia or "")[:200]
        cp = (cpfcnpj or "")[:20]
        cli = str(bool(cliente)).lower()
        forn = str(bool(fornecedor)).lower()

        param_sets = [
            {
                "nomefantasia": nf,
                "cpfcnpj": cp,
                "pageSize": ps,
                "skip": sk,
                "cliente": cli,
                "fornecedor": forn,
            },
            {
                "NomeFantasia": nf,
                "CpfCnpj": cp,
                "PageSize": ps,
                "Skip": sk,
                "Cliente": cli,
                "Fornecedor": forn,
            },
            {
                "nomefantasia": nf,
                "cpfcnpj": cp,
                "pageSize": ps,
                "skip": sk,
            },
            {
                "NomeFantasia": nf,
                "CpfCnpj": cp,
                "PageSize": ps,
                "Skip": sk,
            },
        ]
        try:
            for i, params in enumerate(param_sets):
                res = requests.get(url, headers=self._headers_get(), params=params, timeout=30)
                if not (200 <= res.status_code < 300):
                    continue
                try:
                    j = res.json()
                except Exception:
                    j = []
                if j is None:
                    j = []
                if _erp_json_tem_linhas_pessoa(j) or i == len(param_sets) - 1:
                    return True, j
            return False, []
        except Exception:
            return False, []

    def pessoas_get_all(self, page_size=200, skip=0):
        """GET /api/request/Pessoas/GetAll (tenta pageSize/skip e PageSize/Skip)."""
        if not self.token:
            return False, []
        url = f"{self.base_url}/api/request/Pessoas/GetAll"
        ps = min(max(int(page_size or 200), 1), 500)
        sk = max(int(skip or 0), 0)
        param_variants = (
            {"pageSize": ps, "skip": sk},
            {"PageSize": ps, "Skip": sk},
        )
        last_status = 0
        try:
            for params in param_variants:
                res = requests.get(url, headers=self._headers_get(), params=params, timeout=45)
                last_status = res.status_code
                if 200 <= res.status_code < 300:
                    try:
                        return True, res.json()
                    except Exception:
                        return True, []
            return False, {"_http_status": last_status, "_body": ""}
        except Exception as e:
            return False, {"_erro": str(e)}

    def _headers_post(self):
        return {
            "Authorization-Token": self.token,
            "User": self.user,
            "App": self.app,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _financeiro_post(self, path: str, body: dict, *, timeout: int = 60) -> tuple[bool, object]:
        if not path or not self.token:
            return False, "API financeira não configurada ou sem token"
        url = f"{self.base_url}/api/request/{path}"
        try:
            res = requests.post(url, json=body, headers=self._headers_post(), timeout=timeout)
            logger.info("VendaERP financeiro POST %s → HTTP %s", path, res.status_code)
            if 200 <= res.status_code < 300:
                try:
                    return True, res.json()
                except Exception:
                    return True, res.text or "OK"
            try:
                return False, res.json()
            except Exception:
                return False, (res.text or res.reason or "")[:2000]
        except Exception as e:
            logger.warning("VendaERP financeiro POST %s erro: %s", path, e)
            return False, str(e)

    def financeiro_tentar_baixa_api(self, body: dict):
        """
        POST opcional após baixa no Mongo. Configure ``VENDA_ERP_API_FINANCEIRO_BAIXA_PATH``
        com o sufixo após ``/api/request/`` (ex.: ``Lancamentos/SalvarBaixa`` — confirmar no Swagger do WL).

        O corpo inclui ``titulos`` (snapshot dos documentos após a baixa), além de ``ids`` / ``tipo`` / ``payload``.
        Em **baixa parcial**, também são enviados ``parcelas_baixa``, ``pagamentos`` e ``pagamentos_relacionados``
        (linhas no estilo da aba *Pagamentos* do SisVale), para o servidor poder gravar movimentos filhos.
        """
        path = _env_or_setting_path("VENDA_ERP_API_FINANCEIRO_BAIXA_PATH")
        return self._financeiro_post(path, body, timeout=60)

    def financeiro_tentar_lancamentos_api(self, body: dict):
        """
        POST opcional após lançamento manual no Agro. Configure ``VENDA_ERP_API_FINANCEIRO_LANCAMENTO_PATH``
        (sufixo após ``/api/request/``). O corpo traz ``titulos`` com recorte do DtoLancamento gravado no Mongo.
        """
        path = _env_or_setting_path("VENDA_ERP_API_FINANCEIRO_LANCAMENTO_PATH")
        return self._financeiro_post(path, body, timeout=60)

    # --- Relatórios HTTP v3 (ex.: ``https://<wl>.vendaerp.com.br/v3/.../Report``) ---

    @staticmethod
    def _linhas_de_resposta_relatorio_v3(payload) -> list[dict]:
        """Extrai lista de objetos (linhas) de respostas JSON típicas de relatório."""
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        if not isinstance(payload, dict):
            return []
        for key in (
            "value",
            "Data",
            "data",
            "Items",
            "items",
            "Result",
            "result",
            "Rows",
            "rows",
            "Lista",
            "lista",
            "Relatorio",
            "relatorio",
            "Pedidos",
            "pedidos",
        ):
            v = payload.get(key)
            if isinstance(v, list):
                out = [x for x in v if isinstance(x, dict)]
                if out:
                    return out
        best: list[dict] = []
        for _k, v in payload.items():
            if isinstance(v, list):
                cand = [x for x in v if isinstance(x, dict)]
                if len(cand) > len(best):
                    best = cand
        return best

    def _v3_report_linhas(
        self,
        path_after_v3: str,
        data_ini: date,
        data_fim: date,
        *,
        timeout: int = 90,
    ) -> tuple[bool, list[dict]]:
        """
        Busca linhas em ``{base_url}/v3/{path_after_v3}`` (GET com query e, se vazio, POST JSON).
        Usa os mesmos headers de token que ``/api/request/``.
        """
        if not self.token:
            return False, []
        base = self.base_url.rstrip("/")
        suf = (path_after_v3 or "").strip().lstrip("/")
        if not suf:
            return False, []
        urls = (f"{base}/v3/{suf}", f"{base}/v3/{suf}/")
        di = data_ini.isoformat()[:10]
        df = data_fim.isoformat()[:10]
        query_variants = (
            {"dataInicial": di, "dataFinal": df},
            {"DataInicial": di, "DataFinal": df},
            {"dataDe": di, "dataAte": df},
            {"DataDe": di, "DataAte": df},
            {"dtInicial": di, "dtFinal": df},
            {"DtInicial": di, "DtFinal": df},
            {"Inicio": di, "Fim": df},
            {"PeriodoInicial": di, "PeriodoFinal": df},
        )
        body_variants = (
            {"dataInicial": di, "dataFinal": df},
            {"DataInicial": di, "DataFinal": df},
            {"dataDe": di, "dataAte": df},
            {"DataDe": di, "DataAte": df},
            {"filtros": {"DataInicial": di, "DataFinal": df}},
            {"filtros": {"dataInicial": di, "dataFinal": df}},
            {"Filtros": {"DataInicial": di, "DataFinal": df}},
        )
        headers_get = self._headers_get()
        headers_post = self._headers_post()
        last_err = ""
        for url in urls:
            for params in query_variants:
                try:
                    res = requests.get(url, headers=headers_get, params=params, timeout=timeout)
                    last_err = f"GET {url} {params!r} → HTTP {res.status_code}"
                    if not (200 <= res.status_code < 300):
                        continue
                    try:
                        payload = res.json()
                    except Exception:
                        continue
                    rows = self._linhas_de_resposta_relatorio_v3(payload)
                    if rows:
                        logger.info("VendaERP v3 relatório OK: %s (%s linhas)", last_err, len(rows))
                        return True, rows
                except Exception as e:
                    last_err = str(e)
                    logger.warning("VendaERP v3 GET %s: %s", url, e)
            for body in body_variants:
                try:
                    res = requests.post(url, headers=headers_post, json=body, timeout=timeout)
                    last_err = f"POST {url} → HTTP {res.status_code}"
                    if not (200 <= res.status_code < 300):
                        continue
                    try:
                        payload = res.json()
                    except Exception:
                        continue
                    rows = self._linhas_de_resposta_relatorio_v3(payload)
                    if rows:
                        logger.info("VendaERP v3 relatório OK: %s (%s linhas)", last_err, len(rows))
                        return True, rows
                except Exception as e:
                    last_err = str(e)
                    logger.warning("VendaERP v3 POST %s: %s", url, e)
        logger.info("VendaERP v3 relatório sem linhas: %s", last_err[:300])
        return False, []

    def relatorio_pedidos_itens_report(self, data_ini: date, data_fim: date):
        """
        Relatório de itens de pedidos (ex.: ``/v3/PedidosItens/Report`` no host configurado em
        ``VENDA_ERP_API_URL`` / ``VENDA_ERP_API_BASE_URL``).
        """
        return self._v3_report_linhas("PedidosItens/Report", data_ini, data_fim)

    def relatorio_condensado_vendas_por_vendedor_report(self, data_ini: date, data_fim: date):
        """
        Condensado de vendas por vendedor (ex.: ``/v3/CondensadoVendasPorVendedor/Report``).
        """
        return self._v3_report_linhas("CondensadoVendasPorVendedor/Report", data_ini, data_fim)


def _row_lc(row: dict) -> dict[str, object]:
    return {str(k).lower(): v for k, v in row.items()}


def _g(row: dict, *names: str):
    lm = _row_lc(row)
    for n in names:
        if not n:
            continue
        k = str(n).lower()
        if k in lm:
            return lm[k]
    return None


def _cell_float(v) -> float:
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s:
        return 0.0
    s = re.sub(r"^\s*R\$\s*", "", s, flags=re.I).replace(" ", "")
    if re.search(r",\d{1,2}$", s) and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def _cell_str(v) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    return s if s and s.lower() not in ("none", "null") else ""


def normalizar_linhas_top_produtos_v3(rows: list[dict], *, limite: int = 8) -> list[dict]:
    """
    Converte linhas do relatório ``PedidosItens/Report`` para o formato do dashboard
    (nome, total, qtd_total), agregando por nome normalizado.
    """
    limite = max(1, min(int(limite or 8), 30))
    agg: dict[str, dict] = defaultdict(lambda: {"nome": "", "total": 0.0, "qtd_total": 0.0})
    for row in rows:
        nome = _cell_str(
            _g(
                row,
                "nomeproduto",
                "produto",
                "descricao",
                "nome",
                "descricaoproduto",
                "produtonome",
                "item",
                "denominacao",
                "produtodescricao",
            )
        )
        if not nome:
            continue
        key = nome.strip().upper()[:160]
        qtd = _cell_float(
            _g(
                row,
                "quantidade",
                "qtd",
                "qtde",
                "quantidadetotal",
                "qtdtotal",
                "volume",
                "quant",
            )
        )
        val = _cell_float(
            _g(
                row,
                "valortotal",
                "total",
                "valor",
                "subtotal",
                "valoritem",
                "vltotal",
                "faturamento",
                "valorr",
            )
        )
        if val <= 0 and qtd > 0:
            pu = _cell_float(
                _g(row, "precounitario", "valorunitario", "preco", "vlunitario", "unitario")
            )
            val = pu * qtd
        e = agg[key]
        if not e["nome"]:
            e["nome"] = nome[:50]
        e["total"] += val
        e["qtd_total"] += qtd
    out = sorted(agg.values(), key=lambda x: x["total"], reverse=True)[:limite]
    return [
        {
            "nome": (x["nome"] or "?")[:50],
            "total": round(float(x["total"]), 2),
            "qtd_total": round(float(x["qtd_total"]), 3),
        }
        for x in out
        if x["total"] > 0 or x["qtd_total"] > 0
    ]


def normalizar_linhas_ranking_vendedores_v3(rows: list[dict], *, limite: int = 8) -> list[dict]:
    """Linhas de ``CondensadoVendasPorVendedor/Report`` → nome, total, n_vendas."""
    limite = max(1, min(int(limite or 8), 30))
    agg: dict[str, dict] = defaultdict(lambda: {"nome": "", "total": 0.0, "n_vendas": 0})
    for row in rows:
        nome = _cell_str(
            _g(
                row,
                "vendedor",
                "nomevendedor",
                "usuario",
                "nome",
                "descricao",
                "vendedornome",
                "atendente",
                "funcionario",
            )
        )
        if not nome:
            continue
        key = nome.strip().upper()[:160]
        val = _cell_float(
            _g(
                row,
                "total",
                "valortotal",
                "faturamento",
                "venda",
                "valor",
                "totalvenda",
                "vlvendas",
            )
        )
        nv = _cell_float(
            _g(
                row,
                "quantidadepedidos",
                "pedidos",
                "nvendas",
                "numeropedidos",
                "qtdpedidos",
                "vendas",
                "n",
                "qtde",
                "qtd",
            )
        )
        nvi = int(round(nv)) if nv > 0 else 0
        if nvi <= 0 and val > 0:
            nvi = 1
        e = agg[key]
        if not e["nome"]:
            e["nome"] = nome[:120]
        e["total"] += val
        e["n_vendas"] += nvi
    out = sorted(agg.values(), key=lambda x: x["total"], reverse=True)[:limite]
    return [
        {
            "nome": x["nome"][:120],
            "total": round(float(x["total"]), 2),
            "n_vendas": int(x["n_vendas"]),
        }
        for x in out
        if x["total"] > 0 or x["n_vendas"] > 0
    ]