from pymongo import MongoClient
from decouple import config
from datetime import datetime


def _mongo_timeout_ms(key: str, default: int) -> int:
    """Timeouts em ms; use env no Render se o ERP demorar (médias, estoque). Mínimo 5s."""
    try:
        n = int(config(key, default=str(default), cast=int))
    except (TypeError, ValueError):
        return default
    return max(5000, min(n, 600_000))


class VendaERPMongoClient:
    def __init__(self):
        self.uri = config("VENDA_ERP_MONGO_URL")

        # Padrões mais tolerantes para nuvem (Render → Mongo ERP). Sobrescreva no .env se precisar.
        # AGRO_MONGO_SERVER_SELECTION_TIMEOUT_MS, AGRO_MONGO_CONNECT_TIMEOUT_MS, AGRO_MONGO_SOCKET_TIMEOUT_MS
        sel_ms = _mongo_timeout_ms("AGRO_MONGO_SERVER_SELECTION_TIMEOUT_MS", 30_000)
        conn_ms = _mongo_timeout_ms("AGRO_MONGO_CONNECT_TIMEOUT_MS", 20_000)
        sock_ms = _mongo_timeout_ms("AGRO_MONGO_SOCKET_TIMEOUT_MS", 120_000)

        self.client = MongoClient(
            self.uri,
            serverSelectionTimeoutMS=sel_ms,
            connectTimeoutMS=conn_ms,
            socketTimeoutMS=sock_ms,
            retryWrites=False,
            tls=False,
            ssl=False,
        )

        self.db = self.client[config("VENDA_ERP_MONGO_DB")]

        self.col_p = "DtoProduto"
        self.col_e = "DtoEstoqueDepositoProduto"
        self.col_c = "DtoPessoa"

        self.DEPOSITO_CENTRO = "698e36e0d34f9b3013b16da6"
        self.DEPOSITO_VILA_ELIAS = "69960ed00a7abd17679e2ec7"

    def buscar_estoques_por_produto_ids(self, produto_ids):
        """Saldos por produto (``DtoEstoqueDepositoProduto``). Usado pelo endpoint legado em ``estoque.views``."""
        if not produto_ids:
            return []
        ids = [str(x) for x in produto_ids if x is not None and str(x).strip() != ""]
        if not ids:
            return []
        return list(self.db[self.col_e].find({"ProdutoID": {"$in": ids}}))

    def vendas_agro_collection(self):
        return self.db["vendas_agro"]

    @staticmethod
    def _parse_dt(v):
        if isinstance(v, datetime):
            return v
        if not v:
            return None
        s = str(v).strip()
        if not s:
            return None
        s = s.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(s)
        except Exception:
            pass
        for fmt in ("%d/%m/%Y - %H:%M", "%d/%m/%Y %H:%M", "%d/%m/%Y"):
            try:
                return datetime.strptime(s, fmt)
            except Exception:
                continue
        return None

    @staticmethod
    def _to_float(v):
        try:
            return float(v or 0)
        except (TypeError, ValueError):
            return 0.0

    def normalizar_pedido_para_vendas_agro(self, row: dict):
        if not isinstance(row, dict):
            return None
        venda_id = (
            row.get("Id")
            or row.get("ID")
            or row.get("_id")
            or row.get("PedidoID")
            or row.get("pedido_id")
            or row.get("Numero")
            or row.get("Codigo")
            or row.get("numero")
        )
        if venda_id is None:
            return None
        dt = (
            self._parse_dt(row.get("Data"))
            or self._parse_dt(row.get("data"))
            or self._parse_dt(row.get("DataFaturamento"))
            or self._parse_dt(row.get("DataAprovacaoPedido"))
            or self._parse_dt(row.get("CriadoEm"))
            or self._parse_dt(row.get("criado_em"))
            or datetime.utcnow()
        )
        total = 0.0
        for k in ("ValorTotal", "Total", "total", "Valor", "valor", "ValorLiquido", "valor_total", "ValorFinal"):
            if row.get(k) is not None:
                total = self._to_float(row.get(k))
                break
        return {
            "venda_id": str(venda_id),
            "data": dt,
            "valor_total": total,
            "cliente": (row.get("ClienteNome") or row.get("cliente") or row.get("NomeCliente") or "")[:240],
            "raw": row,
            "atualizado_em": datetime.utcnow(),
        }

    def upsert_vendas_agro(self, rows: list[dict]):
        col = self.vendas_agro_collection()
        inseridos = 0
        atualizados = 0
        ignorados = 0
        for row in rows or []:
            doc = self.normalizar_pedido_para_vendas_agro(row)
            if not doc:
                ignorados += 1
                continue
            r = col.update_one({"venda_id": doc["venda_id"]}, {"$set": doc}, upsert=True)
            if r.upserted_id is not None:
                inseridos += 1
            elif r.modified_count > 0:
                atualizados += 1
            else:
                ignorados += 1
        try:
            col.create_index([("venda_id", 1)], unique=True)
            col.create_index([("data", -1)])
        except Exception:
            pass
        return {"inseridos": inseridos, "atualizados": atualizados, "ignorados": ignorados}

    def obter_vendas_agro_periodo(self, dt_ini: datetime, dt_fim: datetime):
        col = self.vendas_agro_collection()
        q = {"data": {"$gte": dt_ini, "$lte": dt_fim}}
        return list(col.find(q, {"_id": 0}).sort("data", 1))