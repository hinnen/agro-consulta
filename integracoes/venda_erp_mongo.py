from pymongo import MongoClient
from decouple import config


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