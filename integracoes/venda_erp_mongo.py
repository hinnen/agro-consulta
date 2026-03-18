import re
import unicodedata
from datetime import datetime, timedelta
from difflib import SequenceMatcher

from pymongo import MongoClient
from django.conf import settings
from django.core.cache import cache

from integracoes.texto import tokens


_mongo_client = None


def get_mongo_client():
    global _mongo_client
    if _mongo_client is None:
        _mongo_client = MongoClient(settings.VENDA_ERP_MONGO_URL)
    return _mongo_client


class VendaERPMongoClient:

    DIAS_RANKING_VENDAS = 180

    def __init__(self):
        self.client = get_mongo_client()
        self.db = self.client[settings.VENDA_ERP_MONGO_DB]

    # =========================
    # NORMALIZAÇÃO
    # =========================

    def _normalizar(self, texto):
        texto = str(texto or "").lower()
        texto = unicodedata.normalize("NFKD", texto)
        texto = "".join(c for c in texto if not unicodedata.combining(c))
        texto = re.sub(r"[^a-z0-9\s]", " ", texto)
        return re.sub(r"\s+", " ", texto).strip()

    def _tokens(self, termo):
        base = tokens(termo)
        if base:
            return base
        return self._normalizar(termo).split()

    # =========================
    # GRANEL (CORRIGIDO)
    # =========================

    def _eh_granel(self, produto):
        nome = self._normalizar(produto.get("Nome"))
        categoria = self._normalizar(produto.get("Categoria"))
        sub = self._normalizar(
            produto.get("SubCategoria") or produto.get("Subcategoria")
        )

        texto = f"{nome} {categoria} {sub}"

        if "granel" in texto:
            return True

        if any(x in texto for x in ["1kg", "1 kg"]):
            if any(t in texto for t in ["milho", "racao", "ração", "farelo"]):
                return True

        return False

    # =========================
    # BUSCA
    # =========================

    def _buscar(self, termo):
        tks = self._tokens(termo)

        if not tks:
            return []

        filtro = {
            "$and": [
                {"CadastroInativo": False},
                {
                    "$and": [
                        {
                            "$or": [
                                {"BuscaTexto": {"$regex": t, "$options": "i"}},
                                {"Nome": {"$regex": t, "$options": "i"}},
                            ]
                        }
                        for t in tks
                    ]
                },
            ]
        }

        return list(self.db["DtoProduto"].find(filtro).limit(200))

    # =========================
    # SCORE (RELEVÂNCIA REAL)
    # =========================

    def _score(self, produto, termo):
        nome = self._normalizar(produto.get("Nome"))
        termo = self._normalizar(termo)

        score = 0

        if termo in nome:
            score += 1000

        if nome.startswith(termo):
            score += 500

        for tk in self._tokens(termo):
            if tk in nome:
                score += 200
            else:
                for palavra in nome.split():
                    if SequenceMatcher(None, tk, palavra).ratio() > 0.8:
                        score += 100

        return score

    # =========================
    # RANKING VENDAS
    # =========================

    def _ranking_vendas(self, ids):
        if not ids:
            return {}

        data_inicio = datetime.utcnow() - timedelta(days=self.DIAS_RANKING_VENDAS)

        pipeline = [
            {
                "$match": {
                    "Movimentacao": "Venda",
                    "Produto_ID": {"$in": ids},
                    "Data": {"$gte": data_inicio},
                }
            },
            {
                "$group": {
                    "_id": "$Produto_ID",
                    "qtd": {"$sum": "$Quantidade"},
                }
            },
        ]

        dados = list(self.db["ReportViewDtoEstoqueSaida"].aggregate(pipeline))

        return {str(d["_id"]): d["qtd"] for d in dados}

    # =========================
    # ORDENAÇÃO FINAL
    # =========================

    def _ordenar(self, produtos, termo):
        ids = [str(p["_id"]) for p in produtos if p.get("_id")]
        ranking = self._ranking_vendas(ids)

        def chave(p):
            pid = str(p.get("_id"))
            vendidos = ranking.get(pid, 0)

            return (
                self._eh_granel(p),        # GRANEL SEMPRE POR ÚLTIMO
                -self._score(p, termo),    # RELEVÂNCIA PRIMEIRO
                -vendidos,                # MAIS VENDIDOS
                self._normalizar(p.get("Nome")),
            )

        return sorted(produtos, key=chave)

    # =========================
    # PÚBLICO
    # =========================

    def buscar_produtos(self, termo):
        if not termo:
            return []

        produtos = self._buscar(termo)

        produtos = self._ordenar(produtos, termo)

        return produtos[:50]

    # =========================
    # ESTOQUE (CORRIGIDO)
    # =========================

    def buscar_estoques_por_produto_ids(self, produto_ids):
        if not produto_ids:
            return []

        return list(
            self.db["DtoEstoqueDepositoProduto"].find(
                {"ProdutoID": {"$in": [str(pid) for pid in produto_ids]}},
                {
                    "ProdutoID": 1,
                    "Deposito": 1,
                    "Saldo": 1,
                },
            )
        )