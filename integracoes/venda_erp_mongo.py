import re
import unicodedata
from datetime import datetime, timedelta
from difflib import SequenceMatcher

from pymongo import MongoClient
from django.conf import settings

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
        texto = texto.replace("ç", "c")
        texto = re.sub(r"[^a-z0-9\s]", " ", texto)
        return re.sub(r"\s+", " ", texto).strip()

    def _tokens(self, termo):
        base = tokens(termo)
        if base:
            return base
        return [t for t in self._normalizar(termo).split() if t]

    # =========================
    # GRANEL
    # =========================

    def _eh_granel(self, produto):
        texto = self._normalizar(
            f"{produto.get('Nome')} {produto.get('Categoria')} {produto.get('SubCategoria')}"
        )

        if "granel" in texto:
            return True

        if any(x in texto for x in ["1kg", "1 kg"]):
            if any(t in texto for t in ["milho", "racao", "ração", "farelo"]):
                return True

        return False

    # =========================
    # PESO
    # =========================

    def _peso_produto(self, produto):
        nome = self._normalizar(produto.get("Nome"))
        match = re.search(r"(\\d+)(kg|g)", nome)

        if not match:
            return 0

        valor = int(match.group(1))
        unidade = match.group(2)

        return valor * 1000 if unidade == "kg" else valor

    def _peso_alvo(self, termo):
        termo = self._normalizar(termo)

        if "turtle" in termo:
            return 300

        if "racao" in termo or "ração" in termo:
            return 15000

        return None

    # =========================
    # BUSCA (COM MONGO INDEX)
    # =========================

    def _buscar(self, termo):
        return list(
            self.db["DtoProduto"].find(
                {
                    "$text": {"$search": termo},
                    "CadastroInativo": False
                },
                {
                    "score": {"$meta": "textScore"},
                    "Nome": 1,
                    "Categoria": 1,
                    "SubCategoria": 1,
                    "PrecoVenda": 1,
                }
            ).sort([("score", {"$meta": "textScore"})]).limit(200)
        )

    # =========================
    # SCORE
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
    # VENDAS
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
        peso_alvo = self._peso_alvo(termo)

        def chave(p):
            pid = str(p.get("_id"))
            vendidos = ranking.get(pid, 0)
            peso = self._peso_produto(p)

            distancia_peso = abs(peso - peso_alvo) if peso_alvo and peso else 999999

            return (
                self._eh_granel(p),
                -self._score(p, termo),
                distancia_peso,
                -vendidos,
                self._normalizar(p.get("Nome")),
            )

        return sorted(produtos, key=chave)

    # =========================
    # PUBLICO
    # =========================

    def buscar_produtos(self, termo):
        if not termo:
            return []

        produtos = self._buscar(termo)
        produtos = self._ordenar(produtos, termo)

        return produtos[:50]

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