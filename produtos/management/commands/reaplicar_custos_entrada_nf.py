"""
Reaplica custo da Entrada NF (após PIN) no Cadastro SisVale — overlay + Produto.

Uma nota (dry-run):
  python manage.py reaplicar_custos_entrada_nf --nf=77846

Uma nota (gravar):
  python manage.py reaplicar_custos_entrada_nf --nf=77846 --aplicar

Todas as notas aprovadas (da mais antiga à mais recente — a última NF manda no custo):
  python manage.py reaplicar_custos_entrada_nf --todas
  python manage.py reaplicar_custos_entrada_nf --todas --aplicar

Opcional: só a partir de uma data (aprovação PIN):
  python manage.py reaplicar_custos_entrada_nf --todas --desde=2026-01-01 --aplicar

Ou por id do rascunho:
  python manage.py reaplicar_custos_entrada_nf --id=<objectid> --aplicar
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from django.core.management.base import BaseCommand


def _aprovacao_em(doc: dict[str, Any]) -> str:
    extra = doc.get("extra") if isinstance(doc.get("extra"), dict) else {}
    return str(extra.get("aprovacao_wizard_em") or "").strip()


def _parse_desde(raw: str) -> datetime | None:
    s = (raw or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y"):
        try:
            dt = datetime.strptime(s[:19], fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None


def _aprovacao_dt(doc: dict[str, Any]) -> datetime:
    raw = _aprovacao_em(doc)
    if not raw:
        return datetime.min.replace(tzinfo=timezone.utc)
    try:
        # ISO com ou sem Z
        s = raw.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)


def _nf_label(doc: dict[str, Any]) -> str:
    cab = doc.get("cabecalho") if isinstance(doc.get("cabecalho"), dict) else {}
    return str(cab.get("nNF") or cab.get("numero") or cab.get("numero_nf") or "?")


class Command(BaseCommand):
    help = "Reaplica custos da Entrada NF aprovada no catálogo SisVale (sem mexer em estoque)."

    def add_arguments(self, parser):
        parser.add_argument("--nf", type=str, default="", help="Número da NF (ex.: 77846).")
        parser.add_argument("--id", type=str, default="", help="ID do rascunho aprovado.")
        parser.add_argument(
            "--todas",
            action="store_true",
            help="Todas as NFs com PIN (aprovadas). Ordem cronológica: última manda no custo.",
        )
        parser.add_argument(
            "--desde",
            type=str,
            default="",
            help="Com --todas: só notas aprovadas a partir desta data (AAAA-MM-DD).",
        )
        parser.add_argument(
            "--limite",
            type=int,
            default=0,
            help="Com --todas: processa no máximo N notas (0 = sem teto).",
        )
        parser.add_argument(
            "--aplicar",
            action="store_true",
            help="Grava custo. Sem isto, só mostra o que faria.",
        )
        parser.add_argument(
            "--verboso",
            action="store_true",
            help="Lista linhas de produto (em --todas fica quieto por padrão).",
        )

    def handle(self, *args, **options):
        from produtos.nfe_entrada_util import _entrada_nota_rascunho_store, _object_id_rascunho
        from produtos.views import (
            _entrada_nfe_aplicar_custos_catalogo_pos_aprovacao,
            _entrada_nfe_custo_prev_sisvale,
            _entrada_nfe_decimal_v_un,
            obter_conexao_mongo,
        )

        nf = str(options.get("nf") or "").strip()
        rid = str(options.get("id") or "").strip()
        todas = bool(options.get("todas"))
        aplicar = bool(options.get("aplicar"))
        verboso = bool(options.get("verboso"))
        limite = int(options.get("limite") or 0)
        desde = _parse_desde(str(options.get("desde") or ""))

        if not nf and not rid and not todas:
            self.stderr.write(
                "Informe --nf=77846, --id=<rascunho> ou --todas (lote)."
            )
            return
        if desde and not todas:
            self.stderr.write("--desde só vale com --todas.")
            return

        _, db = obter_conexao_mongo()
        col = _entrada_nota_rascunho_store(db)
        if col is None:
            self.stderr.write("Armazenamento de rascunho indisponível.")
            return

        docs: list[dict[str, Any]] = []
        if todas:
            docs = self._listar_aprovadas(col, desde=desde, limite=limite)
            if not docs:
                self.stderr.write("Nenhuma NF aprovada (PIN) encontrada com esses filtros.")
                return
            self.stdout.write(
                f"Lote: {len(docs)} nota(s) aprovada(s), ordem antiga→nova "
                f"(última NF de cada produto fica no custo)."
            )
        elif rid:
            oid = _object_id_rascunho(rid)
            if oid is None:
                self.stderr.write("ID inválido.")
                return
            doc = col.find_one({"_id": oid})
            if not doc:
                self.stderr.write("Rascunho não encontrado.")
                return
            docs = [doc]
        else:
            doc = self._buscar_por_nf(col, nf)
            if not doc:
                self.stderr.write(f"Nenhuma NF {nf} aprovada (PIN) encontrada.")
                return
            docs = [doc]

        tot_sv = 0
        tot_mongo = 0
        tot_err = 0
        processadas = 0

        for doc in docs:
            oid = str(doc.get("_id") or "")
            cab = doc.get("cabecalho") if isinstance(doc.get("cabecalho"), dict) else {}
            linhas = doc.get("linhas") if isinstance(doc.get("linhas"), list) else []
            extra = doc.get("extra") if isinstance(doc.get("extra"), dict) else {}
            n_label = _nf_label(doc)
            aprov = _aprovacao_em(doc)[:19]
            self.stdout.write(
                f"[{processadas + 1}/{len(docs)}] NF {n_label} id={oid[:12]}… "
                f"linhas={len(linhas)} aprovada={aprov}"
            )

            if verboso or (not todas and len(docs) == 1):
                for ln in linhas:
                    if not isinstance(ln, dict):
                        continue
                    pid = str(ln.get("produto_id") or "").strip()
                    if not pid or pid.lower().startswith("local:"):
                        continue
                    v_nf = _entrada_nfe_decimal_v_un(ln)
                    atual = _entrada_nfe_custo_prev_sisvale(pid)
                    nome = str(ln.get("x_prod") or "")[:40]
                    self.stdout.write(
                        f"  {pid[:12]} {nome:40} custo_atual={atual} v_un_nf={v_nf}"
                    )

            if not aplicar:
                processadas += 1
                continue

            client, db2 = obter_conexao_mongo()
            out = _entrada_nfe_aplicar_custos_catalogo_pos_aprovacao(
                db=db2,
                client_m=client,
                linhas=linhas,
                cab=cab,
                extra=extra,
                user_pk=None,
                excluir_rascunho_id=oid,
            )
            sv = int(out.get("atualizados_sisvale") or 0)
            mg = int(out.get("atualizados_mongo") or 0)
            errs = out.get("erros") or []
            tot_sv += sv
            tot_mongo += mg
            tot_err += len(errs)
            processadas += 1
            self.stdout.write(f"  → sisvale={sv} mongo={mg} erros={len(errs)}")
            for err in errs[:5]:
                self.stderr.write(f"  ! {err}")

        if not aplicar:
            self.stdout.write(
                f"Dry-run: {processadas} nota(s). Para gravar: acrescente --aplicar"
            )
            return

        self.stdout.write(
            f"OK lote: notas={processadas} sisvale={tot_sv} mongo={tot_mongo} erros={tot_err}"
        )

    def _listar_aprovadas(self, col, *, desde: datetime | None, limite: int) -> list[dict]:
        """Notas com PIN, ordem cronológica (antiga → nova)."""
        docs: list[dict] = []

        # Loja: rascunhos no Postgres — varre tudo (o find “estilo Mongo” corta o scan).
        try:
            from produtos.agro_fonte_config import agro_entrada_nota_rascunho_postgres
            from produtos.entrada_nota_rascunho_pg_util import row_to_doc
            from produtos.models import EntradaNotaRascunhoAgro

            use_pg = agro_entrada_nota_rascunho_postgres()
        except Exception:
            use_pg = False

        if use_pg:
            for row in EntradaNotaRascunhoAgro.objects.all().iterator(chunk_size=200):
                doc = row_to_doc(row)
                if not isinstance(doc, dict) or not _aprovacao_em(doc):
                    continue
                if desde is not None and _aprovacao_dt(doc) < desde:
                    continue
                docs.append(doc)
        else:
            filt: dict[str, Any] = {
                "extra.aprovacao_wizard_em": {"$exists": True, "$nin": [None, ""]},
            }
            cur = col.find(filt).sort("atualizado_em", 1).limit(20_000)
            for d in cur:
                if not isinstance(d, dict) or not _aprovacao_em(d):
                    continue
                if desde is not None and _aprovacao_dt(d) < desde:
                    continue
                docs.append(d)

        docs.sort(key=_aprovacao_dt)
        if limite and limite > 0:
            docs = docs[:limite]
        return docs

    def _buscar_por_nf(self, col, nf: str) -> dict | None:
        candidatos = list(
            col.find(
                {
                    "extra.aprovacao_wizard_em": {"$exists": True, "$nin": [None, ""]},
                    "$or": [
                        {"cabecalho.nNF": nf},
                        {"cabecalho.numero": nf},
                        {"cabecalho.numero_nf": nf},
                        {"cabecalho.nnf": nf},
                    ],
                }
            )
            .sort("atualizado_em", -1)
            .limit(5)
        )
        if not candidatos:
            try:
                nfi = int(nf)
            except ValueError:
                nfi = None
            q_or = [
                {"cabecalho.nNF": nf},
                {"cabecalho.numero": nf},
            ]
            if nfi is not None:
                q_or.extend(
                    [
                        {"cabecalho.nNF": nfi},
                        {"cabecalho.numero": nfi},
                        {"cabecalho.numero_nf": nfi},
                    ]
                )
            candidatos = list(
                col.find(
                    {
                        "extra.aprovacao_wizard_em": {"$exists": True, "$nin": [None, ""]},
                        "$or": q_or,
                    }
                )
                .sort("atualizado_em", -1)
                .limit(5)
            )
        if not candidatos:
            return None
        if len(candidatos) > 1:
            self.stdout.write(
                f"Achei {len(candidatos)} — usando a mais recente "
                f"id={candidatos[0].get('_id')}"
            )
        return candidatos[0]
