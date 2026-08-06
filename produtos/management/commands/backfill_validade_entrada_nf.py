"""
Recupera lote/validade das Entradas NF antigas para a tela Validade (EstoqueLote).

Cuidado (padrão):
  - Só dry-run até passar ``--aplicar``
  - Só notas com estoque já lançado
  - Só linhas com data de validade + produto do catálogo + qtd > 0
  - NÃO altera lote que já existe (mesmo código)
  - NÃO soma quantidade em lote existente
  - Reexecução: pula rascunhos já marcados (extra.validade_backfill_em)
  - Código de lote vazio vira marcador estável BF-NF-<id>-L<n> (idempotente)

Uso:
  python manage.py backfill_validade_entrada_nf
  python manage.py backfill_validade_entrada_nf --limit 50
  python manage.py backfill_validade_entrada_nf --aplicar
  python manage.py backfill_validade_entrada_nf --nf=77846 --aplicar
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone


def _produto_id_ok(pid: str) -> bool:
    """Aceita ObjectId hex (24) ou id Agro (AGRO…); evita lixo tipo «PRODUTO ALTERADO…»."""
    s = str(pid or "").strip()
    if not s or s.startswith("local:"):
        return False
    if s.startswith('"') or " " in s:
        return False
    if len(s) == 24 and all(c in "0123456789abcdefABCDEF" for c in s):
        return True
    if s.upper().startswith("AGRO") and len(s) >= 8:
        return True
    # Outros ids curtos/numéricos do catálogo legado
    if 6 <= len(s) <= 64 and all(c.isalnum() or c in "-_" for c in s):
        return True
    return False


def _estoque_ja_lancado(row) -> bool:
    if getattr(row, "estoque_aplicado_em", None):
        return True
    st = str(getattr(row, "status", "") or "").strip().lower()
    if st == "estoque_aplicado":
        return True
    ex = row.extra if isinstance(getattr(row, "extra", None), dict) else {}
    if str(ex.get("estoque_agro_registrado_em") or "").strip():
        return True
    ids = ex.get("estoque_agro_ajuste_ids")
    if isinstance(ids, list) and any(x is not None for x in ids):
        return True
    return False


def _qtd_linha(ln: dict) -> Decimal:
    raw = ln.get("q_estoque", ln.get("q_com", 0))
    try:
        q = Decimal(str(raw).replace(",", ".").strip() or "0")
    except Exception:
        return Decimal("0")
    return q if q > 0 else Decimal("0")


def _lote_codigo_backfill(ln: dict, *, rid: str, idx: int, nf_numero: str) -> str:
    real = str(ln.get("lote_numero") or "").strip()[:100]
    if real:
        return real
    base = (nf_numero or "").strip() or rid[:8]
    return f"BF-NF-{base}-L{idx}"[:100]


def _nome_linha(ln: dict) -> str:
    return str(ln.get("x_prod") or ln.get("nome") or "").strip()[:255]


class Command(BaseCommand):
    help = (
        "Backfill cuidadoso: validade da Entrada NF → EstoqueLote. "
        "Padrão = dry-run; use --aplicar para gravar."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--aplicar",
            action="store_true",
            help="Grava de verdade. Sem isto = só simula (dry-run).",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=0,
            help="Máximo de rascunhos a varrer (0 = todos).",
        )
        parser.add_argument(
            "--nf",
            type=str,
            default="",
            help="Só a NF com este número (cabecalho.numero).",
        )
        parser.add_argument(
            "--id",
            type=str,
            default="",
            help="Só o rascunho com este rascunho_id.",
        )
        parser.add_argument(
            "--incluir-ja-marcados",
            action="store_true",
            help="Não pula rascunhos com extra.validade_backfill_em (reprocessa; ainda não sobrescreve lote existente).",
        )
        parser.add_argument(
            "--mostrar",
            type=int,
            default=40,
            help="Quantas linhas de detalhe imprimir (padrão 40).",
        )

    def handle(self, *args, **options):
        from produtos.models import (
            EntradaNotaRascunhoAgro,
            EstoqueLote,
            ProdutoGestaoOverlayAgro,
            parse_data_validade_entrada_nf,
        )

        aplicar = bool(options.get("aplicar"))
        lim = int(options.get("limit") or 0)
        nf_filtro = str(options.get("nf") or "").strip()
        rid_filtro = str(options.get("id") or "").strip()
        incluir_marcados = bool(options.get("incluir_ja_marcados"))
        mostrar = max(0, int(options.get("mostrar") or 40))

        qs = EntradaNotaRascunhoAgro.objects.exclude(status="descartada").order_by(
            "criado_em", "rascunho_id"
        )
        if rid_filtro:
            qs = qs.filter(rascunho_id=rid_filtro[:24])
        if lim > 0:
            qs = qs[:lim]

        cont = {
            "docs": 0,
            "docs_estoque": 0,
            "docs_marcados_skip": 0,
            "linhas_candidatas": 0,
            "criar": 0,
            "skip_existe": 0,
            "skip_sem_data": 0,
            "skip_sem_produto": 0,
            "skip_qtd": 0,
            "erros": 0,
            "docs_marcados_ok": 0,
        }
        detalhes: list[str] = []
        conflitos: list[str] = []

        modo = "APLICAR" if aplicar else "DRY-RUN"
        self.stdout.write(self.style.WARNING(f"=== backfill_validade_entrada_nf · {modo} ==="))

        for row in qs.iterator(chunk_size=40):
            cont["docs"] += 1
            if nf_filtro:
                cab0 = row.cabecalho if isinstance(row.cabecalho, dict) else {}
                if str(cab0.get("numero") or "").strip() != nf_filtro:
                    continue
            if not _estoque_ja_lancado(row):
                continue
            cont["docs_estoque"] += 1

            ex = dict(row.extra) if isinstance(row.extra, dict) else {}
            if (not incluir_marcados) and str(ex.get("validade_backfill_em") or "").strip():
                cont["docs_marcados_skip"] += 1
                continue

            cab = row.cabecalho if isinstance(row.cabecalho, dict) else {}
            nf_num = str(cab.get("numero") or "").strip()
            rid = str(row.rascunho_id)
            linhas = row.linhas if isinstance(row.linhas, list) else []
            criados_neste = 0

            for idx, ln in enumerate(linhas, start=1):
                if not isinstance(ln, dict):
                    continue
                pid = str(ln.get("produto_id") or "").strip()
                if not _produto_id_ok(pid):
                    cont["skip_sem_produto"] += 1
                    if str(ln.get("lote_validade") or "").strip():
                        conflitos.append(
                            f"SKIP pid inválido · NF {nf_num or '?'} L{idx} · pid={pid[:40]!r}"
                        )
                    continue
                dv = parse_data_validade_entrada_nf(ln.get("lote_validade"))
                if dv is None:
                    if str(ln.get("lote_validade") or "").strip():
                        cont["skip_sem_data"] += 1
                    continue
                qtd = _qtd_linha(ln)
                if qtd <= 0:
                    cont["skip_qtd"] += 1
                    continue

                cont["linhas_candidatas"] += 1
                lote_cod = _lote_codigo_backfill(ln, rid=rid, idx=idx, nf_numero=nf_num)
                nome = _nome_linha(ln)

                ov = ProdutoGestaoOverlayAgro.objects.filter(
                    produto_externo_id=pid[:64]
                ).first()
                el = None
                if ov is not None:
                    el = EstoqueLote.objects.filter(overlay=ov, lote_codigo=lote_cod).first()

                if el is not None:
                    cont["skip_existe"] += 1
                    msg = (
                        f"SKIP existe · NF {nf_num or '?'} L{idx} · pid={pid[:20]} · "
                        f"lote={lote_cod} · val={dv.isoformat()} · qtd_nf={qtd} · "
                        f"qtd_lote={el.quantidade_atual}"
                    )
                    if el.data_validade != dv:
                        conflitos.append(
                            msg + f" · DATA LOTE DIFERE ({el.data_validade.isoformat()})"
                        )
                    elif len(detalhes) < mostrar:
                        detalhes.append(msg)
                    continue

                msg = (
                    f"CRIAR · NF {nf_num or '?'} L{idx} · pid={pid[:20]} · "
                    f"{(nome or '')[:40]} · lote={lote_cod} · val={dv.isoformat()} · qtd={qtd}"
                )
                if len(detalhes) < mostrar:
                    detalhes.append(msg)

                if not aplicar:
                    cont["criar"] += 1
                    criados_neste += 1
                    continue

                try:
                    with transaction.atomic():
                        ov2, _ = ProdutoGestaoOverlayAgro.objects.get_or_create(
                            produto_externo_id=pid[:64],
                            defaults={"nome": nome},
                        )
                        if nome and not (ov2.nome or "").strip():
                            ov2.nome = nome
                            ov2.save(update_fields=["nome", "atualizado_em"])
                        # Dupla checagem dentro da transação
                        if EstoqueLote.objects.filter(
                            overlay=ov2, lote_codigo=lote_cod
                        ).exists():
                            cont["skip_existe"] += 1
                            continue
                        EstoqueLote.objects.create(
                            overlay=ov2,
                            lote_codigo=lote_cod,
                            data_validade=dv,
                            quantidade_atual=qtd.quantize(Decimal("0.01")),
                        )
                    cont["criar"] += 1
                    criados_neste += 1
                except Exception as exc:
                    cont["erros"] += 1
                    conflitos.append(f"ERRO · NF {nf_num} L{idx} pid={pid[:20]} · {exc}")

            if aplicar and criados_neste > 0:
                try:
                    ex["validade_backfill_em"] = timezone.now().isoformat()
                    ex["validade_backfill_n"] = int(criados_neste)
                    row.extra = ex
                    row.save(update_fields=["extra", "atualizado_em"])
                    cont["docs_marcados_ok"] += 1
                except Exception as exc:
                    conflitos.append(f"ERRO marcar rascunho {rid[:12]} · {exc}")

        for line in detalhes:
            self.stdout.write(line)
        if len(detalhes) >= mostrar and cont["criar"] + cont["skip_existe"] > mostrar:
            self.stdout.write(f"… (mais linhas; use --mostrar N)")

        if conflitos:
            self.stdout.write(self.style.WARNING("--- alertas / conflitos ---"))
            for c in conflitos[:60]:
                self.stdout.write(self.style.WARNING(c))

        self.stdout.write("")
        self.stdout.write(
            f"Rascunhos lidos: {cont['docs']} · com estoque: {cont['docs_estoque']} · "
            f"já backfill (skip): {cont['docs_marcados_skip']}"
        )
        self.stdout.write(
            f"Linhas com validade: {cont['linhas_candidatas']} · "
            f"criar: {cont['criar']} · já existia: {cont['skip_existe']} · "
            f"erros: {cont['erros']}"
        )
        if not aplicar:
            self.stdout.write(
                self.style.NOTICE(
                    "Dry-run OK. Se a lista bater, rode de novo com --aplicar "
                    "(não reabre nota; não mexe em lote que já existe)."
                )
            )
        else:
            self.stdout.write(
                self.style.SUCCESS(
                    f"Gravado: {cont['criar']} lote(s) · "
                    f"{cont['docs_marcados_ok']} nota(s) marcada(s)."
                )
            )
            try:
                from produtos.views import _invalidar_cache_dashboard_perdas_validade

                _invalidar_cache_dashboard_perdas_validade()
            except Exception:
                pass
