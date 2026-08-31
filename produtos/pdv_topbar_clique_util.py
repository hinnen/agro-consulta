"""Contagem de cliques da topbar do PDV (base quente/frio)."""
from __future__ import annotations

from datetime import timedelta

from django.db.models import F, Sum
from django.utils import timezone

from produtos.models import PdvTopbarCliqueDiaAgro

BOTAO_KEYS = frozenset(
    {
        "mais",
        "saldo_vila",
        "pedir_loja",
        "pesar",
        "caixa",
        "vendas",
        "fiado",
        "uso_loja",
        "repasse",
        "entregas",
        "nova_venda",
        "pin",
    }
)


def normalizar_botao(raw: str | None) -> str:
    key = str(raw or "").strip().lower().replace("-", "_")[:40]
    return key if key in BOTAO_KEYS else ""


def normalizar_deposito(raw: str | None) -> str:
    d = str(raw or "").strip().lower()[:16]
    if d in ("centro", "vila", "vila_elias"):
        return "vila" if d.startswith("vila") else d
    return d or ""


def registrar_clique(*, botao: str, deposito: str = "") -> tuple[bool, str]:
    key = normalizar_botao(botao)
    if not key:
        return False, "Botão inválido."
    dep = normalizar_deposito(deposito)
    hoje = timezone.localdate()
    row, created = PdvTopbarCliqueDiaAgro.objects.get_or_create(
        botao=key,
        deposito=dep,
        data=hoje,
        defaults={"cliques": 1},
    )
    if not created:
        PdvTopbarCliqueDiaAgro.objects.filter(pk=row.pk).update(cliques=F("cliques") + 1)
    return True, ""


def resumo_cliques(*, dias: int = 14) -> list[dict]:
    dias = max(1, min(int(dias or 14), 90))
    inicio = timezone.localdate() - timedelta(days=dias - 1)
    qs = (
        PdvTopbarCliqueDiaAgro.objects.filter(data__gte=inicio)
        .values("botao")
        .annotate(total=Sum("cliques"))
        .order_by("-total", "botao")
    )
    return [{"botao": r["botao"], "total": int(r["total"] or 0)} for r in qs]
