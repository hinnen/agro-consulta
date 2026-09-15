"""Layout quente/frio da topbar do PDV (Postgres · todos os PCs)."""
from __future__ import annotations

from typing import Any

from produtos.models import PdvTopbarLayoutAgro

CHAVE_DEFAULT = "default"

# Ordem padrão após Fiado no quente (PDV-TOPBAR-LAYOUT).
QUENTE_DEFAULT: list[str] = [
    "pedir_loja",
    "vendas",
    "uso_loja",
    "entregas",
    "caixa",
    "fiado",
    "nova_venda",
]
FRIO_DEFAULT: list[str] = [
    "saldo_vila",
    "repasse",
    "pesar",
    "pin",
]

MOVABLE_KEYS: frozenset[str] = frozenset(QUENTE_DEFAULT + FRIO_DEFAULT)

LABELS: dict[str, str] = {
    "pedir_loja": "Pedir loja",
    "vendas": "Vendas",
    "uso_loja": "Uso loja",
    "entregas": "Entregas",
    "caixa": "Caixa/loja",
    "fiado": "Fiado",
    "nova_venda": "Nova venda",
    "saldo_vila": "Saldo Vila",
    "repasse": "Repasse",
    "pesar": "Pesar",
    "pin": "PIN",
}


def _limpar_lista(raw: Any) -> list[str]:
    if not isinstance(raw, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for item in raw:
        key = str(item or "").strip().lower().replace("-", "_")[:40]
        if key not in MOVABLE_KEYS or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def normalizar_layout(*, quente: Any = None, frio: Any = None) -> dict[str, list[str]]:
    """Garante listas válidas, sem duplicata; completa com o que faltou no frio."""
    q = _limpar_lista(quente)
    f = _limpar_lista(frio)
    q_set = set(q)
    f = [k for k in f if k not in q_set]
    usados = set(q) | set(f)
    for k in QUENTE_DEFAULT + FRIO_DEFAULT:
        if k not in usados:
            f.append(k)
            usados.add(k)
    if not q:
        q = list(QUENTE_DEFAULT)
        f = [k for k in FRIO_DEFAULT if k not in set(q)]
    return {"quente": q, "frio": f}


def layout_default() -> dict[str, list[str]]:
    return {"quente": list(QUENTE_DEFAULT), "frio": list(FRIO_DEFAULT)}


def obter_layout() -> dict[str, list[str]]:
    row = PdvTopbarLayoutAgro.objects.filter(chave=CHAVE_DEFAULT).first()
    if not row:
        return layout_default()
    return normalizar_layout(quente=row.quente, frio=row.frio)


def salvar_layout(*, quente: Any, frio: Any, usuario=None) -> dict[str, list[str]]:
    layout = normalizar_layout(quente=quente, frio=frio)
    row, _ = PdvTopbarLayoutAgro.objects.get_or_create(
        chave=CHAVE_DEFAULT,
        defaults={"quente": layout["quente"], "frio": layout["frio"]},
    )
    row.quente = layout["quente"]
    row.frio = layout["frio"]
    if usuario is not None and getattr(usuario, "is_authenticated", False):
        row.atualizado_por = usuario
    row.save(update_fields=["quente", "frio", "atualizado_por", "atualizado_em"])
    return layout


def payload_api() -> dict[str, Any]:
    layout = obter_layout()
    return {
        "ok": True,
        "quente": layout["quente"],
        "frio": layout["frio"],
        "labels": LABELS,
        "default": layout_default(),
    }
