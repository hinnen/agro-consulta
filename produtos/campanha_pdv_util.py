"""
Campanha pontual no PDV (ex.: inauguração Vila Elias).

Não altera preço de cadastro/overlay — só o valor cobrado na venda do dia.
Kill switch: AGRO_CAMPANHA_INAUGURACAO_OFF=1
Teste local fora da data: AGRO_CAMPANHA_INAUGURACAO_TEST=1
"""

from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any
from zoneinfo import ZoneInfo

from django.conf import settings

TZ_LOJA = ZoneInfo("America/Sao_Paulo")

CAMPANHA_ID = "inauguracao_vila_2026_08_08"
CAMPANHA_NOME = "Inauguração Vila Elias"
CAMPANHA_DEPOSITO = "vila"
CAMPANHA_PCT_DEFAULT = Decimal("5")
CAMPANHA_DATA_DEFAULT = date(2026, 8, 8)


def _env_truthy(name: str) -> bool:
    raw = str(getattr(settings, name, None) or "").strip().lower()
    if not raw:
        import os

        raw = str(os.environ.get(name) or "").strip().lower()
    return raw in ("1", "true", "yes", "on", "sim")


def _env_str(name: str, default: str = "") -> str:
    raw = getattr(settings, name, None)
    if raw is None or str(raw).strip() == "":
        import os

        raw = os.environ.get(name)
    return str(raw or default).strip()


def _parse_date(raw: str | None, fallback: date) -> date:
    s = str(raw or "").strip()
    if not s:
        return fallback
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        return fallback


def percentual_campanha() -> Decimal:
    raw = _env_str("AGRO_CAMPANHA_INAUGURACAO_PCT", "")
    if not raw:
        return CAMPANHA_PCT_DEFAULT
    try:
        pct = Decimal(str(raw).replace(",", "."))
    except Exception:
        return CAMPANHA_PCT_DEFAULT
    if pct <= 0 or pct >= 100:
        return CAMPANHA_PCT_DEFAULT
    return pct


def datas_campanha() -> tuple[date, date]:
    ini = _parse_date(_env_str("AGRO_CAMPANHA_INAUGURACAO_INICIO", ""), CAMPANHA_DATA_DEFAULT)
    fim = _parse_date(_env_str("AGRO_CAMPANHA_INAUGURACAO_FIM", ""), ini)
    if fim < ini:
        fim = ini
    return ini, fim


def hoje_loja(agora: datetime | date | None = None) -> date:
    if isinstance(agora, date) and not isinstance(agora, datetime):
        return agora
    if isinstance(agora, datetime):
        if agora.tzinfo is None:
            agora = agora.replace(tzinfo=TZ_LOJA)
        return agora.astimezone(TZ_LOJA).date()
    return datetime.now(TZ_LOJA).date()


def campanha_desligada() -> bool:
    return _env_truthy("AGRO_CAMPANHA_INAUGURACAO_OFF")


def campanha_forcar_teste() -> bool:
    """Ativa fora da data (só para prova no PC)."""
    return _env_truthy("AGRO_CAMPANHA_INAUGURACAO_TEST")


def campanha_no_calendario(
    *,
    agora: datetime | date | None = None,
) -> dict[str, Any] | None:
    """Regra do dia (ignora loja) — p/ o PDV ligar/desligar ao trocar Centro↔Vila."""
    if campanha_desligada():
        return None
    hoje = hoje_loja(agora)
    ini, fim = datas_campanha()
    no_periodo = ini <= hoje <= fim
    if not no_periodo and not campanha_forcar_teste():
        return None
    pct = percentual_campanha()
    fator = (Decimal("1") - (pct / Decimal("100"))).quantize(Decimal("0.0001"))
    return {
        "id": CAMPANHA_ID,
        "nome": CAMPANHA_NOME,
        "deposito": CAMPANHA_DEPOSITO,
        "percentual": float(pct),
        "fator": float(fator),
        "data_inicio": ini.isoformat(),
        "data_fim": fim.isoformat(),
        "rotulo": f"{CAMPANHA_NOME}: {pct.normalize()}% off automático",
        "teste": bool(campanha_forcar_teste() and not no_periodo),
    }


def campanha_ativa_para_deposito(
    deposito: str | None,
    *,
    agora: datetime | date | None = None,
) -> dict[str, Any] | None:
    regra = campanha_no_calendario(agora=agora)
    if not regra:
        return None
    dep = str(deposito or "").strip().lower()
    if dep != CAMPANHA_DEPOSITO:
        return None
    return regra


def bootstrap_campanha(deposito: str | None) -> dict[str, Any]:
    """Pacote p/ o wizard: regra do dia + se aplica na loja atual."""
    regra = campanha_no_calendario()
    dep = str(deposito or "").strip().lower()
    if not regra:
        return {"ativa": False, "regra": None}
    aplica = dep == CAMPANHA_DEPOSITO
    return {
        "ativa": aplica,
        "regra": regra,
        "depositoAtual": dep or "centro",
    }


def _q2(v: Decimal) -> Decimal:
    return v.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _q4(v: Decimal) -> Decimal:
    return v.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


def _dec_preco(raw) -> Decimal:
    try:
        if raw is None or str(raw).strip() == "":
            return Decimal("0")
        s = str(raw).strip().replace(",", ".")
        return Decimal(s)
    except Exception:
        return Decimal("0")


def aplicar_desconto_campanha_nos_itens(
    raw_itens: list | None,
    deposito: str | None,
    *,
    agora: datetime | date | None = None,
) -> tuple[list, dict[str, Any] | None]:
    """
    Multiplica preço unitário pelo fator da campanha (ex. 0,95).
    Itens já devem vir com preço pós-promo / pós-forma, SEM o desconto da campanha.
    """
    camp = campanha_ativa_para_deposito(deposito, agora=agora)
    if not camp or not isinstance(raw_itens, list):
        return list(raw_itens or []), camp

    fator = Decimal(str(camp["fator"]))
    out: list = []
    for row in raw_itens:
        if not isinstance(row, dict):
            continue
        item = deepcopy(row)
        vu = _dec_preco(item.get("preco"))
        if vu > 0:
            item["preco"] = float(_q4(vu * fator))
        item["campanha_id"] = camp["id"]
        item["campanha_pct"] = camp["percentual"]
        out.append(item)
    return out, camp


def anexar_campanha_no_payload(data: dict, deposito: str | None) -> dict:
    """Garante flag no payload da venda quando a campanha está ativa."""
    camp = campanha_ativa_para_deposito(deposito)
    if not camp or not isinstance(data, dict):
        return data
    data = dict(data)
    data["campanha_id"] = camp["id"]
    data["campanha_pct"] = camp["percentual"]
    return data
