"""Histórico de vendas diárias importado (planilha) + merge com VendaAgro para meta C do BI."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path

from django.core.cache import cache
from django.db import transaction
from django.utils import timezone

from produtos.models import DashboardVendaDiaHistoricoAgro


def dashboard_vendas_historico_planilha_por_dia(
    data_ini: date, data_fim: date
) -> dict[str, float]:
    out: dict[str, float] = {}
    for row in DashboardVendaDiaHistoricoAgro.objects.filter(
        data__gte=data_ini,
        data__lte=data_fim,
    ).values("data", "total"):
        d = row.get("data")
        if not d:
            continue
        try:
            out[d.isoformat()] = round(float(row.get("total") or 0), 2)
        except (TypeError, ValueError):
            continue
    return out


def dashboard_vendas_serie_meta_merged(data_ini: date, data_fim: date) -> dict:
    """
    Série diária merge planilha + PDV (PDV sobrescreve o dia).
    Usada na meta C e no gráfico de barras do BI (meses set/25–mai/26).
    """
    ck = f"dash:mvs:v5:meta:{data_ini.isoformat()}:{data_fim.isoformat()}"
    cached = cache.get(ck)
    if isinstance(cached, dict) and cached.get("_t") == "mvs":
        return {k: v for k, v in cached.items() if k != "_t"}

    from produtos.views import _dashboard_vendas_serie_pdv

    plan = dashboard_vendas_historico_planilha_por_dia(data_ini, data_fim)
    pdv = _dashboard_vendas_serie_pdv(data_ini, data_fim)
    por_dia = dict(plan)
    por_dia.update(pdv.get("por_dia") or {})

    qtd_por_dia = dict(pdv.get("qtd_por_dia") or {})
    total = round(sum(float(v or 0) for v in por_dia.values()), 2)

    pdv_lojas = pdv.get("vendas_por_loja") or []
    pdv_vila = 0.0
    for row in pdv_lojas:
        if "Vila" in str(row.get("loja") or ""):
            pdv_vila = round(float(row.get("total") or 0), 2)
            break
    centro_total = round(max(total - pdv_vila, 0.0), 2)

    out = {
        "ok": True,
        "erro": "",
        "total": total,
        "por_dia": por_dia,
        "qtd_por_dia": qtd_por_dia,
        "vendas_por_loja": [
            {"loja": "Centro", "total": centro_total, "color": "#00BFFF"},
            {"loja": "Vila Elias", "total": pdv_vila, "color": "#64748b"},
        ],
        "fonte": "pdv+planilha",
    }
    cache.set(ck, {**out, "_t": "mvs"}, timeout=120)
    return out


def dashboard_invalidar_cache_meta_merged(
    data_ini: date | None = None, data_fim: date | None = None
) -> None:
    """Limpa cache v5 da meta C (intervalo ou últimos ~18 meses civis)."""
    if data_ini and data_fim:
        cache.delete(f"dash:mvs:v5:meta:{data_ini.isoformat()}:{data_fim.isoformat()}")
        return
    hoje = timezone.localdate()
    cur = hoje.replace(day=1)
    for _ in range(18):
        fp = cur
        lp = (cur.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        cache.delete(f"dash:mvs:v5:meta:{fp.isoformat()}:{lp.isoformat()}")
        cur = fp - timedelta(days=1)
        cur = cur.replace(day=1)


def _parse_valor_celula(raw) -> Decimal | None:
    if raw is None or raw == "":
        return None
    if isinstance(raw, (int, float)):
        return Decimal(str(raw)).quantize(Decimal("0.01"))
    s = str(raw).strip().replace("R$", "").replace(" ", "")
    if not s:
        return None
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return Decimal(s).quantize(Decimal("0.01"))
    except InvalidOperation:
        return None


def _parse_data_celula(raw) -> date | None:
    if raw is None or raw == "":
        return None
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, date):
        return raw
    s = str(raw).strip().lower()
    meses = {
        "jan": 1,
        "fev": 2,
        "mar": 3,
        "abr": 4,
        "mai": 5,
        "jun": 6,
        "jul": 7,
        "ago": 8,
        "set": 9,
        "out": 10,
        "nov": 11,
        "dez": 12,
    }
    if "/" in s:
        a, b = s.split("/", 1)
        a = a.strip()
        b = b.strip()
        if b.isdigit() and len(b) <= 2:
            dia = int(a)
            mes = int(b)
            ano = 2025 if mes >= 9 else 2026
        elif a.isdigit() and len(a) <= 2:
            dia = int(a)
            tok = b[:3]
            mes = meses.get(tok)
            if not mes:
                return None
            ano = 2025 if mes >= 9 else 2026
        else:
            return None
        try:
            return date(ano, mes, dia)
        except ValueError:
            return None
    return None


def _normalizar_data_sequencial(d: date, prev: date | None) -> date:
    """Corrige ano errado do Excel (ex.: nov/2025 serializado como 2026-11-01)."""
    if prev is None:
        return d
    if d <= prev:
        if prev.month == 12 and d.month == 1:
            candidato = date(prev.year + 1, d.month, d.day)
            if candidato > prev:
                return candidato
        # Jan/2026 após linha espúria 2027-01-01 ou salto de ano à frente
        if d.year < prev.year:
            candidato = date(prev.year, d.month, d.day)
            if candidato > prev:
                return candidato
            candidato = date(d.year, d.month, d.day)
            if candidato > prev:
                return candidato
        return prev + timedelta(days=1)
    delta = (d - prev).days
    if d.year > prev.year and delta > 62:
        candidato = d.replace(year=prev.year)
        if candidato > prev:
            return candidato
        candidato = d.replace(year=prev.year + 1)
        if candidato > prev:
            return candidato
    if d.year > prev.year + 1:
        candidato = d.replace(year=prev.year + 1)
        if candidato > prev:
            return candidato
    return d


def importar_dashboard_vendas_historico_xlsx(
    path: Path | str,
    *,
    deposito: str = "centro",
    limpar_intervalo: bool = False,
    limpar_tudo: bool = False,
) -> dict:
    """
    Importa planilha (col A=data, col B=total). Upsert por data.
    Retorna contadores para comando/migration.
    """
    import openpyxl

    path = Path(path)
    if not path.is_file():
        return {"ok": False, "erro": f"Arquivo não encontrado: {path}", "inseridos": 0, "atualizados": 0}

    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb.active
    rows_raw: list[tuple[date, Decimal]] = []
    for row in ws.iter_rows(values_only=True):
        if not row:
            continue
        d = _parse_data_celula(row[0] if len(row) > 0 else None)
        v = _parse_valor_celula(row[1] if len(row) > 1 else None)
        if d is None or v is None:
            continue
        # Linha espúria comum: 2027-01-01 R$ 0 entre dez/2025 e jan/2026
        if v == 0 and d.year >= 2027 and d.month == 1 and d.day == 1:
            continue
        rows_raw.append((d, v))

    prev: date | None = None
    rows: list[tuple[date, Decimal]] = []
    for d, v in rows_raw:
        d2 = _normalizar_data_sequencial(d, prev)
        rows.append((d2, v))
        prev = d2

    if not rows:
        return {"ok": False, "erro": "Nenhuma linha válida na planilha.", "inseridos": 0, "atualizados": 0}

    data_min = min(r[0] for r in rows)
    data_max = max(r[0] for r in rows)
    inseridos = 0
    atualizados = 0

    with transaction.atomic():
        if limpar_tudo:
            DashboardVendaDiaHistoricoAgro.objects.all().delete()
        elif limpar_intervalo:
            DashboardVendaDiaHistoricoAgro.objects.filter(
                data__gte=data_min,
                data__lte=data_max,
            ).delete()
        for d, v in rows:
            obj, created = DashboardVendaDiaHistoricoAgro.objects.update_or_create(
                data=d,
                defaults={
                    "total": v,
                    "deposito": deposito[:16],
                    "fonte": "planilha",
                },
            )
            if created:
                inseridos += 1
            else:
                atualizados += 1

    dashboard_invalidar_cache_meta_merged()
    return {
        "ok": True,
        "erro": "",
        "inseridos": inseridos,
        "atualizados": atualizados,
        "linhas": len(rows),
        "de": data_min.isoformat(),
        "ate": data_max.isoformat(),
    }
