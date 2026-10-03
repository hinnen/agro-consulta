"""
Simula ou aplica unificação de nomes de plano em TituloFinanceiroAgro (CP).

Uso:
  python manage.py unificar_planos_despesa --dry-run
  python manage.py unificar_planos_despesa --aplicar --confirmar

Padrão do mapa: docs/dados/plano_despesas_mapa_unificacao.csv
"""
from __future__ import annotations

import csv
import unicodedata
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from produtos.lancamentos_financeiro_pg_util import (
    _CAP_LINHAS,
    _dec2,
    dedup_titulos,
    titulos_financeiro_montar_qs,
)
from produtos.models import PlanoUnificacaoLoteAgro, TituloFinanceiroAgro


def _fmt_brl(valor: Decimal) -> str:
    return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _colunas_mapa_csv(fieldnames: list[str] | None) -> tuple[str, str]:
    if not fieldnames:
        raise CommandError("CSV sem cabeçalho")
    cols = {(c or "").strip().lower(): c for c in fieldnames}
    k_ant = cols.get("nome antigo (como está no cp)") or cols.get("nome antigo")
    k_ofi = cols.get("nome oficial")
    if not k_ant or not k_ofi:
        raise CommandError(
            "CSV precisa das colunas: Nome antigo (como está no CP); Nome oficial"
        )
    return k_ant, k_ofi


def _carregar_mapa_grupos(path: Path) -> dict[str, list[str]]:
    """Oficial → grafias distintas listadas no mapa (inclui já-oficial)."""
    if not path.is_file():
        raise CommandError(f"Mapa não encontrado: {path}")
    grupos: dict[str, set[str]] = defaultdict(set)
    with path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        k_ant, k_ofi = _colunas_mapa_csv(reader.fieldnames)
        for row in reader:
            antigo = (row.get(k_ant) or "").strip()
            oficial = (row.get(k_ofi) or "").strip()
            if not antigo or not oficial:
                continue
            grupos[oficial].add(antigo)
    return {
        oficial: sorted(grafias, key=lambda s: s.casefold())
        for oficial, grafias in grupos.items()
    }


def _carregar_mapa(path: Path) -> list[tuple[str, str]]:
    """Retorna pares (antigo, oficial) onde o nome muda."""
    pares: list[tuple[str, str]] = []
    for oficial, grafias in _carregar_mapa_grupos(path).items():
        for antigo in grafias:
            if antigo != oficial:
                pares.append((antigo, oficial))
    return pares


def _norm_plano_chave(nome: str) -> str:
    s = (nome or "").strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return " ".join(s.casefold().split())


def _planos_cp_distintos() -> set[str]:
    return {
        str(p).strip()
        for p in TituloFinanceiroAgro.objects.filter(despesa=True)
        .exclude(plano_conta="")
        .values_list("plano_conta", flat=True)
        .distinct()
        if str(p).strip()
    }


def _contagem_plano(grafia: str, *, status: str = "todos") -> tuple[int, Decimal]:
    """Mesma regra da CP: filtro + deduplicar antes de contar/somar bruto."""
    qs = titulos_financeiro_montar_qs(despesa=True, status=status).filter(plano_conta=grafia)
    cap = _CAP_LINHAS
    rows = list(qs[: cap + 1])
    if len(rows) > cap:
        rows = rows[:cap]
    deduped = dedup_titulos(rows)
    bruto = sum((_dec2(t.valor_bruto) for t in deduped), Decimal("0"))
    return len(deduped), bruto


def _contagem_planos_conjunto(
    grafias: list[str], *, status: str = "todos"
) -> tuple[int, Decimal]:
    """Igual CP com vários planos marcados: filtra todos e deduplica uma vez."""
    grafias_u = list(dict.fromkeys((g or "").strip() for g in grafias if (g or "").strip()))
    if not grafias_u:
        return 0, Decimal("0")
    qs = titulos_financeiro_montar_qs(despesa=True, status=status).filter(
        plano_conta__in=grafias_u
    )
    cap = _CAP_LINHAS
    rows = list(qs[: cap + 1])
    if len(rows) > cap:
        rows = rows[:cap]
    deduped = dedup_titulos(rows)
    bruto = sum((_dec2(t.valor_bruto) for t in deduped), Decimal("0"))
    return len(deduped), bruto


def _total_geral_despesa_cp(*, status: str = "todos") -> tuple[int, Decimal]:
    """Toda despesa CP deduplicada — compare com «todos planos» marcados na lista."""
    qs = titulos_financeiro_montar_qs(despesa=True, status=status)
    cap = _CAP_LINHAS
    rows = list(qs[: cap + 1])
    if len(rows) > cap:
        rows = rows[:cap]
    deduped = dedup_titulos(rows)
    bruto = sum((_dec2(t.valor_bruto) for t in deduped), Decimal("0"))
    return len(deduped), bruto


def _montar_por_oficial(
    grupos: dict[str, list[str]], planos_cp: set[str]
) -> list[dict]:
    """Por nome oficial: grafias do mapa + extras no CP (mesma «família» de nome)."""
    por_oficial: list[dict] = []
    for oficial, grafias_mapa in grupos.items():
        mapa_set = set(grafias_mapa)
        chave = _norm_plano_chave(oficial)
        extras_cp = sorted(
            (p for p in planos_cp if _norm_plano_chave(p) == chave and p not in mapa_set),
            key=str.casefold,
        )
        todas_grafias = sorted(set(grafias_mapa) | set(extras_cp), key=str.casefold)

        corrige: list[dict] = []
        ja_ok: list[dict] = []
        fora_mapa: list[dict] = []
        for grafia in todas_grafias:
            n, bruto = _contagem_plano(grafia)
            if n == 0:
                continue
            entry = {"grafia": grafia, "titulos": n, "valor_bruto": bruto}
            if grafia in extras_cp:
                fora_mapa.append(entry)
            elif grafia == oficial:
                ja_ok.append(entry)
            else:
                corrige.append(entry)

        total_n, total_bruto = _contagem_planos_conjunto(todas_grafias)
        if total_n == 0:
            continue
        por_oficial.append(
            {
                "oficial": oficial,
                "corrige": sorted(
                    corrige, key=lambda r: (-r["titulos"], r["grafia"].casefold())
                ),
                "ja_ok": sorted(
                    ja_ok, key=lambda r: (-r["titulos"], r["grafia"].casefold())
                ),
                "fora_mapa": sorted(
                    fora_mapa, key=lambda r: (-r["titulos"], r["grafia"].casefold())
                ),
                "grafias_cp": todas_grafias,
                "titulos": total_n,
                "valor_bruto": total_bruto,
            }
        )
    por_oficial.sort(key=lambda r: (-r["titulos"], r["oficial"].casefold()))
    return por_oficial


def simular_unificacao(
    pares: list[tuple[str, str]], *, path: Path | None = None
) -> dict:
    """Conta títulos e soma valor_bruto por rename (só leitura)."""
    por_rename: list[dict] = []
    total_titulos = 0
    total_bruto = Decimal("0")
    nao_encontrados: list[str] = []

    for antigo, oficial in pares:
        n, bruto = _contagem_plano(antigo)
        if n == 0:
            nao_encontrados.append(antigo)
            continue
        por_rename.append(
            {
                "antigo": antigo,
                "oficial": oficial,
                "titulos": n,
                "valor_bruto": bruto,
            }
        )
        total_titulos += n
        total_bruto += bruto

    # planos oficiais após merge (só os que mudam + destino)
    destinos = defaultdict(lambda: {"titulos": 0, "valor_bruto": Decimal("0")})
    for row in por_rename:
        d = destinos[row["oficial"]]
        d["titulos"] += row["titulos"]
        d["valor_bruto"] += row["valor_bruto"]

    grupos = _carregar_mapa_grupos(path) if path else {}
    planos_cp = _planos_cp_distintos() if grupos else set()
    por_oficial = _montar_por_oficial(grupos, planos_cp) if grupos else []

    mapeados: set[str] = set()
    if grupos:
        for oficial, grafias in grupos.items():
            mapeados.add(oficial)
            mapeados.update(grafias)
    else:
        mapeados = {a for a, _ in pares} | {o for _, o in pares}

    extras = (
        TituloFinanceiroAgro.objects.filter(despesa=True)
        .exclude(plano_conta="")
        .values_list("plano_conta", flat=True)
        .distinct()
    )
    fora_mapa = sorted(
        {str(p).strip() for p in extras if str(p).strip() and str(p).strip() not in mapeados},
        key=lambda s: s.casefold(),
    )
    total_geral_n, total_geral_bruto = _total_geral_despesa_cp()
    fora_mapa_n, fora_mapa_bruto = (
        _contagem_planos_conjunto(fora_mapa) if fora_mapa else (0, Decimal("0"))
    )

    return {
        "por_rename": por_rename,
        "por_oficial": por_oficial,
        "destinos": dict(destinos),
        "nao_encontrados": nao_encontrados,
        "fora_mapa": fora_mapa,
        "fora_mapa_titulos": fora_mapa_n,
        "fora_mapa_bruto": fora_mapa_bruto,
        "total_geral_cp": {"titulos": total_geral_n, "valor_bruto": total_geral_bruto},
        "total_titulos": total_titulos,
        "total_bruto": total_bruto,
        "pares": len(pares),
    }


def aplicar_unificacao(
    pares: list[tuple[str, str]], *, usuario=None
) -> dict:
    """Renomeia plano_conta; grava lote para reverter."""
    alteracoes: list[dict] = []
    atualizados = 0
    detalhes = []
    for antigo, oficial in pares:
        qs = TituloFinanceiroAgro.objects.filter(despesa=True, plano_conta=antigo)
        mids = list(qs.values_list("mongo_id", flat=True))
        if not mids:
            continue
        for mid in mids:
            alteracoes.append(
                {"mongo_id": str(mid), "de": antigo, "para": oficial}
            )
        n = qs.update(plano_conta=oficial[:200])
        if n:
            detalhes.append({"antigo": antigo, "oficial": oficial, "titulos": n})
            atualizados += n

    lote_id = None
    if alteracoes:
        u = usuario if getattr(usuario, "is_authenticated", False) else None
        lote = PlanoUnificacaoLoteAgro.objects.create(
            usuario=u,
            n_titulos=len(alteracoes),
            alteracoes=alteracoes,
        )
        lote_id = lote.pk

    return {
        "titulos_atualizados": atualizados,
        "detalhes": detalhes,
        "lote_id": lote_id,
    }


def reverter_ultimo_lote(*, usuario=None) -> dict:
    """Desfaz o último lote aplicado (plano_conta volta ao nome antigo)."""
    lote = (
        PlanoUnificacaoLoteAgro.objects.filter(
            status=PlanoUnificacaoLoteAgro.Status.APLICADO
        )
        .order_by("-criado_em")
        .first()
    )
    if not lote:
        raise CommandError("Nenhum lote aplicado para reverter neste ambiente.")

    revertidos = 0
    pulados = 0
    for item in lote.alteracoes or []:
        mid = str(item.get("mongo_id") or "").strip()
        de = str(item.get("de") or "").strip()
        para = str(item.get("para") or "").strip()
        if not mid or not de or not para:
            pulados += 1
            continue
        n = TituloFinanceiroAgro.objects.filter(
            mongo_id=mid, plano_conta=para
        ).update(plano_conta=de[:200])
        if n:
            revertidos += 1
        else:
            pulados += 1

    u = usuario if getattr(usuario, "is_authenticated", False) else None
    lote.status = PlanoUnificacaoLoteAgro.Status.REVERTIDO
    lote.revertido_em = timezone.now()
    lote.revertido_por = u
    lote.save(update_fields=["status", "revertido_em", "revertido_por"])

    return {
        "lote_id": lote.pk,
        "revertidos": revertidos,
        "pulados": pulados,
        "criado_em": lote.criado_em,
    }


def formatar_relatorio(sim: dict) -> str:
    lines = [
        "SIMULAÇÃO — unificar planos de despesa (CP)",
        "Nada foi alterado. Só contagem.",
        "",
        f"Renomes no mapa (nome muda): {sim['pares']}",
        f"Títulos que seriam renomeados: {sim['total_titulos']}",
        f"Soma valor bruto desses títulos: R$ {_fmt_brl(sim['total_bruto'])}",
        "",
        "=== TOTAL GERAL CP (marque TODOS os planos na lista) ===",
        "Situação Todos · sem data · deduplicado.",
        (
            f"▸ TODA despesa  |  {sim['total_geral_cp']['titulos']} título(s)  |  "
            f"R$ {_fmt_brl(sim['total_geral_cp']['valor_bruto'])}"
        ),
        (
            f"   Planos fora do mapa CSV: {sim.get('fora_mapa_titulos', 0)} título(s)  |  "
            f"R$ {_fmt_brl(sim.get('fora_mapa_bruto') or 0)}"
        ),
        "",
        "=== CONFERIR NA CP (TOTAL = marque TODAS as grafias listadas abaixo) ===",
        "Situação Todos · sem data · deduplicado igual CP · inclui grafias ainda fora do mapa CSV.",
        "",
    ]
    for bloco in sim.get("por_oficial") or []:
        n_graf = len(bloco.get("grafias_cp") or [])
        lines.append(
            f"▸ {bloco['oficial']}  |  TOTAL {bloco['titulos']} título(s)  |  "
            f"R$ {_fmt_brl(bloco['valor_bruto'])}  |  {n_graf} grafia(s) no CP"
        )
        for row in bloco["corrige"]:
            lines.append(
                f"    VAI CORRIGIR: {row['grafia']}  |  {row['titulos']} título(s)  |  "
                f"R$ {_fmt_brl(row['valor_bruto'])}"
            )
        for row in bloco["ja_ok"]:
            lines.append(
                f"    JÁ ESTÁ OK: {row['grafia']}  |  {row['titulos']} título(s)  |  "
                f"R$ {_fmt_brl(row['valor_bruto'])}"
            )
        for row in bloco.get("fora_mapa") or []:
            lines.append(
                f"    FALTA NO MAPA: {row['grafia']}  |  {row['titulos']} título(s)  |  "
                f"R$ {_fmt_brl(row['valor_bruto'])}  ← marque na CP; incluir no CSV antes de aplicar"
            )
        lines.append("")
    if not sim.get("por_oficial"):
        lines.append("(nenhum plano oficial com título no CP)")
        lines.append("")

    lines.append("=== DETALHE — só o que seria renomeado (antigo → oficial) ===")
    for row in sorted(sim["por_rename"], key=lambda r: (-r["titulos"], r["antigo"].casefold())):
        lines.append(
            f"- {row['antigo']}  →  {row['oficial']}  |  {row['titulos']} título(s)  |  "
            f"R$ {_fmt_brl(row['valor_bruto'])}"
        )
    if not sim["por_rename"]:
        lines.append("(nenhum título encontrado com os nomes antigos do mapa)")

    lines.append("")
    lines.append("=== NO MAPA MAS SEM TÍTULO NO CP ===")
    for nome in sim["nao_encontrados"][:80]:
        lines.append(f"- {nome}")
    if len(sim["nao_encontrados"]) > 80:
        lines.append(f"... +{len(sim['nao_encontrados']) - 80} nomes")

    lines.append("")
    lines.append("=== NO CP (despesa) E FORA DO MAPA — revisar ===")
    for nome in sim["fora_mapa"][:100]:
        lines.append(f"- {nome}")
    if len(sim["fora_mapa"]) > 100:
        lines.append(f"... +{len(sim['fora_mapa']) - 100} planos")
    if not sim["fora_mapa"]:
        lines.append("(nenhum)")

    lines.append("")
    lines.append("Próximo: se OK → python manage.py unificar_planos_despesa --aplicar --confirmar")
    return "\n".join(lines)


class Command(BaseCommand):
    help = "Simula ou aplica unificação de planos de despesa (TituloFinanceiroAgro)."

    def add_arguments(self, parser):
        base = Path(settings.BASE_DIR) / "docs" / "dados" / "plano_despesas_mapa_unificacao.csv"
        parser.add_argument(
            "--mapa",
            type=str,
            default=str(base),
            help="Caminho do CSV de unificação",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Só simula (padrão se não passar --aplicar)",
        )
        parser.add_argument(
            "--aplicar",
            action="store_true",
            help="Aplica renomes no banco",
        )
        parser.add_argument(
            "--confirmar",
            action="store_true",
            help="Obrigatório junto com --aplicar",
        )

    def handle(self, *args, **options):
        path = Path(options["mapa"])
        pares = _carregar_mapa(path)
        if not pares:
            raise CommandError("Nenhum rename no mapa (todos antigo==oficial?).")

        aplicar = bool(options["aplicar"])
        if aplicar and not options["confirmar"]:
            raise CommandError("Para aplicar use: --aplicar --confirmar")

        if not aplicar:
            sim = simular_unificacao(pares, path=path)
            self.stdout.write(formatar_relatorio(sim))
            return

        # aplica
        sim = simular_unificacao(pares, path=path)
        self.stdout.write(formatar_relatorio(sim))
        self.stdout.write("")
        self.stdout.write("Aplicando renomes…")
        out = aplicar_unificacao(pares)
        self.stdout.write(
            self.style.SUCCESS(
                f"OK — {out['titulos_atualizados']} título(s) atualizados."
            )
        )
        if out.get("lote_id"):
            self.stdout.write(f"Lote backup #{out['lote_id']} (reversível).")
        for d in out["detalhes"]:
            self.stdout.write(f"  {d['antigo']} → {d['oficial']}: {d['titulos']}")
