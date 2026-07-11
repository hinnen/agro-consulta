"""
Simula ou aplica unificação de nomes de plano em TituloFinanceiroAgro (CP).

Uso:
  python manage.py unificar_planos_despesa --dry-run
  python manage.py unificar_planos_despesa --aplicar --confirmar

Padrão do mapa: docs/dados/plano_despesas_mapa_unificacao.csv
"""
from __future__ import annotations

import csv
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from produtos.lancamentos_financeiro_pg_util import (
    _CAP_LINHAS,
    _dec2,
    dedup_titulos,
    titulos_financeiro_montar_qs,
)
from produtos.models import TituloFinanceiroAgro


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


def _montar_por_oficial(grupos: dict[str, list[str]]) -> list[dict]:
    """Por nome oficial: grafias que corrigem, que já estão OK e total (≈ CP)."""
    por_oficial: list[dict] = []
    for oficial, grafias in grupos.items():
        corrige: list[dict] = []
        ja_ok: list[dict] = []
        total_n = 0
        total_bruto = Decimal("0")
        for grafia in grafias:
            n, bruto = _contagem_plano(grafia)
            if n == 0:
                continue
            entry = {"grafia": grafia, "titulos": n, "valor_bruto": bruto}
            if grafia == oficial:
                ja_ok.append(entry)
            else:
                corrige.append(entry)
            total_n += n
            total_bruto += bruto
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
    por_oficial = _montar_por_oficial(grupos) if grupos else []

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

    return {
        "por_rename": por_rename,
        "por_oficial": por_oficial,
        "destinos": dict(destinos),
        "nao_encontrados": nao_encontrados,
        "fora_mapa": fora_mapa,
        "total_titulos": total_titulos,
        "total_bruto": total_bruto,
        "pares": len(pares),
    }


def aplicar_unificacao(pares: list[tuple[str, str]]) -> dict:
    """Renomeia plano_conta; não apaga títulos."""
    atualizados = 0
    detalhes = []
    for antigo, oficial in pares:
        n = TituloFinanceiroAgro.objects.filter(
            despesa=True, plano_conta=antigo
        ).update(plano_conta=oficial[:200])
        if n:
            detalhes.append({"antigo": antigo, "oficial": oficial, "titulos": n})
            atualizados += n
    return {"titulos_atualizados": atualizados, "detalhes": detalhes}


def formatar_relatorio(sim: dict) -> str:
    lines = [
        "SIMULAÇÃO — unificar planos de despesa (CP)",
        "Nada foi alterado. Só contagem.",
        "",
        f"Renomes no mapa (nome muda): {sim['pares']}",
        f"Títulos que seriam renomeados: {sim['total_titulos']}",
        f"Soma valor bruto desses títulos: R$ {_fmt_brl(sim['total_bruto'])}",
        "",
        "=== CONFERIR NA CP (total = soma das grafias → bate com filtro do plano) ===",
        "Situação Todos · sem data · marque todas as grafias · contagem deduplicada (igual CP).",
        "",
    ]
    for bloco in sim.get("por_oficial") or []:
        lines.append(
            f"▸ {bloco['oficial']}  |  TOTAL {bloco['titulos']} título(s)  |  "
            f"R$ {_fmt_brl(bloco['valor_bruto'])}"
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
        for d in out["detalhes"]:
            self.stdout.write(f"  {d['antigo']} → {d['oficial']}: {d['titulos']}")
