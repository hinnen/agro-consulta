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
from django.db.models import Count, Sum

from produtos.models import TituloFinanceiroAgro


def _carregar_mapa(path: Path) -> list[tuple[str, str]]:
    """Retorna pares (antigo, oficial) onde o nome muda."""
    if not path.is_file():
        raise CommandError(f"Mapa não encontrado: {path}")
    pares: list[tuple[str, str]] = []
    with path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        if not reader.fieldnames:
            raise CommandError("CSV sem cabeçalho")
        # aceita nomes com ou sem espaços
        cols = { (c or "").strip().lower(): c for c in reader.fieldnames }
        k_ant = cols.get("nome antigo (como está no cp)") or cols.get("nome antigo")
        k_ofi = cols.get("nome oficial")
        if not k_ant or not k_ofi:
            raise CommandError(
                "CSV precisa das colunas: Nome antigo (como está no CP); Nome oficial"
            )
        for row in reader:
            antigo = (row.get(k_ant) or "").strip()
            oficial = (row.get(k_ofi) or "").strip()
            if not antigo or not oficial:
                continue
            if antigo == oficial:
                continue
            pares.append((antigo, oficial))
    return pares


def simular_unificacao(pares: list[tuple[str, str]]) -> dict:
    """Conta títulos e soma valor_bruto por rename (só leitura)."""
    por_rename: list[dict] = []
    total_titulos = 0
    total_bruto = Decimal("0")
    nao_encontrados: list[str] = []

    for antigo, oficial in pares:
        agg = TituloFinanceiroAgro.objects.filter(
            despesa=True, plano_conta=antigo
        ).aggregate(n=Count("id"), bruto=Sum("valor_bruto"))
        n = int(agg["n"] or 0)
        bruto = Decimal(str(agg["bruto"] or 0))
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

    # planos no CP que não estão no mapa (despesa)
    mapeados = {a for a, _ in pares} | {o for _, o in pares}
    # também nomes que já são oficiais (iguais no CSV)
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
        f"Soma valor bruto desses títulos: R$ {sim['total_bruto']:,.2f}".replace(",", "X")
        .replace(".", ",")
        .replace("X", "."),
        "",
        "=== POR RENAME (antigo → oficial) ===",
    ]
    for row in sorted(sim["por_rename"], key=lambda r: (-r["titulos"], r["antigo"].casefold())):
        vb = f"{row['valor_bruto']:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        lines.append(
            f"- {row['antigo']}  →  {row['oficial']}  |  {row['titulos']} título(s)  |  R$ {vb}"
        )
    if not sim["por_rename"]:
        lines.append("(nenhum título encontrado com os nomes antigos do mapa)")

    lines.append("")
    lines.append("=== CONSOLIDADO POR NOME OFICIAL (só o que muda) ===")
    for nome, d in sorted(sim["destinos"].items(), key=lambda x: x[0].casefold()):
        vb = f"{d['valor_bruto']:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        lines.append(f"- {nome}  |  +{d['titulos']} título(s)  |  R$ {vb}")

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
            sim = simular_unificacao(pares)
            self.stdout.write(formatar_relatorio(sim))
            return

        # aplica
        sim = simular_unificacao(pares)
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
