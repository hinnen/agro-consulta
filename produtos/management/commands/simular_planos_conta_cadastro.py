"""
Simula o efeito do cadastro oficial de planos — só leitura (não grava nada).

  python manage.py simular_planos_conta_cadastro
"""
from __future__ import annotations

from collections import defaultdict
from pathlib import Path

from django.core.management.base import BaseCommand
from django.db.models import Count

from produtos.models import TituloFinanceiroAgro
from produtos.plano_conta_agro_util import (
    _csv_mapa_path,
    _csv_niveis_path,
    norm_plano_chave,
)


def _carregar_mapa_csv() -> tuple[set[str], dict[str, str]]:
    """oficiais; chave_norm → oficial (inclui aliases do CSV)."""
    import csv

    oficiais: set[str] = set()
    mapa: dict[str, str] = {}

    niveis = _csv_niveis_path()
    if niveis.is_file():
        with niveis.open(encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f, delimiter=";")
            for row in reader:
                nome = (row.get("Plano oficial") or row.get("plano oficial") or "").strip()
                if nome:
                    oficiais.add(nome)
                    mapa[norm_plano_chave(nome)] = nome

    path = _csv_mapa_path()
    if path.is_file():
        with path.open(encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f, delimiter=";")
            cols = {(c or "").strip().lower(): c for c in (reader.fieldnames or [])}
            k_ant = cols.get("nome antigo (como está no cp)") or cols.get("nome antigo")
            k_ofi = cols.get("nome oficial")
            if k_ant and k_ofi:
                for row in reader:
                    antigo = (row.get(k_ant) or "").strip()
                    oficial = (row.get(k_ofi) or "").strip()
                    if not oficial:
                        continue
                    oficiais.add(oficial)
                    mapa[norm_plano_chave(oficial)] = oficial
                    if antigo and antigo != oficial:
                        mapa[norm_plano_chave(antigo)] = oficial
    return oficiais, mapa


class Command(BaseCommand):
    help = "Simula cadastro de planos CP (só leitura — nenhum título alterado)."

    def handle(self, *args, **options):
        oficiais, mapa = _carregar_mapa_csv()
        rows = list(
            TituloFinanceiroAgro.objects.filter(despesa=True)
            .exclude(plano_conta="")
            .values("plano_conta")
            .annotate(c=Count("id"))
            .order_by("-c")
        )
        total_titulos = sum(r["c"] for r in rows)
        grafias = len(rows)

        grupos: dict[str, dict] = {}
        orfaos: list[tuple[str, int]] = []
        for r in rows:
            nome = (r["plano_conta"] or "").strip()
            n = int(r["c"] or 0)
            oficial = mapa.get(norm_plano_chave(nome))
            if oficial:
                g = grupos.setdefault(
                    oficial, {"oficial": oficial, "titulos": 0, "grafias": []}
                )
                g["titulos"] += n
                if nome not in g["grafias"]:
                    g["grafias"].append(nome)
            elif nome in oficiais:
                g = grupos.setdefault(
                    nome, {"oficial": nome, "titulos": 0, "grafias": []}
                )
                g["titulos"] += n
                if nome not in g["grafias"]:
                    g["grafias"].append(nome)
            else:
                from produtos.mongo_financeiro_util import EMPRESTIMO_DUAL_LABEL

                if nome == EMPRESTIMO_DUAL_LABEL:
                    # pseudo-plano do sistema — não é órfão de cadastro
                    continue
                orfaos.append((nome, n))

        merges = [g for g in grupos.values() if len(g["grafias"]) > 1]
        merges.sort(key=lambda g: -g["titulos"])
        orfaos.sort(key=lambda x: -x[1])

        self.stdout.write("")
        self.stdout.write("=== SIMULAÇÃO planos CP (só leitura) ===")
        self.stdout.write(f"Títulos CP com plano: {total_titulos}")
        self.stdout.write(f"Grafias distintas HOJE (checkboxes brutos): {grafias}")
        self.stdout.write(
            f"Após cadastro+aliases (checkboxes agrupados): {len(grupos) + len(orfaos)}"
        )
        self.stdout.write(f"Grupos com 2+ grafias unificadas na tela: {len(merges)}")
        self.stdout.write(f"Órfãos (fora do mapa — vão no alerta): {len(orfaos)}")
        self.stdout.write("")
        self.stdout.write("NÃO altera: valor, data, fornecedor, quitação, texto do plano no título.")
        self.stdout.write("SÓ cria: tabelas PlanoContaAgro + aliases (seed CSV).")
        self.stdout.write("")

        if merges:
            self.stdout.write("--- Merges na tela (exemplos) ---")
            for g in merges[:20]:
                graf = " | ".join(g["grafias"])
                self.stdout.write(
                    f"  -> «{g['oficial']}» · {g['titulos']} tit. · grafias: {graf}"
                )
            if len(merges) > 20:
                self.stdout.write(f"  … +{len(merges) - 20} grupos")
            self.stdout.write("")

        if orfaos:
            self.stdout.write("--- Órfãos (alerta no CP) ---")
            for nome, n in orfaos[:40]:
                self.stdout.write(f"  · {n} tít. · {nome}")
            if len(orfaos) > 40:
                self.stdout.write(f"  … +{len(orfaos) - 40}")
            self.stdout.write("")
        else:
            self.stdout.write("Órfãos: nenhum (mapa cobre todas as grafias deste banco).")
            self.stdout.write("")

        self.stdout.write(
            f"CSV oficiais: {len(oficiais)} · mapa path: {Path(_csv_mapa_path()).name}"
        )
        self.stdout.write("=== fim simulação ===")
        self.stdout.write("")
