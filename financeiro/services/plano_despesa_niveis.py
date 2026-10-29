"""Classificação Fixa/Variável/Outra + Grupo — planilha oficial CP."""
from __future__ import annotations

import csv
import unicodedata
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from django.conf import settings

from financeiro.models import LancamentoFinanceiro as NF

_TIPO_UI = {
    "fixa": "fixa",
    "variável": "variavel",
    "variavel": "variavel",
    "outra": "outra",
}


def _fold(s: str) -> str:
    s = unicodedata.normalize("NFKD", (s or "").strip())
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.casefold()


@dataclass(frozen=True)
class PlanoNivel:
    plano: str
    tipo: str
    grupo: str
    observacao: str = ""
    ordem: int = 0

    @property
    def tipo_ui(self) -> str:
        return _TIPO_UI.get(_fold(self.tipo), "outra")

    @property
    def vale_nao_soma_pessoal(self) -> bool:
        return "não somar" in (self.observacao or "").casefold() and "pessoal" in (
            self.observacao or ""
        ).casefold()


def _csv_path() -> Path:
    return Path(settings.BASE_DIR) / "docs" / "dados" / "plano_despesas_niveis_proposta.csv"


def _colunas_niveis(fieldnames: list[str] | None) -> tuple[str, str, str, str | None]:
    if not fieldnames:
        raise ValueError("CSV níveis sem cabeçalho")
    cols = {(c or "").strip().lower(): c for c in fieldnames}
    k_plano = cols.get("plano oficial")
    k_tipo = cols.get("tipo")
    k_grupo = cols.get("grupo")
    k_obs = cols.get("observação") or cols.get("observacao")
    if not k_plano or not k_tipo or not k_grupo:
        raise ValueError("CSV níveis precisa: Plano oficial; Tipo; Grupo")
    return k_plano, k_tipo, k_grupo, k_obs


@lru_cache(maxsize=1)
def _carregar_niveis() -> tuple[dict[str, PlanoNivel], list[str], list[str]]:
    """Por chave normalizada → registro; ordem de grupos e tipos como no CSV."""
    path = _csv_path()
    por_chave: dict[str, PlanoNivel] = {}
    ordem_grupos: list[str] = []
    ordem_tipos: list[str] = []
    vistos_g: set[str] = set()
    vistos_t: set[str] = set()

    if not path.is_file():
        return por_chave, ordem_grupos, ordem_tipos

    with path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        k_plano, k_tipo, k_grupo, k_obs = _colunas_niveis(reader.fieldnames)
        for i, row in enumerate(reader):
            plano = (row.get(k_plano) or "").strip()
            tipo = (row.get(k_tipo) or "").strip()
            grupo = (row.get(k_grupo) or "").strip()
            obs = (row.get(k_obs) or "").strip() if k_obs else ""
            if not plano:
                continue
            reg = PlanoNivel(
                plano=plano,
                tipo=tipo,
                grupo=grupo or "A conferir",
                observacao=obs,
                ordem=i,
            )
            por_chave[_fold(plano)] = reg
            gk = reg.grupo
            if gk not in vistos_g:
                vistos_g.add(gk)
                ordem_grupos.append(gk)
            tk = reg.tipo_ui
            if tk not in vistos_t:
                vistos_t.add(tk)
                ordem_tipos.append(tk)
    return por_chave, ordem_grupos, ordem_tipos


def lookup_plano_nivel(nome_plano: str) -> PlanoNivel | None:
    return _carregar_niveis()[0].get(_fold(nome_plano))


def grupo_negocio_ui(nome_plano: str) -> str:
    reg = lookup_plano_nivel(nome_plano)
    return reg.grupo if reg else "A conferir"


def tipo_ui(nome_plano: str) -> str | None:
    reg = lookup_plano_nivel(nome_plano)
    return reg.tipo_ui if reg else None


def ordem_grupos_negocio() -> list[str]:
    return list(_carregar_niveis()[1])


def natureza_dre_por_planilha(nome_plano: str) -> str | None:
    """Natureza DRE a partir da planilha; None se plano não cadastrado."""
    reg = lookup_plano_nivel(nome_plano)
    if not reg:
        return None
    t = reg.tipo_ui
    g = _fold(reg.grupo)
    f = _fold(nome_plano)

    if t == "fixa":
        return NF.NATUREZA_DESPESA_FIXA
    if t == "variavel":
        return NF.NATUREZA_DESPESA_VARIAVEL

    if "cmv" in g or "mercadoria" in g:
        return NF.NATUREZA_CMV
    if "emprestimo" in g or "empréstimo" in reg.grupo.casefold():
        if "juros" in f:
            return NF.NATUREZA_EMPRESTIMO_AMORTIZACAO
        return NF.NATUREZA_EMPRESTIMO_AMORTIZACAO
    if "socio" in g or "sócio" in reg.grupo:
        return NF.NATUREZA_RETIRADA_SOCIO
    if "investimento" in g:
        return NF.NATUREZA_DESPESA_FINANCEIRA
    return NF.NATUREZA_DESPESA_FINANCEIRA


def invalidar_cache_niveis() -> None:
    _carregar_niveis.cache_clear()
