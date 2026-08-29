"""Tabelas globais de % desconto/acréscimo por forma de pagamento (PDV)."""
from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any

from produtos.precos_forma_pagamento_util import (
    _forma_canonica,
    extrair_precos_grupos_cadastro_extras,
    extrair_precos_modo_cadastro_extras,
    extrair_precos_por_forma_cadastro_extras,
    formas_pagamento_lista,
    normalizar_precos_modo,
    preco_venda_para_forma,
)


def _dec(v: Any, default: Decimal = Decimal("0")) -> Decimal:
    if v is None or str(v).strip() == "":
        return default
    try:
        return Decimal(str(v).replace(",", ".").strip())
    except (InvalidOperation, ValueError, TypeError):
        return default


def arredondar_dezena_centavos(valor: Any) -> Decimal:
    """
    Arredonda para múltiplo de R$ 0,10.
    Unidades de centavo ≤4 descem; ≥5 sobem.
    Ex.: 10,43→10,40 · 10,45→10,50 · 10,47→10,50.
    """
    v = _dec(valor)
    if v <= 0:
        return Decimal("0.00")
    # Trabalha em centavos inteiros
    cents = int((v * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    dezena = cents // 10
    resto = cents % 10
    if resto <= 4:
        cents_out = dezena * 10
    else:
        cents_out = (dezena + 1) * 10
    return (Decimal(cents_out) / Decimal("100")).quantize(Decimal("0.01"))


def preco_com_percentual(
    base: Any, percentual: Any, *, arredondar: bool = False
) -> Decimal:
    """base × (1 + pct/100). percentual negativo = desconto."""
    b = _dec(base)
    if b <= 0:
        return Decimal("0.00")
    pct = _dec(percentual)
    out = b * (Decimal("1") + pct / Decimal("100"))
    out = out.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    if arredondar:
        out = arredondar_dezena_centavos(out)
    if out < 0:
        return Decimal("0.00")
    return out


def _norm_str_list(raw: Any) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    if not isinstance(raw, (list, tuple)):
        return out
    for it in raw:
        s = str(it or "").strip()
        if not s:
            continue
        key = s.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(s)
    return out


def _norm_id_list(raw: Any) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    if not isinstance(raw, (list, tuple)):
        return out
    for it in raw:
        s = str(it or "").strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _norm_formas(raw: Any) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    if not isinstance(raw, (list, tuple)):
        return out
    for it in raw:
        f = _forma_canonica(str(it or ""))
        if not f or f in seen:
            continue
        seen.add(f)
        out.append(f)
    return out


def obter_ou_criar_duas():
    """Garante slots 1 e 2. Retorna lista ordenada [t1, t2]."""
    from produtos.models import TabelaPrecoFormaAgro

    out = []
    for slot, nome in ((1, "Tabela 1"), (2, "Tabela 2")):
        obj, _ = TabelaPrecoFormaAgro.objects.get_or_create(
            slot=slot,
            defaults={
                "nome": nome,
                "ativo": False,
                "percentual": Decimal("0"),
                "arredondar_dezena_centavos": False,
                "formas": [],
                "categorias_vetadas": [],
                "produtos_vetados": [],
            },
        )
        out.append(obj)
    return out


def tabela_para_dict(t) -> dict[str, Any]:
    return {
        "id": t.pk,
        "slot": int(t.slot),
        "nome": str(t.nome or f"Tabela {t.slot}"),
        "ativo": bool(t.ativo),
        "percentual": float(_dec(t.percentual)),
        "arredondar_dezena_centavos": bool(t.arredondar_dezena_centavos),
        "formas": _norm_formas(t.formas),
        "categorias_vetadas": _norm_str_list(t.categorias_vetadas),
        "produtos_vetados": _norm_id_list(t.produtos_vetados),
    }


def payload_pdv_tabelas() -> dict[str, Any]:
    """Payload leve para bootstrap do PDV."""
    from produtos.models import TabelaPrecoFormaResolucaoAgro

    tabelas = obter_ou_criar_duas()
    ativas = [t for t in tabelas if t.ativo and _norm_formas(t.formas)]
    resolucoes: dict[str, dict[str, str]] = {}
    if ativas:
        qs = TabelaPrecoFormaResolucaoAgro.objects.filter(
            tabela_id__in=[t.pk for t in ativas]
        ).values_list("tabela__slot", "produto_externo_id", "preferencia")
        for slot, pid, pref in qs:
            key = str(int(slot))
            resolucoes.setdefault(key, {})[str(pid)] = str(pref or "individual")
    return {
        "tabelas": [tabela_para_dict(t) for t in ativas],
        "resolucoes": resolucoes,
        "formas_disponiveis": formas_pagamento_lista(),
    }


def produto_elegivel(tabela_dict: dict, produto: dict | None) -> bool:
    if not produto or not isinstance(tabela_dict, dict):
        return False
    if not tabela_dict.get("ativo"):
        return False
    pid = str(produto.get("id") or produto.get("produto_id") or "").strip()
    vetados = set(_norm_id_list(tabela_dict.get("produtos_vetados")))
    if pid and pid in vetados:
        return False
    cats_vet = {c.casefold() for c in _norm_str_list(tabela_dict.get("categorias_vetadas"))}
    if cats_vet:
        cat = str(
            produto.get("categoria")
            or produto.get("Categoria")
            or (produto.get("cadastro_extras") or {}).get("categoria")
            or ""
        ).strip()
        if cat and cat.casefold() in cats_vet:
            return False
    return bool(_norm_formas(tabela_dict.get("formas")))


def tabela_para_forma(
    forma: str | None,
    tabelas: list[dict] | None,
    *,
    produto: dict | None = None,
) -> dict | None:
    """Primeira tabela ativa (slot 1 depois 2) cuja lista inclui a forma e o produto é elegível."""
    forma_n = _forma_canonica(str(forma or ""))
    if not forma_n or not tabelas:
        return None
    ordered = sorted(
        [t for t in tabelas if isinstance(t, dict)],
        key=lambda x: int(x.get("slot") or 99),
    )
    for t in ordered:
        if not t.get("ativo"):
            continue
        if forma_n not in set(_norm_formas(t.get("formas"))):
            continue
        if produto is not None and not produto_elegivel(t, produto):
            continue
        return t
    return None


def _tem_preco_individual_na_forma(produto: dict, forma: str) -> bool:
    """True se cadastro tem preço próprio (por_forma ou grupos) para esta forma."""
    forma_n = _forma_canonica(forma)
    if not forma_n or not produto:
        return False
    ce = produto.get("cadastro_extras") if isinstance(produto.get("cadastro_extras"), dict) else {}
    modo = normalizar_precos_modo(
        produto.get("precos_modo") or ce.get("precos_modo")
    )
    base = float(produto.get("preco_padrao") or produto.get("preco_venda") or produto.get("preco") or 0)
    ppf = produto.get("precos_por_forma") or extrair_precos_por_forma_cadastro_extras(ce)
    pg = produto.get("precos_grupos") or extrair_precos_grupos_cadastro_extras(ce)
    if modo == "grupos" and pg:
        calc = preco_venda_para_forma(base, None, forma_n, precos_modo="grupos", precos_grupos=pg)
        return abs(calc - base) > 0.0001 or forma_n in set(
            _norm_formas(pg.get("formas_a"))
        ) or forma_n in set(_norm_formas(pg.get("formas_b")))
    if ppf and isinstance(ppf, dict):
        for k in ppf.keys():
            if _forma_canonica(str(k)) == forma_n:
                return float(ppf.get(k) or 0) > 0
    return False


def preferencia_resolucao(
    slot: int,
    produto_id: str,
    resolucoes: dict | None,
) -> str:
    """tabela | individual. Default = individual."""
    if not resolucoes or not isinstance(resolucoes, dict):
        return "individual"
    by_slot = resolucoes.get(str(int(slot))) or resolucoes.get(int(slot))
    if not isinstance(by_slot, dict):
        return "individual"
    pref = str(by_slot.get(str(produto_id)) or "").strip().lower()
    if pref == "tabela":
        return "tabela"
    return "individual"


def preco_tabela_para_produto(produto: dict, tabela: dict) -> Decimal | None:
    """Preço base × % da tabela (para chips), se elegível."""
    if not produto_elegivel(tabela, produto):
        return None
    base = _dec(
        produto.get("preco_padrao")
        if produto.get("preco_padrao") is not None
        else produto.get("preco_venda")
        if produto.get("preco_venda") is not None
        else produto.get("preco")
    )
    if base <= 0:
        return None
    return preco_com_percentual(
        base,
        tabela.get("percentual"),
        arredondar=bool(tabela.get("arredondar_dezena_centavos")),
    )


def preco_pdv_para_forma(
    produto: dict,
    forma: str | None,
    *,
    tabelas: list[dict] | None = None,
    resolucoes: dict | None = None,
    preco_base_override: float | None = None,
) -> float:
    """
    Preço cobrado no PDV para a forma.
    Ordem: tabela elegível → se individual na forma e resolução≠tabela → individual;
    senão % tabela; senão lógica atual (por_forma/grupos/base).
    """
    base = float(
        preco_base_override
        if preco_base_override is not None
        else produto.get("preco_padrao")
        if produto.get("preco_padrao") is not None
        else produto.get("preco_venda")
        if produto.get("preco_venda") is not None
        else produto.get("preco")
        or 0
    )
    ce = produto.get("cadastro_extras") if isinstance(produto.get("cadastro_extras"), dict) else {}
    modo = normalizar_precos_modo(produto.get("precos_modo") or ce.get("precos_modo"))
    ppf = produto.get("precos_por_forma") or extrair_precos_por_forma_cadastro_extras(ce)
    pg = produto.get("precos_grupos") or extrair_precos_grupos_cadastro_extras(ce)
    individual = preco_venda_para_forma(
        base, ppf, forma, precos_modo=modo, precos_grupos=pg
    )

    t = tabela_para_forma(forma, tabelas, produto=produto)
    if not t:
        return float(individual)

    pid = str(produto.get("id") or produto.get("produto_id") or "").strip()
    tem_ind = _tem_preco_individual_na_forma(produto, str(forma or ""))
    pref = preferencia_resolucao(int(t.get("slot") or 0), pid, resolucoes)
    if tem_ind and pref != "tabela":
        return float(individual)

    calc = preco_com_percentual(
        base,
        t.get("percentual"),
        arredondar=bool(t.get("arredondar_dezena_centavos")),
    )
    return float(calc)


def validar_overlap_formas(tabelas_payload: list[dict]) -> str | None:
    """Retorna mensagem de erro se T1 e T2 compartilham forma."""
    by_slot: dict[int, set[str]] = {}
    for t in tabelas_payload or []:
        if not isinstance(t, dict):
            continue
        slot = int(t.get("slot") or 0)
        if slot not in (1, 2):
            continue
        if not t.get("ativo"):
            continue
        by_slot[slot] = set(_norm_formas(t.get("formas")))
    if 1 in by_slot and 2 in by_slot:
        inter = by_slot[1] & by_slot[2]
        if inter:
            nomes = ", ".join(sorted(inter))
            return f"Forma(s) em comum nas duas tabelas: {nomes}. Cada forma só pode estar em uma."
    return None


def aplicar_payload_tabela(obj, data: dict) -> None:
    if "nome" in data:
        nome = str(data.get("nome") or "").strip()[:80]
        if nome:
            obj.nome = nome
    if "ativo" in data:
        obj.ativo = bool(data.get("ativo"))
    if "percentual" in data:
        obj.percentual = _dec(data.get("percentual"))
    if "arredondar_dezena_centavos" in data:
        obj.arredondar_dezena_centavos = bool(data.get("arredondar_dezena_centavos"))
    if "formas" in data:
        obj.formas = _norm_formas(data.get("formas"))
    if "categorias_vetadas" in data:
        obj.categorias_vetadas = _norm_str_list(data.get("categorias_vetadas"))
    if "produtos_vetados" in data:
        obj.produtos_vetados = _norm_id_list(data.get("produtos_vetados"))


def listar_conflitos(tabela, *, limit: int = 200) -> list[dict]:
    """
    Produtos com preço individual (por forma ou grupos) que cruzam as formas da tabela.
    """
    from produtos.models import ProdutoGestaoOverlayAgro, TabelaPrecoFormaResolucaoAgro

    formas = set(_norm_formas(tabela.formas))
    if not formas:
        return []
    resol_map = {
        str(r.produto_externo_id): r.preferencia
        for r in TabelaPrecoFormaResolucaoAgro.objects.filter(tabela=tabela)
    }
    out: list[dict] = []
    qs = ProdutoGestaoOverlayAgro.objects.exclude(cadastro_extras={}).iterator(
        chunk_size=500
    )
    for ov in qs:
        ce = ov.cadastro_extras if isinstance(ov.cadastro_extras, dict) else {}
        if not ce:
            continue
        modo = extrair_precos_modo_cadastro_extras(ce)
        ppf = extrair_precos_por_forma_cadastro_extras(ce) or {}
        pg = extrair_precos_grupos_cadastro_extras(ce)
        hit_formas: list[str] = []
        if modo == "grupos" and pg:
            fa = set(_norm_formas(pg.get("formas_a")))
            fb = set(_norm_formas(pg.get("formas_b")))
            hit_formas = sorted(formas & (fa | fb))
        else:
            for k in ppf.keys():
                fk = _forma_canonica(str(k))
                if fk in formas and float(ppf.get(k) or 0) > 0:
                    hit_formas.append(fk)
        if not hit_formas:
            continue
        pid = str(ov.produto_externo_id or "").strip()
        out.append(
            {
                "produto_id": pid,
                "nome": str(getattr(ov, "nome", None) or ce.get("nome") or pid)[:120],
                "formas": hit_formas,
                "preferencia": resol_map.get(pid) or "individual",
            }
        )
        if len(out) >= limit:
            break
    return out


def mesclar_resolucoes(tabela, itens: list[dict]) -> int:
    """itens: [{produto_id, preferencia}]. Retorna qtd gravada."""
    from produtos.models import TabelaPrecoFormaResolucaoAgro

    n = 0
    for it in itens or []:
        if not isinstance(it, dict):
            continue
        pid = str(it.get("produto_id") or "").strip()
        pref = str(it.get("preferencia") or "").strip().lower()
        if not pid or pref not in ("tabela", "individual"):
            continue
        TabelaPrecoFormaResolucaoAgro.objects.update_or_create(
            tabela=tabela,
            produto_externo_id=pid,
            defaults={"preferencia": pref},
        )
        n += 1
    return n


def regra_promo_vs_tabela(
    preco_promo: float,
    preco_tabela_ou_base: float,
    regra: str | None,
) -> float:
    """maior (default) | promo | tabela."""
    r = str(regra or "maior").strip().lower()
    p = float(preco_promo or 0)
    t = float(preco_tabela_ou_base or 0)
    if r == "promo":
        return p if p > 0 else t
    if r == "tabela":
        return t if t > 0 else p
    # maior valor
    if p <= 0:
        return t
    if t <= 0:
        return p
    return max(p, t)
