"""Views da Central de Relatórios (além de validade/etiquetas)."""
from __future__ import annotations

import logging

from django.shortcuts import render
from django.views.decorators.http import require_GET

from produtos import relatorios_vendas_util as ru

logger = logging.getLogger(__name__)


def _qs_export(request) -> str:
    q = request.GET.copy()
    q["export"] = "xlsx"
    # QueryDict.urlencode() preserva multi (categoria=Gato&categoria=Cachorro)
    return "?" + q.urlencode()


def _qs_get(q) -> str:
    """Serializa QueryDict preservando valores repetidos."""
    if not q:
        return "?"
    return "?" + q.urlencode()


def _periodo_filtros(request, padrao: str = "mes_atual") -> dict:
    return ru.parse_periodo_request(request, padrao=padrao)


def _extra_filtros_catalogo(facetas: dict, **extra) -> dict:
    out = {
        "categoria": list(facetas.get("categoria") or []),
        "categorias": facetas.get("categorias") or [],
        "subcategoria": list(facetas.get("subcategoria") or []),
        "subcategorias": facetas.get("subcategorias") or [],
        "subcategoria_2": list(facetas.get("subcategoria_2") or []),
        "subcategorias_2": facetas.get("subcategorias_2") or [],
        "subcategoria_3": list(facetas.get("subcategoria_3") or []),
        "subcategorias_3": facetas.get("subcategorias_3") or [],
        "subcategoria_4": list(facetas.get("subcategoria_4") or []),
        "subcategorias_4": facetas.get("subcategorias_4") or [],
    }
    out.update(extra)
    return out


def _subtitulo_catalogo(base: str, facetas: dict) -> str:
    partes = [base]
    for campo, rotulo in (
        ("categoria", None),
        ("subcategoria", "sub"),
        ("subcategoria_2", "sub2"),
        ("subcategoria_3", "sub3"),
        ("subcategoria_4", "sub4"),
    ):
        vals = ru._as_filtro_lista(facetas.get(campo))
        if not vals:
            continue
        texto = " + ".join(vals)
        partes.append(f"{rotulo} {texto}" if rotulo else texto)
    return " · ".join(partes)


def _kw_filtros_catalogo(facetas: dict) -> dict:
    return {
        "categoria": list(facetas.get("categoria") or []) or None,
        "subcategoria": list(facetas.get("subcategoria") or []) or None,
        "subcategoria_2": list(facetas.get("subcategoria_2") or []) or None,
        "subcategoria_3": list(facetas.get("subcategoria_3") or []) or None,
        "subcategoria_4": list(facetas.get("subcategoria_4") or []) or None,
    }



@require_GET
def relatorios_mais_vendidos(request):
    try:
        return _relatorios_mais_vendidos_impl(request)
    except Exception as exc:
        logger.exception("relatorios_mais_vendidos")
        if (request.GET.get("diag") or "") == "1":
            from django.http import HttpResponse

            return HttpResponse(
                f"ERRO relatorios_mais_vendidos: {type(exc).__name__}: {exc}",
                status=500,
                content_type="text/plain; charset=utf-8",
            )
        raise


def _relatorios_mais_vendidos_impl(request):
    f = _periodo_filtros(request)
    ordenar = (request.GET.get("ordenar") or "valor").strip().lower()
    if ordenar not in ("valor", "qtd"):
        ordenar = "valor"
    sentido = (request.GET.get("sentido") or "mais").strip().lower()
    if sentido not in ("mais", "menos"):
        sentido = "mais"
    facetas, rows_all = ru.facetas_categoria_sub(
        f["desde"],
        f["ate_dt"],
        ordenar=ordenar,
        sentido=sentido,
        **ru.filtros_catalogo_request(request),
    )
    rows = ru.limitar_ranking(rows_all, 100)
    headers = [
        "#", "Código GM", "Produto", "Categoria", "Sub", "Sub 2", "Sub 3", "Sub 4",
        "Qtd", "Ticket médio", "Total R$",
    ]
    sub_periodo = _subtitulo_catalogo(f["label"], facetas)
    if request.GET.get("export") == "xlsx":
        data = [
            [
                r["pos"], r["codigo"], r["nome"],
                r.get("categoria") or "", r.get("subcategoria") or "",
                r.get("subcategoria_2") or "", r.get("subcategoria_3") or "",
                r.get("subcategoria_4") or "",
                r["qtd"], r["ticket_medio"], r["valor"],
            ]
            for r in rows
        ]
        return ru.xlsx_http_response(
            "mais-vendidos.xlsx",
            ru.montar_xlsx("Produtos mais/menos vendidos", headers, data, subtitulo=sub_periodo),
        )
    total_qtd = round(sum(r["qtd"] for r in rows), 3)
    total_rs = round(sum(r["valor"] for r in rows), 2)
    return render(
        request,
        "produtos/relatorios_generico.html",
        {
            "titulo": "Produtos mais vendidos",
            "eyebrow": "Ranking",
            "subtitulo": "Ordene por valor ou quantidade. Marque várias categorias/subs · Atualizar.",
            "filtros": f,
            "extra_filtros": _extra_filtros_catalogo(facetas, ordenar=ordenar, sentido=sentido),
            "filtro_parcial": "mais_vendidos",
            "rel_help": "mais_vendidos",
            "headers": headers,
            "rows": [
                [
                    r["pos"], r["codigo"], r["nome"],
                    r.get("categoria") or "Sem categoria",
                    r.get("subcategoria") or "—",
                    r.get("subcategoria_2") or "—",
                    r.get("subcategoria_3") or "—",
                    r.get("subcategoria_4") or "—",
                    f'{r["qtd"]:.3f}'.rstrip("0").rstrip("."),
                    ru.fmt_brl(r["ticket_medio"]),
                    ru.fmt_brl(r["valor"]),
                ]
                for r in rows
            ],
            "totais": [f"Qtd {total_qtd}", ru.fmt_brl(total_rs)],
            "export_qs": _qs_export(request),
            "vazio_msg": "Nenhuma venda neste período (ou nestes filtros).",
        },
    )


@require_GET
def relatorios_vendas_grupo(request):
    f = _periodo_filtros(request)
    agrupar = (request.GET.get("agrupar") or "categoria").strip().lower()
    if agrupar not in (
        "categoria", "subcategoria", "subcategoria_2", "subcategoria_3", "subcategoria_4"
    ):
        agrupar = "categoria"
    rows, meta = ru.vendas_por_grupo_relatorio(
        f["desde"], f["ate_dt"], agrupar=agrupar, **ru.filtros_catalogo_request(request)
    )
    headers = ["#", meta.get("col_grupo") or "Grupo", "SKUs", "Qtd", "Total R$", "%"]
    sub_periodo = _subtitulo_catalogo(f["label"], meta)
    if agrupar != "categoria":
        sub_periodo = f"{sub_periodo} · por {meta.get('col_grupo') or agrupar}"
    if request.GET.get("export") == "xlsx":
        data = [
            [r["pos"], r["grupo"], r["skus"], r["qtd"], r["valor"], r["pct"]]
            for r in rows
        ]
        return ru.xlsx_http_response(
            "vendas-por-grupo.xlsx",
            ru.montar_xlsx("Vendas por grupo", headers, data, subtitulo=sub_periodo),
        )
    return render(
        request,
        "produtos/relatorios_generico.html",
        {
            "titulo": "Vendas por grupo",
            "eyebrow": "Categoria",
            "subtitulo": "Faturamento por categoria ou subcategoria (1–4). Combine os filtros.",
            "filtros": f,
            "filtro_parcial": "vendas_grupo",
            "rel_help": "vendas_grupo",
            "extra_filtros": _extra_filtros_catalogo(meta, agrupar=agrupar),
            "headers": headers,
            "rows": [
                [
                    r["pos"], r["grupo"], r["skus"],
                    f'{r["qtd"]:.3f}'.rstrip("0").rstrip("."),
                    ru.fmt_brl(r["valor"]),
                    f'{r["pct"]}%',
                ]
                for r in rows
            ],
            "totais": [ru.fmt_brl(sum(r["valor"] for r in rows))],
            "export_qs": _qs_export(request),
            "vazio_msg": "Nenhuma venda neste período (ou nestes filtros).",
        },
    )


@require_GET
def relatorios_vendas_marca(request):
    f = _periodo_filtros(request)
    ordenar = (request.GET.get("ordenar") or "valor").strip().lower()
    if ordenar not in ("valor", "qtd"):
        ordenar = "valor"
    rows = ru.vendas_por_marca(f["desde"], f["ate_dt"], ordenar=ordenar)
    headers = ["#", "Marca", "SKUs", "Qtd", "Total R$", "%"]
    if request.GET.get("export") == "xlsx":
        data = [
            [r["pos"], r["marca"], r["skus"], r["qtd"], r["valor"], r["pct"]]
            for r in rows
        ]
        return ru.xlsx_http_response(
            "vendas-por-marca.xlsx",
            ru.montar_xlsx("Vendas por marca", headers, data, subtitulo=f["label"]),
        )
    total_qtd = round(sum(r["qtd"] for r in rows), 3)
    total_rs = round(sum(r["valor"] for r in rows), 2)
    return render(
        request,
        "produtos/relatorios_generico.html",
        {
            "titulo": "Vendas por marca",
            "eyebrow": "Marcas",
            "subtitulo": "Ordene por valor total ou quantidade. Marca do cadastro Agro.",
            "filtros": f,
            "extra_filtros": {"ordenar": ordenar},
            "filtro_parcial": "vendas_marca",
            "rel_help": "vendas_marca",
            "headers": headers,
            "rows": [
                [
                    r["pos"],
                    r["marca"],
                    r["skus"],
                    f'{r["qtd"]:.3f}'.rstrip("0").rstrip("."),
                    ru.fmt_brl(r["valor"]),
                    f'{r["pct"]}%',
                ]
                for r in rows
            ],
            "totais": [f"Qtd {total_qtd}", ru.fmt_brl(total_rs)],
            "export_qs": _qs_export(request),
            "vazio_msg": "Nenhuma venda neste período.",
        },
    )


@require_GET
def relatorios_curva_abc(request):
    f = _periodo_filtros(request)
    todos = (request.GET.get("todos") or "").strip() in ("1", "sim", "true", "yes")
    filt = ru.filtros_catalogo_request(request)
    rows, meta = ru.curva_abc(f["desde"], f["ate_dt"], todos=todos, **filt)
    headers = [
        "#", "Classe", "Código GM", "Produto",
        "Categoria", "Sub", "Sub 2", "Sub 3", "Sub 4",
        "Total R$", "%", "% acum.",
    ]
    sub_periodo = _subtitulo_catalogo(f["label"], meta)
    if request.GET.get("export") == "xlsx":
        if not todos:
            rows, meta = ru.curva_abc(f["desde"], f["ate_dt"], todos=True, **filt)
            sub_periodo = _subtitulo_catalogo(f["label"], meta)
        data = [
            [
                r["pos"], r["classe"], r["codigo"], r["nome"],
                r.get("categoria") or "Sem categoria",
                r.get("subcategoria") or "",
                r.get("subcategoria_2") or "",
                r.get("subcategoria_3") or "",
                r.get("subcategoria_4") or "",
                r["valor"], r["pct"], r["pct_acum"],
            ]
            for r in rows
        ]
        return ru.xlsx_http_response(
            "curva-abc.xlsx",
            ru.montar_xlsx("Curva ABC", headers, data, subtitulo=sub_periodo),
        )
    q = request.GET.copy()
    q["todos"] = "1"
    ver_todos_qs = _qs_get(q)
    q_menos = request.GET.copy()
    if "todos" in q_menos:
        del q_menos["todos"]
    ver_menos_qs = _qs_get(q_menos) if q_menos else "?"
    recorte = meta.get("recorte") or "período"
    totais = [
        f"{meta['n_tela']} de {meta['n_total']} produtos",
        f"Total {recorte} {ru.fmt_brl(meta['total_periodo'])}",
    ]
    ativos = [
        meta.get(c) for c in (
            "categoria", "subcategoria", "subcategoria_2",
            "subcategoria_3", "subcategoria_4",
        ) if meta.get(c)
    ]
    if ativos:
        pct_txt = "% sobre o total do recorte filtrado."
    else:
        pct_txt = "% sobre o total do período."
    return render(
        request,
        "produtos/relatorios_generico.html",
        {
            "titulo": "Curva ABC",
            "eyebrow": "Classificação",
            "subtitulo": f"A ≈ 80% do faturamento · B ≈ 15% · C ≈ 5%. {pct_txt}",
            "filtros": f,
            "filtro_parcial": "curva_abc",
            "rel_help": "curva_abc",
            "extra_filtros": _extra_filtros_catalogo(meta),
            "headers": headers,
            "rows": [
                [
                    r["pos"], r["classe"], r["codigo"], r["nome"],
                    r.get("categoria") or "Sem categoria",
                    r.get("subcategoria") or "—",
                    r.get("subcategoria_2") or "—",
                    r.get("subcategoria_3") or "—",
                    r.get("subcategoria_4") or "—",
                    ru.fmt_brl(r["valor"]),
                    f'{r["pct"]}%',
                    f'{r["pct_acum"]}%',
                ]
                for r in rows
            ],
            "totais": totais,
            "export_qs": _qs_export(request),
            "vazio_msg": (
                "Nenhuma venda com estes filtros no período."
                if ativos
                else "Nenhuma venda neste período."
            ),
            "ver_mais": {
                "truncado": meta["truncado"],
                "todos": meta["todos"],
                "ver_todos_qs": ver_todos_qs,
                "ver_menos_qs": ver_menos_qs,
                "n_total": meta["n_total"],
                "n_tela": meta["n_tela"],
            },
        },
    )


@require_GET
def relatorios_giro_estoque(request):
    pacote = ru.giro_e_parado(limite=150)
    aba = (request.GET.get("aba") or "parado").strip().lower()
    if aba not in ("parado", "giro"):
        aba = "parado"
    ordenar = (request.GET.get("ordenar") or "").strip().lower()
    validos = (
        ("ultima_venda_desc", "ultima_venda_asc")
        if aba == "parado"
        else ("ultima_venda_desc", "ultima_venda_asc", "receita_desc", "qtd_desc")
    )
    if ordenar not in validos:
        ordenar = "ultima_venda_asc" if aba == "parado" else "ultima_venda_desc"
    if aba == "giro":
        headers = ["#", "Produto", "Última venda", "Qtd 30d", "Receita 30d"]
        rows_raw = pacote["giro"]
        if ordenar == "ultima_venda_asc":
            rows_raw = sorted(
                rows_raw,
                key=lambda r: (
                    r.get("ultima_venda") is None,
                    r.get("ultima_venda") or ru.timezone.now(),
                ),
            )
        elif ordenar == "qtd_desc":
            rows_raw = sorted(rows_raw, key=lambda r: r.get("qtd") or 0, reverse=True)
        elif ordenar == "receita_desc":
            rows_raw = sorted(rows_raw, key=lambda r: r.get("valor") or 0, reverse=True)
        else:
            rows_raw = sorted(
                rows_raw,
                key=lambda r: (
                    r.get("ultima_venda") is not None,
                    r.get("ultima_venda") or ru.timezone.now(),
                ),
                reverse=True,
            )
        for i, r in enumerate(rows_raw, start=1):
            r["pos"] = i
        xlsx_rows = [
            [r["pos"], r["nome"], ru.fmt_data_curta(r.get("ultima_venda")), r["qtd"], r["valor"]]
            for r in rows_raw
        ]
        display = [
            [r["pos"], r["nome"], ru.fmt_data_curta(r.get("ultima_venda")), r["qtd"], ru.fmt_brl(r["valor"])]
            for r in rows_raw
        ]
        nome = "giro-30d.xlsx"
        titulo = "Top giro (30 dias)"
    else:
        headers = ["#", "Produto", "Última venda", "Estoque", "Custo", "Valor parado"]
        rows_raw = pacote["parado"]
        if ordenar == "ultima_venda_desc":
            rows_raw = sorted(
                rows_raw,
                key=lambda r: (
                    r.get("ultima_venda") is not None,
                    r.get("ultima_venda") or ru.timezone.now(),
                ),
                reverse=True,
            )
        else:
            rows_raw = sorted(
                rows_raw,
                key=lambda r: (
                    r.get("ultima_venda") is None,
                    r.get("ultima_venda") or ru.timezone.now(),
                ),
            )
        for i, r in enumerate(rows_raw, start=1):
            r["pos"] = i
        xlsx_rows = [
            [
                r["pos"],
                r["nome"],
                ru.fmt_data_curta(r.get("ultima_venda")),
                r["estoque"],
                r["custo"],
                r["valor_parado"],
            ]
            for r in rows_raw
        ]
        display = [
            [
                r["pos"],
                r["nome"],
                ru.fmt_data_curta(r.get("ultima_venda")),
                r["estoque"],
                ru.fmt_brl(r["custo"]),
                ru.fmt_brl(r["valor_parado"]),
            ]
            for r in rows_raw
        ]
        nome = "estoque-parado.xlsx"
        titulo = "Estoque parado (90 dias sem venda)"
    if request.GET.get("export") == "xlsx":
        return ru.xlsx_http_response(
            nome,
            ru.montar_xlsx(titulo, headers, xlsx_rows),
        )
    return render(
        request,
        "produtos/relatorios_generico.html",
        {
            "titulo": "Giro e estoque parado",
            "eyebrow": "Estoque",
            "subtitulo": "Giro = mais vendidos em 30 dias. Parado = saldo com venda há mais de 90 dias.",
            "filtros": {"periodo": "", "de": "", "ate": "", "label": ""},
            "filtro_parcial": "giro",
            "rel_help": "giro",
            "extra_filtros": {"aba": aba, "ordenar": ordenar},
            "headers": headers,
            "rows": display,
            "totais": (
                [f"Valor parado {ru.fmt_brl(pacote['total_parado'])}"]
                if aba == "parado"
                else []
            ),
            "export_qs": _qs_export(request),
            "vazio_msg": "Nenhum item nesta lista.",
        },
    )


@require_GET
def relatorios_margem(request):
    f = _periodo_filtros(request)
    ordenar = (request.GET.get("ordenar") or "margem_rs").strip().lower()
    if ordenar not in ("margem_rs", "margem_pct"):
        ordenar = "margem_rs"
    facetas, rows_all = ru.facetas_categoria_sub(
        f["desde"], f["ate_dt"], **ru.filtros_catalogo_request(request)
    )
    rows = ru.margem_produtos(
        f["desde"], f["ate_dt"], ordenar=ordenar, limite=100, rows=rows_all
    )
    headers = [
        "#", "Código GM", "Produto", "Categoria", "Sub", "Sub 2", "Sub 3", "Sub 4",
        "Qtd", "Venda R$", "Custo R$", "Margem R$", "Margem %",
    ]
    sub_periodo = _subtitulo_catalogo(f["label"], facetas)
    if request.GET.get("export") == "xlsx":
        data = [
            [
                r["pos"], r["codigo"], r["nome"],
                r.get("categoria") or "", r.get("subcategoria") or "",
                r.get("subcategoria_2") or "", r.get("subcategoria_3") or "",
                r.get("subcategoria_4") or "",
                r["qtd"], r["valor"], r["custo_total"], r["margem_rs"], r["margem_pct"],
            ]
            for r in rows
        ]
        return ru.xlsx_http_response(
            "margem-produtos.xlsx",
            ru.montar_xlsx("Margem por produto", headers, data, subtitulo=sub_periodo),
        )
    return render(
        request,
        "produtos/relatorios_generico.html",
        {
            "titulo": "Margem por produto",
            "eyebrow": "Lucratividade",
            "subtitulo": "Venda líquida menos custo × quantidade. Combine categoria e sub 1–4.",
            "filtros": f,
            "filtro_parcial": "margem",
            "rel_help": "margem",
            "extra_filtros": _extra_filtros_catalogo(facetas, ordenar=ordenar),
            "headers": headers,
            "rows": [
                [
                    r["pos"], r["codigo"], r["nome"],
                    r.get("categoria") or "Sem categoria",
                    r.get("subcategoria") or "—",
                    r.get("subcategoria_2") or "—",
                    r.get("subcategoria_3") or "—",
                    r.get("subcategoria_4") or "—",
                    r["qtd"],
                    ru.fmt_brl(r["valor"]),
                    ru.fmt_brl(r["custo_total"]),
                    ru.fmt_brl(r["margem_rs"]),
                    f'{r["margem_pct"]}%',
                ]
                for r in rows
            ],
            "totais": [
                ru.fmt_brl(sum(r["valor"] for r in rows)),
                ru.fmt_brl(sum(r["margem_rs"] for r in rows)),
            ],
            "export_qs": _qs_export(request),
            "vazio_msg": "Nenhuma venda neste período (ou nestes filtros).",
        },
    )


@require_GET
def relatorios_vendas_operador(request):
    f = _periodo_filtros(request)
    rows = ru.vendas_por_operador(f["desde"], f["ate_dt"])
    headers = ["#", "Operador", "Vendas", "Ticket médio", "Frete", "Total R$"]
    if request.GET.get("export") == "xlsx":
        data = [
            [r["pos"], r["operador"], r["vendas"], r["ticket"], r["frete"], r["total"]]
            for r in rows
        ]
        return ru.xlsx_http_response(
            "vendas-operador.xlsx",
            ru.montar_xlsx("Vendas por operador", headers, data, subtitulo=f["label"]),
        )
    return render(
        request,
        "produtos/relatorios_generico.html",
        {
            "titulo": "Vendas por operador",
            "eyebrow": "Equipe",
            "subtitulo": "Totais por quem registrou a venda no PDV.",
            "filtros": f,
            "filtro_parcial": "periodo",
            "rel_help": "operador",
            "headers": headers,
            "rows": [
                [
                    r["pos"],
                    r["operador"],
                    r["vendas"],
                    ru.fmt_brl(r["ticket"]),
                    ru.fmt_brl(r["frete"]),
                    ru.fmt_brl(r["total"]),
                ]
                for r in rows
            ],
            "totais": [ru.fmt_brl(sum(r["total"] for r in rows))],
            "export_qs": _qs_export(request),
            "vazio_msg": "Nenhuma venda neste período.",
        },
    )


@require_GET
def relatorios_ranking_clientes(request):
    f = _periodo_filtros(request)
    ordenar = (request.GET.get("ordenar") or "valor").strip().lower()
    if ordenar not in ("valor", "qtd"):
        ordenar = "valor"
    rows = ru.ranking_clientes(f["desde"], f["ate_dt"], ordenar=ordenar, limite=100)
    headers = ["#", "Cliente", "Documento", "Vendas", "Ticket", "Total R$"]
    if request.GET.get("export") == "xlsx":
        data = [
            [r["pos"], r["cliente"], r["documento"], r["vendas"], r["ticket"], r["total"]]
            for r in rows
        ]
        return ru.xlsx_http_response(
            "ranking-clientes.xlsx",
            ru.montar_xlsx("Ranking de clientes", headers, data, subtitulo=f["label"]),
        )
    return render(
        request,
        "produtos/relatorios_generico.html",
        {
            "titulo": "Ranking de clientes",
            "eyebrow": "Relacionamento",
            "subtitulo": "Quem mais comprou no período (valor ou nº de vendas).",
            "filtros": f,
            "filtro_parcial": "clientes",
            "rel_help": "clientes",
            "extra_filtros": {"ordenar": ordenar},
            "headers": headers,
            "rows": [
                [
                    r["pos"],
                    r["cliente"],
                    r["documento"],
                    r["vendas"],
                    ru.fmt_brl(r["ticket"]),
                    ru.fmt_brl(r["total"]),
                ]
                for r in rows
            ],
            "totais": [ru.fmt_brl(sum(r["total"] for r in rows))],
            "export_qs": _qs_export(request),
            "vazio_msg": "Nenhuma venda com cliente neste período.",
        },
    )


@require_GET
def relatorios_comparativo(request):
    fa = _periodo_filtros(request, padrao="mes_atual")
    fb = ru.parse_periodo_b_request(request)
    dados = ru.comparativo_periodos(fa["desde"], fa["ate_dt"], fb["desde"], fb["ate_dt"])
    headers = ["Indicador", "Período A", "Período B", "Variação %"]
    labels = [
        ("Vendas (cupons)", "vendas", False),
        ("Faturamento", "faturamento", True),
        ("Itens (qtd)", "itens", False),
        ("Itens (R$)", "itens_rs", True),
    ]
    rows_disp = []
    rows_xlsx = []
    for label, key, money in labels:
        va = dados["a"][key]
        vb = dados["b"][key]
        var = dados["var"][key]
        var_s = "—" if var is None else f"{var}%"
        rows_disp.append(
            [
                label,
                ru.fmt_brl(va) if money else va,
                ru.fmt_brl(vb) if money else vb,
                var_s,
            ]
        )
        rows_xlsx.append([label, va, vb, var if var is not None else ""])
    if request.GET.get("export") == "xlsx":
        return ru.xlsx_http_response(
            "comparativo.xlsx",
            ru.montar_xlsx(
                "Comparativo de períodos",
                headers,
                rows_xlsx,
                subtitulo=f"A: {fa['label']} | B: {fb['label']}",
            ),
        )
    return render(
        request,
        "produtos/relatorios_generico.html",
        {
            "titulo": "Comparativo de períodos",
            "eyebrow": "Tendência",
            "subtitulo": "Período A (filtro de cima) versus período B (mês passado ou custom).",
            "filtros": fa,
            "filtro_parcial": "comparativo",
            "rel_help": "comparativo",
            "extra_filtros": fb,
            "headers": headers,
            "rows": rows_disp,
            "totais": [],
            "export_qs": _qs_export(request),
            "vazio_msg": "",
        },
    )


@require_GET
def relatorios_formas_pagamento(request):
    f = _periodo_filtros(request)
    rows = ru.formas_pagamento(f["desde"], f["ate_dt"])
    headers = ["#", "Forma", "Lançamentos", "Total R$", "%"]
    if request.GET.get("export") == "xlsx":
        data = [
            [r["pos"], r["forma"], r["vendas"], r["total"], r["pct"]] for r in rows
        ]
        return ru.xlsx_http_response(
            "formas-pagamento.xlsx",
            ru.montar_xlsx("Formas de pagamento", headers, data, subtitulo=f["label"]),
        )
    return render(
        request,
        "produtos/relatorios_generico.html",
        {
            "titulo": "Formas de pagamento",
            "eyebrow": "Caixa",
            "subtitulo": "Soma por forma (inclui vendas com mais de um pagamento).",
            "filtros": f,
            "filtro_parcial": "periodo",
            "rel_help": "formas_pagamento",
            "headers": headers,
            "rows": [
                [
                    r["pos"],
                    r["forma"],
                    r["vendas"],
                    ru.fmt_brl(r["total"]),
                    f'{r["pct"]}%',
                ]
                for r in rows
            ],
            "totais": [ru.fmt_brl(sum(r["total"] for r in rows))],
            "export_qs": _qs_export(request),
            "vazio_msg": "Nenhuma venda neste período.",
        },
    )


@require_GET
def relatorios_ruptura(request):
    dias = request.GET.get("dias") or "30"
    try:
        dias_i = max(7, min(180, int(dias)))
    except (TypeError, ValueError):
        dias_i = 30
    rows = ru.ruptura_estoque(dias_venda=dias_i, limite=150)
    headers = ["#", "Código GM", "Produto", "Categoria", "Estoque C+V"]
    if request.GET.get("export") == "xlsx":
        data = [
            [r["pos"], r["codigo"], r["nome"], r["categoria"], r["estoque"]]
            for r in rows
        ]
        return ru.xlsx_http_response(
            "ruptura.xlsx",
            ru.montar_xlsx(
                "Ruptura de estoque",
                headers,
                data,
                subtitulo=f"Venda nos últimos {dias_i} dias e saldo zerado",
            ),
        )
    return render(
        request,
        "produtos/relatorios_generico.html",
        {
            "titulo": "Ruptura de estoque",
            "eyebrow": "Perdas / falta",
            "subtitulo": "Vendeu recentemente e estoque C+V está zerado. Validade: use o relatório de validade.",
            "filtros": {"periodo": "", "de": "", "ate": "", "label": f"{dias_i} dias"},
            "filtro_parcial": "ruptura",
            "rel_help": "ruptura",
            "extra_filtros": {"dias": str(dias_i)},
            "headers": headers,
            "rows": [
                [r["pos"], r["codigo"], r["nome"], r["categoria"], r["estoque"]]
                for r in rows
            ],
            "totais": [f"{len(rows)} produtos"],
            "export_qs": _qs_export(request),
            "vazio_msg": "Nenhuma ruptura encontrada.",
            "link_extra": {"url_name": "relatorios_validade", "label": "Abrir validade"},
        },
    )


@require_GET
def relatorios_comissao(request):
    f = _periodo_filtros(request)
    rows = ru.comissao_estimada(f["desde"], f["ate_dt"], limite=200)
    headers = [
        "#",
        "Código GM",
        "Produto",
        "Qtd",
        "Venda R$",
        "Comissão %",
        "Comissão R$/un",
        "Estimada R$",
    ]
    if request.GET.get("export") == "xlsx":
        data = [
            [
                r["pos"],
                r["codigo"],
                r["nome"],
                r["qtd"],
                r["valor"],
                r["comissao_pct"],
                r["comissao_rs_unit"],
                r["comissao_estimada"],
            ]
            for r in rows
        ]
        return ru.xlsx_http_response(
            "comissao.xlsx",
            ru.montar_xlsx("Comissão estimada", headers, data, subtitulo=f["label"]),
        )
    return render(
        request,
        "produtos/relatorios_generico.html",
        {
            "titulo": "Comissão estimada",
            "eyebrow": "Equipe",
            "subtitulo": "Usa % e R$ de comissão do cadastro (quando existir). Sem meta de vendedor.",
            "filtros": f,
            "filtro_parcial": "periodo",
            "rel_help": "comissao",
            "headers": headers,
            "rows": [
                [
                    r["pos"],
                    r["codigo"],
                    r["nome"],
                    r["qtd"],
                    ru.fmt_brl(r["valor"]),
                    r["comissao_pct"] if r["comissao_pct"] != "" else "—",
                    r["comissao_rs_unit"] if r["comissao_rs_unit"] != "" else "—",
                    ru.fmt_brl(r["comissao_estimada"]),
                ]
                for r in rows
            ],
            "totais": [ru.fmt_brl(sum(r["comissao_estimada"] for r in rows))],
            "export_qs": _qs_export(request),
            "vazio_msg": "Nenhuma venda neste período.",
        },
    )


def _filtros_inv_label(f: dict) -> str:
    partes = []
    dep = f.get("deposito") or "ambos"
    if dep == "centro":
        partes.append("Depósito Centro")
    elif dep == "vila":
        partes.append("Depósito Vila")
    else:
        partes.append("Centro + Vila")
    if f.get("categoria"):
        partes.append(f"Cat. {f['categoria']}")
    if f.get("marca"):
        partes.append(f"Marca {f['marca']}")
    if f.get("q"):
        partes.append(f"Busca «{f['q']}»")
    ativos = f.get("ativos") or "ativos"
    if ativos == "inativos":
        partes.append("Só inativos")
    elif ativos == "todos":
        partes.append("Ativos e inativos")
    if f.get("so_saldo"):
        partes.append("Só com saldo")
    else:
        partes.append("Com e sem saldo")
    return " · ".join(partes)


def _pacote_inventario(request, *, so_saldo_override: bool | None = None):
    from produtos import relatorios_estoque_util as reu

    f = reu.parse_filtros_inventario(request.GET)
    if so_saldo_override is not None:
        f["so_saldo"] = so_saldo_override
    pacote = reu.coletar_linhas_inventario(
        deposito=f["deposito"],
        categoria=f["categoria"],
        marca=f["marca"],
        ativos=f["ativos"],
        so_saldo=f["so_saldo"],
        q=f["q"],
    )
    return f, pacote


def _ver_mais_inv(request, n_total: int, n_tela: int):
    from urllib.parse import urlencode

    from produtos.relatorios_estoque_util import TELA_LIMITE

    if n_total <= TELA_LIMITE:
        return None
    q = request.GET.copy()
    todos = (request.GET.get("todos") or "") == "1"
    if not todos:
        q["todos"] = "1"
        return {
            "truncado": True,
            "n_total": n_total,
            "n_tela": n_tela,
            "ver_todos_qs": "?" + urlencode(q, doseq=True),
        }
    q.pop("todos", None)
    return {
        "truncado": False,
        "todos": True,
        "n_total": n_total,
        "n_tela": n_tela,
        "ver_menos_qs": "?" + urlencode(q, doseq=True),
    }


def _slice_tela(request, linhas: list) -> tuple[list, dict | None]:
    from produtos.relatorios_estoque_util import TELA_LIMITE

    todos = (request.GET.get("todos") or "") == "1"
    n = len(linhas)
    if todos or n <= TELA_LIMITE:
        return linhas, _ver_mais_inv(request, n, n)
    slice_rows = linhas[:TELA_LIMITE]
    return slice_rows, _ver_mais_inv(request, n, len(slice_rows))


@require_GET
def relatorios_inventario(request):
    from produtos import relatorios_estoque_util as reu

    f, pacote = _pacote_inventario(request)
    linhas = pacote["linhas"]
    t = pacote["totais"]
    headers = [
        "#",
        "Código GM",
        "Produto",
        "Cód. sistema",
        "Barras",
        "Categoria",
        "Marca",
        "UN",
        "Saldo C",
        "Saldo V",
        "Total",
        "Mín C",
        "Mín V",
        "Custo",
        "Valor custo",
        "P.venda",
        "Valor venda",
        "Ativo",
    ]

    def _row_data(r):
        return [
            r["pos"],
            r["codigo_gm"],
            r["nome"],
            r["codigo_sistema"],
            r["codigo_barras"],
            r["categoria"],
            r["marca"],
            r["unidade"],
            r["saldo_centro"],
            r["saldo_vila"],
            r["saldo_total"],
            r["estoque_min_centro"] if r["estoque_min_centro"] is not None else "",
            r["estoque_min_vila"] if r["estoque_min_vila"] is not None else "",
            r["custo"],
            r["valor_custo"],
            r["preco_venda"],
            r["valor_venda"],
            "Sim" if r["ativo"] else "Não",
        ]

    if request.GET.get("export") == "xlsx":
        return ru.xlsx_http_response(
            "inventario-valorizado.xlsx",
            ru.montar_xlsx(
                "Inventário valorizado",
                headers,
                [_row_data(r) for r in linhas],
                subtitulo=_filtros_inv_label(f),
            ),
        )

    tela, ver_mais = _slice_tela(request, linhas)
    return render(
        request,
        "produtos/relatorios_generico.html",
        {
            "titulo": "Inventário valorizado",
            "eyebrow": "Estoque · cadastro",
            "subtitulo": "Saldo Agro + valor no custo e no preço de venda. Excel traz todas as linhas do filtro.",
            "filtros": {"periodo": "", "de": "", "ate": "", "label": _filtros_inv_label(f)},
            "filtro_parcial": "inventario",
            "rel_help": "inventario",
            "extra_filtros": {
                **f,
                "categorias": pacote["categorias"],
                "marcas": pacote["marcas"],
                "so_saldo": "1" if f["so_saldo"] else "0",
            },
            "headers": headers,
            "rows": [
                [
                    r["pos"],
                    r["codigo_gm"],
                    r["nome"],
                    r["codigo_sistema"],
                    r["codigo_barras"] or "—",
                    r["categoria"],
                    r["marca"],
                    r["unidade"],
                    reu.fmt_num(r["saldo_centro"], 3),
                    reu.fmt_num(r["saldo_vila"], 3),
                    reu.fmt_num(r["saldo_total"], 3),
                    reu.fmt_num(r["estoque_min_centro"], 3),
                    reu.fmt_num(r["estoque_min_vila"], 3),
                    ru.fmt_brl(r["custo"]),
                    ru.fmt_brl(r["valor_custo"]),
                    ru.fmt_brl(r["preco_venda"]),
                    ru.fmt_brl(r["valor_venda"]),
                    "Sim" if r["ativo"] else "Não",
                ]
                for r in tela
            ],
            "totais": [
                f"{t['skus']} SKUs",
                f"{t['com_saldo']} com saldo",
                f"Estoque (custo) {ru.fmt_brl(t['valor_custo'])}",
                f"Potencial (venda) {ru.fmt_brl(t['valor_venda'])}",
                f"Margem pot. {ru.fmt_brl(t['margem_rs'])} ({t['margem_pct']}%)",
            ],
            "export_qs": _qs_export(request),
            "vazio_msg": "Nenhum produto no filtro.",
            "ver_mais": ver_mais,
        },
    )


@require_GET
def relatorios_estoque_min_max(request):
    from produtos import relatorios_estoque_util as reu

    f, pacote = _pacote_inventario(request, so_saldo_override=False)
    modo = (request.GET.get("modo") or "abaixo").strip().lower()
    if modo not in ("abaixo", "acima", "ambos"):
        modo = "abaixo"
    linhas = reu.inventario_min_max(pacote, modo=modo)
    headers = [
        "#",
        "Código GM",
        "Produto",
        "Saldo C",
        "Mín C",
        "Máx C",
        "Saldo V",
        "Mín V",
        "Máx V",
        "Alerta",
        "Valor custo",
    ]
    if request.GET.get("export") == "xlsx":
        data = [
            [
                r["pos"],
                r["codigo_gm"],
                r["nome"],
                r["saldo_centro"],
                r.get("min_centro") if r.get("min_centro") is not None else "",
                r.get("max_centro") if r.get("max_centro") is not None else "",
                r["saldo_vila"],
                r.get("min_vila") if r.get("min_vila") is not None else "",
                r.get("max_vila") if r.get("max_vila") is not None else "",
                r["alerta"],
                r["valor_custo"],
            ]
            for r in linhas
        ]
        return ru.xlsx_http_response(
            "estoque-min-max.xlsx",
            ru.montar_xlsx(
                "Estoque mínimo / máximo",
                headers,
                data,
                subtitulo=_filtros_inv_label(f) + f" · modo {modo}",
            ),
        )
    tela, ver_mais = _slice_tela(request, linhas)
    return render(
        request,
        "produtos/relatorios_generico.html",
        {
            "titulo": "Estoque mínimo / máximo",
            "eyebrow": "Estoque · meta",
            "subtitulo": "Itens fora da meta cadastrada (mínimo ou máximo) no Centro e/ou Vila.",
            "filtros": {
                "periodo": "",
                "de": "",
                "ate": "",
                "label": _filtros_inv_label(f) + f" · {modo}",
            },
            "filtro_parcial": "estoque_min_max",
            "rel_help": "estoque_min_max",
            "extra_filtros": {
                **f,
                "modo": modo,
                "categorias": pacote["categorias"],
                "marcas": pacote["marcas"],
                "so_saldo": "0",
            },
            "headers": headers,
            "rows": [
                [
                    r["pos"],
                    r["codigo_gm"],
                    r["nome"],
                    reu.fmt_num(r["saldo_centro"], 3),
                    reu.fmt_num(r.get("min_centro"), 3),
                    reu.fmt_num(r.get("max_centro"), 3),
                    reu.fmt_num(r["saldo_vila"], 3),
                    reu.fmt_num(r.get("min_vila"), 3),
                    reu.fmt_num(r.get("max_vila"), 3),
                    r["alerta"],
                    ru.fmt_brl(r["valor_custo"]),
                ]
                for r in tela
            ],
            "totais": [f"{len(linhas)} produtos fora da meta"],
            "export_qs": _qs_export(request),
            "vazio_msg": "Nenhum produto fora da meta (ou meta não cadastrada).",
            "ver_mais": ver_mais,
        },
    )


@require_GET
def relatorios_estoque_resumo(request):
    from produtos import relatorios_estoque_util as reu

    f, pacote = _pacote_inventario(request)
    agrupar = (request.GET.get("agrupar") or "categoria").strip().lower()
    if agrupar not in ("categoria", "marca", "unidade"):
        agrupar = "categoria"
    linhas = reu.inventario_resumo(pacote, agrupar=agrupar)
    label_g = {"categoria": "Categoria", "marca": "Marca", "unidade": "Unidade"}[agrupar]
    headers = ["#", label_g, "SKUs", "Saldo", "Valor custo", "Valor venda", "% venda"]
    if request.GET.get("export") == "xlsx":
        data = [
            [
                r["pos"],
                r["grupo"],
                r["skus"],
                r["saldo"],
                r["valor_custo"],
                r["valor_venda"],
                r["pct"],
            ]
            for r in linhas
        ]
        return ru.xlsx_http_response(
            "estoque-resumo.xlsx",
            ru.montar_xlsx(
                f"Resumo estoque por {label_g.lower()}",
                headers,
                data,
                subtitulo=_filtros_inv_label(f),
            ),
        )
    t = pacote["totais"]
    return render(
        request,
        "produtos/relatorios_generico.html",
        {
            "titulo": "Resumo do estoque",
            "eyebrow": "Estoque · agregado",
            "subtitulo": "Totais de saldo e valor agrupados por categoria, marca ou unidade.",
            "filtros": {"periodo": "", "de": "", "ate": "", "label": _filtros_inv_label(f)},
            "filtro_parcial": "estoque_resumo",
            "rel_help": "estoque_resumo",
            "extra_filtros": {
                **f,
                "agrupar": agrupar,
                "categorias": pacote["categorias"],
                "marcas": pacote["marcas"],
                "so_saldo": "1" if f["so_saldo"] else "0",
            },
            "headers": headers,
            "rows": [
                [
                    r["pos"],
                    r["grupo"],
                    r["skus"],
                    reu.fmt_num(r["saldo"], 3),
                    ru.fmt_brl(r["valor_custo"]),
                    ru.fmt_brl(r["valor_venda"]),
                    f'{r["pct"]}%',
                ]
                for r in linhas
            ],
            "totais": [
                f"{len(linhas)} grupos",
                f"Estoque (custo) {ru.fmt_brl(t['valor_custo'])}",
                f"Potencial (venda) {ru.fmt_brl(t['valor_venda'])}",
            ],
            "export_qs": _qs_export(request),
            "vazio_msg": "Nada para agregar no filtro.",
        },
    )


@require_GET
def relatorios_estoque_sem_custo(request):
    from produtos import relatorios_estoque_util as reu

    f, pacote = _pacote_inventario(request, so_saldo_override=True)
    linhas = reu.inventario_sem_custo(pacote)
    headers = [
        "#",
        "Código GM",
        "Produto",
        "Categoria",
        "Saldo C",
        "Saldo V",
        "Total",
        "P.venda",
        "Valor venda",
    ]
    if request.GET.get("export") == "xlsx":
        data = [
            [
                r["pos"],
                r["codigo_gm"],
                r["nome"],
                r["categoria"],
                r["saldo_centro"],
                r["saldo_vila"],
                r["saldo_total"],
                r["preco_venda"],
                r["valor_venda"],
            ]
            for r in linhas
        ]
        return ru.xlsx_http_response(
            "estoque-sem-custo.xlsx",
            ru.montar_xlsx(
                "Estoque sem custo cadastrado",
                headers,
                data,
                subtitulo=_filtros_inv_label(f),
            ),
        )
    tela, ver_mais = _slice_tela(request, linhas)
    return render(
        request,
        "produtos/relatorios_generico.html",
        {
            "titulo": "Estoque sem custo",
            "eyebrow": "Cadastro · buracos",
            "subtitulo": "Tem saldo na loja e o custo está zerado — inventário a custo fica incompleto.",
            "filtros": {"periodo": "", "de": "", "ate": "", "label": _filtros_inv_label(f)},
            "filtro_parcial": "estoque_sem_custo",
            "rel_help": "estoque_sem_custo",
            "extra_filtros": {
                **f,
                "categorias": pacote["categorias"],
                "marcas": pacote["marcas"],
                "so_saldo": "1",
            },
            "headers": headers,
            "rows": [
                [
                    r["pos"],
                    r["codigo_gm"],
                    r["nome"],
                    r["categoria"],
                    reu.fmt_num(r["saldo_centro"], 3),
                    reu.fmt_num(r["saldo_vila"], 3),
                    reu.fmt_num(r["saldo_total"], 3),
                    ru.fmt_brl(r["preco_venda"]),
                    ru.fmt_brl(r["valor_venda"]),
                ]
                for r in tela
            ],
            "totais": [f"{len(linhas)} produtos com saldo e sem custo"],
            "export_qs": _qs_export(request),
            "vazio_msg": "Nenhum buraco de custo com saldo.",
            "ver_mais": ver_mais,
        },
    )


@require_GET
def relatorios_estoque_zerados(request):
    from produtos import relatorios_estoque_util as reu

    f, pacote = _pacote_inventario(request, so_saldo_override=False)
    modo = (request.GET.get("modo") or "zerados").strip().lower()
    if modo not in ("zerados", "negativos", "ambos"):
        modo = "zerados"
    linhas = reu.inventario_zerados(pacote, modo=modo)
    headers = [
        "#",
        "Código GM",
        "Produto",
        "Categoria",
        "Saldo C",
        "Saldo V",
        "Total",
        "Custo",
        "P.venda",
        "Ativo",
    ]
    if request.GET.get("export") == "xlsx":
        data = [
            [
                r["pos"],
                r["codigo_gm"],
                r["nome"],
                r["categoria"],
                r["saldo_centro"],
                r["saldo_vila"],
                r["saldo_relevante"],
                r["custo"],
                r["preco_venda"],
                "Sim" if r["ativo"] else "Não",
            ]
            for r in linhas
        ]
        return ru.xlsx_http_response(
            "estoque-zerados.xlsx",
            ru.montar_xlsx(
                "Estoque zerado / negativo",
                headers,
                data,
                subtitulo=_filtros_inv_label(f) + f" · {modo}",
            ),
        )
    tela, ver_mais = _slice_tela(request, linhas)
    return render(
        request,
        "produtos/relatorios_generico.html",
        {
            "titulo": "Zerados e negativos",
            "eyebrow": "Estoque · conferência",
            "subtitulo": "Saldo do depósito filtrado zerado ou negativo (Centro, Vila ou C+V).",
            "filtros": {
                "periodo": "",
                "de": "",
                "ate": "",
                "label": _filtros_inv_label(f) + f" · {modo}",
            },
            "filtro_parcial": "estoque_zerados",
            "rel_help": "estoque_zerados",
            "extra_filtros": {
                **f,
                "modo": modo,
                "categorias": pacote["categorias"],
                "marcas": pacote["marcas"],
                "so_saldo": "0",
            },
            "headers": headers,
            "rows": [
                [
                    r["pos"],
                    r["codigo_gm"],
                    r["nome"],
                    r["categoria"],
                    reu.fmt_num(r["saldo_centro"], 3),
                    reu.fmt_num(r["saldo_vila"], 3),
                    reu.fmt_num(r["saldo_relevante"], 3),
                    ru.fmt_brl(r["custo"]),
                    ru.fmt_brl(r["preco_venda"]),
                    "Sim" if r["ativo"] else "Não",
                ]
                for r in tela
            ],
            "totais": [f"{len(linhas)} produtos"],
            "export_qs": _qs_export(request),
            "vazio_msg": "Nenhum zerado/negativo no filtro.",
            "ver_mais": ver_mais,
        },
    )
