"""Views da Central de Relatórios (além de validade/etiquetas)."""
from __future__ import annotations

import logging
from urllib.parse import urlencode

from django.shortcuts import render
from django.views.decorators.http import require_GET

from produtos import relatorios_vendas_util as ru

logger = logging.getLogger(__name__)


def _qs_export(request) -> str:
    q = request.GET.copy()
    q["export"] = "xlsx"
    return "?" + urlencode(q, doseq=True)


def _periodo_filtros(request, padrao: str = "mes_atual") -> dict:
    return ru.parse_periodo_request(request, padrao=padrao)


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
    rows = ru.ranking_produtos(
        f["desde"], f["ate_dt"], ordenar=ordenar, sentido=sentido, limite=100
    )
    headers = ["#", "Código GM", "Produto", "Qtd", "Ticket médio", "Total R$"]
    if request.GET.get("export") == "xlsx":
        data = [
            [r["pos"], r["codigo"], r["nome"], r["qtd"], r["ticket_medio"], r["valor"]]
            for r in rows
        ]
        return ru.xlsx_http_response(
            "mais-vendidos.xlsx",
            ru.montar_xlsx(
                "Produtos mais/menos vendidos",
                headers,
                data,
                subtitulo=f["label"],
            ),
        )
    total_qtd = round(sum(r["qtd"] for r in rows), 3)
    total_rs = round(sum(r["valor"] for r in rows), 2)
    return render(
        request,
        "produtos/relatorios_generico.html",
        {
            "titulo": "Produtos mais vendidos",
            "eyebrow": "Ranking",
            "subtitulo": "Ordene por valor total ou quantidade. Período e sentido (mais/menos).",
            "filtros": f,
            "extra_filtros": {
                "ordenar": ordenar,
                "sentido": sentido,
            },
            "filtro_parcial": "mais_vendidos",
            "rel_help": "mais_vendidos",
            "headers": headers,
            "rows": [
                [
                    r["pos"],
                    r["codigo"],
                    r["nome"],
                    f'{r["qtd"]:.3f}'.rstrip("0").rstrip("."),
                    ru.fmt_brl(r["ticket_medio"]),
                    ru.fmt_brl(r["valor"]),
                ]
                for r in rows
            ],
            "totais": [f"Qtd {total_qtd}", ru.fmt_brl(total_rs)],
            "export_qs": _qs_export(request),
            "vazio_msg": "Nenhuma venda neste período.",
        },
    )


@require_GET
def relatorios_vendas_grupo(request):
    f = _periodo_filtros(request)
    rows = ru.vendas_por_grupo(f["desde"], f["ate_dt"])
    headers = ["#", "Grupo / categoria", "SKUs", "Qtd", "Total R$", "%"]
    if request.GET.get("export") == "xlsx":
        data = [
            [r["pos"], r["grupo"], r["skus"], r["qtd"], r["valor"], r["pct"]]
            for r in rows
        ]
        return ru.xlsx_http_response(
            "vendas-por-grupo.xlsx",
            ru.montar_xlsx("Vendas por grupo", headers, data, subtitulo=f["label"]),
        )
    return render(
        request,
        "produtos/relatorios_generico.html",
        {
            "titulo": "Vendas por grupo",
            "eyebrow": "Categoria",
            "subtitulo": "Faturamento e quantidade por categoria do cadastro.",
            "filtros": f,
            "filtro_parcial": "periodo",
            "rel_help": "vendas_grupo",
            "headers": headers,
            "rows": [
                [
                    r["pos"],
                    r["grupo"],
                    r["skus"],
                    f'{r["qtd"]:.3f}'.rstrip("0").rstrip("."),
                    ru.fmt_brl(r["valor"]),
                    f'{r["pct"]}%',
                ]
                for r in rows
            ],
            "totais": [ru.fmt_brl(sum(r["valor"] for r in rows))],
            "export_qs": _qs_export(request),
            "vazio_msg": "Nenhuma venda neste período.",
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
    categoria = (request.GET.get("categoria") or "").strip()
    rows, meta = ru.curva_abc(
        f["desde"], f["ate_dt"], todos=todos, categoria=categoria or None
    )
    headers = [
        "#",
        "Classe",
        "Código GM",
        "Produto",
        "Categoria",
        "Total R$",
        "%",
        "% acum.",
    ]
    cat_label = meta.get("categoria") or ""
    sub_periodo = f["label"]
    if cat_label:
        sub_periodo = f"{sub_periodo} · categoria {cat_label}"
    if request.GET.get("export") == "xlsx":
        # Excel sempre completo (respeita filtro de categoria)
        if not todos:
            rows, meta = ru.curva_abc(
                f["desde"], f["ate_dt"], todos=True, categoria=categoria or None
            )
            cat_label = meta.get("categoria") or cat_label
            if cat_label:
                sub_periodo = f'{f["label"]} · categoria {cat_label}'
        data = [
            [
                r["pos"],
                r["classe"],
                r["codigo"],
                r["nome"],
                r.get("categoria") or "Sem categoria",
                r["valor"],
                r["pct"],
                r["pct_acum"],
            ]
            for r in rows
        ]
        return ru.xlsx_http_response(
            "curva-abc.xlsx",
            ru.montar_xlsx("Curva ABC", headers, data, subtitulo=sub_periodo),
        )
    q = request.GET.copy()
    q["todos"] = "1"
    ver_todos_qs = "?" + urlencode(q, doseq=True)
    q_menos = request.GET.copy()
    if "todos" in q_menos:
        del q_menos["todos"]
    ver_menos_qs = "?" + urlencode(q_menos, doseq=True) if q_menos else "?"
    totais = [
        f"{meta['n_tela']} de {meta['n_total']} produtos",
        f"Total {'categoria' if cat_label else 'período'} {ru.fmt_brl(meta['total_periodo'])}",
    ]
    subtitulo = (
        "A ≈ 80% do faturamento · B ≈ 15% · C ≈ 5%. "
        + (
            f"% sobre o total da categoria «{cat_label}»."
            if cat_label
            else "% sobre o total do período."
        )
    )
    return render(
        request,
        "produtos/relatorios_generico.html",
        {
            "titulo": "Curva ABC",
            "eyebrow": "Classificação",
            "subtitulo": subtitulo,
            "filtros": f,
            "filtro_parcial": "curva_abc",
            "rel_help": "curva_abc",
            "extra_filtros": {
                "categoria": cat_label,
                "categorias": meta.get("categorias") or [],
            },
            "headers": headers,
            "rows": [
                [
                    r["pos"],
                    r["classe"],
                    r["codigo"],
                    r["nome"],
                    r.get("categoria") or "Sem categoria",
                    ru.fmt_brl(r["valor"]),
                    f'{r["pct"]}%',
                    f'{r["pct_acum"]}%',
                ]
                for r in rows
            ],
            "totais": totais,
            "export_qs": _qs_export(request),
            "vazio_msg": (
                "Nenhuma venda nesta categoria no período."
                if cat_label
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
    rows = ru.margem_produtos(f["desde"], f["ate_dt"], ordenar=ordenar, limite=100)
    headers = [
        "#",
        "Código GM",
        "Produto",
        "Qtd",
        "Venda R$",
        "Custo R$",
        "Margem R$",
        "Margem %",
    ]
    if request.GET.get("export") == "xlsx":
        data = [
            [
                r["pos"],
                r["codigo"],
                r["nome"],
                r["qtd"],
                r["valor"],
                r["custo_total"],
                r["margem_rs"],
                r["margem_pct"],
            ]
            for r in rows
        ]
        return ru.xlsx_http_response(
            "margem-produtos.xlsx",
            ru.montar_xlsx("Margem por produto", headers, data, subtitulo=f["label"]),
        )
    return render(
        request,
        "produtos/relatorios_generico.html",
        {
            "titulo": "Margem por produto",
            "eyebrow": "Lucratividade",
            "subtitulo": "Venda líquida menos custo do cadastro × quantidade.",
            "filtros": f,
            "filtro_parcial": "margem",
            "rel_help": "margem",
            "extra_filtros": {"ordenar": ordenar},
            "headers": headers,
            "rows": [
                [
                    r["pos"],
                    r["codigo"],
                    r["nome"],
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
            "vazio_msg": "Nenhuma venda neste período.",
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
