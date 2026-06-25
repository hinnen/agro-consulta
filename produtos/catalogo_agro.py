"""Catálogo PostgreSQL (``Produto``) — ``AGRO_FONTE_CATALOGO=agro_pg``."""
from __future__ import annotations

import secrets
from decimal import Decimal

from django.db.models import Q

from produtos.models import Produto, ProdutoGestaoOverlayAgro

_SORT_MAP = {
    "nome": "nome",
    "marca": "marca",
    "unidade": "unidade",
    "categoria": "categoria",
    "subcategoria": "subcategoria",
    "preco_custo": "custo",
    "preco_venda": "preco_venda",
}


def _dec(v) -> float:
    try:
        return round(float(v or 0), 2)
    except (TypeError, ValueError):
        return 0.0


def _dec_opt(v) -> Decimal | None:
    if v is None or str(v).strip() == "":
        return None
    try:
        return Decimal(str(v).replace(",", ".").strip()).quantize(Decimal("0.01"))
    except Exception:
        return None


def _overlay_mapa_por_ids(ids: list[str]) -> dict[str, ProdutoGestaoOverlayAgro]:
    ids_u = [str(x)[:64] for x in ids if x]
    if not ids_u:
        return {}
    return {
        o.produto_externo_id: o
        for o in ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id__in=ids_u)
    }


def _aplicar_overlay_em_row(row: dict, ov: ProdutoGestaoOverlayAgro | None) -> dict:
    if not ov:
        return row
    from produtos.views import _aplicar_produto_gestao_overlay_em_dict

    _aplicar_produto_gestao_overlay_em_dict(row, ov)
    return row


def produto_agro_para_row(p: Produto, ov: ProdutoGestaoOverlayAgro | None = None) -> dict:
    pid = (p.produto_externo_id or p.erp_produto_id or str(p.pk)).strip()
    row = {
        "id": pid,
        "nome": (p.nome or "").strip(),
        "marca": (p.marca or "").strip(),
        "codigo": (p.codigo_interno or "").strip(),
        "codigo_nfe": (p.codigo_nfe or p.codigo_interno or "").strip(),
        "codigo_barras": (p.codigo_barras or "").strip(),
        "preco_venda": _dec(p.preco_venda),
        "preco_custo": _dec(p.custo),
        "categoria": (p.categoria or "").strip(),
        "subcategoria": (p.subcategoria or "").strip(),
        "subcategoria_2": (p.subcategoria_2 or "").strip(),
        "subcategoria_3": (p.subcategoria_3 or "").strip(),
        "subcategoria_4": (p.subcategoria_4 or "").strip(),
        "categoria_listagem": "",
        "prateleira": "",
        "fornecedor": (p.fornecedor_texto or "").strip(),
        "imagem": "",
        "inativo": bool(p.cadastro_inativo or not p.ativo),
        "unidade": (p.unidade or "UN").strip() or "UN",
        "descricao": (p.descricao or "").strip(),
        "ncm": (p.ncm or "").strip(),
        "cadastro_somente_agro": bool(p.cadastro_somente_agro),
        "fonte": "agro_pg",
    }
    from produtos.cadastro_busca_codigo_util import index_codigos_de_campos

    row["index_codigos"] = index_codigos_de_campos(
        codigo=row.get("codigo"),
        codigo_nfe=row.get("codigo_nfe"),
        codigo_barras=row.get("codigo_barras"),
    )
    row["busca_texto"] = " ".join(
        x
        for x in (
            row.get("nome"),
            row.get("marca"),
            row.get("codigo"),
            row.get("codigo_nfe"),
            row.get("codigo_barras"),
            row.get("categoria"),
            row.get("subcategoria"),
            row.get("fornecedor"),
        )
        if x
    ).strip()
    if ov is None and pid:
        ov = ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid[:64]).first()
    row = _aplicar_overlay_em_row(row, ov)
    from produtos.catalogo_nome_util import aplicar_nome_resolvido_em_row

    return aplicar_nome_resolvido_em_row(row, p, ov)


def queryset_catalogo_ativos(*, inativos: bool = False):
    qs = Produto.objects.all()
    if not inativos:
        qs = qs.filter(cadastro_inativo=False, ativo=True)
    return qs


def _rows_de_produtos(produtos: list[Produto]) -> list[dict]:
    if not produtos:
        return []
    ids = [
        str(p.produto_externo_id or p.erp_produto_id or p.pk).strip()[:64] for p in produtos
    ]
    ov_map = _overlay_mapa_por_ids(ids)
    out: list[dict] = []
    for p in produtos:
        pid = str(p.produto_externo_id or p.erp_produto_id or p.pk).strip()[:64]
        out.append(produto_agro_para_row(p, ov=ov_map.get(pid)))
    return out


def listar_paginado(
    *,
    pagina: int = 1,
    por_pagina: int = 72,
    sort_key: str = "nome",
    sort_direction: int = 1,
    inativos: bool = False,
) -> tuple[list[dict], bool]:
    qs = queryset_catalogo_ativos(inativos=inativos)
    field = _SORT_MAP.get(sort_key, "nome")
    order = field if sort_direction >= 0 else f"-{field}"
    skip = max(0, (pagina - 1) * por_pagina)
    chunk = list(qs.order_by(order, "pk")[skip : skip + por_pagina + 1])
    has_more = len(chunk) > por_pagina
    return _rows_de_produtos(chunk[:por_pagina]), has_more


def _q_tokens_todos_cadastro(termo: str):
    from produtos.cadastro_busca_codigo_util import q_icontains_cadastro

    parts = [p.strip() for p in (termo or "").split() if len(p.strip()) >= 2]
    if len(parts) < 2:
        return None
    q = Q()
    for pl in parts:
        q &= q_icontains_cadastro(pl)
    return q


def _cadastro_pg_append_unicos(found: list, seen: set, items, lim: int) -> None:
    for p in items:
        if p.pk in seen:
            continue
        seen.add(p.pk)
        found.append(p)
        if len(found) >= lim:
            return


def buscar(q: str, *, limit: int = 80, inativos: bool = False) -> list[dict]:
    from produtos.cadastro_busca_codigo_util import (
        overlay_pids_por_codigo,
        parece_codigo_cadastro,
        q_icontains_cadastro,
        termo_eh_codigo_gm,
        termo_bate_codigos_produto,
    )

    termo = (q or "").strip()
    if not termo:
        return []
    qs = queryset_catalogo_ativos(inativos=inativos)
    lim = max(1, min(int(limit or 80), 160))
    found: list[Produto] = []
    seen_pk: set[int] = set()

    if parece_codigo_cadastro(termo):
        pids = overlay_pids_por_codigo(termo, limit=lim)
        if pids:
            _cadastro_pg_append_unicos(
                found,
                seen_pk,
                qs.filter(produto_externo_id__in=pids).order_by("nome", "pk")[:lim],
                lim,
            )
        if len(found) < lim:
            _cadastro_pg_append_unicos(
                found,
                seen_pk,
                qs.filter(q_icontains_cadastro(termo)).order_by("nome", "pk")[:lim],
                lim,
            )
        if len(found) < lim:
            for p in qs.iterator(chunk_size=400):
                if termo_bate_codigos_produto(
                    termo,
                    codigo_interno=p.codigo_interno,
                    codigo_nfe=p.codigo_nfe,
                    codigo_barras=p.codigo_barras,
                    extras=(p.produto_externo_id, p.erp_produto_id),
                ):
                    _cadastro_pg_append_unicos(found, seen_pk, [p], lim)
                    if len(found) >= lim:
                        break
    else:
        _cadastro_pg_append_unicos(
            found,
            seen_pk,
            qs.filter(q_icontains_cadastro(termo)).order_by("nome", "pk")[:lim],
            lim,
        )
        if len(found) < lim:
            q_tok = _q_tokens_todos_cadastro(termo)
            if q_tok is not None:
                _cadastro_pg_append_unicos(
                    found,
                    seen_pk,
                    qs.filter(q_tok).order_by("nome", "pk")[:lim],
                    lim,
                )

    partes_txt = [p.strip().lower() for p in termo.split() if len(p.strip()) >= 2]
    if partes_txt and len(found) < lim:
        from produtos.catalogo_nome_util import produto_fantasma_catalogo, queryset_produtos_nome_corrupto

        for p in queryset_produtos_nome_corrupto(qs).iterator(chunk_size=160):
            if p.pk in seen_pk:
                continue
            if not produto_fantasma_catalogo(p):
                continue
            row = produto_agro_para_row(p)
            bt = str(row.get("busca_texto") or row.get("nome") or "").lower()
            if bt and all(pl in bt for pl in partes_txt):
                _cadastro_pg_append_unicos(found, seen_pk, [p], lim)
                if len(found) >= lim:
                    break

    if parece_codigo_cadastro(termo) and len(found) < lim:
        from produtos.catalogo_nome_util import produto_fantasma_catalogo, queryset_produtos_nome_corrupto

        for p in queryset_produtos_nome_corrupto(qs).iterator(chunk_size=160):
            if p.pk in seen_pk:
                continue
            if not produto_fantasma_catalogo(p):
                continue
            row = produto_agro_para_row(p)
            if termo_bate_codigos_produto(
                termo,
                codigo_interno=row.get("codigo"),
                codigo_nfe=row.get("codigo_nfe"),
                codigo_barras=row.get("codigo_barras"),
                extras=(row.get("id"),),
            ):
                _cadastro_pg_append_unicos(found, seen_pk, [p], lim)
                if len(found) >= lim:
                    break

    if found:
        if termo_eh_codigo_gm(termo):
            found = [
                p
                for p in found
                if termo_bate_codigos_produto(
                    termo,
                    codigo_interno=p.codigo_interno,
                    codigo_nfe=p.codigo_nfe,
                    codigo_barras=p.codigo_barras,
                    extras=(p.produto_externo_id, p.erp_produto_id),
                )
            ]
        if found:
            return _rows_de_produtos(found[:lim])

    if parece_codigo_cadastro(termo):
        try:
            from produtos.views import obter_conexao_mongo

            client, db = obter_conexao_mongo()
            if db is not None and client is not None:
                from produtos.cadastro_busca_codigo_util import cadastro_mongo_busca_por_codigo

                docs = cadastro_mongo_busca_por_codigo(
                    db,
                    client,
                    termo,
                    limit=lim,
                    include_inactive=inativos,
                    projection={"Id": 1, "_id": 1},
                )
                ext_ids = [str(d.get("Id") or d.get("_id") or "").strip() for d in docs]
                ext_ids = [x for x in ext_ids if x]
                if ext_ids:
                    chunk = list(qs.filter(produto_externo_id__in=ext_ids).order_by("nome", "pk")[:lim])
                    if chunk:
                        return _rows_de_produtos(chunk)
        except Exception:
            pass

    return []


def obter_produto_model(produto_id: str) -> Produto | None:
    pid = (produto_id or "").strip()
    if not pid:
        return None
    p = (
        Produto.objects.filter(
            Q(produto_externo_id=pid) | Q(erp_produto_id=pid) | Q(codigo_interno=pid)
        )
        .order_by("pk")
        .first()
    )
    if p is None and pid.isdigit():
        try:
            p = Produto.objects.filter(pk=int(pid)).first()
        except (TypeError, ValueError):
            p = None
    return p


def produto_por_externo_id(produto_id: str) -> dict | None:
    p = obter_produto_model(produto_id)
    if p is None:
        return None
    return produto_agro_para_row(p)


def produto_model_para_detalhe(p: Produto) -> dict:
    row = produto_agro_para_row(p)
    pv = float(row.get("preco_venda") or 0)
    pc = float(row.get("preco_custo") or 0)
    mva_rs = round(pv - pc, 2) if pv and pc else 0.0
    mva_pct = round((mva_rs / pc) * 100, 2) if pc > 0 else 0.0
    return {
        **row,
        "preco_custo_com_acrescimos": pc,
        "preco_custo_final": pc,
        "mva_lucro_reais": mva_rs,
        "mva_lucro_percentual": mva_pct,
        "cadastro_somente_agro": bool(p.cadastro_somente_agro),
        "fonte": "agro_pg",
    }


def produto_model_para_resposta_salvar(p: Produto, ov: ProdutoGestaoOverlayAgro | None = None) -> dict:
    """JSON compatível com ``agroCadastroMergeProdutoCacheLocal`` após salvar overlay."""
    row = produto_agro_para_row(p, ov=ov)
    row["codigo_gm"] = row.get("codigo_nfe") or row.get("codigo") or ""
    row["preco_custo_final"] = row.get("preco_custo")
    row["tem_overlay"] = ov is not None
    return row


def sincronizar_modelo_produto_de_overlay(
    pid: str,
    ov: ProdutoGestaoOverlayAgro,
    *,
    custo_payload: Decimal | None = None,
    payload: dict | None = None,
) -> Produto:
    """Espelha overlay + payload no modelo ``Produto`` (fonte cadastro ``agro_pg``)."""
    pid64 = str(pid or "").strip()[:64]
    payload = payload or {}
    p = obter_produto_model(pid64)

    def _txt(key: str, mx: int = 300) -> str:
        return str(payload.get(key) or "").strip()[:mx]

    from produtos.cadastro_codigo_sequencial_util import gm_sugerido_de_codigo_sistema

    cod_sys = _txt("codigo", 50)
    codigo_interno = cod_sys[:50] if cod_sys else (_txt("codigo_nfe", 64)[:50] or pid64[:50])
    codigo_nfe_val = (
        ov.codigo_nfe.strip()
        or _txt("codigo_nfe", 64)
        or gm_sugerido_de_codigo_sistema(cod_sys)
        or codigo_interno
    )[:64]
    nome = (ov.nome.strip() if ov.nome.strip() else _txt("nome", 300)) or "—"
    custo = custo_payload
    if custo is None and ov.cadastro_extras and isinstance(ov.cadastro_extras, dict):
        raw_ce = ov.cadastro_extras.get("preco_custo_overlay")
        if raw_ce is not None:
            custo = _dec_opt(raw_ce)
    if custo is None and p is not None:
        custo = p.custo
    if custo is None:
        custo = Decimal("0")

    pv = ov.preco_venda if ov.preco_venda is not None else (p.preco_venda if p else Decimal("0"))
    ativo = True
    cad_inativo = False
    if ov.ativo_exibicao is not None:
        ativo = bool(ov.ativo_exibicao)
        cad_inativo = not ativo

    defaults = {
        "codigo_interno": codigo_interno,
        "codigo_nfe": codigo_nfe_val,
        "codigo_barras": (ov.codigo_barras.strip() or None),
        "nome": nome[:300],
        "marca": ov.marca.strip()[:120],
        "categoria": ov.categoria.strip()[:200] or None,
        "subcategoria": ov.subcategoria.strip()[:200],
        "subcategoria_2": ov.subcategoria_2.strip()[:200],
        "subcategoria_3": ov.subcategoria_3.strip()[:200],
        "subcategoria_4": ov.subcategoria_4.strip()[:200],
        "fornecedor_texto": ov.fornecedor_texto.strip()[:300],
        "unidade": (ov.unidade.strip() or "UN")[:20],
        "descricao": ov.descricao.strip()[:16000],
        "custo": custo,
        "preco_venda": pv,
        "ativo": ativo,
        "cadastro_inativo": cad_inativo,
    }
    if p is None:
        p = Produto.objects.create(produto_externo_id=pid64, **defaults)
    else:
        for k, v in defaults.items():
            setattr(p, k, v)
        p.save()
    return p


def try_criar_produto_postgres_somente_agro(payload: dict) -> tuple[dict | None, str | None]:
    """Cria ``Produto`` mínimo (somente SisVale). Retorna (erro_json, None) ou (None, novo_id)."""
    from django.http import JsonResponse

    def pt(key: str, mx: int = 300) -> str:
        return str(payload.get(key) or "").strip()[:mx]

    nome = pt("nome", 300)
    if len(nome) < 2:
        return (
            JsonResponse(
                {"ok": False, "erro": "Informe o nome do produto (mínimo 2 caracteres)."},
                status=400,
            ),
            None,
        )

    cod_int = pt("codigo", 80)
    cod_nfe = pt("codigo_nfe", 64)
    cod_cb = pt("codigo_barras", 80)
    if cod_int.lower() == "__novo__":
        cod_int = ""
    if cod_nfe.lower() == "__novo__":
        cod_nfe = ""

    from produtos.cadastro_codigo_sequencial_util import (
        alocar_codigo_sequencial_novo_cadastro,
        erro_codigo_sistema_4_digitos,
        gm_sugerido_de_codigo_sistema,
    )

    if not cod_int and not cod_nfe:
        from produtos.views import obter_conexao_mongo

        client, db = obter_conexao_mongo()
        col = client.col_p if client is not None else None
        err_al, c_sys, c_gm = alocar_codigo_sequencial_novo_cadastro(db, col)
        if err_al is not None:
            return (
                JsonResponse(
                    {"ok": False, "erro": err_al.get("erro", "Erro ao gerar código.")},
                    status=int(err_al.get("status") or 400),
                ),
                None,
            )
        cod_int = str(c_sys or "").strip()
        cod_nfe = str(c_gm or "").strip()
    else:
        err_cod = erro_codigo_sistema_4_digitos(cod_int, obrigatorio=True)
        if err_cod:
            return JsonResponse({"ok": False, "erro": err_cod}, status=400), None
        if not cod_nfe:
            cod_nfe = gm_sugerido_de_codigo_sistema(cod_int)

    if not cod_int and not cod_nfe and not cod_cb:
        return (
            JsonResponse(
                {
                    "ok": False,
                    "erro": "Informe código interno, código NFe/GM ou código de barras.",
                },
                status=400,
            ),
            None,
        )

    for _ in range(16):
        cand = "AGRO" + secrets.token_hex(12).upper()
        if not Produto.objects.filter(produto_externo_id=cand).exists():
            novo_id = cand
            break
    else:
        return JsonResponse({"ok": False, "erro": "Não foi possível gerar Id único."}, status=500), None

    try:
        pv = _dec_opt(payload.get("preco_venda")) or Decimal("0")
        pc = _dec_opt(payload.get("preco_custo")) or Decimal("0")
    except Exception:
        return JsonResponse({"ok": False, "erro": "Preço inválido."}, status=400), None

    codigo_interno_salvar = (cod_int or cod_cb or novo_id)[:50]
    codigo_nfe_salvar = (cod_nfe or cod_int or cod_cb or novo_id)[:64]

    Produto.objects.create(
        produto_externo_id=novo_id,
        codigo_interno=codigo_interno_salvar,
        codigo_nfe=codigo_nfe_salvar,
        codigo_barras=cod_cb[:50] if cod_cb else None,
        nome=nome,
        marca=pt("marca", 120),
        categoria=pt("categoria", 200) or None,
        subcategoria=pt("subcategoria", 200),
        fornecedor_texto=pt("fornecedor_texto", 300),
        unidade=pt("unidade", 20) or "UN",
        descricao=str(payload.get("descricao") or "")[:16000],
        custo=pc,
        preco_venda=pv,
        cadastro_somente_agro=True,
        cadastro_inativo=False,
        ativo=True,
    )
    return None, novo_id


def defaults_import_com_overlay(pid: str, defaults: dict) -> dict:
    """Mescla overlay local no dict de import Mongo→PG (preço da loja prevalece)."""
    ov = ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid[:64]).first()
    if not ov:
        return defaults
    out = dict(defaults)
    if ov.nome.strip():
        out["nome"] = ov.nome.strip()[:300]
    if ov.marca.strip():
        out["marca"] = ov.marca.strip()[:120]
    if ov.categoria.strip():
        out["categoria"] = ov.categoria.strip()[:200]
    if ov.subcategoria.strip():
        out["subcategoria"] = ov.subcategoria.strip()[:200]
    if ov.fornecedor_texto.strip():
        out["fornecedor_texto"] = ov.fornecedor_texto.strip()[:300]
    if ov.unidade.strip():
        out["unidade"] = ov.unidade.strip()[:20]
    if ov.codigo_barras.strip():
        out["codigo_barras"] = ov.codigo_barras.strip()[:50]
    if ov.codigo_nfe.strip():
        out["codigo_nfe"] = ov.codigo_nfe.strip()[:64]
        out["codigo_interno"] = ov.codigo_nfe.strip()[:50]
    if ov.preco_venda is not None:
        out["preco_venda"] = ov.preco_venda
    if ov.ativo_exibicao is not None:
        out["ativo"] = bool(ov.ativo_exibicao)
        out["cadastro_inativo"] = not bool(ov.ativo_exibicao)
    ce = ov.cadastro_extras if isinstance(ov.cadastro_extras, dict) else {}
    if ce.get("preco_custo_overlay") is not None:
        try:
            out["custo"] = Decimal(str(ce["preco_custo_overlay"])).quantize(Decimal("0.01"))
        except Exception:
            pass
    return out


def _queryset_gestao_status(status_q: str):
    """Queryset ``Produto`` conforme filtro status da gestão operacional."""
    status_q = (status_q or "ativos").strip().lower()
    if status_q == "inativos":
        return Produto.objects.filter(Q(cadastro_inativo=True) | Q(ativo=False))
    if status_q == "todos":
        return Produto.objects.all()
    return queryset_catalogo_ativos(inativos=False)


def row_para_doc_gestao_lista(row: dict) -> dict:
    """Documento estilo Mongo para ``_linha_gestao_produto_json``."""
    inativo = bool(row.get("inativo"))
    cat = str(row.get("categoria") or "").strip()
    sub = str(row.get("subcategoria") or "").strip()
    forn = str(row.get("fornecedor") or "").strip()
    cod = str(row.get("codigo") or "").strip()
    cnfe = str(row.get("codigo_nfe") or cod).strip()
    pid = str(row.get("id") or "").strip()
    return {
        "Id": pid,
        "_id": pid,
        "CadastroInativo": inativo,
        "Nome": str(row.get("nome") or "").strip(),
        "Marca": str(row.get("marca") or "").strip(),
        "NomeCategoria": cat,
        "Categoria": cat,
        "Grupo": cat,
        "NomeFornecedor": forn,
        "Fornecedor": forn,
        "SubGrupo": sub,
        "Subcategoria": sub,
        "NomeSubcategoria": sub,
        "CodigoNFe": cnfe,
        "Codigo": cod,
        "CodigoBarras": str(row.get("codigo_barras") or "").strip(),
        "Unidade": str(row.get("unidade") or "UN").strip() or "UN",
        "ValorVenda": row.get("preco_venda"),
        "PrecoVenda": row.get("preco_venda"),
        "PrecoCusto": row.get("preco_custo"),
        "ValorCusto": row.get("preco_custo"),
        "Descricao": str(row.get("descricao") or "").strip(),
    }


def _gestao_aplicar_filtros_qs(qs, *, marca: str, categoria: str, fornecedor: str):
    marca = (marca or "").strip()
    categoria = (categoria or "").strip()
    fornecedor = (fornecedor or "").strip()
    if marca:
        qs = qs.filter(marca=marca)
    if categoria:
        qs = qs.filter(categoria=categoria)
    if fornecedor:
        qs = qs.filter(fornecedor_texto__icontains=fornecedor)
    return qs


def listar_gestao_paginado(
    *,
    pagina: int = 1,
    por_pagina: int = 40,
    status_q: str = "ativos",
    marca: str = "",
    categoria: str = "",
    fornecedor: str = "",
) -> tuple[list[dict], bool, int | None]:
    """Lista paginada gestão operacional (Postgres + overlay)."""
    qs = _queryset_gestao_status(status_q)
    qs = _gestao_aplicar_filtros_qs(qs, marca=marca, categoria=categoria, fornecedor=fornecedor)
    skip = max(0, (pagina - 1) * por_pagina)
    chunk = list(qs.order_by("nome", "pk")[skip : skip + por_pagina + 1])
    has_more = len(chunk) > por_pagina
    rows = _rows_de_produtos(chunk[:por_pagina])
    return rows, has_more, None


def _gestao_row_passa_status(row: dict, status_q: str) -> bool:
    inativo = bool(row.get("inativo"))
    if status_q == "inativos":
        return inativo
    if status_q == "todos":
        return True
    return not inativo


def _gestao_row_passa_filtros(row: dict, marca: str, categoria: str, fornecedor: str) -> bool:
    if marca and str(row.get("marca") or "").strip() != marca:
        return False
    if categoria and str(row.get("categoria") or "").strip() != categoria:
        return False
    if fornecedor:
        f = str(row.get("fornecedor") or "").strip().lower()
        if fornecedor.strip().lower() not in f:
            return False
    return True


def buscar_gestao(
    q: str,
    *,
    limit: int = 120,
    status_q: str = "ativos",
    marca: str = "",
    categoria: str = "",
    fornecedor: str = "",
) -> list[dict]:
    """Busca gestão com filtros (Postgres)."""
    include_inactive = status_q in ("todos", "inativos")
    rows = buscar(q, limit=limit, inativos=include_inactive)
    out: list[dict] = []
    for row in rows:
        if not _gestao_row_passa_status(row, status_q):
            continue
        if not _gestao_row_passa_filtros(row, marca, categoria, fornecedor):
            continue
        out.append(row)
    return out


def _faceta_valores_distintos(valores, *, limite: int = 200) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in valores:
        s = str(raw or "").strip()
        if not s:
            continue
        key = s.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(s)
        if len(out) >= limite:
            break
    return sorted(out, key=lambda x: x.lower())


def facetas_gestao(*, limite: int = 200) -> dict[str, list[str]]:
    """Marcas, categorias, subcategorias e fornecedores (Postgres + overlay)."""
    lim = max(1, min(int(limite or 200), 300))
    qs = queryset_catalogo_ativos(inativos=False)
    marcas = _faceta_valores_distintos(qs.exclude(marca="").values_list("marca", flat=True).distinct(), limite=lim)
    categorias = _faceta_valores_distintos(
        qs.exclude(categoria="").values_list("categoria", flat=True).distinct(), limite=lim
    )
    subcategorias = _faceta_valores_distintos(
        qs.exclude(subcategoria="").values_list("subcategoria", flat=True).distinct(), limite=lim
    )
    fornecedores = _faceta_valores_distintos(
        qs.exclude(fornecedor_texto="").values_list("fornecedor_texto", flat=True).distinct(), limite=lim + 100
    )

    ov_qs = ProdutoGestaoOverlayAgro.objects.all()
    marcas = _faceta_valores_distintos(
        list(marcas)
        + [x for x in ov_qs.exclude(marca="").values_list("marca", flat=True).distinct()[: lim + 50]],
        limite=lim,
    )
    categorias = _faceta_valores_distintos(
        list(categorias)
        + [x for x in ov_qs.exclude(categoria="").values_list("categoria", flat=True).distinct()[: lim + 50]],
        limite=lim,
    )
    subcategorias = _faceta_valores_distintos(
        list(subcategorias)
        + [x for x in ov_qs.exclude(subcategoria="").values_list("subcategoria", flat=True).distinct()[: lim + 50]],
        limite=lim,
    )
    fornecedores = _faceta_valores_distintos(
        list(fornecedores)
        + [
            x
            for x in ov_qs.exclude(fornecedor_texto="").values_list("fornecedor_texto", flat=True).distinct()[
                : lim + 100
            ]
        ],
        limite=lim + 100,
    )
    return {
        "marcas": marcas,
        "categorias": categorias,
        "subcategorias": subcategorias,
        "fornecedores": fornecedores,
    }


def listar_todos_rows_ativos() -> list[dict]:
    """Todos os produtos ativos do Postgres (catálogo ``agro_pg``)."""
    out: list[dict] = []
    pagina = 1
    while True:
        chunk, has_more = listar_paginado(pagina=pagina, por_pagina=500, sort_key="nome", sort_direction=1)
        out.extend(chunk)
        if not has_more:
            break
        pagina += 1
    return out


def row_para_doc_busca_pdv(row: dict) -> dict:
    """Documento estilo Mongo para o loop ``api_buscar_produtos``."""
    from produtos.mongo_index_codigos import INDEX_CODIGOS_CAMPO

    ix = row.get("index_codigos") or []
    return {
        "Id": row.get("id"),
        "_id": row.get("id"),
        "Nome": row.get("nome") or "",
        "Marca": row.get("marca") or "",
        "Codigo": row.get("codigo") or "",
        "CodigoNFe": row.get("codigo_nfe") or row.get("codigo") or "",
        "CodigoBarras": row.get("codigo_barras") or "",
        "EAN_NFe": row.get("codigo_barras") or "",
        "ValorVenda": row.get("preco_venda"),
        "PrecoVenda": row.get("preco_venda"),
        "NomeCategoria": row.get("categoria") or "",
        "Categoria": row.get("categoria") or "",
        "SubGrupo": row.get("subcategoria") or "",
        "NomeFornecedor": row.get("fornecedor") or "",
        INDEX_CODIGOS_CAMPO: ix if isinstance(ix, list) else [],
        "BuscaTexto": row.get("busca_texto") or "",
    }


def _alnum_codigo_cmp(val: object) -> str:
    import re

    return re.sub(r"[^a-z0-9]", "", str(val or "").strip().lower())


def _nome_doc_busca_pdv(doc: dict) -> str:
    return str(doc.get("Nome") or doc.get("nome") or "").strip()


def _chaves_codigo_doc_busca_pdv(doc: dict) -> set[str]:
    keys: set[str] = set()
    for k in ("CodigoNFe", "Codigo", "codigo_nfe", "codigo", "CodigoBarras", "codigo_barras"):
        al = _alnum_codigo_cmp(doc.get(k))
        if al and len(al) >= 4:
            keys.add(al)
    ix = doc.get("index_codigos") or doc.get("IndexCodigos")
    if isinstance(ix, list):
        for x in ix:
            al = _alnum_codigo_cmp(x)
            if al and len(al) >= 4:
                keys.add(al)
    return keys


def _dedupe_prods_busca_preferir_com_nome(prods: list) -> list:
    """Remove fantasma Mongo (sem nome) quando Postgres trouxe o mesmo GM/código."""
    if not prods or len(prods) < 2:
        return prods
    grupos: dict[str, list[int]] = {}
    for i, p in enumerate(prods):
        for k in _chaves_codigo_doc_busca_pdv(p):
            grupos.setdefault(k, []).append(i)
    drop: set[int] = set()
    for idxs in grupos.values():
        if len(idxs) < 2:
            continue
        uniq = sorted(set(idxs))
        if len(uniq) < 2:
            continue

        def _rank(i: int) -> tuple:
            nome = _nome_doc_busca_pdv(prods[i])
            return (1 if nome else 0, len(nome), i)

        best = max(uniq, key=_rank)
        for i in uniq:
            if i != best:
                drop.add(i)
    if not drop:
        return prods
    return [p for i, p in enumerate(prods) if i not in drop]


def fundir_doc_mongo_com_row_pg(doc: dict, row: dict) -> dict:
    from produtos.mongo_index_codigos import INDEX_CODIGOS_CAMPO

    out = dict(doc)
    nome_pg = str(row.get("nome") or "").strip()
    if nome_pg:
        out["Nome"] = nome_pg
    if row.get("marca") is not None:
        out["Marca"] = row["marca"]
    cod = row.get("codigo") or ""
    if cod:
        out["Codigo"] = cod
    cnfe = row.get("codigo_nfe") or cod
    if cnfe:
        out["CodigoNFe"] = cnfe
    cb = row.get("codigo_barras")
    if cb:
        out["CodigoBarras"] = cb
        out["EAN_NFe"] = cb
    if row.get("preco_venda") is not None:
        pv = float(row["preco_venda"])
        out["ValorVenda"] = pv
        out["PrecoVenda"] = pv
    if row.get("categoria"):
        out["NomeCategoria"] = row["categoria"]
        out["Categoria"] = row["categoria"]
    if row.get("subcategoria"):
        out["SubGrupo"] = row["subcategoria"]
    if row.get("fornecedor"):
        out["NomeFornecedor"] = row["fornecedor"]
    ix = row.get("index_codigos")
    if isinstance(ix, list) and ix:
        out[INDEX_CODIGOS_CAMPO] = ix
    if row.get("busca_texto"):
        out["BuscaTexto"] = row["busca_texto"]
    return out


def prods_mongo_style_busca_pdv(
    *,
    q: str = "",
    wizard_catalog: bool = False,
    limit: int = 80,
) -> list[dict]:
    """Documentos estilo Mongo a partir do catálogo Postgres (PDV sem espelho)."""
    if wizard_catalog:
        rows = listar_todos_rows_ativos()
    else:
        termo = (q or "").strip()
        rows = list(buscar(termo, limit=limit)) if termo else []
    return [row_para_doc_busca_pdv(r) for r in rows]


def mesclar_prods_busca_pdv(
    prods: list,
    *,
    q: str = "",
    wizard_catalog: bool = False,
    limit: int = 80,
) -> list:
    """Inclui/atualiza produtos do Postgres no resultado de busca do PDV."""
    from produtos.agro_fonte_config import agro_pdv_merge_catalogo_postgres

    if not agro_pdv_merge_catalogo_postgres():
        return prods

    ids_vistos: set[str] = set()
    idx_por_id: dict[str, int] = {}
    for i, p in enumerate(prods):
        pid = str(p.get("Id") or p.get("_id") or "").strip()
        if pid:
            ids_vistos.add(pid)
            idx_por_id[pid] = i

    if wizard_catalog:
        pg_rows = listar_todos_rows_ativos()
    else:
        termo = (q or "").strip()
        pg_rows = buscar(termo, limit=limit) if termo else []

    for row in pg_rows:
        pid = str(row.get("id") or "").strip()
        if not pid:
            continue
        if pid in ids_vistos:
            prods[idx_por_id[pid]] = fundir_doc_mongo_com_row_pg(prods[idx_por_id[pid]], row)
        else:
            prods.append(row_para_doc_busca_pdv(row))
            ids_vistos.add(pid)
            idx_por_id[pid] = len(prods) - 1
    return _dedupe_prods_busca_preferir_com_nome(prods)


def mesclar_catalogo_pdv_cache(itens: list[dict]) -> list[dict]:
    """Mescla catálogo local do PDV (``api_todos_produtos_local``) com Postgres."""
    from produtos.agro_fonte_config import agro_pdv_merge_catalogo_postgres
    from produtos.mongo_index_codigos import normalizar

    if not agro_pdv_merge_catalogo_postgres():
        return itens

    por_id = {str(x.get("id") or ""): x for x in itens if x.get("id") is not None}
    for row in listar_todos_rows_ativos():
        pid = str(row.get("id") or "")
        if not pid:
            continue
        pv = float(row.get("preco_venda") or 0)
        pc = float(row.get("preco_custo") or 0)
        if pid in por_id:
            ex = por_id[pid]
            ex["nome"] = row.get("nome") or ex.get("nome")
            ex["marca"] = row.get("marca") or ex.get("marca")
            ex["preco_venda"] = pv
            ex["preco_custo"] = pc
            ex["preco_custo_final"] = pc
            ex["preco_custo_acrescimo"] = pc
            ex["codigo_nfe"] = row.get("codigo_nfe") or ex.get("codigo_nfe")
            ex["codigo_barras"] = row.get("codigo_barras") or ex.get("codigo_barras")
            ex["categoria"] = row.get("categoria") or ex.get("categoria")
            ex["subcategoria"] = row.get("subcategoria") or ex.get("subcategoria")
            ex["fornecedor"] = row.get("fornecedor") or ex.get("fornecedor")
            ix = row.get("index_codigos") or []
            if isinstance(ix, list):
                ex["index_codigos"] = [str(x) for x in ix[:260] if x is not None and str(x).strip()]
            partes = [
                row.get("nome"),
                row.get("marca"),
                row.get("codigo_nfe"),
                row.get("codigo_barras"),
                row.get("categoria"),
            ]
            busca = normalizar(" ".join(str(x) for x in partes if x)).strip()
            if busca:
                ex["busca_texto"] = busca
        else:
            ix = row.get("index_codigos") or []
            ix_list = [str(x) for x in ix[:260] if x is not None and str(x).strip()] if isinstance(ix, list) else []
            partes = [
                row.get("nome"),
                row.get("marca"),
                row.get("categoria"),
                row.get("codigo_nfe"),
                row.get("codigo"),
                row.get("codigo_barras"),
            ]
            busca = normalizar(" ".join(str(x) for x in partes if x)).strip()
            novo = {
                "id": pid,
                "nome": row.get("nome"),
                "marca": row.get("marca"),
                "prateleira": "",
                "fornecedor": row.get("fornecedor") or "",
                "categoria": row.get("categoria") or "",
                "subcategoria": row.get("subcategoria") or "",
                "codigo_nfe": row.get("codigo_nfe") or row.get("codigo"),
                "codigo_barras": row.get("codigo_barras") or "",
                "referencia": "",
                "sku": "",
                "codigo_interno": row.get("codigo") or "",
                "codigo_fornecedor": "",
                "preco_venda": pv,
                "preco_custo": pc,
                "preco_custo_acrescimo": pc,
                "preco_custo_final": pc,
                "saldo_centro": 0.0,
                "saldo_vila": 0.0,
                "saldo_erp_centro": 0.0,
                "saldo_erp_vila": 0.0,
                "busca_texto": busca,
                "media_venda_diaria_30d": 0.0,
                "index_codigos": ix_list,
            }
            itens.append(novo)
            por_id[pid] = novo
    return itens
