"""Catálogo PostgreSQL (``Produto``) — ``AGRO_FONTE_CATALOGO=agro_pg``."""
from __future__ import annotations

import re
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
    modelo = str(row.get("modelo") or "").strip()
    if modelo:
        busca_txt = str(row.get("busca_texto") or "").strip()
        if modelo.lower() not in busca_txt.lower():
            row["busca_texto"] = f"{busca_txt} {modelo}".strip()
    return row


def produto_agro_para_row(
    p: Produto,
    ov: ProdutoGestaoOverlayAgro | None = None,
    *,
    resolver_overlay_faltante: bool = True,
) -> dict:
    pid = (p.produto_externo_id or p.erp_produto_id or str(p.pk)).strip()
    row = {
        "id": pid,
        "nome": (p.nome or "").strip(),
        "marca": (p.marca or "").strip(),
        "modelo": (getattr(p, "modelo", None) or "").strip(),
        "codigo": (p.codigo_interno or "").strip(),
        "codigo_nfe": (p.codigo_nfe or p.codigo_interno or "").strip(),
        "codigo_barras": (p.codigo_barras or "").strip(),
        "preco_venda": _dec(p.preco_venda),
        "preco_custo": _dec(p.custo),
        "preco_custo_com_acrescimos": _dec(p.custo),
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
        "criado_em": p.criado_em.isoformat() if getattr(p, "criado_em", None) else "",
        "fonte": "agro_pg",
    }
    from produtos.cadastro_busca_codigo_util import index_codigos_de_campos
    from produtos.mongo_index_codigos import (
        _eans_embalagem_nf_de_cadastro_extras,
        codigos_barras_opcionais_de_cadastro_extras,
    )

    # Batch (_rows_de_produtos) já monta ov_map: NÃO reconsultar overlay por produto
    # (N+1 derruba /api/todos-produtos/delta/ e trava o PDV).
    if resolver_overlay_faltante and ov is None and pid:
        ov = ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid[:64]).first()
    row = _aplicar_overlay_em_row(row, ov)
    # index/busca_texto depois do overlay — GM da loja costuma estar só no overlay
    extras_ix: list[str] = []
    if ov is not None and isinstance(getattr(ov, "cadastro_extras", None), dict):
        extras_ix.extend(codigos_barras_opcionais_de_cadastro_extras(ov.cadastro_extras))
        extras_ix.extend(_eans_embalagem_nf_de_cadastro_extras(ov.cadastro_extras))
    row["index_codigos"] = index_codigos_de_campos(
        codigo=row.get("codigo"),
        codigo_nfe=row.get("codigo_nfe"),
        codigo_barras=row.get("codigo_barras"),
        extras=extras_ix,
    )
    row["busca_texto"] = " ".join(
        x
        for x in (
            row.get("nome"),
            row.get("marca"),
            row.get("modelo"),
            row.get("codigo"),
            row.get("codigo_nfe"),
            row.get("codigo_barras"),
            *extras_ix,
            row.get("categoria"),
            row.get("subcategoria"),
            row.get("fornecedor"),
        )
        if x
    ).strip()
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
        out.append(
            produto_agro_para_row(
                p, ov=ov_map.get(pid), resolver_overlay_faltante=False
            )
        )
    return out


def listar_paginado(
    *,
    pagina: int = 1,
    por_pagina: int = 72,
    sort_key: str = "nome",
    sort_direction: int = 1,
    inativos: bool = False,
    marca: str = "",
    categoria: str = "",
    fornecedor: str = "",
    filtros: dict | None = None,
) -> tuple[list[dict], bool]:
    qs = queryset_catalogo_ativos(inativos=inativos)
    if filtros:
        from produtos.cadastro_filtros_util import aplicar_filtros_cadastro_qs

        qs = aplicar_filtros_cadastro_qs(qs, filtros)
    else:
        qs = _gestao_aplicar_filtros_qs(qs, marca=marca, categoria=categoria, fornecedor=fornecedor)
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
        q_codigo_exato_cadastro,
        q_familia_gm_cadastro,
        q_icontains_cadastro,
        q_nome_tokens_cadastro,
        termo_bate_codigos_produto,
        termo_eh_codigo_gm,
    )

    termo = (q or "").strip()
    if not termo:
        return []
    qs = queryset_catalogo_ativos(inativos=inativos)
    lim = max(1, min(int(limit or 80), 160))
    found: list[Produto] = []
    seen_pk: set[int] = set()

    def _append_overlay_modelo_matches(raw_termo: str) -> None:
        termo_txt = str(raw_termo or "").strip()
        if not termo_txt or len(found) >= lim:
            return
        partes = [p.strip() for p in termo_txt.split() if len(p.strip()) >= 2]
        if not partes:
            partes = [termo_txt]
        try:
            ovs = ProdutoGestaoOverlayAgro.objects.all()
            for pl in partes:
                ovs = ovs.filter(cadastro_extras__modelo__icontains=pl[:120])
            pids = list(ovs.values_list("produto_externo_id", flat=True)[:lim])
            if not pids:
                return
            _cadastro_pg_append_unicos(
                found,
                seen_pk,
                qs.filter(produto_externo_id__in=pids).order_by("nome", "pk")[:lim],
                lim,
            )
        except Exception:
            return

    if parece_codigo_cadastro(termo):
        pids = overlay_pids_por_codigo(termo, limit=lim)
        if pids:
            _cadastro_pg_append_unicos(
                found,
                seen_pk,
                qs.filter(produto_externo_id__in=pids).order_by("nome", "pk")[:lim],
                lim,
            )
        fam = q_familia_gm_cadastro(termo)
        if fam is not None and len(found) < lim:
            _cadastro_pg_append_unicos(
                found,
                seen_pk,
                qs.filter(fam).order_by("nome", "pk")[:lim],
                lim,
            )
        q_ex = q_codigo_exato_cadastro(termo)
        if q_ex is not None and len(found) < lim:
            _cadastro_pg_append_unicos(
                found,
                seen_pk,
                qs.filter(q_ex).order_by("nome", "pk")[:lim],
                lim,
            )
        digits_only = re.sub(r"\D", "", termo)
        if len(found) < lim:
            _cadastro_pg_append_unicos(
                found,
                seen_pk,
                qs.filter(q_icontains_cadastro(termo)).order_by("nome", "pk")[:lim],
                lim,
            )
    else:
        q_nome = q_nome_tokens_cadastro(termo)
        if q_nome is not None:
            _cadastro_pg_append_unicos(
                found,
                seen_pk,
                qs.filter(q_nome).order_by("nome", "pk")[:lim],
                lim,
            )
        _append_overlay_modelo_matches(termo)
        # Fallback icontains largo: só se quase vazio (OR em 8 colunas é caro).
        if len(found) < min(3, lim):
            _cadastro_pg_append_unicos(
                found,
                seen_pk,
                qs.filter(q_icontains_cadastro(termo)).order_by("nome", "pk")[:lim],
                lim,
            )

    if found:
        rows = _rows_de_produtos(found[: lim * 2 if lim < 80 else lim])
        if termo_eh_codigo_gm(termo):
            # Filtrar depois do overlay: código GM costuma estar no overlay, não só em Produto.
            rows = [
                r
                for r in rows
                if termo_bate_codigos_produto(
                    termo,
                    codigo_interno=r.get("codigo"),
                    codigo_nfe=r.get("codigo_nfe") or r.get("codigo_gm"),
                    codigo_barras=r.get("codigo_barras"),
                    extras=(r.get("id"),),
                )
            ]
        if rows:
            return rows[:lim]

    if parece_codigo_cadastro(termo):
        from produtos.agro_fonte_config import agro_catalogo_usa_postgres

        if not agro_catalogo_usa_postgres():
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
    pid64 = str(row.get("id") or "").strip()[:64]
    ov = ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid64).first() if pid64 else None
    ce = ov.cadastro_extras if ov and isinstance(ov.cadastro_extras, dict) else {}
    # Coluna Produto.modelo + overlay — nunca sumir ao reabrir o modal
    if not str(row.get("modelo") or "").strip():
        if ce.get("modelo") is not None:
            row["modelo"] = str(ce.get("modelo") or "").strip()[:200]
    row["modelo"] = str(row.get("modelo") or "").strip()[:200]
    row["cadastro_extras"] = dict(ce) if ce else {}

    # Custo família (saco) + composição (kit) — sem isso o Salvar apaga o vínculo ao reabrir (agro_pg).
    try:
        from produtos.composicao_kit_util import extrair_composicao_overlay
        from produtos.custo_familia_util import (
            calcular_custo_filho,
            extrair_custo_familia,
            ler_custo_produto,
            resumo_filhos_do_pai,
        )

        cf = extrair_custo_familia(ce)
        if cf:
            row["custo_familia"] = dict(cf)
            cp_pai = ler_custo_produto(cf.get("pai_produto_id") or "")
            if cp_pai is not None:
                row["custo_familia"]["custo_pai_atual"] = float(cp_pai)
                calc = calcular_custo_filho(cp_pai, cf.get("kg_pai"), cf.get("kg_filho"))
                if calc is not None:
                    row["custo_familia"]["custo_calculado"] = float(calc)
        else:
            row["custo_familia"] = None
        row["custo_familia_filhos"] = resumo_filhos_do_pai(pid64) if pid64 else []

        comp = extrair_composicao_overlay(ce) if "composicao" in ce else []
        for it in comp:
            if not isinstance(it, dict):
                continue
            spid = str(it.get("produto_id") or "").strip()
            if not spid:
                it["custo_unitario_agro"] = None
                continue
            cdec = ler_custo_produto(spid)
            it["custo_unitario_agro"] = round(float(cdec), 4) if cdec is not None else None
        row["composicao"] = comp
        # eh_kit só se houver insumos manuais (não só linha automática do saco)
        manuais = [x for x in comp if str(x.get("origem") or "") != "custo_familia"]
        row["eh_kit"] = bool(manuais)
    except Exception:
        row.setdefault("custo_familia", None)
        row.setdefault("custo_familia_filhos", [])
        row.setdefault("composicao", [])
        row.setdefault("eh_kit", False)

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
    if not str(row.get("modelo") or "").strip() and ov and isinstance(ov.cadastro_extras, dict):
        row["modelo"] = str(ov.cadastro_extras.get("modelo") or "").strip()[:200]
    if not str(row.get("modelo") or "").strip():
        row["modelo"] = str(getattr(p, "modelo", None) or "").strip()[:200]
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

    modelo_val = ""
    if "modelo" in payload:
        modelo_val = _txt("modelo", 200)
    elif ov.cadastro_extras and isinstance(ov.cadastro_extras, dict):
        modelo_val = str(ov.cadastro_extras.get("modelo") or "").strip()[:200]
    elif p is not None:
        modelo_val = str(getattr(p, "modelo", None) or "").strip()[:200]

    defaults = {
        "codigo_interno": codigo_interno,
        "codigo_nfe": codigo_nfe_val,
        "codigo_barras": (ov.codigo_barras.strip() or None),
        "nome": nome[:300],
        "marca": ov.marca.strip()[:120],
        "modelo": modelo_val,
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
        err_al, c_sys, c_gm = alocar_codigo_sequencial_novo_cadastro(None, None)
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
    filtros: dict | None = None,
) -> list[dict]:
    """Busca gestão com filtros (Postgres)."""
    include_inactive = status_q in ("todos", "inativos")
    # Com filtros avançados (estoque/data/multi), amplia o pool da busca e filtra depois.
    lim_busca = limit
    if filtros:
        from produtos.cadastro_filtros_util import filtros_cadastro_ativos

        if filtros_cadastro_ativos(filtros):
            lim_busca = max(int(limit or 120), 400)
    rows = buscar(q, limit=lim_busca, inativos=include_inactive)
    out: list[dict] = []
    for row in rows:
        if not _gestao_row_passa_status(row, status_q):
            continue
        if filtros:
            from produtos.cadastro_filtros_util import row_passa_filtros_cadastro

            # Estoque precisa de saldo — aplica dims sem estoque aqui; estoque depois do enrich.
            f_sem_est = dict(filtros)
            f_sem_est["estoque_sinal"] = ""
            if not row_passa_filtros_cadastro(row, f_sem_est):
                continue
        elif not _gestao_row_passa_filtros(row, marca, categoria, fornecedor):
            continue
        out.append(row)
    return out[: max(1, int(limit or 120))]


def _faceta_valores_distintos(valores, *, limite: int = 200) -> list[str]:
    seen: set[str] = set()
    uniq: list[str] = []
    for raw in valores:
        s = str(raw or "").strip()
        if not s:
            continue
        key = s.lower()
        if key in seen:
            continue
        seen.add(key)
        uniq.append(s)
    uniq.sort(key=lambda x: x.lower())
    lim = int(limite or 0)
    if lim > 0:
        return uniq[:lim]
    return uniq


def compras_dimensoes_relatorio(
    tipo: str,
    q: str = "",
    *,
    completa: bool = False,
    limit: int = 40,
) -> list[str]:
    """Categorias ou unidades distintas (Postgres + overlay) — relatórios Compras sem Mongo."""
    tipo = (tipo or "").strip().lower()
    if tipo not in ("categoria", "unidade"):
        return []
    lim = min(max(int(limit or 40), 1), 500 if completa else 80)
    qtxt = (q or "").strip()[:120]
    seen: set[str] = set()
    out: list[str] = []

    def _add(val: object) -> None:
        s = str(val or "").strip()
        if not s:
            return
        if qtxt and qtxt.lower() not in s.lower():
            return
        k = s.lower()
        if k in seen:
            return
        seen.add(k)
        out.append(s)

    qs = queryset_catalogo_ativos(inativos=False)
    if tipo == "categoria":
        for fld in ("categoria", "subcategoria"):
            qv = qs.exclude(**{fld: ""})
            if qtxt:
                qv = qv.filter(**{f"{fld}__icontains": qtxt})
            cap = 5000 if completa else 400
            for v in qv.values_list(fld, flat=True).distinct()[:cap]:
                _add(v)
                if not completa and len(out) >= lim:
                    break
        oqs = ProdutoGestaoOverlayAgro.objects.exclude(categoria="")
        if qtxt:
            oqs = oqs.filter(categoria__icontains=qtxt)
        for v in oqs.values_list("categoria", flat=True).distinct()[:2400 if completa else 240]:
            _add(v)
    else:
        for v in qs.exclude(unidade="").values_list("unidade", flat=True).distinct()[
            :5000 if completa else 400
        ]:
            _add(v)
        oqs = ProdutoGestaoOverlayAgro.objects.exclude(unidade="")
        if qtxt:
            oqs = oqs.filter(unidade__icontains=qtxt)
        for v in oqs.values_list("unidade", flat=True).distinct()[:2400 if completa else 240]:
            _add(v)

    out.sort(key=lambda x: x.lower())
    return out[:lim]


def lista_produto_externo_ids_por_categoria(categoria: str, *, limit: int = 800) -> list[str]:
    cat = str(categoria or "").strip()
    if not cat:
        return []
    lim = max(1, min(int(limit or 800), 1200))
    esc = cat[:200]
    qs = queryset_catalogo_ativos(inativos=False).filter(
        Q(categoria__icontains=esc) | Q(subcategoria__icontains=esc)
    )
    ids: list[str] = []
    seen: set[str] = set()
    for p in qs.order_by("nome")[: lim + 200]:
        pid = str(p.produto_externo_id or p.pk).strip()
        if not pid or pid in seen:
            continue
        seen.add(pid)
        ids.append(pid)
        if len(ids) >= lim:
            break
    for oid in _produto_overlay_ids_categoria_agro(cat):
        if oid not in seen and len(ids) < lim:
            seen.add(oid)
            ids.append(oid)
    return ids[:lim]


def doc_pedido_erp_por_externo_id(pid: str) -> dict | None:
    """Documento estilo Mongo para ``_linha_item_pedido_erp`` sem espelho ERP."""
    esc = str(pid or "").strip()
    if not esc:
        return None
    q = Q(produto_externo_id=esc) | Q(erp_produto_id=esc)
    if esc.isdigit():
        try:
            q |= Q(pk=int(esc))
        except ValueError:
            pass
    p = Produto.objects.filter(q).first()
    if p:
        pid_key = str(p.produto_externo_id or p.erp_produto_id or p.pk).strip()[:64]
        ov_map = _overlay_mapa_por_ids([pid_key])
        return row_para_doc_gestao_lista(
            produto_agro_para_row(
                p, ov=ov_map.get(pid_key), resolver_overlay_faltante=False
            )
        )
    ov = ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=esc[:64]).first()
    if not ov:
        return None
    row = {
        "id": esc,
        "nome": (ov.nome or "").strip() or esc,
        "codigo": (ov.codigo_nfe or ov.codigo_barras or esc).strip(),
        "codigo_nfe": (ov.codigo_nfe or esc).strip(),
        "codigo_barras": (ov.codigo_barras or "").strip(),
        "inativo": False,
    }
    return row_para_doc_gestao_lista(row)


def produtos_docs_relatorio_por_externo_ids(p_ids: list[str]) -> list[dict]:
    """Documentos estilo Mongo para planilhas Compras (categoria/unidade) sem catálogo Mongo."""
    ids: list[str] = []
    seen: set[str] = set()
    pk_cand: list[int] = []
    for raw in p_ids or []:
        s = str(raw or "").strip()
        if not s or s in seen:
            continue
        seen.add(s)
        ids.append(s[:64])
        if s.isdigit():
            try:
                pk_cand.append(int(s))
            except ValueError:
                pass
    if not ids:
        return []
    q = Q(produto_externo_id__in=ids[:900]) | Q(erp_produto_id__in=ids[:900])
    if pk_cand:
        q |= Q(pk__in=pk_cand[:900])
    produtos = list(Produto.objects.filter(q).distinct()[:1200])
    return [row_para_doc_gestao_lista(r) for r in _rows_de_produtos(produtos)]


def lista_produto_externo_ids_por_unidade(unidade: str, *, limit: int = 800) -> list[str]:
    uni = str(unidade or "").strip()
    if not uni:
        return []
    lim = max(1, min(int(limit or 800), 1200))
    qs = queryset_catalogo_ativos(inativos=False).filter(unidade__iexact=uni[:20])
    ids: list[str] = []
    seen: set[str] = set()
    for p in qs.order_by("nome")[:lim]:
        pid = str(p.produto_externo_id or p.pk).strip()
        if pid and pid not in seen:
            seen.add(pid)
            ids.append(pid)
    for oid in _produto_overlay_ids_unidade_agro(uni):
        if oid not in seen and len(ids) < lim:
            seen.add(oid)
            ids.append(oid)
    return ids[:lim]


def lista_produto_externo_ids_por_fornecedor(
    fornecedor_nome: str,
    fornecedor_id: str | None = None,
    *,
    limit: int = 800,
) -> tuple[list[str], dict[str, str]]:
    """IDs ativos cujo fornecedor no catálogo/overlay casa com nome (folha Compras sem Mongo)."""
    fn = str(fornecedor_nome or "").strip()
    _fid = str(fornecedor_id or "").strip()  # reservado — catálogo PG não tem id ERP de fornecedor
    if not fn and not _fid:
        return [], {}
    lim = max(1, min(int(limit or 800), 1200))
    ids: list[str] = []
    nomes: dict[str, str] = {}
    seen: set[str] = set()

    def _add(pid: str, nome: str = "") -> None:
        p = str(pid or "").strip()
        if not p or p in seen:
            return
        seen.add(p)
        ids.append(p)
        nm = str(nome or "").strip()
        if nm:
            nomes[p] = nm
            if p.isdigit():
                nomes[str(int(p))] = nm

    qs = queryset_catalogo_ativos(inativos=False)
    if fn:
        qs = qs.filter(fornecedor_texto__icontains=fn[:120])
    else:
        qs = qs.none()
    for p in qs.order_by("nome")[: lim + 200]:
        pid = str(p.produto_externo_id or p.pk).strip()
        _add(pid, (p.nome or "").strip())
        if len(ids) >= lim:
            break
    if fn:
        for oid in _produto_overlay_ids_fornecedor_agro(fn):
            if len(ids) >= lim:
                break
            if oid not in seen:
                _add(oid)
    return ids[:lim], nomes


def _produto_overlay_ids_categoria_agro(termo: str) -> list[str]:
    t = str(termo or "").strip()
    if not t:
        return []
    try:
        return [
            str(x).strip()
            for x in ProdutoGestaoOverlayAgro.objects.filter(categoria__iexact=t[:200]).values_list(
                "produto_externo_id", flat=True
            )[:1600]
            if x
        ]
    except Exception:
        return []


def _produto_overlay_ids_unidade_agro(termo: str) -> list[str]:
    t = str(termo or "").strip()
    if not t:
        return []
    try:
        return [
            str(x).strip()
            for x in ProdutoGestaoOverlayAgro.objects.filter(unidade__iexact=t[:20]).values_list(
                "produto_externo_id", flat=True
            )[:1600]
            if x
        ]
    except Exception:
        return []


def _produto_overlay_ids_fornecedor_agro(termo: str) -> list[str]:
    t = str(termo or "").strip()
    if not t:
        return []
    try:
        return [
            str(x).strip()
            for x in ProdutoGestaoOverlayAgro.objects.filter(
                fornecedor_texto__icontains=t[:120]
            ).values_list("produto_externo_id", flat=True)[:1600]
            if x
        ]
    except Exception:
        return []


def facetas_gestao(*, limite: int = 500) -> dict[str, list[str]]:
    """Marcas, categorias, subcategorias e fornecedores (Postgres + overlay)."""
    lim_cat = max(1, min(int(limite or 500), 2000))
    qs = queryset_catalogo_ativos(inativos=False)
    marcas = _faceta_valores_distintos(qs.exclude(marca="").values_list("marca", flat=True).distinct(), limite=0)
    categorias = _faceta_valores_distintos(
        qs.exclude(categoria="").values_list("categoria", flat=True).distinct(), limite=lim_cat
    )
    subcategorias = _faceta_valores_distintos(
        qs.exclude(subcategoria="").values_list("subcategoria", flat=True).distinct(), limite=lim_cat
    )
    fornecedores = _faceta_valores_distintos(
        qs.exclude(fornecedor_texto="").values_list("fornecedor_texto", flat=True).distinct(), limite=lim_cat + 200
    )

    ov_qs = ProdutoGestaoOverlayAgro.objects.all()
    marcas = _faceta_valores_distintos(
        list(marcas)
        + [x for x in ov_qs.exclude(marca="").values_list("marca", flat=True).distinct()],
        limite=0,
    )
    categorias = _faceta_valores_distintos(
        list(categorias)
        + [x for x in ov_qs.exclude(categoria="").values_list("categoria", flat=True).distinct()],
        limite=lim_cat,
    )
    subcategorias = _faceta_valores_distintos(
        list(subcategorias)
        + [x for x in ov_qs.exclude(subcategoria="").values_list("subcategoria", flat=True).distinct()],
        limite=lim_cat,
    )
    fornecedores = _faceta_valores_distintos(
        list(fornecedores)
        + [x for x in ov_qs.exclude(fornecedor_texto="").values_list("fornecedor_texto", flat=True).distinct()],
        limite=lim_cat + 200,
    )
    unidades = _faceta_valores_distintos(
        list(qs.exclude(unidade="").values_list("unidade", flat=True).distinct())
        + [x for x in ov_qs.exclude(unidade="").values_list("unidade", flat=True).distinct()],
        limite=lim_cat,
    )
    subcategorias_2 = _faceta_valores_distintos(
        list(qs.exclude(subcategoria_2="").values_list("subcategoria_2", flat=True).distinct())
        + [x for x in ov_qs.exclude(subcategoria_2="").values_list("subcategoria_2", flat=True).distinct()],
        limite=lim_cat,
    )
    subcategorias_3 = _faceta_valores_distintos(
        list(qs.exclude(subcategoria_3="").values_list("subcategoria_3", flat=True).distinct())
        + [x for x in ov_qs.exclude(subcategoria_3="").values_list("subcategoria_3", flat=True).distinct()],
        limite=lim_cat,
    )
    subcategorias_4 = _faceta_valores_distintos(
        list(qs.exclude(subcategoria_4="").values_list("subcategoria_4", flat=True).distinct())
        + [x for x in ov_qs.exclude(subcategoria_4="").values_list("subcategoria_4", flat=True).distinct()],
        limite=lim_cat,
    )
    modelos = _faceta_valores_distintos(
        list(qs.exclude(modelo="").values_list("modelo", flat=True).distinct()),
        limite=lim_cat,
    )
    return {
        "marcas": marcas,
        "categorias": categorias,
        "subcategorias": subcategorias,
        "subcategorias_2": subcategorias_2,
        "subcategorias_3": subcategorias_3,
        "subcategorias_4": subcategorias_4,
        "fornecedores": fornecedores,
        "unidades": unidades,
        "modelos": modelos,
    }


def listar_todos_rows_ativos() -> list[dict]:
    """Todos os produtos ativos do Postgres (catálogo ``agro_pg``).

    Uma query (sem OFFSET paginado) — montar o cache diário do PDV com N páginas
    ``ORDER BY … OFFSET`` sobrecarregava o agro-db na 1ª abertura do dia.
    """
    qs = queryset_catalogo_ativos(inativos=False).order_by("nome", "pk")
    return _rows_de_produtos(list(qs))


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
    """Remove fantasma Mongo (sem nome) quando Postgres trouxe o mesmo GM/código.

    Dois produtos **com nome** e mesmo GM (ex. GM9503 shampoo + teste) **permanecem**.
    """
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

        def _tem_nome(i: int) -> bool:
            return len(_nome_doc_busca_pdv(prods[i]).strip()) >= 2

        com_nome = [i for i in uniq if _tem_nome(i)]
        if len(com_nome) >= 2:
            continue

        def _rank(i: int) -> tuple:
            nome = _nome_doc_busca_pdv(prods[i])
            return (1 if nome else 0, len(nome), i)

        best = max(uniq, key=_rank)
        for i in uniq:
            if i != best and not _tem_nome(i):
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

    if not agro_pdv_merge_catalogo_postgres():
        return itens

    from integracoes.texto import normalizar

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


def listar_slim_rows_pdv() -> list[dict]:
    """Catálogo SLIM só Postgres p/ busca local do PDV (Plano B).

    Campos: id, nome, codigo, codigo_nfe, codigo_barras, preco_venda, index_codigos, busca_texto.
    Sem saldo, sem Mongo, sem N+1, sem hidratar modelos — ``values()`` + 1 batch de overlay.
    """
    from produtos.cadastro_busca_codigo_util import index_codigos_de_campos

    qs = (
        queryset_catalogo_ativos(inativos=False)
        .order_by("nome", "pk")
        .values(
            "pk",
            "produto_externo_id",
            "erp_produto_id",
            "nome",
            "marca",
            "modelo",
            "codigo_interno",
            "codigo_nfe",
            "codigo_barras",
            "preco_venda",
        )
    )
    rows_raw = list(qs)
    if not rows_raw:
        return []

    ids = [
        str(r.get("produto_externo_id") or r.get("erp_produto_id") or r.get("pk") or "").strip()[:64]
        for r in rows_raw
    ]
    ids = [i for i in ids if i]
    ov_map: dict[str, dict] = {}
    if ids:
        # batch em fatias (evita IN gigante)
        for i in range(0, len(ids), 900):
            fatia = ids[i : i + 900]
            for o in ProdutoGestaoOverlayAgro.objects.filter(
                produto_externo_id__in=fatia
            ).values(
                "produto_externo_id",
                "nome",
                "codigo_nfe",
                "codigo_barras",
                "preco_venda",
                "cadastro_extras",
            ):
                pid = str(o.get("produto_externo_id") or "").strip()[:64]
                if pid:
                    ov_map[pid] = o

    out: list[dict] = []
    for r in rows_raw:
        pid = str(r.get("produto_externo_id") or r.get("erp_produto_id") or r.get("pk") or "").strip()
        if not pid:
            continue
        ov = ov_map.get(pid[:64]) or {}
        nome = (ov.get("nome") or r.get("nome") or "").strip() or pid
        codigo = (r.get("codigo_interno") or "").strip()
        codigo_nfe = (ov.get("codigo_nfe") or r.get("codigo_nfe") or codigo or "").strip()
        codigo_barras = (ov.get("codigo_barras") or r.get("codigo_barras") or "").strip()
        preco = ov.get("preco_venda")
        if preco is None:
            preco = r.get("preco_venda")
        marca = (r.get("marca") or "").strip()
        modelo = (r.get("modelo") or "").strip()
        ce = ov.get("cadastro_extras") if isinstance(ov.get("cadastro_extras"), dict) else None
        from produtos.mongo_index_codigos import (
            _eans_embalagem_nf_de_cadastro_extras,
            codigos_barras_opcionais_de_cadastro_extras,
        )

        extras_ix = list(codigos_barras_opcionais_de_cadastro_extras(ce)) + list(
            _eans_embalagem_nf_de_cadastro_extras(ce)
        )
        ix = index_codigos_de_campos(
            codigo=codigo,
            codigo_nfe=codigo_nfe,
            codigo_barras=codigo_barras,
            extras=extras_ix,
        )
        busca = " ".join(
            x for x in (nome, marca, modelo, codigo, codigo_nfe, codigo_barras, *extras_ix) if x
        ).strip()
        # PreÃ§os A/B / por forma â€” sem isso o PDV adiciona do cache slim e a forma nÃ£o muda o valor.
        from produtos.precos_forma_pagamento_util import (
            extrair_precos_grupos_cadastro_extras,
            extrair_precos_modo_cadastro_extras,
            extrair_precos_por_forma_cadastro_extras,
        )

        modo = extrair_precos_modo_cadastro_extras(ce)
        pg = extrair_precos_grupos_cadastro_extras(ce)
        ppf = extrair_precos_por_forma_cadastro_extras(ce)
        if pg and modo != "grupos":
            modo = "grupos"
        row_slim: dict = {
            "id": pid,
            "nome": nome,
            "codigo": codigo,
            "codigo_nfe": codigo_nfe,
            "codigo_barras": codigo_barras,
            "preco_venda": _dec(preco),
            "index_codigos": ix if isinstance(ix, list) else [],
            "busca_texto": busca,
            # campos mÃ­nimos que o PDV espera em normalize
            "marca": marca,
            "preco_custo": 0.0,
            "preco_custo_final": 0.0,
            "saldo_centro": 0.0,
            "saldo_vila": 0.0,
            "precos_modo": modo,
        }
        if pg:
            row_slim["precos_grupos"] = pg
        if ppf:
            row_slim["precos_por_forma"] = ppf
        out.append(row_slim)
    return out


