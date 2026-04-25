import copy
import csv
import secrets
from functools import wraps
import json
import logging
import re
import time
import unicodedata
import hashlib
from datetime import date, datetime, time as dtime, timedelta, timezone
from urllib.parse import urlencode
from io import StringIO
from decimal import Decimal
from bson import ObjectId

from django.conf import settings
from decouple import config
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.db.models import Count, Q, Sum
from django.shortcuts import get_object_or_404, redirect, render
from django.http import HttpResponse, JsonResponse
from django.urls import reverse
from django.core.cache import cache
from django.templatetags.static import static
from django.views.decorators.cache import never_cache
from django.middleware.csrf import get_token
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_GET, require_http_methods, require_POST
from django.utils import timezone
from django.utils.safestring import mark_safe
from django.db import IntegrityError, transaction

from base.models import Empresa, Loja, PerfilUsuario, IntegracaoERP
from estoque.models import AjusteRapidoEstoque, OrigemAjusteEstoque, PedidoTransferencia
from .forms import ClienteAgroForm
from .models import (
    ClienteAgro,
    ItemVendaAgro,
    LancamentoAtalhoFiltro,
    OpcaoBaixaFinanceiroExtra,
    PedidoEntrega,
    EstoqueLote,
    ProdutoGestaoOverlayAgro,
    ProdutoGrupoAgro,
    ProdutoGrupoVarianteAgro,
    ProdutoMarcaVariacaoAgro,
    SessaoCaixa,
    VendaAgro,
    sync_overlay_validade_resumo_de_lotes,
)
from integracoes.texto import normalizar, expandir_tokens
from integracoes.venda_erp_mongo import VendaERPMongoClient
from integracoes.venda_erp_api import (
    VendaERPAPIClient,
    normalizar_linhas_ranking_vendedores_v3,
    normalizar_linhas_top_produtos_v3,
)
from .mongo_vendas_util import (
    _filtro_venda_ativa_mongo,
    dashboard_ranking_vendedores_mongo,
    dashboard_top_produtos_mongo,
)
from .nfe_entrada_util import (
    atualizar_rascunho_entrada,
    buscar_fornecedores_entrada_nfe,
    casar_produtos_mongo,
    excluir_rascunho_entrada,
    gravar_ult_nsu,
    listar_rascunhos_entrada,
    marcar_rascunho_estoque_aplicado,
    marcar_rascunho_financeiro_lancado,
    obter_rascunho_entrada,
    obter_ult_nsu,
    parse_nfe_xml_bytes,
    pipeline_acao_rascunho_entrada,
    salvar_rascunho_entrada,
)
from .mongo_index_codigos import (
    INDEX_CODIGOS_CAMPO,
    aplicar_index_codigos_no_mongo,
    merge_busca_codigo_prioridade_principal,
    produto_termo_bate_campos_principais,
)
from .rota_entregas_geo import ordenar_entregas_por_proximidade
from .lancamentos_financeiro_pdf import montar_pdf_financeiro_padrao
from .lancamentos_financeiro_xlsx import montar_planilha_financeiro_padrao
from .mongo_financeiro_util import (
    COL_DTO_LANCAMENTO,
    atualizar_lancamento_mongo_agro,
    baixar_lancamento_parcial_mongo,
    baixar_lancamentos_mongo,
    definir_lancamento_recorrente_mongo,
    contas_pagar_buscar_pagina,
    contas_pagar_montar_query_mongo,
    dre_resumo_simples_mongo,
    excluir_lancamento_mongo_agro,
    financeiro_projecao_fluxo_diario,
    inserir_lancamentos_manual_lote,
    split_decimal_em_parcelas,
    criar_emprestimo_externo_agro,
    emprestimo_defaults_para_ui,
    listar_emprestimos_agro,
    listar_lancamentos_emprestimo_do_mongo,
    mongo_emprestimo_como_item_agro,
    registrar_emprestimo_interno_agro,
    registrar_pagamento_emprestimo_interno_agro,
    excluir_pagamento_emprestimo_interno_agro,
    excluir_registro_emprestimo_interno_agro,
    LANCAMENTOS_ORDENACOES_VALIDAS,
    lancamentos_buscar_pagina,
    lancamentos_montar_query_mongo,
    dashboard_gerencial_linhas_financeiras,
    lancamentos_planos_distintos_no_filtro,
    lancamentos_sugestoes_campo,
    listar_formas_e_bancos_distintos,
    registrar_titulo_juros_apos_baixa_contas_pagar,
    montar_payload_erp_baixa,
    montar_payload_erp_lancamentos_novos,
    candidatos_texto_plano_para_api_pedido,
    documento_plano_mestre_por_id_mongo,
    resolver_plano_conta_para_pedido_erp,
)


logger = logging.getLogger(__name__)


def _dashboard_login_required(view_func):
    """login_required, exceto se settings.AGRO_PUBLIC_DASHBOARD (painel BI só leitura na web)."""
    protected = login_required(login_url="/admin/login/")(view_func)

    @wraps(view_func)
    def wrapper(request, *args, **kwargs):
        if getattr(settings, "AGRO_PUBLIC_DASHBOARD", False):
            return view_func(request, *args, **kwargs)
        return protected(request, *args, **kwargs)

    return wrapper


def _token_cron_alerta_valido(request) -> bool:
    token_cfg = (getattr(settings, "ALERTA_VENDAS_CRON_TOKEN", "") or "").strip()
    if (token_cfg.startswith('"') and token_cfg.endswith('"')) or (
        token_cfg.startswith("'") and token_cfg.endswith("'")
    ):
        token_cfg = token_cfg[1:-1].strip()
    if not token_cfg:
        return False
    token_q = (request.GET.get("token") or "").strip()
    token_h = (request.headers.get("X-Agro-Cron-Token") or "").strip()
    auth = (request.headers.get("Authorization") or "").strip()
    token_bearer = ""
    if auth.lower().startswith("bearer "):
        token_bearer = auth[7:].strip()
    for cand in (token_q, token_h, token_bearer):
        if cand and secrets.compare_digest(cand, token_cfg):
            return True
    return False

# --- CONEXÃO MONGO ---
_cached_mongo_client = None


def obter_conexao_mongo():
    global _cached_mongo_client
    try:
        if _cached_mongo_client is None:
            _cached_mongo_client = VendaERPMongoClient()
        db = _cached_mongo_client.db if _cached_mongo_client else None
        return _cached_mongo_client, db
    except Exception:
        _cached_mongo_client = None
        return None, None


# --- AUXILIARES GERAIS ---
def _somente_alnum(txt):
    return re.sub(r"[^a-zA-Z0-9]", "", str(txt or ""))


def _regex_exato_ci(valor):
    return re.compile(rf"^{re.escape(str(valor))}$", re.IGNORECASE)


def _regex_inicio_ci(valor):
    return re.compile(rf"^{re.escape(str(valor))}", re.IGNORECASE)


def _regex_contem_ci(valor):
    return re.compile(re.escape(str(valor)), re.IGNORECASE)


def _termo_parece_codigo(termo_original):
    termo = str(termo_original or "").strip()
    termo_limpo = _somente_alnum(termo)

    if not termo_limpo:
        return False

    # EAN / barras
    if termo_limpo.isdigit() and len(termo_limpo) >= 6:
        return True

    # Código misto tipo GM123 / 123ABC
    tem_letra = any(c.isalpha() for c in termo_limpo)
    tem_numero = any(c.isdigit() for c in termo_limpo)
    if tem_letra and tem_numero and len(termo_limpo) >= 3 and " " not in termo:
        return True

    # Prefixos comuns digitados
    if termo_limpo.lower().startswith("gm") and len(termo_limpo) >= 2:
        return True

    return False


def _extrair_codigo_barras(p):
    return (
        p.get("CodigoBarras")
        or p.get("EAN_NFe")
        or p.get("EAN")
        or p.get("CodigoDeBarras")
        or p.get("CodigoBarrasProduto")
        or p.get("GTIN")
        or ""
    )


def _float_api_json(val, default=0.0):
    """Garante número finito para JsonResponse — ``NaN``/``Inf`` quebram ``JSON.parse`` no navegador."""
    try:
        v = float(val if val is not None else default)
    except (TypeError, ValueError):
        return default
    if v != v or v in (float("inf"), float("-inf")):  # nan ou inf
        return default
    return v


def _mapear_estoques_por_produto(estoques, client):
    mapa = {}

    for e in estoques:
        # Não usar (ProdutoID or ""): Id 0 numérico viraria "" e perderia o saldo.
        pid = str(e.get("ProdutoID"))
        dep = str(e.get("DepositoID") or "")
        saldo = float(e.get("Saldo", 0) or 0)

        if pid not in mapa:
            mapa[pid] = {"centro": 0.0, "vila": 0.0}

        if dep == client.DEPOSITO_CENTRO:
            mapa[pid]["centro"] += saldo
        elif dep == client.DEPOSITO_VILA_ELIAS:
            mapa[pid]["vila"] += saldo

    return mapa


def _mapa_saldos_finais_por_produtos(db, client, p_ids):
    """
    Para cada produto em p_ids: saldos ERP (centro/vila) + ajuste rápido Django (PIN).
    Retorno: { pid_str: { saldo_centro, saldo_vila, saldo_erp_centro, saldo_erp_vila } }.
    """
    p_ids = [str(x) for x in p_ids if x is not None]
    if not p_ids:
        return {}
    estoques = list(
        db[client.col_e].find(
            {"ProdutoID": {"$in": p_ids}},
            {"ProdutoID": 1, "DepositoID": 1, "Saldo": 1, "_id": 0},
        )
    )
    estoque_map = _mapear_estoques_por_produto(estoques, client)
    ajustes_bd = AjusteRapidoEstoque.objects.all().order_by(
        "produto_externo_id", "deposito", "-criado_em"
    )
    ajustes_map = {}
    for aj in ajustes_bd:
        if (aj.produto_externo_id, aj.deposito) not in ajustes_map:
            ajustes_map[(aj.produto_externo_id, aj.deposito)] = aj
    out = {}
    for pid in p_ids:
        s_c = float(estoque_map.get(pid, {}).get("centro", 0.0))
        s_v = float(estoque_map.get(pid, {}).get("vila", 0.0))
        aj_c = ajustes_map.get((pid, "centro"))
        aj_v = ajustes_map.get((pid, "vila"))
        saldo_f_c = (
            float(aj_c.saldo_informado) + (s_c - float(aj_c.saldo_erp_referencia))
            if aj_c
            else s_c
        )
        saldo_f_v = (
            float(aj_v.saldo_informado) + (s_v - float(aj_v.saldo_erp_referencia))
            if aj_v
            else s_v
        )
        out[pid] = {
            "saldo_centro": round(saldo_f_c, 2),
            "saldo_vila": round(saldo_f_v, 2),
            "saldo_erp_centro": s_c,
            "saldo_erp_vila": s_v,
        }
    return out


def _gestao_doc_passa_status(p: dict, status_q: str) -> bool:
    if status_q == "inativos":
        return bool(p.get("CadastroInativo"))
    if status_q == "todos":
        return True
    return not bool(p.get("CadastroInativo"))


def _gestao_doc_passa_filtros(p: dict, marca: str, categoria: str, fornecedor: str) -> bool:
    if marca and str(p.get("Marca") or "").strip() != marca:
        return False
    if categoria:
        cat = str(p.get("NomeCategoria") or p.get("Categoria") or p.get("Grupo") or "").strip()
        if cat != categoria:
            return False
    if fornecedor:
        f = str(p.get("NomeFornecedor") or p.get("Fornecedor") or "").strip().lower()
        if fornecedor.strip().lower() not in f:
            return False
    return True


def _overlay_mapa_por_ids(ids: list[str]) -> dict[str, ProdutoGestaoOverlayAgro]:
    ids = [str(x) for x in ids if x]
    if not ids:
        return {}
    return {o.produto_externo_id: o for o in ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id__in=ids)}


def _linha_gestao_produto_json(
    p: dict, saldos: dict[str, dict], ov: ProdutoGestaoOverlayAgro | None
) -> dict:
    pid = str(p.get("Id") or p["_id"])
    s = saldos.get(pid) or {}
    sc = float(s.get("saldo_centro") or 0)
    sv = float(s.get("saldo_vila") or 0)
    codigo_nfe = str(p.get("CodigoNFe") or p.get("Codigo") or "").strip()
    cb = str(_extrair_codigo_barras(p) or "").strip()
    subcat = str(
        p.get("SubGrupo") or p.get("Subcategoria") or p.get("NomeSubcategoria") or ""
    ).strip()
    descricao = str(
        p.get("Descricao") or p.get("Observacao") or p.get("Complemento") or ""
    ).strip()
    nome = str(p.get("Nome") or "").strip()
    marca = str(p.get("Marca") or "").strip()
    cat = str(p.get("NomeCategoria") or p.get("Categoria") or p.get("Grupo") or "").strip()
    forn = str(
        p.get("NomeFornecedor") or p.get("Fornecedor") or p.get("RazaoSocialFornecedor") or ""
    ).strip()
    unidade = str(p.get("Unidade") or p.get("SiglaUnidade") or "").strip()
    tamanho = str(
        p.get("Modelo")
        or p.get("Tamanho")
        or p.get("Volume")
        or p.get("DescricaoTamanho")
        or ""
    ).strip()
    if not tamanho:
        tamanho = unidade
    img_url = _formatar_url_imagem(_extrair_imagem_produto(p, {}, pid))
    pv = _float_api_json(p.get("ValorVenda") or p.get("PrecoVenda") or 0)
    p_custo = _float_api_json(p.get("PrecoCusto") or p.get("ValorCusto") or 0)
    inativo_mongo = bool(p.get("CadastroInativo"))
    inativo = inativo_mongo
    tem_overlay = ov is not None
    ativo_exibicao = ov.ativo_exibicao if ov else None
    emin_c = emax_c = emin_v = emax_v = None
    if ov:
        if ov.nome.strip():
            nome = ov.nome.strip()
        if ov.marca.strip():
            marca = ov.marca.strip()
        if ov.categoria.strip():
            cat = ov.categoria.strip()
        if ov.fornecedor_texto.strip():
            forn = ov.fornecedor_texto.strip()
        if ov.unidade.strip():
            unidade = ov.unidade.strip()
        if ov.preco_venda is not None:
            pv = float(ov.preco_venda)
        if ov.ativo_exibicao is not None:
            inativo = not bool(ov.ativo_exibicao)
        if ov.estoque_min_centro is not None:
            emin_c = float(ov.estoque_min_centro)
        if ov.estoque_max_centro is not None:
            emax_c = float(ov.estoque_max_centro)
        if ov.estoque_min_vila is not None:
            emin_v = float(ov.estoque_min_vila)
        if ov.estoque_max_vila is not None:
            emax_v = float(ov.estoque_max_vila)
        if ov.codigo_barras.strip():
            cb = ov.codigo_barras.strip()
        if ov.codigo_nfe.strip():
            codigo_nfe = ov.codigo_nfe.strip()
        if ov.subcategoria.strip():
            subcat = ov.subcategoria.strip()
        if ov.descricao.strip():
            descricao = ov.descricao.strip()
    return {
        "id": pid,
        "nome": nome,
        "codigo_gm": codigo_nfe,
        "codigo_barras": cb,
        "subcategoria": subcat,
        "tamanho": tamanho,
        "imagem": img_url or "",
        "descricao": descricao,
        "marca": marca,
        "categoria": cat,
        "fornecedor": forn,
        "unidade": unidade,
        "preco_custo": round(p_custo, 2),
        "preco_venda": round(pv, 2),
        "saldo_centro": round(sc, 2),
        "saldo_vila": round(sv, 2),
        "saldo_total": round(sc + sv, 2),
        "inativo": inativo,
        "inativo_mongo": inativo_mongo,
        "ativo_exibicao": ativo_exibicao,
        "tem_overlay": tem_overlay,
        "estoque_min_centro": emin_c,
        "estoque_max_centro": emax_c,
        "estoque_min_vila": emin_v,
        "estoque_max_vila": emax_v,
    }


def _aplicar_produto_gestao_overlay_em_dict(
    row: dict, ov: ProdutoGestaoOverlayAgro | None
) -> dict:
    """Sobrescreve campos de exibição no Agro (PDV, cadastro ERP, APIs) sem alterar o Mongo."""
    if not ov:
        return row
    if ov.nome.strip():
        row["nome"] = ov.nome.strip()
    if ov.marca.strip():
        row["marca"] = ov.marca.strip()
    if ov.categoria.strip():
        row["categoria"] = ov.categoria.strip()
    if ov.fornecedor_texto.strip():
        row["fornecedor"] = ov.fornecedor_texto.strip()
    if ov.unidade.strip():
        row["unidade"] = ov.unidade.strip()
    if ov.preco_venda is not None:
        row["preco_venda"] = round(float(ov.preco_venda), 2)
    if ov.codigo_barras.strip():
        row["codigo_barras"] = ov.codigo_barras.strip()
    if ov.codigo_nfe.strip():
        cn = ov.codigo_nfe.strip()
        row["codigo_nfe"] = cn
        row["codigo"] = cn
    if ov.subcategoria.strip():
        row["subcategoria"] = ov.subcategoria.strip()
    if ov.descricao.strip():
        row["descricao"] = ov.descricao.strip()
    if ov.ativo_exibicao is not None:
        row["inativo"] = not bool(ov.ativo_exibicao)
        if "cadastro_inativo" in row:
            row["cadastro_inativo"] = bool(row["inativo"])
    row["ativo_exibicao"] = ov.ativo_exibicao
    return row


def _overlay_mapa_por_ids_chunked(ids: list[str]) -> dict[str, ProdutoGestaoOverlayAgro]:
    ids_u = [str(x) for x in ids if x]
    acc: dict[str, ProdutoGestaoOverlayAgro] = {}
    step = 400
    for i in range(0, len(ids_u), step):
        acc.update(_overlay_mapa_por_ids(ids_u[i : i + step]))
    return acc


def _mongo_produtos_por_overlay_codigo_busca(
    q_raw: str, db, client_m, ja_ids: set[str]
) -> list[dict]:
    """Resolve produtos pelo código/barras gravados só no overlay Agro (SQLite) e variações locais.

    Similares exclusivos do espelho ERP/Mongo entram pelo ``motor_de_busca_agro`` (``$elemMatch``).
    """
    q_raw = str(q_raw or "").strip()
    if not q_raw or not _termo_parece_codigo(q_raw):
        return []
    tl = _somente_alnum(q_raw)
    q0 = Q(codigo_barras__iexact=q_raw) | Q(codigo_nfe__iexact=q_raw)
    if tl and tl != q_raw:
        q0 |= Q(codigo_barras__iexact=tl) | Q(codigo_nfe__iexact=tl)
    pids = list(
        ProdutoGestaoOverlayAgro.objects.filter(q0).values_list("produto_externo_id", flat=True)[:30]
    )
    qv = (
        Q(codigo_barras__iexact=q_raw)
        | Q(codigo_fornecedor__iexact=q_raw)
        | Q(codigo_interno__iexact=q_raw)
    )
    if tl and tl != q_raw:
        qv |= (
            Q(codigo_barras__iexact=tl)
            | Q(codigo_fornecedor__iexact=tl)
            | Q(codigo_interno__iexact=tl)
        )
    pids_m = list(
        ProdutoMarcaVariacaoAgro.objects.filter(qv).values_list("produto_externo_id", flat=True)[:30]
    )
    seen_p: set[str] = set()
    out: list[dict] = []
    for raw_pid in pids + pids_m:
        ps = str(raw_pid or "").strip()
        if not ps or ps in ja_ids or ps in seen_p:
            continue
        seen_p.add(ps)
        doc = _produto_mongo_por_id_externo(db, client_m, ps)
        if doc:
            out.append(doc)
    return out


@login_required(login_url="/admin/login/")
@require_GET
def api_produtos_gestao_facetas(request):
    """Marcas, categorias, subcategorias e fornecedores distintos (Mongo) para filtros da gestão."""
    client, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"ok": False, "erro": "Mongo indisponível"}, status=503)
    col = db[client.col_p]
    base = {"CadastroInativo": {"$ne": True}}
    try:
        marcas = sorted(
            {str(x).strip() for x in col.distinct("Marca", base) if str(x or "").strip()},
            key=lambda s: s.lower(),
        )[:200]
        cats: set[str] = set()
        for k in ("NomeCategoria", "Categoria", "Grupo"):
            for x in col.distinct(k, base):
                s = str(x or "").strip()
                if s:
                    cats.add(s)
        categorias = sorted(cats, key=lambda s: s.lower())[:200]
        subs: set[str] = set()
        for k in ("SubGrupo", "Subcategoria", "NomeSubcategoria"):
            for x in col.distinct(k, base):
                s = str(x or "").strip()
                if s:
                    subs.add(s)
        subcategorias = sorted(subs, key=lambda s: s.lower())[:200]
        forns: set[str] = set()
        for k in ("NomeFornecedor", "Fornecedor"):
            for x in col.distinct(k, base):
                s = str(x or "").strip()
                if s:
                    forns.add(s)
        fornecedores = sorted(forns, key=lambda s: s.lower())[:300]
    except Exception as e:
        logger.warning("api_produtos_gestao_facetas: %s", e, exc_info=True)
        return JsonResponse({"ok": False, "erro": str(e)}, status=500)
    return JsonResponse(
        {
            "ok": True,
            "marcas": marcas,
            "categorias": categorias,
            "subcategorias": subcategorias,
            "fornecedores": fornecedores,
        }
    )


@login_required(login_url="/admin/login/")
@require_GET
def api_produtos_gestao_lista(request):
    """
    Lista paginada para gestão operacional: saldos centro/vila (Agro), merge com overlay local.
    """
    client, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"ok": False, "erro": "Mongo indisponível", "produtos": []}, status=503)

    q_raw = str(request.GET.get("q") or "").strip()
    status_q = str(request.GET.get("status") or "ativos").strip().lower()
    marca_f = str(request.GET.get("marca") or "").strip()
    cat_f = str(request.GET.get("categoria") or "").strip()
    forn_f = str(request.GET.get("fornecedor") or "").strip()

    try:
        por_pagina = int(request.GET.get("por_pagina") or 40)
    except ValueError:
        por_pagina = 40
    por_pagina = max(10, min(por_pagina, 80))

    try:
        pagina = int(request.GET.get("pagina") or 1)
    except ValueError:
        pagina = 1
    pagina = max(1, pagina)

    include_inactive = status_q in ("todos", "inativos")

    try:
        if q_raw:
            prods = motor_de_busca_agro(
                q_raw, db, client, limit=120, include_inactive=include_inactive
            )
            prods = [
                p
                for p in prods
                if _gestao_doc_passa_status(p, status_q)
                and _gestao_doc_passa_filtros(p, marca_f, cat_f, forn_f)
            ]
            total = len(prods)
            skip = (pagina - 1) * por_pagina
            chunk = prods[skip : skip + por_pagina]
            has_more = skip + por_pagina < total
        else:
            clauses: list[dict] = []
            if status_q == "inativos":
                clauses.append({"CadastroInativo": True})
            elif status_q != "todos":
                clauses.append({"CadastroInativo": {"$ne": True}})
            if marca_f:
                clauses.append({"Marca": marca_f})
            if cat_f:
                clauses.append(
                    {
                        "$or": [
                            {"NomeCategoria": cat_f},
                            {"Categoria": cat_f},
                            {"Grupo": cat_f},
                        ]
                    }
                )
            if forn_f:
                clauses.append(
                    {"NomeFornecedor": {"$regex": re.escape(forn_f), "$options": "i"}}
                )
            filtro = {"$and": clauses} if len(clauses) > 1 else (clauses[0] if clauses else {})
            skip = (pagina - 1) * por_pagina
            cur = (
                db[client.col_p]
                .find(filtro)
                .sort("Nome", 1)
                .skip(skip)
                .limit(por_pagina + 1)
            )
            chunk = list(cur)
            has_more = len(chunk) > por_pagina
            chunk = chunk[:por_pagina]
            total = None

        p_ids = [str(p.get("Id") or p["_id"]) for p in chunk]
        saldos = _mapa_saldos_finais_por_produtos(db, client, p_ids)
        ovs = _overlay_mapa_por_ids(p_ids)
        rows = [_linha_gestao_produto_json(p, saldos, ovs.get(str(p.get("Id") or p["_id"]))) for p in chunk]
        return JsonResponse(
            {
                "ok": True,
                "modo": "busca" if q_raw else "lista",
                "pagina": pagina,
                "por_pagina": por_pagina,
                "has_more": has_more,
                "total": total,
                "produtos": rows,
            }
        )
    except Exception as e:
        logger.warning("api_produtos_gestao_lista: %s", e, exc_info=True)
        return JsonResponse({"ok": False, "erro": str(e), "produtos": []}, status=500)


@login_required(login_url="/admin/login/")
@require_POST
def api_produtos_gestao_ajuste_estoque(request):
    """
    Ajuste de saldo exibido (camada Agro / AjusteRapidoEstoque), sem alterar Mongo ERP.
    JSON: produto_id, saldo_centro (opcional), saldo_vila (opcional) — valores absolutos desejados.
    """
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)
    pid = str(payload.get("produto_id") or "").strip()
    if not pid:
        return JsonResponse({"ok": False, "erro": "produto_id obrigatório"}, status=400)

    def _dec(key):
        raw = payload.get(key)
        if raw is None or str(raw).strip() == "":
            return None
        try:
            return Decimal(str(raw).replace(",", ".").strip())
        except Exception:
            raise ValueError(f"Valor inválido: {key}")

    try:
        novo_c = _dec("saldo_centro")
        novo_v = _dec("saldo_vila")
    except ValueError as e:
        return JsonResponse({"ok": False, "erro": str(e)}, status=400)
    if novo_c is None and novo_v is None:
        return JsonResponse({"ok": False, "erro": "Informe saldo_centro e/ou saldo_vila"}, status=400)

    client, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"ok": False, "erro": "Mongo indisponível"}, status=503)

    doc = _produto_mongo_por_id_externo(db, client, pid)
    if not doc:
        return JsonResponse({"ok": False, "erro": "Produto não encontrado no espelho."}, status=404)
    nome_p = str(doc.get("Nome") or "")[:200]
    codigo = str(doc.get("CodigoNFe") or doc.get("Codigo") or "")[:100]

    empresa = Empresa.objects.filter(nome_fantasia="Agro Mais").first() or Empresa.objects.first()

    try:
        with transaction.atomic():
            if novo_c is not None:
                erp_c = _saldo_erp_produto_deposito_mongo(db, client, pid, "centro")
                AjusteRapidoEstoque.objects.create(
                    empresa=empresa,
                    produto_externo_id=pid[:100],
                    codigo_interno=codigo,
                    nome_produto=(nome_p or pid)[:255],
                    deposito="centro",
                    saldo_erp_referencia=erp_c,
                    saldo_informado=novo_c,
                    origem=OrigemAjusteEstoque.OUTRO,
                    observacao="Gestão produtos — ajuste centro",
                    usuario=request.user if request.user.is_authenticated else None,
                )
            if novo_v is not None:
                erp_v = _saldo_erp_produto_deposito_mongo(db, client, pid, "vila")
                AjusteRapidoEstoque.objects.create(
                    empresa=empresa,
                    produto_externo_id=pid[:100],
                    codigo_interno=codigo,
                    nome_produto=(nome_p or pid)[:255],
                    deposito="vila",
                    saldo_erp_referencia=erp_v,
                    saldo_informado=novo_v,
                    origem=OrigemAjusteEstoque.OUTRO,
                    observacao="Gestão produtos — ajuste vila",
                    usuario=request.user if request.user.is_authenticated else None,
                )
        _invalidar_caches_apos_ajuste_pin()
    except Exception as e:
        logger.warning("api_produtos_gestao_ajuste_estoque: %s", e, exc_info=True)
        return JsonResponse({"ok": False, "erro": str(e)}, status=500)

    saldos = _mapa_saldos_finais_por_produtos(db, client, [pid])
    ov = ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid).first()
    row = _linha_gestao_produto_json(doc or {"Id": pid, "Nome": nome_p}, saldos, ov)
    return JsonResponse({"ok": True, "produto": row})


@login_required(login_url="/admin/login/")
@require_POST
def api_produtos_gestao_overlay_salvar(request):
    """Grava ou atualiza overlay local (edição de cadastro na gestão)."""
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)
    pid = str(payload.get("produto_id") or "").strip()
    if not pid:
        return JsonResponse({"ok": False, "erro": "produto_id obrigatório"}, status=400)

    def _txt(key, mx=300):
        return str(payload.get(key) or "").strip()[:mx]

    def _dec_opt(key):
        v = payload.get(key)
        if v is None or str(v).strip() == "":
            return None
        try:
            return Decimal(str(v).replace(",", ".").strip())
        except Exception:
            return None

    ov, _ = ProdutoGestaoOverlayAgro.objects.get_or_create(
        produto_externo_id=pid[:64],
        defaults={"usuario": request.user if request.user.is_authenticated else None},
    )
    if "nome" in payload:
        ov.nome = _txt("nome", 300)
    if "marca" in payload:
        ov.marca = _txt("marca", 120)
    if "categoria" in payload:
        ov.categoria = _txt("categoria", 200)
    if "fornecedor_texto" in payload:
        ov.fornecedor_texto = _txt("fornecedor_texto", 300)
    if "unidade" in payload:
        ov.unidade = _txt("unidade", 20)
    if "codigo_barras" in payload:
        ov.codigo_barras = _txt("codigo_barras", 80)
    if "codigo_nfe" in payload:
        ov.codigo_nfe = _txt("codigo_nfe", 64)
    if "subcategoria" in payload:
        ov.subcategoria = _txt("subcategoria", 200)
    if "descricao" in payload:
        ov.descricao = str(payload.get("descricao") or "")[:16000]
    pv = payload.get("preco_venda")
    if pv is not None:
        if str(pv).strip() == "":
            ov.preco_venda = None
        else:
            try:
                ov.preco_venda = Decimal(str(pv).replace(",", ".").strip()).quantize(Decimal("0.01"))
            except Exception:
                return JsonResponse({"ok": False, "erro": "preço inválido"}, status=400)
    if "ativo_exibicao" in payload:
        ae = payload.get("ativo_exibicao")
        if ae is None or str(ae).strip() == "":
            ov.ativo_exibicao = None
        else:
            ov.ativo_exibicao = str(ae).strip().lower() in ("1", "true", "yes", "on", "sim", "s")
    for fld, key in (
        ("estoque_min_centro", "estoque_min_centro"),
        ("estoque_max_centro", "estoque_max_centro"),
        ("estoque_min_vila", "estoque_min_vila"),
        ("estoque_max_vila", "estoque_max_vila"),
    ):
        if key in payload:
            d = _dec_opt(key)
            setattr(ov, fld, d)
    ov.usuario = request.user if request.user.is_authenticated else ov.usuario

    client, db = obter_conexao_mongo()
    p_doc = _produto_mongo_por_id_externo(db, client, pid) if db is not None else None

    variacoes_novas: list[ProdutoMarcaVariacaoAgro] | None = None
    if "variacoes" in payload:
        rawv = payload.get("variacoes")
        if rawv is None:
            rawv = []
        if not isinstance(rawv, list):
            return JsonResponse({"ok": False, "erro": "variacoes deve ser uma lista"}, status=400)
        if len(rawv) > 200:
            return JsonResponse({"ok": False, "erro": "no máximo 200 variações"}, status=400)
        variacoes_novas = []
        for i, it in enumerate(rawv):
            if not isinstance(it, dict):
                continue
            marca = str(it.get("marca") or "").strip()[:120]
            cb = str(it.get("codigo_barras") or "").strip()[:80]
            cf = str(it.get("codigo_fornecedor") or "").strip()[:80]
            ci = str(it.get("codigo_interno") or "").strip()[:80]
            if not marca and not cb and not cf and not ci:
                continue
            try:
                est = Decimal(str(it.get("estoque") or 0).replace(",", ".").strip() or 0)
            except Exception:
                est = Decimal(0)
            try:
                cu = Decimal(str(it.get("custo_unitario") or 0).replace(",", ".").strip() or 0)
            except Exception:
                cu = Decimal(0)
            variacoes_novas.append(
                ProdutoMarcaVariacaoAgro(
                    produto_externo_id=pid[:64],
                    codigo_interno=ci,
                    marca=marca or "—",
                    codigo_barras=cb,
                    codigo_fornecedor=cf,
                    estoque=est.quantize(Decimal("0.0001")),
                    custo_unitario=cu.quantize(Decimal("0.0001")),
                    ordem=i,
                )
            )

    custo_payload = _dec_opt("preco_custo") if "preco_custo" in payload else None
    expected: Decimal | None = None
    if db is not None and p_doc and _mongo_primeiro_bool(
        p_doc, ("Kit", "ProdutoKit", "EhKit", "EKit", "IndicaKit")
    ):
        comp0 = _extrair_composicao_produto_mongo(p_doc)
        if comp0:
            kk = _custo_total_kit_composicao_agro(db, client, p_doc)
            if kk is not None:
                expected = kk
    if expected is None and variacoes_novas is not None:
        expected = _custo_medio_ponderado_variacoes_rows(variacoes_novas)

    if expected is not None and custo_payload is not None:
        if abs(expected - custo_payload) > Decimal("0.06"):
            return JsonResponse(
                {
                    "ok": False,
                    "erro": "Preço de custo não confere com o cálculo esperado (KIT ou média ponderada das marcas).",
                },
                status=400,
            )

    ex: dict = {}
    if isinstance(getattr(ov, "cadastro_extras", None), dict):
        ex = dict(ov.cadastro_extras)
    if "fiscal" in payload and isinstance(payload.get("fiscal"), dict):
        f_in = payload["fiscal"]
        f_prev = dict(ex.get("fiscal") or {}) if isinstance(ex.get("fiscal"), dict) else {}
        for k, mx in (("ncm", 14), ("cest", 10), ("cfop", 7), ("csosn", 7), ("origem", 4)):
            if k in f_in:
                f_prev[k] = str(f_in.get(k) or "").strip()[:mx]
        ex["fiscal"] = f_prev
    if "kit" in payload and isinstance(payload.get("kit"), dict):
        k_in = payload["kit"]
        k_prev = dict(ex.get("kit") or {}) if isinstance(ex.get("kit"), dict) else {}
        if "baixa_componentes" in k_in:
            k_prev["baixa_componentes"] = bool(k_in.get("baixa_componentes"))
        if "deposito" in k_in:
            k_prev["deposito"] = str(k_in.get("deposito") or "").strip()[:16]
        ex["kit"] = k_prev
    if "permite_venda_estoque_negativo" in payload:
        pvneg = payload.get("permite_venda_estoque_negativo")
        if isinstance(pvneg, bool):
            ex["permite_venda_estoque_negativo"] = pvneg
        else:
            ex["permite_venda_estoque_negativo"] = str(pvneg or "").strip().lower() in (
                "1",
                "true",
                "yes",
                "on",
                "sim",
                "s",
            )
    if "extra_validade" in payload:
        v = str(payload.get("extra_validade") or "").strip()[:16]
        if v:
            ex["validade"] = v
            try:
                dv = datetime.strptime(v[:10], "%Y-%m-%d").date()
                if dv.year >= 2000:
                    ex["validade_alerta"] = False
                    ex.pop("validade_msg", None)
            except (ValueError, TypeError):
                pass
        else:
            ex.pop("validade", None)
            ex.pop("validade_alerta", None)
            ex.pop("validade_msg", None)
    if "extra_lote" in payload:
        l = str(payload.get("extra_lote") or "").strip()[:80]
        if l:
            ex["lote"] = l
        else:
            ex.pop("lote", None)
    ov.cadastro_extras = ex

    with transaction.atomic():
        ov.save()
        if variacoes_novas is not None:
            ProdutoMarcaVariacaoAgro.objects.filter(produto_externo_id=pid[:64]).delete()
            if variacoes_novas:
                ProdutoMarcaVariacaoAgro.objects.bulk_create(variacoes_novas)

    try:
        cache.delete(CATALOGO_PDV_CACHE_ENTRY_KEY)
        cache.delete(CATALOGO_PDV_CACHE_PREV_ENTRY_KEY)
    except Exception:
        pass

    if db is None:
        return JsonResponse({"ok": True, "aviso": "Mongo indisponível — overlay salvo.", "produto": None})
    doc = _produto_mongo_por_id_externo(db, client, pid) or {"Id": pid}
    if doc.get("_id"):
        try:
            aplicar_index_codigos_no_mongo(db, client.col_p, doc, produto_externo_id=pid)
        except Exception:
            logger.warning("api_produtos_gestao_overlay_salvar: index_codigos", exc_info=True)
    saldos = _mapa_saldos_finais_por_produtos(db, client, [pid])
    row = _linha_gestao_produto_json(doc, saldos, ov)
    return JsonResponse({"ok": True, "produto": row})


# Lista de clientes PDV (ClienteAgro) — cache curto; invalida em sync e em save/delete (signals).
API_LIST_CUSTOMERS_CACHE_KEY = "api_list_customers_v1"
API_LIST_CUSTOMERS_TTL = 45

# Catálogo PDV: um snapshot por dia civil (TIME_ZONE) + invalidação manual. Estoque ao vivo via /api/pdv/saldos/.
CATALOGO_PDV_CACHE_ENTRY_KEY = "pdv_catalogo_produtos_por_dia_v1"
CATALOGO_PDV_CACHE_PREV_ENTRY_KEY = "pdv_catalogo_produtos_prev_v1"

# Snapshot de saldos: vários caixas/abas compartilham; TTL curto protege o Mongo sem atrasar o PDV.
_SALDOS_PDV_CACHE_KEY = "pdv_saldos_compacto_snapshot_v1"
_SALDOS_PDV_CACHE_TTL = 5

_METRICAS_PDV_BUCKETS_INVALIDAR = 4
_METRICAS_PDV_DIAS_COMUNS = (7, 14, 21, 28, 30, 45, 60, 90, 120, 180, 365)


def _pdv_metricas_cache_key(dias: int, bucket: int) -> str:
    return f"pdv_metricas_v4_{dias}_{bucket}"


def _pdv_top_vendidos_cache_key(dias: int, limite: int, bucket: int) -> str:
    return f"pdv_top_vend_v3_{dias}_{limite}_{bucket}"


def _pdv_top_v_float(v):
    """Evita falha do JsonResponse com Decimal128, Decimal ou float não finito do Mongo."""
    if v is None:
        return 0.0
    try:
        if hasattr(v, "to_decimal"):
            x = float(v.to_decimal())
        elif isinstance(v, Decimal):
            x = float(v)
        else:
            x = float(v)
    except (TypeError, ValueError, ArithmeticError, AttributeError):
        return 0.0
    if x != x or x == float("inf") or x == float("-inf"):
        return 0.0
    return x


def _pdv_top_v_texto_produto(v, fallback: str = "") -> str:
    if v is None:
        return fallback
    if isinstance(v, (dict, list, bytes)):
        return fallback
    s = str(v).strip()
    return s if s else fallback


_TOP_VENDIDOS_LIMITES_CACHE = (10, 15, 20)


def _invalidar_cache_saldos_pdv():
    cache.delete(_SALDOS_PDV_CACHE_KEY)


def _invalidar_cache_metricas_pdv():
    b = int(time.time() // 300)
    for dias in _METRICAS_PDV_DIAS_COMUNS:
        for bb in range(b, b - _METRICAS_PDV_BUCKETS_INVALIDAR - 1, -1):
            cache.delete(_pdv_metricas_cache_key(dias, bb))
            for lim in _TOP_VENDIDOS_LIMITES_CACHE:
                cache.delete(_pdv_top_vendidos_cache_key(dias, lim, bb))


def _invalidar_caches_apos_ajuste_pin():
    _invalidar_cache_saldos_pdv()
    cache.delete(CATALOGO_PDV_CACHE_ENTRY_KEY)
    cache.delete(_CACHE_MEDIAS_VENDA_ENTRY)
    _invalidar_cache_metricas_pdv()


def _indice_semana_4(dt, now) -> int | None:
    """0 = semana mais antiga (dias 28–22), 3 = últimos 7 dias."""
    if dt is None or not isinstance(dt, datetime):
        return None
    if dt < now - timedelta(days=28):
        return None
    if dt >= now - timedelta(days=7):
        return 3
    if dt >= now - timedelta(days=14):
        return 2
    if dt >= now - timedelta(days=21):
        return 1
    return 0


def _metricas_vendas_agregadas_por_produto(db, dias_media: int):
    """
    Uma passagem em DtoVenda + DtoVendaProduto:
    - totais no período [now-dias_media, now] para média diária
    - últimos 7d vs 7d anteriores (variação semanal)
    - quantidades por semana nos últimos 28d (4 faixas, para sparkline Compras)
    """
    now = datetime.now()
    t_m = now - timedelta(days=dias_media)
    t_w0 = now - timedelta(days=7)
    t_w1 = now - timedelta(days=14)
    t_28 = now - timedelta(days=28)
    limite = min(t_m, t_w1, t_28)
    q = {"Data": {"$gte": limite}, **_filtro_venda_ativa_mongo()}
    vendas = list(db["DtoVenda"].find(q, {"Id": 1, "_id": 1, "Data": 1}))
    if not vendas:
        return {}, {}, {}, {}
    vmap = {}
    for v in vendas:
        dt = v.get("Data")
        for key in (str(v.get("Id")), str(v.get("_id"))):
            if key and key != "None":
                vmap[key] = dt
    venda_ids_obj = []
    venda_ids_str = []
    for v in vendas:
        vid = str(v.get("Id") or v.get("_id"))
        venda_ids_str.append(vid)
        if len(vid) == 24:
            try:
                venda_ids_obj.append(ObjectId(vid))
            except Exception:
                pass
    query_itens = {
        "$or": [
            {"VendaID": {"$in": venda_ids_obj}},
            {"VendaID": {"$in": venda_ids_str}},
        ]
    }
    media_tot = {}
    w0 = {}
    w1 = {}
    spark: dict[str, list[float]] = {}
    for item in db["DtoVendaProduto"].find(query_itens):
        # Não usar (ProdutoID or ""): Id numérico 0 sumiria.
        raw_pid = item.get("ProdutoID")
        if raw_pid is None:
            continue
        pid = str(raw_pid)
        if not pid or pid == "None":
            continue
        vid_raw = item.get("VendaID")
        vid = str(vid_raw) if vid_raw is not None else ""
        dt = vmap.get(vid)
        if dt is None or not isinstance(dt, datetime):
            continue
        try:
            qtd = float(item.get("Quantidade") or 0)
        except (TypeError, ValueError):
            qtd = 0.0
        if dt >= t_m:
            media_tot[pid] = media_tot.get(pid, 0.0) + qtd
        if dt >= t_w0:
            w0[pid] = w0.get(pid, 0.0) + qtd
        if t_w1 <= dt < t_w0:
            w1[pid] = w1.get(pid, 0.0) + qtd
        bi = _indice_semana_4(dt, now)
        if bi is not None:
            if pid not in spark:
                spark[pid] = [0.0, 0.0, 0.0, 0.0]
            spark[pid][bi] += qtd
    return media_tot, w0, w1, spark


def _merge_ultima_entrada_entrada_nota_fiscal_mov_estoque(db, agregado, since, names):
    """
    Última entrada vinculada a NF-e: registros de movimentação de estoque com tipo
    EntradaNotaFiscal (como no painel do ERP). Mescla em agregado competindo pela
    data mais recente por produto.
    """
    candidatas = []
    for n in (
        "DtoMovimentacaoEstoque",
        "DtoMovimentacaoEstoqueProduto",
        "DtoHistoricoMovimentacaoEstoque",
        "DtoLogMovimentacaoEstoque",
        "DtoRegistroMovimentacaoEstoque",
        "MovimentacaoEstoque",
    ):
        if n in names:
            candidatas.append(n)
    for n in sorted(names):
        if len(candidatas) >= 14:
            break
        low = n.lower()
        if "moviment" in low and "estoq" in low and n not in candidatas:
            candidatas.append(n)

    tipo_entrada_nfe = {"$regex": r"^EntradaNotaFiscal$", "$options": "i"}
    match_tipo = {
        "$or": [
            {"Movimentacao": tipo_entrada_nfe},
            {"TipoMovimentacao": tipo_entrada_nfe},
            {"TipoMovimentacaoEstoque": tipo_entrada_nfe},
        ]
    }

    def _absorver(pid, dt, qtd):
        if not pid or pid in ("None", "null"):
            return
        try:
            qtd = float(qtd or 0)
        except (TypeError, ValueError):
            qtd = 0.0
        prev = agregado.get(pid)
        if prev is None or dt > prev[0]:
            agregado[pid] = (dt, qtd)
        elif dt == prev[0]:
            agregado[pid] = (dt, prev[1] + qtd)

    for col_name in candidatas:
        try:
            coll = db[col_name]
            match = {
                "$and": [
                    {
                        "$or": [
                            {"Data": {"$gte": since}},
                            {"DataMovimentacao": {"$gte": since}},
                        ]
                    },
                    {"Cancelada": {"$ne": True}},
                    match_tipo,
                ]
            }
            pipeline = [
                {"$match": match},
                {
                    "$addFields": {
                        "_ord": {"$ifNull": ["$Data", "$DataMovimentacao"]},
                    }
                },
                {"$match": {"_ord": {"$gte": since}}},
                {"$sort": {"_ord": -1}},
                {
                    "$group": {
                        "_id": "$ProdutoID",
                        "ultimaData": {"$first": "$_ord"},
                        "qtd": {
                            "$first": {
                                "$ifNull": [
                                    "$Quantidade",
                                    {"$ifNull": ["$Qtd", 0]},
                                ]
                            }
                        },
                    }
                },
            ]
            for row in coll.aggregate(pipeline, allowDiskUse=True):
                pid_raw = row.get("_id")
                pid = str(pid_raw) if pid_raw is not None else ""
                _absorver(pid, row.get("ultimaData"), row.get("qtd"))
        except Exception as exc:
            logger.warning("ultima_entrada EntradaNotaFiscal %s: %s", col_name, exc)


def _ultima_entrada_mercadoria_por_produto(db):
    """
    Melhor esforço: movimentação EntradaNotaFiscal no estoque; depois DtoCompra*,
    DtoPedidoCompra*, DtoNotaEntrada* (se existirem).
    Retorno: { pid: {"data": iso str, "qtd": float} }
    """
    agregado = {}
    try:
        names = set(db.list_collection_names())
        since = datetime.now() - timedelta(days=800)
        _merge_ultima_entrada_entrada_nota_fiscal_mov_estoque(db, agregado, since, names)
        pares = [
            ("DtoCompraProduto", "CompraID", "DtoCompra"),
            ("DtoPedidoCompraProduto", "PedidoCompraID", "DtoPedidoCompra"),
            ("DtoNotaEntradaProduto", "NotaEntradaID", "DtoNotaEntrada"),
            ("DtoEntradaMercadoriaProduto", "EntradaID", "DtoEntradaMercadoria"),
        ]
        for col_p, fk, col_h in pares:
            if col_p not in names or col_h not in names:
                continue
            heads = list(
                db[col_h].find(
                    {"Data": {"$gte": since}, "Cancelada": {"$ne": True}},
                    {"Id": 1, "_id": 1, "Data": 1},
                )
            )
            if not heads:
                continue
            cmap = {}
            for h in heads:
                hid = str(h.get("Id") or h.get("_id"))
                cmap[hid] = h.get("Data")
            hids_obj = []
            hids_str = []
            for k in cmap:
                hids_str.append(k)
                if len(k) == 24:
                    try:
                        hids_obj.append(ObjectId(k))
                    except Exception:
                        pass
            q = {"$or": [{fk: {"$in": hids_obj}}, {fk: {"$in": hids_str}}]}
            for item in db[col_p].find(q):
                pid = str(item.get("ProdutoID") or "")
                if not pid:
                    continue
                hid = str(item.get(fk) or "")
                dt = cmap.get(hid)
                if dt is None:
                    continue
                try:
                    qtd = float(item.get("Quantidade") or item.get("Qtd") or 0)
                except (TypeError, ValueError):
                    qtd = 0.0
                prev = agregado.get(pid)
                if prev is None or dt > prev[0]:
                    agregado[pid] = (dt, qtd)
                elif dt == prev[0]:
                    agregado[pid] = (dt, prev[1] + qtd)
        serial = {}
        for pid, (dt, qtd) in agregado.items():
            if hasattr(dt, "isoformat"):
                dts = dt.isoformat()
            else:
                dts = str(dt) if dt is not None else ""
            serial[pid] = {"data": dts[:19] if len(dts) > 19 else dts, "qtd": round(qtd, 4)}
        return serial
    except Exception as exc:
        logger.warning("ultima_entrada_mercadoria_por_produto: %s", exc)
        return {}


# --- AUXILIARES DE IMAGEM ---
def _formatar_url_imagem(img_str):
    img_str = str(img_str or "").strip()
    if not img_str or img_str == "None":
        return ""
    if img_str.startswith("data:image"):
        return img_str
    if len(img_str) > 1000 and not img_str.startswith("http"):
        return "data:image/jpeg;base64," + img_str

    base_url = "https://cw.vendaerp.com.br"
    try:
        integ = IntegracaoERP.objects.filter(ativo=True).first()
        if integ and integ.url_base:
            base_url = integ.url_base.rstrip("/")
    except Exception:
        pass

    if img_str.startswith("Uploads/"):
        return base_url + "/" + img_str
    elif img_str.startswith("/Uploads/"):
        return base_url + img_str
    elif not img_str.startswith("http"):
        return base_url + "/Uploads/Produtos/" + img_str.lstrip("/")

    return img_str


def _extrair_imagem_produto(p, mapa_imagens, pid):
    if mapa_imagens.get(pid):
        return mapa_imagens.get(pid)
    if p.get("Codigo") and mapa_imagens.get(str(p.get("Codigo"))):
        return mapa_imagens.get(str(p.get("Codigo")))
    if p.get("CodigoNFe") and mapa_imagens.get(str(p.get("CodigoNFe"))):
        return mapa_imagens.get(str(p.get("CodigoNFe")))

    for c in [
        "UrlImagem",
        "Imagem",
        "CaminhoImagem",
        "Foto",
        "Url",
        "UrlImagemPrincipal",
        "ImagemPrincipal",
        "ImagemBase64",
        "FotoBase64",
    ]:
        val = p.get(c)
        if val and isinstance(val, str) and len(val.strip()) > 2:
            return val

    for c in ["Imagens", "Fotos", "ImagemProduto", "ProdutoImagem"]:
        arr = p.get(c)
        if isinstance(arr, list) and len(arr) > 0:
            i = arr[0]
            if isinstance(i, dict):
                for sub_c in [
                    "Url",
                    "UrlImagem",
                    "Caminho",
                    "Imagem",
                    "Path",
                    "ImagemBase64",
                    "Base64",
                ]:
                    val = i.get(sub_c)
                    if val and isinstance(val, str) and len(val.strip()) > 2:
                        return val
            elif isinstance(i, str):
                return i

    return ""


def _custo_com_acrescimos_explicito(p, preco_base, preco_venda_val=0.0):
    """
    Preço de custo com acréscimos (frete, ST, etc.) gravado pelo ERP — alinha com a tela Custos e Precificação.
    Ignora zero (campo não calculado / legado).
    Só aceita valor >= custo base e <= 5× o base (mesma faixa inferior/superior da heurística de custo).
    Rejeita valor acima do preço de venda (com 1 centavo de folga): na precificação ERP,
    custo c/ acréscimos + MVA ≈ venda; valores acima da venda costumam ser campo errado ou lixo.
    """
    chaves = (
        "PrecoCustoComAcrescimos",
        "ValorPrecoCustoComAcrescimos",
        "PrecoCustoComAcrescimo",
        "ValorCustoComAcrescimos",
        "ValorCustoComAcrescimo",
        "CustoComAcrescimos",
    )
    for key in chaves:
        raw = p.get(key)
        if raw is None or raw == "":
            continue
        try:
            v = float(str(raw).replace(",", "."))
        except (ValueError, TypeError):
            continue
        if v <= 0:
            continue
        if preco_base > 0:
            # Alinhado a _heuristic_custo_maximo_doc: acréscimo não pode ser < custo base (100%–500% do base).
            if v + 1e-9 < preco_base or v > preco_base * 5:
                continue
        if preco_venda_val > 0 and v > preco_venda_val + 0.01:
            continue
        return v
    return None


def _custo_com_acrescimos_estimado_percentuais_compra(p, preco_base):
    """
    Custo com acréscimos ≈ PrecoCusto * (1 + soma dos % de compra), no mesmo espírito da precificação VendaERP.
    Os campos *CompraPercentual no DtoProduto são alíquotas (número 6,13 = 6,13%), não valores em R$.
    ICMS da compra não entra na soma: costuma ser crédito; o quadro "custo c/ acréscimos" usa frete/ST/IPI/seguro/FCP ST.
    """
    if preco_base <= 0:
        return None
    pct_fields = (
        "FreteCompraPercentual",
        "SeguroCompraPercentual",
        "IPICompraPercentual",
        "ICMSSTCompraPercentual",
        "FCPSTCompraPercentual",
    )
    total_pct = 0.0
    for field in pct_fields:
        raw = p.get(field)
        if raw is None or raw == "":
            continue
        try:
            x = float(str(raw).replace(",", "."))
        except (ValueError, TypeError):
            continue
        if x > 0:
            total_pct += x
    if total_pct <= 0:
        return None
    return round(preco_base * (1.0 + total_pct / 100.0), 4)


def _heuristic_custo_maximo_doc(p, preco_custo_val, preco_venda_val):
    """Varre o documento do produto em busca do maior valor plausível de custo (taxas, frete, etc.)."""
    max_val = preco_custo_val

    def traverse(obj):
        nonlocal max_val
        if isinstance(obj, dict):
            for k, v in obj.items():
                k_lower = k.lower()
                bad_keys = [
                    "venda", "lucro", "margem", "id", "codigo", "ean", "ncm", "cest",
                    "peso", "qtd", "quantidade", "estoque", "altura", "largura",
                    "comprimento", "profundidade", "medida", "volume", "nfe",
                    "cfop", "gtin", "dia", "mes", "ano", "prazo", "validade",
                    "caixa", "unidade", "fator", "tabela", "atacado", "varejo", "promocao",
                    "percentual", "porcentagem", "aliquota", "taxa",
                    # Componentes fiscais em R$ ou bases — não são o custo com acréscimos total
                    "valor_icms_st", "valor_icms_substituto", "fcpst_valor", "fcpst_basecalculo",
                    "valor_ipi", "fcpst_percentual",
                ]
                if any(x in k_lower for x in bad_keys):
                    continue

                if isinstance(v, (dict, list)):
                    traverse(v)
                else:
                    # Não usar nomes genéricos de imposto (muitas vezes são alíquotas %, ex.: ICMSCompraPercentual)
                    good_cost_indicators = [
                        "custo", "compra", "reposicao", "fornecedor", "entrada",
                        "valor", "preco", "total", "final", "bruto", "liquido",
                        "medio", "acrescimo", "despesa", "frete", "seguro",
                        "imposto",
                        "real", "efetivo",
                    ]
                    if any(x in k_lower for x in good_cost_indicators):
                        if v is not None:
                            try:
                                val_f = float(str(v).replace(",", "."))
                                if preco_venda_val > 0 and val_f == preco_venda_val:
                                    continue
                                if preco_venda_val > 0 and val_f > preco_venda_val + 0.01:
                                    continue
                                if preco_custo_val > 0 and preco_custo_val <= val_f <= (preco_custo_val * 5):
                                    if val_f > max_val:
                                        max_val = val_f
                                elif preco_custo_val == 0 and 0 < val_f < 100000:
                                    if val_f > max_val:
                                        max_val = val_f
                            except (ValueError, TypeError):
                                pass
        elif isinstance(obj, list):
            for item in obj:
                traverse(item)

    traverse(p)
    return max_val


def _custo_final_explicito_campos(p, preco_venda_val):
    """Valores explícitos de custo final no cadastro (quando o ERP já grava o custo líquido/c/ imposto)."""
    chaves = (
        "PrecoCustoFinal",
        "ValorCustoFinal",
        "CustoFinal",
        "PrecoCustoComImposto",
        "PrecoCustoTotal",
        "ValorCustoComImposto",
        "CustoMedioCompra",
        "PrecoReposicao",
        "ValorCustoCompra",
        "CustoCompraFinal",
        "ValorCustoReposicao",
    )
    vals = []
    for key in chaves:
        raw = p.get(key)
        if raw is None or raw == "":
            continue
        try:
            v = float(str(raw).replace(",", "."))
            if v <= 0:
                continue
            if preco_venda_val > 0 and abs(v - preco_venda_val) < 0.01:
                continue
            if preco_venda_val > 0 and v > preco_venda_val + 0.01:
                continue
            vals.append(v)
        except (ValueError, TypeError):
            continue
    return max(vals) if vals else None


def _custos_compra_produto(p):
    """
    Retorna custo base (nota) e custo final para compra (base + taxas/impostos quando identificáveis).
    """
    preco_bruto = p.get("PrecoCusto") or p.get("ValorCusto") or 0
    try:
        preco_custo_val = float(str(preco_bruto).replace(",", "."))
    except ValueError:
        preco_custo_val = 0.0
    preco_venda_val = float(p.get("ValorVenda") or p.get("PrecoVenda") or 0)
    com_acresc = _custo_com_acrescimos_explicito(p, preco_custo_val, preco_venda_val)
    est_pct = _custo_com_acrescimos_estimado_percentuais_compra(p, preco_custo_val)
    heuristic = _heuristic_custo_maximo_doc(p, preco_custo_val, preco_venda_val)
    explicit = _custo_final_explicito_campos(p, preco_venda_val)
    raw_max = max(heuristic, explicit or 0.0)

    if com_acresc is not None:
        final = com_acresc
    elif preco_venda_val > 0 and raw_max > preco_venda_val + 0.01 and est_pct is not None:
        # Custo cadastral acima da venda (ex.: 16,10 vs 15,99) — típico de campo errado; % de compra batem com o ERP
        final = round(max(preco_custo_val, est_pct), 2)
    elif est_pct is not None:
        final = round(max(raw_max, est_pct), 2)
    else:
        final = raw_max
    return {"preco_custo": preco_custo_val, "preco_custo_final": final}


def _preco_unit_linha_compra_mongo(item):
    """
    Retorna (valor_unitário, já_é_custo_final_erp).
    Quando o ERP grava custo com acréscimos na linha, não reaplicamos % do cadastro.
    """
    if not isinstance(item, dict):
        return 0.0, False
    for k in (
        "ValorCustoComAcrescimos",
        "PrecoCustoComAcrescimos",
        "ValorUnitarioComAcrescimos",
        "PrecoUnitarioComAcrescimos",
    ):
        raw = item.get(k)
        if raw is None or raw == "":
            continue
        try:
            v = float(str(raw).replace(",", "."))
            if v > 0:
                return v, True
        except (ValueError, TypeError):
            continue
    for k in ("ValorUnitario", "PrecoUnitario", "ValorUnit", "Preco"):
        raw = item.get(k)
        if raw is None or raw == "":
            continue
        try:
            v = float(str(raw).replace(",", "."))
            if v > 0:
                return v, False
        except (ValueError, TypeError):
            continue
    try:
        tot = float(str(item.get("ValorTotal") or item.get("Total") or 0).replace(",", "."))
        q = float(str(item.get("Quantidade") or item.get("Qtd") or 0).replace(",", "."))
        if tot > 0 and q > 0:
            return tot / q, False
    except (ValueError, TypeError, ZeroDivisionError):
        pass
    return 0.0, False


def _preco_unitario_entrada_com_acrescimo_cadastro(p, preco_unit_nota, ja_final_erp=False):
    """
    Alinha à lógica da tela de compras: se a linha já traz custo c/ acréscimo do ERP, usa direto;
    senão aplica a mesma proporção cadastral (custo final / custo base) ou % de compra do produto.
    """
    if ja_final_erp:
        try:
            return round(float(str(preco_unit_nota).replace(",", ".")), 2)
        except (ValueError, TypeError):
            return 0.0
    try:
        base = float(str(preco_unit_nota).replace(",", "."))
    except (ValueError, TypeError):
        base = 0.0
    if base <= 0:
        return 0.0
    pp = p if isinstance(p, dict) else {}
    custos = _custos_compra_produto(pp)
    pc = float(custos.get("preco_custo") or 0)
    fin = float(custos.get("preco_custo_final") or 0)
    if pc > 0 and fin > 0:
        return round(base * (fin / pc), 2)
    est = _custo_com_acrescimos_estimado_percentuais_compra(pp, base)
    if est is not None:
        try:
            return round(float(est), 2)
        except (ValueError, TypeError):
            pass
    return round(base, 2)


def _data_cabecalho_compra(h):
    if not isinstance(h, dict):
        return None
    for k in (
        "DataEntradaNota",
        "DataEmissaoNota",
        "Data",
        "DataEmissao",
        "DataEntrada",
        "DataMovimento",
    ):
        d = h.get(k)
        if isinstance(d, datetime):
            return d
    return None


def _nome_fornecedor_compra_head(h):
    if not isinstance(h, dict):
        return ""
    for key in (
        "NomeFornecedor",
        "RazaoSocialFornecedor",
        "FornecedorNome",
        "NomeFantasiaFornecedor",
        "PessoaNome",
        "NomePessoa",
        "RazaoSocial",
        "Fornecedor",
        "Nome",
    ):
        v = h.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()[:200]
    return ""


def _numero_documento_compra_head(h):
    if not isinstance(h, dict):
        return ""
    for key in ("NumeroNF", "NumeroNFe", "NumeroNota", "NotaFiscal", "ChaveNFe", "Numero", "Documento"):
        v = h.get(key)
        if v is not None and str(v).strip() and str(v).strip().lower() not in ("null", "none"):
            s = str(v).strip()
            ser = h.get("SerieNota") or h.get("Serie")
            if key == "NumeroNota" and ser and str(ser).strip():
                return f"{str(ser).strip()}/{s}"[:120]
            return s[:120]
    ser = h.get("SerieNota") or h.get("Serie")
    if ser and str(ser).strip():
        return str(ser).strip()[:120]
    return ""


def _mongo_ids_para_query_in(ids_str: list[str]) -> list:
    out = []
    seen = set()
    for s in ids_str:
        t = str(s or "").strip()
        if not t or t in seen:
            continue
        seen.add(t)
        out.append(t)
        if len(t) == 24 and all(c in "0123456789abcdefABCDEF" for c in t):
            try:
                oid = ObjectId(t)
                if oid not in seen:
                    seen.add(oid)
                    out.append(oid)
            except Exception:
                pass
    return out


def _produto_ids_variants_mongo(p_ids: list[str]) -> list:
    out = []
    seen = set()
    for raw in p_ids:
        s = str(raw or "").strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
        if s.isdigit():
            try:
                n = int(s)
                if n not in seen:
                    seen.add(n)
                    out.append(n)
            except Exception:
                pass
        if len(s) == 24 and all(c in "0123456789abcdefABCDEF" for c in s):
            try:
                oid = ObjectId(s)
                if oid not in seen:
                    seen.add(oid)
                    out.append(oid)
            except Exception:
                pass
    return out


def _ultimas_compras_cutoff_dt() -> datetime:
    """Limite inferior (UTC naive) para considerar compras recentes."""
    return datetime.utcnow() - timedelta(days=800)


def _mongo_dt_utc_naive(dt):
    if dt is None or not isinstance(dt, datetime):
        return None
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _mongo_dt_maior_ou_igual(dt, cutoff_naive_utc: datetime) -> bool:
    """Evita TypeError entre datetime com fuso (Mongo) e cutoff naive."""
    d = _mongo_dt_utc_naive(dt)
    if d is None:
        return False
    return d >= cutoff_naive_utc


def _mongo_dt_sort_key(dt):
    d = _mongo_dt_utc_naive(dt) if isinstance(dt, datetime) else None
    return d if d is not None else datetime.min


def _append_eventos_dto_nota_entrada_por_linha(
    db,
    *,
    variants: list,
    pid_ok: set[str],
    eventos: dict[str, list[dict]],
    since: datetime,
) -> None:
    """
    Nota de entrada VendaERP: ligação DtoNotaEntradaProduto.EntradaID → DtoNotaEntrada._id.
    Busca por linha (ProdutoID) evita depender de filtro de data no cabeçalho, que varia por instalação.
    """
    try:
        proj_ln = {
            "ProdutoID": 1,
            "EntradaID": 1,
            "NotaEntradaID": 1,
            "Quantidade": 1,
            "Qtd": 1,
            "Cancelada": 1,
            "ValorUnitario": 1,
            "PrecoUnitario": 1,
            "ValorTotal": 1,
            "Total": 1,
            "ValorCustoComAcrescimos": 1,
            "PrecoCustoComAcrescimos": 1,
            "ValorUnitarioComAcrescimos": 1,
            "PrecoUnitarioComAcrescimos": 1,
            "LastUpdate": 1,
        }
        proj_h = {
            "Id": 1,
            "_id": 1,
            "Data": 1,
            "DataEmissao": 1,
            "DataEntrada": 1,
            "DataEntradaNota": 1,
            "DataEmissaoNota": 1,
            "Cancelada": 1,
            "NomeFornecedor": 1,
            "RazaoSocialFornecedor": 1,
            "FornecedorNome": 1,
            "Fornecedor": 1,
            "NomeFantasiaFornecedor": 1,
            "PessoaNome": 1,
            "NomePessoa": 1,
            "RazaoSocial": 1,
            "NumeroNF": 1,
            "NumeroNFe": 1,
            "Numero": 1,
            "NotaFiscal": 1,
            "ChaveNFe": 1,
            "Serie": 1,
            "NumeroNota": 1,
            "SerieNota": 1,
        }
        cur = db["DtoNotaEntradaProduto"].find(
            {"ProdutoID": {"$in": variants}},
            proj_ln,
        ).sort("LastUpdate", -1).limit(12000)
        lines = list(cur)
    except Exception as exc:
        logger.warning("ultimas_compras nota_entrada por_linha: %s", exc)
        return
    eids_ordered: list[str] = []
    seen_e: set[str] = set()
    for ln in lines:
        eid = str(ln.get("EntradaID") or ln.get("NotaEntradaID") or "")
        if eid and eid not in seen_e:
            seen_e.add(eid)
            eids_ordered.append(eid)
    heads: dict[str, dict] = {}
    chunk_sz = 400
    for i in range(0, len(eids_ordered), chunk_sz):
        chunk = eids_ordered[i : i + chunk_sz]
        mixed = _mongo_ids_para_query_in(chunk)
        if not mixed:
            continue
        try:
            for h in db["DtoNotaEntrada"].find(
                {"$or": [{"_id": {"$in": mixed}}, {"Id": {"$in": mixed}}]},
                proj_h,
            ):
                hid = str(h.get("Id") or h.get("_id") or "")
                if hid:
                    heads[hid] = h
        except Exception as exc:
            logger.warning("ultimas_compras nota_entrada heads: %s", exc)
            continue
    for ln in lines:
        if ln.get("Cancelada") in (True, "Sim", 1, "true", "True"):
            continue
        pid = str(ln.get("ProdutoID") or "")
        if pid not in pid_ok:
            continue
        eid = str(ln.get("EntradaID") or ln.get("NotaEntradaID") or "")
        h = heads.get(eid)
        if not h:
            continue
        if h.get("Cancelada") in (True, "Sim", 1, "true", "True"):
            continue
        dt = _data_cabecalho_compra(h)
        if not _mongo_dt_maior_ou_igual(dt, since):
            continue
        unit, ja_final = _preco_unit_linha_compra_mongo(ln)
        qtd = _float_api_json(ln.get("Quantidade") or ln.get("Qtd") or 0)
        eventos[pid].append(
            {
                "dt": dt,
                "fornecedor": _nome_fornecedor_compra_head(h),
                "qtd": qtd,
                "unit_base": unit,
                "unit_ja_final": ja_final,
                "numero_doc": _numero_documento_compra_head(h),
                "tipo_fonte": "nota_entrada",
            }
        )


def _ultimas_compras_por_produto_ids(
    db,
    p_ids: list[str],
    produtos_por_id: dict,
    *,
    limit: int = 3,
) -> dict[str, list[dict]]:
    """
    Últimas compras por produto a partir de DtoCompra*, DtoNotaEntrada*, etc. (Mongo ERP).
    """
    out_map: dict[str, list[dict]] = {str(pid): [] for pid in p_ids}
    if db is None or not p_ids:
        return out_map
    variants = _produto_ids_variants_mongo([str(x) for x in p_ids])
    if not variants:
        return out_map
    try:
        names = set(db.list_collection_names())
    except Exception as exc:
        logger.warning("ultimas_compras list_collection_names: %s", exc)
        return out_map
    since = _ultimas_compras_cutoff_dt()
    pares = (
        ("DtoCompraProduto", "CompraID", "DtoCompra", "compra"),
        ("DtoPedidoCompraProduto", "PedidoCompraID", "DtoPedidoCompra", "pedido_compra"),
        ("DtoEntradaMercadoriaProduto", "EntradaID", "DtoEntradaMercadoria", "entrada_mercadoria"),
    )
    proj_h = {
        "Id": 1,
        "_id": 1,
        "Data": 1,
        "DataEmissao": 1,
        "DataEntrada": 1,
        "DataEntradaNota": 1,
        "DataEmissaoNota": 1,
        "Cancelada": 1,
        "NomeFornecedor": 1,
        "RazaoSocialFornecedor": 1,
        "FornecedorNome": 1,
        "Fornecedor": 1,
        "NomeFantasiaFornecedor": 1,
        "PessoaNome": 1,
        "NomePessoa": 1,
        "RazaoSocial": 1,
        "NumeroNF": 1,
        "NumeroNFe": 1,
        "Numero": 1,
        "NotaFiscal": 1,
        "ChaveNFe": 1,
        "Serie": 1,
        "NumeroNota": 1,
        "SerieNota": 1,
    }
    eventos: dict[str, list[dict]] = {str(pid): [] for pid in p_ids}
    pid_ok = {str(x) for x in p_ids}

    if "DtoNotaEntradaProduto" in names and "DtoNotaEntrada" in names:
        _append_eventos_dto_nota_entrada_por_linha(
            db,
            variants=variants,
            pid_ok=pid_ok,
            eventos=eventos,
            since=since,
        )

    for col_p, fk, col_h, origem_label in pares:
        if col_p not in names or col_h not in names:
            continue
        proj_ln = {
            "ProdutoID": 1,
            fk: 1,
            "Quantidade": 1,
            "Qtd": 1,
            "Cancelada": 1,
            "ValorUnitario": 1,
            "PrecoUnitario": 1,
            "ValorTotal": 1,
            "Total": 1,
            "ValorCustoComAcrescimos": 1,
            "PrecoCustoComAcrescimos": 1,
            "ValorUnitarioComAcrescimos": 1,
            "PrecoUnitarioComAcrescimos": 1,
        }
        try:
            q_head = {
                "Cancelada": {"$ne": True},
                "$or": [
                    {"Data": {"$gte": since}},
                    {"DataEmissao": {"$gte": since}},
                    {"DataEntrada": {"$gte": since}},
                    {"DataEntradaNota": {"$gte": since}},
                    {"DataEmissaoNota": {"$gte": since}},
                ],
            }
            heads = list(db[col_h].find(q_head, proj_h))
        except Exception as exc:
            logger.warning("ultimas_compras heads %s: %s", col_h, exc)
            continue
        if len(heads) > 25000:
            heads.sort(
                key=lambda h: _mongo_dt_sort_key(_data_cabecalho_compra(h)),
                reverse=True,
            )
            heads = heads[:25000]
        cmap: dict[str, dict] = {}
        for h in heads:
            hid = str(h.get("Id") or h.get("_id") or "")
            if hid:
                cmap[hid] = h
        hid_all = list(cmap.keys())
        chunk_sz = 400
        for i in range(0, len(hid_all), chunk_sz):
            chunk = hid_all[i : i + chunk_sz]
            mixed = _mongo_ids_para_query_in(chunk)
            try:
                cur = db[col_p].find(
                    {fk: {"$in": mixed}, "ProdutoID": {"$in": variants}},
                    proj_ln,
                )
            except Exception as exc:
                logger.warning("ultimas_compras find %s: %s", col_p, exc)
                continue
            for ln in cur:
                if ln.get("Cancelada") in (True, "Sim", 1, "true", "True"):
                    continue
                pid = str(ln.get("ProdutoID") or "")
                if pid not in pid_ok:
                    continue
                hid = str(ln.get(fk) or "")
                h = cmap.get(hid)
                if not h:
                    continue
                dt = _data_cabecalho_compra(h)
                if not _mongo_dt_maior_ou_igual(dt, since):
                    continue
                unit, ja_final = _preco_unit_linha_compra_mongo(ln)
                qtd = _float_api_json(ln.get("Quantidade") or ln.get("Qtd") or 0)
                eventos[pid].append(
                    {
                        "dt": dt,
                        "fornecedor": _nome_fornecedor_compra_head(h),
                        "qtd": qtd,
                        "unit_base": unit,
                        "unit_ja_final": ja_final,
                        "numero_doc": _numero_documento_compra_head(h),
                        "tipo_fonte": origem_label,
                    }
                )

    for pid in p_ids:
        spid = str(pid)
        evs = eventos.get(spid, [])
        evs.sort(key=lambda e: _mongo_dt_sort_key(e.get("dt")), reverse=True)
        deduped = []
        seen_k = set()
        for e in evs:
            dt = e.get("dt")
            try:
                ub = round(float(e.get("unit_base") or 0), 4)
            except (TypeError, ValueError):
                ub = 0.0
            key = (
                dt.isoformat()[:16] if isinstance(dt, datetime) else "",
                (e.get("fornecedor") or "")[:80],
                (e.get("numero_doc") or "")[:40],
                e.get("tipo_fonte") or "",
                ub,
            )
            if key in seen_k:
                continue
            seen_k.add(key)
            deduped.append(e)
            if len(deduped) >= limit:
                break
        p_doc = produtos_por_id.get(spid)
        for e in deduped:
            pf = _preco_unitario_entrada_com_acrescimo_cadastro(
                p_doc, e.get("unit_base") or 0, bool(e.get("unit_ja_final"))
            )
            dt = e.get("dt")
            iso = dt.isoformat()[:19] if isinstance(dt, datetime) else ""
            try:
                ub = round(float(e.get("unit_base") or 0), 4)
            except (TypeError, ValueError):
                ub = 0.0
            try:
                qtdv = round(float(e.get("qtd") or 0), 4)
            except (TypeError, ValueError):
                qtdv = 0.0
            forn = (e.get("fornecedor") or "—")[:200]
            out_map[spid].append(
                {
                    "fornecedor": forn,
                    "preco_final": round(float(pf), 2),
                    "detalhe": {
                        "data": iso,
                        "quantidade": qtdv,
                        "preco_unitario_nota": ub,
                        "preco_unitario_final": round(float(pf), 2),
                        "preco_ja_com_acrescimo_erp": bool(e.get("unit_ja_final")),
                        "fornecedor": forn,
                        "documento": (e.get("numero_doc") or "")[:120],
                        "origem": e.get("tipo_fonte") or "",
                    },
                }
            )
    return out_map


def _sanear_itens_checkout_sessao(itens):
    out = []
    if not isinstance(itens, list):
        return out
    for i in itens[:400]:
        if not isinstance(i, dict):
            continue
        try:
            qtd = float(i.get("qtd") or 0)
            preco = float(i.get("preco") or 0)
        except (TypeError, ValueError):
            continue
        if qtd <= 0:
            continue
        out.append(
            {
                "id": str(i.get("id") or "")[:80],
                "nome": str(i.get("nome") or "")[:500],
                "qtd": qtd,
                "preco": preco,
                "codigo": str(i.get("codigo") or i.get("Codigo") or "")[:120],
            }
        )
    return out


def _sanear_cliente_extra_sessao(raw):
    if not isinstance(raw, dict):
        return None
    out = {}
    for k, maxlen in (
        ("id", 80),
        ("documento", 300),
        ("telefone", 300),
        ("nome", 300),
        ("razao_social", 300),
        ("endereco", 500),
        ("logradouro", 300),
        ("numero", 80),
        ("bairro", 200),
        ("cidade", 200),
        ("uf", 8),
        ("cep", 24),
        ("plus_code", 120),
        ("referencia_rural", 500),
        ("maps_url_manual", 800),
    ):
        v = raw.get(k)
        if v is not None and str(v).strip():
            out[k] = str(v).strip()[:maxlen]
    pk = raw.get("cliente_agro_pk")
    if pk is not None and str(pk).strip():
        try:
            out["cliente_agro_pk"] = int(pk)
        except (TypeError, ValueError):
            out["cliente_agro_pk"] = str(pk).strip()[:40]
    return out if out else None


def _forma_pagamento_rotulo_sem_valor_moeda(s: str) -> str:
    """
    ERP costuma mostrar valor em coluna à parte; texto ``Dinheiro R$ 4,00`` vem de resumos antigos
    ou PDV clássico. Remove ``(troco R$ …)``, trecho que é só ``R$``+valor e sufixo `` R$``+valor.
    """
    s = str(s or "").strip()
    if not s:
        return ""
    parts = [p.strip() for p in re.split(r"\s+\+\s+", s) if p.strip()]
    out: list[str] = []
    for p in parts:
        t = p
        t = re.sub(r"^\s*R\$\s*[\d\s.,]+\s*$", "", t, flags=re.IGNORECASE)
        t = re.sub(r"\s*\(troco\s+R\$[\d\s.,]+\)\s*$", "", t, flags=re.IGNORECASE)
        t = re.sub(r"\s+R\$\s*[\d\s.,]+$", "", t, flags=re.IGNORECASE)
        t = t.strip()
        if t:
            out.append(t)
    return " + ".join(out)[:200]


def _normalizar_linhas_pagamento_pedido(raw_list) -> list[dict]:
    """Lista de parcelas para Pedidos/Salvar (camelCase). Usado no payload e no rascunho checkout."""
    if not isinstance(raw_list, list) or not raw_list:
        return []
    out: list[dict] = []
    for row in raw_list[:30]:
        if not isinstance(row, dict):
            continue
        fn = str(
            row.get("formaPagamento")
            or row.get("forma_pagamento")
            or row.get("forma")
            or ""
        ).strip()[:200]
        vp_raw = row.get("valorPagamento", row.get("valor_pagamento", row.get("valor")))
        try:
            vp = float(vp_raw)
        except (TypeError, ValueError):
            vp = 0.0
        vp = round(vp, 2)
        fn = _forma_pagamento_rotulo_sem_valor_moeda(fn)
        if not fn and vp <= 0:
            continue
        if not fn:
            fn = "Não informado"
        item: dict = {
            "formaPagamento": fn,
            "valorPagamento": vp,
            "quitar": bool(row.get("quitar", True)),
        }
        desc = str(
            row.get("descricaoPagamento") or row.get("descricao_pagamento") or ""
        ).strip()[:300]
        if desc:
            item["descricaoPagamento"] = desc
        out.append(item)
    return out


def _resumo_forma_pagamento_de_linhas(pagamentos: list[dict]) -> str:
    labels: list[str] = []
    for p in pagamentos:
        t = _forma_pagamento_rotulo_sem_valor_moeda(str(p.get("formaPagamento") or ""))
        if t and t not in labels:
            labels.append(t)
    return (" + ".join(labels))[:200]


def _parse_data_iso(s):
    if not s or not str(s).strip():
        return None
    try:
        return datetime.strptime(str(s).strip()[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _periodo_vendas_from_request(request):
    hoje = timezone.localdate()
    preset = (request.GET.get("preset") or "").strip().lower()
    de_str = request.GET.get("de")
    ate_str = request.GET.get("ate")
    di = _parse_data_iso(de_str)
    df = _parse_data_iso(ate_str)
    if di and df and di > df:
        di, df = df, di
    if preset == "hoje":
        di = df = hoje
        label = "Hoje"
    elif preset == "7d":
        di = hoje - timedelta(days=6)
        df = hoje
        label = "Últimos 7 dias"
    elif preset == "30d":
        di = hoje - timedelta(days=29)
        df = hoje
        label = "Últimos 30 dias"
    elif di and df:
        label = f"{di.strftime('%d/%m/%Y')} — {df.strftime('%d/%m/%Y')}"
    elif di:
        df = hoje
        label = f"Desde {di.strftime('%d/%m/%Y')}"
    elif df:
        di = df
        label = f"Dia {df.strftime('%d/%m/%Y')}"
    else:
        di = hoje - timedelta(days=6)
        df = hoje
        label = "Últimos 7 dias"
    return di, df, label


def _obter_sessao_caixa_aberta(request):
    sid = request.session.get("pdv_sessao_caixa_id")
    if not sid:
        return None
    try:
        return SessaoCaixa.objects.get(pk=int(sid), fechado_em__isnull=True)
    except (SessaoCaixa.DoesNotExist, ValueError, TypeError):
        request.session.pop("pdv_sessao_caixa_id", None)
        return None


def _format_moeda_br(val: Decimal) -> str:
    q = val.quantize(Decimal("0.01"))
    neg = q < 0
    q = abs(q)
    inteiro, _, frac = f"{q:.2f}".partition(".")
    out = []
    while inteiro:
        out.append(inteiro[-3:])
        inteiro = inteiro[:-3]
    corpo = ".".join(reversed(out)) if out else "0"
    s = f"{corpo},{frac}"
    return f"-{s}" if neg else s


def _home_quick_stats(request):
    """Indicadores leves para a home administrativa (sem agregações Mongo pesadas)."""
    hoje = timezone.localdate()
    limite_validade = hoje + timedelta(days=ALERTA_VALIDADE_DIAS)
    agg = VendaAgro.objects.filter(criado_em__date=hoje).aggregate(soma=Sum("total"))
    soma = agg["soma"] if agg["soma"] is not None else Decimal("0")
    total_vendas_dia = soma.quantize(Decimal("0.01"))
    entregas_pendentes = PedidoEntrega.objects.exclude(
        status__in=(PedidoEntrega.Status.ENTREGUE, PedidoEntrega.Status.CANCELADO)
    ).count()
    produtos_vencendo = (
        EstoqueLote.objects.filter(
            quantidade_atual__gt=0,
            data_validade__lte=limite_validade,
        )
        .values("overlay_id")
        .distinct()
        .count()
    )
    sess = getattr(request, "session", None)
    caixa_aberto = _obter_sessao_caixa_aberta(request) is not None if sess is not None else False
    return {
        "total_vendas_dia": total_vendas_dia,
        "total_vendas_dia_fmt": _format_moeda_br(total_vendas_dia),
        "entregas_pendentes": entregas_pendentes,
        "produtos_vencendo": produtos_vencendo,
        "caixa_aberto": caixa_aberto,
    }


def _empresa_home_atual():
    empresas = list(
        Empresa.objects.filter(ativo=True).only("id", "nome_fantasia").order_by("nome_fantasia")[:2]
    )
    if len(empresas) == 1:
        return empresas[0]
    return None


def _home_admin_navegacao():
    dre_ativo = getattr(settings, "LANCAMENTOS_DRE_ATIVO", False)
    agro_legado_url = reverse("consulta_produtos")
    # Teclas únicas (sem modificador), priorizando F2–F12 + letras para o restante — ver AGENTS.md §5 (teclado primeiro, fonte grande).
    top_items = [
        {
            "title": "Entrar no PDV",
            "href": reverse("pdv_home"),
            "icon": "shopping-cart",
            "shortcut": "F1",
            "shortcut_key": "f1",
            "accent": "capri",
            "pin_protected": True,
        },
        {
            "title": "Consulta / Orçamentos",
            "href": agro_legado_url,
            "icon": "search",
            "shortcut": "F2",
            "shortcut_key": "f2",
            "accent": "capri",
            "pin_protected": True,
        },
    ]
    # F4/F5 reservados (ex.: navegador / refresh); Entrada NF-e por último na grade.
    grid_items = [
        {
            "title": "Compras",
            "href": reverse("compras_view"),
            "icon": "package",
            "shortcut": "F6",
            "shortcut_key": "f6",
            "pin_protected": True,
        },
        {
            "title": "Relatórios",
            "href": reverse("relatorios_hub"),
            "icon": "line-chart",
            "shortcut": "T",
            "shortcut_key": "t",
            "pin_protected": True,
        },
        {
            "title": "Cadastro Produtos",
            "href": reverse("produtos_cadastro_erp"),
            "icon": "clipboard-list",
            "shortcut": "R",
            "shortcut_key": "r",
            "pin_protected": True,
        },
        {
            "title": "Lançamentos",
            "href": reverse("lancamentos_financeiros"),
            "icon": "wallet",
            "shortcut": "F7",
            "shortcut_key": "f7",
            "pin_protected": True,
        },
        {
            "title": "Gestão de empréstimos",
            "href": reverse("emprestimos_gestao"),
            "icon": "landmark",
            "shortcut": "G",
            "shortcut_key": "g",
            "pin_protected": True,
        },
        {
            "title": "Resumo gerencial",
            "href": reverse("resumo_financeiro_gerencial"),
            "icon": "pie-chart",
            "shortcut": "F8",
            "shortcut_key": "f8",
            "pin_protected": True,
        },
        {
            "title": "Dashboard gerencial",
            "href": reverse("dashboard_gerencial"),
            "icon": "layout-dashboard",
            "shortcut": "B",
            "shortcut_key": "b",
            "pin_protected": True,
        },
        {
            "title": "DRE (off)" if not dre_ativo else "DRE simples",
            "href": reverse("lancamentos_dre") if dre_ativo else "",
            "icon": "bar-chart-3",
            "shortcut": "F9",
            "shortcut_key": "f9",
            "disabled": not dre_ativo,
        },
        {
            "title": "Logística",
            "href": reverse("sugestao_transferencia"),
            "icon": "arrow-left-right",
            "shortcut": "F10",
            "shortcut_key": "f10",
            "pin_protected": True,
        },
        {
            "title": "Vendas",
            "href": reverse("vendas_lista"),
            "icon": "receipt",
            "shortcut": "F11",
            "shortcut_key": "f11",
            "pin_protected": True,
        },
        {
            "title": "Clientes",
            "href": reverse("clientes_lista"),
            "icon": "users",
            "shortcut": "F12",
            "shortcut_key": "f12",
            "pin_protected": True,
        },
        {
            "title": "Caixa",
            "href": reverse("caixa_painel"),
            "icon": "banknote",
            "shortcut": "Q",
            "shortcut_key": "q",
            "pin_protected": True,
        },
        {
            "title": "RH",
            "href": reverse("rh_painel"),
            "icon": "id-card",
            "shortcut": "W",
            "shortcut_key": "w",
            "pin_protected": True,
        },
        {
            "title": "Orçamentos salvos",
            "href": f"{agro_legado_url}?orcamentos=1",
            "icon": "history",
            "shortcut": "O",
            "shortcut_key": "o",
            "pin_protected": True,
        },
        {
            "title": "Entregas",
            "href": reverse("entregas_painel"),
            "icon": "truck",
            "shortcut": "E",
            "shortcut_key": "e",
            "pin_protected": True,
        },
        {
            "title": "Ajuste Mobile",
            "href": reverse("ajuste_mobile"),
            "icon": "smartphone",
            "shortcut": "M",
            "shortcut_key": "m",
            "pin_protected": True,
        },
        {
            "title": "Entrada NF-e",
            "href": reverse("entrada_nota"),
            "icon": "file-input",
            "shortcut": "N",
            "shortcut_key": "n",
            "pin_protected": True,
        },
        {
            "title": "Estoque (Agro)",
            "href": reverse("estoque_sincronizacao"),
            "icon": "activity",
            "shortcut": "Y",
            "shortcut_key": "y",
            "pin_protected": True,
        },
    ]
    return {"top_items": top_items, "grid_items": grid_items}


def _dashboard_periodo_from_request(request):
    hoje = timezone.localdate()
    preset = (request.GET.get("periodo") or "mes").strip().lower()
    if preset == "hoje":
        return hoje, hoje, "Hoje", preset
    if preset == "7d":
        return hoje - timedelta(days=6), hoje, "Últimos 7 dias", preset
    if preset == "30d":
        return hoje - timedelta(days=29), hoje, "Últimos 30 dias", preset
    if preset == "ano":
        ini = hoje.replace(month=1, day=1)
        return ini, hoje, "Ano atual", preset
    ini = hoje.replace(day=1)
    return ini, hoje, "Mês atual", "mes"


def _dashboard_prev_periodo(data_ini, data_fim):
    span = (data_fim - data_ini).days + 1
    prev_fim = data_ini - timedelta(days=1)
    prev_ini = prev_fim - timedelta(days=span - 1)
    return prev_ini, prev_fim


def _dashboard_float(value):
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _dashboard_ticket_medio_intervalo(data_ini: date, data_fim: date) -> float:
    """Ticket médio (Soma faturamento / N pedidos) no intervalo, Mongo + fallback SQLite como no dashboard."""
    ser = _dashboard_mongo_vendas_serie(data_ini, data_fim)
    qtd = sum(int(v or 0) for v in (ser.get("qtd_por_dia") or {}).values())
    total = _dashboard_float(ser.get("total"))
    if qtd <= 0:
        ticket_qs = VendaAgro.objects.filter(criado_em__date__gte=data_ini, criado_em__date__lte=data_fim)
        ticket_agg = ticket_qs.aggregate(total=Sum("total"), n=Count("id"))
        total = _dashboard_float(ticket_agg.get("total"))
        qtd = int(ticket_agg.get("n") or 0)
    return round((total / qtd), 2) if qtd > 0 else 0.0


def _dashboard_doc_total(doc):
    for campo in ("ValorTotal", "ValorLiquido", "Total", "Valor", "total", "ValorFinal"):
        v = doc.get(campo)
        if v is not None:
            f = _dashboard_float(v)
            if f > 0:
                return f
    return 0.0


def _dashboard_doc_data_venda(doc: dict):
    """Data da venda com prioridade para faturamento no espelho ERP."""
    for campo in ("DataFaturamento", "Data", "data", "CriadoEm", "criado_em"):
        dt = doc.get(campo)
        if isinstance(dt, datetime):
            return dt
    return None


def _dashboard_serie_mes_a_mes(data_ini: date, data_fim: date, atual: dict) -> tuple[list[str], list[float], list[float]]:
    """
    Agrupa a serie diaria em meses (rotulos, vendas, ticket medio).
    Usado no filtro anual para visual mensal.
    """
    meses = [
        "Jan",
        "Fev",
        "Mar",
        "Abr",
        "Mai",
        "Jun",
        "Jul",
        "Ago",
        "Set",
        "Out",
        "Nov",
        "Dez",
    ]
    soma_mes: dict[int, float] = {m: 0.0 for m in range(1, 13)}
    qtd_mes: dict[int, int] = {m: 0 for m in range(1, 13)}
    por_dia = atual.get("por_dia") or {}
    qtd_por_dia = atual.get("qtd_por_dia") or {}
    cur = data_ini
    while cur <= data_fim:
        chave = cur.isoformat()
        mes = cur.month
        soma_mes[mes] += _dashboard_float(por_dia.get(chave))
        qtd_mes[mes] += int(qtd_por_dia.get(chave) or 0)
        cur += timedelta(days=1)

    labels: list[str] = []
    serie_vendas: list[float] = []
    serie_ticket: list[float] = []
    for m in range(1, 13):
        if m > data_fim.month:
            break
        labels.append(meses[m - 1])
        total_m = round(soma_mes[m], 2)
        serie_vendas.append(total_m)
        n = qtd_mes[m]
        serie_ticket.append(round((total_m / n), 2) if n > 0 else 0.0)
    return labels, serie_vendas, serie_ticket


def _dashboard_bounds_mes_anterior_para_dia(d: date) -> tuple[date, date]:
    """Primeiro e último dia do mês civil imediatamente anterior ao mês de `d`."""
    first_cur = d.replace(day=1)
    last_prev = first_cur - timedelta(days=1)
    first_prev = last_prev.replace(day=1)
    return first_prev, last_prev


def _dashboard_vendas_meta_c_para_dia(d: date, por_dia_prev: dict) -> float:
    """
    Meta diária C = (A + B) / 2: A = média das vendas no mesmo weekday no mês anterior;
    B = venda na primeira data desse weekday no mês anterior.
    """
    wd = d.weekday()
    first_prev, last_prev = _dashboard_bounds_mes_anterior_para_dia(d)
    vals_same: list[float] = []
    b_val: float | None = None
    cur = first_prev
    while cur <= last_prev:
        if cur.weekday() == wd:
            v = round(_dashboard_float(por_dia_prev.get(cur.isoformat())), 2)
            vals_same.append(v)
            if b_val is None:
                b_val = v
        cur += timedelta(days=1)
    if not vals_same:
        return 0.0
    a = sum(vals_same) / len(vals_same)
    b = float(b_val or 0.0)
    return round((a + b) / 2.0, 2)


def _dashboard_serie_meta_c_vendas(data_ini: date, data_fim: date) -> list[float]:
    """Uma meta C por dia no intervalo (visão anual mensal não aplica esta regra)."""
    dias = (data_fim - data_ini).days + 1
    cache: dict[tuple[date, date], dict] = {}
    out: list[float] = []
    for i in range(dias):
        d = data_ini + timedelta(days=i)
        fp, lp = _dashboard_bounds_mes_anterior_para_dia(d)
        key = (fp, lp)
        if key not in cache:
            ser = _dashboard_mongo_vendas_serie(fp, lp)
            cache[key] = ser.get("por_dia") or {}
        out.append(_dashboard_vendas_meta_c_para_dia(d, cache[key]))
    return out


def _dashboard_mongo_vendas_serie(data_ini, data_fim):
    client, db = obter_conexao_mongo()
    if db is None:
        return {"ok": False, "erro": "Mongo indisponível", "total": 0.0, "por_dia": {}, "qtd_por_dia": {}}
    dt_ini = datetime.combine(data_ini, dtime.min)
    dt_fim = datetime.combine(data_fim, dtime.max)
    por_dia = {}
    total = 0.0
    qtd_por_dia = {}
    try:
        # Fonte principal: DtoVenda (espelho ERP), priorizando DataFaturamento.
        q = {
            "$and": [
                _filtro_venda_ativa_mongo(),
                {
                    "$or": [
                        {"DataFaturamento": {"$gte": dt_ini, "$lte": dt_fim}},
                        {"Data": {"$gte": dt_ini, "$lte": dt_fim}},
                    ]
                },
            ]
        }
        proj = {
            "DataFaturamento": 1,
            "Data": 1,
            "ValorTotal": 1,
            "ValorLiquido": 1,
            "Total": 1,
            "Valor": 1,
            "ValorFinal": 1,
        }
        for doc in db["DtoVenda"].find(q, proj):
            dt = _dashboard_doc_data_venda(doc)
            if not isinstance(dt, datetime):
                continue
            chave = dt.date().isoformat()
            v = _dashboard_doc_total(doc)
            por_dia[chave] = por_dia.get(chave, 0.0) + v
            qtd_por_dia[chave] = qtd_por_dia.get(chave, 0) + 1
            total += v

        # Fallback: coleção consolidada local quando DtoVenda não trouxer dados.
        if total <= 0:
            vendas_agro = client.obter_vendas_agro_periodo(dt_ini, dt_fim)
            for doc in vendas_agro:
                dt = doc.get("data")
                if not isinstance(dt, datetime):
                    continue
                chave = dt.date().isoformat()
                v = _dashboard_float(doc.get("valor_total"))
                por_dia[chave] = por_dia.get(chave, 0.0) + v
                qtd_por_dia[chave] = qtd_por_dia.get(chave, 0) + 1
                total += v
    except Exception as exc:
        logger.warning("dashboard_gerencial mongo serie: %s", exc, exc_info=True)
        return {"ok": False, "erro": "Falha ao consultar Mongo", "total": 0.0, "por_dia": {}, "qtd_por_dia": {}}
    # Fallback local: se ERP/Mongo não trouxer valores no período, usa VendaAgro (SQLite/Postgres local).
    if total <= 0:
        rows = (
            VendaAgro.objects.filter(criado_em__date__gte=data_ini, criado_em__date__lte=data_fim)
            .values("criado_em__date")
            .annotate(soma=Sum("total"), n=Count("id"))
        )
        por_dia = {}
        qtd_por_dia = {}
        total = 0.0
        for row in rows:
            d = row.get("criado_em__date")
            if not d:
                continue
            v = _dashboard_float(row.get("soma"))
            por_dia[d.isoformat()] = round(v, 2)
            qtd_por_dia[d.isoformat()] = int(row.get("n") or 0)
            total += v
    return {"ok": True, "erro": "", "total": total, "por_dia": por_dia, "qtd_por_dia": qtd_por_dia}


def _dashboard_perdas_validade_hoje():
    hoje = timezone.localdate()
    custo_por_produto = {}
    for row in ProdutoMarcaVariacaoAgro.objects.values("produto_externo_id").annotate(
        custo_medio=Sum("custo_unitario"), n=Count("id")
    ):
        n = int(row.get("n") or 0)
        if n <= 0:
            continue
        custo_por_produto[str(row["produto_externo_id"])] = _dashboard_float(row["custo_medio"]) / n
    total = 0.0
    for lote in (
        EstoqueLote.objects.select_related("overlay")
        .filter(quantidade_atual__gt=0, data_validade__lte=hoje)
        .only("quantidade_atual", "overlay__produto_externo_id", "overlay__preco_venda")
    ):
        pid = str(getattr(lote.overlay, "produto_externo_id", "") or "")
        custo = custo_por_produto.get(pid)
        if custo is None:
            custo = _dashboard_float(getattr(lote.overlay, "preco_venda", 0)) * 0.65
        total += _dashboard_float(lote.quantidade_atual) * max(custo, 0.0)
    return total


def _dashboard_top_produtos_sqlite(data_ini, data_fim, limite=8):
    rows = (
        ItemVendaAgro.objects.filter(venda__criado_em__date__gte=data_ini, venda__criado_em__date__lte=data_fim)
        .values("descricao")
        .annotate(total=Sum("valor_total"), qtd_total=Sum("quantidade"))
        .order_by("-total")[:limite]
    )
    out = []
    for row in rows:
        nome = (row.get("descricao") or "Sem descrição").strip()[:50]
        out.append(
            {
                "nome": nome,
                "total": round(_dashboard_float(row.get("total")), 2),
                "qtd_total": round(_dashboard_float(row.get("qtd_total")), 3),
            }
        )
    return out


def _dashboard_ranking_vendedores_sqlite(data_ini, data_fim, limite=8):
    """Ranking por faturamento (PDV local): campo ``usuario_registro`` da venda."""
    rows = (
        VendaAgro.objects.filter(criado_em__date__gte=data_ini, criado_em__date__lte=data_fim)
        .values("usuario_registro")
        .annotate(total=Sum("total"), n_vendas=Count("id"))
        .order_by("-total")[:limite]
    )
    out = []
    for row in rows:
        raw = (row.get("usuario_registro") or "").strip()
        nome = raw if raw else "Não informado"
        out.append(
            {
                "nome": nome[:120],
                "total": round(_dashboard_float(row.get("total")), 2),
                "n_vendas": int(row.get("n_vendas") or 0),
            }
        )
    return out


def _dashboard_top_produtos_capri(data_ini, data_fim, limite=8):
    """
    Ordem na abertura (rápido → lento): Mongo (espelho) → PDV local (SQLite) → opcional ERP v3 HTTP
    (``AGRO_DASHBOARD_ERP_V3_REPORTS``; só se ainda não houver linhas).
    """
    ck = f"dash:tp:v1:{data_ini}:{data_fim}:{limite}"
    hit = cache.get(ck)
    if isinstance(hit, list):
        return hit
    out: list[dict] = []
    if getattr(settings, "AGRO_DASHBOARD_MONGO_RANKING_FALLBACK", False):
        client, db = obter_conexao_mongo()
        if client is not None and db is not None:
            try:
                rows = dashboard_top_produtos_mongo(
                    client, db, data_ini, data_fim, limite=limite
                )
                if rows is not None and len(rows) > 0:
                    out = rows
            except Exception:
                logger.exception("dashboard_top_produtos_capri: mongo")
    if not out:
        out = _dashboard_top_produtos_sqlite(data_ini, data_fim, limite=limite)
    if not out and getattr(settings, "AGRO_DASHBOARD_ERP_V3_REPORTS", False):
        try:
            api = VendaERPAPIClient()
            ok, rows = api.relatorio_pedidos_itens_report(data_ini, data_fim)
            if ok and rows:
                erp = normalizar_linhas_top_produtos_v3(rows, limite=limite)
                if erp:
                    out = erp
        except Exception:
            logger.exception("dashboard_top_produtos_capri: erp v3")
    if out:
        cache.set(ck, out, timeout=180)
    return out


def _dashboard_ranking_vendedores_capri(data_ini, data_fim, limite=8):
    """
    Mesma ordem que top produtos: Mongo → SQLite → ERP v3 (opcional, no fim).
    """
    ck = f"dash:rv:v1:{data_ini}:{data_fim}:{limite}"
    hit = cache.get(ck)
    if isinstance(hit, list):
        return hit
    out: list[dict] = []
    if getattr(settings, "AGRO_DASHBOARD_MONGO_RANKING_FALLBACK", False):
        client, db = obter_conexao_mongo()
        if client is not None and db is not None:
            try:
                rows = dashboard_ranking_vendedores_mongo(
                    client, db, data_ini, data_fim, limite=limite
                )
                if rows is not None and len(rows) > 0:
                    out = rows
            except Exception:
                logger.exception("dashboard_ranking_vendedores_capri: mongo")
    if not out:
        out = _dashboard_ranking_vendedores_sqlite(data_ini, data_fim, limite=limite)
    if not out and getattr(settings, "AGRO_DASHBOARD_ERP_V3_REPORTS", False):
        try:
            api = VendaERPAPIClient()
            ok, rows = api.relatorio_condensado_vendas_por_vendedor_report(data_ini, data_fim)
            if ok and rows:
                erp = normalizar_linhas_ranking_vendedores_v3(rows, limite=limite)
                if erp:
                    out = erp
        except Exception:
            logger.exception("dashboard_ranking_vendedores_capri: erp v3")
    if out:
        cache.set(ck, out, timeout=180)
    return out


def _dashboard_entregas_pendentes_count() -> int:
    """Pedidos de entrega ainda não entregues nem cancelados."""
    return PedidoEntrega.objects.exclude(
        status__in=(PedidoEntrega.Status.ENTREGUE, PedidoEntrega.Status.CANCELADO)
    ).count()


def _dashboard_entregas_criadas_por_dia_ultimos(n_dias: int = 7) -> list[int]:
    """
    Uma contagem por dia (mais antigo → mais recente) para o sparkline de entregas.
    Base: pedidos criados naquele dia (volume operacional).
    """
    hoje = timezone.localdate()
    n = max(1, min(int(n_dias or 7), 31))
    out: list[int] = []
    for i in range(n - 1, -1, -1):
        d = hoje - timedelta(days=i)
        out.append(PedidoEntrega.objects.filter(criado_em__date=d).count())
    return out


def _dashboard_vendas_por_loja(data_ini, data_fim):
    """Faturamento Centro × Vila Elias — mesma janela e fonte principal que o gráfico diário (DtoVenda)."""
    out = [
        {"loja": "Centro", "total": 0.0, "color": "#00BFFF"},
        {"loja": "Vila Elias", "total": 0.0, "color": "#64748b"},
    ]
    client, db = obter_conexao_mongo()
    if db is None or client is None:
        return out
    dep_centro = str(getattr(client, "DEPOSITO_CENTRO", "") or "")
    dep_vila = str(getattr(client, "DEPOSITO_VILA_ELIAS", "") or "")
    dt_ini = datetime.combine(data_ini, dtime.min)
    dt_fim = datetime.combine(data_fim, dtime.max)
    centro = 0.0
    vila = 0.0

    def _somar_por_deposito(doc: dict, total: float) -> None:
        nonlocal centro, vila
        dep_id = str(doc.get("DepositoID") or "")
        dep_nome = str(doc.get("Deposito") or "").lower()
        empresa_nome = str(doc.get("Empresa") or "").lower()
        if dep_id == dep_centro or "centro" in dep_nome or "centro" in empresa_nome:
            centro += total
        elif dep_id == dep_vila or "vila" in dep_nome or "vila" in empresa_nome:
            vila += total

    try:
        q = {
            "$and": [
                _filtro_venda_ativa_mongo(),
                {
                    "$or": [
                        {"DataFaturamento": {"$gte": dt_ini, "$lte": dt_fim}},
                        {"Data": {"$gte": dt_ini, "$lte": dt_fim}},
                    ]
                },
            ]
        }
        proj = {
            "DataFaturamento": 1,
            "Data": 1,
            "ValorTotal": 1,
            "ValorLiquido": 1,
            "Total": 1,
            "Valor": 1,
            "ValorFinal": 1,
            "DepositoID": 1,
            "Deposito": 1,
            "Empresa": 1,
        }
        for doc in db["DtoVenda"].find(q, proj):
            if not isinstance(_dashboard_doc_data_venda(doc), datetime):
                continue
            _somar_por_deposito(doc, _dashboard_doc_total(doc))

        if centro + vila <= 0:
            vendas = client.obter_vendas_agro_periodo(dt_ini, dt_fim)
            for v in vendas:
                raw = v if isinstance(v, dict) else {}
                if isinstance(v.get("raw"), dict):
                    raw = v.get("raw") or {}
                total = _dashboard_float(v.get("valor_total"))
                _somar_por_deposito(raw, total)
    except Exception as exc:
        logger.warning("dashboard_gerencial vendas_por_loja: %s", exc, exc_info=True)

    out[0]["total"] = round(centro, 2)
    out[1]["total"] = round(vila, 2)
    return out


def _dashboard_contexto_dinamico(request):
    data_ini, data_fim, periodo_label, periodo_key = _dashboard_periodo_from_request(request)
    prev_ini, prev_fim = _dashboard_prev_periodo(data_ini, data_fim)
    atual = _dashboard_mongo_vendas_serie(data_ini, data_fim)
    anterior = _dashboard_mongo_vendas_serie(prev_ini, prev_fim)
    dias = (data_fim - data_ini).days + 1
    labels = [(data_ini + timedelta(days=i)).strftime("%d/%m") for i in range(dias)]
    serie_atual = []
    serie_prev = []
    for i in range(dias):
        d_atual = data_ini + timedelta(days=i)
        d_prev = prev_ini + timedelta(days=i)
        serie_atual.append(round(atual["por_dia"].get(d_atual.isoformat(), 0.0), 2))
        serie_prev.append(round(anterior["por_dia"].get(d_prev.isoformat(), 0.0), 2))
    vendas_hoje = _dashboard_mongo_vendas_serie(timezone.localdate(), timezone.localdate())["total"]
    perdas_validade = _dashboard_perdas_validade_hoje()
    top_produtos = _dashboard_top_produtos_sqlite(data_ini, data_fim)
    produto_filtro = (request.GET.get("produto") or "").strip()
    if produto_filtro:
        top_produtos = [p for p in top_produtos if p["nome"] == produto_filtro] or top_produtos
    return {
        "periodo_label": periodo_label,
        "periodo_key": periodo_key,
        "kpi_vendas_hoje": round(vendas_hoje, 2),
        "kpi_perdas_validade": round(perdas_validade, 2),
        "kpi_faturamento_periodo": round(atual["total"], 2),
        "kpi_variacao_periodo": (
            round(((atual["total"] / anterior["total"]) - 1) * 100, 2)
            if anterior["total"] > 0
            else 0.0
        ),
        "labels_json": json.dumps(labels),
        "serie_atual_json": json.dumps(serie_atual),
        "serie_prev_json": json.dumps(serie_prev),
        "top_produtos": top_produtos,
        "ranking_vendedores": _dashboard_ranking_vendedores_sqlite(data_ini, data_fim),
        "mongo_ok": atual["ok"] and anterior["ok"],
        "mongo_erro": (atual.get("erro") or anterior.get("erro") or "")[:180],
        "produto_filtro": produto_filtro,
    }


def _dashboard_mongo_total_por_dia_vendas_agro(alvo: date) -> float:
    """Soma de vendas do dia com prioridade no espelho ERP (DtoVenda)."""
    client, db = obter_conexao_mongo()
    if db is None or client is None:
        return 0.0
    ini_dt = datetime.combine(alvo, dtime.min)
    fim_dt = datetime.combine(alvo, dtime.max)
    total = _dashboard_mongo_vendas_serie(alvo, alvo).get("total", 0.0)
    if total > 0:
        return total
    try:
        vendas = client.obter_vendas_agro_periodo(ini_dt, fim_dt)
        if vendas:
            return sum(_dashboard_float(v.get("valor_total")) for v in vendas)
    except Exception:
        pass
    agg = VendaAgro.objects.filter(criado_em__date=alvo).aggregate(soma=Sum("total"))
    return _dashboard_float(agg.get("soma"))


def _dashboard_sync_vendas_erp_para_mongo(data_ini: date, data_fim: date):
    api = VendaERPAPIClient()
    mongo = VendaERPMongoClient()
    acumulado = []
    skip = 0
    page_size = 200
    max_paginas = 20
    paginas_lidas = 0
    dt_ini = datetime.combine(data_ini, dtime.min)
    dt_fim = datetime.combine(data_fim, dtime.max)

    while paginas_lidas < max_paginas:
        ok, rows = api.pedidos_listar_periodo(data_ini, data_fim, page_size=page_size, skip=skip)
        if not ok:
            if paginas_lidas == 0:
                return {"ok": False, "erro": "Falha ao buscar vendas do ERP.", "inseridos": 0, "atualizados": 0}
            break
        if not rows:
            break
        paginas_lidas += 1
        skip += len(rows)
        for row in rows:
            doc = mongo.normalizar_pedido_para_vendas_agro(row)
            if not doc:
                continue
            dt = doc.get("data")
            if isinstance(dt, datetime) and dt_ini <= dt <= dt_fim:
                acumulado.append(row)
        if len(rows) < page_size:
            break

    out = mongo.upsert_vendas_agro(acumulado)
    return {"ok": True, **out, "linhas_erp": len(acumulado), "paginas_lidas": paginas_lidas}


def _dashboard_capri_context(request):
    data_ini, data_fim, periodo_label, periodo_key = _dashboard_periodo_from_request(request)
    prev_ini, prev_fim = _dashboard_prev_periodo(data_ini, data_fim)
    atual = _dashboard_mongo_vendas_serie(data_ini, data_fim)
    anterior = _dashboard_mongo_vendas_serie(prev_ini, prev_fim)
    dias = (data_fim - data_ini).days + 1
    labels = [(data_ini + timedelta(days=i)).strftime("%d/%m") for i in range(dias)]
    _wk_map = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sab", "Dom"]
    weekday_initials = [_wk_map[(data_ini + timedelta(days=i)).weekday()] for i in range(dias)]
    serie_atual = [round(atual["por_dia"].get((data_ini + timedelta(days=i)).isoformat(), 0.0), 2) for i in range(dias)]
    # Ticket médio por dia (série real — mesma regra de ticket_por_dia após a lista de kpis, antes reutilizada aqui)
    ticket_por_dia: list = []
    qtd_map = atual.get("qtd_por_dia") or {}
    for i in range(dias):
        chave = (data_ini + timedelta(days=i)).isoformat()
        soma = _dashboard_float(atual["por_dia"].get(chave))
        n = int(qtd_map.get(chave) or 0)
        ticket_por_dia.append(round((soma / n), 2) if n > 0 else 0.0)

    hoje = timezone.localdate()
    vendas_hoje = _dashboard_mongo_total_por_dia_vendas_agro(hoje)
    vendas_ontem = _dashboard_mongo_total_por_dia_vendas_agro(hoje - timedelta(days=1))
    variacao_dia = ((vendas_hoje / vendas_ontem) - 1) * 100 if vendas_ontem > 0 else 0.0

    perdas_validade = _dashboard_perdas_validade_hoje()
    qtd_total_periodo = sum(int(v or 0) for v in (atual.get("qtd_por_dia") or {}).values())
    total_ticket = _dashboard_float(atual.get("total"))
    if qtd_total_periodo <= 0:
        ticket_qs = VendaAgro.objects.filter(criado_em__date__gte=data_ini, criado_em__date__lte=data_fim)
        ticket_agg = ticket_qs.aggregate(total=Sum("total"), n=Count("id"))
        total_ticket = _dashboard_float(ticket_agg.get("total"))
        qtd_total_periodo = int(ticket_agg.get("n") or 0)
    ticket_medio = (total_ticket / qtd_total_periodo) if qtd_total_periodo > 0 else 0.0

    novos_clientes_30 = ClienteAgro.objects.filter(criado_em__date__gte=hoje - timedelta(days=30)).count()
    prev_clientes_30 = ClienteAgro.objects.filter(
        criado_em__date__gte=hoje - timedelta(days=60),
        criado_em__date__lte=hoje - timedelta(days=31),
    ).count()
    tendencia_clientes = ((novos_clientes_30 / prev_clientes_30) - 1) * 100 if prev_clientes_30 > 0 else 0.0

    total_entregas_pendentes = _dashboard_entregas_pendentes_count()
    entregas_serie_7d = _dashboard_entregas_criadas_por_dia_ultimos(7)

    u7 = serie_atual[-7:] if len(serie_atual) >= 7 else list(serie_atual)
    media_fat_7d = round(sum(u7) / max(len(u7), 1), 2)
    t7 = ticket_por_dia[-7:] if len(ticket_por_dia) >= 7 else list(ticket_por_dia)
    t7_com_venda = [x for x in t7 if (x or 0) > 0]
    media_tkt_7d = (
        round(sum(t7_com_venda) / max(len(t7_com_venda), 1), 2) if t7_com_venda else 0.0
    )
    mes_ant_ini, mes_ant_fim = _dashboard_bounds_mes_anterior_para_dia(data_fim)
    ticket_medio_mes_civil_anterior = _dashboard_ticket_medio_intervalo(mes_ant_ini, mes_ant_fim)
    if ticket_medio_mes_civil_anterior > 0 and ticket_medio > 0:
        var_tkt_vs_mes_ant = ((ticket_medio / ticket_medio_mes_civil_anterior) - 1) * 100
        tkt_trend = f"{var_tkt_vs_mes_ant:+.1f}% vs mês ant."
        tkt_trend_class = (
            "text-emerald-800 bg-emerald-200 ring-1 ring-emerald-300"
            if var_tkt_vs_mes_ant >= 0
            else "text-red-800 bg-red-200 ring-1 ring-red-300"
        )
    else:
        tkt_trend = "Sem base mês ant."
        tkt_trend_class = "text-slate-600 bg-slate-100 ring-1 ring-slate-300"
    tot_ent_7d = int(sum(entregas_serie_7d)) if entregas_serie_7d else 0
    hoje_cri = int(entregas_serie_7d[-1]) if entregas_serie_7d else 0
    kpis = [
        {
            "label": "Faturamento do Dia",
            "value": _format_moeda_br(Decimal(str(round(vendas_hoje, 2)))),
            "prefix": "R$",
            "trend": f"{variacao_dia:+.1f}% vs ontem",
            "trend_class": "text-emerald-800 bg-emerald-200 ring-1 ring-emerald-300" if variacao_dia >= 0 else "text-red-800 bg-red-200 ring-1 ring-red-300",
            "context_lines": [
                f"Ontem: {_format_moeda_br(Decimal(str(round(vendas_ontem, 2))))}",
                f"Média 7d (últ. dias do filtro): {_format_moeda_br(Decimal(str(round(media_fat_7d, 2))))}/dia",
            ],
        },
        {
            "label": "Ticket Médio",
            "value": _format_moeda_br(Decimal(str(round(ticket_medio, 2)))),
            "prefix": "R$",
            "trend": tkt_trend,
            "trend_class": tkt_trend_class,
            "context_lines": [
                "Número grande: ticket médio do período filtrado (faturamento ÷ nº de vendas).",
                f"O % do selo compara com o mês civil anterior completo ({mes_ant_ini.strftime('%d/%m/%Y')} a {mes_ant_fim.strftime('%d/%m/%Y')}: ticket {_format_moeda_br(Decimal(str(round(ticket_medio_mes_civil_anterior, 2))))}).",
                f"Referência 7d (últ. dias do gráfico, só dia com venda): {_format_moeda_br(Decimal(str(round(media_tkt_7d, 2))))}.",
            ],
        },
        {
            "label": "Perda Validade",
            "value": _format_moeda_br(Decimal(str(round(perdas_validade, 2)))),
            "prefix": "R$",
            "trend": "Monitorado",
            "trend_class": "text-rose-800 bg-rose-200 ring-1 ring-rose-300",
            "context_lines": [
                "Valor = perda (lotes) hoje, não a série de vendas.",
            ],
        },
        {
            "label": "Novos Clientes",
            "value": str(novos_clientes_30),
            "prefix": "",
            "trend": f"{tendencia_clientes:+.1f}% vs 30d ant.",
            "trend_class": "text-emerald-800 bg-emerald-200 ring-1 ring-emerald-300" if tendencia_clientes >= 0 else "text-red-800 bg-red-200 ring-1 ring-red-300",
            "context_lines": [
                "Cadastros com data de criação nos últimos 30 dias.",
            ],
        },
        {
            "label": "Entregas",
            "value": str(total_entregas_pendentes),
            "prefix": "",
            "trend": "PENDENTES",
            "trend_class": "text-amber-900 bg-amber-200 ring-1 ring-amber-300",
            "context_lines": [
                f"Últ. 7 dias: {tot_ent_7d} pedido(s) criado(s) no agendador · hoje: {hoje_cri} pedido(s).",
            ],
            "variant": "entregas",
        },
    ]

    if periodo_key == "ano":
        labels, serie_atual, ticket_por_dia = _dashboard_serie_mes_a_mes(data_ini, data_fim, atual)
        serie_compare = [0.0] * len(serie_atual)
        weekday_initials = []
    else:
        serie_compare = _dashboard_serie_meta_c_vendas(data_ini, data_fim)

    _mdb, db_fin = obter_conexao_mongo()
    contas_receber: list = []
    contas_pagar: list = []
    total_receber_hoje = 0.0
    total_pagar_hoje = 0.0
    if db_fin is not None:
        try:
            contas_receber, contas_pagar = dashboard_gerencial_linhas_financeiras(
                db_fin, hoje=hoje, limite=12
            )
            q_rec_hoje = lancamentos_montar_query_mongo(
                despesa=False, status="abertos", vencimento_de=hoje, vencimento_ate=hoje
            )
            q_pag_hoje = lancamentos_montar_query_mongo(
                despesa=True, status="abertos", vencimento_de=hoje, vencimento_ate=hoje
            )
            _lr, _tr, tot_rec_hoje = lancamentos_buscar_pagina(
                db_fin, q_rec_hoje, False, page=1, page_size=1, ordenacao="vencimento_asc"
            )
            _lp, _tp, tot_pag_hoje = lancamentos_buscar_pagina(
                db_fin, q_pag_hoje, True, page=1, page_size=1, ordenacao="vencimento_asc"
            )
            total_receber_hoje = round(_dashboard_float(tot_rec_hoje.get("saldo_aberto")), 2)
            total_pagar_hoje = round(_dashboard_float(tot_pag_hoje.get("saldo_aberto")), 2)
        except Exception:
            logger.exception("dashboard_gerencial_linhas_financeiras")
    ontem = hoje - timedelta(days=1)
    total_receber_atraso = 0.0
    total_pagar_atraso = 0.0
    if db_fin is not None:
        try:
            q_rec_atraso = lancamentos_montar_query_mongo(
                despesa=False, status="abertos", vencimento_ate=ontem
            )
            q_pag_atraso = lancamentos_montar_query_mongo(
                despesa=True, status="abertos", vencimento_ate=ontem
            )
            _lra, _tra, tot_rec_atraso = lancamentos_buscar_pagina(
                db_fin, q_rec_atraso, False, page=1, page_size=1, ordenacao="vencimento_asc"
            )
            _lpa, _tpa, tot_pag_atraso = lancamentos_buscar_pagina(
                db_fin, q_pag_atraso, True, page=1, page_size=1, ordenacao="vencimento_asc"
            )
            total_receber_atraso = round(_dashboard_float(tot_rec_atraso.get("saldo_aberto")), 2)
            total_pagar_atraso = round(_dashboard_float(tot_pag_atraso.get("saldo_aberto")), 2)
        except Exception:
            logger.exception("dashboard_gerencial_totais_atraso")

    vendas_por_loja = _dashboard_vendas_por_loja(data_ini, data_fim)
    return {
        "periodo_label": periodo_label,
        "periodo_key": periodo_key,
        "kpis": kpis,
        "chart_labels": json.dumps(labels),
        "chart_weekday_initials": json.dumps(weekday_initials),
        "chart_data": json.dumps(serie_atual),
        "chart_compare_data": json.dumps(serie_compare),
        "chart_total_periodo": _format_moeda_br(Decimal(str(round(_dashboard_float(atual.get("total")), 2)))),
        "ticket_por_dia": json.dumps(ticket_por_dia),
        "top_produtos": _dashboard_top_produtos_capri(data_ini, data_fim),
        "ranking_vendedores": _dashboard_ranking_vendedores_capri(data_ini, data_fim),
        "vendas_por_loja": vendas_por_loja,
        "stores_chart_json": json.dumps(
            {
                "labels": [x["loja"] for x in vendas_por_loja],
                "values": [round(_dashboard_float(x["total"]), 2) for x in vendas_por_loja],
            },
            ensure_ascii=False,
        ),
        "contas_receber": contas_receber,
        "contas_pagar": contas_pagar,
        "total_receber_atraso": total_receber_atraso,
        "total_pagar_atraso": total_pagar_atraso,
        "total_receber_hoje": total_receber_hoje,
        "total_pagar_hoje": total_pagar_hoje,
        "lancamentos_hub_url": reverse("lancamentos_financeiros"),
        "lancamentos_receber_url": reverse("lancamentos_contas_receber"),
        "lancamentos_pagar_url": reverse("lancamentos_contas_pagar"),
        "entregas_painel_url": reverse("entregas_painel"),
        "total_entregas_pendentes": total_entregas_pendentes,
        "mongo_ok": atual["ok"] and anterior["ok"],
        "mongo_erro": (atual.get("erro") or anterior.get("erro") or "")[:180],
    }


# --- VIEWS DE PÁGINA ---
@ensure_csrf_cookie
def home(request):
    nav = _home_admin_navegacao()
    u = ""
    if getattr(request, "user", None) and request.user.is_authenticated:
        u = (request.user.get_full_name() or "").strip() or (
            request.user.get_username() if hasattr(request.user, "get_username") else ""
        )
    home_pdv_bootstrap = {
        "csrfToken": get_token(request),
        "usuarioSalvamento": u,
        "urls": {
            "apiPdvSaldos": reverse("api_pdv_saldos"),
            "apiTodosProdutosDelta": reverse("api_todos_produtos_delta"),
            "apiListCustomers": reverse("api_list_customers"),
            "consultaPdv": reverse("consulta_produtos"),
            "apiPdvSalvarCheckoutDraft": reverse("api_pdv_salvar_checkout_draft"),
            "pdvWizardHome": reverse("pdv_home"),
        },
    }
    stats = _home_quick_stats(request)
    return render(
        request,
        "home.html",
        {
            "empresa_atual": _empresa_home_atual(),
            "home_top_items": nav["top_items"],
            "home_grid_items": nav["grid_items"],
            "home_pdv_bootstrap": home_pdv_bootstrap,
            "home_pdv_url": reverse("pdv_home"),
            "home_validade_url": reverse("relatorios_validade"),
            **stats,
        },
    )


@ensure_csrf_cookie
@login_required(login_url="/admin/login/")
def estoque_sincronizacao_view(request):
    """Painel: saúde da leitura Mongo (espelho ERP), alertas e auditoria da camada Agro."""
    return render(request, "produtos/estoque_sincronizacao.html")


@ensure_csrf_cookie
@_dashboard_login_required
def dashboard_gerencial_view(request):
    # A raiz ``/`` é esta view; probes (Render, uptime) usam HEAD e não precisam do BI completo.
    if request.method == "HEAD":
        return HttpResponse(status=200)
    sync_status = None
    if request.GET.get("sync") == "1" and request.user.is_authenticated:
        di, df, _lbl, _k = _dashboard_periodo_from_request(request)
        sync_status = _dashboard_sync_vendas_erp_para_mongo(di, df)
    return render(
        request,
        "produtos/dashboard_gerencial.html",
        {**_dashboard_capri_context(request), "sync_status": sync_status},
    )


@login_required(login_url="/admin/login/")
@require_POST
def dashboard_gerencial_sincronizar(request):
    di, df, _lbl, _k = _dashboard_periodo_from_request(request)
    out = _dashboard_sync_vendas_erp_para_mongo(di, df)
    status = 200 if out.get("ok") else 502
    return JsonResponse(out, status=status)


@never_cache
@_dashboard_login_required
@require_GET
def dashboard_gerencial_conteudo(request):
    return render(
        request,
        "produtos/partials/dashboard_gerencial_body.html",
        _dashboard_capri_context(request),
    )


@never_cache
@_dashboard_login_required
@require_GET
def dashboard_gerencial_feed(request):
    try:
        offset = max(int(request.GET.get("offset") or 0), 0)
    except ValueError:
        offset = 0
    limite = 20
    produto_filtro = (request.GET.get("produto") or "").strip()
    qs = VendaAgro.objects.order_by("-criado_em")
    if produto_filtro:
        qs = qs.filter(itens__descricao__icontains=produto_filtro).distinct()
    vendas = list(qs[offset : offset + limite])
    next_offset = offset + len(vendas)
    has_more = len(vendas) == limite
    return render(
        request,
        "produtos/partials/dashboard_gerencial_feed.html",
        {
            "vendas": vendas,
            "next_offset": next_offset,
            "has_more": has_more,
            "produto_filtro": produto_filtro,
        },
    )


@require_GET
def api_cron_estoque_mongo_ping(request):
    """
    Agendador externo: ping Mongo e atualiza ``EstoqueSyncHealth``.
    Mesmo token que o cron de alerta de vendas (``ALERTA_VENDAS_CRON_TOKEN``).
    """
    if not _token_cron_alerta_valido(request):
        return JsonResponse({"ok": False, "erro": "token"}, status=403)
    from estoque.sync_health import registrar_ping_mongo

    client, db = obter_conexao_mongo()
    if db is None:
        registrar_ping_mongo(False, "Mongo indisponível (cron)")
        return JsonResponse({"ok": False, "mongo": False})
    try:
        db[client.col_p].find_one({}, {"_id": 1})
        registrar_ping_mongo(True)
        return JsonResponse({"ok": True, "mongo": True})
    except Exception as e:
        registrar_ping_mongo(False, str(e))
        return JsonResponse({"ok": False, "mongo": False, "erro": str(e)[:500]}, status=503)


def _render_pdv_operacional(request, rota_nome="consulta_produtos"):
    pdv_root_url = reverse(rota_nome)
    pdv_dedicado = rota_nome == "pdv_home"
    ctx = {}
    if request.GET.get("reabrir") == "1":
        draft = request.session.get("pdv_checkout")
        if draft and draft.get("itens"):
            ctx["pdv_reabrir_data"] = draft
    ctx["caixa_aberto"] = _obter_sessao_caixa_aberta(request)
    ctx["pdv_entrega_whatsapp"] = getattr(settings, "PDV_ENTREGA_WHATSAPP", "") or ""
    ctx["lancamentos_dre_ativo"] = getattr(settings, "LANCAMENTOS_DRE_ATIVO", False)
    ctx["pdv_root_url"] = pdv_root_url
    ctx["pdv_dedicado"] = pdv_dedicado
    u_pdv = ""
    if getattr(request, "user", None) and request.user.is_authenticated:
        u_pdv = (request.user.get_full_name() or "").strip() or (
            request.user.get_username() if hasattr(request.user, "get_username") else ""
        )
    ctx["pdv_bootstrap"] = {
        "csrfToken": request.META.get("CSRF_COOKIE", "") or "",
        "usuarioSalvamento": u_pdv,
        "urls": {
            "apiPdvSalvarCheckoutDraft": reverse("api_pdv_salvar_checkout_draft"),
            "pdvCheckout": reverse("pdv_checkout"),
            "pdvWizardHome": reverse("pdv_home"),
            "apiEntregaRegistrar": reverse("api_entrega_registrar"),
            "apiListCustomers": reverse("api_list_customers"),
            "apiBuscarClientes": reverse("api_buscar_clientes"),
            "apiPdvTopVendidos": reverse("api_pdv_top_vendidos"),
            "apiPdvSaldos": reverse("api_pdv_saldos"),
            "apiPdvInvalidarCatalogo": reverse("api_pdv_invalidar_catalogo"),
            "apiTodosProdutosDelta": reverse("api_todos_produtos_delta"),
            "apiTodosProdutosLocal": reverse("api_todos_produtos_local"),
            "apiEnviarPedidoErp": reverse("api_enviar_pedido_erp"),
            "apiPdvClienteRapido": reverse("api_pdv_cliente_rapido"),
            "pdvRootUrl": pdv_root_url,
        },
        "assets": {
            "placeholderProduto": static("img/agro-mais-logo-buscador.png"),
        },
    }
    ctx["pdv_consulta_only"] = rota_nome == "consulta_produtos"
    return render(request, "produtos/consulta_produtos.html", ctx)


def consulta_produtos(request):
    return _render_pdv_operacional(request, "consulta_produtos")


def pdv_checkout(request):
    """Legado: rascunho vai para o wizard PDV (sem tela intermediária)."""
    draft = request.session.get("pdv_checkout")
    if not draft or not draft.get("itens"):
        return redirect("consulta_produtos")
    return redirect(f"{reverse('pdv_home')}?reabrir=1")


def vendas_hoje_redirect(request):
    return redirect(f"{reverse('vendas_lista')}?preset=hoje")


@login_required(login_url="/admin/login/")
def vendas_lista(request):
    di, df, label = _periodo_vendas_from_request(request)
    qs = (
        VendaAgro.objects.filter(criado_em__date__gte=di, criado_em__date__lte=df)
        .select_related("sessao_caixa")
        .order_by("-criado_em")
    )
    agg = qs.aggregate(soma=Sum("total"), n=Count("id"))
    soma = agg["soma"] if agg["soma"] is not None else Decimal("0")
    return render(
        request,
        "produtos/vendas_lista.html",
        {
            "data_ini": di,
            "data_fim": df,
            "periodo_label": label,
            "vendas": qs,
            "total_periodo": soma.quantize(Decimal("0.01")),
            "quantidade_vendas": agg["n"] or 0,
            "preset_ativo": (request.GET.get("preset") or "").strip().lower(),
        },
    )


@login_required(login_url="/admin/login/")
def vendas_exportar_csv(request):
    di, df, _label = _periodo_vendas_from_request(request)
    qs = (
        VendaAgro.objects.filter(criado_em__date__gte=di, criado_em__date__lte=df)
        .select_related("sessao_caixa")
        .order_by("-criado_em")
    )
    resp = HttpResponse(content_type="text/csv; charset=utf-8")
    resp["Content-Disposition"] = (
        f'attachment; filename="vendas_{di.isoformat()}_{df.isoformat()}.csv"'
    )
    resp.write("\ufeff")
    w = csv.writer(resp)
    w.writerow(
        [
            "id",
            "data_hora",
            "cliente",
            "cliente_id_erp",
            "cpf_cnpj",
            "forma_pagamento",
            "total",
            "erp_situacao",
            "enviado_erp",
            "usuario",
            "sessao_caixa_id",
        ]
    )
    for v in qs:
        situacao = dict(VendaAgro.ErpSyncStatus.choices).get(
            v.erp_sync_efetivo, v.erp_sync_efetivo
        )
        w.writerow(
            [
                v.pk,
                v.criado_em.strftime("%Y-%m-%d %H:%M:%S"),
                v.cliente_nome,
                v.cliente_id_erp,
                v.cliente_documento,
                v.forma_pagamento,
                str(v.total).replace(".", ","),
                situacao,
                "sim" if v.enviado_erp else "nao",
                v.usuario_registro,
                v.sessao_caixa_id or "",
            ]
        )
    return resp


@login_required(login_url="/admin/login/")
def clientes_lista(request):
    q = (request.GET.get("q") or "").strip()
    qs = ClienteAgro.objects.all()
    if q:
        qs = qs.filter(
            Q(nome__icontains=q)
            | Q(whatsapp__icontains=q)
            | Q(cpf__icontains=q)
            | Q(endereco__icontains=q)
            | Q(cep__icontains=q)
            | Q(cidade__icontains=q)
            | Q(bairro__icontains=q)
            | Q(logradouro__icontains=q)
            | Q(complemento__icontains=q)
        )
    qs = qs.order_by("nome")
    return render(
        request,
        "produtos/clientes_lista.html",
        {"clientes": qs, "busca": q},
    )


@login_required(login_url="/admin/login/")
def cliente_novo(request):
    if request.method == "POST":
        form = ClienteAgroForm(request.POST)
        if form.is_valid():
            obj = form.save(commit=False)
            obj.editado_local = True
            obj.save()
            messages.success(request, "Cliente cadastrado.")
            return redirect("clientes_lista")
    else:
        form = ClienteAgroForm()
    return render(request, "produtos/cliente_form.html", {"form": form, "titulo": "Novo cliente"})


@login_required(login_url="/admin/login/")
def cliente_editar(request, pk):
    cli = get_object_or_404(ClienteAgro, pk=pk)
    if request.method == "POST":
        form = ClienteAgroForm(request.POST, instance=cli)
        if form.is_valid():
            obj = form.save(commit=False)
            obj.editado_local = True
            obj.save()
            messages.success(request, "Cliente atualizado.")
            return redirect("clientes_lista")
    else:
        form = ClienteAgroForm(instance=cli)
    return render(
        request,
        "produtos/cliente_form.html",
        {"form": form, "titulo": f"Editar: {cli.nome}", "cliente": cli},
    )


@login_required(login_url="/admin/login/")
@require_POST
def clientes_sincronizar(request):
    """Importa clientes de Mongo + API ERP para ClienteAgro; não grava no ERP."""
    from .services_clientes_sync import sincronizar_clientes_fontes_para_agro

    try:
        r = sincronizar_clientes_fontes_para_agro()
    except Exception as exc:
        logger.exception("clientes_sincronizar")
        messages.error(request, f"Sincronização falhou: {exc}")
        return redirect("clientes_lista")
    messages.success(
        request,
        (
            f"Sincronizado: {r['criados']} novos, {r['atualizados']} atualizados, "
            f"{r['ignorados_editados_local']} preservados (editados no Agro). "
            f"Fontes: Mongo {r['linhas_mongo']} linhas, ERP {r['linhas_erp']} linhas."
        ),
    )
    cache.delete(API_LIST_CUSTOMERS_CACHE_KEY)
    return redirect("clientes_lista")


@login_required(login_url="/admin/login/")
def caixa_painel(request):
    aberto = _obter_sessao_caixa_aberta(request)
    ctx = {"sessao_aberta": aberto}
    if aberto:
        vendas = VendaAgro.objects.filter(sessao_caixa=aberto)
        ctx["qtd_vendas_sessao"] = vendas.count()
        s = vendas.aggregate(soma=Sum("total"))["soma"]
        ctx["total_vendas_sessao"] = (
            s.quantize(Decimal("0.01")) if s is not None else Decimal("0")
        )
    return render(request, "produtos/caixa_painel.html", ctx)


@login_required(login_url="/admin/login/")
@ensure_csrf_cookie
def caixa_saida_view(request):
    """Formulário dedicado: saída rápida no caixa (plano de conta + quem levou)."""
    from produtos.saida_caixa_planos import SAIDA_CAIXA_PLANOS

    from rh.utils import resolver_empresa_por_nome_fantasia

    empresa_padrao = getattr(settings, "AGRO_SAIDA_CAIXA_EMPRESA_PADRAO", "") or "Agro Mais Centro"
    emp = resolver_empresa_por_nome_fantasia(empresa_padrao)
    empresa_padrao_id = emp.pk if emp else ""
    return render(
        request,
        "produtos/caixa_saida.html",
        {
            "planos_json": json.dumps(SAIDA_CAIXA_PLANOS, ensure_ascii=False),
            "empresa_padrao": empresa_padrao,
            "empresa_padrao_id": empresa_padrao_id,
        },
    )


@login_required(login_url="/admin/login/")
def caixa_abrir(request):
    if _obter_sessao_caixa_aberta(request):
        messages.warning(request, "Já existe um caixa aberto neste navegador. Feche-o antes de abrir outro.")
        return redirect("caixa_painel")
    if request.method == "POST":
        raw = (request.POST.get("valor_abertura") or "0").replace(",", ".").strip()
        try:
            va = Decimal(raw)
        except Exception:
            va = Decimal("0")
        obs = (request.POST.get("observacao_abertura") or "").strip()[:500]
        s = SessaoCaixa.objects.create(
            usuario=request.user,
            valor_abertura=va.quantize(Decimal("0.01")),
            observacao_abertura=obs,
        )
        request.session["pdv_sessao_caixa_id"] = s.pk
        messages.success(request, f"Caixa #{s.pk} aberto. Valor de abertura: R$ {s.valor_abertura}")
        return redirect("consulta_produtos")
    return render(request, "produtos/caixa_abrir.html")


@login_required(login_url="/admin/login/")
def caixa_fechar(request):
    sessao = _obter_sessao_caixa_aberta(request)
    if not sessao:
        messages.info(request, "Nenhum caixa aberto neste navegador.")
        return redirect("caixa_painel")
    vendas = VendaAgro.objects.filter(sessao_caixa=sessao)
    qtd = vendas.count()
    tot = vendas.aggregate(s=Sum("total"))["s"] or Decimal("0")
    tot = tot.quantize(Decimal("0.01"))
    if request.method == "POST":
        raw = (request.POST.get("valor_fechamento") or "").replace(",", ".").strip()
        try:
            vf = Decimal(raw) if raw else None
        except Exception:
            vf = None
        obs = (request.POST.get("observacao_fechamento") or "").strip()[:500]
        sessao.fechado_em = timezone.now()
        if vf is not None:
            sessao.valor_fechamento = vf.quantize(Decimal("0.01"))
        sessao.observacao_fechamento = obs
        sessao.save()
        request.session.pop("pdv_sessao_caixa_id", None)
        messages.success(request, f"Caixa #{sessao.pk} fechado.")
        return redirect("caixa_painel")
    return render(
        request,
        "produtos/caixa_fechar.html",
        {
            "sessao": sessao,
            "qtd_vendas": qtd,
            "total_vendas": tot,
        },
    )


@login_required(login_url="/admin/login/")
@ensure_csrf_cookie
def venda_agro_detalhe(request, pk):
    v = get_object_or_404(
        VendaAgro.objects.select_related("sessao_caixa").prefetch_related("itens"),
        pk=pk,
    )
    erp_txt = ""
    if v.erp_resposta is not None:
        try:
            erp_txt = json.dumps(v.erp_resposta, ensure_ascii=False, indent=2)
        except Exception:
            erp_txt = str(v.erp_resposta)
    return render(
        request,
        "produtos/venda_agro_detalhe.html",
        {"v": v, "erp_resposta_text": erp_txt},
    )


def historico_ajustes(request):
    ajustes = AjusteRapidoEstoque.objects.all().order_by("-criado_em")
    return render(request, "produtos/historico_ajustes.html", {"ajustes": ajustes})


def sugestao_transferencia(request):
    tw = (getattr(settings, "TRANSFERENCIA_WHATSAPP", None) or "").strip()
    if not tw:
        tw = (getattr(settings, "PDV_ENTREGA_WHATSAPP", None) or "").strip()
    return render(
        request,
        "produtos/transferencias.html",
        {"transferencia_whatsapp": tw},
    )


@ensure_csrf_cookie
def compras_view(request):
    return render(request, "produtos/compras.html")


def _ctx_produtos_cadastro_erp(request):
    return {
        "pode_editar_overlay": getattr(request, "user", None) and request.user.is_authenticated,
        "login_overlay_next": request.get_full_path() or reverse("produtos_cadastro_erp"),
    }


@ensure_csrf_cookie
def produtos_cadastro_erp_view(request):
    """Lista do cadastro espelho ERP; clique abre página dedicada de detalhe."""
    q = (request.GET.get("produto") or "").strip()
    if q:
        return redirect("produtos_cadastro_erp_produto", produto_id=q)
    ctx = _ctx_produtos_cadastro_erp(request)
    ctx["cadastro_erp_modo"] = "lista"
    ctx["cadastro_erp_produto_id"] = ""
    return render(request, "produtos/produtos_cadastro_erp.html", ctx)


@ensure_csrf_cookie
def produtos_cadastro_erp_produto_view(request, produto_id: str):
    """Detalhe de um produto do espelho ERP (tela cheia, sem lista ao lado)."""
    ctx = _ctx_produtos_cadastro_erp(request)
    ctx["cadastro_erp_modo"] = "detalhe"
    ctx["cadastro_erp_produto_id"] = str(produto_id or "").strip()
    return render(request, "produtos/produtos_cadastro_erp.html", ctx)


@ensure_csrf_cookie
@login_required(login_url="/admin/login/")
def produtos_gestao_view(request):
    """Gestão operacional de produtos (espelho Mongo + overlay local + ajuste estoque Agro)."""
    emp = Empresa.objects.first()
    return render(
        request,
        "produtos/produtos_gestao.html",
        {
            "empresa_nome": getattr(emp, "nome_fantasia", None) or getattr(emp, "razao_social", None) or "",
            "usuario_label": (getattr(request.user, "email", None) or request.user.get_username() or str(request.user.pk))[:120],
        },
    )


def _lancamentos_parse_date_param(s):
    if not s or not str(s).strip():
        return None
    try:
        return date.fromisoformat(str(s).strip()[:10])
    except ValueError:
        return None


def _lancamentos_filtros_echo_dict(
    v_de: date | None,
    v_ate: date | None,
    c_de: date | None,
    c_ate: date | None,
    p_de: date | None,
    p_ate: date | None,
) -> dict[str, str | None]:
    """Eco das datas efetivamente lidas do GET (confirmação na UI / suporte)."""

    def iso(d: date | None) -> str | None:
        return d.isoformat() if d else None

    return {
        "venc_de": iso(v_de),
        "venc_ate": iso(v_ate),
        "comp_de": iso(c_de),
        "comp_ate": iso(c_ate),
        "pag_de": iso(p_de),
        "pag_ate": iso(p_ate),
    }


def _lancamentos_excluir_planos_from_request(request) -> list[str]:
    raw = request.GET.getlist("excluir_plano")
    out: list[str] = []
    seen: set[str] = set()
    for x in raw:
        s = (x or "").strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s[:400])
        if len(out) >= 200:
            break
    return out


def _api_lancamentos_lista_core(request, despesa: bool):
    status = (request.GET.get("status") or "abertos").strip().lower()
    if status not in ("abertos", "quitados", "todos"):
        status = "abertos"

    v_de = _lancamentos_parse_date_param(request.GET.get("venc_de"))
    v_ate = _lancamentos_parse_date_param(request.GET.get("venc_ate"))
    c_de = _lancamentos_parse_date_param(request.GET.get("comp_de"))
    c_ate = _lancamentos_parse_date_param(request.GET.get("comp_ate"))
    p_de = _lancamentos_parse_date_param(request.GET.get("pag_de"))
    p_ate = _lancamentos_parse_date_param(request.GET.get("pag_ate"))
    filtros_echo = _lancamentos_filtros_echo_dict(v_de, v_ate, c_de, c_ate, p_de, p_ate)
    texto = (request.GET.get("q") or "").strip() or None

    _, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse(
            {
                "erro": "Mongo indisponível",
                "lancamentos": [],
                "total": 0,
                "page": 1,
                "page_size": 50,
                "totais": {},
                "filtros": filtros_echo,
            },
            status=503,
        )

    try:
        page = max(1, int(request.GET.get("page") or 1))
    except ValueError:
        page = 1
    try:
        page_size = int(request.GET.get("page_size") or 50)
    except ValueError:
        page_size = 50

    ordenacao = (request.GET.get("ordenacao") or "vencimento_asc").strip().lower()
    if ordenacao not in LANCAMENTOS_ORDENACOES_VALIDAS:
        ordenacao = "vencimento_asc"

    excl_planos = _lancamentos_excluir_planos_from_request(request)
    query = lancamentos_montar_query_mongo(
        despesa=despesa,
        status=status,
        vencimento_de=v_de,
        vencimento_ate=v_ate,
        competencia_de=c_de,
        competencia_ate=c_ate,
        pagamento_de=p_de,
        pagamento_ate=p_ate,
        texto=texto,
        excluir_planos_nomes=excl_planos or None,
    )
    linhas, total, totais = lancamentos_buscar_pagina(
        db,
        query,
        despesa,
        page=page,
        page_size=page_size,
        ordenacao=ordenacao,
    )

    tot_out = {
        "quantidade": totais["quantidade"],
        "bruto": totais["bruto"],
        "movimentado": totais["movimentado"],
        "saldo_aberto": totais["saldo_aberto"],
        "previsto": totais["bruto"],
        "pago": totais["movimentado"],
        "a_pagar": totais["saldo_aberto"] if despesa else 0.0,
        "a_receber": totais["saldo_aberto"] if not despesa else 0.0,
    }

    return JsonResponse(
        {
            "lancamentos": linhas,
            "total": total,
            "page": page,
            "page_size": page_size,
            "totais": tot_out,
            "status_filtro": status,
            "tipo": "pagar" if despesa else "receber",
            "planos_excluidos_aplicados": len(excl_planos),
            "filtros": filtros_echo,
        }
    )


def _ctx_lancamentos_financeiros(modo_contas: str):
    """
    ``modo_contas``: ``pagar`` | ``receber`` — lista fixa em um tipo (sem abas).
    """
    return {
        "lancamentos_dre_ativo": getattr(settings, "LANCAMENTOS_DRE_ATIVO", False),
        "modo_contas": modo_contas,
    }


@ensure_csrf_cookie
@login_required(login_url="/admin/login/")
def resumo_financeiro_gerencial_view(request):
    """DRE gerencial (Postgres + consolidação grupo) — leitura de snapshot agregado."""
    from financeiro.models import GrupoEmpresarial

    empresas = Empresa.objects.filter(ativo=True).order_by("nome_fantasia")
    grupos = GrupoEmpresarial.objects.filter(ativo=True).order_by("nome")
    return render(
        request,
        "produtos/resumo_financeiro_gerencial.html",
        {
            "empresas": empresas,
            "grupos": grupos,
        },
    )


@ensure_csrf_cookie
@login_required(login_url="/admin/login/")
def lancamentos_financeiros_view(request):
    """Entrada do módulo: escolha entre Contas a pagar e Contas a receber."""
    return render(
        request,
        "produtos/lancamentos_hub.html",
        {
            "lancamentos_dre_ativo": getattr(settings, "LANCAMENTOS_DRE_ATIVO", False),
        },
    )


@ensure_csrf_cookie
@login_required(login_url="/admin/login/")
def lancamentos_contas_pagar_view(request):
    """Lista de contas a pagar (filtros, export, baixa)."""
    return render(
        request,
        "produtos/lancamentos_financeiros.html",
        _ctx_lancamentos_financeiros("pagar"),
    )


@ensure_csrf_cookie
@login_required(login_url="/admin/login/")
def lancamentos_contas_receber_view(request):
    """Lista de contas a receber (filtros, export, baixa)."""
    return render(
        request,
        "produtos/lancamentos_financeiros.html",
        _ctx_lancamentos_financeiros("receber"),
    )


@ensure_csrf_cookie
@login_required(login_url="/admin/login/")
def lancamentos_dre_view(request):
    """DRE simples por plano (Mongo) — tela separada; desligada por LANCAMENTOS_DRE_ATIVO."""
    if not getattr(settings, "LANCAMENTOS_DRE_ATIVO", False):
        return render(request, "produtos/lancamentos_dre_desativado.html")
    return render(request, "produtos/lancamentos_dre.html")


@never_cache
@login_required(login_url="/admin/login/")
@require_GET
def api_lancamentos_lista(request):
    """Lista paginada: ``?tipo=pagar`` (default) ou ``?tipo=receber``."""
    tipo = (request.GET.get("tipo") or "pagar").strip().lower()
    despesa = tipo != "receber"
    return _api_lancamentos_lista_core(request, despesa)


@ensure_csrf_cookie
@login_required(login_url="/admin/login/")
def lancamentos_fluxo_calendario_view(request):
    """Calendário analítico: projeção de fluxo (vendas médias + vencimentos)."""
    return render(request, "produtos/lancamentos_fluxo_calendario.html")


@never_cache
@login_required(login_url="/admin/login/")
@require_GET
def api_lancamentos_planos_distintos(request):
    """Planos de conta distintos no filtro atual (para marcar/desmarcar exclusões)."""
    _, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"erro": "Mongo indisponível", "planos": []}, status=503)
    tipo = (request.GET.get("tipo") or "pagar").strip().lower()
    despesa = tipo != "receber"
    status = (request.GET.get("status") or "abertos").strip().lower()
    if status not in ("abertos", "quitados", "todos"):
        status = "abertos"
    v_de = _lancamentos_parse_date_param(request.GET.get("venc_de"))
    v_ate = _lancamentos_parse_date_param(request.GET.get("venc_ate"))
    c_de = _lancamentos_parse_date_param(request.GET.get("comp_de"))
    c_ate = _lancamentos_parse_date_param(request.GET.get("comp_ate"))
    p_de = _lancamentos_parse_date_param(request.GET.get("pag_de"))
    p_ate = _lancamentos_parse_date_param(request.GET.get("pag_ate"))
    texto = (request.GET.get("q") or "").strip() or None
    try:
        lim = min(int(request.GET.get("limit") or 400), 500)
    except ValueError:
        lim = 400
    planos = lancamentos_planos_distintos_no_filtro(
        db,
        despesa=despesa,
        status=status,
        vencimento_de=v_de,
        vencimento_ate=v_ate,
        competencia_de=c_de,
        competencia_ate=c_ate,
        pagamento_de=p_de,
        pagamento_ate=p_ate,
        texto=texto,
        limit=lim,
    )
    return JsonResponse({"planos": planos})


@never_cache
@login_required(login_url="/admin/login/")
@require_http_methods(["GET", "POST", "DELETE"])
def api_lancamentos_atalhos_filtro(request):
    """Dois atalhos de filtro por usuário (payload JSON espelha favoritos locais)."""
    if request.method == "GET":
        rows = LancamentoAtalhoFiltro.objects.filter(usuario=request.user).order_by("slot")
        return JsonResponse(
            {
                "ok": True,
                "atalhos": [
                    {"slot": r.slot, "nome": r.nome, "payload": r.payload or {}}
                    for r in rows
                ],
            }
        )
    if request.method == "POST":
        try:
            body = json.loads(request.body.decode("utf-8") or "{}")
        except Exception:
            return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)
        try:
            slot = int(body.get("slot"))
        except (TypeError, ValueError):
            return JsonResponse({"ok": False, "erro": "slot inválido"}, status=400)
        if slot not in (1, 2):
            return JsonResponse({"ok": False, "erro": "slot inválido"}, status=400)
        nome = (body.get("nome") or "").strip()[:80]
        if not nome:
            return JsonResponse({"ok": False, "erro": "nome obrigatório"}, status=400)
        payload = body.get("payload")
        if not isinstance(payload, dict):
            return JsonResponse({"ok": False, "erro": "payload deve ser objeto"}, status=400)
        obj, _ = LancamentoAtalhoFiltro.objects.update_or_create(
            usuario=request.user,
            slot=slot,
            defaults={"nome": nome, "payload": payload},
        )
        return JsonResponse({"ok": True, "slot": obj.slot, "nome": obj.nome})
    try:
        slot = int(request.GET.get("slot") or request.POST.get("slot") or 0)
    except (TypeError, ValueError):
        return JsonResponse({"ok": False, "erro": "slot inválido"}, status=400)
    if slot not in (1, 2):
        return JsonResponse({"ok": False, "erro": "slot inválido"}, status=400)
    LancamentoAtalhoFiltro.objects.filter(usuario=request.user, slot=slot).delete()
    return JsonResponse({"ok": True})


@login_required(login_url="/admin/login/")
@require_GET
def api_lancamentos_fluxo_calendario(request):
    """JSON: projeção diária (média vendas + títulos a pagar/receber por vencimento)."""
    _, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"erro": "Mongo indisponível", "dias": [], "meta": {}}, status=503)
    try:
        horiz = int(request.GET.get("horizonte") or 60)
    except ValueError:
        horiz = 60
    try:
        dias_m = int(request.GET.get("dias_media") or 30)
    except ValueError:
        dias_m = 30
    incl = (request.GET.get("incluir_media") or "1").strip().lower() not in (
        "0",
        "false",
        "nao",
        "não",
        "no",
    )
    out = financeiro_projecao_fluxo_diario(
        db,
        dias_media_vendas=dias_m,
        horizonte_dias=horiz,
        incluir_media_vendas=incl,
    )
    if out.get("erro"):
        return JsonResponse(out, status=503)
    return JsonResponse(out)


def _mascarar_cnpj(cnpj: str) -> str:
    d = re.sub(r"\D", "", str(cnpj or ""))
    if len(d) != 14:
        return ""
    return f"**.***.***/****-{d[-2:]}"


@ensure_csrf_cookie
@login_required(login_url="/admin/login/")
def entrada_nota_view(request):
    """Entrada de NF-e: manual, XML e Distribuição DF-e (SEFAZ)."""
    empresas_entrada_nfe = [
        {"id": e.pk, "nome": e.nome_fantasia}
        for e in Empresa.objects.filter(ativo=True).order_by("nome_fantasia")
    ]
    return render(
        request,
        "produtos/entrada_nota.html",
        {"empresas_entrada_nfe": empresas_entrada_nfe},
    )


@login_required(login_url="/admin/login/")
@require_GET
def api_entrada_nota_sefaz_status(request):
    from produtos.sefaz_dfe_client import distribuicao_dfe_configurada

    cnpj = re.sub(r"\D", "", config("NFE_DIST_DFE_CNPJ", default="") or "")[:14]
    return JsonResponse(
        {
            "configurada": distribuicao_dfe_configurada(),
            "uf": (config("NFE_DIST_DFE_UF", default="") or "").strip().upper()[:2],
            "cnpj_mascarado": _mascarar_cnpj(cnpj),
            "tp_amb": config("NFE_DIST_DFE_TP_AMB", default="2"),
        }
    )


@login_required(login_url="/admin/login/")
@require_POST
def api_entrada_nota_parse_xml(request):
    arq = request.FILES.get("arquivo")
    if not arq:
        return JsonResponse({"ok": False, "erro": "Envie um arquivo XML (NF-e autorizada)."}, status=400)
    raw = arq.read()
    if len(raw) > 2_500_000:
        return JsonResponse({"ok": False, "erro": "Arquivo muito grande (máx. ~2,5 MB)."}, status=400)
    parsed = parse_nfe_xml_bytes(raw)
    if not parsed.get("ok"):
        return JsonResponse(
            {"ok": False, "erro": parsed.get("erro") or "Não foi possível ler a NF-e."},
            status=400,
        )
    client, db = obter_conexao_mongo()
    if db is not None and client is not None:
        parsed["itens"] = casar_produtos_mongo(db, client.col_p, parsed.get("itens") or [])
    return JsonResponse({"ok": True, "nota": parsed})


@login_required(login_url="/admin/login/")
@require_POST
def api_entrada_nota_salvar(request):
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)
    modo = str(payload.get("modo") or "manual").strip()[:40]
    cab = payload.get("cabecalho") if isinstance(payload.get("cabecalho"), dict) else {}
    linhas = payload.get("linhas")
    if not isinstance(linhas, list) or not linhas:
        return JsonResponse({"ok": False, "erro": "Inclua ao menos uma linha."}, status=400)
    xml_chave = str(payload.get("xml_chave") or "").strip()[:44] or None
    extra = payload.get("extra") if isinstance(payload.get("extra"), dict) else {}
    usuario = ""
    if request.user.is_authenticated:
        usuario = (
            getattr(request.user, "email", None) or request.user.get_username() or str(request.user.pk)
        )[:120]
    _, db = obter_conexao_mongo()
    r = salvar_rascunho_entrada(
        db,
        usuario=usuario,
        modo=modo,
        cabecalho=cab,
        linhas=linhas,
        xml_chave=xml_chave,
        extra=extra,
    )
    st = 200 if r.get("ok") else 400
    return JsonResponse(r, status=st)


@login_required(login_url="/admin/login/")
@require_GET
def api_entrada_nota_rascunhos(request):
    _, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"erro": "Mongo indisponível", "itens": []}, status=503)
    try:
        lim = min(int(request.GET.get("limit") or 25), 80)
    except ValueError:
        lim = 25
    filtro = (request.GET.get("filtro") or "todas").strip()[:24]
    return JsonResponse({"itens": listar_rascunhos_entrada(db, limit=lim, filtro=filtro or None)})


@login_required(login_url="/admin/login/")
@require_GET
def api_entrada_nota_rascunho_obter(request):
    oid = (request.GET.get("id") or "").strip()
    _, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"ok": False, "erro": "Mongo indisponível"}, status=503)
    doc = obter_rascunho_entrada(db, oid)
    if not doc:
        return JsonResponse({"ok": False, "erro": "Rascunho não encontrado ou ID inválido."}, status=404)
    return JsonResponse({"ok": True, "rascunho": doc})


@login_required(login_url="/admin/login/")
@require_POST
def api_entrada_nota_rascunho_excluir(request):
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)
    oid = str(payload.get("id") or "").strip()
    _, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"ok": False, "erro": "Mongo indisponível"}, status=503)
    r = excluir_rascunho_entrada(db, oid)
    st = 200 if r.get("ok") else 400
    return JsonResponse(r, status=st)


@login_required(login_url="/admin/login/")
@require_POST
def api_entrada_nota_rascunho_atualizar(request):
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)
    oid = str(payload.get("id") or "").strip()
    modo = str(payload.get("modo") or "manual").strip()[:40]
    cab = payload.get("cabecalho") if isinstance(payload.get("cabecalho"), dict) else {}
    linhas = payload.get("linhas")
    if not oid:
        return JsonResponse({"ok": False, "erro": "Informe o id do rascunho."}, status=400)
    if not isinstance(linhas, list) or not linhas:
        return JsonResponse({"ok": False, "erro": "Inclua ao menos uma linha."}, status=400)
    xml_chave = str(payload.get("xml_chave") or "").strip()[:44] or None
    extra = payload.get("extra") if isinstance(payload.get("extra"), dict) else {}
    usuario = ""
    if request.user.is_authenticated:
        usuario = (
            getattr(request.user, "email", None) or request.user.get_username() or str(request.user.pk)
        )[:120]
    _, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"ok": False, "erro": "Mongo indisponível"}, status=503)
    r = atualizar_rascunho_entrada(
        db,
        oid,
        usuario=usuario,
        modo=modo,
        cabecalho=cab,
        linhas=linhas,
        xml_chave=xml_chave,
        extra=extra,
    )
    st = 200 if r.get("ok") else 400
    return JsonResponse(r, status=st)


@login_required(login_url="/admin/login/")
@require_POST
def api_entrada_nota_rascunho_acao(request):
    """Descartar ou reabrir nota (rascunho) na listagem. Encerrar manual está desativado."""
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)
    oid = str(payload.get("id") or "").strip()
    acao = str(payload.get("acao") or "").strip().lower()
    if not oid:
        return JsonResponse({"ok": False, "erro": "Informe o id do rascunho."}, status=400)
    usuario = ""
    if request.user.is_authenticated:
        usuario = (
            getattr(request.user, "email", None) or request.user.get_username() or str(request.user.pk)
        )[:120]
    _, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"ok": False, "erro": "Mongo indisponível"}, status=503)
    r = pipeline_acao_rascunho_entrada(db, oid, acao, usuario=usuario)
    st = 200 if r.get("ok") else 400
    return JsonResponse(r, status=st)


def _empresa_loja_padrao_agro_estoque(deposito: str) -> tuple[Empresa | None, Loja | None]:
    dep = (deposito or "centro").strip().lower()
    if dep not in ("centro", "vila"):
        dep = "centro"
    empresa = Empresa.objects.filter(nome_fantasia="Agro Mais").first()
    loja = None
    if empresa:
        if dep == "vila":
            loja = Loja.objects.filter(empresa=empresa, nome__icontains="vila").first()
        else:
            loja = Loja.objects.filter(empresa=empresa, nome__icontains="centro").first()
    return empresa, loja


def _empresa_loja_entrada_nfe(deposito: str, empresa_faturada_id: int | None) -> tuple[Empresa | None, Loja | None]:
    """Empresa escolhida na tela de NF-e + loja ativa (Centro ou Vila) dessa empresa."""
    dep = (deposito or "centro").strip().lower()
    if dep not in ("centro", "vila"):
        dep = "centro"
    empresa: Empresa | None = None
    if empresa_faturada_id is not None:
        try:
            eid = int(empresa_faturada_id)
        except (TypeError, ValueError):
            eid = None
        if eid:
            empresa = Empresa.objects.filter(pk=eid, ativo=True).first()
    if empresa is None:
        empresa = Empresa.objects.filter(nome_fantasia="Agro Mais").first()
    if empresa is None:
        empresa = Empresa.objects.filter(ativo=True).order_by("pk").first()
    loja = None
    if empresa:
        qs = Loja.objects.filter(empresa=empresa, ativa=True).order_by("nome")
        if dep == "vila":
            loja = qs.filter(nome__icontains="vila").first()
        else:
            loja = qs.filter(nome__icontains="centro").first()
        if loja is None:
            loja = qs.first()
    return empresa, loja


def _saldo_erp_produto_deposito_mongo(db, client_m, produto_id: str, deposito: str) -> Decimal:
    dep_id = (
        client_m.DEPOSITO_VILA_ELIAS if deposito == "vila" else client_m.DEPOSITO_CENTRO
    )
    tot = Decimal("0")
    try:
        for e in db[client_m.col_e].find({"ProdutoID": produto_id, "DepositoID": dep_id}):
            tot += Decimal(str(float(e.get("Saldo") or 0)))
    except Exception:
        pass
    return tot.quantize(Decimal("0.001"))


def _saldo_final_agro_com_pin(produto_id: str, deposito: str, saldo_erp: Decimal) -> Decimal:
    aj = (
        AjusteRapidoEstoque.objects.filter(produto_externo_id=produto_id, deposito=deposito)
        .order_by("-criado_em")
        .first()
    )
    if aj is None:
        return saldo_erp.quantize(Decimal("0.001"))
    ref = Decimal(str(aj.saldo_erp_referencia))
    inf = Decimal(str(aj.saldo_informado))
    return (inf + (saldo_erp - ref)).quantize(Decimal("0.001"))


def aplicar_entrada_nota_estoque_agro(
    *,
    db,
    client_m,
    linhas: list,
    deposito: str,
    usuario_label: str,
    cabecalho: dict | None,
    usuario_django=None,
    empresa_faturada_id: int | None = None,
) -> dict:
    """
    Incrementa o saldo **visto pelo Agro** via ``AjusteRapidoEstoque``, sem alterar o Mongo do ERP.
    Mantém a mesma lógica do PDV/ajuste PIN: final = saldo_informado + (ERP_atual - saldo_erp_referencia).
    """
    dep = (deposito or "centro").strip().lower()
    if dep not in ("centro", "vila"):
        dep = "centro"
    cab = cabecalho if isinstance(cabecalho, dict) else {}
    eid_payload = empresa_faturada_id
    if eid_payload is None and cab:
        raw_e = cab.get("empresa_faturada_id")
        if raw_e not in (None, ""):
            try:
                eid_payload = int(str(raw_e).strip())
            except (TypeError, ValueError):
                eid_payload = None
    empresa, loja = _empresa_loja_entrada_nfe(dep, eid_payload)
    if empresa is None:
        return {
            "ok": False,
            "aplicados": [],
            "erros": [{"erro": "Nenhuma empresa ativa. Cadastre em Admin → Empresas."}],
            "avisos": [{"msg": "Defina a empresa faturada na nota antes de registrar o estoque."}],
        }
    if loja is None:
        return {
            "ok": False,
            "aplicados": [],
            "erros": [
                {
                    "erro": (
                        f'Empresa «{empresa.nome_fantasia}» sem loja ativa compatível '
                        f'({"Vila" if dep == "vila" else "Centro"}). Cadastre a loja ou ajuste o depósito.'
                    )
                }
            ],
            "avisos": [],
        }
    nf_bits = " ".join(
        p
        for p in (
            str(cab.get("numero") or "").strip() and f"NF {cab.get('numero')}",
            (str(cab.get("chave") or "").strip()[:12] + "…") if cab.get("chave") else "",
        )
        if p
    )
    ref_txt = (nf_bits or "entrada NF-e")[:120]
    user = (usuario_label or "Agro")[:80]

    aplicados: list[dict] = []
    erros: list[dict] = []
    idx = 0
    for ln in linhas or []:
        idx += 1
        if not isinstance(ln, dict):
            continue
        pid = str(ln.get("produto_id") or "").strip()
        if not pid or pid.startswith("local:"):
            continue
        try:
            qtd = Decimal(str(ln.get("q_estoque", ln.get("q_com", 0))).replace(",", ".").strip() or "0")
        except Exception:
            erros.append({"linha": idx, "erro": "Quantidade inválida."})
            continue
        if qtd <= 0:
            continue

        saldo_erp = _saldo_erp_produto_deposito_mongo(db, client_m, pid, dep)
        saldo_final_antes = _saldo_final_agro_com_pin(pid, dep, saldo_erp)
        saldo_final_depois = (saldo_final_antes + qtd).quantize(Decimal("0.001"))

        doc = _produto_mongo_por_id_externo(db, client_m, pid)
        nome_p = str((doc or {}).get("Nome") or ln.get("x_prod") or "")[:200]
        codigo = str((doc or {}).get("CodigoNFe") or (doc or {}).get("Codigo") or ln.get("c_prod") or "")[
            :100
        ]

        try:
            adj = AjusteRapidoEstoque.objects.create(
                empresa=empresa,
                loja=loja,
                produto_externo_id=pid[:100],
                codigo_interno=codigo,
                nome_produto=(f"{nome_p} · Entrada NF-e Agro ({ref_txt}) · {user}")[:255],
                deposito=dep,
                saldo_erp_referencia=saldo_erp,
                saldo_informado=saldo_final_depois,
                origem=OrigemAjusteEstoque.ENTRADA_NF_AGRO,
                usuario=usuario_django if usuario_django is not None else None,
            )
            aplicados.append(
                {
                    "linha": idx,
                    "produto_id": pid,
                    "deposito": dep,
                    "quantidade": float(qtd),
                    "saldo_erp_mongo": float(saldo_erp),
                    "saldo_agro_antes": float(saldo_final_antes),
                    "saldo_agro_depois": float(saldo_final_depois),
                    "ajuste_id": adj.pk,
                }
            )
        except Exception as exc:
            logger.exception("aplicar_entrada_nota_estoque_agro linha %s", idx)
            erros.append({"linha": idx, "produto_id": pid, "erro": str(exc)[:300]})

    return {
        "ok": len(erros) == 0 and len(aplicados) > 0,
        "aplicados": aplicados,
        "erros": erros,
        "avisos": []
        if aplicados
        else ([{"msg": "Nenhuma linha com produto vinculado (catálogo) e quantidade > 0."}]),
    }


def aplicar_baixa_estoque_venda_agro(
    *,
    db,
    client_m,
    venda: VendaAgro,
    deposito: str,
    usuario_label: str,
    usuario_django=None,
) -> dict:
    """
    Baixa de estoque só na camada Agro (``AjusteRapidoEstoque``), como entrada NF / PIN.
    Não altera o Mongo do ERP diretamente — o saldo exibido no PDV segue a fórmula Agro.
    """
    dep_sess = (deposito or "centro").strip().lower()
    if dep_sess not in ("centro", "vila"):
        dep_sess = "centro"
    user = (usuario_label or "PDV")[:80]
    aplicados: list[dict] = []
    erros: list[dict] = []

    def _uma_baixa(
        pid_loc: str,
        qtd_loc: Decimal,
        nome_ref: str,
        codigo_interno_ln: str,
        dep_loc: str,
    ) -> None:
        dep_l = (dep_loc or "centro").strip().lower()
        if dep_l not in ("centro", "vila"):
            dep_l = "centro"
        empresa, loja = _empresa_loja_padrao_agro_estoque(dep_l)
        saldo_erp = _saldo_erp_produto_deposito_mongo(db, client_m, pid_loc, dep_l)
        saldo_antes = _saldo_final_agro_com_pin(pid_loc, dep_l, saldo_erp)
        saldo_depois = (saldo_antes - qtd_loc).quantize(Decimal("0.001"))
        try:
            AjusteRapidoEstoque.objects.create(
                empresa=empresa,
                loja=loja,
                produto_externo_id=pid_loc[:100],
                codigo_interno=str(codigo_interno_ln or "")[:100],
                nome_produto=(
                    f"{(nome_ref or '')[:120]} · Baixa venda #{venda.pk} Agro ({user})"
                )[:255],
                deposito=dep_l,
                saldo_erp_referencia=saldo_erp,
                saldo_informado=saldo_depois,
                origem=OrigemAjusteEstoque.BAIXA_VENDA_PDV,
                usuario=usuario_django if usuario_django is not None else None,
            )
            aplicados.append(
                {
                    "produto_id": pid_loc,
                    "deposito": dep_l,
                    "quantidade": float(qtd_loc),
                    "saldo_agro_antes": float(saldo_antes),
                    "saldo_agro_depois": float(saldo_depois),
                }
            )
        except Exception as exc:
            logger.exception(
                "aplicar_baixa_estoque_venda_agro venda=%s produto=%s", venda.pk, pid_loc
            )
            erros.append({"produto_id": pid_loc, "erro": str(exc)[:300]})

    for it in venda.itens.all():
        pid = str(it.produto_id_externo or "").strip()
        if not pid or pid.lower().startswith("local:"):
            continue
        try:
            qtd = Decimal(str(it.quantidade))
        except Exception:
            erros.append({"produto_id": pid, "erro": "Quantidade inválida."})
            continue
        if qtd <= 0:
            continue

        ov = ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid[:64]).first()
        ex = (ov.cadastro_extras if ov and isinstance(ov.cadastro_extras, dict) else {}) or {}
        kit_cfg = ex.get("kit") if isinstance(ex.get("kit"), dict) else {}
        baixa_cmp = bool(kit_cfg.get("baixa_componentes"))
        dep_kit = _deposito_baixa_kit_componente(kit_cfg, dep_sess)

        p_doc = _produto_mongo_por_id_externo(db, client_m, pid) if db is not None else None
        comp = _extrair_composicao_produto_mongo(p_doc or {}) if p_doc else []

        if baixa_cmp and comp:
            for c in comp:
                child_pid = str(c.get("produto_id") or "").strip()
                if not child_pid:
                    continue
                qraw = c.get("quantidade") or c.get("Qtd") or 1
                try:
                    fq = Decimal(str(qraw).replace(",", ".").strip() or "1")
                except Exception:
                    fq = Decimal(1)
                if fq <= 0:
                    fq = Decimal(1)
                q_need = (fq * qtd).quantize(Decimal("0.001"))
                nome_c = str(c.get("nome") or "").strip() or child_pid
                cod_c = str(c.get("codigo") or "").strip()
                dep_linha = dep_kit
                cdep = str(c.get("deposito") or "").strip().lower()
                if cdep in ("centro", "vila"):
                    dep_linha = cdep
                elif cdep == "1":
                    dep_linha = "centro"
                elif cdep == "2":
                    dep_linha = "vila"
                elif cdep == "3":
                    dep_linha = "centro"
                _uma_baixa(child_pid, q_need, nome_c, cod_c, dep_linha)
        else:
            _uma_baixa(
                pid,
                qtd,
                str(it.descricao or ""),
                str(it.codigo or ""),
                dep_sess,
            )

    return {
        "ok": len(erros) == 0 and len(aplicados) > 0,
        "aplicados": aplicados,
        "erros": erros,
        "avisos": []
        if aplicados
        else ([{"msg": "Nenhum item com produto de catálogo para baixar."}]),
    }


@login_required(login_url="/admin/login/")
@require_POST
def api_entrada_nota_estoque_agro(request):
    """
    Aplica entrada de estoque só na camada Agro (``AjusteRapidoEstoque``), como PIN / ajuste rápido.
    Opcionalmente salva rascunho no Mongo no mesmo POST.
    """
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)

    linhas = payload.get("linhas")
    if not isinstance(linhas, list) or not linhas:
        return JsonResponse({"ok": False, "erro": "Informe as linhas da nota."}, status=400)

    deposito = str(payload.get("deposito") or "centro").strip().lower()
    if deposito not in ("centro", "vila"):
        deposito = "centro"

    cab = payload.get("cabecalho") if isinstance(payload.get("cabecalho"), dict) else {}
    salvar_rascunho = payload.get("salvar_rascunho") is True
    modo = str(payload.get("modo") or "manual").strip()[:40]
    xml_chave = str(payload.get("xml_chave") or "").strip()[:44] or None
    extra = payload.get("extra") if isinstance(payload.get("extra"), dict) else {}
    extra = {**extra, "estoque_agro_registrado_em": timezone.now().isoformat()}
    rascunho_id_req = str(payload.get("rascunho_id") or "").strip()

    empresa_fat_raw = cab.get("empresa_faturada_id") if isinstance(cab, dict) else None
    if empresa_fat_raw in (None, "") and isinstance(payload, dict):
        empresa_fat_raw = payload.get("empresa_faturada_id")
    try:
        empresa_fat_id = int(str(empresa_fat_raw).strip()) if empresa_fat_raw not in (None, "", False) else None
    except (TypeError, ValueError):
        empresa_fat_id = None

    usuario = ""
    if request.user.is_authenticated:
        usuario = (
            getattr(request.user, "email", None) or request.user.get_username() or str(request.user.pk)
        )[:120]

    client, db = obter_conexao_mongo()
    if db is None or client is None:
        return JsonResponse({"ok": False, "erro": "Mongo indisponível"}, status=503)

    r_rasc: dict | None = None
    if salvar_rascunho:
        r_rasc = salvar_rascunho_entrada(
            db,
            usuario=usuario,
            modo=modo,
            cabecalho=cab,
            linhas=linhas,
            xml_chave=xml_chave,
            extra=extra,
        )
        if not r_rasc.get("ok"):
            return JsonResponse({**r_rasc, "estoque": None}, status=400)

    resultado = aplicar_entrada_nota_estoque_agro(
        db=db,
        client_m=client,
        linhas=linhas,
        deposito=deposito,
        usuario_label=usuario,
        cabecalho=cab,
        usuario_django=request.user if request.user.is_authenticated else None,
        empresa_faturada_id=empresa_fat_id,
    )
    if resultado.get("aplicados"):
        _invalidar_caches_apos_ajuste_pin()

    ok = bool(resultado.get("ok"))
    st = 200 if ok else (207 if resultado.get("aplicados") else 400)
    out = {
        "ok": ok,
        "estoque": resultado,
        "rascunho": r_rasc,
    }
    if ok and db is not None:
        rid_marcar = rascunho_id_req or (
            str(r_rasc.get("id") or "").strip() if isinstance(r_rasc, dict) and r_rasc.get("ok") else ""
        )
        if rid_marcar:
            mr = marcar_rascunho_estoque_aplicado(
                db,
                rid_marcar,
                usuario=usuario,
                patch_extra={"estoque_agro_registrado_em": extra.get("estoque_agro_registrado_em")},
            )
            if not mr.get("ok"):
                out["aviso_status_rascunho"] = mr.get("erro")
    if not resultado.get("aplicados"):
        err = None
        if resultado.get("erros"):
            err = resultado["erros"][0].get("erro")
        elif resultado.get("avisos"):
            err = resultado["avisos"][0].get("msg")
        if err:
            out["erro"] = err
    return JsonResponse(out, status=st)


def _entrada_nota_fornecedor_chave_nome(nome) -> str:
    """Uma entrada na lista por nome exibido (ignora origem): prioridade na ordem mongo → titulo → agro."""
    return " ".join(str(nome or "").strip().lower().split())


@login_required(login_url="/admin/login/")
@require_GET
def api_entrada_nota_fornecedores(request):
    """Fornecedores: Mongo (DtoPessoa) + nomes já usados em títulos + ClienteAgro local.

    Mesmo nome em mais de uma origem vira **uma** linha (mantém a primeira: cadastro Mongo,
    senão título, senão Agro local).
    """
    client, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"itens": [], "erro": "Mongo indisponível"}, status=503)
    q = (request.GET.get("q") or "").strip()
    inicial = request.GET.get("inicial") in ("1", "true", "yes")
    try:
        lim = min(int(request.GET.get("limit") or 50), 100)
    except ValueError:
        lim = 50
    col = getattr(client, "col_c", None) or "DtoPessoa"
    mongo_rows = buscar_fornecedores_entrada_nfe(
        db,
        col,
        q or None,
        inicial=bool(inicial and not q),
        limit=lim,
    )
    extras: list[dict[str, str]] = []
    if q:
        for s in lancamentos_sugestoes_campo(db, "cliente", q=q, limit=25):
            nm = (s.get("nome") or "").strip()
            if not nm:
                continue
            extras.append(
                {
                    "id": str(s.get("id") or "").strip(),
                    "nome": nm[:300],
                    "documento": "",
                    "origem": "titulo",
                }
            )
    agro_rows: list[dict[str, str]] = []
    if q:
        try:
            for c in ClienteAgro.objects.filter(ativo=True, nome__icontains=q).order_by("nome")[:20]:
                agro_rows.append(
                    {
                        "id": f"local:{c.pk}",
                        "nome": (c.nome or "")[:300],
                        "documento": (c.cpf or "").strip()[:20],
                        "origem": "agro",
                    }
                )
        except Exception:
            pass
    seen_nome: set[str] = set()
    merged: list[dict[str, str]] = []
    for row in mongo_rows + extras + agro_rows:
        nk = _entrada_nota_fornecedor_chave_nome(row.get("nome"))
        if not nk:
            continue
        if nk in seen_nome:
            continue
        seen_nome.add(nk)
        merged.append(row)
    return JsonResponse({"itens": merged[:lim]})


@login_required(login_url="/admin/login/")
@require_POST
def api_entrada_nota_financeiro(request):
    """
    Salva rascunho da NF-e e gera lançamento(amentos) a pagar no Mongo (mesmo fluxo do manual).
    Plano de contas: ``financeiro.plano_padrao`` ou, se vazio, ``cabecalho.plano_conta`` (nota inteira).
    ``modo_lancamento`` ``por_item`` ainda é aceito na API; a tela manual envia só ``unico``.
    Modo ``unico``: opcional ``num_parcelas`` (1–60) e ``parcelas_intervalo_dias`` (1–366);
    ``data_vencimento`` é o 1º vencimento; demais parcelas somam o intervalo em dias.
    Opcional ``parcelas_manual``: lista ``[{ "valor", "data_vencimento", "data_competencia"? }]``
    com soma igual ao total da nota (tol. 1 centavo); quando enviada, substitui o cálculo automático.
    Opcional ``quitar_ao_salvar`` (bool): grava os títulos a pagar já quitados no Mongo (data de
    pagamento = vencimento de cada parcela) e, se ``VENDA_ERP_API_FINANCEIRO_BAIXA_PATH`` estiver
    configurado, tenta a baixa no ERP após o envio dos lançamentos novos.
    """
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)

    modo = str(payload.get("modo") or "manual").strip()[:40]
    cab = payload.get("cabecalho") if isinstance(payload.get("cabecalho"), dict) else {}
    linhas = payload.get("linhas")
    if not isinstance(linhas, list) or not linhas:
        return JsonResponse({"ok": False, "erro": "Inclua ao menos uma linha na nota."}, status=400)
    fin = payload.get("financeiro")
    if not isinstance(fin, dict):
        return JsonResponse({"ok": False, "erro": "Informe o bloco financeiro."}, status=400)

    xml_chave = str(payload.get("xml_chave") or "").strip()[:44] or None
    extra = payload.get("extra") if isinstance(payload.get("extra"), dict) else {}
    extra["financeiro_solicitado"] = True

    usuario = ""
    if request.user.is_authenticated:
        usuario = (
            getattr(request.user, "email", None) or request.user.get_username() or str(request.user.pk)
        )[:120]

    _, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"ok": False, "erro": "Mongo indisponível"}, status=503)

    r_rasc = salvar_rascunho_entrada(
        db,
        usuario=usuario,
        modo=modo,
        cabecalho=cab,
        linhas=linhas,
        xml_chave=xml_chave,
        extra=extra,
    )
    if not r_rasc.get("ok"):
        return JsonResponse(r_rasc, status=400)

    def _d_fin(key: str):
        s = str(fin.get(key) or "").strip()[:10]
        if not s:
            return None
        return date.fromisoformat(s)

    dc = _d_fin("data_competencia")
    dv = _d_fin("data_vencimento")
    if dc is None or dv is None:
        return JsonResponse(
            {"ok": False, "erro": "Informe data de competência e vencimento no financeiro.", "rascunho": r_rasc},
            status=400,
        )

    empresa_nome = str(fin.get("empresa_nome") or "").strip()
    pessoa_nome = str(cab.get("emit_nome") or fin.get("pessoa_nome") or "").strip()
    if not empresa_nome or not pessoa_nome:
        return JsonResponse(
            {
                "ok": False,
                "erro": "Preencha empresa (financeiro) e fornecedor na nota.",
                "rascunho": r_rasc,
            },
            status=400,
        )

    banco_nome = str(fin.get("banco_nome") or "").strip()
    forma_nome = str(fin.get("forma_nome") or "").strip()
    if not banco_nome or not forma_nome:
        return JsonResponse(
            {"ok": False, "erro": "Informe forma de pagamento e banco/conta (financeiro).", "rascunho": r_rasc},
            status=400,
        )

    quitar_ao_salvar = bool(fin.get("quitar_ao_salvar") or fin.get("quitar_na_entrada"))

    modo_lanc = str(fin.get("modo_lancamento") or "unico").strip().lower()
    plano_pad = str(fin.get("plano_padrao") or cab.get("plano_conta") or "").strip()
    plano_pad_id = str(fin.get("plano_padrao_id") or cab.get("plano_conta_id") or "").strip() or None

    pessoa_id_raw = str(cab.get("emit_fornecedor_id") or fin.get("pessoa_id") or "").strip() or None
    if pessoa_id_raw and pessoa_id_raw.startswith("local:"):
        pessoa_id_raw = None

    linhas_fin: list[dict] = []
    if modo_lanc == "por_item":
        for ln in linhas:
            if not isinstance(ln, dict):
                continue
            try:
                qtd = float(str(ln.get("q_com", "")).replace(",", ".").strip() or 0)
                vu = float(str(ln.get("v_un_com", "")).replace(",", ".").strip() or 0)
            except (TypeError, ValueError):
                continue
            val = round(qtd * vu, 2)
            if val <= 0:
                continue
            desc = str(ln.get("x_prod") or "Item NF").strip()[:500]
            pn = str(ln.get("plano_conta") or "").strip() or plano_pad
            pid = ln.get("plano_conta_id") or plano_pad_id
            if not pn:
                return JsonResponse(
                    {
                        "ok": False,
                        "erro": "Modo por item: defina plano em cada linha ou um plano padrão.",
                        "rascunho": r_rasc,
                    },
                    status=400,
                )
            linhas_fin.append(
                {
                    "valor": val,
                    "descricao": desc,
                    "plano_conta": pn,
                    "plano_conta_id": pid,
                    "observacao": f"Entrada NF-e {cab.get('numero') or ''} item",
                }
            )
        if not linhas_fin:
            return JsonResponse(
                {"ok": False, "erro": "Nenhum item com valor > 0 para lançar.", "rascunho": r_rasc},
                status=400,
            )
    else:
        total = Decimal("0")
        for ln in linhas:
            if not isinstance(ln, dict):
                continue
            try:
                qtd = Decimal(str(ln.get("q_com", "")).replace(",", ".").strip() or "0")
                vu = Decimal(str(ln.get("v_un_com", "")).replace(",", ".").strip() or "0")
            except Exception:
                continue
            total += qtd * vu
        tot_dec = total.quantize(Decimal("0.01"))
        tot_f = float(tot_dec)
        if tot_f <= 0:
            return JsonResponse(
                {"ok": False, "erro": "Valor total da nota é zero.", "rascunho": r_rasc},
                status=400,
            )
        if not plano_pad:
            return JsonResponse(
                {
                    "ok": False,
                    "erro": "Informe o plano de contas da nota (tela NF-e) ou plano_padrao no bloco financeiro.",
                    "rascunho": r_rasc,
                },
                status=400,
            )
        try:
            nparc = int(fin.get("num_parcelas") or fin.get("numero_parcelas") or 1)
        except (TypeError, ValueError):
            nparc = 1
        nparc = max(1, min(60, nparc))
        try:
            int_dias = int(fin.get("parcelas_intervalo_dias") or fin.get("intervalo_dias") or 30)
        except (TypeError, ValueError):
            int_dias = 30
        int_dias = max(1, min(366, int_dias))

        nf_num = str(cab.get("numero") or "").strip()
        base_desc = (f"NF {nf_num} — {pessoa_nome}" if nf_num else pessoa_nome)[:500]
        base_obs = (
            f"Entrada NF-e Agro · chave {cab.get('chave') or '—'} · {cab.get('data_entrada') or ''}"
        )[:500]

        pm_raw = fin.get("parcelas_manual")
        if isinstance(pm_raw, list) and len(pm_raw) > 0:
            linhas_fin = []
            sum_man = Decimal("0")
            n_pm = len(pm_raw)
            if n_pm > 60:
                return JsonResponse(
                    {"ok": False, "erro": "No máximo 60 parcelas.", "rascunho": r_rasc},
                    status=400,
                )
            for i, row in enumerate(pm_raw):
                if not isinstance(row, dict):
                    return JsonResponse(
                        {"ok": False, "erro": f"Parcela {i + 1}: formato inválido.", "rascunho": r_rasc},
                        status=400,
                    )
                try:
                    v_dec = Decimal(str(row.get("valor", "")).replace(",", ".").strip() or "0").quantize(
                        Decimal("0.01")
                    )
                except Exception:
                    return JsonResponse(
                        {"ok": False, "erro": f"Parcela {i + 1}: valor inválido.", "rascunho": r_rasc},
                        status=400,
                    )
                if v_dec <= 0:
                    return JsonResponse(
                        {"ok": False, "erro": f"Parcela {i + 1}: valor deve ser > 0.", "rascunho": r_rasc},
                        status=400,
                    )
                sum_man += v_dec
                vs = str(row.get("data_vencimento") or "").strip()[:10]
                if not vs:
                    return JsonResponse(
                        {
                            "ok": False,
                            "erro": f"Parcela {i + 1}: informe data_vencimento (AAAA-MM-DD).",
                            "rascunho": r_rasc,
                        },
                        status=400,
                    )
                try:
                    date.fromisoformat(vs)
                except ValueError:
                    return JsonResponse(
                        {
                            "ok": False,
                            "erro": f"Parcela {i + 1}: data_vencimento inválida.",
                            "rascunho": r_rasc,
                        },
                        status=400,
                    )
                cs = str(row.get("data_competencia") or "").strip()[:10]
                if cs:
                    try:
                        date.fromisoformat(cs)
                    except ValueError:
                        return JsonResponse(
                            {
                                "ok": False,
                                "erro": f"Parcela {i + 1}: data_competencia inválida.",
                                "rascunho": r_rasc,
                            },
                            status=400,
                        )
                else:
                    cs = dc.isoformat()
                linhas_fin.append(
                    {
                        "valor": float(v_dec),
                        "descricao": f"{base_desc} (parcela {i + 1}/{n_pm})"[:500],
                        "plano_conta": plano_pad,
                        "plano_conta_id": plano_pad_id,
                        "observacao": base_obs,
                        "data_vencimento": vs,
                        "data_competencia": cs,
                    }
                )
            if abs(sum_man - tot_dec) > Decimal("0.02"):
                return JsonResponse(
                    {
                        "ok": False,
                        "erro": (
                            f"Soma das parcelas ({sum_man}) difere do total da nota ({tot_dec}). "
                            "Ajuste os valores na prévia."
                        ),
                        "rascunho": r_rasc,
                    },
                    status=400,
                )
        elif nparc <= 1:
            linhas_fin = [
                {
                    "valor": tot_f,
                    "descricao": base_desc,
                    "plano_conta": plano_pad,
                    "plano_conta_id": plano_pad_id,
                    "observacao": base_obs,
                }
            ]
        else:
            parcelas_vals = split_decimal_em_parcelas(tot_dec, nparc)
            linhas_fin = []
            for i in range(nparc):
                v_i = float(parcelas_vals[i])
                venc_i = dv + timedelta(days=i * int_dias)
                linhas_fin.append(
                    {
                        "valor": v_i,
                        "descricao": f"{base_desc} (parcela {i + 1}/{nparc})"[:500],
                        "plano_conta": plano_pad,
                        "plano_conta_id": plano_pad_id,
                        "observacao": base_obs,
                        "data_vencimento": venc_i.isoformat(),
                        "data_competencia": dc.isoformat(),
                    }
                )

    resultado = inserir_lancamentos_manual_lote(
        db,
        despesa=True,
        empresa_nome=empresa_nome,
        empresa_id=str(fin.get("empresa_id") or "").strip() or None,
        pessoa_nome=pessoa_nome,
        pessoa_id=pessoa_id_raw,
        data_competencia=dc,
        data_vencimento=dv,
        banco_nome=banco_nome,
        banco_id=str(fin.get("banco_id") or "").strip() or None,
        forma_nome=forma_nome,
        forma_id=str(fin.get("forma_id") or "").strip() or None,
        grupo_nome=str(fin.get("grupo_nome") or "").strip() or None,
        grupo_id=str(fin.get("grupo_id") or "").strip() or None,
        usuario_label=usuario,
        linhas=linhas_fin,
        marcar_quitado_pagar=quitar_ao_salvar,
    )

    ok = bool(resultado.get("ok"))
    ids = resultado.get("ids") or []
    erros = resultado.get("erros") or []
    aviso_api_erp = None
    erp_lanc_ok = None
    erp_baixa_ok = None
    path_lanc = (
        (config("VENDA_ERP_API_FINANCEIRO_LANCAMENTO_PATH", default="") or "")
        or getattr(settings, "VENDA_ERP_API_FINANCEIRO_LANCAMENTO_PATH", "")
        or ""
    ).strip()
    if path_lanc and ids:
        try:
            cli = VendaERPAPIClient()
            body_erp = montar_payload_erp_lancamentos_novos(db, ids, str(resultado.get("lote") or ""), True)
            ok_erp, api_msg = cli.financeiro_tentar_lancamentos_api(body_erp)
            erp_lanc_ok = bool(ok_erp)
            if not ok_erp:
                if isinstance(api_msg, dict):
                    try:
                        aviso_api_erp = json.dumps(api_msg, ensure_ascii=False)[:800]
                    except Exception:
                        aviso_api_erp = str(api_msg)[:800]
                else:
                    aviso_api_erp = str(api_msg)[:800]
        except Exception as exc:
            erp_lanc_ok = False
            aviso_api_erp = str(exc)[:800]

    path_baixa = (
        (config("VENDA_ERP_API_FINANCEIRO_BAIXA_PATH", default="") or "")
        or getattr(settings, "VENDA_ERP_API_FINANCEIRO_BAIXA_PATH", "")
        or ""
    ).strip()
    if quitar_ao_salvar and ids and path_baixa:
        try:
            cli_b = VendaERPAPIClient()
            dmov_fin = dv
            for ln in linhas_fin:
                if not isinstance(ln, dict):
                    continue
                dvs = str(ln.get("data_vencimento") or "").strip()[:10]
                if dvs:
                    try:
                        dd = date.fromisoformat(dvs)
                        if dd < dmov_fin:
                            dmov_fin = dd
                    except ValueError:
                        pass
            payload_baixa = {
                "tipo": "pagar",
                "forma_pagamento": forma_nome,
                "forma_pagamento_id": str(fin.get("forma_id") or "").strip() or None,
                "banco": banco_nome,
                "banco_id": str(fin.get("banco_id") or "").strip() or None,
                "data_movimento": dmov_fin.isoformat(),
            }
            body_b = montar_payload_erp_baixa(
                db, ids, True, payload_baixa, extras={"operacao_baixa": "total"}
            )
            ok_b, api_msg_b = cli_b.financeiro_tentar_baixa_api(body_b)
            erp_baixa_ok = bool(ok_b)
            if not ok_b:
                suf = str(api_msg_b)[:800]
                aviso_api_erp = (aviso_api_erp + " " if aviso_api_erp else "") + ("Baixa ERP: " + suf)
        except Exception as exc:
            erp_baixa_ok = False
            suf = str(exc)[:800]
            aviso_api_erp = (aviso_api_erp + " " if aviso_api_erp else "") + ("Baixa ERP: " + suf)

    out = {
        "ok": ok and not erros,
        "rascunho": r_rasc,
        "financeiro": {
            "ok": ok,
            "lote": resultado.get("lote"),
            "ids": ids,
            "erros": erros,
            "quitar_ao_salvar": quitar_ao_salvar,
        },
    }
    if erp_lanc_ok is not None:
        out["erp_lancamento_ok"] = erp_lanc_ok
    if erp_baixa_ok is not None:
        out["erp_baixa_ok"] = erp_baixa_ok
    if aviso_api_erp:
        out["aviso_api"] = aviso_api_erp
    if r_rasc.get("ok") and ids and db is not None:
        rid_fin = str(r_rasc.get("id") or "").strip()
        if rid_fin:
            mf = marcar_rascunho_financeiro_lancado(db, rid_fin, ids=ids, usuario=usuario)
            if not mf.get("ok"):
                out["aviso_financeiro_rascunho"] = mf.get("erro")
    st = 200 if ok and not erros else (207 if ids else 400)
    return JsonResponse(out, status=st)


@login_required(login_url="/admin/login/")
@require_POST
def api_entrada_nota_dist_dfe(request):
    from produtos.sefaz_dfe_client import distribuicao_dfe_configurada, nfe_distribuicao_dfe_interesse

    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        payload = {}
    cnpj_cfg = re.sub(r"\D", "", config("NFE_DIST_DFE_CNPJ", default="") or "")[:14]
    ult_pedido = payload.get("ult_nsu")
    client, db = obter_conexao_mongo()
    if ult_pedido is not None and str(ult_pedido).strip() != "":
        ult = re.sub(r"\D", "", str(ult_pedido))[:15] or "0"
    elif db is not None and len(cnpj_cfg) == 14:
        ult = obter_ult_nsu(db, cnpj_cfg)
    else:
        ult = "0"

    if not distribuicao_dfe_configurada():
        return JsonResponse(
            {
                "ok": False,
                "erro": "Distribuição DF-e não configurada. Defina no .env: NFE_DIST_DFE_CERT_PATH, "
                "NFE_DIST_DFE_CERT_PASSWORD, NFE_DIST_DFE_CNPJ, NFE_DIST_DFE_UF e opcionalmente "
                "NFE_DIST_DFE_TP_AMB (1=produção, 2=homologação). Instale: pip install cryptography lxml signxml",
                "ult_nsu": ult,
            },
            status=400,
        )

    res = nfe_distribuicao_dfe_interesse(ult)
    previews: list[dict] = []
    for xml_txt in res.get("notas_xml") or []:
        p = parse_nfe_xml_bytes(xml_txt.encode("utf-8"))
        if p.get("ok"):
            if db is not None and client is not None:
                p["itens"] = casar_produtos_mongo(db, client.col_p, p.get("itens") or [])
            previews.append(
                {
                    "chave": p.get("chave"),
                    "numero": p.get("numero"),
                    "emit_nome": p.get("emit_nome"),
                    "valor_total": p.get("valor_total"),
                    "n_itens": len(p.get("itens") or []),
                    "nota": p,
                }
            )

    if db is not None and len(cnpj_cfg) == 14 and res.get("ult_nsu"):
        gravar_ult_nsu(db, cnpj_cfg, str(res["ult_nsu"]))

    res["previews"] = previews
    if not res.get("ok") and res.get("erro"):
        return JsonResponse(res, status=502)
    return JsonResponse(res)


@login_required(login_url="/admin/login/")
@require_GET
def api_lancamentos_contas_pagar(request):
    """Compatibilidade: sempre contas a pagar."""
    return _api_lancamentos_lista_core(request, True)


@login_required(login_url="/admin/login/")
@require_GET
def api_lancamentos_export_csv(request):
    """Exporta CSV com os mesmos filtros da lista (até 5000 linhas)."""
    _, db = obter_conexao_mongo()
    if db is None:
        return HttpResponse("Mongo indisponível", status=503, content_type="text/plain; charset=utf-8")

    tipo = (request.GET.get("tipo") or "pagar").strip().lower()
    despesa = tipo != "receber"
    status = (request.GET.get("status") or "abertos").strip().lower()
    if status not in ("abertos", "quitados", "todos"):
        status = "abertos"
    v_de = _lancamentos_parse_date_param(request.GET.get("venc_de"))
    v_ate = _lancamentos_parse_date_param(request.GET.get("venc_ate"))
    c_de = _lancamentos_parse_date_param(request.GET.get("comp_de"))
    c_ate = _lancamentos_parse_date_param(request.GET.get("comp_ate"))
    p_de = _lancamentos_parse_date_param(request.GET.get("pag_de"))
    p_ate = _lancamentos_parse_date_param(request.GET.get("pag_ate"))
    texto = (request.GET.get("q") or "").strip() or None
    ordenacao = (request.GET.get("ordenacao") or "vencimento_asc").strip().lower()
    if ordenacao not in LANCAMENTOS_ORDENACOES_VALIDAS:
        ordenacao = "vencimento_asc"

    excl_planos = _lancamentos_excluir_planos_from_request(request)
    query = lancamentos_montar_query_mongo(
        despesa=despesa,
        status=status,
        vencimento_de=v_de,
        vencimento_ate=v_ate,
        competencia_de=c_de,
        competencia_ate=c_ate,
        pagamento_de=p_de,
        pagamento_ate=p_ate,
        texto=texto,
        excluir_planos_nomes=excl_planos or None,
    )
    linhas, _, _ = lancamentos_buscar_pagina(
        db,
        query,
        despesa,
        page=1,
        page_size=5000,
        ordenacao=ordenacao,
        limite_max=5000,
    )

    buf = StringIO()
    # Excel (pt-BR) costuma interpretar melhor CSV com ";".
    w = csv.writer(buf, delimiter=";")
    label_mov = "Pago" if despesa else "Recebido / mov."
    label_saldo = "A pagar" if despesa else "A receber"
    w.writerow(["Relatório de lançamentos financeiros"])
    w.writerow(["Tipo", "Contas a pagar" if despesa else "Contas a receber"])
    w.writerow(["Status", status])
    w.writerow(["Vencimento (de / até)", (v_de.isoformat() if v_de else ""), (v_ate.isoformat() if v_ate else "")])
    w.writerow(["Competência (de / até)", (c_de.isoformat() if c_de else ""), (c_ate.isoformat() if c_ate else "")])
    w.writerow(["Pagamento / quitação (de / até)", (p_de.isoformat() if p_de else ""), (p_ate.isoformat() if p_ate else "")])
    w.writerow(["Busca", texto or ""])
    w.writerow([])
    w.writerow(
        [
            "Vencimento",
            "Cliente / favorecido",
            "Descrição",
            "Doc.",
            "Forma pagamento",
            "Banco",
            "Plano conta",
            "Grupo",
            "Valor bruto",
            label_mov,
            label_saldo,
            "Situação",
            "Data quitação",
        ]
    )
    soma_bruto = 0.0
    soma_mov = 0.0
    soma_saldo = 0.0
    por_plano: dict[str, dict[str, float]] = {}
    for row in linhas:
        vb = float(row.get("valor_bruto") or 0)
        vm = float(row.get("valor_movimentado") or 0)
        rs = float(row.get("restante") or 0)
        soma_bruto += vb
        soma_mov += vm
        soma_saldo += rs
        plano = str(row.get("plano_conta") or "(sem plano)").strip() or "(sem plano)"
        ag = por_plano.setdefault(plano, {"qtd": 0.0, "bruto": 0.0, "mov": 0.0, "saldo": 0.0})
        ag["qtd"] += 1
        ag["bruto"] += vb
        ag["mov"] += vm
        ag["saldo"] += rs
        w.writerow(
            [
                (row.get("data_vencimento") or "")[:19],
                row.get("cliente") or "",
                row.get("descricao") or "",
                row.get("numero_documento") or "",
                row.get("forma_pagamento") or "",
                row.get("banco") or "",
                row.get("plano_conta") or "",
                row.get("grupo") or "",
                row.get("valor_bruto"),
                row.get("valor_movimentado"),
                row.get("restante"),
                "Quitado" if row.get("pago") else "Aberto",
                (row.get("data_pagamento") or "")[:19],
            ]
        )

    w.writerow([])
    w.writerow(["Resumo geral"])
    w.writerow(["Quantidade de títulos", len(linhas)])
    w.writerow(["Total bruto", round(soma_bruto, 2)])
    w.writerow([label_mov, round(soma_mov, 2)])
    w.writerow([label_saldo, round(soma_saldo, 2)])

    w.writerow([])
    w.writerow(["Subtotais por plano de conta"])
    w.writerow(["Plano de conta", "Títulos", "Bruto", label_mov, label_saldo])
    for plano in sorted(por_plano.keys(), key=lambda x: x.lower()):
        ag = por_plano[plano]
        w.writerow(
            [
                plano,
                int(ag["qtd"]),
                round(ag["bruto"], 2),
                round(ag["mov"], 2),
                round(ag["saldo"], 2),
            ]
        )

    nome = f"lancamentos_{'pagar' if despesa else 'receber'}_{timezone.localdate().isoformat()}.csv"
    resp = HttpResponse("\ufeff" + buf.getvalue(), content_type="text/csv; charset=utf-8")
    resp["Content-Disposition"] = f'attachment; filename="{nome}"'
    return resp


def _lancamentos_financeiro_dados_export(request):
    """
    Mesmos filtros da lista / CSV / Excel / PDF — até 5000 títulos deduplicados.
    Retorna (resposta_erro, None) ou (None, dict com linhas, despesa, v_de, v_ate, status).
    """
    _, db = obter_conexao_mongo()
    if db is None:
        return HttpResponse("Mongo indisponível", status=503, content_type="text/plain; charset=utf-8"), None

    tipo = (request.GET.get("tipo") or "pagar").strip().lower()
    despesa = tipo != "receber"
    status = (request.GET.get("status") or "abertos").strip().lower()
    if status not in ("abertos", "quitados", "todos"):
        status = "abertos"
    v_de = _lancamentos_parse_date_param(request.GET.get("venc_de"))
    v_ate = _lancamentos_parse_date_param(request.GET.get("venc_ate"))
    c_de = _lancamentos_parse_date_param(request.GET.get("comp_de"))
    c_ate = _lancamentos_parse_date_param(request.GET.get("comp_ate"))
    p_de = _lancamentos_parse_date_param(request.GET.get("pag_de"))
    p_ate = _lancamentos_parse_date_param(request.GET.get("pag_ate"))
    texto = (request.GET.get("q") or "").strip() or None
    ordenacao = (request.GET.get("ordenacao") or "vencimento_asc").strip().lower()
    if ordenacao not in LANCAMENTOS_ORDENACOES_VALIDAS:
        ordenacao = "vencimento_asc"

    excl_planos = _lancamentos_excluir_planos_from_request(request)
    query = lancamentos_montar_query_mongo(
        despesa=despesa,
        status=status,
        vencimento_de=v_de,
        vencimento_ate=v_ate,
        competencia_de=c_de,
        competencia_ate=c_ate,
        pagamento_de=p_de,
        pagamento_ate=p_ate,
        texto=texto,
        excluir_planos_nomes=excl_planos or None,
    )
    linhas, _, _ = lancamentos_buscar_pagina(
        db,
        query,
        despesa,
        page=1,
        page_size=5000,
        ordenacao=ordenacao,
        limite_max=5000,
    )
    return None, {
        "linhas": linhas,
        "despesa": despesa,
        "v_de": v_de,
        "v_ate": v_ate,
        "c_de": c_de,
        "c_ate": c_ate,
        "status": status,
    }


@login_required(login_url="/admin/login/")
@require_GET
def api_lancamentos_export_financeiro_xlsx(request):
    """
    Excel (.xlsx) no layout do relatório financeiro de referência: período, colunas resumidas
    (vencimento dd/mmm, favorecido, plano, valores estilo R$, pago, qual conta) e bloco inferior
    para preenchimento manual (entrada / dívida / beneficiário / observações).
    """
    err, data = _lancamentos_financeiro_dados_export(request)
    if err:
        return err
    blob = montar_planilha_financeiro_padrao(
        data["linhas"],
        despesa=data["despesa"],
        v_de=data["v_de"],
        v_ate=data["v_ate"],
    )
    ref = data["v_ate"] or data["v_de"] or timezone.localdate()
    nome = f"Financeiro_{ref.strftime('%d.%m.%Y')}.xlsx"
    resp = HttpResponse(
        blob,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    resp["Content-Disposition"] = f'attachment; filename="{nome}"'
    return resp


@login_required(login_url="/admin/login/")
@require_GET
def api_lancamentos_export_financeiro_pdf(request):
    """
    PDF no padrão do relatório financeiro de referência — tipografia e cores discretas,
    tabela com cabeçalho destacado, linhas zebradas e bloco manual inferior.
    """
    err, data = _lancamentos_financeiro_dados_export(request)
    if err:
        return err
    blob = montar_pdf_financeiro_padrao(
        data["linhas"],
        despesa=data["despesa"],
        v_de=data["v_de"],
        v_ate=data["v_ate"],
        status=data["status"],
    )
    ref = data["v_ate"] or data["v_de"] or timezone.localdate()
    nome = f"Financeiro_{ref.strftime('%d.%m.%Y')}.pdf"
    resp = HttpResponse(blob, content_type="application/pdf")
    resp["Content-Disposition"] = f'attachment; filename="{nome}"'
    return resp


def _mesclar_opcoes_baixa_com_extras(
    base: list[dict],
    extras_qs,
) -> tuple[list[dict], list[dict]]:
    """
    Acrescenta opções cadastradas pelo usuário sem duplicar id (quando preenchido) ou nome (sem id).
    Retorna (lista_mesclada, detalhe_extras_para_ui).
    """
    keys: set[tuple[str, str]] = set()
    for x in base:
        oid = (x.get("id") or "").strip()
        nome = (x.get("nome") or "").strip().lower()
        if not (x.get("nome") or "").strip():
            continue
        keys.add(("i", oid) if oid else ("n", nome))
    out = list(base)
    detalhe: list[dict] = []
    for e in extras_qs:
        i = (e.id_erp or "").strip()
        n = (e.nome or "").strip()
        if not n:
            continue
        k = ("i", i) if i else ("n", n.lower())
        if k in keys:
            continue
        keys.add(k)
        out.append({"id": i, "nome": n, "origem": "manual"})
        detalhe.append(
            {"pk": e.pk, "tipo": e.tipo, "nome": n, "id_erp": i}
        )
    return out, detalhe


@login_required(login_url="/admin/login/")
@require_GET
def api_lancamentos_opcoes_baixa(request):
    """Formas de pagamento e bancos: Mongo (modo ERP ou histórico) + opções extras do usuário.

    Query ``apenas_cadastro_erp=1`` (ou ``true``/``sim``): força modo ERP, sem lista pessoal
    (útil na tela de lançamentos quando só se quer o cadastro visto no ERP via IDs nos títulos).
    """
    raw_apenas = (request.GET.get("apenas_cadastro_erp") or "").strip().lower()
    apenas_cadastro_erp = raw_apenas in ("1", "true", "yes", "sim", "on")
    modo = (request.GET.get("modo") or "erp").strip().lower()
    if apenas_cadastro_erp:
        modo = "erp"
    elif modo not in ("erp", "historico"):
        modo = "erp"
    _, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse(
            {"erro": "Mongo indisponível", "formas": [], "bancos": [], "modo": modo, "extras": []},
            status=503,
        )
    formas, bancos = listar_formas_e_bancos_distintos(
        db, modo=modo, fonte_cadastro_mestre=apenas_cadastro_erp
    )
    if apenas_cadastro_erp:
        det_f: list[dict] = []
        det_b: list[dict] = []
    else:
        extras_q = OpcaoBaixaFinanceiroExtra.objects.filter(usuario=request.user)
        formas, det_f = _mesclar_opcoes_baixa_com_extras(
            formas, extras_q.filter(tipo=OpcaoBaixaFinanceiroExtra.Tipo.FORMA)
        )
        bancos, det_b = _mesclar_opcoes_baixa_com_extras(
            bancos, extras_q.filter(tipo=OpcaoBaixaFinanceiroExtra.Tipo.BANCO)
        )
    formas.sort(key=lambda x: (x.get("nome") or "").lower())
    bancos.sort(key=lambda x: (x.get("nome") or "").lower())
    return JsonResponse(
        {
            "formas": formas,
            "bancos": bancos,
            "modo": modo,
            "extras": det_f + det_b,
        }
    )


@login_required(login_url="/admin/login/")
@require_POST
def api_lancamentos_opcoes_baixa_extra_criar(request):
    """Inclui forma ou conta na lista pessoal da baixa."""
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)
    tipo = str(payload.get("tipo") or "").strip().lower()
    if tipo not in ("forma", "banco"):
        return JsonResponse({"ok": False, "erro": 'Informe tipo "forma" ou "banco".'}, status=400)
    nome = str(payload.get("nome") or "").strip()[:300]
    if not nome:
        return JsonResponse({"ok": False, "erro": "Informe o nome exibido na lista."}, status=400)
    id_erp = str(payload.get("id_erp") or "").strip()[:80]
    tipo_choice = (
        OpcaoBaixaFinanceiroExtra.Tipo.FORMA
        if tipo == "forma"
        else OpcaoBaixaFinanceiroExtra.Tipo.BANCO
    )
    try:
        obj = OpcaoBaixaFinanceiroExtra.objects.create(
            usuario=request.user,
            tipo=tipo_choice,
            nome=nome,
            id_erp=id_erp,
        )
    except IntegrityError:
        return JsonResponse(
            {"ok": False, "erro": "Já existe uma opção igual (mesmo tipo e ID ou mesmo nome sem ID)."},
            status=400,
        )
    return JsonResponse(
        {
            "ok": True,
            "item": {
                "pk": obj.pk,
                "tipo": obj.tipo,
                "nome": obj.nome,
                "id_erp": obj.id_erp,
            },
        }
    )


@login_required(login_url="/admin/login/")
@require_POST
def api_lancamentos_opcoes_baixa_extra_excluir(request, pk: int):
    """Remove opção pessoal da lista da baixa."""
    q = OpcaoBaixaFinanceiroExtra.objects.filter(pk=pk, usuario=request.user)
    if not q.exists():
        return JsonResponse({"ok": False, "erro": "Registro não encontrado."}, status=404)
    q.delete()
    return JsonResponse({"ok": True})


@login_required(login_url="/admin/login/")
@require_POST
def api_lancamentos_baixa(request):
    """
    Quitação total no Mongo dos títulos selecionados, com forma e banco escolhidos no ato.
    Opcional: VendaERPAPIClient.financeiro_tentar_baixa_api se configurado.
    """
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)

    ids = payload.get("ids")
    if not isinstance(ids, list) or not ids:
        return JsonResponse({"ok": False, "erro": "Informe ao menos um lançamento (ids)."}, status=400)

    tipo = str(payload.get("tipo") or "pagar").strip().lower()
    despesa = tipo != "receber"
    forma_nome = str(payload.get("forma_pagamento") or "").strip()
    forma_id = payload.get("forma_pagamento_id")
    banco_nome = str(payload.get("banco") or "").strip()
    banco_id = payload.get("banco_id")
    data_str = str(payload.get("data_movimento") or "").strip()[:10]
    try:
        dmov = date.fromisoformat(data_str) if data_str else timezone.localdate()
    except ValueError:
        return JsonResponse({"ok": False, "erro": "Data inválida (use AAAA-MM-DD)."}, status=400)

    valor_juros_dec: Decimal | None = None
    vj_raw = payload.get("valor_juros")
    if vj_raw is not None and str(vj_raw).strip() != "":
        try:
            valor_juros_dec = Decimal(str(vj_raw).replace(",", ".").strip()).quantize(Decimal("0.01"))
        except Exception:
            return JsonResponse({"ok": False, "erro": "Valor de juros inválido."}, status=400)
        if valor_juros_dec <= 0:
            valor_juros_dec = None

    tz = timezone.get_current_timezone()
    data_movimento = timezone.make_aware(datetime.combine(dmov, dtime(12, 0, 0)), tz)

    _, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"ok": False, "erro": "Mongo indisponível"}, status=503)

    usuario = ""
    if request.user.is_authenticated:
        usuario = (getattr(request.user, "email", None) or request.user.get_username() or str(request.user.pk))[:120]

    resultado = baixar_lancamentos_mongo(
        db,
        [str(i) for i in ids],
        despesa=despesa,
        data_movimento=data_movimento,
        forma_nome=forma_nome,
        forma_id=str(forma_id).strip() if forma_id else None,
        banco_nome=banco_nome,
        banco_id=str(banco_id).strip() if banco_id else None,
        usuario_label=usuario,
    )

    path_baixa = (
        (config("VENDA_ERP_API_FINANCEIRO_BAIXA_PATH", default="") or "")
        or getattr(settings, "VENDA_ERP_API_FINANCEIRO_BAIXA_PATH", "")
        or ""
    ).strip()
    if path_baixa and resultado.get("atualizados"):
        try:
            cli = VendaERPAPIClient()
            body_erp = montar_payload_erp_baixa(
                db, resultado["atualizados"], despesa, payload, extras={"operacao_baixa": "total"}
            )
            ok_api, api_msg = cli.financeiro_tentar_baixa_api(body_erp)
            if ok_api:
                resultado["erp_baixa_ok"] = True
            else:
                resultado["aviso_api"] = str(api_msg)[:800]
                resultado["erp_baixa_ok"] = False
        except Exception as exc:
            resultado["aviso_api"] = str(exc)[:800]
            resultado["erp_baixa_ok"] = False

    ok_all = bool(resultado.get("ok"))
    atual = resultado.get("atualizados") or []
    erros = resultado.get("erros") or []
    aviso_juros = None
    juros_id = None
    if despesa and valor_juros_dec and valor_juros_dec > 0 and atual:
        rj = registrar_titulo_juros_apos_baixa_contas_pagar(
            db,
            mongo_id_titulo_referencia=str(atual[0]),
            valor_juros=valor_juros_dec,
            data_movimento=dmov,
            forma_nome=forma_nome,
            forma_id=str(forma_id).strip() if forma_id else None,
            banco_nome=banco_nome,
            banco_id=str(banco_id).strip() if banco_id else None,
            usuario_label=usuario,
        )
        if rj.get("ok"):
            juros_id = rj.get("id")
        else:
            aviso_juros = str(rj.get("erro") or "Não foi possível registrar o título de juros.")[:800]

    if ok_all:
        http_st = 200
    elif atual:
        http_st = 207
    else:
        http_st = 400

    out_j = {
        "ok": ok_all,
        "atualizados": atual,
        "erros": erros,
        "aviso_api": resultado.get("aviso_api"),
    }
    if "erp_baixa_ok" in resultado:
        out_j["erp_baixa_ok"] = resultado["erp_baixa_ok"]
    if juros_id:
        out_j["juros_id"] = juros_id
    if aviso_juros:
        out_j["aviso_juros"] = aviso_juros
    return JsonResponse(out_j, status=http_st)


@login_required(login_url="/admin/login/")
@require_POST
def api_lancamentos_baixa_parcial(request):
    """Uma linha selecionada: várias parcelas (valor + forma + banco) até quitar o saldo."""
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)

    lid = str(payload.get("lancamento_id") or payload.get("id") or "").strip()
    if not lid:
        return JsonResponse({"ok": False, "erro": "Informe o lançamento (lancamento_id)."}, status=400)

    tipo = str(payload.get("tipo") or "pagar").strip().lower()
    despesa = tipo != "receber"
    parcelas = payload.get("parcelas")
    if not isinstance(parcelas, list) or not parcelas:
        return JsonResponse({"ok": False, "erro": "Informe ao menos uma parcela (valor, forma, banco)."}, status=400)

    data_str = str(payload.get("data_movimento") or "").strip()[:10]
    try:
        dmov = date.fromisoformat(data_str) if data_str else timezone.localdate()
    except ValueError:
        return JsonResponse({"ok": False, "erro": "Data inválida (use AAAA-MM-DD)."}, status=400)

    tz = timezone.get_current_timezone()
    data_movimento = timezone.make_aware(datetime.combine(dmov, dtime(12, 0, 0)), tz)

    _, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"ok": False, "erro": "Mongo indisponível"}, status=503)

    usuario = ""
    if request.user.is_authenticated:
        usuario = (getattr(request.user, "email", None) or request.user.get_username() or str(request.user.pk))[:120]

    resultado = baixar_lancamento_parcial_mongo(
        db,
        lid,
        despesa=despesa,
        data_movimento=data_movimento,
        parcelas=parcelas,
        usuario_label=usuario,
    )

    if not resultado.get("ok"):
        return JsonResponse(
            {"ok": False, "erro": resultado.get("erro") or "Falha na baixa parcial."},
            status=400,
        )

    out_j = {
        "ok": True,
        "id": resultado.get("id"),
        "quitado": bool(resultado.get("quitado")),
    }

    path_baixa = (
        (config("VENDA_ERP_API_FINANCEIRO_BAIXA_PATH", default="") or "")
        or getattr(settings, "VENDA_ERP_API_FINANCEIRO_BAIXA_PATH", "")
        or ""
    ).strip()
    # Sincroniza também baixa parcial (antes só quando quitado — o ERP não recebia parcelas).
    if path_baixa and resultado.get("id"):
        try:
            cli = VendaERPAPIClient()
            # Mesmo corpo base da baixa total (``titulos`` + ``ids`` + ``payload``), sem chaves extras no nível raiz.
            body_erp = montar_payload_erp_baixa(db, [str(resultado["id"])], despesa, payload)
            ok_api, api_msg = cli.financeiro_tentar_baixa_api(body_erp)
            out_j["erp_baixa_ok"] = bool(ok_api)
            if not ok_api:
                out_j["aviso_api"] = str(api_msg)[:800]
        except Exception as exc:
            out_j["erp_baixa_ok"] = False
            out_j["aviso_api"] = str(exc)[:800]
    # Resposta do ERP/sync pode regravar o DtoLancamento com DataPagamento = data mínima (.NET).
    if resultado.get("ok") and resultado.get("id") and not resultado.get("quitado"):
        try:
            oid = ObjectId(str(resultado["id"]).strip())
            db[COL_DTO_LANCAMENTO].update_one(
                {"_id": oid},
                {"$set": {"DataPagamento": data_movimento, "LastUpdate": timezone.now()}},
            )
        except Exception:
            logger.exception("api_lancamentos_baixa_parcial: reaplicar DataPagamento após ERP")
    return JsonResponse(out_j, status=200)


@login_required(login_url="/admin/login/")
@require_POST
def api_lancamentos_alterar(request):
    """Edita lançamento em aberto no Mongo (descrição, favorecido, vencimento, plano, valor bruto sem movimento)."""
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)

    lid = str(payload.get("id") or "").strip()
    if not lid:
        return JsonResponse({"ok": False, "erro": "Informe o id do lançamento."}, status=400)

    patch = {k: payload[k] for k in payload if k != "id"}
    if not patch:
        return JsonResponse({"ok": False, "erro": "Nenhum campo para atualizar."}, status=400)

    _, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"ok": False, "erro": "Mongo indisponível"}, status=503)

    usuario = ""
    if request.user.is_authenticated:
        usuario = (getattr(request.user, "email", None) or request.user.get_username() or str(request.user.pk))[:120]

    r = atualizar_lancamento_mongo_agro(db, lid, patch, usuario)
    if not r.get("ok"):
        return JsonResponse({"ok": False, "erro": r.get("erro") or "Falha ao atualizar."}, status=400)
    return JsonResponse({"ok": True, "id": r.get("id")})


@login_required(login_url="/admin/login/")
@require_POST
def api_lancamentos_excluir(request):
    """Remove lançamento no Mongo somente se manual Agro ou sem vínculo ERP, sem pagamento."""
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)

    lid = str(payload.get("id") or "").strip()
    if not lid:
        return JsonResponse({"ok": False, "erro": "Informe o id do lançamento."}, status=400)

    _, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"ok": False, "erro": "Mongo indisponível"}, status=503)

    usuario = ""
    if request.user.is_authenticated:
        usuario = (getattr(request.user, "email", None) or request.user.get_username() or str(request.user.pk))[:120]

    r = excluir_lancamento_mongo_agro(db, lid, usuario)
    if not r.get("ok"):
        return JsonResponse({"ok": False, "erro": r.get("erro") or "Falha ao excluir."}, status=400)
    try:
        from rh.services.importador_vales_caixa import marcar_vales_cancelados_por_lancamento_removido

        marcar_vales_cancelados_por_lancamento_removido(lid)
    except Exception:
        logger.exception("RH: cancelar vales após exclusão de lançamento")
    return JsonResponse({"ok": True})


@login_required(login_url="/admin/login/")
@require_POST
def api_lancamentos_saida_caixa(request):
    """Registra uma despesa rápida (saída de caixa) com plano de conta — grava como lançamento manual de 1 linha."""
    from produtos.saida_caixa_planos import SAIDA_CAIXA_PLANOS

    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)

    try:
        valor = float(str(payload.get("valor", "")).replace(",", ".").strip())
    except (ValueError, TypeError):
        return JsonResponse({"ok": False, "erro": "Valor inválido."}, status=400)
    if valor <= 0:
        return JsonResponse({"ok": False, "erro": "Valor deve ser maior que zero."}, status=400)

    plano_id_req = str(payload.get("plano_id") or "").strip()
    plan_map = {p["id"]: p for p in SAIDA_CAIXA_PLANOS}
    is_outros = False
    if plano_id_req:
        if plano_id_req not in plan_map:
            return JsonResponse({"ok": False, "erro": "Plano de conta inválido."}, status=400)
        entry = plan_map[plano_id_req]
        plano = entry["plano"]
        is_outros = bool(entry.get("outros"))
    else:
        plano = str(payload.get("plano_conta") or payload.get("plano_nome") or "").strip()
        if not plano:
            return JsonResponse({"ok": False, "erro": "Escolha o plano de conta."}, status=400)

    motivo = str(payload.get("motivo") or "").strip()[:200]
    pessoa_id_final = str(payload.get("pessoa_id") or "").strip() or None
    pessoa_nome = ""
    if plano_id_req:
        if is_outros:
            if len(motivo) < 15:
                return JsonResponse(
                    {"ok": False, "erro": "No plano Outros, o motivo é obrigatório (mínimo 15 caracteres)."},
                    status=400,
                )
        caid = payload.get("cliente_agro_id")
        if caid is not None and str(caid).strip() != "":
            try:
                ca_pk = int(caid)
            except (TypeError, ValueError):
                return JsonResponse({"ok": False, "erro": "cliente_agro_id inválido."}, status=400)
            ca = ClienteAgro.objects.filter(pk=ca_pk, ativo=True).first()
            if not ca:
                return JsonResponse(
                    {"ok": False, "erro": "Pessoa base (ClienteAgro) não encontrada ou inativa."},
                    status=400,
                )
            pessoa_nome = (ca.nome or "")[:300]
            pessoa_id_final = ((ca.externo_id or "").strip() or f"local:{ca.pk}")[:120]
        else:
            quem = str(payload.get("quem_leva") or "").strip()
            quem_outro = str(payload.get("quem_leva_outro_nome") or "").strip()[:200]
            if quem.upper() in ("OUTRO", "__OUTRO__") or quem == "__OUTRO__":
                if len(quem_outro) < 2:
                    return JsonResponse({"ok": False, "erro": "Informe o nome completo da pessoa."}, status=400)
                pessoa_nome = quem_outro
            else:
                if not quem:
                    return JsonResponse({"ok": False, "erro": "Selecione quem está levando o dinheiro."}, status=400)
                pessoa_nome = quem
    else:
        if not motivo:
            return JsonResponse({"ok": False, "erro": "Informe o motivo (ex.: troco, compra emergencial)."}, status=400)
        pessoa_nome = str(payload.get("pessoa_nome") or "").strip() or (
            (config("AGRO_FIN_SAIDA_CAIXA_PESSOA", default="") or "").strip() or "Operação / caixa"
        )

    dc = _lancamentos_parse_date_param(payload.get("data_competencia"))
    dv = _lancamentos_parse_date_param(payload.get("data_vencimento"))
    if dc is None:
        dc = timezone.localdate()
    if dv is None:
        dv = dc

    empresa_nome = str(payload.get("empresa_nome") or "").strip() or (
        (config("AGRO_FIN_SAIDA_CAIXA_EMPRESA", default="") or "").strip() or "Loja"
    )
    banco_nome = str(payload.get("banco_nome") or "").strip()
    forma_nome = str(payload.get("forma_nome") or "").strip()
    if not banco_nome or not forma_nome:
        return JsonResponse({"ok": False, "erro": "Informe conta/banco e forma de pagamento."}, status=400)

    _, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"ok": False, "erro": "Mongo indisponível"}, status=503)

    usuario = ""
    if request.user.is_authenticated:
        usuario = (
            getattr(request.user, "email", None) or request.user.get_username() or str(request.user.pk)
        )[:120]

    if plano_id_req:
        partes = []
        if motivo:
            partes.append(motivo)
        partes.append(f"Quem: {pessoa_nome}")
        desc_linha = "Saída caixa — " + " · ".join(partes)
    else:
        desc_linha = f"Saída caixa — {motivo}"

    # Vale (adiantamento): baixa parcial no título único de salário — sem novo DtoLancamento de vale.
    if plano_id_req == "adiant_vale":
        from rh.services.salario_financeiro_mongo import tentar_caixa_adiant_vale_como_baixa_parcial

        alt = tentar_caixa_adiant_vale_como_baixa_parcial(
            db=db,
            data_competencia=dc,
            empresa_nome=empresa_nome,
            pessoa_nome=pessoa_nome,
            pessoa_id=pessoa_id_final,
            valor=valor,
            forma_nome=forma_nome,
            forma_id=str(payload.get("forma_id") or "").strip() or None,
            banco_nome=banco_nome,
            banco_id=str(payload.get("banco_id") or "").strip() or None,
            usuario=request.user,
            observacao_desc=desc_linha,
        )
        if alt is not None:
            st = 200 if alt.get("ok") else 400
            out = {
                "ok": bool(alt.get("ok")),
                "lote": alt.get("lote"),
                "ids": alt.get("ids") or [],
                "erros": alt.get("erros") or [],
            }
            if not alt.get("ok"):
                out["erro"] = alt.get("erro") or "Não foi possível registrar o vale."
            return JsonResponse(out, status=st)
        return JsonResponse(
            {
                "ok": False,
                "erro": (
                    "Vale no caixa: use a pessoa cadastrada no perfil RH (ClienteAgro) e gere o título de salário "
                    "com vencimento no fechamento da competência — o vale entra como pagamento parcial desse título."
                ),
                "ids": [],
                "erros": [],
            },
            status=400,
        )

    linhas = [
        {
            "plano_conta": plano,
            "plano_conta_id": str(payload.get("plano_conta_id") or "").strip() or None,
            "valor": valor,
            "descricao": desc_linha[:500],
            "observacao": str(payload.get("observacao") or "").strip()[:500],
        }
    ]

    resultado = inserir_lancamentos_manual_lote(
        db,
        despesa=True,
        empresa_nome=empresa_nome,
        empresa_id=str(payload.get("empresa_id") or "").strip() or None,
        pessoa_nome=pessoa_nome,
        pessoa_id=pessoa_id_final,
        data_competencia=dc,
        data_vencimento=dv,
        banco_nome=banco_nome,
        banco_id=str(payload.get("banco_id") or "").strip() or None,
        forma_nome=forma_nome,
        forma_id=str(payload.get("forma_id") or "").strip() or None,
        grupo_nome=str(payload.get("grupo_nome") or "").strip() or None,
        grupo_id=str(payload.get("grupo_id") or "").strip() or None,
        usuario_label=usuario,
        linhas=linhas,
    )

    ok = bool(resultado.get("ok"))
    ids = resultado.get("ids") or []
    erros = resultado.get("erros") or []
    aviso_api_erp = None
    erp_lanc_ok = None
    path_lanc = (
        (config("VENDA_ERP_API_FINANCEIRO_LANCAMENTO_PATH", default="") or "")
        or getattr(settings, "VENDA_ERP_API_FINANCEIRO_LANCAMENTO_PATH", "")
        or ""
    ).strip()
    if path_lanc and ids:
        try:
            cli = VendaERPAPIClient()
            body_erp = montar_payload_erp_lancamentos_novos(db, ids, str(resultado.get("lote") or ""), True)
            ok_erp, api_msg = cli.financeiro_tentar_lancamentos_api(body_erp)
            erp_lanc_ok = bool(ok_erp)
            if not ok_erp:
                if isinstance(api_msg, dict):
                    try:
                        aviso_api_erp = json.dumps(api_msg, ensure_ascii=False)[:800]
                    except Exception:
                        aviso_api_erp = str(api_msg)[:800]
                else:
                    aviso_api_erp = str(api_msg)[:800]
        except Exception as exc:
            erp_lanc_ok = False
            aviso_api_erp = str(exc)[:800]
    st = 200 if ok else (207 if ids else 400)
    out = {
        "ok": ok,
        "lote": resultado.get("lote"),
        "ids": ids,
        "erros": erros,
    }
    if erp_lanc_ok is not None:
        out["erp_lancamento_ok"] = erp_lanc_ok
    if aviso_api_erp:
        out["aviso_api"] = aviso_api_erp
    # Vale RH: dispara mesmo com status 207 (ok falso) desde que tenha inserido no Mongo.
    if plano_id_req == "adiant_vale" and ids:
        try:
            from rh.services.importador_vales_caixa import processar_saida_caixa_apos_gravar

            processar_saida_caixa_apos_gravar(
                plano_id=plano_id_req,
                mongo_ids=ids,
                pessoa_nome=pessoa_nome,
                data_competencia=dc,
                empresa_nome=empresa_nome,
                valor=valor,
                usuario=request.user,
            )
        except Exception:
            logger.exception("RH: vale automático pós-saída caixa")
    return JsonResponse(out, status=st)


@login_required(login_url="/admin/login/")
@require_GET
def api_lancamentos_dre_resumo(request):
    """Totais por plano de conta no período (base para DRE simples)."""
    de = _lancamentos_parse_date_param(request.GET.get("de"))
    ate = _lancamentos_parse_date_param(request.GET.get("ate"))
    if de is None or ate is None:
        return JsonResponse({"ok": False, "erro": "Informe de e até (AAAA-MM-DD)."}, status=400)
    if de > ate:
        de, ate = ate, de
    por = (request.GET.get("por") or "competencia").strip().lower()
    if por not in ("competencia", "vencimento", "pagamento"):
        por = "competencia"

    valor = (request.GET.get("valor") or "bruto").strip().lower()
    if valor not in ("bruto", "realizado"):
        valor = "bruto"

    contas = (request.GET.get("contas") or "").strip().lower()
    if not contas:
        contas = getattr(settings, "DRE_RESULTADO_FILTRO", "resultado") or "resultado"
    extra_rx = getattr(settings, "DRE_RESULTADO_EXCLUIR_REGEX_EXTRA", "") or ""
    empresa = (request.GET.get("empresa") or "").strip() or None
    empresa_id = (request.GET.get("empresa_id") or "").strip() or None

    _, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"ok": False, "erro": "Mongo indisponível"}, status=503)

    r = dre_resumo_simples_mongo(
        db,
        data_de=de,
        data_ate=ate,
        por=por,
        valor=valor,
        filtro_contas=contas,
        regex_excluir_extra=extra_rx or None,
        empresa=empresa,
        empresa_id=empresa_id,
    )
    if not r.get("ok"):
        return JsonResponse(r, status=500)
    return JsonResponse(r)


@ensure_csrf_cookie
@login_required(login_url="/admin/login/")
def lancamentos_manual_view(request):
    """Lançamento manual em lote (cabeçalho fixo + linhas de detalhe) gravado no Mongo."""
    return render(request, "produtos/lancamentos_manual.html")


def _parse_decimal_dinheiro_br(s) -> Decimal | None:
    raw = str(s or "").strip().replace("R$", "").replace(" ", "")
    if not raw:
        return None
    if "," in raw and "." in raw:
        raw = raw.replace(".", "").replace(",", ".")
    elif "," in raw:
        raw = raw.replace(",", ".")
    try:
        return Decimal(raw).quantize(Decimal("0.01"))
    except Exception:
        return None


def _emprestimo_tentar_erp_batches(db, resultado_ext: dict) -> tuple[bool | None, str]:
    """Envia cada lote (entrada + parcelas) ao ERP, se configurado."""
    path_lanc = (
        (config("VENDA_ERP_API_FINANCEIRO_LANCAMENTO_PATH", default="") or "")
        or getattr(settings, "VENDA_ERP_API_FINANCEIRO_LANCAMENTO_PATH", "")
        or ""
    ).strip()
    if not path_lanc:
        return None, ""
    avisos: list[str] = []
    try:
        cli = VendaERPAPIClient()
        ent = resultado_ext.get("entrada") or {}
        if ent.get("ids"):
            body = montar_payload_erp_lancamentos_novos(
                db, ent["ids"], str(ent.get("lote") or ""), False
            )
            ok_erp, api_msg = cli.financeiro_tentar_lancamentos_api(body)
            if not ok_erp:
                avisos.append(f"Entrada: {api_msg}")
        for r_p in resultado_ext.get("parcelas") or []:
            if not r_p.get("ids"):
                continue
            body = montar_payload_erp_lancamentos_novos(
                db, r_p["ids"], str(r_p.get("lote") or ""), True
            )
            ok_erp, api_msg = cli.financeiro_tentar_lancamentos_api(body)
            if not ok_erp:
                avisos.append(f"Parcela: {api_msg}")
    except Exception as exc:
        return False, str(exc)[:800]
    if not avisos:
        return True, ""
    return False, "; ".join(avisos)[:800]


@ensure_csrf_cookie
@login_required(login_url="/admin/login/")
def emprestimos_gestao_view(request):
    """Hub: escolha entre externo, interno e consulta (telas separadas)."""
    return render(
        request,
        "produtos/emprestimos_hub.html",
        {"emprestimos_nav": "hub"},
    )


@ensure_csrf_cookie
@login_required(login_url="/admin/login/")
def emprestimos_externo_view(request):
    return render(
        request,
        "produtos/emprestimos_externo.html",
        {
            "emprestimos_defaults": emprestimo_defaults_para_ui(),
            "emprestimos_nav": "externo",
        },
    )


@ensure_csrf_cookie
@login_required(login_url="/admin/login/")
def emprestimos_interno_view(request):
    return render(
        request,
        "produtos/emprestimos_interno.html",
        {
            "emprestimos_defaults": emprestimo_defaults_para_ui(),
            "emprestimos_nav": "interno",
        },
    )


@ensure_csrf_cookie
@login_required(login_url="/admin/login/")
def emprestimos_consulta_view(request):
    return render(
        request,
        "produtos/emprestimos_consulta.html",
        {"emprestimos_nav": "consulta"},
    )


@login_required(login_url="/admin/login/")
@require_GET
def api_emprestimos_defaults(request):
    return JsonResponse({"ok": True, **emprestimo_defaults_para_ui()})


@login_required(login_url="/admin/login/")
@require_GET
def api_emprestimos_erp_lancamentos(request):
    empresa_id = str(request.GET.get("empresa_id") or "").strip()
    empresa_nome = str(request.GET.get("empresa_nome") or "").strip()
    try:
        lim = int(request.GET.get("limit") or 200)
    except ValueError:
        lim = 200
    _, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"ok": False, "erro": "Mongo indisponível", "itens": []}, status=503)
    itens = listar_lancamentos_emprestimo_do_mongo(
        db,
        empresa_id=empresa_id or None,
        empresa_nome=empresa_nome or None,
        limit=lim,
    )
    return JsonResponse({"ok": True, "itens": itens, "total": len(itens)})


@login_required(login_url="/admin/login/")
@require_GET
def api_emprestimos_listar(request):
    raw = (request.GET.get("tipo") or request.GET.get("categoria") or "").strip().lower()
    aliases = {
        "externos": "externo",
        "externo": "externo",
        "ext": "externo",
        "internos": "interno",
        "interno": "interno",
        "int": "interno",
        "socio": "interno",
        "sócio": "interno",
    }
    tipo_filtro = aliases.get(raw, raw if raw in ("externo", "interno") else "")
    try:
        lim = int(request.GET.get("limit") or 300)
    except ValueError:
        lim = 300
    lim = min(max(lim, 1), 500)
    empresa_id = str(request.GET.get("empresa_id") or "").strip() or None
    try:
        lim_mongo = int(request.GET.get("limit_mongo") or 400)
    except ValueError:
        lim_mongo = 400
    lim_mongo = min(max(lim_mongo, 1), 500)
    _, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"ok": False, "erro": "Mongo indisponível", "itens": []}, status=503)
    itens = listar_emprestimos_agro(
        db, tipo=tipo_filtro if tipo_filtro in ("externo", "interno") else None, limit=lim
    )
    if tipo_filtro in ("", "interno", "externo"):
        mongo_rows = listar_lancamentos_emprestimo_do_mongo(
            db, empresa_id=empresa_id, limit=lim_mongo
        )
        for row in mongo_rows:
            et = str(row.get("emprestimo_tipo") or "").strip().lower()
            if tipo_filtro == "interno" and et != "interno":
                continue
            if tipo_filtro == "externo" and et != "externo":
                continue
            if tipo_filtro == "" and et not in ("interno", "externo"):
                continue
            itens.append(mongo_emprestimo_como_item_agro(row))

        itens.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)

    return JsonResponse({"ok": True, "itens": itens, "filtro_tipo": tipo_filtro or None})


@login_required(login_url="/admin/login/")
@require_POST
def api_emprestimos_criar(request):
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)

    usuario = ""
    if request.user.is_authenticated:
        usuario = (
            getattr(request.user, "email", None) or request.user.get_username() or str(request.user.pk)
        )[:120]

    tipo = str(payload.get("tipo") or "").strip().lower()
    empresa_nome = str(payload.get("empresa_nome") or "").strip()
    empresa_id = str(payload.get("empresa_id") or "").strip() or None

    def _d(key: str) -> date | None:
        s = str(payload.get(key) or "").strip()[:10]
        if not s:
            return None
        try:
            return date.fromisoformat(s)
        except ValueError:
            return None

    _, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"ok": False, "erro": "Mongo indisponível"}, status=503)

    if tipo == "interno":
        v_aporte = _parse_decimal_dinheiro_br(payload.get("valor_aporte"))
        v_dev = _parse_decimal_dinheiro_br(payload.get("valor_devolucao_total"))
        if v_aporte is None or v_dev is None:
            return JsonResponse({"ok": False, "erro": "Valores inválidos."}, status=400)
        try:
            parc = int(payload.get("parcelas") or 1)
        except (TypeError, ValueError):
            parc = 1
        try:
            interv = int(payload.get("intervalo_dias") or 30)
        except (TypeError, ValueError):
            interv = 30
        d0 = _d("primeira_data_prevista")
        r = registrar_emprestimo_interno_agro(
            db,
            usuario_label=usuario,
            empresa_nome=empresa_nome,
            empresa_id=empresa_id,
            mutuario_label=str(payload.get("mutuario_label") or "").strip(),
            valor_aporte=v_aporte,
            valor_devolucao_total=v_dev,
            primeira_data_prevista=d0,
            parcelas=parc,
            intervalo_dias=interv,
            observacao=str(payload.get("observacao") or "").strip(),
        )
        st = 200 if r.get("ok") else 400
        return JsonResponse(r, status=st)

    if tipo != "externo":
        return JsonResponse({"ok": False, "erro": "tipo deve ser externo ou interno."}, status=400)

    v_rec = _parse_decimal_dinheiro_br(payload.get("valor_recebido"))
    v_tot = _parse_decimal_dinheiro_br(payload.get("valor_total_devido"))
    if v_rec is None or v_tot is None:
        return JsonResponse({"ok": False, "erro": "Valores inválidos."}, status=400)
    data_entrada = _d("data_entrada") or timezone.localdate()
    primeiro_venc = _d("primeiro_vencimento")
    if primeiro_venc is None:
        return JsonResponse({"ok": False, "erro": "Informe o primeiro vencimento das parcelas."}, status=400)
    try:
        parc = int(payload.get("parcelas") or 1)
    except (TypeError, ValueError):
        parc = 1
    try:
        interv = int(payload.get("intervalo_dias") or 30)
    except (TypeError, ValueError):
        interv = 30

    v_juros = _parse_decimal_dinheiro_br(payload.get("valor_juros"))
    if v_juros is None:
        v_juros = Decimal("0")
    eq_raw = payload.get("entrada_ja_quitada")
    entrada_quitada = True if eq_raw is None else bool(eq_raw)

    r = criar_emprestimo_externo_agro(
        db,
        usuario_label=usuario,
        empresa_nome=empresa_nome,
        empresa_id=empresa_id,
        credor_nome=str(payload.get("credor_nome") or "").strip(),
        credor_id=str(payload.get("credor_id") or "").strip() or None,
        valor_recebido=v_rec,
        valor_total_devido=v_tot,
        data_entrada=data_entrada,
        primeiro_vencimento=primeiro_venc,
        parcelas=parc,
        intervalo_dias=interv,
        banco_nome=str(payload.get("banco_nome") or "").strip(),
        banco_id=str(payload.get("banco_id") or "").strip() or None,
        forma_nome=str(payload.get("forma_nome") or "").strip(),
        forma_id=str(payload.get("forma_id") or "").strip() or None,
        plano_entrada_nome=str(payload.get("plano_entrada_nome") or "").strip(),
        plano_entrada_id=str(payload.get("plano_entrada_id") or "").strip() or None,
        plano_divida_nome=str(payload.get("plano_divida_nome") or "").strip(),
        plano_divida_id=str(payload.get("plano_divida_id") or "").strip() or None,
        grupo_nome=str(payload.get("grupo_nome") or "").strip() or None,
        grupo_id=str(payload.get("grupo_id") or "").strip() or None,
        observacao=str(payload.get("observacao") or "").strip(),
        entrada_ja_quitada=entrada_quitada,
        valor_juros=v_juros,
        plano_juros_nome=str(payload.get("plano_juros_nome") or "").strip() or None,
        plano_juros_id=str(payload.get("plano_juros_id") or "").strip() or None,
    )

    erp_ok, erp_msg = _emprestimo_tentar_erp_batches(db, r)
    out = dict(r)
    if erp_ok is not None:
        out["erp_lancamento_ok"] = erp_ok
    if erp_msg:
        out["aviso_api"] = erp_msg

    st = 200 if r.get("ok") else (207 if (r.get("ids_entrada") or r.get("ids_divida")) else 400)
    return JsonResponse(out, status=st)


@login_required(login_url="/admin/login/")
@require_POST
def api_emprestimos_interno_pagamento(request):
    """Pagamento ou devolução ao sócio em empréstimo interno (parcial ou integral ao saldo)."""
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)
    meta_id = str(payload.get("id") or payload.get("_id") or "").strip()
    val = _parse_decimal_dinheiro_br(payload.get("valor"))
    if val is None:
        return JsonResponse({"ok": False, "erro": "Valor inválido."}, status=400)
    ds = str(payload.get("data_pagamento") or "").strip()[:10]
    if not ds:
        return JsonResponse({"ok": False, "erro": "Informe a data do pagamento."}, status=400)
    try:
        dp = date.fromisoformat(ds)
    except ValueError:
        return JsonResponse({"ok": False, "erro": "Data inválida."}, status=400)
    obs = str(payload.get("observacao") or "").strip()
    usuario = ""
    if request.user.is_authenticated:
        usuario = (
            getattr(request.user, "email", None) or request.user.get_username() or str(request.user.pk)
        )[:120]
    _, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"ok": False, "erro": "Mongo indisponível"}, status=503)
    r = registrar_pagamento_emprestimo_interno_agro(
        db,
        meta_id=meta_id,
        valor=val,
        data_pagamento=dp,
        observacao=obs,
        usuario_label=usuario,
    )
    return JsonResponse(r, status=200 if r.get("ok") else 400)


def _emprestimos_interno_validar_pin(pin: str) -> tuple[bool, str]:
    pin = (pin or "").strip()
    if not pin:
        return False, "Informe o PIN."
    if pin == "1234":
        return False, "Senha padrão (1234) bloqueada. Troque seu PIN."
    if not PerfilUsuario.objects.filter(senha_rapida=pin).exists():
        return False, "PIN incorreto."
    return True, ""


@login_required(login_url="/admin/login/")
@require_POST
def api_emprestimos_interno_pagamento_excluir(request):
    """Exclui um pagamento no interno ou o cadastro inteiro (sem pagamentos), sempre motivo + PIN."""
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)

    ok_pin, err_pin = _emprestimos_interno_validar_pin(str(payload.get("pin") or ""))
    if not ok_pin:
        return JsonResponse({"ok": False, "erro": err_pin}, status=403)

    motivo = str(payload.get("motivo") or "").strip()
    if len(motivo) < 10:
        return JsonResponse(
            {"ok": False, "erro": "Informe o motivo da exclusão (mínimo 10 caracteres)."},
            status=400,
        )

    meta_id = str(payload.get("id") or payload.get("_id") or "").strip()
    excluir_registro = payload.get("excluir_registro") in (True, "true", "1", 1)

    usuario = ""
    if request.user.is_authenticated:
        usuario = (
            getattr(request.user, "email", None) or request.user.get_username() or str(request.user.pk)
        )[:120]

    _, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"ok": False, "erro": "Mongo indisponível"}, status=503)

    if excluir_registro:
        r = excluir_registro_emprestimo_interno_agro(
            db, meta_id=meta_id, motivo=motivo, usuario_label=usuario
        )
        return JsonResponse(r, status=200 if r.get("ok") else 400)

    pagamento_id = str(payload.get("pagamento_id") or "").strip() or None
    indice_raw = payload.get("indice")
    indice: int | None
    try:
        indice = int(indice_raw) if indice_raw is not None and str(indice_raw).strip() != "" else None
    except (TypeError, ValueError):
        indice = None

    if not pagamento_id and indice is None:
        return JsonResponse(
            {"ok": False, "erro": "Informe o pagamento (id ou índice) ou marque exclusão do cadastro."},
            status=400,
        )

    r = excluir_pagamento_emprestimo_interno_agro(
        db,
        meta_id=meta_id,
        pagamento_id=pagamento_id,
        indice=indice,
        motivo=motivo,
        usuario_label=usuario,
    )
    return JsonResponse(r, status=200 if r.get("ok") else 400)


@login_required(login_url="/admin/login/")
@require_GET
def api_lancamentos_sugestoes(request):
    """Autocomplete: campo=empresa|cliente|plano|forma|banco|grupo|centro&q=

    Só para ``campo=cliente`` (credor/fornecedor no financeiro):
    ``escopo`` = todos | pagar | receber | emprestimo;
    ``ordenar`` = nome | nome_desc | recente | frequencia;
    ``empresa_id`` = filtra lançamentos dessa empresa.
    """
    _, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"erro": "Mongo indisponível", "itens": []}, status=503)
    campo = (request.GET.get("campo") or "").strip().lower()
    q = (request.GET.get("q") or "").strip()
    try:
        raw_lim = int(request.GET.get("limit") or 30)
    except ValueError:
        raw_lim = 30
    cap = 500 if campo == "plano" else 80
    lim = min(max(raw_lim, 1), cap)
    escopo = (request.GET.get("escopo") or "todos").strip().lower()
    ordenar = (request.GET.get("ordenar") or "nome").strip().lower()
    empresa_id = (request.GET.get("empresa_id") or "").strip() or None
    itens = lancamentos_sugestoes_campo(
        db,
        campo,
        q=q or None,
        limit=lim,
        escopo=escopo,
        ordenar=ordenar,
        empresa_id=empresa_id,
    )
    return JsonResponse({"campo": campo, "itens": itens})


@login_required(login_url="/admin/login/")
@require_POST
def api_lancamentos_criar_manual_lote(request):
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)

    tipo = str(payload.get("tipo") or "pagar").strip().lower()
    despesa = tipo != "receber"

    def _d(key):
        s = str(payload.get(key) or "").strip()[:10]
        if not s:
            return None
        return date.fromisoformat(s)

    dc = _d("data_competencia")
    dv = _d("data_vencimento")
    if dc is None or dv is None:
        return JsonResponse({"ok": False, "erro": "Informe data de competência e de vencimento."}, status=400)

    linhas = payload.get("linhas")
    if not isinstance(linhas, list):
        return JsonResponse({"ok": False, "erro": "Campo linhas deve ser uma lista."}, status=400)

    raw_q = payload.get("quitado")
    quitado = raw_q is True or str(raw_q).strip().lower() in ("1", "true", "yes", "sim", "on")
    raw_rec = payload.get("recorrente")
    recorrente = raw_rec is True or str(raw_rec).strip().lower() in ("1", "true", "yes", "sim", "on")
    rec_mod = str(payload.get("recorrente_modo") or "").strip().lower()
    if rec_mod not in ("sempre", "normal"):
        rec_mod = "sempre"
    try:
        rec_par = int(
            payload.get("recorrente_parcelas")
            or payload.get("recorrencia_intervalo_meses")
            or payload.get("intervalo_meses")
            or 1
        )
    except (TypeError, ValueError):
        rec_par = 1
    if quitado and not str(payload.get("banco_id") or "").strip():
        return JsonResponse(
            {
                "ok": False,
                "erro": "Para lançar quitado, informe a conta bancária no cabeçalho (conta com ID do ERP).",
            },
            status=400,
        )

    _, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"ok": False, "erro": "Mongo indisponível"}, status=503)

    usuario = ""
    if request.user.is_authenticated:
        usuario = (
            getattr(request.user, "email", None) or request.user.get_username() or str(request.user.pk)
        )[:120]

    resultado = inserir_lancamentos_manual_lote(
        db,
        despesa=despesa,
        empresa_nome=str(payload.get("empresa_nome") or "").strip(),
        empresa_id=str(payload.get("empresa_id") or "").strip() or None,
        pessoa_nome=str(payload.get("pessoa_nome") or "").strip(),
        pessoa_id=str(payload.get("pessoa_id") or "").strip() or None,
        data_competencia=dc,
        data_vencimento=dv,
        banco_nome=str(payload.get("banco_nome") or "").strip(),
        banco_id=str(payload.get("banco_id") or "").strip() or None,
        forma_nome=str(payload.get("forma_nome") or "").strip(),
        forma_id=str(payload.get("forma_id") or "").strip() or None,
        grupo_nome=str(payload.get("grupo_nome") or "").strip() or None,
        grupo_id=str(payload.get("grupo_id") or "").strip() or None,
        usuario_label=usuario,
        linhas=linhas,
        marcar_quitado_pagar=bool(quitado and despesa),
        marcar_quitado_receber=bool(quitado and not despesa),
        recorrente=bool(recorrente),
        recorrente_modo=rec_mod,
        recorrente_parcelas=rec_par,
    )

    ok = bool(resultado.get("ok"))
    ids = resultado.get("ids") or []
    erros = resultado.get("erros") or []
    aviso_api_erp = None
    erp_lanc_ok = None
    path_lanc = (
        (config("VENDA_ERP_API_FINANCEIRO_LANCAMENTO_PATH", default="") or "")
        or getattr(settings, "VENDA_ERP_API_FINANCEIRO_LANCAMENTO_PATH", "")
        or ""
    ).strip()
    if path_lanc and ids:
        try:
            cli = VendaERPAPIClient()
            body_erp = montar_payload_erp_lancamentos_novos(db, ids, str(resultado.get("lote") or ""), despesa)
            ok_erp, api_msg = cli.financeiro_tentar_lancamentos_api(body_erp)
            erp_lanc_ok = bool(ok_erp)
            if not ok_erp:
                if isinstance(api_msg, dict):
                    try:
                        aviso_api_erp = json.dumps(api_msg, ensure_ascii=False)[:800]
                    except Exception:
                        aviso_api_erp = str(api_msg)[:800]
                else:
                    aviso_api_erp = str(api_msg)[:800]
        except Exception as exc:
            erp_lanc_ok = False
            aviso_api_erp = str(exc)[:800]
    elif ids and not path_lanc:
        logger.info(
            "Lançamento manual: Mongo gravou %s título(s); POST ao ERP não executado "
            "(VENDA_ERP_API_FINANCEIRO_LANCAMENTO_PATH vazio no ambiente).",
            len(ids),
        )
    if ok:
        st = 200
    elif ids:
        st = 207
    else:
        st = 400
    out_lm = {
        "ok": ok,
        "lote": resultado.get("lote"),
        "ids": ids,
        "erros": erros,
        "erp_inclusao_configurada": bool(path_lanc),
    }
    if erp_lanc_ok is not None:
        out_lm["erp_lancamento_ok"] = erp_lanc_ok
    if aviso_api_erp:
        out_lm["aviso_api"] = aviso_api_erp
    return JsonResponse(out_lm, status=st)


@login_required(login_url="/admin/login/")
@require_POST
def api_lancamentos_definir_recorrente(request):
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)
    lid = str(payload.get("id") or "").strip()
    if not lid:
        return JsonResponse({"ok": False, "erro": "Informe o id do lançamento."}, status=400)
    raw_r = payload.get("recorrente")
    recorrente = raw_r is True or str(raw_r).strip().lower() in ("1", "true", "yes", "sim", "on")
    try:
        intervalo = int(payload.get("intervalo_meses") or payload.get("recorrencia_intervalo_meses") or 1)
    except (TypeError, ValueError):
        intervalo = 1

    _, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"ok": False, "erro": "Mongo indisponível"}, status=503)

    usuario = ""
    if request.user.is_authenticated:
        usuario = (
            getattr(request.user, "email", None) or request.user.get_username() or str(request.user.pk)
        )[:120]

    r = definir_lancamento_recorrente_mongo(
        db,
        lid,
        recorrente=bool(recorrente),
        intervalo_meses=intervalo,
        usuario_label=usuario,
    )
    if not r.get("ok"):
        return JsonResponse({"ok": False, "erro": r.get("erro") or "Falha ao atualizar."}, status=400)
    return JsonResponse(
        {
            "ok": True,
            "recorrente": bool(recorrente),
            "intervalo_meses": 1 if recorrente else max(1, min(intervalo, 36)),
        }
    )


def ajuste_mobile_view(request):
    if not request.session.get("mobile_auth"):
        return render(request, "produtos/ajuste_mobile_login.html")
    return render(request, "produtos/mobile_ajuste.html")


# --- MOTOR DE BUSCA ÚNICO ---
def motor_de_busca_agro(
    termo_original,
    db,
    client,
    limit=20,
    include_inactive=False,
    *,
    regex_stage2_cap: int | None = None,
    regex_stage3_cap: int | None = None,
    regex_stage3b_cap: int | None = None,
):
    """Busca produtos no Mongo (PDV e cadastro).

    ``regex_stage*_cap`` reduz documentos lidos em consultas com ``$regex``
    (telas como cadastro ERP podem passar caps menores para resposta mais rápida).
    """
    termo_original = str(termo_original or "").strip()
    if not termo_original:
        return []

    if regex_stage2_cap is not None:
        lim_s2 = max(20, min(int(regex_stage2_cap), 220))
    else:
        lim_s2 = 160
    if regex_stage3_cap is not None:
        lim_s3 = max(20, min(int(regex_stage3_cap), 220))
    else:
        lim_s3 = 160
    # ``regex_stage3b_cap <= 0`` desliga o estágio 3b (OR gigante de regex), mais leve no cadastro.
    if regex_stage3b_cap is not None:
        _raw3b = int(regex_stage3b_cap)
        lim_s3b = 0 if _raw3b <= 0 else max(40, min(_raw3b, 280))
    else:
        lim_s3b = 220

    termo_limpo = _somente_alnum(termo_original)
    termo_ix = termo_limpo.lower() if termo_limpo else ""
    palavras = [p for p in termo_original.split() if p]
    base_filter = {} if include_inactive else {"CadastroInativo": {"$ne": True}}

    candidatos = []
    vistos = set()

    def adicionar(lista):
        for item in lista:
            pid = str(item.get("Id") or item.get("_id"))
            if pid not in vistos:
                vistos.add(pid)
                candidatos.append(item)

    # 1) Código / barras — campo denormalizado ``index_codigos`` (+ índice multikey)
    if termo_limpo and _termo_parece_codigo(termo_original):
        query_cod_exato = {**base_filter, INDEX_CODIGOS_CAMPO: termo_ix}
        exatos = list(db[client.col_p].find(query_cod_exato).limit(max(limit, 10)))
        if exatos:
            return merge_busca_codigo_prioridade_principal(exatos, [], termo_limpo, limit)

        query_cod_prefixo = {
            **base_filter,
            INDEX_CODIGOS_CAMPO: _regex_inicio_ci(termo_limpo),
        }
        adicionar(list(db[client.col_p].find(query_cod_prefixo).limit(30)))

    # 2) Busca por palavras com expansão
    condicoes_and = []
    for p in palavras:
        tokens = expandir_tokens(p)
        p_norm = normalizar(p)
        if p_norm and p_norm not in tokens:
            tokens.append(p_norm)

        regex_tokens = []
        for t in tokens:
            if not t:
                continue
            regex_tokens.append(_regex_contem_ci(t))

        if regex_tokens:
            or_palavra = [
                {"BuscaTexto": {"$in": regex_tokens}},
                {"Nome": {"$in": regex_tokens}},
                {"Marca": {"$in": regex_tokens}},
                {"NomeNormalizado": {"$in": regex_tokens}},
                {INDEX_CODIGOS_CAMPO: {"$in": regex_tokens}},
            ]
            condicoes_and.append({"$or": or_palavra})

    if condicoes_and:
        adicionar(list(db[client.col_p].find({
            **base_filter,
            "$and": condicoes_and
        }).limit(lim_s2)))

    # 3) Fallback por frase inteira
    if len(candidatos) < limit:
        termo_regex = _regex_contem_ci(termo_original)
        or_frase = [
            {"Nome": termo_regex},
            {"BuscaTexto": termo_regex},
            {"Marca": termo_regex},
            {"NomeNormalizado": termo_regex},
            {INDEX_CODIGOS_CAMPO: termo_regex},
        ]
        adicionar(list(db[client.col_p].find({**base_filter, "$or": or_frase}).limit(lim_s3)))

    # 3b) Qualquer palavra/token bate (recall alto; útil com BuscaTexto defasado)
    if lim_s3b > 0 and len(candidatos) < max(limit, 24) and palavras:
        tokens_flat = []
        visto_tok = set()
        for p in palavras:
            for t in expandir_tokens(p):
                if t and t not in visto_tok:
                    visto_tok.add(t)
                    tokens_flat.append(t)
            p_norm = normalizar(p)
            if p_norm and p_norm not in visto_tok:
                visto_tok.add(p_norm)
                tokens_flat.append(p_norm)
        or_clauses = []
        for t in tokens_flat:
            if not t or len(str(t)) < 2:
                continue
            rx = _regex_contem_ci(t)
            or_clauses.extend([
                {"BuscaTexto": rx},
                {"Nome": rx},
                {"Marca": rx},
                {"NomeNormalizado": rx},
            ])
        if or_clauses:
            adicionar(list(db[client.col_p].find({
                **base_filter,
                "$or": or_clauses,
            }).limit(lim_s3b)))

    # 4) Ordenação de relevância
    termo_norm = normalizar(termo_original)
    termo_limpo_lower = termo_limpo.lower()

    def score(p):
        nome = str(p.get("Nome") or "")
        marca = str(p.get("Marca") or "")

        nome_norm = normalizar(nome)
        marca_norm = normalizar(marca)

        s = 0

        if termo_limpo_lower and _termo_parece_codigo(termo_original):
            if produto_termo_bate_campos_principais(p, termo_limpo):
                s += 6000

        if termo_limpo_lower:
            exact_ok = False
            pref_ok = False
            idx = p.get(INDEX_CODIGOS_CAMPO)
            if isinstance(idx, list):
                for x in idx:
                    xs = str(x).lower()
                    if xs == termo_limpo_lower:
                        exact_ok = True
                        break
                    if xs.startswith(termo_limpo_lower):
                        pref_ok = True
            if not exact_ok:
                ext_b = _somente_alnum(str(_extrair_codigo_barras(p) or "")).lower()
                if ext_b == termo_limpo_lower:
                    exact_ok = True
                elif ext_b.startswith(termo_limpo_lower):
                    pref_ok = True
            if exact_ok:
                s += 5000
            elif pref_ok:
                s += 1750

        if termo_norm:
            if nome_norm == termo_norm:
                s += 1600
            elif nome_norm.startswith(termo_norm):
                s += 1200
            elif termo_norm in nome_norm:
                s += 700

            if marca_norm.startswith(termo_norm):
                s += 200

        if palavras:
            presentes = 0
            for p_txt in palavras:
                p_norm = normalizar(p_txt)
                if p_norm and p_norm in nome_norm:
                    presentes += 1
            s += presentes * 120
            if presentes == len(palavras):
                s += 300

        s -= len(nome_norm.split())
        return s

    candidatos.sort(key=lambda p: (-score(p), str(p.get("Nome") or "").lower()))
    return candidatos[:limit]


def _ean13_digito_verificador(d12: str) -> int | None:
    """Último dígito do EAN-13 a partir dos 12 primeiros (GTIN)."""
    if len(d12) != 12 or not d12.isdigit():
        return None
    s = 0
    for i, ch in enumerate(d12):
        n = int(ch)
        s += n if i % 2 == 0 else n * 3
    mod = s % 10
    return 0 if mod == 0 else 10 - mod


def _parse_etiqueta_balanca_ean13_br(q: str):
    """
    Padrão comum de balança: 2 C C C C 0 T T T T T T DV (EAN-13).
    C = código interno (4 dígitos), T = valor total em centavos (6 dígitos, 2 decimais).
    Só aceita código com dígito verificador EAN-13 válido (evita preço arbitrário).
    """
    d = re.sub(r"\D", "", str(q or ""))
    if len(d) != 13 or d[0] != "2":
        return None
    dv_exp = _ean13_digito_verificador(d[:12])
    if dv_exp is None or int(d[12]) != dv_exp:
        return None
    cod4 = d[1:5]
    # sep = d[5] — costuma ser 0
    try:
        valor_cent = int(d[6:12])
    except ValueError:
        return None
    preco = (Decimal(valor_cent) / Decimal(100)).quantize(Decimal("0.01"))
    return cod4, preco


def _buscar_produto_por_codigo_interno_balanca(db, client, cod4: str):
    """Resolve produto pelos 4 dígitos do código na etiqueta (via ``index_codigos``)."""
    col = db[client.col_p]
    base = {"CadastroInativo": {"$ne": True}}
    variants = set()
    variants.add(cod4)
    variants.add(cod4.lstrip("0") or "0")
    for z in (5, 6, 7):
        variants.add(cod4.zfill(z))
    alvos = sorted({str(v).strip().lower() for v in variants if str(v).strip()})
    if not alvos:
        return None
    try:
        return col.find_one({**base, INDEX_CODIGOS_CAMPO: {"$in": alvos}})
    except Exception:
        return None


# --- APIs DE BUSCA ---
@require_GET
def api_buscar_produtos(request):
    """Busca única: PDV (`/api/buscar/`) ou tela de compras com `?compras=1` (inclui custos)."""
    compras = getattr(request, "_compras_mode", False) or request.GET.get("compras") in (
        "1",
        "true",
        "yes",
    )
    wizard_mode = (request.GET.get("wizard") or "").strip().lower() in ("1", "true", "yes")
    wizard_catalog = wizard_mode and (request.GET.get("wizard_catalog") or "").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    q = request.GET.get("q", "").strip()
    client, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"produtos": []})
    if not q and not wizard_catalog:
        return JsonResponse({"produtos": []})

    try:
        balanca_auditoria_q: str | None = None
        if wizard_catalog:
            prods = list(
                db[client.col_p]
                .find({"CadastroInativo": {"$ne": True}})
                .sort("Nome", 1)
                .limit(25000)
            )
            preco_por_id = {}
        else:
            preco_por_id = {}
            bal = _parse_etiqueta_balanca_ean13_br(q)
            if bal:
                cod4, preco_etiqueta = bal
                d_lido = re.sub(r"\D", "", str(q or ""))
                if len(d_lido) == 13 and d_lido[0] == "2":
                    balanca_auditoria_q = d_lido
                p_escolhido = _buscar_produto_por_codigo_interno_balanca(db, client, cod4)
                if p_escolhido:
                    pid_b = str(p_escolhido.get("Id") or p_escolhido.get("_id"))
                    preco_por_id[pid_b] = preco_etiqueta
                    prods = [p_escolhido]
                else:
                    prods = motor_de_busca_agro(q, db, client, limit=80)
            else:
                prods = motor_de_busca_agro(q, db, client, limit=80)
        vistos_busca = {str(p.get("Id") or p["_id"]) for p in prods}
        if not wizard_catalog and q:
            extras_ov = _mongo_produtos_por_overlay_codigo_busca(q, db, client, vistos_busca)
            if extras_ov:
                prods = extras_ov + prods
        p_ids = [str(p.get("Id") or p["_id"]) for p in prods]

        medias_map = {}
        if not wizard_mode:
            try:
                medias_map = _obter_mapa_medias_venda_cache(db)
            except Exception:
                logger.warning("api_buscar_produtos: medias indisponíveis", exc_info=True)

        estoque_map = {}
        try:
            if p_ids:
                _emax = 2000
                if len(p_ids) > _emax:
                    for _ej in range(0, len(p_ids), _emax):
                        _es = p_ids[_ej : _ej + _emax]
                        estoques = list(db[client.col_e].find({"ProdutoID": {"$in": _es}}))
                        estoque_map.update(_mapear_estoques_por_produto(estoques, client))
                else:
                    estoques = list(db[client.col_e].find({"ProdutoID": {"$in": p_ids}}))
                    estoque_map = _mapear_estoques_por_produto(estoques, client)
        except Exception:
            logger.warning("api_buscar_produtos: estoque indisponível — retornando saldo 0", exc_info=True)

        ajustes_map = {}
        try:
            if p_ids:
                # SQLite (e outros) limitam variáveis por query; catálogo wizard pode ter ~25k ids.
                _chunk = 400
                for _i in range(0, len(p_ids), _chunk):
                    _slice = p_ids[_i : _i + _chunk]
                    for aj in AjusteRapidoEstoque.objects.filter(produto_externo_id__in=_slice):
                        ajustes_map[(aj.produto_externo_id, aj.deposito)] = aj
        except Exception:
            logger.warning("api_buscar_produtos: ajustes PIN indisponíveis", exc_info=True)

        pedido_sep_map: dict[str, float] = {}
        try:
            if p_ids:
                _chunk_pt = 400
                for _i in range(0, len(p_ids), _chunk_pt):
                    _slice = p_ids[_i : _i + _chunk_pt]
                    for ped in PedidoTransferencia.objects.filter(
                        produto_externo_id__in=_slice, status="IMPRESSO"
                    ):
                        pedido_sep_map[str(ped.produto_externo_id)] = float(ped.quantidade)
        except Exception:
            logger.warning("api_buscar_produtos: pedidos transferência indisponível", exc_info=True)

        ultimas_compras_map: dict[str, list] = {}
        if compras and prods and not (wizard_catalog and len(prods) > 400):
            try:
                prod_por_id = {str(x.get("Id") or x.get("_id")): x for x in prods}
                p_ids_busca = [str(x.get("Id") or x.get("_id")) for x in prods]
                ultimas_compras_map = _ultimas_compras_por_produto_ids(
                    db, p_ids_busca, prod_por_id, limit=3
                )
            except Exception as exc:
                logger.warning("api_buscar_produtos: ultimas_compras indisponível — %s", exc)

        overlay_pdv_map = _overlay_mapa_por_ids_chunked(p_ids) if p_ids else {}

        res = []
        for p in prods:
            pid = str(p.get("Id") or p["_id"])

            saldo_centro_erp = _float_api_json(estoque_map.get(pid, {}).get("centro", 0.0))
            saldo_vila_erp = _float_api_json(estoque_map.get(pid, {}).get("vila", 0.0))

            ac = ajustes_map.get((pid, "centro"))
            av = ajustes_map.get((pid, "vila"))

            saldo_centro = (
                _float_api_json(ac.saldo_informado) + (saldo_centro_erp - _float_api_json(ac.saldo_erp_referencia))
                if ac else saldo_centro_erp
            )
            saldo_vila = (
                _float_api_json(av.saldo_informado) + (saldo_vila_erp - _float_api_json(av.saldo_erp_referencia))
                if av else saldo_vila_erp
            )

            codigo = p.get("Codigo") or ""
            codigo_nfe = p.get("CodigoNFe") or codigo or ""
            codigo_barras = _extrair_codigo_barras(p)
            media_d = _float_api_json(medias_map.get(pid, 0.0))
            pv = (
                _float_api_json(preco_por_id[pid])
                if pid in preco_por_id
                else _float_api_json(p.get("ValorVenda") or p.get("PrecoVenda") or 0)
            )
            prateleira_busca = (
                p.get("Prateleira")
                or p.get("Localizacao")
                or p.get("LocalEstoque")
                or p.get("Setor")
                or p.get("EnderecoPrateleira")
                or ""
            )
            _sub_w = str(
                p.get("SubGrupo") or p.get("Subcategoria") or p.get("NomeSubcategoria") or ""
            ).strip()
            _cat_w = p.get("NomeCategoria") or p.get("Categoria") or p.get("Grupo") or ""
            if not str(_cat_w or "").strip() and _sub_w:
                _cat_w = _sub_w

            _ix_raw = p.get(INDEX_CODIGOS_CAMPO)
            if wizard_catalog:
                _ix_out: list[str] = []
            elif isinstance(_ix_raw, list):
                _ix_out = [str(x) for x in _ix_raw[:260]]
            else:
                _ix_out = []
            row = {
                "id": pid,
                "nome": p.get("Nome"),
                "marca": p.get("Marca") or "",
                "codigo": codigo,
                "codigo_nfe": codigo_nfe,
                "codigo_barras": codigo_barras,
                "index_codigos": _ix_out,
                "preco_venda": round(_float_api_json(pv), 2),
                "imagem": _formatar_url_imagem(_extrair_imagem_produto(p, {}, pid)),
                "saldo_centro": round(_float_api_json(saldo_centro), 2),
                "saldo_vila": round(_float_api_json(saldo_vila), 2),
                "qtd_separacao_transferencia": round(
                    _float_api_json(pedido_sep_map.get(pid, 0.0)), 3
                ),
                "media_venda_diaria_30d": round(_float_api_json(media_d), 4),
                "preco_etiqueta_balanca": bool(pid in preco_por_id) and not compras,
            }
            if not wizard_mode:
                row.update(
                    {
                        "prateleira": str(prateleira_busca).strip() if prateleira_busca is not None else "",
                        "fornecedor": p.get("NomeFornecedor")
                        or p.get("Fornecedor")
                        or p.get("RazaoSocialFornecedor")
                        or p.get("Fabricante")
                        or "",
                        "categoria": _cat_w,
                        "subcategoria": _sub_w,
                        "saldo_centro_erp": round(saldo_centro_erp, 2),
                        "saldo_vila_erp": round(saldo_vila_erp, 2),
                        "saldo_erp_centro": round(saldo_centro_erp, 2),  # compatibilidade com mobile atual
                        "saldo_erp_vila": round(saldo_vila_erp, 2),
                        "auditoria_codigo_bip": balanca_auditoria_q
                        if (balanca_auditoria_q and pid in preco_por_id)
                        else None,
                        "referencia": _mongo_primeiro_texto(
                            p,
                            ("Referencia", "CodigoReferencia", "ReferenciaFornecedor"),
                        ),
                        "sku": _mongo_primeiro_texto(p, ("Sku", "SKU", "CodigoSku")),
                    }
                )
            if compras:
                custos = _custos_compra_produto(p)
                row["preco_custo"] = custos["preco_custo"]
                row["preco_custo_acrescimo"] = custos["preco_custo_final"]
                row["preco_custo_final"] = custos["preco_custo_final"]
                row["ultimas_compras"] = ultimas_compras_map.get(pid, [])
            _aplicar_produto_gestao_overlay_em_dict(row, overlay_pdv_map.get(pid))
            res.append(row)

        if wizard_catalog:
            res.sort(key=lambda r: str(r.get("nome") or "").lower())
        elif wizard_mode:
            q_wiz = str(q or "").strip().lower()
            res.sort(
                key=lambda r: (
                    -1
                    if str(r.get("codigo_barras") or "") == q
                    or str(r.get("codigo_nfe") or "") == q
                    or (
                        q_wiz
                        and any(
                            str(x).lower() == q_wiz
                            for x in (r.get("index_codigos") or [])
                            if x is not None
                        )
                    )
                    else 0,
                    str(r.get("nome") or "").lower(),
                )
            )
            res = res[:24]
        else:
            res.sort(
                key=lambda r: (
                    -float(r.get("media_venda_diaria_30d") or 0),
                    str(r.get("nome") or "").lower(),
                )
            )

        exact = bool(preco_por_id) and len(res) == 1 and not wizard_catalog
        return JsonResponse({"produtos": res, "exact_barcode_match": exact})
    except Exception as e:
        return JsonResponse({"erro": str(e)}, status=500)


@require_GET
def api_buscar_compras(request):
    """Compatibilidade: mesmo motor e payload que GET /api/buscar/?compras=1"""
    setattr(request, "_compras_mode", True)
    try:
        return api_buscar_produtos(request)
    finally:
        if hasattr(request, "_compras_mode"):
            delattr(request, "_compras_mode")


def _produto_mongo_para_cadastro_row(p: dict) -> dict:
    """Monta JSON de cadastro (sem estoque) a partir de um documento da coleção de produtos Mongo."""
    pid = str(p.get("Id") or p["_id"])
    codigo = p.get("Codigo")
    codigo_s = "" if codigo is None else str(codigo).strip()
    codigo_nfe = p.get("CodigoNFe")
    codigo_nfe_s = codigo_s
    if codigo_nfe is not None:
        cn = str(codigo_nfe).strip()
        if cn:
            codigo_nfe_s = cn
    codigo_barras = _extrair_codigo_barras(p) or ""
    _sub_w = str(
        p.get("SubGrupo") or p.get("Subcategoria") or p.get("NomeSubcategoria") or ""
    ).strip()
    _cat_w = p.get("NomeCategoria") or p.get("Categoria") or p.get("Grupo") or ""
    if not str(_cat_w or "").strip() and _sub_w:
        _cat_w = _sub_w
    prateleira_raw = (
        p.get("Prateleira")
        or p.get("Localizacao")
        or p.get("LocalEstoque")
        or p.get("Setor")
        or p.get("EnderecoPrateleira")
        or ""
    )
    pv = _float_api_json(p.get("ValorVenda") or p.get("PrecoVenda") or 0)
    p_custo = _float_api_json(p.get("PrecoCusto") or p.get("ValorCusto") or 0)
    fornecedor = (
        p.get("NomeFornecedor")
        or p.get("Fornecedor")
        or p.get("RazaoSocialFornecedor")
        or p.get("Fabricante")
        or ""
    )
    unidade = str(p.get("Unidade") or p.get("SiglaUnidade") or "").strip()
    descricao = (
        str(p.get("Descricao") or "").strip()
        or str(p.get("Observacao") or "").strip()
        or str(p.get("Complemento") or "").strip()
    )
    ncm = str(p.get("NCM") or p.get("CodigoNCM") or "").strip()
    return {
        "id": pid,
        "nome": str(p.get("Nome") or "").strip(),
        "marca": str(p.get("Marca") or "").strip(),
        "codigo": codigo_s,
        "codigo_nfe": codigo_nfe_s,
        "codigo_barras": str(codigo_barras).strip() if codigo_barras else "",
        "preco_venda": round(pv, 2),
        "preco_custo": round(p_custo, 2),
        "categoria": str(_cat_w or "").strip(),
        "subcategoria": _sub_w,
        "prateleira": str(prateleira_raw).strip() if prateleira_raw else "",
        "fornecedor": str(fornecedor).strip(),
        "imagem": _formatar_url_imagem(_extrair_imagem_produto(p, {}, pid)),
        "inativo": bool(p.get("CadastroInativo")),
        "unidade": unidade,
        "descricao": descricao,
        "ncm": ncm,
    }


def _mongo_primeiro_texto(p: dict, chaves: tuple[str, ...], default: str = "") -> str:
    for k in chaves:
        v = p.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return default


def _mongo_primeiro_float(p: dict, chaves: tuple[str, ...]) -> float | None:
    for k in chaves:
        raw = p.get(k)
        if raw is None or raw == "":
            continue
        try:
            return float(str(raw).replace(",", "."))
        except (ValueError, TypeError):
            continue
    return None


def _mongo_primeiro_bool(p: dict, chaves: tuple[str, ...]) -> bool | None:
    for k in chaves:
        if k not in p:
            continue
        v = p.get(k)
        if isinstance(v, bool):
            return v
        if v in (1, "1", "true", "True", "SIM", "Sim", "S", "s"):
            return True
        if v in (0, "0", "false", "False", "NAO", "Não", "N", "n"):
            return False
    return None


def _extrair_composicao_produto_mongo(p: dict) -> list[dict]:
    candidatos = (
        "ItensComposicao",
        "ComposicaoProduto",
        "Composicao",
        "KitItens",
        "ProdutosComposicao",
        "ListaComposicao",
        "ItemComposicaoProduto",
        "ItensKit",
        "ProdutoComposicaoItens",
        "DtoComposicaoProduto",
        "ItensComposicaoProduto",
        "ListaItemComposicao",
        "ComposicaoItens",
    )
    for key in candidatos:
        val = p.get(key)
        if not isinstance(val, list):
            continue
        out: list[dict] = []
        for it in val:
            if not isinstance(it, dict):
                continue
            pid = (
                it.get("ProdutoID")
                or it.get("IdProduto")
                or it.get("Produto_Id")
                or it.get("ItemProdutoID")
            )
            nome = it.get("Nome") or it.get("NomeProduto") or it.get("ProdutoNome") or it.get("Produto")
            cod = it.get("Codigo") or it.get("CodigoNFe") or it.get("CodigoProduto") or ""
            qraw = it.get("Quantidade") or it.get("Qtd") or it.get("QuantidadeComposicao") or 1
            try:
                q = float(str(qraw).replace(",", "."))
            except (ValueError, TypeError):
                q = 1.0
            dep = it.get("Deposito") or it.get("DepositoNome") or it.get("NomeDeposito") or ""
            out.append(
                {
                    "produto_id": str(pid).strip() if pid is not None else "",
                    "nome": str(nome or "").strip(),
                    "codigo": str(cod or "").strip(),
                    "quantidade": q,
                    "deposito": str(dep or "").strip(),
                }
            )
        if out:
            return out
    return []


def _serialize_variacoes_marca_rows(rows: list[ProdutoMarcaVariacaoAgro]) -> list[dict]:
    out = []
    for r in rows:
        out.append(
            {
                "id": r.pk,
                "codigo_interno": str(getattr(r, "codigo_interno", None) or "").strip(),
                "marca": str(r.marca or "").strip(),
                "codigo_barras": str(r.codigo_barras or "").strip(),
                "codigo_fornecedor": str(r.codigo_fornecedor or "").strip(),
                "estoque": float(r.estoque or 0),
                "custo_unitario": float(r.custo_unitario or 0),
                "ordem": int(r.ordem or 0),
            }
        )
    return out


def _merge_fiscal_overlay_sobre_row_cadastro(row: dict, ov: ProdutoGestaoOverlayAgro | None) -> None:
    """Sobrescreve campos fiscais exibidos com o JSON local (NFC-e futura), quando preenchidos."""
    if not ov:
        row.setdefault("cadastro_extras", {})
        return
    ce = ov.cadastro_extras if isinstance(getattr(ov, "cadastro_extras", None), dict) else {}
    row["cadastro_extras"] = dict(ce)
    fis = ce.get("fiscal")
    if not isinstance(fis, dict):
        return
    mapping = (
        ("ncm", "ncm", 20),
        ("cest", "cest", 12),
        ("cfop", "cfop_padrao", 10),
        ("csosn", "csosn", 10),
        ("origem", "origem_mercadoria", 8),
    )
    for json_k, row_k, mx in mapping:
        v = str(fis.get(json_k) or "").strip()
        if v:
            row[row_k] = v[:mx]


def _deposito_baixa_kit_componente(kit_cfg: dict | None, dep_sessao: str) -> str:
    d = (dep_sessao or "centro").strip().lower()
    if d not in ("centro", "vila"):
        d = "centro"
    if not isinstance(kit_cfg, dict):
        return d
    raw = str(kit_cfg.get("deposito") or "").strip()
    if not raw:
        return d
    rl = raw.lower()
    if rl in ("centro", "vila"):
        return rl
    if raw == "1":
        return "centro"
    if raw == "2":
        return "vila"
    if raw == "3":
        return "centro"
    return d


def _custo_medio_ponderado_variacoes_rows(rows: list[ProdutoMarcaVariacaoAgro]) -> Decimal | None:
    tot_e = Decimal(0)
    tot_v = Decimal(0)
    for r in rows:
        try:
            e = Decimal(str(r.estoque or 0))
        except Exception:
            e = Decimal(0)
        try:
            c = Decimal(str(r.custo_unitario or 0))
        except Exception:
            c = Decimal(0)
        tot_e += e
        tot_v += e * c
    if tot_e <= 0:
        return None
    return (tot_v / tot_e).quantize(Decimal("0.0001"))


def _custo_medio_ponderado_variacoes_dicts(items: list[dict]) -> Decimal | None:
    tot_e = Decimal(0)
    tot_v = Decimal(0)
    for it in items:
        if not isinstance(it, dict):
            continue
        try:
            e = Decimal(str(it.get("estoque") or 0).replace(",", ".").strip() or 0)
        except Exception:
            e = Decimal(0)
        try:
            c = Decimal(str(it.get("custo_unitario") or 0).replace(",", ".").strip() or 0)
        except Exception:
            c = Decimal(0)
        tot_e += e
        tot_v += e * c
    if tot_e <= 0:
        return None
    return (tot_v / tot_e).quantize(Decimal("0.0001"))


def _custo_unitario_agro_para_produto_id(
    db, client_m, produto_id: str, doc: dict | None
) -> Decimal | None:
    pid = str(produto_id or "").strip()[:64]
    if not pid:
        return None
    qs = list(
        ProdutoMarcaVariacaoAgro.objects.filter(produto_externo_id=pid).order_by("ordem", "id")
    )
    w = _custo_medio_ponderado_variacoes_rows(qs)
    if w is not None:
        return w
    if doc:
        custos = _custos_compra_produto(doc)
        raw = custos.get("preco_custo_final")
        if raw is None:
            raw = custos.get("preco_custo")
        if raw is not None:
            try:
                return Decimal(str(raw)).quantize(Decimal("0.0001"))
            except Exception:
                return None
    return None


def _custo_total_kit_composicao_agro(db, client_m, p: dict) -> Decimal | None:
    comp = _extrair_composicao_produto_mongo(p)
    if not comp:
        return None
    tot = Decimal(0)
    for it in comp:
        spid = str(it.get("produto_id") or "").strip()
        if not spid:
            continue
        try:
            q = Decimal(str(it.get("quantidade") or 0).replace(",", ".").strip() or 0)
        except Exception:
            q = Decimal(0)
        doc_c = _produto_mongo_por_id_externo(db, client_m, spid)
        cu = _custo_unitario_agro_para_produto_id(db, client_m, spid, doc_c)
        if cu is None:
            continue
        tot += q * cu
    return tot.quantize(Decimal("0.0001"))


def _mongo_produto_ids_por_codigos_cadastro(
    db,
    client_m,
    codigos_barras: list[str],
    codigos_forn: list[str],
    codigos_interno: list[str] | None = None,
) -> set[str]:
    out: set[str] = set()
    ors = []
    for c in codigos_barras:
        c = str(c or "").strip()
        if not c:
            continue
        ors.append({"CodigoBarras": c})
        ors.append({"EAN_NFe": c})
        if c.isdigit():
            try:
                ors.append({"CodigoBarras": str(int(c))})
            except (ValueError, TypeError):
                pass
    for c in codigos_forn:
        c = str(c or "").strip()
        if not c:
            continue
        ors.append({"Codigo": c})
        ors.append({"CodigoNFe": c})
    for c in codigos_interno or []:
        c = str(c or "").strip()
        if not c:
            continue
        ors.append({"Codigo": c})
        ors.append({"CodigoNFe": c})
    if not ors:
        return out
    try:
        cur = db[client_m.col_p].find({"$or": ors}, {"Id": 1, "_id": 1}).limit(400)
        for d in cur:
            pid = str(d.get("Id") or d.get("_id") or "").strip()
            if pid:
                out.add(pid)
    except Exception as exc:
        logger.warning("mongo_produto_ids_por_codigos_cadastro: %s", exc)
    return out


def _linha_similar_de_dict_embedded(d: dict) -> dict | None:
    """Monta uma linha de similar a partir de objeto embutido no próprio produto (lista de dicts)."""
    if not isinstance(d, dict) or not d:
        return None
    pid = None
    for key in (
        "Id",
        "ProdutoID",
        "ProdutoId",
        "IdProduto",
        "Produto_Id",
        "id",
        "ID",
    ):
        v = d.get(key)
        if v is not None and str(v).strip():
            pid = str(v).strip()
            break
    if not pid and d.get("_id") is not None:
        pid = str(d.get("_id")).strip()
    if not pid:
        return None
    nome = str(
        d.get("Nome") or d.get("NomeProduto") or d.get("Descricao") or d.get("nome") or ""
    ).strip()
    cb = (
        str(
            d.get("CodigoBarras")
            or d.get("EAN")
            or d.get("EAN_NFe")
            or d.get("CodigoBarrasProduto")
            or ""
        ).strip()
        or str(d.get("CodigoNFe") or d.get("Codigo") or "").strip()
    )
    marca = str(d.get("Marca") or "").strip()
    modelo = str(d.get("Modelo") or d.get("NomeModelo") or "").strip()
    fabricante = str(
        d.get("Fabricante") or d.get("NomeFabricante") or d.get("Fornecedor") or ""
    ).strip()
    return {
        "id": pid,
        "nome": nome,
        "codigo": cb,
        "marca": marca,
        "modelo": modelo,
        "fabricante": fabricante,
    }


def _merge_similar_emb_mongo(emb: dict | None, mongo: dict | None) -> dict:
    """Embutido no produto (ERP) primeiro; Mongo completa o que faltar."""
    out: dict = {
        "id": "",
        "nome": "",
        "codigo": "",
        "marca": "",
        "modelo": "",
        "fabricante": "",
    }
    for src in (emb, mongo):
        if not src:
            continue
        for k in out:
            v = str(src.get(k) or "").strip()
            if v and not str(out.get(k) or "").strip():
                out[k] = v
        sid = str(src.get("id") or "").strip()
        if sid and not str(out.get("id") or "").strip():
            out["id"] = sid
    return out


def _resolver_similares_produto_mongo(db, client_m, p: dict, limite: int = 40) -> list[dict]:
    ids: list[str] = []
    embedded_by_id: dict[str, dict] = {}
    for k in (
        "ProdutosSimilares",
        "IdsProdutosSimilares",
        "SimilarProdutoIds",
        "Similares",
        "IdsSimilares",
        "ListaSimilares",
        "ListaProdutosSimilares",
        "ProdutoSimilares",
        "ItensSimilares",
        "VinculosSimilares",
        "ListaVinculosSimilares",
    ):
        val = p.get(k)
        if isinstance(val, list):
            for x in val:
                if x is None:
                    continue
                if isinstance(x, dict):
                    row_e = _linha_similar_de_dict_embedded(x)
                    if row_e and row_e.get("id"):
                        pid_e = str(row_e["id"]).strip()
                        embedded_by_id[pid_e] = row_e
                        ids.append(pid_e)
                    continue
                s = str(x).strip()
                if s:
                    ids.append(s)
        elif val is not None and str(val).strip():
            ids.append(str(val).strip())
    seen = set()
    uniq: list[str] = []
    for i in ids:
        if i in seen:
            continue
        seen.add(i)
        uniq.append(i)
        if len(uniq) >= limite:
            break
    if not uniq:
        for k in ("NomesSimilares", "ProdutosSimilaresNomes"):
            val = p.get(k)
            if isinstance(val, list):
                return [
                    {
                        "id": "",
                        "nome": str(x).strip(),
                        "codigo": "",
                        "marca": "",
                        "modelo": "",
                        "fabricante": "",
                    }
                    for x in val
                    if str(x).strip()
                ][:limite]
        return []
    ors = []
    for pid in uniq:
        ors.append({"Id": pid})
        try:
            ors.append({"_id": ObjectId(pid)})
        except Exception:
            pass
    proj = {
        "Nome": 1,
        "Codigo": 1,
        "CodigoNFe": 1,
        "CodigoBarras": 1,
        "EAN": 1,
        "EAN_NFe": 1,
        "Marca": 1,
        "Modelo": 1,
        "NomeModelo": 1,
        "Fabricante": 1,
        "NomeFabricante": 1,
        "Id": 1,
    }
    por_id: dict[str, dict] = {}

    def _linha_similar_de_doc(doc: dict, pid_fallback: str) -> dict:
        pid = str(doc.get("Id") or doc.get("_id") or pid_fallback)
        nome = str(doc.get("Nome") or "").strip()
        cb = (
            str(doc.get("CodigoBarras") or doc.get("EAN") or doc.get("EAN_NFe") or "").strip()
            or str(doc.get("CodigoNFe") or doc.get("Codigo") or "").strip()
        )
        marca = str(doc.get("Marca") or "").strip()
        modelo = str(doc.get("Modelo") or doc.get("NomeModelo") or "").strip()
        fabricante = str(doc.get("Fabricante") or doc.get("NomeFabricante") or "").strip()
        return {
            "id": pid,
            "nome": nome,
            "codigo": cb,
            "marca": marca,
            "modelo": modelo,
            "fabricante": fabricante,
        }

    try:
        cur = db[client_m.col_p].find({"$or": ors}, proj)
        for doc in cur:
            pid = str(doc.get("Id") or doc.get("_id"))
            ln = _linha_similar_de_doc(doc, pid)
            por_id[pid] = ln
            oid = doc.get("_id")
            if oid is not None:
                por_id[str(oid)] = ln
    except Exception:
        logger.warning("similares: falha ao resolver nomes no Mongo", exc_info=True)
    out_sim: list[dict] = []
    for i in uniq:
        i_s = str(i).strip()
        m = por_id.get(i_s) or por_id.get(i)
        if not m:
            for k2, v2 in por_id.items():
                if str(k2).strip() == i_s:
                    m = v2
                    break
        emb = embedded_by_id.get(i_s)
        merged = _merge_similar_emb_mongo(emb, m)
        if not str(merged.get("id") or "").strip():
            merged["id"] = i_s
        if not any(str(merged.get(f) or "").strip() for f in ("nome", "codigo", "marca")):
            merged["nome"] = (
                f"Produto não encontrado no espelho (id {i_s[:40]}"
                + ("…" if len(i_s) > 40 else "")
                + "). Confira o cadastro no ERP/Mongo."
            )
        out_sim.append(merged)
    return out_sim


def _montar_produto_cadastro_detalhe(db, client_m, p: dict) -> dict:
    """Enriquece o JSON de cadastro com campos usados na tela ERP (leitura Mongo)."""
    row = _produto_mongo_para_cadastro_row(p)
    pid_det = str(p.get("Id") or p.get("_id") or "")
    ov_det = ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid_det[:64]).first()
    _aplicar_produto_gestao_overlay_em_dict(row, ov_det)
    custos = _custos_compra_produto(p)
    pv = float(row.get("preco_venda") or 0)
    pc = float(custos.get("preco_custo") or 0)
    pca = float(custos.get("preco_custo_final") or 0)

    mva_rs_doc = _mongo_primeiro_float(
        p,
        (
            "ValorLucroMVA",
            "LucroMVA",
            "MVValorLucro",
            "MargemValor",
            "LucroReais",
            "ValorMargemLucro",
            "MvaValor",
        ),
    )
    mva_pct_doc = _mongo_primeiro_float(
        p,
        (
            "PercentualLucro",
            "MargemLucro",
            "MVAPercentual",
            "MvaPercentual",
            "PercentualMargem",
            "LucroPercentual",
        ),
    )
    base_mva = pca if pca > 0 else (pc if pc > 0 else None)
    mva_rs = mva_rs_doc
    if mva_rs is None and base_mva is not None and pv > 0:
        mva_rs = round(pv - base_mva, 4)
    mva_pct = mva_pct_doc
    if mva_pct is None and mva_rs is not None and base_mva and base_mva > 0:
        mva_pct = round((mva_rs / base_mva) * 100, 2)

    def _b(chaves: tuple[str, ...]) -> bool:
        v = _mongo_primeiro_bool(p, chaves)
        return bool(v) if v is not None else False

    extra = {
        "modelo": _mongo_primeiro_texto(
            p, ("Modelo", "NomeModelo", "ProdutoModelo", "DescricaoModelo")
        ),
        "fornecedor_padrao_id": _mongo_primeiro_texto(
            p, ("FornecedorID", "IdFornecedor", "FornecedorId", "IdFornecedorPadrao")
        ),
        "cadastro_inativo": bool(p.get("CadastroInativo")),
        "ocultar_nas_vendas": _b(
            ("OcultarVendas", "OcultarNasVendas", "NaoExibirVendas", "OcultoPDV", "OcultoVenda")
        ),
        "preco_custo": round(pc, 4),
        "preco_custo_com_acrescimos": round(pca, 4) if pca else round(pc, 4),
        "mva_lucro_reais": round(mva_rs, 2) if mva_rs is not None else None,
        "mva_lucro_percentual": round(mva_pct, 2) if mva_pct is not None else None,
        "comissao_vendedor_reais": _mongo_primeiro_float(
            p,
            (
                "ComissaoVendedor",
                "ValorComissaoVendedor",
                "ComissaoValor",
                "VendedorComissaoValor",
            ),
        ),
        "comissao_vendedor_percentual": _mongo_primeiro_float(
            p,
            (
                "ComissaoVendedorPercentual",
                "PercentualComissaoVendedor",
                "ComissaoPercentual",
                "PercentualComissao",
            ),
        ),
        "unidade_estoque": (
            _mongo_primeiro_texto(
                p, ("UnidadeEstoque", "UnidadeMedidaEstoque", "SiglaUnidadeEstoque")
            )
            or (row.get("unidade") or "")
        ),
        "estoque_minimo": _mongo_primeiro_float(
            p, ("EstoqueMinimo", "MinimoEstoque", "EstoqueMin", "QuantidadeMinimaEstoque")
        ),
        "estoque_maximo": _mongo_primeiro_float(
            p, ("EstoqueMaximo", "MaximoEstoque", "EstoqueMax", "QuantidadeMaximaEstoque")
        ),
        "permite_venda_estoque_negativo": _b(
            (
                "PermitirEstoqueNegativo",
                "VendaComEstoqueNegativo",
                "PermiteVendaSemEstoque",
                "EstoqueNegativo",
                "PermiteVendaEstoqueNegativo",
                "NaoEmitirAlertasPermitirVendaEstoqueNegativo",
            )
        ),
        "nao_emitir_alertas_estoque": _b(
            (
                "NaoEmitirAlertaEstoque",
                "DesativarAlertaEstoque",
                "SemAlertaEstoqueMinimo",
                "IgnorarEstoqueMinimo",
                "NaoEmitirAlertasEstoque",
                "SuprimirAlertaEstoque",
            )
        ),
        "eh_kit": _b(("Kit", "ProdutoKit", "EhKit", "EKit", "IndicaKit")),
        "calcular_custo_automaticamente": _b(
            ("CalcularCustoAutomaticamente", "CustoAutomatico", "CalculoCustoAutomatico")
        ),
        "origem_mercadoria": _mongo_primeiro_texto(
            p, ("OrigemMercadoria", "Origem", "CodigoOrigem", "IcmsOrigem", "OrigemICMS")
        ),
        "cfop_padrao": _mongo_primeiro_texto(
            p, ("CfopPadrao", "CFOP", "CodigoCfop", "Cfop", "CodigoCFOP")
        ),
        "csosn": _mongo_primeiro_texto(p, ("CSOSN", "Csosn", "CodigoCsosn")),
        "cest": _mongo_primeiro_texto(p, ("CEST", "Cest", "CodigoCEST")),
        "cst_pis_cofins": _mongo_primeiro_texto(
            p, ("CstPisCofins", "CSTPIS", "PisCofinsCST", "CstPis", "CstCofins")
        ),
        "composicao": _extrair_composicao_produto_mongo(p),
        "similares": _resolver_similares_produto_mongo(db, client_m, p),
    }
    row.update(extra)
    _merge_fiscal_overlay_sobre_row_cadastro(row, ov_det)
    ce_ov = (
        ov_det.cadastro_extras if ov_det and isinstance(ov_det.cadastro_extras, dict) else None
    )
    if ce_ov and "permite_venda_estoque_negativo" in ce_ov:
        row["permite_venda_estoque_negativo"] = bool(ce_ov.get("permite_venda_estoque_negativo"))
    if ov_det and ov_det.estoque_min_centro is not None:
        row["estoque_minimo"] = float(ov_det.estoque_min_centro)
    if ov_det and ov_det.estoque_max_centro is not None:
        row["estoque_maximo"] = float(ov_det.estoque_max_centro)

    var_rows = list(
        ProdutoMarcaVariacaoAgro.objects.filter(produto_externo_id=pid_det[:64]).order_by("ordem", "id")
    )
    row["variacoes_marca"] = _serialize_variacoes_marca_rows(var_rows)
    wv = _custo_medio_ponderado_variacoes_rows(var_rows)
    row["custo_medio_variacoes"] = round(float(wv), 4) if wv is not None else None

    if extra.get("eh_kit"):
        ck = _custo_total_kit_composicao_agro(db, client_m, p)
        row["custo_kit_composicao"] = round(float(ck), 4) if ck is not None else None
    else:
        row["custo_kit_composicao"] = None

    comp = row.get("composicao")
    if isinstance(comp, list):
        for it in comp:
            if not isinstance(it, dict):
                continue
            spid = str(it.get("produto_id") or "").strip()
            if not spid:
                it["custo_unitario_agro"] = None
                it["custo_medio_variacoes_componente"] = None
                continue
            dchild = _produto_mongo_por_id_externo(db, client_m, spid)
            cdec = _custo_unitario_agro_para_produto_id(db, client_m, spid, dchild)
            it["custo_unitario_agro"] = round(float(cdec), 4) if cdec is not None else None
            vchild = list(
                ProdutoMarcaVariacaoAgro.objects.filter(produto_externo_id=spid[:64]).order_by("ordem", "id")
            )
            wch = _custo_medio_ponderado_variacoes_rows(vchild)
            it["custo_medio_variacoes_componente"] = round(float(wch), 4) if wch is not None else None

    if ov_det:
        row["overlay_id"] = ov_det.pk
        row["lotes"] = [
            {
                "id": el.pk,
                "lote_codigo": el.lote_codigo,
                "data_validade": el.data_validade.isoformat()[:10],
                "quantidade_atual": str(el.quantidade_atual),
            }
            for el in EstoqueLote.objects.filter(overlay=ov_det).order_by(
                "data_validade", "id"
            )
        ]
    else:
        row["overlay_id"] = None
        row["lotes"] = []

    return row


@login_required(login_url="/admin/login/")
@require_POST
def api_overlay_lote_adicionar(request):
    """Cria ou atualiza um lote (mesmo código de lote no mesmo overlay)."""
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)
    pid = str(payload.get("produto_id") or "").strip()
    if not pid:
        return JsonResponse({"ok": False, "erro": "produto_id obrigatório"}, status=400)
    lote_cod = str(payload.get("lote_codigo") or payload.get("lote") or "").strip()[:100]
    if not lote_cod:
        lote_cod = "—"
    d_raw = str(payload.get("data_validade") or payload.get("validade") or "").strip()[:16]
    if not d_raw:
        return JsonResponse({"ok": False, "erro": "Data de validade obrigatória"}, status=400)
    try:
        dv = datetime.strptime(d_raw[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return JsonResponse({"ok": False, "erro": "Data inválida (use AAAA-MM-DD)"}, status=400)
    q = payload.get("quantidade", 0)
    try:
        qtd = Decimal(str(q).replace(",", ".").strip() or "0").quantize(Decimal("0.01"))
    except Exception:
        return JsonResponse({"ok": False, "erro": "Quantidade inválida"}, status=400)

    ov, _ = ProdutoGestaoOverlayAgro.objects.get_or_create(
        produto_externo_id=pid[:64],
        defaults={
            "usuario": request.user if request.user.is_authenticated else None,
        },
    )
    el, _ = EstoqueLote.objects.update_or_create(
        overlay=ov,
        lote_codigo=lote_cod,
        defaults={
            "data_validade": dv,
            "quantidade_atual": qtd,
        },
    )
    sync_overlay_validade_resumo_de_lotes(ov)
    return JsonResponse(
        {
            "ok": True,
            "lote": {
                "id": el.pk,
                "lote_codigo": el.lote_codigo,
                "data_validade": el.data_validade.isoformat()[:10],
                "quantidade_atual": str(el.quantidade_atual),
            },
        }
    )


@login_required(login_url="/admin/login/")
@require_POST
def api_overlay_lote_remover(request, lote_id: int):
    lote = get_object_or_404(EstoqueLote, id=int(lote_id))
    ov = lote.overlay
    lote.delete()
    if EstoqueLote.objects.filter(overlay=ov).exists():
        sync_overlay_validade_resumo_de_lotes(ov)
    else:
        ex = dict(ov.cadastro_extras) if isinstance(ov.cadastro_extras, dict) else {}
        ex.pop("validade", None)
        ex.pop("lote", None)
        ov.cadastro_extras = ex
        ov.save(update_fields=["cadastro_extras", "atualizado_em"])
    return JsonResponse({"ok": True})


@require_GET
def api_produtos_cadastro_compras_historico(request):
    """
    Histórico de compras (Mongo ERP) para o cadastro: agrega por produto mestre,
    variantes de Id e produtos encontrados pelos códigos de barras / fornecedor (aba Marcas).
    """
    client, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"ok": False, "erro": "Mongo indisponível", "linhas": []}, status=503)
    pid = str(request.GET.get("produto_id") or "").strip()
    if not pid:
        return JsonResponse({"ok": False, "erro": "produto_id obrigatório", "linhas": []}, status=400)
    cbs = [str(x).strip() for x in request.GET.getlist("cb") if str(x).strip()]
    cfs = [str(x).strip() for x in request.GET.getlist("cf") if str(x).strip()]
    cis = [str(x).strip() for x in request.GET.getlist("ci") if str(x).strip()]

    pid_set: set[str] = {pid[:64]}
    try:
        pid_set.update(str(x) for x in _produto_ids_variants_mongo([pid]) if x)
    except Exception:
        pass
    pid_set.update(_mongo_produto_ids_por_codigos_cadastro(db, client, cbs, cfs, cis))

    pid_list = [x for x in pid_set if x][:120]
    por_id: dict[str, dict] = {}
    for p in pid_list:
        doc = _produto_mongo_por_id_externo(db, client, p)
        if doc:
            por_id[p] = doc

    mapa = _ultimas_compras_por_produto_ids(db, pid_list, por_id, limit=30)
    merged: list[dict] = []
    for plid, rows in mapa.items():
        for r in rows or []:
            det = r.get("detalhe") if isinstance(r.get("detalhe"), dict) else {}
            merged.append(
                {
                    "fornecedor": str(r.get("fornecedor") or "—")[:200],
                    "preco_pago": r.get("preco_final"),
                    "data": str(det.get("data") or ""),
                    "produto_id": str(plid),
                }
            )
    merged.sort(key=lambda x: x.get("data") or "", reverse=True)
    seen: set[tuple] = set()
    out: list[dict] = []
    for row in merged:
        try:
            pp = round(float(row.get("preco_pago") or 0), 2)
        except (TypeError, ValueError):
            pp = 0.0
        key = (row.get("data") or "", (row.get("fornecedor") or "")[:80], pp)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "fornecedor": row.get("fornecedor") or "—",
                "data": row.get("data") or "",
                "preco_pago": pp,
            }
        )
        if len(out) >= 100:
            break

    return JsonResponse({"ok": True, "linhas": out})


@require_GET
def api_produtos_cadastro_detalhe(request, produto_id: str):
    """Detalhe completo do cadastro (Mongo / ERP) para a tela de consulta — sem saldos."""
    client, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"ok": False, "erro": "Mongo indisponível"}, status=503)
    pid = str(produto_id or "").strip()
    if not pid:
        return JsonResponse({"ok": False, "erro": "Id inválido"}, status=400)
    p = _produto_mongo_por_id_externo(db, client, pid)
    if not p:
        return JsonResponse({"ok": False, "erro": "Produto não encontrado"}, status=404)
    detalhe = _montar_produto_cadastro_detalhe(db, client, p)
    return JsonResponse({"ok": True, "produto": detalhe})


_ALLOWED_SORT_CADASTRO = frozenset(
    {"nome", "marca", "unidade", "categoria", "subcategoria", "preco_custo", "preco_venda"}
)
_MONGO_SORT_CADASTRO = {
    "nome": "Nome",
    "marca": "Marca",
    "unidade": "Unidade",
    "categoria": "NomeCategoria",
    "subcategoria": "SubGrupo",
    "preco_custo": "PrecoCusto",
    "preco_venda": "ValorVenda",
}


def _parse_sort_cadastro_request(request) -> tuple[str, int]:
    raw = str(request.GET.get("sort") or "").strip().lower()
    dir_raw = str(request.GET.get("dir") or "asc").strip().lower()
    direction = -1 if dir_raw == "desc" else 1
    if not raw or raw not in _ALLOWED_SORT_CADASTRO:
        return "nome", 1
    return raw, direction


def _sort_cadastro_rows_inplace(rows: list[dict], sort_key: str, direction: int) -> None:
    """Ordena linhas já com overlay (mesmas chaves do JSON de cadastro)."""
    if sort_key not in _ALLOWED_SORT_CADASTRO:
        sort_key = "nome"
    desc = direction < 0

    if sort_key in ("preco_custo", "preco_venda"):

        def key_num(r: dict) -> tuple[float, str]:
            try:
                v = float(r.get(sort_key) or 0)
            except (TypeError, ValueError):
                v = float("-inf")
            return (v, str(r.get("id") or ""))

        rows.sort(key=key_num, reverse=desc)
        return

    def key_txt(r: dict) -> tuple[str, str]:
        return (str(r.get(sort_key) or "").lower(), str(r.get("id") or ""))

    rows.sort(key=key_txt, reverse=desc)


@require_GET
def api_produtos_cadastro(request):
    """
    Lista / busca cadastro de produtos no Mongo (ERP), sem saldos nem médias.
    - Com `q`: usa o mesmo motor de busca do PDV.
    - Sem `q`: paginação alfabética por Nome (`pagina`, `por_pagina`).
    - `inativos=1`: inclui cadastros inativos.
    - `sort` / `dir`: ordenação (nome, marca, unidade, categoria, subcategoria, preco_custo, preco_venda; asc|desc).
    """
    client, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"ok": False, "erro": "Mongo indisponível", "produtos": []}, status=503)

    inativos = request.GET.get("inativos") in ("1", "true", "yes")
    q_raw = str(request.GET.get("q") or "").strip()

    try:
        lim_busca = int(request.GET.get("limit") or 80)
    except ValueError:
        lim_busca = 80
    lim_busca = max(1, min(lim_busca, 160))

    try:
        por_pagina = int(request.GET.get("por_pagina") or 72)
    except ValueError:
        por_pagina = 72
    por_pagina = max(1, min(por_pagina, 120))

    try:
        pagina = int(request.GET.get("pagina") or 1)
    except ValueError:
        pagina = 1
    pagina = max(1, pagina)

    sort_key, sort_direction = _parse_sort_cadastro_request(request)

    try:
        if q_raw:
            prods: list = []
            bal_cad = _parse_etiqueta_balanca_ean13_br(q_raw)
            if bal_cad:
                cod4_cad, _pc = bal_cad
                p_bal_cad = _buscar_produto_por_codigo_interno_balanca(db, client, cod4_cad)
                if p_bal_cad:
                    prods = [p_bal_cad]
            if not prods:
                prods = motor_de_busca_agro(
                    q_raw,
                    db,
                    client,
                    limit=lim_busca,
                    include_inactive=inativos,
                    regex_stage2_cap=56,
                    regex_stage3_cap=56,
                    regex_stage3b_cap=0,
                )
            vistos_cad = {str(p.get("Id") or p.get("_id")) for p in prods if p}
            extras_cad = _mongo_produtos_por_overlay_codigo_busca(q_raw, db, client, vistos_cad)
            if extras_cad:
                prods = list(extras_cad) + list(prods)
            prods = prods[:lim_busca]
            rows = [_produto_mongo_para_cadastro_row(p) for p in prods]
            _ovs = _overlay_mapa_por_ids_chunked([str(r.get("id") or "") for r in rows])
            for _r in rows:
                _aplicar_produto_gestao_overlay_em_dict(_r, _ovs.get(str(_r.get("id") or "")))
            _sort_cadastro_rows_inplace(rows, sort_key, sort_direction)
            return JsonResponse(
                {
                    "ok": True,
                    "modo": "busca",
                    "q": q_raw,
                    "produtos": rows,
                    "total_retornado": len(rows),
                    "sort": sort_key,
                    "dir": "desc" if sort_direction < 0 else "asc",
                }
            )

        filtro = {} if inativos else {"CadastroInativo": {"$ne": True}}
        skip = (pagina - 1) * por_pagina
        mongo_field = _MONGO_SORT_CADASTRO.get(sort_key, "Nome")
        cur = (
            db[client.col_p]
            .find(filtro)
            .sort(mongo_field, sort_direction)
            .skip(skip)
            .limit(por_pagina + 1)
        )
        chunk = list(cur)
        has_more = len(chunk) > por_pagina
        chunk = chunk[:por_pagina]
        rows = [_produto_mongo_para_cadastro_row(p) for p in chunk]
        _ovs2 = _overlay_mapa_por_ids_chunked([str(r.get("id") or "") for r in rows])
        for _r2 in rows:
            _aplicar_produto_gestao_overlay_em_dict(_r2, _ovs2.get(str(_r2.get("id") or "")))
        return JsonResponse(
            {
                "ok": True,
                "modo": "lista",
                "pagina": pagina,
                "por_pagina": por_pagina,
                "has_more": has_more,
                "produtos": rows,
                "sort": sort_key,
                "dir": "desc" if sort_direction < 0 else "asc",
            }
        )
    except Exception as e:
        logger.warning("api_produtos_cadastro falhou: %s", e, exc_info=True)
        return JsonResponse({"ok": False, "erro": str(e), "produtos": []}, status=500)


def _grupo_agro_para_json(g: ProdutoGrupoAgro) -> dict:
    vars_ = [
        {
            "id": v.pk,
            "marca": v.marca,
            "codigo_barras": v.codigo_barras,
            "produto_erp_id": v.produto_erp_id or "",
        }
        for v in g.variantes.all()
    ]
    return {
        "id": g.pk,
        "nome": g.nome,
        "preco_venda": str(g.preco_venda),
        "ativo": g.ativo,
        "variantes": vars_,
    }


@login_required(login_url="/admin/login/")
@require_GET
def api_produtos_grupos_listar(request):
    """Lista grupos locais (nome + preço único + variantes marca/EAN), opcional filtro `q`."""
    qs = ProdutoGrupoAgro.objects.annotate(n_variantes=Count("variantes")).order_by("nome")
    qfilt = str(request.GET.get("q") or "").strip()
    if qfilt:
        qs = qs.filter(nome__icontains=qfilt)
    grupos = [
        {
            "id": g.pk,
            "nome": g.nome,
            "preco_venda": str(g.preco_venda),
            "ativo": g.ativo,
            "n_variantes": g.n_variantes,
        }
        for g in qs[:500]
    ]
    return JsonResponse({"ok": True, "grupos": grupos})


@login_required(login_url="/admin/login/")
@require_GET
def api_produtos_grupo_obter(request, pk: int):
    g = get_object_or_404(ProdutoGrupoAgro.objects.prefetch_related("variantes"), pk=pk)
    return JsonResponse({"ok": True, "grupo": _grupo_agro_para_json(g)})


@login_required(login_url="/admin/login/")
@require_POST
def api_produtos_grupo_salvar(request):
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)

    nome = str(payload.get("nome") or "").strip()[:300]
    if not nome:
        return JsonResponse({"ok": False, "erro": "Informe o nome do produto."}, status=400)

    raw_preco = str(payload.get("preco_venda") or "").strip().replace(",", ".")
    try:
        preco_venda = Decimal(raw_preco)
    except Exception:
        return JsonResponse({"ok": False, "erro": "Preço de venda inválido."}, status=400)
    if preco_venda < 0 or preco_venda > Decimal("99999999.99"):
        return JsonResponse({"ok": False, "erro": "Preço de venda fora do intervalo permitido."}, status=400)

    ativo = payload.get("ativo", True)
    if isinstance(ativo, str):
        ativo = ativo.strip().lower() in ("1", "true", "yes", "on")
    ativo = bool(ativo)

    variantes = payload.get("variantes")
    if not isinstance(variantes, list):
        return JsonResponse({"ok": False, "erro": "O campo variantes deve ser uma lista."}, status=400)

    cleaned = []
    marcas_seen: set[str] = set()
    cods_seen: set[str] = set()
    for i, v in enumerate(variantes):
        if not isinstance(v, dict):
            return JsonResponse({"ok": False, "erro": "Cada variante deve ser um objeto."}, status=400)
        marca = str(v.get("marca") or "").strip()[:120]
        cb = str(v.get("codigo_barras") or "").strip().replace(" ", "")[:80]
        if not marca:
            return JsonResponse(
                {"ok": False, "erro": f"Linha {i + 1}: informe a marca."},
                status=400,
            )
        if not cb:
            return JsonResponse(
                {"ok": False, "erro": f"Linha {i + 1}: informe o código de barras."},
                status=400,
            )
        mk = marca.casefold()
        if mk in marcas_seen:
            return JsonResponse(
                {"ok": False, "erro": f'Marca repetida no formulário: "{marca}".'},
                status=400,
            )
        marcas_seen.add(mk)
        if cb in cods_seen:
            return JsonResponse(
                {"ok": False, "erro": "Código de barras repetido no formulário."},
                status=400,
            )
        cods_seen.add(cb)
        erp_id = str(v.get("produto_erp_id") or "").strip()[:64]
        vid_raw = v.get("id")
        vid = None
        if vid_raw is not None and str(vid_raw).strip() != "":
            try:
                vid = int(vid_raw)
            except (TypeError, ValueError):
                return JsonResponse({"ok": False, "erro": f"Linha {i + 1}: id de variante inválido."}, status=400)
        cleaned.append(
            {
                "id": vid,
                "marca": marca,
                "codigo_barras": cb,
                "produto_erp_id": erp_id,
            }
        )

    gid = payload.get("id")
    try:
        gid_int = int(gid) if gid is not None and str(gid).strip() != "" else None
    except (TypeError, ValueError):
        return JsonResponse({"ok": False, "erro": "Id do grupo inválido."}, status=400)

    try:
        with transaction.atomic():
            if gid_int:
                g = get_object_or_404(ProdutoGrupoAgro, pk=gid_int)
            else:
                g = ProdutoGrupoAgro()
                if request.user.is_authenticated:
                    g.usuario = request.user
            g.nome = nome
            g.preco_venda = preco_venda
            g.ativo = ativo
            g.save()

            kept_ids: list[int] = []
            for cv in cleaned:
                if cv["id"]:
                    var = ProdutoGrupoVarianteAgro.objects.filter(pk=cv["id"], grupo=g).first()
                    if not var:
                        return JsonResponse({"ok": False, "erro": "Variante não encontrada neste grupo."}, status=400)
                    var.marca = cv["marca"]
                    var.codigo_barras = cv["codigo_barras"]
                    var.produto_erp_id = cv["produto_erp_id"]
                    var.save()
                    kept_ids.append(var.pk)
                else:
                    var = ProdutoGrupoVarianteAgro.objects.create(
                        grupo=g,
                        marca=cv["marca"],
                        codigo_barras=cv["codigo_barras"],
                        produto_erp_id=cv["produto_erp_id"],
                    )
                    kept_ids.append(var.pk)
            ProdutoGrupoVarianteAgro.objects.filter(grupo=g).exclude(pk__in=kept_ids).delete()
    except IntegrityError:
        return JsonResponse(
            {
                "ok": False,
                "erro": "Não foi possível salvar: marca já usada neste grupo ou código de barras já existe em outro cadastro.",
            },
            status=400,
        )

    g.refresh_from_db()
    g = ProdutoGrupoAgro.objects.prefetch_related("variantes").get(pk=g.pk)
    return JsonResponse({"ok": True, "grupo": _grupo_agro_para_json(g)})


@login_required(login_url="/admin/login/")
@require_POST
def api_produtos_grupo_excluir(request, pk: int):
    g = get_object_or_404(ProdutoGrupoAgro, pk=pk)
    g.delete()
    return JsonResponse({"ok": True})


# --- APIs DE ESTOQUE E PEDIDO ---
@require_POST
def api_login_mobile(request):
    if PerfilUsuario.objects.filter(senha_rapida=request.POST.get("pin")).exists():
        request.session["mobile_auth"] = True
        return JsonResponse({"ok": True})
    return JsonResponse({"ok": False}, status=403)


@require_POST
def api_ajustar_estoque(request):
    pin = request.POST.get("pin")
    if (pin == "SESSAO" and request.session.get("mobile_auth")) or PerfilUsuario.objects.filter(
        senha_rapida=pin
    ).exists():
        try:
            empresa = Empresa.objects.filter(nome_fantasia="Agro Mais").first()
            AjusteRapidoEstoque.objects.create(
                empresa=empresa,
                produto_externo_id=request.POST.get("produto_id"),
                deposito=request.POST.get("deposito", "centro"),
                nome_produto=request.POST.get("nome_produto"),
                saldo_erp_referencia=Decimal(request.POST.get("saldo_atual", "0")),
                saldo_informado=Decimal(request.POST.get("novo_saldo", "0")),
                origem=OrigemAjusteEstoque.AJUSTE_PIN,
                usuario=request.user if request.user.is_authenticated else None,
            )
            _invalidar_caches_apos_ajuste_pin()
            return JsonResponse({"ok": True})
        except Exception as e:
            return JsonResponse({"ok": False, "erro": str(e)})
    return JsonResponse({"ok": False, "erro": "PIN INCORRETO"}, status=403)


def _json_legivel(val):
    if isinstance(val, (dict, list)):
        return json.dumps(val, ensure_ascii=False)
    return str(val)


def _produto_mongo_por_id_externo(db, client_m, pid_str):
    if db is None or client_m is None:
        return None
    pid_str = str(pid_str or "").strip()
    if not pid_str:
        return None
    ors = [{"Id": pid_str}]
    try:
        ors.append({"_id": ObjectId(pid_str)})
    except Exception:
        pass
    return db[client_m.col_p].find_one({"$or": ors})


def _parece_object_id_mongo(s):
    s = str(s or "").strip()
    if len(s) != 24:
        return False
    return all(c in "0123456789abcdefABCDEF" for c in s)


def _desembrulhar_texto_json_recursivo(val, depth=0):
    if depth > 5:
        return val
    if isinstance(val, (dict, list)):
        return val
    s = str(val).strip()
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        try:
            inner = json.loads(s)
            return _desembrulhar_texto_json_recursivo(inner, depth + 1)
        except Exception:
            return s
    return s


def _texto_heuristico_resposta_pedido_erp(res) -> str:
    """
    Extrai texto legível da resposta do Pedidos/Salvar (dict com 'texto' JSON escapado, etc.)
    para heurística de recusa de negócio — evita str(dict) que não contém frases pesquisáveis.
    """
    chunks = []

    def add(s):
        t = str(s or "").strip()
        if t:
            chunks.append(t)

    def walk(node, depth):
        if depth > 14:
            return
        if node is None:
            return
        if isinstance(node, str):
            s = node.strip()
            add(s)
            if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
                try:
                    walk(json.loads(s), depth + 1)
                except Exception:
                    pass
            elif (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
                try:
                    walk(json.loads(s), depth + 1)
                except Exception:
                    pass
            return
        if isinstance(node, dict):
            for key in (
                "texto",
                "Texto",
                "mensagem",
                "Mensagem",
                "message",
                "Message",
                "erro",
                "Erro",
                "error",
                "Error",
                "detalhes",
                "Detalhes",
            ):
                if key in node and node[key] is not None:
                    walk(node[key], depth + 1)
            return
        if isinstance(node, list):
            for it in node[:80]:
                walk(it, depth + 1)

    walk(res, 0)
    return " ".join(chunks)


def _mensagem_pedido_erp_indica_recusa_negocio(texto_flat: str) -> bool:
    s = str(texto_flat or "").strip().lower()
    folded = "".join(
        c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn"
    )
    markers = (
        "nao foi possivel",
        "e preciso informar",
        "produto valido",
        "informar algum produto",
        "informar produtos",
        "falha ao salvar",
        "status da venda",
        "deve ser valido",
        "ao informar um status",
        "mesmo deve ser valido",
        "nao e valido",
        "invalido",
        "nao permitid",
        "nao pode ser",
        "erro ao salvar",
        "in formar",
        "plano de contas informado",
        "informar o plano",
        "salvar o pedido",
    )
    return any(m in folded for m in markers)


def _mensagem_pedido_erp_indica_falha_salvar_pedido_generica(texto_flat: str) -> bool:
    """Ex.: 'NÃO FOI POSSÍVEL SALVAR O PEDIDO' (sem detalhe de plano/status)."""
    folded, compact = _pedido_erp_texto_fold_e_compact(texto_flat)
    if "naofoipossivelsalvaropedido" in compact:
        return True
    if "nao foi possivel" in folded and "salvar" in folded and "pedido" in folded:
        return True
    return False


def _pedido_erp_texto_fold_e_compact(texto_flat: str) -> tuple[str, str]:
    folded = "".join(
        c
        for c in unicodedata.normalize("NFD", str(texto_flat or "").lower())
        if unicodedata.category(c) != "Mn"
    )
    compact = "".join(folded.split())
    return folded, compact


def _mensagem_pedido_erp_indica_erro_localizar_plano(texto_flat: str) -> bool:
    folded, compact = _pedido_erp_texto_fold_e_compact(texto_flat)
    if "plano" not in folded or "cont" not in folded:
        return False
    return "localizar" in folded or "lovalizar" in folded or "informado" in folded


def _mensagem_pedido_erp_indica_erro_plano_contas(texto_flat: str) -> bool:
    """Localizar / informar plano (inclui 'IN FORMAR' → compact ``informar``)."""
    folded, compact = _pedido_erp_texto_fold_e_compact(texto_flat)
    tem_plano_contas = "planodecontas" in compact or (
        "plano" in folded and "contas" in folded
    )
    if not tem_plano_contas:
        return False
    if "localizar" in folded or "lovalizar" in folded:
        return True
    if "naofoipossivel" in compact:
        return True
    if "informar" in compact and "plano" in folded:
        return True
    return False


def _mensagem_pedido_erp_indica_recusa_ou_erro_plano(flat_erp: str) -> bool:
    return (
        _mensagem_pedido_erp_indica_recusa_negocio(flat_erp)
        or _mensagem_pedido_erp_indica_erro_plano_contas(flat_erp)
        or _mensagem_pedido_erp_indica_falha_salvar_pedido_generica(flat_erp)
    )


def _mensagem_pedido_erp_indica_retry_flat_apos_embutido(flat_erp: str) -> bool:
    """Plano inválido ou corpo rejeitado (mensagem genérica) após JSON com plano embutido."""
    return _mensagem_pedido_erp_indica_erro_plano_contas(
        flat_erp
    ) or _mensagem_pedido_erp_indica_falha_salvar_pedido_generica(flat_erp)


def _pedido_payload_variante_sem_plano_cabecalho(payload_camel: dict) -> dict:
    out = copy.deepcopy(payload_camel)
    for k in list(out.keys()):
        if k == "items":
            continue
        if str(k).lower().startswith("plano"):
            del out[k]
    return out


def _pedido_payload_variante_itens_plano_so_id(payload_camel: dict) -> dict:
    """Remove plano do cabeçalho e remove só os campos de texto do plano nas linhas (mantém *ID* / *Id*)."""
    out = _pedido_payload_variante_sem_plano_cabecalho(payload_camel)
    for row in out.get("items") or []:
        if not isinstance(row, dict):
            continue
        for ik in list(row.keys()):
            lki = str(ik).lower()
            if lki in ("planodeconta", "planoconta", "planodecontas"):
                del row[ik]
    return out


def _pedido_extrair_id_plano_de_dict(d: dict) -> str:
    for key in ("planoDeContaID", "planoContaID", "planoDeContaId", "planoContaId"):
        v = d.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()[:40]
    return ""


def _pedido_extrair_texto_plano_de_dict(d: dict) -> str:
    for key in ("planoDeConta", "planoConta"):
        v = d.get(key)
        if v is None or isinstance(v, dict):
            continue
        s = str(v).strip()
        if s:
            return s[:500]
    return ""


def _pedido_remover_chaves_plano_dict(d: dict) -> None:
    for k in list(d.keys()):
        if str(k).lower().startswith("plano"):
            del d[k]


def _pedido_plano_vai_nos_itens() -> bool:
    """Swagger não lista plano nas linhas; use True só se o WL exigir por item."""
    return bool(getattr(settings, "VENDA_ERP_PEDIDOS_SALVAR_PLANO_NOS_ITENS", False))


def _pedido_payload_flat_com_texto_plano_uniforme(base_flat: dict, texto: str) -> dict:
    """
    ``planoDeConta`` string no cabeçalho; nas linhas só se ``VENDA_ERP_PEDIDOS_SALVAR_PLANO_NOS_ITENS``.
    """
    p = copy.deepcopy(base_flat)
    t = (texto or "").strip()[:500]
    _pedido_remover_chaves_plano_dict(p)
    if t:
        p["planoDeConta"] = t
    nos = _pedido_plano_vai_nos_itens()
    for row in p.get("items") or []:
        if isinstance(row, dict):
            _pedido_remover_chaves_plano_dict(row)
            if nos and t:
                row["planoDeConta"] = t
    return p


def _pedido_montar_plano_conta_objeto_retorno_busca(doc: dict | None) -> dict | None:
    """
    Alinha ao schema WL ``PlanoDeContaRetornoBusca`` (camelCase no JSON).
    Montado a partir do documento Mongo ``DtoPlanoDeConta``.
    """
    if not doc:
        return None
    nome = str(doc.get("Nome") or "").strip()
    if not nome:
        return None
    o: dict = {"nome": nome[:500]}
    oid = doc.get("_id")
    if oid is not None:
        if isinstance(oid, ObjectId):
            o["id"] = str(oid)
        else:
            s = str(oid).strip()
            if s:
                o["id"] = s[:40]
    cn = doc.get("CodigoNatureza")
    if cn is not None and str(cn).strip() != "":
        try:
            o["codigoNatureza"] = int(str(cn).strip())
        except ValueError:
            pass
    tc = str(doc.get("TipoDeConta") or "").strip()
    if tc:
        o["tipoDeConta"] = tc[:40]
    o["despesa"] = bool(doc.get("EhDespesa", False))
    gdre = str(doc.get("GrupoDRE") or "").strip()
    if gdre:
        o["grupoDRE"] = gdre[:200]
    hier = str(doc.get("Hierarquia") or "").strip()
    if hier:
        o["hierarquia"] = hier[:80]
    cc = doc.get("CostCenter")
    if isinstance(cc, dict):
        cnm = str(cc.get("Name") or "").strip()
        if cnm:
            o["centroDeCusto"] = cnm[:200]
    gl = doc.get("GroupEntry")
    if isinstance(gl, dict):
        gnm = str(gl.get("Name") or "").strip()
        if gnm:
            o["grupoLancamento"] = gnm[:200]
    return o


def _pedido_payload_com_plano_objeto_retorno_busca(
    base_flat: dict, doc_plano_mestre: dict | None
) -> dict | None:
    obj = _pedido_montar_plano_conta_objeto_retorno_busca(doc_plano_mestre)
    if not obj:
        return None
    p = copy.deepcopy(base_flat)
    _pedido_remover_chaves_plano_dict(p)
    p["planoDeConta"] = copy.deepcopy(obj)
    if _pedido_plano_vai_nos_itens():
        for row in p.get("items") or []:
            if isinstance(row, dict):
                _pedido_remover_chaves_plano_dict(row)
                row["planoDeConta"] = copy.deepcopy(obj)
    else:
        for row in p.get("items") or []:
            if isinstance(row, dict):
                _pedido_remover_chaves_plano_dict(row)
    return p


def _pedido_montar_objeto_plano_dto(
    pid: str, txt: str, doc_plano_mestre: dict | None
) -> dict:
    o: dict = {"Id": str(pid)[:40], "Nome": str(txt)[:500]}
    if not doc_plano_mestre:
        return o
    gid = str(doc_plano_mestre.get("GrupoDREID") or "").strip()
    if gid:
        o["GrupoDREID"] = gid
    h = str(doc_plano_mestre.get("Hierarquia") or "").strip()
    if h:
        o["Hierarquia"] = h
    pai = str(doc_plano_mestre.get("PlanoPaiId") or "").strip()
    if pai:
        o["PlanoPaiId"] = pai
    return o


def _pedido_payload_variante_plano_aninhado_dto(
    payload_camel: dict, *, doc_plano_mestre: dict | None = None
) -> dict | None:
    """
    Alguns DTOs .NET esperam ``PlanoDeConta`` como objeto (Id + Nome), não string solta + *ID* paralelos.
    ``doc_plano_mestre`` vem do Mongo (GrupoDREID, Hierarquia, PlanoPaiId) quando disponível.
    """
    out = copy.deepcopy(payload_camel)
    pid_root = _pedido_extrair_id_plano_de_dict(out)
    txt_root = _pedido_extrair_texto_plano_de_dict(out)
    if not pid_root:
        return None
    if not txt_root:
        txt_root = pid_root
    _pedido_remover_chaves_plano_dict(out)
    out["planoDeConta"] = _pedido_montar_objeto_plano_dto(pid_root, txt_root, doc_plano_mestre)

    for row in out.get("items") or []:
        if not isinstance(row, dict):
            continue
        if _pedido_plano_vai_nos_itens():
            pid = _pedido_extrair_id_plano_de_dict(row) or pid_root
            txt = _pedido_extrair_texto_plano_de_dict(row) or txt_root
            if not txt:
                txt = txt_root
            _pedido_remover_chaves_plano_dict(row)
            row["planoDeConta"] = _pedido_montar_objeto_plano_dto(pid, txt, doc_plano_mestre)
        else:
            _pedido_remover_chaves_plano_dict(row)
    return out


def _pedido_plano_conta_texto_erp(integ_obj) -> str:
    """Texto do plano de contas para Pedidos/Salvar (cabeçalho e linhas)."""
    if integ_obj:
        raw = str(getattr(integ_obj, "pedido_plano_conta", None) or "").strip()
        if raw:
            return raw
    return str(getattr(settings, "VENDA_ERP_PEDIDO_PLANO_CONTA", "") or "").strip()


def _pedido_plano_conta_id_config_erp(integ_obj) -> str:
    """PlanoDeContaID opcional (.env ou Integração ERP)."""
    if integ_obj:
        raw = str(getattr(integ_obj, "pedido_plano_conta_id", None) or "").strip()
        if raw:
            return raw
    return str(getattr(settings, "VENDA_ERP_PEDIDO_PLANO_CONTA_ID", "") or "").strip()


def _linha_item_pedido_erp(
    db, client_m, item: dict, *, plano_conta: str = "", plano_conta_id: str = ""
) -> dict | None:
    pid = str(item.get("id") or "").strip()
    if not pid:
        return None
    qtd = float(item.get("qtd") or 0)
    vu = float(item.get("preco") or 0)
    nome = str(item.get("nome") or "").strip()
    p_doc = _produto_mongo_por_id_externo(db, client_m, pid)
    produto_id = pid
    codigo = pid
    codigo_barras = ""
    if p_doc:
        bid = p_doc.get("Id")
        if bid is not None and str(bid).strip():
            produto_id = str(bid).strip()
        else:
            produto_id = str(p_doc.get("_id") or pid)
        codigo = (
            str(p_doc.get("CodigoNFe") or p_doc.get("Codigo") or "").strip() or produto_id
        )
        codigo_barras = str(p_doc.get("CodigoBarras") or p_doc.get("EAN_NFe") or "").strip()
        if _parece_object_id_mongo(produto_id) and codigo and not _parece_object_id_mongo(codigo):
            produto_id = codigo
    linha = {
        "produtoID": produto_id,
        "codigo": codigo,
        "unidade": "UN",
        "descricao": nome,
        "quantidade": qtd,
        "valorUnitario": vu,
        "valorTotal": round(qtd * vu, 2),
    }
    if codigo_barras:
        linha["codigoBarras"] = codigo_barras
    if _pedido_plano_vai_nos_itens():
        pc = str(plano_conta or "").strip()
        if pc:
            pcc = pc[:500]
            linha["planoDeConta"] = pcc
            if not getattr(settings, "VENDA_ERP_PEDIDOS_SALVAR_PLANO_SO_TEXTO_SWAGGER", True):
                linha["planoConta"] = pcc
        pid = str(plano_conta_id or "").strip()
        if pid and not getattr(settings, "VENDA_ERP_PEDIDOS_SALVAR_PLANO_SO_TEXTO_SWAGGER", True):
            pids = pid[:40]
            linha["planoDeContaID"] = pids
            linha["planoContaID"] = pids
            linha["planoDeContaId"] = pids
            linha["planoContaId"] = pids
    return linha


_PEDIDO_SALVAR_TOP_CAMEL_PARA_PASCAL = {
    "statusSistema": "StatusSistema",
    "statusDaVenda": "StatusDaVenda",
    "cliente": "Cliente",
    "data": "Data",
    "origemVenda": "OrigemVenda",
    "empresa": "Empresa",
    "deposito": "Deposito",
    "vendedor": "Vendedor",
    "items": "Items",
    "depositoID": "DepositoID",
    "empresaID": "EmpresaID",
    "clienteID": "ClienteID",
    "cpfCnpj": "CpfCnpj",
    "planoDeConta": "PlanoDeConta",
    "planoConta": "PlanoConta",
    "planoDeContaID": "PlanoDeContaID",
    "planoContaID": "PlanoContaID",
    "planoDeContaId": "PlanoDeContaId",
    "planoContaId": "PlanoContaId",
    "formaPagamento": "FormaPagamento",
    "formaPagamentoID": "FormaPagamentoID",
    "valorFinal": "ValorFinal",
    "pagamentos": "Pagamentos",
}

_PEDIDO_SALVAR_PAGAMENTO_CAMEL_PARA_PASCAL = {
    "formaPagamento": "FormaPagamento",
    "descricaoPagamento": "DescricaoPagamento",
    "valorPagamento": "ValorPagamento",
    "bandeiraCartao": "BandeiraCartao",
    "numeroTerminal": "NumeroTerminal",
    "dataTransacao": "DataTransacao",
    "credenciadoraCartao": "CredenciadoraCartao",
    "credenciadoraCNPJ": "CredenciadoraCNPJ",
    "cV_NSU": "CV_NSU",
    "tipoIntegracao": "TipoIntegracao",
    "condicaoPagamento": "CondicaoPagamento",
    "parcelas": "Parcelas",
    "periodoParcelas": "PeriodoParcelas",
    "adiantamento": "Adiantamento",
    "quitar": "Quitar",
}
_PEDIDO_SALVAR_ITEM_CAMEL_PARA_PASCAL = {
    "produtoID": "ProdutoID",
    "codigo": "Codigo",
    "unidade": "Unidade",
    "descricao": "Descricao",
    "quantidade": "Quantidade",
    "valorUnitario": "ValorUnitario",
    "valorTotal": "ValorTotal",
    "codigoBarras": "CodigoBarras",
    "planoDeConta": "PlanoDeConta",
    "planoConta": "PlanoConta",
    "planoDeContaID": "PlanoDeContaID",
    "planoContaID": "PlanoContaID",
    "planoDeContaId": "PlanoDeContaId",
    "planoContaId": "PlanoContaId",
}

# Propriedades internas de ``planoDeConta`` no formato PlanoDeContaRetornoBusca (camel → Pascal).
_PLANO_RETORNO_BUSCA_JSON_CAMEL_PARA_PASCAL = {
    "nome": "Nome",
    "id": "Id",
    "codigoNatureza": "CodigoNatureza",
    "tipoDeConta": "TipoDeConta",
    "despesa": "Despesa",
    "grupoDRE": "GrupoDRE",
    "hierarquia": "Hierarquia",
    "centroDeCusto": "CentroDeCusto",
    "grupoLancamento": "GrupoLancamento",
}


def _pedido_payload_camel_para_pascal(payload: dict) -> dict:
    """Mesmo conteúdo do Pedidos/Salvar com chaves PascalCase (compat .NET legado)."""
    out: dict = {}
    for k, v in payload.items():
        nk = _PEDIDO_SALVAR_TOP_CAMEL_PARA_PASCAL.get(k) or (
            (k[0].upper() + k[1:]) if k and k[0].islower() else k
        )
        if nk == "Items" and isinstance(v, list):
            linhas = []
            for row in v:
                if not isinstance(row, dict):
                    linhas.append(row)
                    continue
                linhas.append(
                    {
                        _PEDIDO_SALVAR_ITEM_CAMEL_PARA_PASCAL.get(ik)
                        or ((ik[0].upper() + ik[1:]) if ik and ik[0].islower() else ik): iv
                        for ik, iv in row.items()
                    }
                )
            out[nk] = linhas
        elif nk == "Pagamentos" and isinstance(v, list):
            pag_list = []
            for row in v:
                if not isinstance(row, dict):
                    pag_list.append(row)
                    continue
                pag_list.append(
                    {
                        _PEDIDO_SALVAR_PAGAMENTO_CAMEL_PARA_PASCAL.get(ik)
                        or ((ik[0].upper() + ik[1:]) if ik and ik[0].islower() else ik): iv
                        for ik, iv in row.items()
                    }
                )
            out[nk] = pag_list
        else:
            out[nk] = v
    return out


def _pedido_payload_plano_retorno_busca_tudo_pascal(camel_body: dict) -> dict:
    """PlanoDeConta aninhado em Pascal + restante do pedido em Pascal (Pedidos/Salvar)."""
    p = copy.deepcopy(camel_body)
    for loc in [p] + [r for r in p.get("items") or [] if isinstance(r, dict)]:
        v = loc.get("planoDeConta")
        if isinstance(v, dict) and "nome" in v:
            loc["planoDeConta"] = {
                _PLANO_RETORNO_BUSCA_JSON_CAMEL_PARA_PASCAL.get(
                    k,
                    (k[0].upper() + k[1:]) if k and k[0].islower() else k,
                ): val
                for k, val in v.items()
            }
    return _pedido_payload_camel_para_pascal(p)


def _decimal_item_pedido(val, default="0"):
    try:
        return Decimal(str(val))
    except Exception:
        return Decimal(default)


def _erp_resposta_para_json(res):
    if res is None:
        return None
    if isinstance(res, (dict, list, bool, int)):
        return res
    if isinstance(res, float):
        return res
    if isinstance(res, str):
        return {"texto": res[:8000]}
    if isinstance(res, bytes):
        try:
            return {"texto": res.decode("utf-8", errors="replace")[:8000]}
        except Exception:
            return {"texto": str(res)[:8000]}
    return {"texto": str(res)[:8000]}


def _cliente_id_e_valido_para_erp(cid) -> bool:
    s = str(cid or "").strip()
    if not s:
        return False
    sl = s.lower()
    if sl.startswith("local:") or sl.startswith("erp-doc:"):
        return False
    return True


def _persistir_venda_agro(
    request,
    data,
    raw_itens,
    erp_http_status,
    erp_resposta_raw,
    enviado_erp_com_sucesso,
    *,
    erp_sync_status: str | None = None,
):
    """
    Grava venda + itens no banco local (sempre que houve tentativa com itens válidos ao ERP).
    """
    user_label = ""
    u = getattr(request, "user", None)
    if u is not None and getattr(u, "is_authenticated", False):
        user_label = str(u.get_username() if hasattr(u, "get_username") else u.pk)[:150]

    cliente = (data.get("cliente") or "").strip() or "CONSUMIDOR NÃO IDENTIFICADO..."
    cid = str(data.get("cliente_id") or data.get("ClienteID") or "").strip()
    if not _cliente_id_e_valido_para_erp(cid):
        cid = ""
    doc = str(data.get("cliente_documento") or data.get("CpfCnpj") or "").strip()
    forma = _forma_pagamento_rotulo_sem_valor_moeda(
        str(data.get("forma_pagamento") or data.get("formaPagamento") or "")
    ).strip()[:80]

    itens_payload = []
    total = Decimal("0")
    for i in raw_itens:
        if not isinstance(i, dict):
            continue
        qtd = _decimal_item_pedido(i.get("qtd"), "0")
        vu = _decimal_item_pedido(i.get("preco"), "0")
        vt = (qtd * vu).quantize(Decimal("0.01"))
        total += vt
        itens_payload.append(
            {
                "produto_id_externo": str(i.get("id") or "").strip()[:64],
                "codigo": str(i.get("codigo") or i.get("Codigo") or "").strip()[:120],
                "descricao": str(i.get("nome") or "").strip()[:500],
                "quantidade": qtd,
                "valor_unitario": vu,
                "valor_total": vt,
            }
        )

    resp_json = _erp_resposta_para_json(erp_resposta_raw)
    st = erp_http_status if erp_http_status is not None and erp_http_status > 0 else None
    sessao = _obter_sessao_caixa_aberta(request)
    sync_st = (erp_sync_status or "").strip()
    if not sync_st:
        sync_st = (
            VendaAgro.ErpSyncStatus.ACEITO
            if enviado_erp_com_sucesso
            else VendaAgro.ErpSyncStatus.FALHA_COMUNICACAO
        )

    with transaction.atomic():
        v = VendaAgro.objects.create(
            cliente_nome=cliente[:300],
            cliente_id_erp=cid[:32],
            cliente_documento=re.sub(r"\D", "", doc)[:20],
            total=total.quantize(Decimal("0.01")),
            forma_pagamento=forma,
            erp_sync_status=sync_st,
            enviado_erp=bool(enviado_erp_com_sucesso),
            erp_http_status=st,
            erp_resposta=resp_json,
            usuario_registro=user_label,
            sessao_caixa=sessao,
            estoque_baixa_agro_aplicada=False,
        )
        for it in itens_payload:
            ItemVendaAgro.objects.create(venda=v, **it)

        if getattr(settings, "PDV_BAIXA_ESTOQUE_AGRO_NA_VENDA", True):
            cm, dbe = obter_conexao_mongo()
            # PyMongo: Database/MongoClient não implementam __bool__ — usar "is not None".
            if cm is not None and dbe is not None:
                dep_v = getattr(settings, "PDV_VENDA_ESTOQUE_DEPOSITO", "centro") or "centro"
                if dep_v not in ("centro", "vila"):
                    dep_v = "centro"
                try:
                    r_baixa = aplicar_baixa_estoque_venda_agro(
                        db=dbe,
                        client_m=cm,
                        venda=v,
                        deposito=dep_v,
                        usuario_label=user_label,
                        usuario_django=request.user
                        if getattr(request, "user", None) is not None
                        and getattr(request.user, "is_authenticated", False)
                        else None,
                    )
                    if r_baixa.get("ok"):
                        v.estoque_baixa_agro_aplicada = True
                        v.save(update_fields=["estoque_baixa_agro_aplicada"])
                        _invalidar_caches_apos_ajuste_pin()
                    elif r_baixa.get("erros"):
                        logger.warning(
                            "Venda %s: baixa estoque Agro incompleta: %s",
                            v.pk,
                            r_baixa.get("erros"),
                        )
                except Exception:
                    logger.exception(
                        "Venda %s: falha na baixa estoque Agro (venda permanece gravada).",
                        v.pk,
                    )
            else:
                logger.warning(
                    "Venda %s: Mongo indisponível — baixa estoque Agro não aplicada.",
                    v.pk,
                )
    return v


def _atualizar_venda_agro_resposta_erp(v: VendaAgro, erp_http_status, erp_resposta_raw, erp_sync: str):
    sucesso = erp_sync == VendaAgro.ErpSyncStatus.ACEITO
    st = erp_http_status if erp_http_status is not None and erp_http_status > 0 else None
    v.erp_sync_status = erp_sync
    v.enviado_erp = bool(sucesso)
    v.erp_http_status = st
    v.erp_resposta = _erp_resposta_para_json(erp_resposta_raw)
    v.save(
        update_fields=[
            "erp_sync_status",
            "enviado_erp",
            "erp_http_status",
            "erp_resposta",
        ]
    )


def _pdv_pedido_linhas_e_valor_final(data: dict, *, client_m, db):
    """
    Monta linhas do pedido ERP e valor final (mesma lógica usada em Pedidos/Salvar).
    Retorno: (JsonResponse erro | None, list | None, float | None).
    """
    raw_itens = data.get("itens", [])
    if not isinstance(raw_itens, list):
        raw_itens = []

    integ = (
        IntegracaoERP.objects.filter(ativo=True, tipo_erp="venda_erp")
        .order_by("-pk")
        .first()
    )

    dep_id = ""
    emp_id = ""
    if db is not None and client_m is not None:
        est = db[client_m.col_e].find_one({"DepositoID": client_m.DEPOSITO_CENTRO})
        if est:
            dep_id = str(est.get("DepositoID") or "")
            emp_id = str(est.get("EmpresaID") or "")

    plano_txt_cfg = _pedido_plano_conta_texto_erp(integ)
    plano_id_cfg = _pedido_plano_conta_id_config_erp(integ)
    plano_txt, plano_id = resolver_plano_conta_para_pedido_erp(
        db,
        texto_config=plano_txt_cfg,
        id_config=plano_id_cfg or None,
        empresa_id=emp_id or None,
    )
    _plano_so_txt = getattr(settings, "VENDA_ERP_PEDIDOS_SALVAR_PLANO_SO_TEXTO_SWAGGER", True)
    _plano_itens = _pedido_plano_vai_nos_itens()
    logger.info(
        "Pedidos/Salvar: plano resolvido — texto=%r | id_mongo=%r | no JSON vai %s | plano nos itens=%s",
        plano_txt,
        plano_id,
        "apenas planoDeConta (string)" if _plano_so_txt else "planoDeConta + planoConta e campos *ID*",
        _plano_itens,
    )

    linhas = []
    for i in raw_itens:
        if not isinstance(i, dict):
            continue
        linha = _linha_item_pedido_erp(
            db, client_m, i, plano_conta=plano_txt, plano_conta_id=plano_id
        )
        if linha:
            linhas.append(linha)

    if not linhas:
        return (
            JsonResponse(
                {
                    "ok": False,
                    "erro": "Nenhum item válido para enviar (verifique IDs dos produtos).",
                },
                status=400,
            ),
            None,
            None,
        )

    valor_final = round(sum(float(r.get("valorTotal") or 0) for r in linhas), 2)
    return None, linhas, valor_final


def _fluxo_enviar_pedido_erp_interno(request, data: dict, *, client_m, db):
    """
    Monta itens, chama Pedidos/Salvar (com retry Pascal) e devolve resultado.
    Retorno: (JsonResponse de erro imediato | None, dict resultado | None).
    """
    raw_itens = data.get("itens", [])
    if not isinstance(raw_itens, list):
        raw_itens = []

    err_early, linhas, valor_final = _pdv_pedido_linhas_e_valor_final(data, client_m=client_m, db=db)
    if err_early is not None:
        return err_early, None

    integ = (
        IntegracaoERP.objects.filter(ativo=True, tipo_erp="venda_erp")
        .order_by("-pk")
        .first()
    )

    dep_id = ""
    emp_id = ""
    if db is not None and client_m is not None:
        est = db[client_m.col_e].find_one({"DepositoID": client_m.DEPOSITO_CENTRO})
        if est:
            dep_id = str(est.get("DepositoID") or "")
            emp_id = str(est.get("EmpresaID") or "")

    plano_txt_cfg = _pedido_plano_conta_texto_erp(integ)
    plano_id_cfg = _pedido_plano_conta_id_config_erp(integ)
    plano_txt, plano_id = resolver_plano_conta_para_pedido_erp(
        db,
        texto_config=plano_txt_cfg,
        id_config=plano_id_cfg or None,
        empresa_id=emp_id or None,
    )

    api_client = VendaERPAPIClient(
        base_url=(integ.url_base.strip() if integ and integ.url_base else None),
        token=(integ.token.strip() if integ and integ.token else None),
    )

    def _lbl(integ_obj, attr, default):
        if not integ_obj:
            return default
        v = getattr(integ_obj, attr, None) or ""
        v = str(v).strip()
        return v or default

    def _status_sistema_pedido_erp(integ_obj):
        raw = _lbl(integ_obj, "pedido_status_sistema", "")
        if raw:
            return raw
        return str(
            getattr(settings, "VENDA_ERP_PEDIDO_STATUS_SISTEMA", "") or "Pedido"
        ).strip() or "Pedido"

    fp_txt = _forma_pagamento_rotulo_sem_valor_moeda(
        str(data.get("forma_pagamento") or data.get("formaPagamento") or "")
    ).strip()[:200]
    fp_id = str(
        data.get("forma_pagamento_id")
        or data.get("formaPagamentoID")
        or data.get("formaPagamentoId")
        or ""
    ).strip()[:40]
    pagamentos_norm = _normalizar_linhas_pagamento_pedido(data.get("pagamentos"))

    st_pedido = _status_sistema_pedido_erp(integ)
    payload = {
        "statusSistema": st_pedido,
        "statusDaVenda": st_pedido,
        "cliente": (data.get("cliente") or "").strip()
        or "CONSUMIDOR NÃO IDENTIFICADO...",
        "data": timezone.now().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "origemVenda": "Venda Direta",
        "empresa": _lbl(integ, "pedido_empresa_label", "Agro Mais Centro"),
        "deposito": _lbl(integ, "pedido_deposito_label", "Deposito Centro"),
        "vendedor": _lbl(integ, "pedido_vendedor_label", "Gm Agro Mais"),
        "items": linhas,
        "valorFinal": valor_final,
    }
    if pagamentos_norm:
        payload["pagamentos"] = pagamentos_norm
        resumo_fp = _resumo_forma_pagamento_de_linhas(pagamentos_norm)
        if resumo_fp:
            payload["formaPagamento"] = resumo_fp
    elif fp_txt:
        payload["formaPagamento"] = fp_txt
        payload["pagamentos"] = [
            {
                "formaPagamento": fp_txt[:200],
                "valorPagamento": valor_final,
                "quitar": True,
            }
        ]
    if fp_id:
        payload["formaPagamentoID"] = fp_id
    if plano_txt:
        pt = plano_txt[:500]
        payload["planoDeConta"] = pt
        if not getattr(settings, "VENDA_ERP_PEDIDOS_SALVAR_PLANO_SO_TEXTO_SWAGGER", True):
            payload["planoConta"] = pt
    if plano_id and not getattr(settings, "VENDA_ERP_PEDIDOS_SALVAR_PLANO_SO_TEXTO_SWAGGER", True):
        pid = plano_id[:40]
        payload["planoDeContaID"] = pid
        payload["planoContaID"] = pid
        payload["planoDeContaId"] = pid
        payload["planoContaId"] = pid
    if dep_id:
        payload["depositoID"] = dep_id
    if emp_id:
        payload["empresaID"] = emp_id

    cid = str(data.get("cliente_id") or data.get("ClienteID") or "").strip()
    if _cliente_id_e_valido_para_erp(cid):
        payload["clienteID"] = cid
    doc_raw = str(data.get("cliente_documento") or data.get("CpfCnpj") or "").strip()
    doc_digits = re.sub(r"\D", "", doc_raw)
    if len(doc_digits) >= 11:
        payload["cpfCnpj"] = doc_digits

    payload = {k: v for k, v in payload.items() if v not in (None, "")}
    payload_camel_flat = copy.deepcopy(payload)
    payload_camel_snapshot = copy.deepcopy(payload)
    doc_plano_mestre = None
    if db is not None and plano_id:
        doc_plano_mestre = documento_plano_mestre_por_id_mongo(db, plano_id)
    usou_embutido = False
    if getattr(settings, "VENDA_ERP_PEDIDOS_SALVAR_PLANO_USO_EMBUTIDO", True):
        pv_emb = _pedido_payload_variante_plano_aninhado_dto(
            payload_camel_snapshot, doc_plano_mestre=doc_plano_mestre
        )
        if pv_emb is not None:
            payload_camel_snapshot = pv_emb
            usou_embutido = True
    payload = copy.deepcopy(payload_camel_snapshot)

    if getattr(settings, "VENDA_ERP_PEDIDOS_SALVAR_JSON_PASCAL", False):
        payload = _pedido_payload_camel_para_pascal(payload)

    if settings.DEBUG:
        try:
            logger.info(
                "Pedidos/Salvar payload: %s",
                json.dumps(payload, ensure_ascii=False, default=str),
            )
        except Exception:
            logger.info("Pedidos/Salvar payload (repr): %r", payload)

    ok, status, res = api_client.salvar_operacao_pdv(payload)
    flat_erp = _texto_heuristico_resposta_pedido_erp(res)
    recusa_erp = _mensagem_pedido_erp_indica_recusa_ou_erro_plano(flat_erp)

    if (
        getattr(settings, "VENDA_ERP_PEDIDOS_SALVAR_RETRY_PASCAL_EM_RECUSA", True)
        and ok
        and recusa_erp
        and not getattr(settings, "VENDA_ERP_PEDIDOS_SALVAR_JSON_PASCAL", False)
    ):
        payload_p = _pedido_payload_camel_para_pascal(payload_camel_snapshot)
        if settings.DEBUG:
            try:
                logger.info(
                    "Pedidos/Salvar retry PascalCase: %s",
                    json.dumps(payload_p, ensure_ascii=False, default=str),
                )
            except Exception:
                pass
        ok2, status2, res2 = api_client.salvar_operacao_pdv(payload_p)
        flat2 = _texto_heuristico_resposta_pedido_erp(res2)
        recusa2 = _mensagem_pedido_erp_indica_recusa_ou_erro_plano(flat2)
        if ok2 and not recusa2:
            ok, status, res = ok2, status2, res2
            flat_erp = flat2
            recusa_erp = recusa2

    if (
        getattr(settings, "VENDA_ERP_PEDIDOS_SALVAR_RETRY_PLANO_ANINHADO", True)
        and ok
        and recusa_erp
        and _mensagem_pedido_erp_indica_erro_plano_contas(flat_erp)
        and not usou_embutido
    ):
        pv_nest = _pedido_payload_variante_plano_aninhado_dto(
            payload_camel_flat, doc_plano_mestre=doc_plano_mestre
        )
        if pv_nest is not None:
            if getattr(settings, "VENDA_ERP_PEDIDOS_SALVAR_JSON_PASCAL", False):
                corpos_n = (_pedido_payload_camel_para_pascal(pv_nest),)
            else:
                corpos_n = (pv_nest,)
                if getattr(settings, "VENDA_ERP_PEDIDOS_SALVAR_RETRY_PASCAL_EM_RECUSA", True):
                    corpos_n = (pv_nest, _pedido_payload_camel_para_pascal(pv_nest))
            for body in corpos_n:
                okn, stn, resn = api_client.salvar_operacao_pdv(body)
                flatn = _texto_heuristico_resposta_pedido_erp(resn)
                recusan = _mensagem_pedido_erp_indica_recusa_ou_erro_plano(flatn)
                if okn and not recusan:
                    ok, status, res = okn, stn, resn
                    flat_erp = flatn
                    recusa_erp = False
                    break

    if (
        usou_embutido
        and ok
        and recusa_erp
        and _mensagem_pedido_erp_indica_retry_flat_apos_embutido(flat_erp)
    ):
        if getattr(settings, "VENDA_ERP_PEDIDOS_SALVAR_JSON_PASCAL", False):
            corpos_flat = (_pedido_payload_camel_para_pascal(payload_camel_flat),)
        else:
            corpos_flat = (payload_camel_flat,)
            if getattr(settings, "VENDA_ERP_PEDIDOS_SALVAR_RETRY_PASCAL_EM_RECUSA", True):
                corpos_flat = (
                    payload_camel_flat,
                    _pedido_payload_camel_para_pascal(payload_camel_flat),
                )
        for body in corpos_flat:
            okf, stf, resf = api_client.salvar_operacao_pdv(body)
            flatf = _texto_heuristico_resposta_pedido_erp(resf)
            recusaf = _mensagem_pedido_erp_indica_recusa_ou_erro_plano(flatf)
            if okf and not recusaf:
                ok, status, res = okf, stf, resf
                flat_erp = flatf
                recusa_erp = False
                break

    if (
        getattr(settings, "VENDA_ERP_PEDIDOS_SALVAR_RETRY_PLANO_ALTERNATIVAS", False)
        and ok
        and recusa_erp
        and _mensagem_pedido_erp_indica_erro_localizar_plano(flat_erp)
    ):
        variantes = (
            _pedido_payload_variante_sem_plano_cabecalho(payload_camel_snapshot),
            _pedido_payload_variante_itens_plano_so_id(payload_camel_snapshot),
        )
        for pv in variantes:
            if getattr(settings, "VENDA_ERP_PEDIDOS_SALVAR_JSON_PASCAL", False):
                corpos = (_pedido_payload_camel_para_pascal(pv),)
            else:
                corpos = (pv,)
                if getattr(settings, "VENDA_ERP_PEDIDOS_SALVAR_RETRY_PASCAL_EM_RECUSA", True):
                    corpos = (pv, _pedido_payload_camel_para_pascal(pv))
            for body in corpos:
                okp, stp, resp = api_client.salvar_operacao_pdv(body)
                flatp = _texto_heuristico_resposta_pedido_erp(resp)
                recusap = _mensagem_pedido_erp_indica_recusa_ou_erro_plano(flatp)
                if okp and not recusap:
                    ok, status, res = okp, stp, resp
                    flat_erp = flatp
                    recusa_erp = False
                    break
            if not recusa_erp:
                break

    if (
        getattr(settings, "VENDA_ERP_PEDIDOS_SALVAR_RETRY_PLANO_TEXTO_VARIANTES", True)
        and ok
        and recusa_erp
        and _mensagem_pedido_erp_indica_erro_localizar_plano(flat_erp)
        and plano_id
        and db is not None
    ):
        cands = candidatos_texto_plano_para_api_pedido(
            db, plano_id=plano_id, texto_ja_resolvido=plano_txt or ""
        )
        for cand in cands:
            if not cand:
                continue
            pvar = _pedido_payload_flat_com_texto_plano_uniforme(payload_camel_flat, cand)
            pvar = {k: v for k, v in pvar.items() if v not in (None, "")}
            if getattr(settings, "VENDA_ERP_PEDIDOS_SALVAR_JSON_PASCAL", False):
                corpos_tv = (_pedido_payload_camel_para_pascal(pvar),)
            else:
                corpos_tv = (pvar,)
                if getattr(settings, "VENDA_ERP_PEDIDOS_SALVAR_RETRY_PASCAL_EM_RECUSA", True):
                    corpos_tv = (pvar, _pedido_payload_camel_para_pascal(pvar))
            for body in corpos_tv:
                okv, stv, resv = api_client.salvar_operacao_pdv(body)
                flatv = _texto_heuristico_resposta_pedido_erp(resv)
                recusav = _mensagem_pedido_erp_indica_recusa_ou_erro_plano(flatv)
                if okv and not recusav:
                    ok, status, res = okv, stv, resv
                    flat_erp = flatv
                    recusa_erp = False
                    break
            if not recusa_erp:
                break

    if (
        getattr(settings, "VENDA_ERP_PEDIDOS_SALVAR_RETRY_PLANO_OBJETO_RETORNO_BUSCA", True)
        and ok
        and recusa_erp
        and _mensagem_pedido_erp_indica_erro_localizar_plano(flat_erp)
        and doc_plano_mestre
    ):
        prb = _pedido_payload_com_plano_objeto_retorno_busca(payload_camel_flat, doc_plano_mestre)
        if prb is not None:
            prb = {k: v for k, v in prb.items() if v not in (None, "")}
            if getattr(settings, "VENDA_ERP_PEDIDOS_SALVAR_JSON_PASCAL", False):
                corpos_rb = (_pedido_payload_plano_retorno_busca_tudo_pascal(prb),)
            else:
                corpos_rb = (prb,)
                if getattr(settings, "VENDA_ERP_PEDIDOS_SALVAR_RETRY_PASCAL_EM_RECUSA", True):
                    corpos_rb = (prb, _pedido_payload_plano_retorno_busca_tudo_pascal(prb))
            for body in corpos_rb:
                okb, stb, resb = api_client.salvar_operacao_pdv(body)
                flatb = _texto_heuristico_resposta_pedido_erp(resb)
                recusab = _mensagem_pedido_erp_indica_recusa_ou_erro_plano(flatb)
                if okb and not recusab:
                    ok, status, res = okb, stb, resb
                    flat_erp = flatb
                    recusa_erp = False
                    break

    if ok and not recusa_erp and _mensagem_pedido_erp_indica_erro_plano_contas(flat_erp):
        recusa_erp = True

    if recusa_erp:
        erp_sync = VendaAgro.ErpSyncStatus.RECUSADO_ERP
    elif ok:
        erp_sync = VendaAgro.ErpSyncStatus.ACEITO
    else:
        erp_sync = VendaAgro.ErpSyncStatus.FALHA_COMUNICACAO
    sucesso_erp = erp_sync == VendaAgro.ErpSyncStatus.ACEITO
    msg_erro_ui = (flat_erp.strip() or _json_legivel(res)).strip()

    return None, {
        "ok": ok,
        "status": status,
        "res": res,
        "flat_erp": flat_erp,
        "recusa_erp": recusa_erp,
        "erp_sync": erp_sync,
        "sucesso_erp": sucesso_erp,
        "raw_itens": raw_itens,
        "msg_erro_ui": msg_erro_ui,
    }


@require_POST
def api_enviar_pedido_erp(request):
    try:
        data = json.loads(request.body)
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)
    data.pop("client_request_id", None)
    data.pop("idempotency_key", None)
    try:
        client_m, db = obter_conexao_mongo()
        err, out = _fluxo_enviar_pedido_erp_interno(
            request, data, client_m=client_m, db=db
        )
        if err is not None:
            return err
        venda_local = _persistir_venda_agro(
            request,
            data,
            out["raw_itens"],
            out["status"],
            out["res"],
            out["sucesso_erp"],
            erp_sync_status=out["erp_sync"],
        )
        vid = venda_local.pk if venda_local else None
        msg_erro_ui = out["msg_erro_ui"]

        if out["ok"] and out["recusa_erp"]:
            return JsonResponse(
                {
                    "ok": False,
                    "erro": msg_erro_ui,
                    "http_status": out["status"],
                    "venda_id": vid,
                },
                status=502,
            )
        if out["ok"]:
            return JsonResponse(
                {
                    "ok": True,
                    "mensagem": _json_legivel(out["res"]),
                    "venda_id": vid,
                }
            )
        return JsonResponse(
            {
                "ok": False,
                "erro": msg_erro_ui or _json_legivel(out["res"]),
                "http_status": out["status"],
                "venda_id": vid,
            },
            status=502 if out["status"] and out["status"] != 0 else 500,
        )
    except Exception as e:
        return JsonResponse({"ok": False, "erro": str(e)}, status=500)


@login_required(login_url="/admin/login/")
@require_POST
def api_venda_agro_reenviar_erp(request, pk):
    """Repete Pedidos/Salvar para uma venda já gravada (ex.: após falha ou recusa do ERP)."""
    try:
        v = get_object_or_404(
            VendaAgro.objects.prefetch_related("itens"),
            pk=pk,
        )
        if not v.itens.exists():
            return JsonResponse(
                {"ok": False, "erro": "Venda sem itens para reenviar."},
                status=400,
            )
        data = {
            "cliente": v.cliente_nome,
            "cliente_id": v.cliente_id_erp,
            "cliente_documento": v.cliente_documento,
            "forma_pagamento": v.forma_pagamento,
            "itens": [
                {
                    "id": it.produto_id_externo,
                    "nome": it.descricao,
                    "qtd": float(it.quantidade),
                    "preco": float(it.valor_unitario),
                    "codigo": it.codigo,
                }
                for it in v.itens.all()
            ],
        }
        client_m, db = obter_conexao_mongo()
        err, out = _fluxo_enviar_pedido_erp_interno(
            request, data, client_m=client_m, db=db
        )
        if err is not None:
            return err
        _atualizar_venda_agro_resposta_erp(
            v, out["status"], out["res"], out["erp_sync"]
        )
        msg_erro_ui = out["msg_erro_ui"]
        if out["ok"] and out["recusa_erp"]:
            return JsonResponse(
                {
                    "ok": False,
                    "erro": msg_erro_ui,
                    "http_status": out["status"],
                    "venda_id": v.pk,
                },
                status=502,
            )
        if out["ok"]:
            return JsonResponse(
                {
                    "ok": True,
                    "mensagem": _json_legivel(out["res"]),
                    "venda_id": v.pk,
                }
            )
        return JsonResponse(
            {
                "ok": False,
                "erro": msg_erro_ui or _json_legivel(out["res"]),
                "http_status": out["status"],
                "venda_id": v.pk,
            },
            status=502 if out["status"] and out["status"] != 0 else 500,
        )
    except Exception as e:
        return JsonResponse({"ok": False, "erro": str(e)}, status=500)


@require_POST
def api_pdv_salvar_checkout_draft(request):
    """Grava carrinho na sessão e permite abrir /pdv/checkout/."""
    try:
        data = json.loads(request.body.decode("utf-8"))
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)
    itens = _sanear_itens_checkout_sessao(data.get("itens"))
    if not itens:
        return JsonResponse(
            {"ok": False, "erro": "Carrinho vazio ou itens inválidos"},
            status=400,
        )
    cli = str(data.get("cliente") or "").strip()[:400]
    if not cli:
        cli = "CONSUMIDOR NÃO IDENTIFICADO..."
    sess_chk: dict = {
        "itens": itens,
        "cliente": cli,
        "cliente_extra": _sanear_cliente_extra_sessao(data.get("cliente_extra")),
        "forma_pagamento": str(data.get("forma_pagamento") or "").strip()[:200],
    }
    pag_chk = _normalizar_linhas_pagamento_pedido(data.get("pagamentos"))
    if pag_chk:
        sess_chk["pagamentos"] = pag_chk
    request.session["pdv_checkout"] = sess_chk
    request.session.modified = True
    return JsonResponse({"ok": True})


@require_POST
def api_pdv_limpar_checkout_draft(request):
    request.session.pop("pdv_checkout", None)
    request.session.modified = True
    return JsonResponse({"ok": True})


def _media_diaria_vendas_por_produto(db, dias=30):
    """
    Quantidade média vendida por dia (total no período / dias), por ProdutoID,
    a partir de DtoVenda + DtoVendaProduto (mesmos filtros que atualizar_medias.py).
    """
    out = {}
    try:
        data_limite = datetime.now() - timedelta(days=dias)
        vendas = list(
            db["DtoVenda"].find(
                {
                    "Data": {"$gte": data_limite},
                    "Cancelada": {"$ne": True},
                    "Status": {"$nin": ["Cancelado", "Cancelada", "Orcamento"]},
                },
                {"_id": 1, "Id": 1},
            )
        )
        if not vendas:
            return out
        venda_ids_obj = []
        venda_ids_str = []
        for v in vendas:
            vid = str(v.get("Id") or v.get("_id"))
            venda_ids_str.append(vid)
            if len(vid) == 24:
                try:
                    venda_ids_obj.append(ObjectId(vid))
                except Exception:
                    pass
        query_itens = {
            "$or": [
                {"VendaID": {"$in": venda_ids_obj}},
                {"VendaID": {"$in": venda_ids_str}},
            ]
        }
        totais = {}
        for item in db["DtoVendaProduto"].find(query_itens):
            raw_pid = item.get("ProdutoID")
            if raw_pid is None:
                continue
            pid = str(raw_pid)
            if not pid or pid == "None":
                continue
            qtd = float(item.get("Quantidade") or 0)
            totais[pid] = totais.get(pid, 0.0) + qtd
        div = float(dias) if dias else 30.0
        for pid, total in totais.items():
            out[pid] = round(total / div, 6)
    except Exception as exc:
        logger.warning("media_diaria_vendas_por_produto: %s", exc)
    return out


# Entrada diária: {"day": "YYYY-MM-DD" (localdate Django), "map": {produto_id: float}}
_CACHE_MEDIAS_VENDA_ENTRY = "pdv_mapa_medias_venda_diaria_30d_entry_v2"


def _obter_mapa_medias_venda_cache(db):
    """
    Médias de venda (30d) recalculadas no máximo 1x por dia civil.
    A troca de dia usa TIME_ZONE do Django (ex.: America/Sao_Paulo): no primeiro
    request após meia-noite local o mapa é refeito.
    Opcional: agendar `python manage.py pdv_refresh_medias_venda` no cron às 00:00
    para pré-aquecer o cache antes do primeiro usuário.
    """
    hoje = timezone.localdate().isoformat()
    entry = cache.get(_CACHE_MEDIAS_VENDA_ENTRY)
    if (
        entry
        and isinstance(entry, dict)
        and entry.get("day") == hoje
        and isinstance(entry.get("map"), dict)
    ):
        return entry["map"]
    m = _media_diaria_vendas_por_produto(db, dias=30)
    cache.set(
        _CACHE_MEDIAS_VENDA_ENTRY,
        {"day": hoje, "map": m},
        timeout=86400 * 2,
    )
    return m


# --- CARGA INICIAL E APIs AUXILIARES ---
def _catalogo_pdv_montar_produtos(db, client):
    query = {"CadastroInativo": {"$ne": True}}
    produtos = list(db[client.col_p].find(query))
    p_ids = [str(p.get("Id") or p["_id"]) for p in produtos]
    saldos_por_pid = _mapa_saldos_finais_por_produtos(db, client, p_ids)
    medias_venda = _obter_mapa_medias_venda_cache(db)
    res = []
    for p in produtos:
        pid = str(p.get("Id") or p["_id"])
        sp = saldos_por_pid.get(pid) or {}
        saldo_f_c = float(sp.get("saldo_centro", 0.0))
        saldo_f_v = float(sp.get("saldo_vila", 0.0))
        s_c = float(sp.get("saldo_erp_centro", 0.0))
        s_v = float(sp.get("saldo_erp_vila", 0.0))

        prateleira_raw = (
            p.get("Prateleira")
            or p.get("Localizacao")
            or p.get("LocalEstoque")
            or p.get("Setor")
            or p.get("EnderecoPrateleira")
            or ""
        )
        sub_grupo_txt = str(
            p.get("SubGrupo") or p.get("Subcategoria") or p.get("NomeSubcategoria") or ""
        ).strip()
        cat_linha = (
            p.get("NomeCategoria")
            or p.get("Categoria")
            or p.get("Grupo")
            or ""
        )
        if not str(cat_linha or "").strip() and sub_grupo_txt:
            cat_linha = sub_grupo_txt
        ix_raw = p.get(INDEX_CODIGOS_CAMPO)
        index_codigos_list: list[str] = []
        if isinstance(ix_raw, list):
            index_codigos_list = [str(x) for x in ix_raw[:260] if x is not None and str(x).strip() != ""]
        partes = [
            p.get("Nome"),
            p.get("Marca"),
            p.get("NomeCategoria"),
            p.get("Categoria"),
            p.get("Grupo"),
            p.get("CodigoNFe"),
            p.get("Codigo"),
            p.get("CodigoBarras"),
            p.get("EAN_NFe"),
            p.get("Referencia"),
            p.get("CodigoReferencia"),
            p.get("Sku") or p.get("SKU"),
            p.get("CodigoSku"),
            p.get("CodigoInterno"),
            p.get("CodigoFornecedor"),
            p.get("CodFornecedor"),
            p.get("GTIN"),
            prateleira_raw or None,
        ]
        partes.extend(index_codigos_list)
        busca_texto_gerado = " ".join(normalizar(str(part)) for part in partes if part).strip()
        busca_texto_existente = normalizar(p.get("BuscaTexto") or "")
        texto_puro = " ".join(str(part) for part in partes if part)
        texto_puro_limpo = "".join(
            c for c in unicodedata.normalize("NFD", texto_puro) if unicodedata.category(c) != "Mn"
        ).lower()
        busca_texto_final = f"{busca_texto_gerado} {busca_texto_existente} {texto_puro_limpo}".strip()

        custos = _custos_compra_produto(p)
        preco_custo_val = custos["preco_custo"]
        preco_custo_acresc_val = custos["preco_custo_final"]
        preco_venda_val = float(p.get("ValorVenda") or p.get("PrecoVenda") or 0)

        res.append(
            {
                "id": pid,
                "nome": p.get("Nome"),
                "marca": p.get("Marca"),
                "prateleira": str(prateleira_raw).strip() if prateleira_raw is not None else "",
                "fornecedor": p.get("NomeFornecedor")
                or p.get("Fornecedor")
                or p.get("RazaoSocialFornecedor")
                or p.get("Fabricante"),
                "categoria": cat_linha,
                "subcategoria": sub_grupo_txt,
                "codigo_nfe": p.get("CodigoNFe") or p.get("Codigo"),
                "codigo_barras": p.get("CodigoBarras") or p.get("EAN_NFe"),
                "referencia": _mongo_primeiro_texto(
                    p,
                    ("Referencia", "CodigoReferencia", "ReferenciaFornecedor"),
                ),
                "sku": _mongo_primeiro_texto(p, ("Sku", "SKU", "CodigoSku")),
                "codigo_interno": _mongo_primeiro_texto(p, ("CodigoInterno", "CodigoAuxiliar")),
                "codigo_fornecedor": _mongo_primeiro_texto(
                    p,
                    ("CodigoFornecedor", "CodFornecedor", "RefFornecedor"),
                ),
                "preco_venda": preco_venda_val,
                "preco_custo": preco_custo_val,
                "preco_custo_acrescimo": preco_custo_acresc_val,
                "preco_custo_final": preco_custo_acresc_val,
                "saldo_centro": round(saldo_f_c, 2),
                "saldo_vila": round(saldo_f_v, 2),
                "saldo_erp_centro": s_c,
                "saldo_erp_vila": s_v,
                "busca_texto": busca_texto_final,
                "media_venda_diaria_30d": float(medias_venda.get(pid, 0.0)),
                "index_codigos": index_codigos_list,
            }
        )
    return res


def _catalogo_pdv_version(produtos: list[dict]) -> str:
    h = hashlib.sha1()
    h.update(str(len(produtos)).encode("utf-8"))
    for p in sorted(produtos, key=lambda x: str(x.get("id") or "")):
        ix = p.get("index_codigos") or []
        ix_fp = ""
        if isinstance(ix, list) and ix:
            ix_fp = str(len(ix)) + ":" + "|".join(str(x) for x in ix[:48])
        h.update(
            (
                f"{p.get('id','')}|{p.get('nome','')}|{p.get('codigo_nfe','')}|"
                f"{p.get('codigo_barras','')}|{p.get('preco_venda',0)}|{p.get('preco_custo_final',0)}|{ix_fp}"
            ).encode("utf-8")
        )
    return h.hexdigest()[:20]


def _catalogo_pdv_entry_atual(db, client):
    hoje_cat = timezone.localdate().isoformat()
    entry_cat = cache.get(CATALOGO_PDV_CACHE_ENTRY_KEY)
    if (
        entry_cat
        and isinstance(entry_cat, dict)
        and entry_cat.get("day") == hoje_cat
        and isinstance(entry_cat.get("body"), dict)
        and "produtos" in entry_cat["body"]
        and entry_cat.get("version")
    ):
        return entry_cat

    produtos = _catalogo_pdv_montar_produtos(db, client)
    now_iso = timezone.now().isoformat()
    version = _catalogo_pdv_version(produtos)
    body = {
        "produtos": produtos,
        "catalog_version": version,
        "catalog_updated_at": now_iso,
    }
    prev = cache.get(CATALOGO_PDV_CACHE_ENTRY_KEY)
    if prev and isinstance(prev, dict):
        cache.set(CATALOGO_PDV_CACHE_PREV_ENTRY_KEY, prev, timeout=86400 * 3)
    new_entry = {"day": hoje_cat, "version": version, "updated_at": now_iso, "body": body}
    cache.set(CATALOGO_PDV_CACHE_ENTRY_KEY, new_entry, timeout=86400 * 2)
    try:
        from estoque.sync_health import registrar_catalogo_built

        registrar_catalogo_built(version)
    except Exception:
        pass
    return new_entry


@require_GET
def api_todos_produtos_local(request):
    from estoque.sync_health import registrar_ping_mongo

    client, db = obter_conexao_mongo()
    if db is None:
        registrar_ping_mongo(False, "Mongo indisponível")
        return JsonResponse({"erro": "Erro conexao"}, status=500)
    try:
        entry = _catalogo_pdv_entry_atual(db, client)
        registrar_ping_mongo(True)
        return JsonResponse(entry["body"])
    except Exception as e:
        registrar_ping_mongo(False, str(e))
        return JsonResponse({"erro": str(e)}, status=500)


@require_GET
def api_todos_produtos_delta(request):
    from estoque.sync_health import registrar_ping_mongo

    client, db = obter_conexao_mongo()
    if db is None:
        registrar_ping_mongo(False, "Mongo indisponível")
        return JsonResponse({"erro": "Erro conexao"}, status=500)
    since = str(request.GET.get("since") or "").strip()
    try:
        current = _catalogo_pdv_entry_atual(db, client)
        cur_v = str(current.get("version") or "")
        cur_body = current.get("body") or {}
        cur_updated = str(current.get("updated_at") or cur_body.get("catalog_updated_at") or "")
        if since and since == cur_v:
            registrar_ping_mongo(True)
            return JsonResponse(
                {
                    "ok": True,
                    "unchanged": True,
                    "catalog_version": cur_v,
                    "catalog_updated_at": cur_updated,
                }
            )

        prev = cache.get(CATALOGO_PDV_CACHE_PREV_ENTRY_KEY)
        if (
            since
            and prev
            and isinstance(prev, dict)
            and str(prev.get("version") or "") == since
            and isinstance(prev.get("body"), dict)
        ):
            prev_rows = prev["body"].get("produtos") or []
            cur_rows = cur_body.get("produtos") or []
            prev_map = {str(p.get("id") or ""): p for p in prev_rows if p.get("id") is not None}
            cur_map = {str(p.get("id") or ""): p for p in cur_rows if p.get("id") is not None}
            changed = []
            removed = []
            for pid, row in cur_map.items():
                old = prev_map.get(pid)
                if old != row:
                    changed.append(row)
            for pid in prev_map:
                if pid not in cur_map:
                    removed.append(pid)
            registrar_ping_mongo(True)
            return JsonResponse(
                {
                    "ok": True,
                    "delta": True,
                    "catalog_version": cur_v,
                    "catalog_updated_at": cur_updated,
                    "changed": changed,
                    "removed_ids": removed,
                }
            )

        registrar_ping_mongo(True)
        return JsonResponse(
            {
                "ok": True,
                "delta": False,
                "full": True,
                "catalog_version": cur_v,
                "catalog_updated_at": cur_updated,
                "produtos": cur_body.get("produtos") or [],
            }
        )
    except Exception as e:
        registrar_ping_mongo(False, str(e))
        return JsonResponse({"erro": str(e)}, status=500)


@require_GET
def api_pdv_invalidar_cache_catalogo(request):
    """Limpa o snapshot diário do catálogo; próximo GET /api/todos-produtos/ refaz do Mongo."""
    cache.delete(CATALOGO_PDV_CACHE_ENTRY_KEY)
    cache.delete(CATALOGO_PDV_CACHE_PREV_ENTRY_KEY)
    return JsonResponse({"ok": True})


@require_GET
def api_cron_enviar_alerta_vendas_dia(request):
    """
    Endpoint para agendador externo (sem shell): dispara alerta de vendas do dia.
    Protegido por token em ALERTA_VENDAS_CRON_TOKEN.
    Aceita:
      - ?token=...
      - Header X-Agro-Cron-Token: ...
      - Header Authorization: Bearer ...
    """
    if not _token_cron_alerta_valido(request):
        token_cfg = (getattr(settings, "ALERTA_VENDAS_CRON_TOKEN", "") or "").strip()
        if (token_cfg.startswith('"') and token_cfg.endswith('"')) or (
            token_cfg.startswith("'") and token_cfg.endswith("'")
        ):
            token_cfg = token_cfg[1:-1].strip()
        return JsonResponse(
            {
                "ok": False,
                "erro": "Não autorizado.",
                "token_configurado": bool(token_cfg),
                "token_tamanho": len(token_cfg),
            },
            status=403,
        )
    force = str(request.GET.get("force") or "").strip().lower() in ("1", "true", "yes", "on")
    from produtos.management.commands.enviar_alerta_vendas_dia import executar_alerta_vendas_dia

    out = executar_alerta_vendas_dia(force=force)
    st = 200 if out.get("ok") else (200 if not out.get("executado") else 503)
    return JsonResponse(out, status=st)


@never_cache
@require_GET
def api_pdv_saldos_compacto(request):
    """
    Saldos atuais (espelho Mongo + camada Agro / ajustes) para todos os produtos ativos — payload compacto.
    Cache de poucos segundos: muitas abas/caixas batem o mesmo snapshot e aliviam o Mongo.
    Resposta sem cache HTTP (evita saldo antigo no Electron / Chromium).
    """
    cached = cache.get(_SALDOS_PDV_CACHE_KEY)
    if cached is not None and isinstance(cached, dict) and "rows" in cached:
        return JsonResponse(cached)

    client, db = obter_conexao_mongo()
    if db is None:
        try:
            from estoque.sync_health import registrar_ping_mongo

            registrar_ping_mongo(False, "Mongo indisponível")
        except Exception:
            pass
        return JsonResponse({"erro": "Erro conexao"}, status=500)
    try:
        query = {"CadastroInativo": {"$ne": True}}
        produtos = list(db[client.col_p].find(query, {"Id": 1, "_id": 1}))
        p_ids = [str(p.get("Id") or p["_id"]) for p in produtos]
        saldos = _mapa_saldos_finais_por_produtos(db, client, p_ids)
        rows = []
        for pid in p_ids:
            sp = saldos.get(pid) or {}
            rows.append(
                [
                    pid,
                    sp.get("saldo_centro", 0.0),
                    sp.get("saldo_vila", 0.0),
                    sp.get("saldo_erp_centro", 0.0),
                    sp.get("saldo_erp_vila", 0.0),
                ]
            )
        payload = {"v": 1, "rows": rows}
        cache.set(_SALDOS_PDV_CACHE_KEY, payload, timeout=_SALDOS_PDV_CACHE_TTL)
        try:
            from estoque.sync_health import registrar_ping_mongo

            registrar_ping_mongo(True)
        except Exception:
            pass
        return JsonResponse(payload)
    except Exception as e:
        try:
            from estoque.sync_health import registrar_ping_mongo

            registrar_ping_mongo(False, str(e))
        except Exception:
            pass
        return JsonResponse({"erro": str(e)}, status=500)


@require_GET
def api_pdv_metricas_produtos(request):
    """
    Por produto: média diária (total/dias no período), vendas últimos 7d, 7d anteriores,
    variação % semana a semana, última entrada (compra/nota), e 4 colunas extras com qtd.
    vendida por semana (janelas de 7d nos últimos 28d, da mais antiga à mais recente).
    Cada linha de rows tem 12 elementos (índices 8–11 = sparkline 4 semanas). Cache ~5 min.
    """
    try:
        dias = int(request.GET.get("dias", 30))
    except (TypeError, ValueError):
        dias = 30
    dias = max(7, min(365, dias))
    bucket = int(time.time() // 300)
    ck = _pdv_metricas_cache_key(dias, bucket)
    hit = cache.get(ck)
    if hit is not None and isinstance(hit, dict) and hit.get("rows"):
        return JsonResponse(hit)

    client, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"erro": "Erro conexao"}, status=500)
    try:
        media_tot, w0, w1, spark_map = _metricas_vendas_agregadas_por_produto(db, dias)
        entradas = _ultima_entrada_mercadoria_por_produto(db)
        query = {"CadastroInativo": {"$ne": True}}
        produtos = list(db[client.col_p].find(query, {"Id": 1, "_id": 1}))
        p_ids = [str(p.get("Id") or p["_id"]) for p in produtos]
        div = float(dias) if dias else 30.0
        rows = []
        for pid in p_ids:
            tot_p = float(media_tot.get(pid, 0.0))
            media_d = round(tot_p / div, 6) if div else 0.0
            s0 = float(w0.get(pid, 0.0))
            s1 = float(w1.get(pid, 0.0))
            if s1 > 0:
                var_pct = round((s0 - s1) / s1 * 100.0, 2)
            elif s0 > 0:
                var_pct = 100.0
            else:
                var_pct = None
            ent = entradas.get(pid) or {}
            sp = spark_map.get(pid) or [0.0, 0.0, 0.0, 0.0]
            rows.append(
                [
                    pid,
                    media_d,
                    round(tot_p, 4),
                    round(s0, 4),
                    round(s1, 4),
                    var_pct,
                    ent.get("data") or "",
                    float(ent.get("qtd") or 0),
                    round(float(sp[0]), 4),
                    round(float(sp[1]), 4),
                    round(float(sp[2]), 4),
                    round(float(sp[3]), 4),
                ]
            )
        payload = {"v": 2, "dias": dias, "rows": rows}
        cache.set(ck, payload, timeout=320)
        return JsonResponse(payload)
    except Exception as e:
        return JsonResponse({"erro": str(e)}, status=500)


@require_GET
def api_pdv_top_vendidos(request):
    """
    Top N produtos por quantidade vendida no período (DtoVenda / DtoVendaProduto).
    Cache ~5 min; invalidação alinhada às métricas PDV.
    """
    try:
        limite = int(request.GET.get("limite") or 10)
    except (TypeError, ValueError):
        limite = 10
    limite = max(1, min(limite, 20))
    try:
        dias = int(request.GET.get("dias") or 30)
    except (TypeError, ValueError):
        dias = 30
    dias = max(7, min(dias, 365))
    bucket = int(time.time() // 300)
    ck = _pdv_top_vendidos_cache_key(dias, limite, bucket)
    hit = cache.get(ck)
    if hit is not None and isinstance(hit, dict) and "itens" in hit:
        try:
            json.dumps(hit)
        except (TypeError, ValueError):
            cache.delete(ck)
        else:
            return JsonResponse(hit)

    client, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"erro": "Mongo indisponível", "itens": []}, status=503)
    try:
        media_tot, _, _, _ = _metricas_vendas_agregadas_por_produto(db, dias)
        ranked = sorted(media_tot.items(), key=lambda x: x[1], reverse=True)[:limite]
        if not ranked:
            payload = {"v": 1, "dias": dias, "limite": limite, "itens": []}
            cache.set(ck, payload, timeout=320)
            return JsonResponse(payload)

        ids_top = [r[0] for r in ranked]
        ors = []
        for pid in ids_top:
            ors.append({"Id": pid})
            if pid.isdigit():
                try:
                    n = int(pid)
                    ors.append({"Id": n})
                    ors.append({"Codigo": n})
                except (TypeError, ValueError):
                    pass
            if len(pid) == 24:
                try:
                    oid = ObjectId(pid)
                    ors.append({"Id": oid})
                    ors.append({"_id": oid})
                except Exception:
                    pass

        col = db[client.col_p]
        prods = list(
            col.find(
                {"$or": ors},
                {
                    "Id": 1,
                    "_id": 1,
                    "Nome": 1,
                    "ValorVenda": 1,
                    "PrecoVenda": 1,
                    "Codigo": 1,
                },
            )
        )

        def _chaves_produto_para_mapa(p) -> list[str]:
            keys = []
            vid = p.get("Id")
            if vid is not None:
                keys.append(str(vid))
            keys.append(str(p.get("_id")))
            cod = p.get("Codigo")
            if cod is not None and str(cod).strip() != "":
                keys.append(str(cod))
            return [k for k in keys if k and k != "None"]

        pmap: dict[str, dict] = {}
        for p in prods:
            for k in _chaves_produto_para_mapa(p):
                pmap[k] = p

        itens: list[dict] = []
        for pid, qtd in ranked:
            p = pmap.get(pid)
            canon_id = str(p.get("Id") or p["_id"]) if p else str(pid)
            raw_nome = p.get("Nome") if p else None
            nome = _pdv_top_v_texto_produto(raw_nome, f"Produto {canon_id}")
            if not nome:
                nome = f"Produto {canon_id}"
            preco = 0.0
            if p:
                raw_preco = p.get("ValorVenda")
                if raw_preco is None:
                    raw_preco = p.get("PrecoVenda")
                if raw_preco is None:
                    raw_preco = 0
                preco = _pdv_top_v_float(raw_preco)
            fq = _pdv_top_v_float(qtd)
            itens.append(
                {
                    "id": canon_id,
                    "nome": nome,
                    "preco_venda": round(preco, 4),
                    "qtd_periodo": round(fq, 4),
                }
            )

        payload = {"v": 1, "dias": dias, "limite": limite, "itens": itens}
        cache.set(ck, payload, timeout=320)
        try:
            return JsonResponse(payload)
        except (TypeError, ValueError) as ser_err:
            logger.exception("api_pdv_top_vendidos JsonResponse: %s", ser_err)
            return JsonResponse(
                {"erro": "Falha ao serializar o ranking; dados de produto inválidos.", "itens": []},
                status=500,
            )
    except Exception as e:
        return JsonResponse({"erro": str(e), "itens": []}, status=500)


def _nome_exibicao_pessoa(doc):
    """Nome exibível: vários ERPs usam campos diferentes em DtoPessoa / cliente."""
    for chave in (
        "Nome",
        "RazaoSocial",
        "NomeFantasia",
        "Fantasia",
        "Apelido",
        "NomeCompleto",
        "NomeReduzido",
        "DenominacaoSocial",
        "nome",
        "razaoSocial",
        "nomeFantasia",
        "Descricao",
        "descricao",
    ):
        v = doc.get(chave)
        if v is not None:
            s = str(v).strip()
            if len(s) >= 2:
                return s[:240]
    bt = doc.get("BuscaTexto") or doc.get("buscaTexto")
    if bt is not None:
        linha = str(bt).strip().split("\n")[0].strip()
        if len(linha) >= 2:
            return linha[:240]
    return ""


def _projecao_pessoa():
    return {
        "Nome": 1,
        "RazaoSocial": 1,
        "NomeFantasia": 1,
        "Fantasia": 1,
        "Apelido": 1,
        "nome": 1,
        "NomeCompleto": 1,
        "NomeReduzido": 1,
        "DenominacaoSocial": 1,
        "Descricao": 1,
        "descricao": 1,
        "BuscaTexto": 1,
        "buscaTexto": 1,
        "Id": 1,
        "CpfCnpj": 1,
        "CPF": 1,
        "Cnpj": 1,
        "Cpf": 1,
        "cpfCnpj": 1,
        "CNPJ": 1,
        "Documento": 1,
        "documento": 1,
        "DocumentoIdentificacao": 1,
        "documentoIdentificacao": 1,
        "InscricaoFederal": 1,
        "inscricaoFederal": 1,
        "CpfCnpjFormatado": 1,
        "cpfCnpjFormatado": 1,
        "CpfCnpjSemFormatacao": 1,
        "cpfCnpjSemFormatacao": 1,
        "NumeroDocumento": 1,
        "numeroDocumento": 1,
        "Identificacao": 1,
        "identificacao": 1,
        "PessoaFisica": 1,
        "pessoaFisica": 1,
        "PessoaJuridica": 1,
        "pessoaJuridica": 1,
        "DadosCadastrais": 1,
        "dadosCadastrais": 1,
        "DadosPrincipais": 1,
        "dadosPrincipais": 1,
        "Telefone": 1,
        "Celular": 1,
        "Fone": 1,
        "CadastroInativo": 1,
        "Inativo": 1,
        # Endereço (plano ou aninhado em DtoPessoa)
        "Logradouro": 1,
        "logradouro": 1,
        "Endereco": 1,
        "endereco": 1,
        "Rua": 1,
        "rua": 1,
        "NomeLogradouro": 1,
        "Numero": 1,
        "numero": 1,
        "NumeroEndereco": 1,
        "Complemento": 1,
        "complemento": 1,
        "Bairro": 1,
        "bairro": 1,
        "NomeBairro": 1,
        "Cidade": 1,
        "cidade": 1,
        "Municipio": 1,
        "municipio": 1,
        "NomeCidade": 1,
        "NomeMunicipio": 1,
        "UF": 1,
        "uf": 1,
        "Estado": 1,
        "estado": 1,
        "SiglaUF": 1,
        "CEP": 1,
        "cep": 1,
        "Cep": 1,
        "EnderecoPrincipal": 1,
        "enderecoPrincipal": 1,
        "DadosEndereco": 1,
        "dadosEndereco": 1,
        "EnderecoCompleto": 1,
        "enderecoCompleto": 1,
        "EnderecoFormatado": 1,
        "enderecoFormatado": 1,
        "PessoaEndereco": 1,
        "pessoaEndereco": 1,
        "EnderecoCobranca": 1,
        "enderecoCobranca": 1,
        "CodigoPostal": 1,
        "codigoPostal": 1,
        "ComplementoEndereco": 1,
        "complementoEndereco": 1,
        "NumeroLogradouro": 1,
        "numeroLogradouro": 1,
        "numeroEndereco": 1,
        "Nro": 1,
        "nro": 1,
        "NroEndereco": 1,
        "nroEndereco": 1,
        "EnderecoNumero": 1,
        "enderecoNumero": 1,
        "Num": 1,
        "num": 1,
        "Predio": 1,
        "predio": 1,
    }


def _colecoes_pessoa_disponiveis(db, client_m):
    """Tenta DtoPessoa e outras coleções comuns quando a principal está vazia ou em outro nome."""
    preferidas = [
        client_m.col_c,
        "DtoPessoa",
        "DtoCliente",
        "Cliente",
        "DtoPessoaCliente",
        "Pessoa",
        "dto_pessoa",
        "dto_cliente",
    ]
    try:
        existentes = set(db.list_collection_names())
    except Exception:
        existentes = set()
    ordem = []
    vistas = set()
    for n in preferidas:
        if not n or n in vistas:
            continue
        if n in existentes:
            ordem.append(n)
            vistas.add(n)
    if client_m.col_c and client_m.col_c not in vistas:
        ordem.append(client_m.col_c)
    if not ordem and client_m.col_c:
        return [client_m.col_c]
    return ordem


def _telefone_pessoa(i):
    for chave in (
        "Telefone",
        "Celular",
        "Fone",
        "telefone",
        "celular",
        "WhatsApp",
        "CelularPrincipal",
    ):
        t = i.get(chave)
        if t is not None and str(t).strip():
            return str(t).strip()
    return ""


def _valor_texto_campo(v):
    """Mongo/BSON: número do imóvel e IDs costumam vir int/Decimal — evita '12.0' e ignora bool."""
    if v is None:
        return ""
    if isinstance(v, bool):
        return ""
    if isinstance(v, Decimal):
        try:
            if v == v.to_integral_value():
                return str(int(v))
        except Exception:
            pass
        s = format(v, "f").rstrip("0").rstrip(".")
        return s if s else ""
    if isinstance(v, float):
        if v.is_integer():
            return str(int(v))
        s = str(v).strip()
        return s
    if isinstance(v, int):
        return str(v)
    return str(v).strip()


_CHAVES_DOCUMENTO_PESSOA = (
    "CpfCnpj",
    "CPF",
    "Cpf",
    "CNPJ",
    "Cnpj",
    "cpfCnpj",
    "CNPJ_CPF",
    "cnpj_CPF",
    "cnpJ_CPF",
    "CnpJ_CPF",
    "Documento",
    "documento",
    "DocumentoIdentificacao",
    "documentoIdentificacao",
    "InscricaoFederal",
    "inscricaoFederal",
    "Inscricao",
    "inscricao",
    "CpfCnpjFormatado",
    "cpfCnpjFormatado",
    "CpfCnpjSemFormatacao",
    "cpfCnpjSemFormatacao",
    "NumeroDocumento",
    "numeroDocumento",
    "CNPJCPF",
    "Identificacao",
    "identificacao",
)


_DOC_NEST_KEYS_DOCUMENTO = (
    "PessoaFisica",
    "pessoaFisica",
    "PessoaJuridica",
    "pessoaJuridica",
    "DadosCadastrais",
    "dadosCadastrais",
    "DadosPrincipais",
    "dadosPrincipais",
    "Cliente",
    "cliente",
    "Fisica",
    "fisica",
    "Juridica",
    "juridica",
)


def _documento_em_dict_plano(i):
    if not isinstance(i, dict):
        return ""
    for chave in _CHAVES_DOCUMENTO_PESSOA:
        s = _valor_texto_campo(i.get(chave))
        if s:
            return s
    return ""


def _documento_pessoa(i):
    """CPF/CNPJ em raiz ou em subdocumentos (DtoPessoa / ERP)."""
    if not isinstance(i, dict):
        return ""
    d = _documento_em_dict_plano(i)
    if d:
        return d
    for nk in _DOC_NEST_KEYS_DOCUMENTO:
        sub = i.get(nk)
        if isinstance(sub, dict):
            d = _documento_em_dict_plano(sub)
            if d:
                return d
            for nk2 in _DOC_NEST_KEYS_DOCUMENTO:
                sub2 = sub.get(nk2)
                if isinstance(sub2, dict):
                    d = _documento_em_dict_plano(sub2)
                    if d:
                        return d
    return ""


def _primeiro_campo_texto(d, *chaves):
    if not isinstance(d, dict):
        return ""
    for chave in chaves:
        s = _valor_texto_campo(d.get(chave))
        if s:
            return s
    return ""


def _separar_numero_do_logradouro(logr: str, numero_ja: str):
    """
    Muitas bases gravam 'Rua X, 123' só em Logradouro. Extrai o sufixo numérico se Numero veio vazio.
    """
    if (numero_ja or "").strip() or not (logr or "").strip():
        return (logr or "").strip(), (numero_ja or "").strip()
    s = str(logr).strip()
    m = re.search(
        r"(?i)[,\s]+(?:n[ºo°\.]\s*|n[úu]mero\s*|num\.?\s*)?(\d+[A-Za-z]?(?:[/\-]\d+)?)\s*$",
        s,
    )
    if not m:
        m = re.search(r",\s*(\d{1,6}[A-Za-z]?)\s*$", s)
    if not m:
        return s, ""
    num = m.group(1)
    base = s[: m.start()].strip().rstrip(",")
    if base and num:
        return base, num
    return s, ""


def _endereco_linha_de_dict_plano(i):
    """Monta uma linha de endereço a partir de campos comuns (Mongo DtoPessoa / ERP)."""
    end_comp = _primeiro_campo_texto(
        i,
        "EnderecoCompleto",
        "enderecoCompleto",
        "EnderecoFormatado",
        "enderecoFormatado",
        "EnderecoResumido",
        "enderecoResumido",
    )
    if end_comp:
        return end_comp[:500]
    logr = _primeiro_campo_texto(
        i,
        "Logradouro",
        "logradouro",
        "NomeLogradouro",
        "Rua",
        "rua",
        "EnderecoLinha",
        "enderecoLinha",
    )
    # "Endereco" às vezes é string única (logradouro completo)
    if not logr:
        end_plain = i.get("Endereco") or i.get("endereco")
        if isinstance(end_plain, str) and end_plain.strip():
            logr = end_plain.strip()
    num = _primeiro_campo_texto(
        i,
        "Numero",
        "numero",
        "NumeroEndereco",
        "numeroEndereco",
        "Nr",
        "nr",
        "Nro",
        "nro",
        "NroEndereco",
        "nroEndereco",
        "EnderecoNumero",
        "enderecoNumero",
        "NumeroLogradouro",
        "numeroLogradouro",
        "Num",
        "num",
        "Predio",
        "predio",
    )
    logr, num = _separar_numero_do_logradouro(logr, num)
    comp = _primeiro_campo_texto(i, "Complemento", "complemento")
    bai = _primeiro_campo_texto(i, "Bairro", "bairro", "NomeBairro")
    cid = _primeiro_campo_texto(
        i,
        "Cidade",
        "cidade",
        "Municipio",
        "municipio",
        "NomeCidade",
        "NomeMunicipio",
    )
    uf = _primeiro_campo_texto(i, "UF", "uf", "Estado", "estado", "SiglaUF")
    cep_raw = _primeiro_campo_texto(i, "CEP", "cep", "Cep")
    parts = []
    linha1 = ", ".join(x for x in (logr, num) if x).strip(", ")
    if linha1:
        parts.append(linha1)
    if comp:
        parts.append(comp)
    if bai:
        parts.append(bai)
    if cid or uf:
        parts.append("/".join(x for x in (cid, uf) if x))
    if cep_raw:
        dcep = re.sub(r"\D", "", cep_raw)
        if len(dcep) == 8:
            parts.append(f"CEP {dcep[:5]}-{dcep[5:]}")
        else:
            parts.append(f"CEP {cep_raw}")
    return " · ".join(parts) if parts else ""


_ENDERECO_NEST_KEYS = (
    "Endereco",
    "endereco",
    "EnderecoPrincipal",
    "enderecoPrincipal",
    "DadosEndereco",
    "dadosEndereco",
    "PessoaEndereco",
    "pessoaEndereco",
    "EnderecoCobranca",
    "enderecoCobranca",
)


def _endereco_partes_vazias():
    return {k: "" for k in ("cep", "uf", "cidade", "bairro", "logradouro", "numero", "complemento")}


def _endereco_partes_extrair_flat(i):
    if not isinstance(i, dict):
        return _endereco_partes_vazias()
    cep_raw = _primeiro_campo_texto(
        i, "CEP", "cep", "Cep", "CodigoPostal", "codigoPostal", "CodigoCEP", "codigoCEP"
    )
    dcep = re.sub(r"\D", "", cep_raw)
    cep_fmt = f"{dcep[:5]}-{dcep[5:]}" if len(dcep) == 8 else (cep_raw or "")[:12]
    logr = _primeiro_campo_texto(
        i,
        "Logradouro",
        "logradouro",
        "NomeLogradouro",
        "Rua",
        "rua",
        "EnderecoLinha",
        "enderecoLinha",
    )
    if not logr:
        end_plain = i.get("Endereco") or i.get("endereco")
        if isinstance(end_plain, str) and end_plain.strip():
            logr = end_plain.strip()
    uf_v = _primeiro_campo_texto(
        i, "UF", "uf", "Estado", "estado", "SiglaUF", "SiglaEstado", "Uf"
    )
    num_v = (
        _primeiro_campo_texto(
            i,
            "Numero",
            "numero",
            "NumeroEndereco",
            "numeroEndereco",
            "Nr",
            "nr",
            "Nro",
            "nro",
            "NroEndereco",
            "nroEndereco",
            "EnderecoNumero",
            "enderecoNumero",
            "NumeroLogradouro",
            "numeroLogradouro",
            "Num",
            "num",
            "Predio",
            "predio",
        )
        or ""
    )
    logr_v, num_v = _separar_numero_do_logradouro((logr or "").strip(), num_v)
    return {
        "cep": cep_fmt[:12],
        "uf": (uf_v or "")[:2].upper(),
        "cidade": (
            _primeiro_campo_texto(
                i,
                "Cidade",
                "cidade",
                "Municipio",
                "municipio",
                "NomeCidade",
                "NomeMunicipio",
            )
            or ""
        )[:120],
        "bairro": (_primeiro_campo_texto(i, "Bairro", "bairro", "NomeBairro") or "")[:120],
        "logradouro": (logr_v or "")[:300],
        "numero": (num_v or "")[:30],
        "complemento": (
            _primeiro_campo_texto(
                i,
                "Complemento",
                "complemento",
                "ComplementoEndereco",
                "complementoEndereco",
            )
            or ""
        )[:200],
    }


def _endereco_partes_extrair(i):
    merged = _endereco_partes_vazias()
    if not isinstance(i, dict):
        return merged
    for nk in _ENDERECO_NEST_KEYS:
        sub = i.get(nk)
        if isinstance(sub, dict):
            fl = _endereco_partes_extrair_flat(sub)
            for k in merged:
                if not merged[k] and fl[k]:
                    merged[k] = fl[k]
    root = _endereco_partes_extrair_flat(i)
    for k in merged:
        if not merged[k] and root[k]:
            merged[k] = root[k]
    lr, nr = _separar_numero_do_logradouro(merged.get("logradouro") or "", merged.get("numero") or "")
    merged["logradouro"] = (lr or "")[:300]
    merged["numero"] = (nr or "")[:30]
    return merged


def _endereco_completo_texto_em_dict(i):
    if not isinstance(i, dict):
        return ""
    for nk in _ENDERECO_NEST_KEYS:
        sub = i.get(nk)
        if isinstance(sub, dict):
            t = _primeiro_campo_texto(
                sub,
                "EnderecoCompleto",
                "enderecoCompleto",
                "EnderecoFormatado",
                "enderecoFormatado",
                "EnderecoResumido",
                "enderecoResumido",
            )
            if t:
                return t
    return _primeiro_campo_texto(
        i,
        "EnderecoCompleto",
        "enderecoCompleto",
        "EnderecoFormatado",
        "enderecoFormatado",
        "EnderecoResumido",
        "enderecoResumido",
    )


def _endereco_info_para_row(i):
    """Linha resumo + partes (CEP, UF, cidade…) para sync / ClienteAgro."""
    if not isinstance(i, dict):
        return "", _endereco_partes_vazias()
    partes = _endereco_partes_extrair(i)
    end_comp = _endereco_completo_texto_em_dict(i)
    if end_comp:
        return end_comp.strip()[:500], partes
    if any((partes[k] or "").strip() for k in partes):
        from .models import compor_endereco_resumo_cliente

        return (
            compor_endereco_resumo_cliente(
                cep=partes["cep"],
                uf=partes["uf"],
                cidade=partes["cidade"],
                bairro=partes["bairro"],
                logradouro=partes["logradouro"],
                numero=partes["numero"],
                complemento=partes["complemento"],
            )[:500],
            partes,
        )
    line = _endereco_linha_de_dict_plano(i)
    return (line or "")[:500], partes


def _montar_linhas_cliente(cursor):
    out = []
    for i in cursor:
        nome = _nome_exibicao_pessoa(i)
        if not nome:
            continue
        doc = _documento_pessoa(i)
        end_linha, partes = _endereco_info_para_row(i)
        row = {
            "id": str(i.get("Id") or i.get("_id")),
            "nome": nome,
            "documento": doc or "—",
            "telefone": _telefone_pessoa(i),
            "endereco": end_linha,
        }
        row.update(partes)
        out.append(row)
    return out


def _venda_erp_api_client_from_db():
    integ = (
        IntegracaoERP.objects.filter(ativo=True, tipo_erp="venda_erp")
        .order_by("-pk")
        .first()
    )
    return VendaERPAPIClient(
        base_url=(integ.url_base.strip() if integ and integ.url_base else None),
        token=(integ.token.strip() if integ and integ.token else None),
    )


def _unwrap_pessoas_erp_response(raw):
    if raw is None:
        return []
    if isinstance(raw, list):
        return [x for x in raw if isinstance(x, dict)]
    if isinstance(raw, dict):
        if raw.get("_http_status") is not None or raw.get("_erro"):
            return []
        for key in (
            "Data",
            "data",
            "Pessoas",
            "pessoas",
            "Items",
            "items",
            "Result",
            "result",
            "Lista",
            "lista",
            "Records",
            "records",
            "Rows",
            "rows",
        ):
            d = raw.get(key)
            if isinstance(d, list):
                return [x for x in d if isinstance(x, dict)]
        if any(raw.get(k) is not None for k in ("Id", "id", "NomeFantasia", "nomeFantasia")):
            return [raw]
        for v in raw.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return v
    return []


def _linha_pessoa_erp_pdv(p):
    if not isinstance(p, dict):
        return None
    nome = (
        p.get("nomeFantasia")
        or p.get("NomeFantasia")
        or p.get("Nome")
        or p.get("nome")
        or p.get("razaoSocial")
        or p.get("RazaoSocial")
        or ""
    )
    nome = str(nome).strip()
    if len(nome) < 2:
        return None
    doc = (_documento_pessoa(p) or "").strip()
    tel = (
        p.get("celular")
        or p.get("Celular")
        or p.get("telefone")
        or p.get("Telefone")
        or ""
    )
    tel = str(tel).strip()
    pid = str(
        p.get("id")
        or p.get("Id")
        or p.get("ID")
        or p.get("PessoaID")
        or p.get("pessoaID")
        or p.get("PessoaId")
        or p.get("ClienteID")
        or p.get("clienteID")
        or p.get("Codigo")
        or p.get("codigo")
        or ""
    ).strip()
    if not pid:
        doc_digits = re.sub(r"\D", "", doc)
        if len(doc_digits) >= 11:
            pid = f"erp-doc:{doc_digits}"
        else:
            return None
    end_linha, partes = _endereco_info_para_row(p)
    row = {
        "id": pid,
        "nome": nome[:240],
        "documento": doc or "—",
        "telefone": tel,
        "endereco": end_linha,
    }
    row.update(partes)
    return row


def _linhas_pessoas_erp_list(lst):
    out = []
    for p in lst:
        row = _linha_pessoa_erp_pdv(p)
        if row:
            out.append(row)
    return out


def _linha_clienteagro_pdv(c: ClienteAgro) -> dict:
    """JSON do cliente no PDV: id compatível com checkout (ERP não recebe ObjectId do Mongo)."""
    eid = (c.externo_id or "").strip()
    orig = (c.origem_import or "").strip()
    if orig == "mongo":
        pid = f"local:{c.pk}"
    elif eid:
        pid = eid
    else:
        pid = f"local:{c.pk}"
    end = (c.endereco or "").strip()
    return {
        "id": pid,
        "nome": c.nome,
        "documento": (c.cpf or "").strip() or "—",
        "telefone": (c.whatsapp or "").strip(),
        "endereco": end,
        "logradouro": (c.logradouro or "").strip(),
        "numero": (c.numero or "").strip(),
        "bairro": (c.bairro or "").strip(),
        "cidade": (c.cidade or "").strip(),
        "uf": (c.uf or "").strip(),
        "cep": (c.cep or "").strip(),
        "plus_code": (getattr(c, "plus_code", None) or "").strip(),
        "referencia_rural": (getattr(c, "referencia_rural", None) or "").strip(),
        "maps_url_manual": (getattr(c, "maps_url_manual", None) or "").strip(),
        "cliente_agro_pk": c.pk,
    }


def _clientes_locais_agro_pdv(termo=""):
    t = (termo or "").strip()
    qs = ClienteAgro.objects.filter(ativo=True)
    if t:
        qs = qs.filter(
            Q(nome__icontains=t)
            | Q(whatsapp__icontains=t)
            | Q(cpf__icontains=t)
            | Q(endereco__icontains=t)
            | Q(plus_code__icontains=t)
        )
    qs = qs.order_by("nome")[:80]
    return [_linha_clienteagro_pdv(c) for c in qs]


def _dedupe_clientes_pdv_por_nome_doc(rows):
    seen = set()
    out = []
    for r in rows:
        k = (str(r.get("nome") or "").lower()[:120], str(r.get("documento") or ""))
        if k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out


def _clientes_lista_via_erp_api(max_total=900):
    api = _venda_erp_api_client_from_db()
    if not (getattr(api, "token", None) or "").strip():
        return []
    all_rows = []
    skip = 0
    page = 300
    while len(all_rows) < max_total:
        ok, raw = api.pessoas_get_all(page_size=page, skip=skip)
        if not ok:
            logger.warning(
                "pessoas_get_all falhou (skip=%s). Resposta: %s",
                skip,
                str(raw)[:400],
            )
            break
        unwrapped = _unwrap_pessoas_erp_response(raw)
        chunk = _linhas_pessoas_erp_list(unwrapped)
        if not chunk and skip == 0 and raw is not None:
            logger.warning(
                "ERP Pessoas/GetAll retornou OK mas nenhuma linha mapeada. "
                "Tipo=%s amostra=%s",
                type(raw).__name__,
                str(raw)[:500],
            )
        if not chunk:
            break
        all_rows.extend(chunk)
        if len(chunk) < page:
            break
        skip += page
    return all_rows


@require_GET
def api_list_customers(request):
    """Lista só ClienteAgro (sincronize em /clientes/ antes). Sem Mongo/API em tempo real."""
    cached = cache.get(API_LIST_CUSTOMERS_CACHE_KEY)
    if cached is not None:
        return JsonResponse(cached)
    qs = ClienteAgro.objects.filter(ativo=True).order_by("nome")[:8000]
    merged = [_linha_clienteagro_pdv(c) for c in qs]
    payload = {"clientes": merged}
    if settings.DEBUG:
        payload["contagem_fontes"] = {
            "cliente_agro": len(merged),
            "total_na_lista": len(merged),
        }
    cache.set(API_LIST_CUSTOMERS_CACHE_KEY, payload, API_LIST_CUSTOMERS_TTL)
    return JsonResponse(payload)


@require_GET
def api_buscar_clientes(request):
    """Busca só em ClienteAgro (ativos)."""
    termo = (request.GET.get("q") or "").strip()
    if not termo:
        return JsonResponse({"clientes": []})

    merged = _clientes_locais_agro_pdv(termo)[:45]
    payload = {"clientes": merged}
    if settings.DEBUG:
        payload["contagem_fontes"] = {
            "cliente_agro": len(merged),
        }
    return JsonResponse(payload)


def _pdv_resumo_endereco_cliente_rapido(data: dict) -> str:
    log = (data.get("logradouro") or "").strip()
    num = (data.get("numero") or "").strip()
    comp = (data.get("complemento") or "").strip()
    bai = (data.get("bairro") or "").strip()
    cid = (data.get("cidade") or "").strip()
    uf = (data.get("uf") or "").strip()
    cep = (data.get("cep") or "").strip()
    parts = [x for x in (log, num, comp, bai, cid, uf, cep) if x]
    return ", ".join(parts)[:500]


@require_POST
def api_pdv_cliente_rapido(request):
    """Cadastro rápido de ClienteAgro a partir do PDV (popup no carrinho)."""
    try:
        data = json.loads(request.body or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido."}, status=400)
    nome = (data.get("nome") or "").strip()
    if len(nome) < 2:
        return JsonResponse(
            {"ok": False, "erro": "Informe o nome do cliente (mínimo 2 caracteres)."},
            status=400,
        )
    wa_raw = (data.get("whatsapp") or data.get("telefone") or "").strip()
    wa_digits = re.sub(r"\D", "", wa_raw)
    if len(wa_digits) > 20:
        wa_digits = wa_digits[-20:]
    resumo_end = _pdv_resumo_endereco_cliente_rapido(data)
    endereco_manual = (data.get("endereco") or "").strip()[:500]
    endereco_final = endereco_manual or resumo_end
    try:
        c = ClienteAgro.objects.create(
            nome=nome[:200],
            whatsapp=wa_digits[:20] if wa_digits else "",
            endereco=endereco_final,
            cep=(data.get("cep") or "").strip()[:12],
            uf=(data.get("uf") or "").strip()[:2].upper(),
            cidade=(data.get("cidade") or "").strip()[:120],
            bairro=(data.get("bairro") or "").strip()[:120],
            logradouro=(data.get("logradouro") or "").strip()[:300],
            numero=(data.get("numero") or "").strip()[:30],
            complemento=(data.get("complemento") or "").strip()[:200],
            plus_code=(data.get("plus_code") or "").strip()[:120],
            referencia_rural=(data.get("referencia_rural") or "").strip()[:300],
            maps_url_manual=(data.get("maps_url_manual") or "").strip()[:600],
        )
    except Exception as e:
        logger.exception("api_pdv_cliente_rapido")
        return JsonResponse({"ok": False, "erro": str(e)[:500]}, status=400)
    return JsonResponse({"ok": True, "cliente": _linha_clienteagro_pdv(c)})


@require_GET
def api_autocomplete_produtos(request):
    client, db = obter_conexao_mongo()
    termo = request.GET.get("q", "")
    if db is None or len(termo) < 2:
        return JsonResponse({"sugestoes": []})

    try:
        ps = motor_de_busca_agro(termo, db, client, limit=8)
        res = [{"id": str(i.get("Id") or i.get("_id")), "nome": i.get("Nome")} for i in ps]
        return JsonResponse({"sugestoes": res})
    except Exception:
        return JsonResponse({"sugestoes": []})


@require_GET
def api_buscar_produto_id(request, id):
    client, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"erro": "Erro conexao"}, status=500)
    try:
        query = {"$or": [{"Id": id}]}
        try:
            query["$or"].append({"_id": ObjectId(id)})
        except Exception:
            pass

        p = db[client.col_p].find_one(query)
        if not p:
            return JsonResponse({"erro": "Produto nao encontrado"}, status=404)

        estoques = list(db[client.col_e].find({"ProdutoID": id}))
        ajustes_bd = AjusteRapidoEstoque.objects.filter(produto_externo_id=id).order_by(
            "deposito", "-criado_em"
        )
        ajustes_map = {}
        for aj in ajustes_bd:
            if aj.deposito not in ajustes_map:
                ajustes_map[aj.deposito] = aj

        s_c = 0.0
        s_v = 0.0
        for est in estoques:
            val = float(est.get("Saldo") or 0)
            did = str(est.get("DepositoID") or "")
            if did == client.DEPOSITO_CENTRO:
                s_c += val
            elif did == client.DEPOSITO_VILA_ELIAS:
                s_v += val

        aj_c = ajustes_map.get("centro")
        aj_v = ajustes_map.get("vila")
        saldo_f_c = (
            float(aj_c.saldo_informado) + (s_c - float(aj_c.saldo_erp_referencia))
            if aj_c
            else s_c
        )
        saldo_f_v = (
            float(aj_v.saldo_informado) + (s_v - float(aj_v.saldo_erp_referencia))
            if aj_v
            else s_v
        )

        mapa_img = {}
        query_ids = [id]
        if p.get("Codigo"):
            query_ids.append(str(p.get("Codigo")))
        try:
            for img in db["DtoImagemProduto"].find({"ProdutoID": {"$in": query_ids}}):
                val = (
                    img.get("Url")
                    or img.get("UrlImagem")
                    or img.get("Imagem")
                    or img.get("ImagemBase64")
                    or img.get("Base64")
                    or ""
                )
                if val:
                    mapa_img[str(img.get("ProdutoID"))] = val
        except Exception:
            pass

        img_url = _formatar_url_imagem(_extrair_imagem_produto(p, mapa_img, id))

        ped_qty = 0.0
        ped_sep = PedidoTransferencia.objects.filter(
            produto_externo_id=str(id), status="IMPRESSO"
        ).first()
        if ped_sep:
            ped_qty = float(ped_sep.quantidade)

        cb_id = str(_extrair_codigo_barras(p) or "").strip()
        _sub_id = str(
            p.get("SubGrupo") or p.get("Subcategoria") or p.get("NomeSubcategoria") or ""
        ).strip()
        _cat_id = p.get("NomeCategoria") or p.get("Categoria") or p.get("Grupo") or ""
        if not str(_cat_id or "").strip() and _sub_id:
            _cat_id = _sub_id
        res = {
            "id": id,
            "nome": p.get("Nome"),
            "marca": p.get("Marca") or "",
            "codigo": str(p.get("CodigoNFe") or p.get("Codigo") or ""),
            "codigo_nfe": p.get("CodigoNFe") or p.get("Codigo") or "",
            "codigo_barras": cb_id,
            "categoria": str(_cat_id or "").strip(),
            "subcategoria": _sub_id,
            "fornecedor": p.get("NomeFornecedor")
            or p.get("Fornecedor")
            or p.get("RazaoSocialFornecedor")
            or "",
            "unidade": str(p.get("Unidade") or p.get("SiglaUnidade") or "").strip(),
            "descricao": (
                str(p.get("Descricao") or "").strip()
                or str(p.get("Observacao") or "").strip()
                or str(p.get("Complemento") or "").strip()
            ),
            "preco_venda": float(p.get("ValorVenda") or p.get("PrecoVenda") or 0),
            "imagem": img_url,
            "saldo_centro": round(saldo_f_c, 2),
            "saldo_vila": round(saldo_f_v, 2),
            "saldo_erp_centro": s_c,
            "saldo_erp_vila": s_v,
            "qtd_separacao_transferencia": round(ped_qty, 3),
        }
        ov_id = ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=str(id)[:64]).first()
        _aplicar_produto_gestao_overlay_em_dict(res, ov_id)
        return JsonResponse(res)
    except Exception as e:
        return JsonResponse({"erro": str(e)}, status=500)


def _parse_hhmm_entrega(val):
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    try:
        parts = s.replace(".", ":").split(":")
        h = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 else 0
        return dtime(max(0, min(h, 23)), max(0, min(m, 59)))
    except (ValueError, TypeError, IndexError):
        return None


def _parse_troco_precisa_val(v):
    if v is None or v == "":
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("true", "1", "sim", "yes", "s"):
        return True
    if s in ("false", "0", "nao", "não", "no", "n"):
        return False
    return None


def _normalizar_digitos_telefone_agro(s: str) -> str:
    d = re.sub(r"\D", "", str(s or ""))
    if d.startswith("55") and len(d) >= 12:
        d = d[2:]
    return d


def _cliente_agro_de_body(body: dict):
    raw = body.get("cliente_agro_id")
    if raw in (None, "", 0, "0", False):
        return None
    try:
        pk = int(raw)
    except (TypeError, ValueError):
        return None
    return ClienteAgro.objects.filter(pk=pk, ativo=True).first()


def _truthy_sincronizar_cliente(v) -> bool:
    if v is True:
        return True
    if isinstance(v, str) and v.strip().lower() in ("1", "true", "sim", "yes"):
        return True
    return False


def _resolver_cliente_agro_para_sincronizar(ent: PedidoEntrega, body: dict):
    c = _cliente_agro_de_body(body)
    if c:
        return c
    if getattr(ent, "cliente_agro_id", None):
        return ent.cliente_agro
    tel = _normalizar_digitos_telefone_agro(
        body.get("telefone") if body.get("telefone") is not None else ent.telefone
    )
    if len(tel) < 10:
        return None
    matches = []
    for ca in ClienteAgro.objects.filter(ativo=True).only("id", "whatsapp"):
        if _normalizar_digitos_telefone_agro(ca.whatsapp) == tel:
            matches.append(ca)
    if len(matches) == 1:
        return matches[0]
    if len(tel) >= 11:
        tail = tel[-11:]
        tail_matches = [
            ca
            for ca in ClienteAgro.objects.filter(ativo=True).only("id", "whatsapp")
            if _normalizar_digitos_telefone_agro(ca.whatsapp).endswith(tail)
        ]
        if len(tail_matches) == 1:
            return tail_matches[0]
    return None


def _sincronizar_clienteagro_desde_modal_entrega(ent: PedidoEntrega, body: dict) -> bool:
    if not _truthy_sincronizar_cliente(body.get("sincronizar_cliente")):
        return False
    cli = _resolver_cliente_agro_para_sincronizar(ent, body)
    if not cli:
        return False
    nome = str(body.get("cliente_nome") or ent.cliente_nome or "").strip()[:200]
    if nome:
        cli.nome = nome
    tel = str(body.get("telefone") if body.get("telefone") is not None else ent.telefone).strip()
    if tel:
        cli.whatsapp = tel[:20]
    if body.get("cli_logradouro") is not None:
        cli.logradouro = str(body.get("cli_logradouro") or "")[:300].strip()
    if body.get("cli_numero") is not None:
        cli.numero = str(body.get("cli_numero") or "")[:30].strip()
    if body.get("cli_bairro") is not None:
        cli.bairro = str(body.get("cli_bairro") or "")[:120].strip()
    if body.get("cli_cidade") is not None:
        cli.cidade = str(body.get("cli_cidade") or "")[:120].strip()
    if body.get("cli_uf") is not None:
        cli.uf = str(body.get("cli_uf") or "").strip().upper()[:2]
    if body.get("cli_cep") is not None:
        cli.cep = str(body.get("cli_cep") or "").strip()[:12]
    if body.get("plus_code") is not None:
        cli.plus_code = str(body.get("plus_code") or "")[:120].strip()
    if body.get("maps_url_manual") is not None:
        cli.maps_url_manual = str(body.get("maps_url_manual") or "")[:600].strip()
    if body.get("referencia_rural") is not None:
        cli.referencia_rural = str(body.get("referencia_rural") or "")[:300].strip()
    cli.editado_local = True
    cli.save()
    ent_updated = False
    if not ent.cliente_agro_id or ent.cliente_agro_id != cli.pk:
        ent.cliente_agro = cli
        ent_updated = True
    if ent_updated:
        ent.save(update_fields=["cliente_agro"])
    return True


@ensure_csrf_cookie
@require_GET
def entregas_painel_view(request):
    tw = (getattr(settings, "PDV_ENTREGA_WHATSAPP", None) or "").strip()
    origens = [
        {
            "id": "centro",
            "label": "Centro — Av. Adhemar de Barros, 230",
            "q": (getattr(settings, "LOJA_MAPS_ORIGEM_CENTRO", None) or "").strip(),
            "link_loja": (getattr(settings, "LOJA_MAPS_LINK_CENTRO", None) or "").strip(),
        },
        {
            "id": "vila",
            "label": "Vila Elias",
            "q": (getattr(settings, "LOJA_MAPS_ORIGEM_VILA", None) or "").strip(),
            "link_loja": (getattr(settings, "LOJA_MAPS_LINK_VILA", None) or "").strip(),
        },
    ]
    return render(
        request,
        "produtos/entregas_painel.html",
        {
            "origens_maps_json": mark_safe(json.dumps(origens, ensure_ascii=False)),
            "pdv_whatsapp_loja": tw,
        },
    )


@require_POST
def api_entrega_registrar(request):
    try:
        body = json.loads(request.body.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)

    orc_raw = body.get("orc_local_id")
    try:
        orc_id = int(orc_raw) if orc_raw is not None and str(orc_raw).strip() != "" else None
    except (TypeError, ValueError):
        orc_id = None

    cliente_nome = (body.get("cliente_nome") or "").strip()[:300]
    if not cliente_nome:
        return JsonResponse({"ok": False, "erro": "cliente_nome obrigatório"}, status=400)

    itens = body.get("itens")
    if not isinstance(itens, list):
        itens = []

    campos = {
        "cliente_nome": cliente_nome,
        "telefone": (body.get("telefone") or "")[:40].strip(),
        "endereco_linha": (body.get("endereco_linha") or "")[:500].strip(),
        "plus_code": (body.get("plus_code") or "")[:120].strip(),
        "referencia_rural": (body.get("referencia_rural") or "")[:300].strip(),
        "maps_url_manual": (body.get("maps_url_manual") or "")[:600].strip(),
        "itens_json": itens,
        "total_texto": (body.get("total_texto") or "")[:48].strip(),
        "retomar_codigo": (body.get("retomar_codigo") or "")[:40].strip(),
        "operador": (body.get("operador") or "")[:120].strip(),
        "hora_prevista": _parse_hhmm_entrega(body.get("hora_prevista")),
        "forma_pagamento": (body.get("forma_pagamento") or "")[:40].strip(),
        "troco_precisa": _parse_troco_precisa_val(body.get("troco_precisa")),
    }
    if campos["forma_pagamento"] == "Dinheiro" and campos["troco_precisa"] is None:
        return JsonResponse(
            {"ok": False, "erro": "Para Dinheiro informe se precisa de troco (troco_precisa: true/false)."},
            status=400,
        )
    if campos["forma_pagamento"] and campos["forma_pagamento"] != "Dinheiro":
        campos["troco_precisa"] = None

    cli_obj = _cliente_agro_de_body(body)

    if orc_id is not None:
        existente = PedidoEntrega.objects.filter(orc_local_id=orc_id).first()
        if existente:
            for k, v in campos.items():
                setattr(existente, k, v)
            if cli_obj is not None:
                existente.cliente_agro = cli_obj
            existente.save()
            obj = existente
        else:
            obj = PedidoEntrega.objects.create(
                orc_local_id=orc_id,
                status=PedidoEntrega.Status.PENDENTE,
                cliente_agro=cli_obj,
                **campos,
            )
    else:
        obj = PedidoEntrega.objects.create(
            status=PedidoEntrega.Status.PENDENTE,
            cliente_agro=cli_obj,
            **campos,
        )

    return JsonResponse({"ok": True, "id": obj.pk})


@require_GET
def api_entregas_listar(request):
    st = (request.GET.get("status") or "").strip()
    try:
        lim = min(max(int(request.GET.get("lim") or 200), 1), 500)
    except (TypeError, ValueError):
        lim = 200
    qs = PedidoEntrega.objects.all().order_by("-criado_em")
    if st:
        qs = qs.filter(status=st)
    qs = qs[:lim]
    status_vals = {c.value for c in PedidoEntrega.Status}
    rows = []
    for e in qs:
        rows.append(
            {
                "id": e.pk,
                "status": e.status,
                "cliente_agro_id": e.cliente_agro_id,
                "cliente_nome": e.cliente_nome,
                "telefone": e.telefone,
                "endereco_linha": e.endereco_linha,
                "plus_code": e.plus_code,
                "referencia_rural": e.referencia_rural,
                "maps_url_manual": e.maps_url_manual or "",
                "itens_json": e.itens_json,
                "total_texto": e.total_texto,
                "orc_local_id": e.orc_local_id,
                "retomar_codigo": e.retomar_codigo,
                "operador": e.operador,
                "hora_prevista": e.hora_prevista.isoformat() if e.hora_prevista else None,
                "hora_saida": e.hora_saida.isoformat() if e.hora_saida else None,
                "hora_entrega": e.hora_entrega.isoformat() if e.hora_entrega else None,
                "observacoes": e.observacoes,
                "forma_pagamento": e.forma_pagamento or "",
                "troco_precisa": e.troco_precisa,
                "criado_em": e.criado_em.isoformat(),
            }
        )
    return JsonResponse({"entregas": rows, "status_opcoes": sorted(status_vals)})


@require_POST
def api_entrega_atualizar(request):
    try:
        body = json.loads(request.body.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)
    pk = body.get("id")
    try:
        pk = int(pk)
    except (TypeError, ValueError):
        return JsonResponse({"ok": False, "erro": "id inválido"}, status=400)
    ent = get_object_or_404(PedidoEntrega, pk=pk)

    status_vals = {c.value for c in PedidoEntrega.Status}
    if "cliente_agro_id" in body:
        raw = body.get("cliente_agro_id")
        if raw in (None, "", 0, "0"):
            ent.cliente_agro = None
        else:
            try:
                pk_ca = int(raw)
                ent.cliente_agro = ClienteAgro.objects.filter(pk=pk_ca, ativo=True).first()
            except (TypeError, ValueError):
                pass
    if "cliente_nome" in body and body["cliente_nome"] is not None:
        cn = str(body["cliente_nome"])[:300].strip()
        if not cn:
            return JsonResponse({"ok": False, "erro": "Nome do cliente não pode ficar vazio."}, status=400)
        ent.cliente_nome = cn
    if "status" in body and body["status"]:
        val = str(body["status"]).strip()
        if val in status_vals:
            ent.status = val
    if "telefone" in body and body["telefone"] is not None:
        ent.telefone = str(body["telefone"])[:40].strip()
    if "endereco_linha" in body and body["endereco_linha"] is not None:
        ent.endereco_linha = str(body["endereco_linha"])[:500].strip()
    if "plus_code" in body and body["plus_code"] is not None:
        ent.plus_code = str(body["plus_code"])[:120].strip()
    if "referencia_rural" in body and body["referencia_rural"] is not None:
        ent.referencia_rural = str(body["referencia_rural"])[:300].strip()
    if "maps_url_manual" in body and body["maps_url_manual"] is not None:
        ent.maps_url_manual = str(body["maps_url_manual"])[:600].strip()
    if "observacoes" in body and body["observacoes"] is not None:
        ent.observacoes = str(body["observacoes"])[:2000]
    if "hora_prevista" in body:
        ent.hora_prevista = _parse_hhmm_entrega(body.get("hora_prevista"))
    if body.get("hora_saida_now"):
        ent.hora_saida = timezone.now()
    if body.get("hora_entrega_now"):
        ent.hora_entrega = timezone.now()
    if body.get("clear_hora_saida"):
        ent.hora_saida = None
    if body.get("clear_hora_entrega"):
        ent.hora_entrega = None
    if "forma_pagamento" in body and body["forma_pagamento"] is not None:
        ent.forma_pagamento = str(body["forma_pagamento"])[:40].strip()
    if "troco_precisa" in body:
        ent.troco_precisa = _parse_troco_precisa_val(body["troco_precisa"])

    fp = (ent.forma_pagamento or "").strip()
    if fp == "Dinheiro" and ent.troco_precisa is None:
        return JsonResponse(
            {"ok": False, "erro": "Para Dinheiro informe se precisa de troco (Sim/Não)."},
            status=400,
        )
    if fp and fp != "Dinheiro":
        ent.troco_precisa = None

    ent.save()
    _sincronizar_clienteagro_desde_modal_entrega(ent, body)
    return JsonResponse({"ok": True})


@require_POST
def api_entregas_ordenar_rota(request):
    """
    Ordena paradas por proximidade (Haversine + vizinho mais próximo).
    Plus Codes (Google OLC) são decodificados no servidor; demais textos via Nominatim (1 req/s; cache).
    Links do Maps com @lat,lng usam coordenadas diretas.
    """
    try:
        body = json.loads(request.body.decode("utf-8") or "{}")
    except (json.JSONDecodeError, UnicodeDecodeError):
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)
    origem = str(body.get("origem_texto") or "").strip()
    paradas = body.get("paradas")
    if not isinstance(paradas, list):
        return JsonResponse({"ok": False, "erro": "Envie paradas (lista)."}, status=400)
    out = ordenar_entregas_por_proximidade(origem, paradas)
    if not out.get("ok"):
        st = 400 if not out.get("paradas") else 422
        return JsonResponse(out, status=st)
    return JsonResponse(out)


@require_GET
def relatorios_hub(request):
    return render(request, "produtos/relatorios_hub.html")


# Janela fixa (dias) para classificar "próximo ao vencimento" no relatório; não é filtro do utilizador.
ALERTA_VALIDADE_DIAS = 30


def _relatorio_validade_saldo_cv_e_vencido(
    mapa: dict,
    produto_id: str,
    status_linha: str,
    *,
    saldo_lote_local: float | None = None,
) -> tuple[float | None, float | None]:
    """
    Saldo operacional (centro + vila) e parte considerada «vencida».
    Com um único lote/validade por produto no overlay, se a data já passou
    o stock total conta como vencido; caso contrário 0. Sem mapa (Mongo fora) → (None, None),
    exceto se `saldo_lote_local` (Agro) for informado: usa saldo do lote no SQL.
    """
    if not mapa and saldo_lote_local is not None:
        total = float(saldo_lote_local)
        if status_linha == "vencido":
            return total, total
        return total, 0.0
    if not mapa:
        return None, None
    s = mapa.get(str(produto_id))
    if s is None:
        return 0.0, 0.0
    total = float(s.get("saldo_centro", 0) or 0) + float(s.get("saldo_vila", 0) or 0)
    if status_linha == "vencido":
        return total, total
    return total, 0.0


def _first_day_of_next_month(d: date) -> date:
    if d.month == 12:
        return date(d.year + 1, 1, 1)
    return date(d.year, d.month + 1, 1)


def _bounds_mes(ano: int, mes: int) -> tuple[date, date] | tuple[None, None]:
    if not (1 <= mes <= 12) or not (2000 <= ano <= 3000):
        return None, None
    from calendar import monthrange

    u = monthrange(ano, mes)[1]
    return date(ano, mes, 1), date(ano, mes, u)


def _bounds_mes_atual(hoje: date) -> tuple[date, date]:
    a, b = _bounds_mes(hoje.year, hoje.month)
    assert a is not None and b is not None
    return a, b


def _bounds_proximo_mes(hoje: date) -> tuple[date, date]:
    f = _first_day_of_next_month(hoje)
    a, b = _bounds_mes(f.year, f.month)
    if a is None or b is None:
        return _bounds_mes_atual(hoje)
    return a, b


@ensure_csrf_cookie
@require_GET
def relatorios_validade(request):
    filtro_status = (request.GET.get("status") or "todos").strip() or "todos"
    if filtro_status not in ("todos", "alerta", "vencido"):
        filtro_status = "todos"
    periodo = (request.GET.get("periodo") or "todos").strip() or "todos"
    if periodo not in ("todos", "mes_atual", "proximo_mes", "mes"):
        periodo = "todos"
    hoje = timezone.now().date()
    ref_mes_ano = (request.GET.get("ref_mes_ano") or "").strip()[:7]
    if not ref_mes_ano:
        ref_mes_ano = f"{hoje.year:04d}-{hoje.month:02d}"

    inicio_p: date | None = None
    fim_p: date | None = None
    if periodo == "mes_atual":
        inicio_p, fim_p = _bounds_mes_atual(hoje)
    elif periodo == "proximo_mes":
        a, b = _bounds_proximo_mes(hoje)
        inicio_p, fim_p = a, b
    elif periodo == "mes":
        try:
            y_str, m_str = ref_mes_ano.split("-", 1)
            a, b = _bounds_mes(int(y_str), int(m_str))
            if a is not None and b is not None:
                inicio_p, fim_p = a, b
        except (ValueError, TypeError):
            inicio_p, fim_p = _bounds_mes_atual(hoje)
    is_80 = (request.GET.get("print") or "").strip() == "80mm"
    somente_com_estoque = (request.GET.get("somente_com_estoque") or "").strip() in (
        "1",
        "on",
        "true",
        "yes",
        "sim",
    )

    overlays = list(
        ProdutoGestaoOverlayAgro.objects.filter(
            Q(cadastro_extras__has_key="validade") | Q(lotes__isnull=False)
        )
        .distinct()
        .prefetch_related("lotes")
    )
    pids = [str(ov.produto_externo_id) for ov in overlays]
    saldos_map: dict = {}
    estoque_mongo_ok = False
    client, db = obter_conexao_mongo()
    if client is not None and db is not None and pids:
        try:
            saldos_map = _mapa_saldos_finais_por_produtos(db, client, pids)
            estoque_mongo_ok = True
        except Exception:
            saldos_map = {}
            estoque_mongo_ok = False

    lista_validade: list[dict] = []
    totais_saldo_c_v = 0.0
    totais_saldo_vencido = 0.0

    def anexa_linha_validade(
        row: dict, *, saldo_lote_local: float | None = None
    ) -> None:
        stf = str(row.get("status") or "")
        pid = str(row.get("produto_id") or "")
        if estoque_mongo_ok and saldos_map:
            scv, sv = _relatorio_validade_saldo_cv_e_vencido(
                saldos_map, pid, stf
            )
        elif not estoque_mongo_ok and saldo_lote_local is not None:
            scv, sv = _relatorio_validade_saldo_cv_e_vencido(
                {},
                pid,
                stf,
                saldo_lote_local=saldo_lote_local,
            )
        else:
            scv, sv = _relatorio_validade_saldo_cv_e_vencido(
                saldos_map, pid, stf
            )
        # ERP 0 com Id divergente do código do relatório é comum: mostrar o saldo do lote (Agro).
        if estoque_mongo_ok and row.get("lote_id") and (row.get("lote_qtd") is not None):
            try:
                lq = float(row.get("lote_qtd") or 0)
            except (TypeError, ValueError):
                lq = 0.0
            if lq > 0 and (scv is None or float(scv) == 0.0):
                scv, sv = _relatorio_validade_saldo_cv_e_vencido(
                    {},
                    pid,
                    stf,
                    saldo_lote_local=lq,
                )
        row["saldo_c_v"] = scv
        row["saldo_vencido"] = sv
        if (
            somente_com_estoque
            and estoque_mongo_ok
            and scv is not None
            and scv <= 0
        ):
            return
        if somente_com_estoque and (not estoque_mongo_ok) and saldo_lote_local is not None:
            try:
                if float(saldo_lote_local) <= 0:
                    return
            except (TypeError, ValueError):
                return
        lista_validade.append(row)

    for ov in overlays:
        ex = (
            ov.cadastro_extras
            if isinstance(getattr(ov, "cadastro_extras", None), dict)
            else {}
        )
        nome_base = (getattr(ov, "nome", None) or "").strip() or f"Produto {ov.produto_externo_id}"
        lotes_ordenados = list(ov.lotes.all())
        lotes_ordenados.sort(
            key=lambda L: (L.data_validade, L.pk or 0)
        )

        if ex.get("validade_alerta") and not lotes_ordenados:
            if filtro_status != "todos":
                continue
            lr = ex.get("lote")
            lote_raw = str(lr).strip()[:80] if lr is not None else ""
            lote = lote_raw or "N/A"
            validade_msg = str(
                ex.get("validade_msg") or "Erro no ficheiro original."
            )[:200]
            anexa_linha_validade(
                {
                    "produto_id": ov.produto_externo_id,
                    "nome": nome_base,
                    "lote": lote,
                    "lote_raw": lote_raw,
                    "lote_id": None,
                    "data_validade": hoje,
                    "dias_restantes": -999,
                    "dias_restantes_abs": 999,
                    "status": "erro_importacao",
                    "validade_alerta": True,
                    "validade_msg": validade_msg,
                }
            )
            continue

        for el in lotes_ordenados:
            data_venc = el.data_validade
            if inicio_p is not None and fim_p is not None and not (
                inicio_p <= data_venc <= fim_p
            ):
                continue
            dias_restantes = (data_venc - hoje).days
            st = (
                "vencido"
                if dias_restantes < 0
                else (
                    "alerta"
                    if dias_restantes <= ALERTA_VALIDADE_DIAS
                    else "ok"
                )
            )
            if filtro_status != "todos" and st != filtro_status:
                continue
            lote_raw = str(el.lote_codigo or "").strip()[:100]
            lote = lote_raw or "N/A"
            qtd = float(el.quantidade_atual or 0)
            anexa_linha_validade(
                {
                    "produto_id": ov.produto_externo_id,
                    "nome": nome_base,
                    "lote": lote,
                    "lote_raw": lote_raw,
                    "lote_id": el.pk,
                    "lote_qtd": qtd,
                    "data_validade": data_venc,
                    "dias_restantes": dias_restantes,
                    "dias_restantes_abs": abs(dias_restantes),
                    "status": st,
                    "validade_alerta": bool(ex.get("validade_alerta")),
                    "validade_msg": str(ex.get("validade_msg") or "")[:200],
                },
                saldo_lote_local=qtd if not estoque_mongo_ok else None,
            )

        if lotes_ordenados:
            continue

        validade_str = ex.get("validade")
        if not validade_str:
            continue
        try:
            data_venc = datetime.strptime(
                str(validade_str).strip()[:10], "%Y-%m-%d"
            ).date()
        except (ValueError, TypeError):
            continue
        if inicio_p is not None and fim_p is not None and not (
            inicio_p <= data_venc <= fim_p
        ):
            continue
        dias_restantes = (data_venc - hoje).days
        st = (
            "vencido"
            if dias_restantes < 0
            else (
                "alerta"
                if dias_restantes <= ALERTA_VALIDADE_DIAS
                else "ok"
            )
        )

        if filtro_status != "todos" and st != filtro_status:
            continue

        nome = nome_base
        lr = ex.get("lote")
        if lr is not None:
            lote_raw = str(lr).strip()[:80]
        else:
            lote_raw = ""
        lote = lote_raw or "N/A"
        validade_alerta = bool(ex.get("validade_alerta"))
        validade_msg = str(ex.get("validade_msg") or "")[:200]
        anexa_linha_validade(
            {
                "produto_id": ov.produto_externo_id,
                "nome": nome,
                "lote": lote,
                "lote_raw": lote_raw,
                "lote_id": None,
                "data_validade": data_venc,
                "dias_restantes": dias_restantes,
                "dias_restantes_abs": abs(dias_restantes),
                "status": st,
                "validade_alerta": validade_alerta,
                "validade_msg": validade_msg,
            }
        )

    if lista_validade and estoque_mongo_ok:
        totais_saldo_c_v = 0.0
        totais_saldo_vencido = 0.0
        por_produto: dict[str, dict] = {}
        for r in lista_validade:
            pid = str(r.get("produto_id") or "")
            if not pid or r.get("status") == "erro_importacao":
                continue
            scv = r.get("saldo_c_v")
            sv = r.get("saldo_vencido")
            if pid not in por_produto:
                por_produto[pid] = {"c_v": scv, "venc": sv}
            else:
                cur = por_produto[pid]
                if scv is not None:
                    if cur.get("c_v") is None:
                        cur["c_v"] = scv
                if sv is not None:
                    a = float(cur["venc"] or 0) if cur.get("venc") is not None else 0.0
                    b = float(sv or 0)
                    cur["venc"] = max(a, b)
        for _pid, t in por_produto.items():
            c = t.get("c_v")
            v = t.get("venc")
            if c is not None:
                totais_saldo_c_v += float(c)
            if v is not None:
                totais_saldo_vencido += float(v)
    elif lista_validade and not estoque_mongo_ok:
        totais_saldo_c_v = 0.0
        totais_saldo_vencido = 0.0
        for r in lista_validade:
            if r.get("status") == "erro_importacao":
                continue
            c = r.get("saldo_c_v")
            v = r.get("saldo_vencido")
            if c is not None:
                totais_saldo_c_v += float(c)
            if v is not None:
                totais_saldo_vencido += float(v)

    lista_validade.sort(
        key=lambda x: (0, x["produto_id"], str(x.get("lote") or ""))
        if x.get("status") == "erro_importacao"
        else (
            1,
            x["data_validade"],
            x["produto_id"],
            str(x.get("lote") or ""),
        )
    )
    login_href = f"{reverse('admin:login')}?{urlencode({'next': request.get_full_path()})}"
    exibir_rodape_totais = bool(lista_validade) and (
        estoque_mongo_ok
        or (not estoque_mongo_ok and (totais_saldo_c_v or totais_saldo_vencido))
        or any(r.get("lote_id") for r in lista_validade)
    )
    ctx = {
        "produtos": lista_validade,
        "estoque_mongo_ok": estoque_mongo_ok,
        "exibir_rodape_totais": exibir_rodape_totais,
        "totais_estoque": {
            "c_v": totais_saldo_c_v,
            "vencido": totais_saldo_vencido,
        },
        "filtros": {
            "status": filtro_status,
            "periodo": periodo,
            "ref_mes_ano": ref_mes_ano,
            "somente_com_estoque": somente_com_estoque,
        },
        "pode_editar_validade": getattr(request, "user", None) and request.user.is_authenticated,
        "url_api_overlay_salvar": reverse("api_produtos_gestao_overlay_salvar"),
        "url_api_lote_upsert": reverse("api_overlay_lote_adicionar"),
        "login_validade_href": login_href,
    }
    if is_80:
        return render(request, "produtos/relatorios_validade_80mm.html", ctx)
    return render(request, "produtos/relatorios_validade.html", ctx)
