import csv
import json
import logging
import re
import time
import unicodedata
from datetime import datetime, timedelta
from decimal import Decimal
from bson import ObjectId

from django.conf import settings
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.db.models import Count, Q, Sum
from django.shortcuts import get_object_or_404, redirect, render
from django.http import HttpResponse, JsonResponse
from django.urls import reverse
from django.core.cache import cache
from django.views.decorators.http import require_GET, require_POST
from django.utils import timezone
from django.db import transaction

from base.models import Empresa, PerfilUsuario, IntegracaoERP
from estoque.models import AjusteRapidoEstoque
from .forms import ClienteAgroForm
from .models import ClienteAgro, ItemVendaAgro, SessaoCaixa, VendaAgro
from integracoes.texto import normalizar, expandir_tokens
from integracoes.venda_erp_mongo import VendaERPMongoClient
from integracoes.venda_erp_api import VendaERPAPIClient
from .mongo_vendas_util import _filtro_venda_ativa_mongo


logger = logging.getLogger(__name__)

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


def _codigo_exact_conditions(termo_limpo):
    conds = [
        {"Codigo": {"$regex": _regex_exato_ci(termo_limpo)}},
        {"CodigoNFe": {"$regex": _regex_exato_ci(termo_limpo)}},
        {"CodigoBarras": {"$regex": _regex_exato_ci(termo_limpo)}},
        {"EAN_NFe": {"$regex": _regex_exato_ci(termo_limpo)}},
    ]

    if termo_limpo.isdigit():
        try:
            numero = int(termo_limpo)
            conds.extend([
                {"Codigo": numero},
                {"CodigoNFe": numero},
                {"CodigoBarras": numero},
                {"EAN_NFe": numero},
            ])
        except Exception:
            pass

    return conds


def _codigo_prefix_conditions(termo_limpo):
    return [
        {"Codigo": {"$regex": _regex_inicio_ci(termo_limpo)}},
        {"CodigoNFe": {"$regex": _regex_inicio_ci(termo_limpo)}},
        {"CodigoBarras": {"$regex": _regex_inicio_ci(termo_limpo)}},
        {"EAN_NFe": {"$regex": _regex_inicio_ci(termo_limpo)}},
    ]


def _extrair_codigo_barras(p):
    return (
        p.get("CodigoBarras")
        or p.get("EAN_NFe")
        or p.get("EAN")
        or p.get("CodigoDeBarras")
        or ""
    )


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


# Catálogo PDV: um snapshot por dia civil (TIME_ZONE) + invalidação manual. Estoque ao vivo via /api/pdv/saldos/.
CATALOGO_PDV_CACHE_ENTRY_KEY = "pdv_catalogo_produtos_por_dia_v1"

# Snapshot de saldos: vários caixas/abas compartilham; TTL curto protege o Mongo sem atrasar o PDV.
_SALDOS_PDV_CACHE_KEY = "pdv_saldos_compacto_snapshot_v1"
_SALDOS_PDV_CACHE_TTL = 5

_METRICAS_PDV_BUCKETS_INVALIDAR = 4
_METRICAS_PDV_DIAS_COMUNS = (7, 14, 21, 28, 30, 45, 60, 90, 120, 180, 365)


def _pdv_metricas_cache_key(dias: int, bucket: int) -> str:
    return f"pdv_metricas_v3_{dias}_{bucket}"


def _invalidar_cache_saldos_pdv():
    cache.delete(_SALDOS_PDV_CACHE_KEY)


def _invalidar_cache_metricas_pdv():
    b = int(time.time() // 300)
    for dias in _METRICAS_PDV_DIAS_COMUNS:
        for bb in range(b, b - _METRICAS_PDV_BUCKETS_INVALIDAR - 1, -1):
            cache.delete(_pdv_metricas_cache_key(dias, bb))


def _invalidar_caches_apos_ajuste_pin():
    _invalidar_cache_saldos_pdv()
    cache.delete(CATALOGO_PDV_CACHE_ENTRY_KEY)
    cache.delete(_CACHE_MEDIAS_VENDA_ENTRY)
    _invalidar_cache_metricas_pdv()


def _metricas_vendas_agregadas_por_produto(db, dias_media: int):
    """
    Uma passagem em DtoVendaProduto (com cabeçalhos no intervalo):
    - totais no período [now-dias_media, now] para média diária
    - últimos 7 dias vs 7 dias anteriores (variação semanal)
    """
    now = datetime.now()
    t_m = now - timedelta(days=dias_media)
    t_w0 = now - timedelta(days=7)
    t_w1 = now - timedelta(days=14)
    limite = min(t_m, t_w1)
    q = {"Data": {"$gte": limite}, **_filtro_venda_ativa_mongo()}
    vendas = list(db["DtoVenda"].find(q, {"Id": 1, "_id": 1, "Data": 1}))
    if not vendas:
        return {}, {}, {}
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
    for item in db["DtoVendaProduto"].find(query_itens):
        pid = str(item.get("ProdutoID") or "")
        if not pid or pid == "None":
            continue
        vid_raw = item.get("VendaID")
        vid = str(vid_raw) if vid_raw is not None else ""
        dt = vmap.get(vid)
        if dt is None:
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
    return media_tot, w0, w1


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
                ]
                if any(x in k_lower for x in bad_keys):
                    continue

                if isinstance(v, (dict, list)):
                    traverse(v)
                else:
                    good_cost_indicators = [
                        "custo", "compra", "reposicao", "fornecedor", "entrada",
                        "valor", "preco", "total", "final", "bruto", "liquido",
                        "medio", "acrescimo", "despesa", "frete", "seguro",
                        "imposto", "tributo", "icms", "ipi", "pis", "cofins",
                        "real", "efetivo",
                    ]
                    if any(x in k_lower for x in good_cost_indicators):
                        if v is not None:
                            try:
                                val_f = float(str(v).replace(",", "."))
                                if preco_venda_val > 0 and val_f == preco_venda_val:
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
    heuristic = _heuristic_custo_maximo_doc(p, preco_custo_val, preco_venda_val)
    explicit = _custo_final_explicito_campos(p, preco_venda_val)
    final = max(heuristic, explicit or 0.0)
    return {"preco_custo": preco_custo_val, "preco_custo_final": final}


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
    for k in ("id", "documento", "telefone", "nome", "razao_social"):
        v = raw.get(k)
        if v is not None and str(v).strip():
            out[k] = str(v).strip()[:300]
    return out if out else None


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


# --- VIEWS DE PÁGINA ---
def consulta_produtos(request):
    ctx = {}
    if request.GET.get("reabrir") == "1":
        draft = request.session.get("pdv_checkout")
        if draft and draft.get("itens"):
            ctx["pdv_reabrir_data"] = draft
    ctx["caixa_aberto"] = _obter_sessao_caixa_aberta(request)
    return render(request, "produtos/consulta_produtos.html", ctx)


def pdv_checkout(request):
    draft = request.session.get("pdv_checkout")
    if not draft or not draft.get("itens"):
        return redirect("consulta_produtos")
    total = Decimal("0")
    disp_itens = []
    for i in draft["itens"]:
        try:
            q = Decimal(str(i.get("qtd") or 0))
            p = Decimal(str(i.get("preco") or 0))
            lin = (q * p).quantize(Decimal("0.01"))
            total += lin
            row = dict(i)
            row["valor_linha"] = lin
            disp_itens.append(row)
        except Exception:
            continue
    draft_display = {**draft, "itens": disp_itens}
    return render(
        request,
        "produtos/pdv_checkout.html",
        {
            "draft": draft,
            "draft_display": draft_display,
            "total_fmt": total.quantize(Decimal("0.01")),
        },
    )


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
            "enviado_erp",
            "usuario",
            "sessao_caixa_id",
        ]
    )
    for v in qs:
        w.writerow(
            [
                v.pk,
                v.criado_em.strftime("%Y-%m-%d %H:%M:%S"),
                v.cliente_nome,
                v.cliente_id_erp,
                v.cliente_documento,
                v.forma_pagamento,
                str(v.total).replace(".", ","),
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
    return render(request, "produtos/transferencias.html")


def compras_view(request):
    return render(request, "produtos/compras.html")


def ajuste_mobile_view(request):
    if not request.session.get("mobile_auth"):
        return render(request, "produtos/ajuste_mobile_login.html")
    return render(request, "produtos/mobile_ajuste.html")


# --- MOTOR DE BUSCA ÚNICO ---
def motor_de_busca_agro(termo_original, db, client, limit=20):
    termo_original = str(termo_original or "").strip()
    if not termo_original:
        return []

    termo_limpo = _somente_alnum(termo_original)
    palavras = [p for p in termo_original.split() if p]
    base_filter = {"CadastroInativo": {"$ne": True}}

    candidatos = []
    vistos = set()

    def adicionar(lista):
        for item in lista:
            pid = str(item.get("Id") or item.get("_id"))
            if pid not in vistos:
                vistos.add(pid)
                candidatos.append(item)

    # 1) Código / barras exato
    if termo_limpo and _termo_parece_codigo(termo_original):
        query_cod_exato = {
            **base_filter,
            "$or": [
                {"Codigo": _regex_exato_ci(termo_limpo)},
                {"CodigoNFe": _regex_exato_ci(termo_limpo)},
                {"CodigoBarras": _regex_exato_ci(termo_limpo)},
                {"EAN_NFe": _regex_exato_ci(termo_limpo)},
            ]
        }

        if termo_limpo.isdigit():
            try:
                numero = int(termo_limpo)
                query_cod_exato["$or"].extend([
                    {"Codigo": numero},
                    {"CodigoNFe": numero},
                    {"CodigoBarras": numero},
                    {"EAN_NFe": numero},
                ])
            except Exception:
                pass

        exatos = list(db[client.col_p].find(query_cod_exato).limit(max(limit, 10)))
        if exatos:
            return exatos[:limit]

        # 1.1) Prefixo de código
        query_cod_prefixo = {
            **base_filter,
            "$or": [
                {"Codigo": _regex_inicio_ci(termo_limpo)},
                {"CodigoNFe": _regex_inicio_ci(termo_limpo)},
                {"CodigoBarras": _regex_inicio_ci(termo_limpo)},
                {"EAN_NFe": _regex_inicio_ci(termo_limpo)},
            ]
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
            condicoes_and.append({
                "$or": [
                    {"BuscaTexto": {"$in": regex_tokens}},
                    {"Nome": {"$in": regex_tokens}},
                    {"Marca": {"$in": regex_tokens}},
                    {"NomeNormalizado": {"$in": regex_tokens}},
                    {"Codigo": {"$in": regex_tokens}},
                    {"CodigoNFe": {"$in": regex_tokens}},
                    {"CodigoBarras": {"$in": regex_tokens}},
                    {"EAN_NFe": {"$in": regex_tokens}},
                ]
            })

    if condicoes_and:
        adicionar(list(db[client.col_p].find({
            **base_filter,
            "$and": condicoes_and
        }).limit(160)))

    # 3) Fallback por frase inteira
    if len(candidatos) < limit:
        termo_regex = _regex_contem_ci(termo_original)
        adicionar(list(db[client.col_p].find({
            **base_filter,
            "$or": [
                {"Nome": termo_regex},
                {"BuscaTexto": termo_regex},
                {"Marca": termo_regex},
                {"NomeNormalizado": termo_regex},
                {"Codigo": termo_regex},
                {"CodigoNFe": termo_regex},
                {"CodigoBarras": termo_regex},
                {"EAN_NFe": termo_regex},
            ]
        }).limit(160)))

    # 3b) Qualquer palavra/token bate (recall alto; útil com BuscaTexto defasado)
    if len(candidatos) < max(limit, 24) and palavras:
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
            }).limit(220)))

    # 4) Ordenação de relevância
    termo_norm = normalizar(termo_original)
    termo_limpo_lower = termo_limpo.lower()

    def score(p):
        nome = str(p.get("Nome") or "")
        marca = str(p.get("Marca") or "")
        codigo = str(p.get("Codigo") or "")
        codigo_nfe = str(p.get("CodigoNFe") or "")
        codigo_barras = str(_extrair_codigo_barras(p) or "")

        nome_norm = normalizar(nome)
        marca_norm = normalizar(marca)
        codigo_alnum = _somente_alnum(codigo).lower()
        codigo_nfe_alnum = _somente_alnum(codigo_nfe).lower()
        barras_alnum = _somente_alnum(codigo_barras).lower()

        s = 0

        if termo_limpo_lower:
            if codigo_alnum == termo_limpo_lower:
                s += 5000
            if codigo_nfe_alnum == termo_limpo_lower:
                s += 4900
            if barras_alnum == termo_limpo_lower:
                s += 5200

            if codigo_alnum.startswith(termo_limpo_lower):
                s += 1800
            if codigo_nfe_alnum.startswith(termo_limpo_lower):
                s += 1700
            if barras_alnum.startswith(termo_limpo_lower):
                s += 1900

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


def _parse_etiqueta_balanca_ean13_br(q: str):
    """
    Padrão comum de balança: 2 C C C C 0 T T T T T T DV (EAN-13).
    C = código interno (4 dígitos), T = valor total em centavos (6 dígitos, 2 decimais).
    """
    d = re.sub(r"\D", "", str(q or ""))
    if len(d) != 13 or d[0] != "2":
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
    """Resolve produto pelos 4 dígitos do código na etiqueta."""
    col = db[client.col_p]
    base = {"CadastroInativo": {"$ne": True}}
    variants = set()
    variants.add(cod4)
    variants.add(cod4.lstrip("0") or "0")
    for z in (5, 6, 7):
        variants.add(cod4.zfill(z))
    ors = []
    for v in variants:
        ors.append({"Codigo": v})
        ors.append({"CodigoNFe": v})
        ors.append({"CodigoBarras": v})
        ors.append({"EAN_NFe": v})
        if v.isdigit():
            try:
                ors.append({"Codigo": int(v)})
            except Exception:
                pass
    try:
        return col.find_one({**base, "$or": ors})
    except Exception:
        return None


# --- APIs DE BUSCA ---
@require_GET
def api_buscar_produtos(request):
    q = request.GET.get("q", "").strip()
    client, db = obter_conexao_mongo()
    if db is None or not q:
        return JsonResponse({"produtos": []})

    try:
        preco_por_id = {}
        bal = _parse_etiqueta_balanca_ean13_br(q)
        if bal:
            cod4, preco_etiqueta = bal
            p_bal = _buscar_produto_por_codigo_interno_balanca(db, client, cod4)
            if p_bal:
                pid_b = str(p_bal.get("Id") or p_bal.get("_id"))
                preco_por_id[pid_b] = preco_etiqueta
                prods = [p_bal]
            else:
                prods = motor_de_busca_agro(q, db, client, limit=80)
        else:
            prods = motor_de_busca_agro(q, db, client, limit=80)
        p_ids = [str(p.get("Id") or p["_id"]) for p in prods]

        medias_map = {}
        try:
            medias_map = _obter_mapa_medias_venda_cache(db)
        except Exception:
            logger.warning("api_buscar_produtos: medias indisponíveis", exc_info=True)

        estoque_map = {}
        try:
            if p_ids:
                estoques = list(db[client.col_e].find({"ProdutoID": {"$in": p_ids}}))
                estoque_map = _mapear_estoques_por_produto(estoques, client)
        except Exception:
            logger.warning("api_buscar_produtos: estoque indisponível — retornando saldo 0", exc_info=True)

        ajustes_map = {}
        try:
            if p_ids:
                ajustes_bd = AjusteRapidoEstoque.objects.filter(produto_externo_id__in=p_ids)
                ajustes_map = {(aj.produto_externo_id, aj.deposito): aj for aj in ajustes_bd}
        except Exception:
            logger.warning("api_buscar_produtos: ajustes PIN indisponíveis", exc_info=True)

        res = []
        for p in prods:
            pid = str(p.get("Id") or p["_id"])

            saldo_centro_erp = float(estoque_map.get(pid, {}).get("centro", 0.0))
            saldo_vila_erp = float(estoque_map.get(pid, {}).get("vila", 0.0))

            ac = ajustes_map.get((pid, "centro"))
            av = ajustes_map.get((pid, "vila"))

            saldo_centro = (
                float(ac.saldo_informado) + (saldo_centro_erp - float(ac.saldo_erp_referencia))
                if ac else saldo_centro_erp
            )
            saldo_vila = (
                float(av.saldo_informado) + (saldo_vila_erp - float(av.saldo_erp_referencia))
                if av else saldo_vila_erp
            )

            codigo = p.get("Codigo") or ""
            codigo_nfe = p.get("CodigoNFe") or codigo or ""
            codigo_barras = _extrair_codigo_barras(p)
            media_d = float(medias_map.get(pid, 0.0))
            pv = float(preco_por_id[pid]) if pid in preco_por_id else float(p.get("ValorVenda") or p.get("PrecoVenda") or 0)

            res.append({
                "id": pid,
                "nome": p.get("Nome"),
                "marca": p.get("Marca") or "",
                "fornecedor": p.get("NomeFornecedor")
                or p.get("Fornecedor")
                or p.get("RazaoSocialFornecedor")
                or p.get("Fabricante")
                or "",
                "categoria": p.get("NomeCategoria")
                or p.get("Categoria")
                or p.get("Grupo")
                or p.get("SubGrupo")
                or "",
                "codigo": codigo,
                "codigo_nfe": codigo_nfe,
                "codigo_barras": codigo_barras,
                "preco_venda": pv,
                "imagem": _formatar_url_imagem(_extrair_imagem_produto(p, {}, pid)),
                "saldo_centro": round(saldo_centro, 2),
                "saldo_vila": round(saldo_vila, 2),
                "saldo_centro_erp": round(saldo_centro_erp, 2),
                "saldo_vila_erp": round(saldo_vila_erp, 2),
                "saldo_erp_centro": round(saldo_centro_erp, 2),  # compatibilidade com mobile atual
                "saldo_erp_vila": round(saldo_vila_erp, 2),
                "media_venda_diaria_30d": media_d,
                "preco_etiqueta_balanca": bool(pid in preco_por_id),
            })

        res.sort(
            key=lambda r: (
                -float(r.get("media_venda_diaria_30d") or 0),
                str(r.get("nome") or "").lower(),
            )
        )

        exact = bool(preco_por_id) and len(res) == 1
        return JsonResponse({"produtos": res, "exact_barcode_match": exact})
    except Exception as e:
        return JsonResponse({"erro": str(e)}, status=500)


@require_GET
def api_buscar_compras(request):
    q = request.GET.get("q", "").strip()
    client, db = obter_conexao_mongo()
    if db is None or not q:
        return JsonResponse({"produtos": []})

    try:
        prods = motor_de_busca_agro(q, db, client, limit=70)
        p_ids = [str(p.get("Id") or p["_id"]) for p in prods]

        estoque_map = {}
        try:
            if p_ids:
                estoques = list(db[client.col_e].find({"ProdutoID": {"$in": p_ids}}))
                estoque_map = _mapear_estoques_por_produto(estoques, client)
        except Exception:
            logger.warning("api_buscar_compras: estoque indisponível", exc_info=True)

        ajustes_map = {}
        try:
            if p_ids:
                ajustes_bd = AjusteRapidoEstoque.objects.filter(produto_externo_id__in=p_ids)
                ajustes_map = {(aj.produto_externo_id, aj.deposito): aj for aj in ajustes_bd}
        except Exception:
            logger.warning("api_buscar_compras: ajustes indisponíveis", exc_info=True)

        res = []
        for p in prods:
            pid = str(p.get("Id") or p["_id"])

            saldo_centro_erp = float(estoque_map.get(pid, {}).get("centro", 0.0))
            saldo_vila_erp = float(estoque_map.get(pid, {}).get("vila", 0.0))

            ac = ajustes_map.get((pid, "centro"))
            av = ajustes_map.get((pid, "vila"))

            saldo_centro = (
                float(ac.saldo_informado) + (saldo_centro_erp - float(ac.saldo_erp_referencia))
                if ac else saldo_centro_erp
            )
            saldo_vila = (
                float(av.saldo_informado) + (saldo_vila_erp - float(av.saldo_erp_referencia))
                if av else saldo_vila_erp
            )

            custos = _custos_compra_produto(p)
            preco_venda = float(p.get("ValorVenda") or p.get("PrecoVenda") or 0)
            codigo = p.get("Codigo") or ""
            codigo_nfe = p.get("CodigoNFe") or codigo or ""
            codigo_barras = _extrair_codigo_barras(p)

            res.append({
                "id": pid,
                "nome": p.get("Nome"),
                "marca": p.get("Marca") or "",
                "codigo": codigo,
                "codigo_nfe": codigo_nfe,
                "codigo_barras": codigo_barras,
                "preco_custo": custos["preco_custo"],
                "preco_custo_acrescimo": custos["preco_custo_final"],
                "preco_custo_final": custos["preco_custo_final"],
                "preco_venda": preco_venda,
                "imagem": _formatar_url_imagem(_extrair_imagem_produto(p, {}, pid)),
                "saldo_centro": round(saldo_centro, 2),
                "saldo_vila": round(saldo_vila, 2),
                "saldo_centro_erp": round(saldo_centro_erp, 2),
                "saldo_vila_erp": round(saldo_vila_erp, 2),
            })

        return JsonResponse({"produtos": res})
    except Exception as e:
        return JsonResponse({"erro": str(e)}, status=500)


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


def _mensagem_pedido_erp_indica_falha(msg) -> bool:
    s = str(_desembrulhar_texto_json_recursivo(msg)).strip().lower()
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
    )
    return any(m in folded for m in markers)


def _linha_item_pedido_erp(db, client_m, item: dict) -> dict | None:
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
    return linha


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


def _persistir_venda_agro(request, data, raw_itens, erp_http_status, erp_resposta_raw, enviado_erp_com_sucesso):
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
    forma = str(data.get("forma_pagamento") or "").strip()[:80]

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

    with transaction.atomic():
        v = VendaAgro.objects.create(
            cliente_nome=cliente[:300],
            cliente_id_erp=cid[:32],
            cliente_documento=re.sub(r"\D", "", doc)[:20],
            total=total.quantize(Decimal("0.01")),
            forma_pagamento=forma,
            enviado_erp=bool(enviado_erp_com_sucesso),
            erp_http_status=st,
            erp_resposta=resp_json,
            usuario_registro=user_label,
            sessao_caixa=sessao,
        )
        for it in itens_payload:
            ItemVendaAgro.objects.create(venda=v, **it)
    return v


@require_POST
def api_enviar_pedido_erp(request):
    try:
        data = json.loads(request.body)
        client_m, db = obter_conexao_mongo()

        dep_id = ""
        emp_id = ""

        if db is not None and client_m is not None:
            est = db[client_m.col_e].find_one({"DepositoID": client_m.DEPOSITO_CENTRO})
            if est:
                dep_id = str(est.get("DepositoID") or "")
                emp_id = str(est.get("EmpresaID") or "")

        integ = (
            IntegracaoERP.objects.filter(ativo=True, tipo_erp="venda_erp")
            .order_by("-pk")
            .first()
        )
        api_client = VendaERPAPIClient(
            base_url=(integ.url_base.strip() if integ and integ.url_base else None),
            token=(integ.token.strip() if integ and integ.token else None),
        )

        # Venda ERP (ASP.NET / JSON camelCase): espera "items" e chaves camelCase — não "Itens"/PascalCase.
        raw_itens = data.get("itens", [])
        if not isinstance(raw_itens, list):
            raw_itens = []

        linhas = []
        for i in raw_itens:
            if not isinstance(i, dict):
                continue
            linha = _linha_item_pedido_erp(db, client_m, i)
            if linha:
                linhas.append(linha)

        if not linhas:
            return JsonResponse(
                {"ok": False, "erro": "Nenhum item válido para enviar (verifique IDs dos produtos)."},
                status=400,
            )

        def _lbl(integ_obj, attr, default):
            if not integ_obj:
                return default
            v = getattr(integ_obj, attr, None) or ""
            v = str(v).strip()
            return v or default

        payload = {
            "statusSistema": "Orçamento",
            "cliente": (data.get("cliente") or "").strip()
            or "CONSUMIDOR NÃO IDENTIFICADO...",
            "data": timezone.now().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "origemVenda": "Venda Direta",
            "empresa": _lbl(integ, "pedido_empresa_label", "Agro Mais Centro"),
            "deposito": _lbl(integ, "pedido_deposito_label", "Deposito Centro"),
            "vendedor": _lbl(integ, "pedido_vendedor_label", "Gm Agro Mais"),
            "items": linhas,
        }
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

        ok, status, res = api_client.salvar_operacao_pdv(payload)
        msg_para_checar = _desembrulhar_texto_json_recursivo(res)
        msg_txt = str(msg_para_checar).strip()
        sucesso_erp = bool(ok and not _mensagem_pedido_erp_indica_falha(msg_txt))
        venda_local = _persistir_venda_agro(
            request, data, raw_itens, status, res, sucesso_erp
        )
        vid = venda_local.pk if venda_local else None

        if ok and _mensagem_pedido_erp_indica_falha(msg_txt):
            return JsonResponse(
                {
                    "ok": False,
                    "erro": msg_txt or _json_legivel(res),
                    "http_status": status,
                    "venda_id": vid,
                },
                status=502,
            )
        if ok:
            return JsonResponse(
                {"ok": True, "mensagem": _json_legivel(res), "venda_id": vid}
            )
        return JsonResponse(
            {
                "ok": False,
                "erro": _json_legivel(res),
                "http_status": status,
                "venda_id": vid,
            },
            status=502 if status and status != 0 else 500,
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
    request.session["pdv_checkout"] = {
        "itens": itens,
        "cliente": cli,
        "cliente_extra": _sanear_cliente_extra_sessao(data.get("cliente_extra")),
        "forma_pagamento": str(data.get("forma_pagamento") or "").strip()[:80],
    }
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
            pid = str(item.get("ProdutoID") or "")
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
@require_GET
def api_todos_produtos_local(request):
    hoje_cat = timezone.localdate().isoformat()
    entry_cat = cache.get(CATALOGO_PDV_CACHE_ENTRY_KEY)
    if (
        entry_cat
        and isinstance(entry_cat, dict)
        and entry_cat.get("day") == hoje_cat
        and isinstance(entry_cat.get("body"), dict)
        and "produtos" in entry_cat["body"]
    ):
        return JsonResponse(entry_cat["body"])

    client, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"erro": "Erro conexao"}, status=500)

    try:
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
            ]
            busca_texto_gerado = " ".join(normalizar(str(part)) for part in partes if part).strip()
            busca_texto_existente = normalizar(p.get("BuscaTexto") or "")

            texto_puro = " ".join(str(part) for part in partes if part)
            texto_puro_limpo = "".join(
                c
                for c in unicodedata.normalize("NFD", texto_puro)
                if unicodedata.category(c) != "Mn"
            ).lower()

            busca_texto_final = f"{busca_texto_gerado} {busca_texto_existente} {texto_puro_limpo}".strip()

            custos = _custos_compra_produto(p)
            preco_custo_val = custos["preco_custo"]
            preco_custo_acresc_val = custos["preco_custo_final"]
            preco_venda_val = float(p.get("ValorVenda") or p.get("PrecoVenda") or 0)

            res.append({
                "id": pid,
                "nome": p.get("Nome"),
                "marca": p.get("Marca"),
                "fornecedor": p.get("NomeFornecedor")
                or p.get("Fornecedor")
                or p.get("RazaoSocialFornecedor")
                or p.get("Fabricante"),
                "categoria": p.get("NomeCategoria")
                or p.get("Categoria")
                or p.get("Grupo")
                or p.get("SubGrupo"),
                "codigo_nfe": p.get("CodigoNFe") or p.get("Codigo"),
                "codigo_barras": p.get("CodigoBarras") or p.get("EAN_NFe"),
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
            })

        resultado_final = {"produtos": res}
        cache.set(
            CATALOGO_PDV_CACHE_ENTRY_KEY,
            {"day": hoje_cat, "body": resultado_final},
            timeout=86400 * 2,
        )
        return JsonResponse(resultado_final)
    except Exception as e:
        return JsonResponse({"erro": str(e)}, status=500)


@require_GET
def api_pdv_invalidar_cache_catalogo(request):
    """Limpa o snapshot diário do catálogo; próximo GET /api/todos-produtos/ refaz do Mongo."""
    cache.delete(CATALOGO_PDV_CACHE_ENTRY_KEY)
    return JsonResponse({"ok": True})


@require_GET
def api_pdv_saldos_compacto(request):
    """
    Saldos atuais (ERP + ajuste PIN) para todos os produtos ativos — payload compacto.
    Cache de poucos segundos: muitas abas/caixas batem o mesmo snapshot e aliviam o Mongo.
    """
    cached = cache.get(_SALDOS_PDV_CACHE_KEY)
    if cached is not None and isinstance(cached, dict) and "rows" in cached:
        return JsonResponse(cached)

    client, db = obter_conexao_mongo()
    if db is None:
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
        return JsonResponse(payload)
    except Exception as e:
        return JsonResponse({"erro": str(e)}, status=500)


@require_GET
def api_pdv_metricas_produtos(request):
    """
    Por produto: média diária (total/dias no período), vendas últimos 7d, 7d anteriores,
    variação % semana a semana, última entrada (compra/nota) se o Mongo tiver as coleções.
    Cache ~5 min por (dias, bucket) para não sobrecarregar o banco.
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
        media_tot, w0, w1 = _metricas_vendas_agregadas_por_produto(db, dias)
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
                ]
            )
        payload = {"v": 1, "dias": dias, "rows": rows}
        cache.set(ck, payload, timeout=320)
        return JsonResponse(payload)
    except Exception as e:
        return JsonResponse({"erro": str(e)}, status=500)


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
    return {
        "id": pid,
        "nome": c.nome,
        "documento": (c.cpf or "").strip() or "—",
        "telefone": (c.whatsapp or "").strip(),
    }


def _clientes_locais_agro_pdv(termo=""):
    t = (termo or "").strip()
    qs = ClienteAgro.objects.filter(ativo=True)
    if t:
        qs = qs.filter(
            Q(nome__icontains=t) | Q(whatsapp__icontains=t) | Q(cpf__icontains=t)
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
    qs = ClienteAgro.objects.filter(ativo=True).order_by("nome")[:8000]
    merged = [_linha_clienteagro_pdv(c) for c in qs]
    payload = {"clientes": merged}
    if settings.DEBUG:
        payload["contagem_fontes"] = {
            "cliente_agro": len(merged),
            "total_na_lista": len(merged),
        }
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

        res = {
            "id": id,
            "nome": p.get("Nome"),
            "marca": p.get("Marca") or "",
            "codigo_nfe": p.get("CodigoNFe") or p.get("Codigo") or "",
            "preco_venda": float(p.get("ValorVenda") or p.get("PrecoVenda") or 0),
            "imagem": img_url,
            "saldo_centro": round(saldo_f_c, 2),
            "saldo_vila": round(saldo_f_v, 2),
            "saldo_erp_centro": s_c,
            "saldo_erp_vila": s_v,
        }
        return JsonResponse(res)
    except Exception as e:
        return JsonResponse({"erro": str(e)}, status=500)
