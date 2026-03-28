import json
import logging
import re
import unicodedata
from datetime import datetime, timedelta
from decimal import Decimal

from bson import ObjectId

from django.conf import settings
from django.shortcuts import render
from django.http import JsonResponse
from django.core.cache import cache
from django.views.decorators.http import require_GET, require_POST
from django.utils import timezone

from base.models import Empresa, PerfilUsuario, IntegracaoERP
from estoque.models import AjusteRapidoEstoque
from integracoes.texto import normalizar, expandir_tokens
from integracoes.venda_erp_mongo import VendaERPMongoClient
from integracoes.venda_erp_api import VendaERPAPIClient


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
        pid = str(e.get("ProdutoID") or "")
        dep = str(e.get("DepositoID") or "")
        saldo = float(e.get("Saldo", 0) or 0)

        if pid not in mapa:
            mapa[pid] = {"centro": 0.0, "vila": 0.0}

        if dep == client.DEPOSITO_CENTRO:
            mapa[pid]["centro"] += saldo
        elif dep == client.DEPOSITO_VILA_ELIAS:
            mapa[pid]["vila"] += saldo

    return mapa


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


# --- VIEWS DE PÁGINA ---
def consulta_produtos(request):
    return render(request, "produtos/consulta_produtos.html")


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
        }).limit(80)))

    # 3) Fallback por frase inteira
    if len(candidatos) < limit:
        termo_regex = _regex_contem_ci(termo_original)
        adicionar(list(db[client.col_p].find({
            **base_filter,
            "$or": [
                {"Nome": termo_regex},
                {"Marca": termo_regex},
                {"Codigo": termo_regex},
                {"CodigoNFe": termo_regex},
                {"CodigoBarras": termo_regex},
                {"EAN_NFe": termo_regex},
            ]
        }).limit(80)))

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


# --- APIs DE BUSCA ---
@require_GET
def api_buscar_produtos(request):
    q = request.GET.get("q", "").strip()
    client, db = obter_conexao_mongo()
    if db is None or not q:
        return JsonResponse({"produtos": []})

    try:
        prods = motor_de_busca_agro(q, db, client, limit=60)
        p_ids = [str(p.get("Id") or p["_id"]) for p in prods]

        medias_map = _obter_mapa_medias_venda_cache(db)

        estoques = list(db[client.col_e].find({"ProdutoID": {"$in": p_ids}}))
        estoque_map = _mapear_estoques_por_produto(estoques, client)

        ajustes_bd = AjusteRapidoEstoque.objects.filter(produto_externo_id__in=p_ids)
        ajustes_map = {(aj.produto_externo_id, aj.deposito): aj for aj in ajustes_bd}

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
                "preco_venda": float(p.get("ValorVenda") or p.get("PrecoVenda") or 0),
                "imagem": _formatar_url_imagem(_extrair_imagem_produto(p, {}, pid)),
                "saldo_centro": round(saldo_centro, 2),
                "saldo_vila": round(saldo_vila, 2),
                "saldo_centro_erp": round(saldo_centro_erp, 2),
                "saldo_vila_erp": round(saldo_vila_erp, 2),
                "saldo_erp_centro": round(saldo_centro_erp, 2),  # compatibilidade com mobile atual
                "saldo_erp_vila": round(saldo_vila_erp, 2),
                "media_venda_diaria_30d": media_d,
            })

        res.sort(
            key=lambda r: (
                -float(r.get("media_venda_diaria_30d") or 0),
                str(r.get("nome") or "").lower(),
            )
        )

        return JsonResponse({"produtos": res})
    except Exception as e:
        return JsonResponse({"erro": str(e)}, status=500)


@require_GET
def api_buscar_compras(request):
    q = request.GET.get("q", "").strip()
    client, db = obter_conexao_mongo()
    if db is None or not q:
        return JsonResponse({"produtos": []})

    try:
        prods = motor_de_busca_agro(q, db, client, limit=50)
        p_ids = [str(p.get("Id") or p["_id"]) for p in prods]

        estoques = list(db[client.col_e].find({"ProdutoID": {"$in": p_ids}}))
        estoque_map = _mapear_estoques_por_produto(estoques, client)

        ajustes_bd = AjusteRapidoEstoque.objects.filter(produto_externo_id__in=p_ids)
        ajustes_map = {(aj.produto_externo_id, aj.deposito): aj for aj in ajustes_bd}

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
            cache.clear()
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

        payload = {
            "statusSistema": "Orçamento",
            "cliente": (data.get("cliente") or "").strip()
            or "CONSUMIDOR NÃO IDENTIFICADO...",
            "data": timezone.now().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "origemVenda": "Venda Direta",
            "empresa": "Agro Mais Centro",
            "deposito": "Deposito Centro",
            "vendedor": "Gm Agro Mais",
            "items": linhas,
        }
        if dep_id:
            payload["depositoID"] = dep_id
        if emp_id:
            payload["empresaID"] = emp_id

        cid = str(data.get("cliente_id") or data.get("ClienteID") or "").strip()
        if cid:
            payload["clienteID"] = cid
        doc_raw = str(data.get("cliente_documento") or data.get("CpfCnpj") or "").strip()
        doc_digits = re.sub(r"\D", "", doc_raw)
        if len(doc_digits) >= 11:
            payload["cpfCnpj"] = doc_digits

        payload = {k: v for k, v in payload.items() if v not in (None, "")}

        ok, status, res = api_client.salvar_operacao_pdv(payload)
        msg_para_checar = _desembrulhar_texto_json_recursivo(res)
        msg_txt = str(msg_para_checar).strip()
        if ok and _mensagem_pedido_erp_indica_falha(msg_txt):
            return JsonResponse(
                {"ok": False, "erro": msg_txt or _json_legivel(res), "http_status": status},
                status=502,
            )
        if ok:
            return JsonResponse({"ok": True, "mensagem": _json_legivel(res)})
        return JsonResponse(
            {"ok": False, "erro": _json_legivel(res), "http_status": status},
            status=502 if status and status != 0 else 500,
        )
    except Exception as e:
        return JsonResponse({"ok": False, "erro": str(e)}, status=500)


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


_CACHE_MEDIAS_VENDA_30D = "pdv_mapa_medias_venda_diaria_30d_v1"


def _obter_mapa_medias_venda_cache(db, timeout=900):
    """Mapa produto_id → média diária (30d), com cache curto para não recalcular a cada busca."""
    m = cache.get(_CACHE_MEDIAS_VENDA_30D)
    if m is None:
        m = _media_diaria_vendas_por_produto(db, dias=30)
        cache.set(_CACHE_MEDIAS_VENDA_30D, m, timeout=timeout)
    return m


# --- CARGA INICIAL E APIs AUXILIARES ---
@require_GET
def api_todos_produtos_local(request):
    cache_key = "carga_inicial_produtos_todos_v26"
    cached_data = cache.get(cache_key)
    if cached_data:
        return JsonResponse(cached_data)

    client, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"erro": "Erro conexao"}, status=500)

    try:
        query = {"CadastroInativo": {"$ne": True}}
        produtos = list(db[client.col_p].find(query))
        p_ids = [str(p.get("Id") or p["_id"]) for p in produtos]

        estoques = list(
            db[client.col_e].find(
                {"ProdutoID": {"$in": p_ids}},
                {"ProdutoID": 1, "DepositoID": 1, "Saldo": 1, "_id": 0},
            )
        )

        ajustes_bd = AjusteRapidoEstoque.objects.all().order_by(
            "produto_externo_id", "deposito", "-criado_em"
        )
        ajustes_map = {}
        for aj in ajustes_bd:
            if (aj.produto_externo_id, aj.deposito) not in ajustes_map:
                ajustes_map[(aj.produto_externo_id, aj.deposito)] = aj

        est_map = {}
        for est in estoques:
            pid = str(est.get("ProdutoID"))
            if pid not in est_map:
                est_map[pid] = {"c": 0.0, "v": 0.0}
            did = str(est.get("DepositoID") or "")
            if did == client.DEPOSITO_CENTRO:
                est_map[pid]["c"] += float(est.get("Saldo") or 0)
            elif did == client.DEPOSITO_VILA_ELIAS:
                est_map[pid]["v"] += float(est.get("Saldo") or 0)

        medias_venda = _obter_mapa_medias_venda_cache(db)

        res = []
        for p in produtos:
            pid = str(p.get("Id") or p["_id"])
            s_c = est_map.get(pid, {}).get("c", 0.0)
            s_v = est_map.get(pid, {}).get("v", 0.0)

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
        cache.set(cache_key, resultado_final, timeout=3600)
        return JsonResponse(resultado_final)
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
        "Telefone": 1,
        "Celular": 1,
        "Fone": 1,
        "CadastroInativo": 1,
        "Inativo": 1,
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


def _documento_pessoa(i):
    for chave in ("CpfCnpj", "CPF", "Cpf", "CNPJ", "Cnpj", "cpfCnpj"):
        v = i.get(chave)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


def _montar_linhas_cliente(cursor):
    out = []
    for i in cursor:
        nome = _nome_exibicao_pessoa(i)
        if not nome:
            continue
        doc = _documento_pessoa(i)
        out.append(
            {
                "id": str(i.get("Id") or i.get("_id")),
                "nome": nome,
                "documento": doc or "—",
                "telefone": _telefone_pessoa(i),
            }
        )
    return out


@require_GET
def api_list_customers(request):
    client_m, db = obter_conexao_mongo()
    if db is None:
        payload = {"clientes": []}
        if settings.DEBUG:
            payload["erro"] = "mongo_indisponivel"
        return JsonResponse(payload)

    try:
        proj = _projecao_pessoa()
        res = []
        for coll in _colecoes_pessoa_disponiveis(db, client_m):
            try:
                clis = list(db[coll].find({}, proj).limit(2500))
            except Exception as exc:
                logger.warning("api_list_customers: ignorando coleção %s: %s", coll, exc)
                continue
            res = _montar_linhas_cliente(clis)
            if res:
                break
        res.sort(key=lambda x: x["nome"])
        return JsonResponse({"clientes": res})
    except Exception as e:
        logger.exception("api_list_customers")
        payload = {"clientes": []}
        if settings.DEBUG:
            payload["erro"] = str(e)
        return JsonResponse(payload)


@require_GET
def api_buscar_clientes(request):
    client_m, db = obter_conexao_mongo()
    termo = (request.GET.get("q") or "").strip()
    if db is None:
        payload = {"clientes": []}
        if settings.DEBUG:
            payload["erro"] = "mongo_indisponivel"
        return JsonResponse(payload)
    if not termo:
        return JsonResponse({"clientes": []})

    try:
        trecho = re.escape(termo)
        regex = {"$regex": trecho, "$options": "i"}
        texto_or = [
            {"Nome": regex},
            {"RazaoSocial": regex},
            {"NomeFantasia": regex},
            {"Fantasia": regex},
            {"Apelido": regex},
            {"nome": regex},
            {"NomeCompleto": regex},
            {"BuscaTexto": regex},
            {"buscaTexto": regex},
            {"CpfCnpj": regex},
            {"CPF": regex},
            {"Cnpj": regex},
            {"Cpf": regex},
            {"Telefone": regex},
            {"Celular": regex},
        ]
        só_digitos = re.sub(r"\D", "", termo)
        if len(só_digitos) >= 4:
            d = re.escape(só_digitos)
            texto_or.append({"CpfCnpj": {"$regex": d}})
            texto_or.append({"CPF": {"$regex": d}})
            texto_or.append({"Cnpj": {"$regex": d}})

        proj = _projecao_pessoa()

        for coll in _colecoes_pessoa_disponiveis(db, client_m):
            try:
                clis = list(db[coll].find({"$or": texto_or}, proj).limit(40))
            except Exception as exc:
                logger.warning("api_buscar_clientes: ignorando coleção %s: %s", coll, exc)
                continue
            res = _montar_linhas_cliente(clis)
            if res:
                return JsonResponse({"clientes": res})
        return JsonResponse({"clientes": []})
    except Exception as e:
        logger.exception("api_buscar_clientes")
        payload = {"clientes": []}
        if settings.DEBUG:
            payload["erro"] = str(e)
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
