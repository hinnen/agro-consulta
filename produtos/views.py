import json, unicodedata, re
from decimal import Decimal
from django.shortcuts import render
from django.http import JsonResponse
from django.core.cache import cache
from django.views.decorators.http import require_GET, require_POST
from base.models import Empresa, PerfilUsuario, IntegracaoERP
from estoque.models import AjusteRapidoEstoque
from integracoes.texto import normalizar, expandir_tokens
from integracoes.venda_erp_mongo import VendaERPMongoClient
from integracoes.venda_erp_api import VendaERPAPIClient
from django.utils import timezone
from bson.objectid import ObjectId

# --- MOTOR DE BUSCA ÚNICO (ESTILO TRANSFERÊNCIA) ---
def motor_de_busca_agro(q_texto, db, client, limit=20):
    if not q_texto: return []
    
    # 1. Limpeza para busca de códigos
    termo_limpo = re.sub(r'[^a-zA-Z0-9]', '', q_texto)
    
    # 2. Tenta Código Exato primeiro (Instantâneo)
    if termo_limpo:
        query_cod = {"$or": [
            {"Codigo": termo_limpo}, {"CodigoNFe": termo_limpo},
            {"CodigoBarras": termo_limpo}, {"EAN_NFe": termo_limpo}
        ], "CadastroInativo": {"$ne": True}}
        prods_cod = list(db[client.col_p].find(query_cod).limit(5))
        if prods_cod: return prods_cod

    # 3. Busca por Palavras (O que acha o "Milho Grande")
    palavras = q_texto.split()
    condicoes_and = []
    
    for p in palavras:
        tokens = expandir_tokens(p)
        p_norm = normalizar(p)
        if p_norm and p_norm not in tokens: tokens.append(p_norm)
        
        # Cada palavra digitada deve existir em algum desses campos
        regex_tokens = [re.compile(re.escape(t), re.IGNORECASE) for t in tokens]
        condicoes_and.append({"$or": [
            {"Nome": {"$in": regex_tokens}},
            {"Marca": {"$in": regex_tokens}},
            {"BuscaTexto": {"$in": regex_tokens}},
            {"Codigo": {"$regex": re.escape(p), "$options": "i"}},
            {"CodigoBarras": {"$regex": re.escape(p), "$options": "i"}}
        ]})

    if not condicoes_and: return []
    
    query = {"$and": condicoes_and, "CadastroInativo": {"$ne": True}}
    return list(db[client.col_p].find(query).limit(limit))

# --- CONEXÃO ---
_cached_mongo_client = None
def obter_conexao_mongo():
    global _cached_mongo_client
    try:
        if _cached_mongo_client is None: _cached_mongo_client = VendaERPMongoClient()
        return _cached_mongo_client, _cached_mongo_client.db
    except: return None, None

def _formatar_url_imagem(img_str):
    if not img_str or img_str == "None": return ""
    if img_str.startswith("data:image") or img_str.startswith("http"): return img_str
    return f"https://cw.vendaerp.com.br/{img_str.lstrip('/')}"

# --- APIs ---

@require_GET
def api_buscar_produtos(request):
    q = request.GET.get("q", "").strip()
    client, db = obter_conexao_mongo()
    if not db or not q: return JsonResponse({"produtos": []})
    
    prods = motor_de_busca_agro(q, db, client)
    p_ids = [str(p.get("Id") or p["_id"]) for p in prods]
    ests = list(db[client.col_e].find({"ProdutoID": {"$in": p_ids}}))
    ajs = {(a.produto_externo_id, a.deposito): a for a in AjusteRapidoEstoque.objects.filter(produto_externo_id__in=p_ids)}
    
    res = []
    for p in prods:
        pid = str(p.get("Id") or p["_id"])
        sc = sum(float(e.get("Saldo", 0)) for e in ests if str(e.get("ProdutoID")) == pid and str(e.get("DepositoID")) == client.DEPOSITO_CENTRO)
        sv = sum(float(e.get("Saldo", 0)) for e in ests if str(e.get("ProdutoID")) == pid and str(e.get("DepositoID")) == client.DEPOSITO_VILA_ELIAS)
        ac, av = ajs.get((pid, 'centro')), ajs.get((pid, 'vila'))
        res.append({
            "id": pid, "nome": p.get("Nome"), "marca": p.get("Marca") or "",
            "codigo_nfe": p.get("CodigoNFe") or p.get("Codigo") or "", 
            "preco_venda": float(p.get("ValorVenda") or p.get("PrecoVenda") or 0),
            "imagem": _formatar_url_imagem(p.get("UrlImagem") or p.get("Imagem") or ""),
            "saldo_centro": round(float(ac.saldo_informado) + (sc - float(ac.saldo_erp_referencia)) if ac else sc, 2),
            "saldo_vila": round(float(av.saldo_informado) + (sv - float(av.saldo_erp_referencia)) if av else sv, 2)
        })
    return JsonResponse({"produtos": res})

@require_GET
def api_buscar_compras(request):
    q = request.GET.get("q", "").strip()
    client, db = obter_conexao_mongo()
    if not db or not q: return JsonResponse({"produtos": []})
    
    prods = motor_de_busca_agro(q, db, client, limit=50)
    p_ids = [str(p.get("Id") or p["_id"]) for p in prods]
    ests = list(db[client.col_e].find({"ProdutoID": {"$in": p_ids}}))
    ajs = {(a.produto_externo_id, a.deposito): a for a in AjusteRapidoEstoque.objects.filter(produto_externo_id__in=p_ids)}
    
    res = []
    for p in prods:
        pid = str(p.get("Id") or p["_id"])
        sc = sum(float(e.get("Saldo", 0)) for e in ests if str(e.get("ProdutoID")) == pid and str(e.get("DepositoID")) == client.DEPOSITO_CENTRO)
        sv = sum(float(e.get("Saldo", 0)) for e in ests if str(e.get("ProdutoID")) == pid and str(e.get("DepositoID")) == client.DEPOSITO_VILA_ELIAS)
        ac, av = ajs.get((pid, 'centro')), ajs.get((pid, 'vila'))
        custo = float(str(p.get("PrecoCusto") or p.get("ValorCusto") or 0).replace(',', '.'))
        res.append({
            "id": pid, "nome": p.get("Nome"), "marca": p.get("Marca") or "", "preco_custo": custo,
            "saldo_centro": round(float(ac.saldo_informado) + (sc - float(ac.saldo_erp_referencia)) if ac else sc, 2),
            "saldo_vila": round(float(av.saldo_informado) + (sv - float(av.saldo_erp_referencia)) if av else sv, 2)
        })
    return JsonResponse({"produtos": res})

# --- PÁGINAS ---
def consulta_produtos(request): return render(request, "produtos/consulta_produtos.html")
def compras_view(request): return render(request, "produtos/compras.html")
def sugestao_transferencia(request): return render(request, "produtos/transferencias.html")
def historico_ajustes(request): return render(request, "produtos/historico_ajustes.html", {"ajustes": AjusteRapidoEstoque.objects.all().order_by('-criado_em')})
def ajuste_mobile_view(request):
    if not request.session.get('mobile_auth'): return render(request, "produtos/ajuste_mobile_login.html")
    return render(request, "produtos/mobile_ajuste.html")

# --- APIs AUXILIARES ---
@require_POST
def api_login_mobile(request):
    if PerfilUsuario.objects.filter(senha_rapida=request.POST.get("pin")).exists():
        request.session['mobile_auth'] = True
        return JsonResponse({"ok": True})
    return JsonResponse({"ok": False}, status=403)

@require_POST
def api_ajustar_estoque(request):
    pin = request.POST.get("pin")
    if (pin == "SESSAO" and request.session.get('mobile_auth')) or PerfilUsuario.objects.filter(senha_rapida=pin).exists():
        try:
            AjusteRapidoEstoque.objects.create(
                empresa=Empresa.objects.first(), produto_externo_id=request.POST.get("produto_id"),
                deposito=request.POST.get("deposito", "centro"), nome_produto=request.POST.get("nome_produto"),
                saldo_erp_referencia=Decimal(request.POST.get("saldo_atual", "0")),
                saldo_informado=Decimal(request.POST.get("novo_saldo", "0"))
            )
            cache.clear()
            return JsonResponse({"ok": True})
        except Exception as e: return JsonResponse({"ok": False, "erro": str(e)})
    return JsonResponse({"ok": False, "erro": "PIN INCORRETO"}, status=403)

def api_todos_produtos_local(request): return JsonResponse({"produtos": []})
def api_autocomplete_produtos(request): return JsonResponse({"sugestoes": []})
def api_buscar_clientes(request): return JsonResponse({"clientes": []})
def api_list_customers(request): return JsonResponse({"clientes": []})
def api_enviar_pedido_erp(request): return JsonResponse({"ok": True})
def api_buscar_produto_id(request, id): return JsonResponse({"id": id})