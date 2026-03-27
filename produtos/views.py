import json, unicodedata, re
from decimal import Decimal
from django.shortcuts import render
from django.http import JsonResponse
from django.core.cache import cache
from django.views.decorators.http import require_GET, require_POST
from base.models import Empresa, PerfilUsuario, IntegracaoERP
from estoque.models import AjusteRapidoEstoque
from integracoes.texto import normalizar, expandir_tokens # Suas funções inteligentes
from integracoes.venda_erp_mongo import VendaERPMongoClient
from integracoes.venda_erp_api import VendaERPAPIClient
from django.utils import timezone
from bson.objectid import ObjectId

# --- CONEXÃO E AUXILIARES (IGUAL ANTES) ---
_cached_mongo_client = None
def obter_conexao_mongo():
    global _cached_mongo_client
    try:
        if _cached_mongo_client is None: _cached_mongo_client = VendaERPMongoClient()
        return _cached_mongo_client, _cached_mongo_client.db
    except: return None, None

def _formatar_url_imagem(img_str):
    img_str = str(img_str or "").strip()
    if not img_str or img_str == "None" or img_str.startswith("data:image"): return img_str
    return f"https://cw.vendaerp.com.br/{img_str}" if img_str.startswith("Uploads/") else img_str

# --- O MOTOR DE BUSCA ÚNICO (CÉREBRO DO SISTEMA) ---
def motor_de_busca_agro(termo_original, db, client, limit=20):
    """
    Esta função centraliza a lógica que funciona bem na transferência:
    Busca por tokens, códigos exatos e regex em múltiplos campos.
    """
    if not termo_original: return []
    
    # 1. Tenta busca exata por código (GM, Barras, EAN) - É instantâneo
    termo_limpo = re.sub(r'[^a-zA-Z0-9]', '', termo_original)
    if termo_limpo:
        query_cod = {"$or": [
            {"Codigo": termo_limpo}, {"CodigoNFe": termo_limpo},
            {"CodigoBarras": termo_limpo}, {"EAN_NFe": termo_limpo}
        ], "CadastroInativo": {"$ne": True}}
        prods_cod = list(db[client.col_p].find(query_cod).limit(5))
        if prods_cod: return prods_cod

    # 2. Busca Inteligente por Palavras (O que faz o 'Milho Grande' funcionar)
    palavras = termo_original.split()
    condicoes_and = []
    
    for p in palavras:
        tokens = expandir_tokens(p) # Usa sua integração
        p_norm = normalizar(p)
        if p_norm and p_norm not in tokens: tokens.append(p_norm)
        
        if tokens:
            # Transforma tokens em Regex para o Mongo
            regex_tokens = [re.compile(re.escape(t), re.IGNORECASE) for t in tokens]
            # Procura a palavra em qualquer um dos campos importantes
            condicoes_and.append({"$or": [
                {"BuscaTexto": {"$in": regex_tokens}},
                {"Nome": {"$regex": re.escape(p), "$options": "i"}},
                {"Marca": {"$regex": re.escape(p), "$options": "i"}},
                {"Codigo": {"$regex": re.escape(p), "$options": "i"}}
            ]})

    if not condicoes_and: return []
    
    query = {"$and": condicoes_and, "CadastroInativo": {"$ne": True}}
    return list(db[client.col_p].find(query).limit(limit))

# --- APIs QUE USAM O MOTOR ---

@require_GET
def api_buscar_produtos(request):
    q = request.GET.get("q", "").strip()
    client, db = obter_conexao_mongo()
    if not db or not q: return JsonResponse({"produtos": []})
    
    produtos = motor_de_busca_agro(q, db, client)
    p_ids = [str(p.get("Id") or p["_id"]) for p in produtos]
    estoques = list(db[client.col_e].find({"ProdutoID": {"$in": p_ids}}))
    ajustes = {(a.produto_externo_id, a.deposito): a for a in AjusteRapidoEstoque.objects.filter(produto_externo_id__in=p_ids)}
    
    res = []
    for p in produtos:
        pid = str(p.get("Id") or p["_id"])
        sc = sum(float(e.get("Saldo", 0)) for e in estoques if str(e.get("ProdutoID")) == pid and str(e.get("DepositoID")) == client.DEPOSITO_CENTRO)
        sv = sum(float(e.get("Saldo", 0)) for e in estoques if str(e.get("ProdutoID")) == pid and str(e.get("DepositoID")) == client.DEPOSITO_VILA_ELIAS)
        ac, av = ajustes.get((pid, 'centro')), ajustes.get((pid, 'vila'))
        
        res.append({
            "id": pid, "nome": p.get("Nome"), "marca": p.get("Marca") or "",
            "codigo_nfe": p.get("CodigoNFe") or p.get("Codigo") or "", 
            "preco_venda": float(p.get("ValorVenda") or p.get("PrecoVenda") or 0),
            "imagem": _formatar_url_imagem(p.get("UrlImagem") or ""),
            "saldo_centro": round(float(ac.saldo_informado) + (sc - float(ac.saldo_erp_referencia)) if ac else sc, 2),
            "saldo_vila": round(float(av.saldo_informado) + (sv - float(av.saldo_erp_referencia)) if av else sv, 2)
        })
    return JsonResponse({"produtos": res})

@require_GET
def api_buscar_compras(request):
    q = request.GET.get("q", "").strip()
    client, db = obter_conexao_mongo()
    if not db or not q: return JsonResponse({"produtos": []})
    
    produtos = motor_de_busca_agro(q, db, client, limit=50)
    p_ids = [str(p.get("Id") or p["_id"]) for p in produtos]
    estoques = list(db[client.col_e].find({"ProdutoID": {"$in": p_ids}}))
    ajustes = {(a.produto_externo_id, a.deposito): a for a in AjusteRapidoEstoque.objects.filter(produto_externo_id__in=p_ids)}
    
    res = []
    for p in produtos:
        pid = str(p.get("Id") or p["_id"])
        sc = sum(float(e.get("Saldo", 0)) for e in estoques if str(e.get("ProdutoID")) == pid and str(e.get("DepositoID")) == client.DEPOSITO_CENTRO)
        sv = sum(float(e.get("Saldo", 0)) for e in estoques if str(e.get("ProdutoID")) == pid and str(e.get("DepositoID")) == client.DEPOSITO_VILA_ELIAS)
        ac, av = ajustes.get((pid, 'centro')), ajustes.get((pid, 'vila'))
        
        res.append({
            "id": pid, "nome": p.get("Nome"), "marca": p.get("Marca") or "",
            "preco_custo": float(str(p.get("PrecoCusto") or p.get("ValorCusto") or 0).replace(',', '.')),
            "saldo_centro": round(float(ac.saldo_informado) + (sc - float(ac.saldo_erp_referencia)) if ac else sc, 2),
            "saldo_vila": round(float(av.saldo_informado) + (sv - float(av.saldo_erp_referencia)) if av else sv, 2)
        })
    return JsonResponse({"produtos": res})

# --- PÁGINAS E OUTRAS APIs (MANTIDAS) ---
def consulta_produtos(request): return render(request, "produtos/consulta_produtos.html")
def compras_view(request): return render(request, "produtos/compras.html")
def ajuste_mobile_view(request):
    if not request.session.get('mobile_auth'): return render(request, "produtos/ajuste_mobile_login.html")
    return render(request, "produtos/mobile_ajuste.html")

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