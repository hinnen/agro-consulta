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

# --- CONEXÃO MONGO ---
_cached_mongo_client = None

def obter_conexao_mongo():
    global _cached_mongo_client
    try:
        if _cached_mongo_client is None:
            _cached_mongo_client = VendaERPMongoClient()
        db = _cached_mongo_client.db if _cached_mongo_client else None
        return _cached_mongo_client, db
    except:
        return None, None

def _formatar_url_imagem(img_str):
    if not img_str or img_str == "None": return ""
    if img_str.startswith("data:image") or img_str.startswith("http"): return img_str
    return f"https://cw.vendaerp.com.br/{img_str.lstrip('/')}"

# --- MOTOR DE BUSCA CENTRALIZADO (ESTILO TRANSFERÊNCIA) ---
def motor_de_busca_agro(q_texto, db, client, limit=20):
    if not q_texto: return []
    termo_limpo = re.sub(r'[^a-zA-Z0-9]', '', q_texto)
    
    # 1. Prioridade Códigos (GM, Barras, EAN)
    if termo_limpo:
        query_cod = {"$or": [
            {"Codigo": termo_limpo}, {"CodigoNFe": termo_limpo},
            {"CodigoBarras": termo_limpo}, {"EAN_NFe": termo_limpo}
        ], "CadastroInativo": {"$ne": True}}
        prods = list(db[client.col_p].find(query_cod).limit(5))
        if prods: return prods

    # 2. Busca por Palavras (O que acha o "Milho Grande")
    palavras = q_texto.split()
    cond_and = []
    for p in palavras:
        tokens = expandir_tokens(p)
        p_norm = normalizar(p)
        if p_norm and p_norm not in tokens: tokens.append(p_norm)
        
        regex_list = [re.compile(re.escape(t), re.IGNORECASE) for t in tokens]
        cond_and.append({"$or": [
            {"Nome": {"$in": regex_list}},
            {"Marca": {"$in": regex_list}},
            {"BuscaTexto": {"$in": regex_list}},
            {"Codigo": {"$regex": re.escape(p), "$options": "i"}}
        ]})
    
    if not cond_and: return []
    return list(db[client.col_p].find({"$and": cond_and, "CadastroInativo": {"$ne": True}}).limit(limit))

# --- PÁGINAS ---
def consulta_produtos(request): return render(request, "produtos/consulta_produtos.html")
def historico_ajustes(request): return render(request, "produtos/historico_ajustes.html", {"ajustes": AjusteRapidoEstoque.objects.all().order_by('-criado_em')})
def sugestao_transferencia(request): return render(request, "produtos/transferencias.html")
def compras_view(request): return render(request, "produtos/compras.html")
def ajuste_mobile_view(request):
    if not request.session.get('mobile_auth'): return render(request, "produtos/ajuste_mobile_login.html")
    return render(request, "produtos/mobile_ajuste.html")

# --- APIs DE BUSCA ---
@require_GET
def api_buscar_produtos(request):
    q = request.GET.get("q", "").strip()
    client, db = obter_conexao_mongo()
    if not db or not q: return JsonResponse({"produtos": []})
    
    prods = motor_de_busca_agro(q, db, client)
    p_ids = [str(p.get("Id") or p["_id"]) for p in prods]
    estoques = list(db[client.col_e].find({"ProdutoID": {"$in": p_ids}}))
    ajustes = {(a.produto_externo_id, a.deposito): a for a in AjusteRapidoEstoque.objects.filter(produto_externo_id__in=p_ids)}
    
    res = []
    for p in prods:
        pid = str(p.get("Id") or p["_id"])
        sc = sum(float(e.get("Saldo", 0)) for e in estoques if str(e.get("ProdutoID")) == pid and str(e.get("DepositoID")) == client.DEPOSITO_CENTRO)
        sv = sum(float(e.get("Saldo", 0)) for e in estoques if str(e.get("ProdutoID")) == pid and str(e.get("DepositoID")) == client.DEPOSITO_VILA_ELIAS)
        ac, av = ajustes.get((pid, 'centro')), ajustes.get((pid, 'vila'))
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
    estoques = list(db[client.col_e].find({"ProdutoID": {"$in": p_ids}}))
    ajustes = {(a.produto_externo_id, a.deposito): a for a in AjusteRapidoEstoque.objects.filter(produto_externo_id__in=p_ids)}
    
    res = []
    for p in prods:
        pid = str(p.get("Id") or p["_id"])
        sc = sum(float(e.get("Saldo", 0)) for e in estoques if str(e.get("ProdutoID")) == pid and str(e.get("DepositoID")) == client.DEPOSITO_CENTRO)
        sv = sum(float(e.get("Saldo", 0)) for e in estoques if str(e.get("ProdutoID")) == pid and str(e.get("DepositoID")) == client.DEPOSITO_VILA_ELIAS)
        ac, av = ajustes.get((pid, 'centro')), ajustes.get((pid, 'vila'))
        custo = float(str(p.get("PrecoCusto") or p.get("ValorCusto") or 0).replace(',', '.'))
        res.append({
            "id": pid, "nome": p.get("Nome"), "marca": p.get("Marca") or "", "preco_custo": custo,
            "saldo_centro": round(float(ac.saldo_informado) + (sc - float(ac.saldo_erp_referencia)) if ac else sc, 2),
            "saldo_vila": round(float(av.saldo_informado) + (sv - float(av.saldo_erp_referencia)) if av else sv, 2)
        })
    return JsonResponse({"produtos": res})

# --- APIs DE ESTOQUE E ERP ---
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

@require_POST
def api_enviar_pedido_erp(request):
    try:
        data = json.loads(request.body)
        client_m, db = obter_conexao_mongo()
        dep_id = ""; emp_id = ""
        if db is not None:
            est = db[client_m.col_e].find_one({"Deposito": {"$regex": "centro", "$options": "i"}})
            if est: dep_id = str(est.get("DepositoID")); emp_id = str(est.get("EmpresaID"))

        payload = {
            "statusSistema": "Orçamento", "cliente": data.get('cliente', 'Consumidor Final'),
            "data": timezone.now().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "origemVenda": "Venda Direta", "empresa": "Agro Mais Centro",
            "deposito": "Deposito Centro", "vendedor": "Gm Agro Mais", "items": []
        }
        if dep_id: payload["depositoID"] = dep_id
        if emp_id: payload["empresaID"] = emp_id

        for i in data.get('itens', []):
            payload["items"].append({"produtoID": i.get("id"), "codigo": i.get("id"), "quantidade": float(i.get("qtd")), "valorUnitario": float(i.get("preco")), "valorTotal": round(float(i.get("qtd"))*float(i.get("preco")), 2)})

        ok, status, res = VendaERPAPIClient().salvar_operacao_pdv(payload)
        return JsonResponse({'ok': ok, 'mensagem': res})
    except Exception as e: return JsonResponse({'ok': False, 'erro': str(e)})

# --- CARGA INICIAL (LOGÍSTICA) ---
@require_GET
def api_todos_produtos_local(request):
    cache_key = "carga_logistica_v1"
    cached = cache.get(cache_key)
    if cached: return JsonResponse(cached)
    client, db = obter_conexao_mongo()
    if not db: return JsonResponse({"erro": "DB Offline"}, status=500)
    try:
        prods = list(db[client.col_p].find({"CadastroInativo": {"$ne": True}}))
        res = []
        for p in prods:
            pid = str(p.get("Id") or p["_id"])
            res.append({"id": pid, "nome": p.get("Nome"), "marca": p.get("Marca") or "", "busca_texto": normalizar(f"{p.get('Nome')} {p.get('Marca')} {p.get('Codigo')}")})
        final = {"produtos": res}
        cache.set(cache_key, final, timeout=3600)
        return JsonResponse(final)
    except: return JsonResponse({"produtos": []})

# --- AUXILIARES RESTANTES ---
def api_login_mobile(request):
    if PerfilUsuario.objects.filter(senha_rapida=request.POST.get("pin")).exists():
        request.session['mobile_auth'] = True
        return JsonResponse({"ok": True})
    return JsonResponse({"ok": False}, status=403)

def api_autocomplete_produtos(request): return JsonResponse({"sugestoes": []})
def api_buscar_clientes(request): return JsonResponse({"clientes": []})
def api_list_customers(request): return JsonResponse({"clientes": []})
def api_buscar_produto_id(request, id): return JsonResponse({"id": id})