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
    img_str = str(img_str or "").strip()
    if not img_str or img_str == "None" or img_str.startswith("data:image"): return img_str
    return f"https://cw.vendaerp.com.br/{img_str}" if img_str.startswith("Uploads/") else img_str

def _extrair_imagem_produto(p, pid):
    for c in ["UrlImagem", "Imagem", "CaminhoImagem", "Foto", "UrlImagemPrincipal"]:
        val = p.get(c)
        if val and isinstance(val, str) and len(val.strip()) > 2: return val
    return ""

# --- VIEWS DE PÁGINA ---
def consulta_produtos(request): return render(request, "produtos/consulta_produtos.html")
def historico_ajustes(request): return render(request, "produtos/historico_ajustes.html", {"ajustes": AjusteRapidoEstoque.objects.all().order_by('-criado_em')})
def sugestao_transferencia(request): return render(request, "produtos/transferencias.html")
def compras_view(request): return render(request, "produtos/compras.html")
def ajuste_mobile_view(request):
    if not request.session.get('mobile_auth'): return render(request, "produtos/ajuste_mobile_login.html")
    return render(request, "produtos/mobile_ajuste.html")

@require_POST
def api_login_mobile(request):
    pin = request.POST.get("pin")
    if PerfilUsuario.objects.filter(senha_rapida=pin).exists():
        request.session['mobile_auth'] = True
        return JsonResponse({"ok": True})
    return JsonResponse({"ok": False}, status=403)

# --- BUSCADOR TURBINADO (IGUAL LOGÍSTICA) ---
@require_GET
def api_buscar_produtos(request):
    client = None
    q = request.GET.get("q", "").strip()
    if not q: return JsonResponse({"produtos": []})
    
    client, db = obter_conexao_mongo()
    if not db: return JsonResponse({"erro": "DB Offline"}, status=503)
    
    try:
        palavras = q.split()
        condicoes_and = []
        for p in palavras:
            p_reg = re.escape(p)
            # PROCURA EM TODOS OS CAMPOS AO MESMO TEMPO
            condicoes_and.append({"$or": [
                {"Nome": {"$regex": p_reg, "$options": "i"}},
                {"Marca": {"$regex": p_reg, "$options": "i"}},
                {"BuscaTexto": {"$regex": p_reg, "$options": "i"}},
                {"CodigoNFe": {"$regex": p_reg, "$options": "i"}},
                {"CodigoBarras": {"$regex": p_reg, "$options": "i"}},
                {"Codigo": {"$regex": p_reg, "$options": "i"}}
            ]})
        
        query = {"$and": condicoes_and, "CadastroInativo": {"$ne": True}}
        prods = list(db[client.col_p].find(query).limit(20))
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
                "preco_venda": float(p.get("ValorVenda") or p.get("PrecoVenda") or 0),
                "imagem": _formatar_url_imagem(_extrair_imagem_produto(p, pid)),
                "saldo_centro": round(float(ac.saldo_informado) + (sc - float(ac.saldo_erp_referencia)) if ac else sc, 2),
                "saldo_vila": round(float(av.saldo_informado) + (sv - float(av.saldo_erp_referencia)) if av else sv, 2)
            })
        return JsonResponse({"produtos": res})
    except Exception as e: return JsonResponse({"erro": str(e)}, status=500)

@require_GET
def api_buscar_compras(request):
    client = None
    q = request.GET.get("q", "").strip()
    client, db = obter_conexao_mongo()
    if not db: return JsonResponse({"erro": "DB Offline"}, status=503)
    try:
        palavras = q.split()
        condicoes_and = []
        for p in palavras:
            p_reg = re.escape(p)
            condicoes_and.append({"$or": [
                {"Nome": {"$regex": p_reg, "$options": "i"}},
                {"Marca": {"$regex": p_reg, "$options": "i"}},
                {"BuscaTexto": {"$regex": p_reg, "$options": "i"}},
                {"CodigoBarras": {"$regex": p_reg, "$options": "i"}}
            ]})
        
        prods = list(db[client.col_p].find({"$and": condicoes_and, "CadastroInativo": {"$ne": True}}).limit(50))
        p_ids = [str(p.get("Id") or p["_id"]) for p in prods]
        ests = list(db[client.col_e].find({"ProdutoID": {"$in": p_ids}}))
        ajs = {(a.produto_externo_id, a.deposito): a for a in AjusteRapidoEstoque.objects.filter(produto_externo_id__in=p_ids)}
        
        res = []
        for p in prods:
            pid = str(p.get("Id") or p["_id"])
            sc = sum(float(e.get("Saldo", 0)) for e in ests if str(e.get("ProdutoID")) == pid and str(e.get("DepositoID")) == client.DEPOSITO_CENTRO)
            sv = sum(float(e.get("Saldo", 0)) for e in ests if str(e.get("ProdutoID")) == pid and str(e.get("DepositoID")) == client.DEPOSITO_VILA_ELIAS)
            ac, av = ajs.get((pid, 'centro')), ajs.get((pid, 'vila'))
            
            custo_bruto = p.get("PrecoCusto") or p.get("ValorCusto") or 0
            try: custo_val = float(str(custo_bruto).replace(',', '.'))
            except: custo_val = 0.0

            res.append({
                "id": pid, "nome": p.get("Nome"), "marca": p.get("Marca") or "",
                "preco_custo": custo_val,
                "saldo_centro": round(float(ac.saldo_informado) + (sc - float(ac.saldo_erp_referencia)) if ac else sc, 2),
                "saldo_vila": round(float(av.saldo_informado) + (sv - float(av.saldo_erp_referencia)) if av else sv, 2)
            })
        return JsonResponse({"produtos": res})
    except Exception as e: return JsonResponse({"erro": str(e)}, status=500)

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
        payload = {"statusSistema": "Orçamento", "cliente": data.get('cliente', 'Consumidor'), "items": []}
        for i in data.get('itens', []):
            payload["items"].append({"produtoID": i.get('id'), "quantidade": float(i.get('qtd')), "valorUnitario": float(i.get('preco')), "valorTotal": round(float(i.get('qtd'))*float(i.get('preco')), 2)})
        ok, st, res = VendaERPAPIClient().salvar_operacao_pdv(payload)
        return JsonResponse({'ok': ok, 'mensagem': 'Sucesso!' if ok else res})
    except Exception as e: return JsonResponse({'ok': False, 'erro': str(e)})

def api_buscar_clientes(request):
    c, db = obter_conexao_mongo()
    if not db: return JsonResponse({"clientes": []})
    clis = list(db[c.col_c].find({"Nome": {"$regex": request.GET.get("q",""), "$options": "i"}}).limit(10))
    return JsonResponse({"clientes": [{"nome": i.get("Nome"), "telefone": i.get("Celular", "")} for i in clis]})

def api_list_customers(request):
    c, db = obter_conexao_mongo()
    if not db: return JsonResponse({"clientes": []})
    clis = list(db[c.col_c].find({}, {"Nome": 1, "Id": 1}).limit(1000))
    res = [{"id": str(i.get("Id")), "nome": i.get("Nome")} for i in clis]
    res.sort(key=lambda x: x['nome'])
    return JsonResponse({"clientes": res})

def api_autocomplete_produtos(request):
    c, db = obter_conexao_mongo()
    if not db: return JsonResponse({"sugestoes": []})
    ps = list(db[c.col_p].find({"Nome": {"$regex": request.GET.get("q",""), "$options": "i"}}, {"Nome": 1, "Id": 1}).limit(8))
    return JsonResponse({"sugestoes": [{"id": str(i.get("Id")), "nome": i.get("Nome")} for i in ps]})

def api_todos_produtos_local(request): return JsonResponse({"produtos": []})
def api_buscar_produto_id(request, id): return JsonResponse({"id": id})