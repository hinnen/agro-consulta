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
    except Exception as e:
        _cached_mongo_client = None
        return None, None

# --- AUXILIARES DE IMAGEM ---
def _formatar_url_imagem(img_str):
    img_str = str(img_str or "").strip()
    if not img_str or img_str == "None": return ""
    if img_str.startswith("data:image"): return img_str
    if len(img_str) > 1000 and not img_str.startswith("http"):
        return "data:image/jpeg;base64," + img_str
    base_url = "https://cw.vendaerp.com.br"
    try:
        integ = IntegracaoERP.objects.filter(ativo=True).first()
        if integ and integ.url_base: base_url = integ.url_base.rstrip("/")
    except: pass
    if img_str.startswith("Uploads/"): return base_url + "/" + img_str
    elif img_str.startswith("/Uploads/"): return base_url + img_str
    elif not img_str.startswith("http"): return base_url + "/Uploads/Produtos/" + img_str.lstrip("/")
    return img_str

def _extrair_imagem_produto(p, mapa_imagens, pid):
    if mapa_imagens.get(pid): return mapa_imagens.get(pid)
    if p.get("Codigo") and mapa_imagens.get(str(p.get("Codigo"))): return mapa_imagens.get(str(p.get("Codigo")))
    if p.get("CodigoNFe") and mapa_imagens.get(str(p.get("CodigoNFe"))): return mapa_imagens.get(str(p.get("CodigoNFe")))
    for c in ["UrlImagem", "Imagem", "CaminhoImagem", "Foto", "Url", "UrlImagemPrincipal", "ImagemPrincipal", "ImagemBase64", "FotoBase64"]:
        val = p.get(c)
        if val and isinstance(val, str) and len(val.strip()) > 2: return val
    for c in ["Imagens", "Fotos", "ImagemProduto", "ProdutoImagem"]:
        arr = p.get(c)
        if isinstance(arr, list) and len(arr) > 0:
            i = arr[0]
            if isinstance(i, dict):
                for sub_c in ["Url", "UrlImagem", "Caminho", "Imagem", "Path", "ImagemBase64", "Base64"]:
                    val = i.get(sub_c)
                    if val and isinstance(val, str) and len(val.strip()) > 2: return val
            elif isinstance(i, str): return i
    return ""

# --- VIEWS DE PÁGINA ---
def consulta_produtos(request): return render(request, "produtos/consulta_produtos.html")
def historico_ajustes(request): 
    ajustes = AjusteRapidoEstoque.objects.all().order_by('-criado_em')
    return render(request, "produtos/historico_ajustes.html", {"ajustes": ajustes})
def sugestao_transferencia(request): return render(request, "produtos/transferencias.html")
def compras_view(request): return render(request, "produtos/compras.html")
def ajuste_mobile_view(request):
    if not request.session.get('mobile_auth'):
        return render(request, "produtos/ajuste_mobile_login.html")
    return render(request, "produtos/mobile_ajuste.html")

# --- MOTOR DE BUSCA ÚNICO (ESTILO TRANSFERÊNCIA) ---
def motor_de_busca_agro(termo_original, db, client, limit=20):
    if not termo_original: return []
    termo_limpo = re.sub(r'[^a-zA-Z0-9]', '', termo_original)
    
    # 1. Tenta Códigos primeiro
    if termo_limpo:
        query_cod = {"$or": [
            {"Codigo": termo_limpo}, {"CodigoNFe": termo_limpo},
            {"CodigoBarras": termo_limpo}, {"EAN_NFe": termo_limpo}
        ], "CadastroInativo": {"$ne": True}}
        prods = list(db[client.col_p].find(query_cod).limit(5))
        if prods: return prods

    # 2. Busca Inteligente (O que faz o Milho Grande funcionar)
    palavras = termo_original.split()
    condicoes_and = []
    for p in palavras:
        tokens = expandir_tokens(p)
        p_norm = normalizar(p)
        if p_norm and p_norm not in tokens: tokens.append(p_norm)
        
        regex_tokens = [re.compile(re.escape(t), re.IGNORECASE) for t in tokens]
        condicoes_and.append({"$or": [
            {"BuscaTexto": {"$in": regex_tokens}},
            {"Nome": {"$regex": re.escape(p), "$options": "i"}},
            {"Marca": {"$regex": re.escape(p), "$options": "i"}},
            {"Codigo": {"$regex": re.escape(p), "$options": "i"}}
        ]})
    
    if not condicoes_and: return []
    return list(db[client.col_p].find({"$and": condicoes_and, "CadastroInativo": {"$ne": True}}).limit(limit))

# --- APIs DE BUSCA ---
@require_GET
def api_buscar_produtos(request):
    q = request.GET.get("q", "").strip()
    client, db = obter_conexao_mongo()
    if not db or not q: return JsonResponse({"produtos": []})
    
    try:
        prods = motor_de_busca_agro(q, db, client)
        p_ids = [str(p.get("Id") or p["_id"]) for p in prods]
        estoques = list(db[client.col_e].find({"ProdutoID": {"$in": p_ids}}))
        ajustes_bd = AjusteRapidoEstoque.objects.filter(produto_externo_id__in=p_ids)
        ajustes_map = {(aj.produto_externo_id, aj.deposito): aj for aj in ajustes_bd}

        res = []
        for p in prods:
            pid = str(p.get("Id") or p["_id"])
            sc = sum(float(e.get("Saldo", 0)) for e in estoques if str(e.get("ProdutoID")) == pid and str(e.get("DepositoID")) == client.DEPOSITO_CENTRO)
            sv = sum(float(e.get("Saldo", 0)) for e in estoques if str(e.get("ProdutoID")) == pid and str(e.get("DepositoID")) == client.DEPOSITO_VILA_ELIAS)
            ac, av = ajustes_map.get((pid, 'centro')), ajustes_map.get((pid, 'vila'))
            
            res.append({
                "id": pid, "nome": p.get("Nome"), "marca": p.get("Marca") or "",
                "codigo_nfe": p.get("CodigoNFe") or p.get("Codigo") or "", 
                "preco_venda": float(p.get("ValorVenda") or p.get("PrecoVenda") or 0),
                "imagem": _formatar_url_imagem(_extrair_imagem_produto(p, {}, pid)),
                "saldo_centro": round(float(ac.saldo_informado) + (sc - float(ac.saldo_erp_referencia)) if ac else sc, 2),
                "saldo_vila": round(float(av.saldo_informado) + (sv - float(av.saldo_erp_referencia)) if av else sv, 2)
            })
        return JsonResponse({"produtos": res})
    except Exception as e: return JsonResponse({"erro": str(e)}, status=500)

@require_GET
def api_buscar_compras(request):
    q = request.GET.get("q", "").strip()
    client, db = obter_conexao_mongo()
    if not db or not q: return JsonResponse({"produtos": []})
    
    try:
        prods = motor_de_busca_agro(q, db, client, limit=50)
        p_ids = [str(p.get("Id") or p["_id"]) for p in prods]
        estoques = list(db[client.col_e].find({"ProdutoID": {"$in": p_ids}}))
        ajustes_bd = AjusteRapidoEstoque.objects.filter(produto_externo_id__in=p_ids)
        ajustes_map = {(aj.produto_externo_id, aj.deposito): aj for aj in ajustes_bd}

        res = []
        for p in prods:
            pid = str(p.get("Id") or p["_id"])
            sc = sum(float(e.get("Saldo", 0)) for e in estoques if str(e.get("ProdutoID")) == pid and str(e.get("DepositoID")) == client.DEPOSITO_CENTRO)
            sv = sum(float(e.get("Saldo", 0)) for e in estoques if str(e.get("ProdutoID")) == pid and str(e.get("DepositoID")) == client.DEPOSITO_VILA_ELIAS)
            ac, av = ajustes_map.get((pid, 'centro')), ajustes_map.get((pid, 'vila'))
            custo = float(str(p.get("PrecoCusto") or p.get("ValorCusto") or 0).replace(',', '.'))
            
            res.append({
                "id": pid, "nome": p.get("Nome"), "marca": p.get("Marca") or "", "preco_custo": custo,
                "saldo_centro": round(float(ac.saldo_informado) + (sc - float(ac.saldo_erp_referencia)) if ac else sc, 2),
                "saldo_vila": round(float(av.saldo_informado) + (sv - float(av.saldo_erp_referencia)) if av else sv, 2)
            })
        return JsonResponse({"produtos": res})
    except Exception as e: return JsonResponse({"erro": str(e)}, status=500)

# --- APIs DE ESTOQUE E PEDIDO ---
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
            empresa = Empresa.objects.filter(nome_fantasia="Agro Mais").first()
            AjusteRapidoEstoque.objects.create(
                empresa=empresa, produto_externo_id=request.POST.get("produto_id"),
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
            payload["items"].append({"produtoID": i.get("id"), "codigo": i.get("id"), "unidade": "UN", "descricao": i.get("nome"), "quantidade": float(i.get("qtd")), "valorUnitario": float(i.get("preco")), "valorTotal": round(float(i.get("qtd"))*float(i.get("preco")), 2)})

        ok, status, res = VendaERPAPIClient().salvar_operacao_pdv(payload)
        return JsonResponse({'ok': ok, 'mensagem': res})
    except Exception as e: return JsonResponse({'ok': False, 'erro': str(e)})

# --- CARGA INICIAL E APIs AUXILIARES ---
@require_GET
def api_todos_produtos_local(request):
    client, db = obter_conexao_mongo()
    if db is None: return JsonResponse({"erro": "Erro conexao"}, status=500)
    try:
        produtos = list(db[client.col_p].find({"CadastroInativo": {"$ne": True}}))
        res = []
        for p in produtos:
            pid = str(p.get("Id") or p["_id"])
            res.append({
                "id": pid, "nome": p.get("Nome"), "marca": p.get("Marca") or "",
                "busca_texto": normalizar(f"{p.get('Nome')} {p.get('Marca')} {p.get('Codigo')}")
            })
        return JsonResponse({"produtos": res})
    except: return JsonResponse({"produtos": []})

def api_list_customers(request):
    client, db = obter_conexao_mongo()
    if not db: return JsonResponse({"clientes": []})
    try:
        clis = list(db[client.col_c].find({"CadastroInativo": {"$ne": True}}, {"Nome": 1, "Id": 1}).limit(1000))
        res = [{"id": str(i.get("Id") or i.get("_id")), "nome": i.get("Nome").strip()} for i in clis]
        res.sort(key=lambda x: x['nome'])
        return JsonResponse({"clientes": res})
    except: return JsonResponse({"clientes": []})

def api_buscar_clientes(request):
    client, db = obter_conexao_mongo()
    termo = request.GET.get("q", "")
    if not db: return JsonResponse({"clientes": []})
    try:
        clis = list(db[client.col_c].find({"Nome": {"$regex": termo, "$options": "i"}}, {"Nome": 1, "CpfCnpj": 1}).limit(10))
        res = [{"nome": i.get("Nome"), "documento": i.get("CpfCnpj") or "Sem Doc"} for i in clis]
        return JsonResponse({"clientes": res})
    except: return JsonResponse({"clientes": []})

def api_autocomplete_produtos(request):
    client, db = obter_conexao_mongo()
    termo = request.GET.get("q", "")
    if not db or len(termo) < 2: return JsonResponse({"sugestoes": []})
    try:
        ps = list(db[client.col_p].find({"Nome": {"$regex": termo, "$options": "i"}}, {"Nome": 1, "Id": 1}).limit(8))
        res = [{"id": str(i.get("Id") or i.get("_id")), "nome": i.get("Nome")} for i in ps]
        return JsonResponse({"sugestoes": res})
    except: return JsonResponse({"sugestoes": []})

def api_buscar_produto_id(request, id):
    client, db = obter_conexao_mongo()
    if not db: return JsonResponse({"erro": "DB Offline"}, status=500)
    p = db[client.col_p].find_one({"Id": id})
    if not p: return JsonResponse({"erro": "Nao encontrado"}, status=404)
    return JsonResponse({"id": id, "nome": p.get("Nome")})