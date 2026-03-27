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
        print(f"--- ERRO MONGO: {e} ---")
        _cached_mongo_client = None
        return None, None

def normalizar_termo(txt):
    if not txt: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', txt)
                  if unicodedata.category(c) != 'Mn').lower()

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
    for c in ["Imagens", "Fotos", "ImagemProduto"]:
        arr = p.get(c)
        if isinstance(arr, list) and len(arr) > 0:
            i = arr[0]
            if isinstance(i, dict):
                for sub_c in ["Url", "Caminho", "Imagem"]:
                    val = i.get(sub_c)
                    if val: return val
            elif isinstance(i, str): return i
    return ""

# --- VIEWS DE PÁGINA ---
def consulta_produtos(request): return render(request, "produtos/consulta_produtos.html")
def historico_ajustes(request): return render(request, "produtos/historico_ajustes.html", {"ajustes": AjusteRapidoEstoque.objects.all().order_by('-criado_em')})
def sugestao_transferencia(request): return render(request, "produtos/transferencias.html")
def compras_view(request): return render(request, "produtos/compras.html")
def ajuste_mobile_view(request):
    if not request.session.get('mobile_auth'): return render(request, "produtos/ajuste_mobile_login.html")
    return render(request, "produtos/mobile_ajuste.html")

# --- APIs DE LOGIN ---
@require_POST
def api_login_mobile(request):
    pin = request.POST.get("pin")
    if PerfilUsuario.objects.filter(senha_rapida=pin).exists():
        request.session['mobile_auth'] = True
        return JsonResponse({"ok": True})
    return JsonResponse({"ok": False}, status=403)

# --- BUSCADOR PRINCIPAL (INTELIGENTE) ---
@require_GET
def api_buscar_produtos(request):
    client = None
    termo_original = request.GET.get("q", "").strip()
    if not termo_original: return JsonResponse({"produtos": []})
    
    cache_key = f"busca_prod_v12_{normalizar(termo_original).replace(' ', '_')}"
    cached = cache.get(cache_key)
    if cached: return JsonResponse(cached)

    client, db = obter_conexao_mongo()
    if db is None: return JsonResponse({"erro": "Erro conexao"}, status=500)

    try:
        palavras_originais = termo_original.split()
        condicoes_and = []
        for palavra in palavras_originais:
            tokens = expandir_tokens(palavra)
            p_norm = normalizar(palavra)
            if p_norm and p_norm not in tokens: tokens.append(p_norm)
            if tokens:
                regex_expandidos = [re.compile(re.escape(token), re.IGNORECASE) for token in tokens]
                condicoes_and.append({"BuscaTexto": {"$in": regex_expandidos}})

        termo_limpo = re.sub(r'[^a-zA-Z0-9]', '', termo_original)
        or_conditions = []
        if termo_limpo:
            or_conditions.extend([{"CodigoNFe": termo_limpo}, {"Codigo": termo_limpo}, {"CodigoBarras": termo_limpo}, {"EAN_NFe": termo_limpo}])
        if condicoes_and: or_conditions.insert(0, {"$and": condicoes_and})

        query = {"$or": or_conditions, "CadastroInativo": {"$ne": True}}
        produtos = list(db[client.col_p].find(query).limit(15))
        p_ids = [str(p.get("Id") or p["_id"]) for p in produtos]
        estoques = list(db[client.col_e].find({"ProdutoID": {"$in": p_ids}}))
        ajustes_bd = AjusteRapidoEstoque.objects.filter(produto_externo_id__in=p_ids)
        ajustes_map = {(aj.produto_externo_id, aj.deposito): aj for aj in ajustes_bd}

        res = []
        for p in produtos:
            pid = str(p.get("Id") or p["_id"])
            sc = sum(float(e.get("Saldo", 0)) for e in estoques if str(e.get("ProdutoID")) == pid and str(e.get("DepositoID")) == client.DEPOSITO_CENTRO)
            sv = sum(float(e.get("Saldo", 0)) for e in estoques if str(e.get("ProdutoID")) == pid and str(e.get("DepositoID")) == client.DEPOSITO_VILA_ELIAS)
            ac, av = ajustes_map.get((pid, 'centro')), ajustes_map.get((pid, 'vila'))
            img = _formatar_url_imagem(_extrair_imagem_produto(p, {}, pid))
            
            res.append({
                "id": pid, "nome": p.get("Nome"), "marca": p.get("Marca") or "",
                "codigo_nfe": p.get("CodigoNFe") or p.get("Codigo") or "", 
                "preco_venda": float(p.get("PrecoVenda", 0)), "imagem": img,
                "saldo_centro": round(float(ac.saldo_informado) + (sc - float(ac.saldo_erp_referencia)) if ac else sc, 2),
                "saldo_vila": round(float(av.saldo_informado) + (sv - float(av.saldo_erp_referencia)) if av else sv, 2)
            })
        
        result = {"produtos": res}
        cache.set(cache_key, result, timeout=60)
        return JsonResponse(result)
    except Exception as e: return JsonResponse({"erro": str(e)}, status=500)

# --- BUSCADOR DE COMPRAS (COM LÓGICA DE CUSTO MÁXIMO) ---
@require_GET
def api_buscar_compras(request):
    client = None
    termo_original = request.GET.get("q", "").strip()
    if not termo_original: return JsonResponse({"produtos": []})
    client, db = obter_conexao_mongo()
    if not db: return JsonResponse({"erro": "DB Offline"}, status=500)
    
    try:
        palavras_originais = termo_original.split()
        condicoes_and = []
        for palavra in palavras_originais:
            tokens = expandir_tokens(palavra)
            if tokens:
                regex_expandidos = [re.compile(re.escape(token), re.IGNORECASE) for token in tokens]
                condicoes_and.append({"$or": [{"BuscaTexto": {"$in": regex_expandidos}}, {"Nome": re.compile(re.escape(palavra), re.IGNORECASE)}]})

        termo_limpo = re.sub(r'[^a-zA-Z0-9]', '', termo_original)
        or_conditions = []
        if termo_limpo:
            or_conditions.extend([{"CodigoNFe": termo_limpo}, {"Codigo": termo_limpo}, {"CodigoBarras": termo_limpo}])
        if condicoes_and: or_conditions.insert(0, {"$and": condicoes_and})

        query = {"$or": or_conditions, "CadastroInativo": {"$ne": True}}
        produtos = list(db[client.col_p].find(query).sort("Nome", 1).limit(50))
        p_ids = [str(p.get("Id") or p["_id"]) for p in produtos]
        estoques = list(db[client.col_e].find({"ProdutoID": {"$in": p_ids}}))
        ajustes_bd = AjusteRapidoEstoque.objects.filter(produto_externo_id__in=p_ids)
        ajustes_map = {(aj.produto_externo_id, aj.deposito): aj for aj in ajustes_bd}

        def get_max_cost(doc, p_venda):
            max_val = float(str(doc.get("PrecoCusto") or doc.get("ValorCusto") or 0).replace(',', '.'))
            def traverse(obj):
                nonlocal max_val
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        if any(x in k.lower() for x in ["custo", "compra", "reposicao"]):
                            try:
                                val_f = float(str(v).replace(',', '.'))
                                if val_f > max_val and val_f < p_venda: max_val = val_f
                            except: pass
                        if isinstance(v, (dict, list)): traverse(v)
                elif isinstance(obj, list):
                    for i in obj: traverse(i)
            traverse(doc)
            return max_val

        res = []
        for p in produtos:
            pid = str(p.get("Id") or p["_id"])
            sc = sum(float(e.get("Saldo", 0)) for e in estoques if str(e.get("ProdutoID")) == pid and str(e.get("DepositoID")) == client.DEPOSITO_CENTRO)
            sv = sum(float(e.get("Saldo", 0)) for e in estoques if str(e.get("ProdutoID")) == pid and str(e.get("DepositoID")) == client.DEPOSITO_VILA_ELIAS)
            ac, av = ajustes_map.get((pid, 'centro')), ajustes_map.get((pid, 'vila'))
            pv = float(p.get("PrecoVenda") or p.get("ValorVenda") or 0)
            
            res.append({
                "id": pid, "nome": p.get("Nome"), "marca": p.get("Marca") or "",
                "preco_custo": get_max_cost(p, pv),
                "saldo_centro": round(float(ac.saldo_informado) + (sc - float(ac.saldo_erp_referencia)) if ac else sc, 2),
                "saldo_vila": round(float(av.saldo_informado) + (sv - float(av.saldo_erp_referencia)) if av else sv, 2)
            })
        return JsonResponse({"produtos": res})
    except Exception as e: return JsonResponse({"erro": str(e)}, status=500)

# --- APIs DE AJUSTE E PEDIDO ---
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
        cliente = data.get('cliente', 'Consumidor Final')
        itens = data.get('itens', [])
        client_m, db = obter_conexao_mongo()
        dep_id = ""; emp_id = ""; tabela_id = ""; cliente_id = ""
        if db is not None:
            est = db[client_m.col_e].find_one({"Deposito": {"$regex": "centro", "$options": "i"}})
            if est:
                dep_id = str(est.get("DepositoID") or ""); emp_id = str(est.get("EmpresaID") or "")
            t_db = db["DtoTabelaPreco"].find_one({"Nome": {"$regex": "PRINCIPAL|Padrão", "$options": "i"}})
            if t_db: tabela_id = str(t_db.get("Id") or t_db.get("_id") or "")
            c_db = db[client_m.col_c].find_one({"Nome": {"$regex": cliente, "$options": "i"}})
            if c_db: cliente_id = str(c_db.get("Id") or c_db.get("_id") or "")

        payload = {
            "statusSistema": "Orçamento", "cliente": cliente, "data": timezone.now().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "origemVenda": "Venda Direta", "empresa": "Agro Mais Centro",
            "deposito": "Deposito Centro", "vendedor": "Gm Agro Mais", "items": []
        }
        if dep_id: payload["depositoID"] = dep_id
        if emp_id: payload["empresaID"] = emp_id
        if tabela_id: payload["tabelaID"] = tabela_id
        if cliente_id: payload["clienteID"] = cliente_id

        for i in itens:
            q = float(i.get("qtd", 1)); v = float(i.get("preco", 0))
            payload["items"].append({"produtoID": i.get("id"), "codigo": i.get("id"), "unidade": "UN", "descricao": i.get("nome"), "quantidade": q, "valorUnitario": v, "valorTotal": round(q * v, 2)})

        ok, st, res = VendaERPAPIClient().salvar_operacao_pdv(payload)
        return JsonResponse({'ok': ok, 'mensagem': 'Sucesso!' if ok else res})
    except Exception as e: return JsonResponse({'ok': False, 'erro': str(e)})

# --- DEMAIS APIs AUXILIARES (CLIENTES, AUTOCOMPLETE, ETC) ---
def api_buscar_clientes(request):
    c, db = obter_conexao_mongo()
    if not db: return JsonResponse({"clientes": []})
    clis = list(db[c.col_c].find({"Nome": {"$regex": request.GET.get("q",""), "$options": "i"}}).limit(10))
    return JsonResponse({"clientes": [{"nome": i.get("Nome"), "documento": i.get("CpfCnpj") or "Sem Doc"} for i in clis]})

def api_list_customers(request):
    c, db = obter_conexao_mongo()
    if not db: return JsonResponse({"clientes": []})
    clis = list(db[c.col_c].find({"CadastroInativo": {"$ne": True}}, {"Nome": 1, "Id": 1}).limit(1000))
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