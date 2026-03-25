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

# --- AUXILIARES DE IMAGEM (A VERSÃO INTELIGENTE) ---
def _formatar_url_imagem(img_str):
    img_str = str(img_str or "").strip()
    if not img_str or img_str == "None": return ""
    
    # Se a imagem for um código Base64 puro
    if img_str.startswith("data:image"):
        return img_str
    if len(img_str) > 1000 and not img_str.startswith("http"):
        return "data:image/jpeg;base64," + img_str
        
    base_url = "https://cw.vendaerp.com.br"
    try:
        integ = IntegracaoERP.objects.filter(ativo=True).first()
        if integ and integ.url_base:
            base_url = integ.url_base.rstrip("/")
    except: pass

    if img_str.startswith("Uploads/"):
        return base_url + "/" + img_str
    elif img_str.startswith("/Uploads/"):
        return base_url + img_str
    elif not img_str.startswith("http"):
        return base_url + "/Uploads/Produtos/" + img_str.lstrip("/")
    return img_str

def _extrair_imagem_produto(p, mapa_imagens, pid):
    # 1. Tenta buscar no mapa usando ID, Código ou CódigoNFe
    if mapa_imagens.get(pid): return mapa_imagens.get(pid)
    if p.get("Codigo") and mapa_imagens.get(str(p.get("Codigo"))): return mapa_imagens.get(str(p.get("Codigo")))
    if p.get("CodigoNFe") and mapa_imagens.get(str(p.get("CodigoNFe"))): return mapa_imagens.get(str(p.get("CodigoNFe")))
    
    # 2. Varredura nos campos diretos do produto
    for c in ["UrlImagem", "Imagem", "CaminhoImagem", "Foto", "Url", "UrlImagemPrincipal", "ImagemPrincipal", "ImagemBase64", "FotoBase64"]:
        val = p.get(c)
        if val and isinstance(val, str) and len(val.strip()) > 2: return val
        
    # 3. Varredura em arrays (Lista de imagens do Venda ERP)
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
def historico_ajustes(request): return render(request, "produtos/historico_ajustes.html", {"ajustes": AjusteRapidoEstoque.objects.all().order_by('-criado_em')})
def sugestao_transferencia(request): return render(request, "produtos/transferencias.html")

def ajuste_mobile_view(request):
    if not request.session.get('mobile_auth'):
        return render(request, "produtos/ajuste_mobile_login.html")
    return render(request, "produtos/mobile_ajuste.html")

# --- APIs DE LOGIN E BUSCA (COM TODA A LÓGICA DE ESTOQUE) ---
@require_POST
def api_login_mobile(request):
    pin = request.POST.get("pin")
    if PerfilUsuario.objects.filter(senha_rapida=pin).exists():
        request.session['mobile_auth'] = True
        return JsonResponse({"ok": True})
    return JsonResponse({"ok": False}, status=403)

@require_GET
def api_buscar_produtos(request):
    termo_original = request.GET.get("q", "").strip()
    if not termo_original:
        return JsonResponse({"produtos": []})

    cache_key = f"busca_prod_v10_{normalizar(termo_original).replace(' ', '_')}"
    cached_data = cache.get(cache_key)
    if cached_data: return JsonResponse(cached_data)

    client, db = obter_conexao_mongo()
    if db is None: return JsonResponse({"erro": "Erro conexao"}, status=500)

    try:
        palavras_originais = termo_original.split()
        condicoes_and = []

        for palavra in palavras_originais:
            if re.search(r'\d', palavra):
                 condicoes_and.append({"BuscaTexto": {"$regex": re.escape(palavra), "$options": "i"}})
            else:
                tokens_expandidos = expandir_tokens(palavra)
                palavra_norm = normalizar(palavra)
                if palavra_norm and palavra_norm not in tokens_expandidos:
                    tokens_expandidos.append(palavra_norm)

                if tokens_expandidos:
                    regex_expandidos = [re.compile(re.escape(token), re.IGNORECASE) for token in tokens_expandidos]
                    condicoes_and.append({"BuscaTexto": {"$in": regex_expandidos}})

        termo_limpo = re.sub(r'[^a-zA-Z0-9]', '', termo_original)
        or_conditions = [
            {"CodigoNFe": termo_limpo},
            {"Codigo": termo_limpo},
            {"CodigoBarras": termo_limpo},
            {"EAN_NFe": termo_limpo}
        ]

        if condicoes_and:
            or_conditions.insert(0, {"$and": condicoes_and})

        if not or_conditions:
             return JsonResponse({"produtos": []})

        query = {"$or": or_conditions, "CadastroInativo": {"$ne": True}}
        produtos = list(db[client.col_p].find(query).limit(15))
        p_ids = [str(p.get("Id") or p["_id"]) for p in produtos]
        estoques = list(db[client.col_e].find({"ProdutoID": {"$in": p_ids}}))

        ajustes_bd = AjusteRapidoEstoque.objects.filter(produto_externo_id__in=p_ids).order_by('produto_externo_id', 'deposito', '-criado_em')
        ajustes_map = {}
        for aj in ajustes_bd:
            if (aj.produto_externo_id, aj.deposito) not in ajustes_map:
                ajustes_map[(aj.produto_externo_id, aj.deposito)] = aj
                
        obj_ids = []
        for pid in p_ids:
            if len(pid) == 24:
                try: obj_ids.append(ObjectId(pid))
                except: pass
                
        query_img = {"$or": [{"ProdutoID": {"$in": p_ids}}]}
        if obj_ids: query_img["$or"].append({"ProdutoID": {"$in": obj_ids}})
        cods = [str(x.get("Codigo")) for x in produtos if x.get("Codigo")]
        if cods: query_img["$or"].append({"ProdutoID": {"$in": cods}})

        mapa_imagens = {}
        try:
            for img in db["DtoImagemProduto"].find(query_img):
                val = img.get("Url") or img.get("UrlImagem") or img.get("Imagem") or img.get("ImagemBase64") or img.get("Base64") or ""
                if val: mapa_imagens[str(img.get("ProdutoID"))] = val
        except: pass
        try:
            for img in db["DtoProdutoImagem"].find(query_img):
                val = img.get("Url") or img.get("UrlImagem") or img.get("Imagem") or img.get("ImagemBase64") or img.get("Base64") or ""
                if val and str(img.get("ProdutoID")) not in mapa_imagens: mapa_imagens[str(img.get("ProdutoID"))] = val
        except: pass

        res = []
        for p in produtos:
            pid = str(p.get("Id") or p["_id"])
            s_c = 0.0; s_v = 0.0
            for est in estoques:
                if str(est.get("ProdutoID")) == pid:
                    val = float(est.get("Saldo") or 0)
                    did = str(est.get("DepositoID") or "")
                    if did == client.DEPOSITO_CENTRO: s_c = val
                    elif did == client.DEPOSITO_VILA_ELIAS: s_v = val

            aj_c = ajustes_map.get((pid, 'centro'))
            aj_v = ajustes_map.get((pid, 'vila'))
            saldo_f_c = float(aj_c.saldo_informado) + (s_c - float(aj_c.saldo_erp_referencia)) if aj_c else s_c
            saldo_f_v = float(aj_v.saldo_informado) + (s_v - float(aj_v.saldo_erp_referencia)) if aj_v else s_v
            img_url = _formatar_url_imagem(_extrair_imagem_produto(p, mapa_imagens, pid))
            
            res.append({
                "id": pid, "nome": p.get("Nome"), "marca": p.get("Marca") or "",
                "codigo_nfe": p.get("CodigoNFe") or p.get("Codigo") or "", 
                "preco_venda": float(p.get("ValorVenda") or p.get("PrecoVenda") or 0),
                "imagem": img_url,
                "saldo_centro": round(saldo_f_c, 2), "saldo_vila": round(saldo_f_v, 2),
                "saldo_erp_centro": s_c, "saldo_erp_vila": s_v
            })
        
        resultado_final = {"produtos": res}
        cache.set(cache_key, resultado_final, timeout=60)
        return JsonResponse(resultado_final)
    except Exception as e: return JsonResponse({"erro": str(e)}, status=500)

@require_POST
def api_ajustar_estoque(request):
    pin = request.POST.get("pin")
    # Aceita se estiver logado no mobile ou se o PIN estiver correto
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

# --- API DE ENVIO AO ERP (A VERSÃO QUE BUSCA OS IDs NO MONGO) ---
@require_POST
def api_enviar_pedido_erp(request):
    try:
        data = json.loads(request.body)
        cliente = data.get('cliente', 'Consumidor Final')
        itens = data.get('itens', [])
        if not itens: return JsonResponse({'ok': False, 'erro': 'Carrinho vazio'})
            
        data_atual = timezone.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")
        client_m, db = obter_conexao_mongo()
        
        dep_id = ""; emp_id = ""; tabela_id = ""; cliente_id = ""

        if client_m and db is not None:
            # Pega IDs reais do Mongo para a API não recusar
            est = db[client_m.col_e].find_one({"Deposito": {"$regex": "centro", "$options": "i"}})
            if est:
                dep_id = str(est.get("DepositoID") or ""); emp_id = str(est.get("EmpresaID") or "")
            t_db = db["DtoTabelaPreco"].find_one({"Nome": {"$regex": "PRINCIPAL|Padrão", "$options": "i"}})
            if t_db: tabela_id = str(t_db.get("Id") or t_db.get("_id") or "")
            c_db = db[client_m.col_c].find_one({"Nome": {"$regex": cliente, "$options": "i"}})
            if c_db: cliente_id = str(c_db.get("Id") or c_db.get("_id") or "")

        payload = {
            "statusSistema": "Orçamento", "cliente": cliente, "data": data_atual,
            "origemVenda": "Venda Direta", "empresa": "Agro Mais Centro",
            "deposito": "Deposito Centro", "vendedor": "Gm Agro Mais", "items": []
        }
        if dep_id: payload["depositoID"] = dep_id
        if emp_id: payload["empresaID"] = emp_id
        if tabela_id: payload["tabelaID"] = tabela_id
        if cliente_id: payload["clienteID"] = cliente_id

        for i in itens:
            q = float(i.get("qtd", 1)); v = float(i.get("preco", 0))
            payload["items"].append({
                "produtoID": i.get("id"), "codigo": i.get("id"), "unidade": "UN",
                "descricao": i.get("nome"), "quantidade": q, "valorUnitario": v, "valorTotal": round(q * v, 2)
            })

        sucesso, status, resposta = VendaERPAPIClient().salvar_operacao_pdv(payload)
        return JsonResponse({'ok': sucesso, 'mensagem': 'Sucesso!' if sucesso else resposta})
    except Exception as e: return JsonResponse({'ok': False, 'erro': str(e)}, status=500)

# --- OUTRAS APIs ---
def api_buscar_clientes(request):
    termo = request.GET.get("q", "").strip()

    cache_key = f"busca_cli_{termo.replace(' ', '_')}"
    cached_data = cache.get(cache_key)
    if cached_data: return JsonResponse(cached_data)

    client, db = obter_conexao_mongo()
    if not termo or db is None: return JsonResponse({"clientes": []})
    try:
        query = {"$or": [
            {"Nome": {"$regex": termo, "$options": "i"}},
            {"RazaoSocial": {"$regex": termo, "$options": "i"}},
            {"NomeFantasia": {"$regex": termo, "$options": "i"}},
            {"CpfCnpj": {"$regex": termo, "$options": "i"}}
        ]}
        
        clientes = list(db[client.col_c].find(query).limit(10))
        
        res = []
        for c in clientes:
            nome = c.get("Nome") or c.get("RazaoSocial") or c.get("NomeFantasia")
            if nome:
                res.append({
                    "nome": nome, 
                    "documento": c.get("CpfCnpj") or c.get("Cpf") or c.get("Cnpj") or "Sem Doc",
                    "telefone": c.get("Celular") or c.get("Telefone") or c.get("Fone") or ""
                })
        
        resultado_final = {"clientes": res}
        cache.set(cache_key, resultado_final, timeout=120)
        return JsonResponse(resultado_final)
    except Exception as e: return JsonResponse({"erro": str(e)}, status=500)

def api_list_customers(request):
    """
    Retorna uma lista de todos os clientes para preencher um select/combobox.
    """
    cache_key = "lista_todos_clientes_v3"
    cached_data = cache.get(cache_key)
    if cached_data:
        return JsonResponse(cached_data)

    client, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"erro": "Erro de conexão com o banco de dados"}, status=500)

    try:
        projecao = {"Nome": 1, "RazaoSocial": 1, "NomeFantasia": 1, "_id": 0, "Id": 1}
        
        clientes_cursor = db[client.col_c].find({"CadastroInativo": {"$ne": True}}, projecao).limit(2000)
        
        lista_clientes = []
        for c in clientes_cursor:
            nome = c.get("Nome") or c.get("RazaoSocial") or c.get("NomeFantasia")
            if nome:
                lista_clientes.append({
                    "id": str(c.get("Id") or c.get("_id")),
                    "nome": nome.strip()
                })
        
        lista_clientes.sort(key=lambda x: x['nome'])

        resultado = {"clientes": lista_clientes}
        
        cache.set(cache_key, resultado, timeout=3600) 
        
        return JsonResponse(resultado)
    except Exception as e:
        return JsonResponse({"erro": str(e)}, status=500)

def api_autocomplete_produtos(request):
    termo = request.GET.get("q", "").strip()
    if len(termo) < 2: return JsonResponse({"sugestoes": []})

    cache_key = f"auto_prod_v10_{normalizar(termo).replace(' ', '_')}"
    cached_data = cache.get(cache_key)
    if cached_data: return JsonResponse(cached_data)

    client, db = obter_conexao_mongo()
    if db is None: return JsonResponse({"sugestoes": []})
    try:
        palavras_originais = termo.split()
        condicoes_and = []

        for palavra in palavras_originais:
            if re.search(r'\d', palavra):
                 condicoes_and.append({"BuscaTexto": {"$regex": re.escape(palavra), "$options": "i"}})
            else:
                tokens_expandidos = expandir_tokens(palavra)
                palavra_norm = normalizar(palavra)
                if palavra_norm and palavra_norm not in tokens_expandidos:
                    tokens_expandidos.append(palavra_norm)

                if tokens_expandidos:
                    regex_expandidos = [re.compile(re.escape(token), re.IGNORECASE) for token in tokens_expandidos]
                    condicoes_and.append({"BuscaTexto": {"$in": regex_expandidos}})

        if not condicoes_and: return JsonResponse({"sugestoes": []})

        query = {"$and": condicoes_and, "CadastroInativo": {"$ne": True}}
        projecao = {"Nome": 1, "Marca": 1, "ValorVenda": 1, "PrecoVenda": 1, "Id": 1, "UrlImagem": 1, "Imagem": 1, "Imagens": 1, "Fotos": 1}
        sugestoes = list(db[client.col_p].find(query, projecao).limit(8))
        res = []
        for s in sugestoes:
            res.append({
                "id": str(s.get("Id") or s["_id"]), "nome": s.get("Nome"), "marca": s.get("Marca") or "", 
                "preco_venda": float(s.get("PrecoVenda") or s.get("ValorVenda") or 0), 
                "imagem": _formatar_url_imagem(_extrair_imagem_produto(s, {}, str(s.get("Id") or s["_id"])))
            })
        cache.set(cache_key, {"sugestoes": res}, timeout=60)
        return JsonResponse({"sugestoes": res})
    except Exception: return JsonResponse({"sugestoes": []})

def api_todos_produtos_local(request):
    cache_key = "carga_inicial_produtos_todos_v12"
    cached_data = cache.get(cache_key)
    if cached_data: return JsonResponse(cached_data)

    client, db = obter_conexao_mongo()
    if db is None: return JsonResponse({"erro": "Erro conexao"}, status=500)

    try:
        query = {"CadastroInativo": {"$ne": True}}
        projecao = {
            "Nome": 1, "Marca": 1, "CodigoNFe": 1, "Codigo": 1, 
            "CodigoBarras": 1, "EAN_NFe": 1, "ValorVenda": 1, "PrecoVenda": 1, 
            "UrlImagem": 1, "Imagem": 1, "Imagens": 1, "Fotos": 1, "CaminhoImagem": 1,
            "NomeCategoria": 1, "Categoria": 1, "Categorias": 1, "Grupo": 1, "SubGrupo": 1,
            "NomeFornecedor": 1, "Fornecedor": 1, "RazaoSocialFornecedor": 1, "Fabricante": 1
        }
        produtos = list(db[client.col_p].find(query, projecao))
        p_ids = [str(p.get("Id") or p["_id"]) for p in produtos]
        
        estoques = list(db[client.col_e].find(
            {"ProdutoID": {"$in": p_ids}},
            {"ProdutoID": 1, "DepositoID": 1, "Saldo": 1, "_id": 0}
        ))
        
        ajustes_bd = AjusteRapidoEstoque.objects.all().order_by('produto_externo_id', 'deposito', '-criado_em')
        ajustes_map = {}
        for aj in ajustes_bd:
            if (aj.produto_externo_id, aj.deposito) not in ajustes_map:
                ajustes_map[(aj.produto_externo_id, aj.deposito)] = aj
        
        est_map = {}
        for est in estoques:
            pid = str(est.get("ProdutoID"))
            if pid not in est_map: est_map[pid] = {'c': 0.0, 'v': 0.0}
            did = str(est.get("DepositoID") or "")
            if did == client.DEPOSITO_CENTRO: est_map[pid]['c'] += float(est.get("Saldo") or 0)
            elif did == client.DEPOSITO_VILA_ELIAS: est_map[pid]['v'] += float(est.get("Saldo") or 0)

        res = []
        for p in produtos:
            pid = str(p.get("Id") or p["_id"])
            s_c = est_map.get(pid, {}).get('c', 0.0)
            s_v = est_map.get(pid, {}).get('v', 0.0)

            aj_c = ajustes_map.get((pid, 'centro'))
            aj_v = ajustes_map.get((pid, 'vila'))
            saldo_f_c = float(aj_c.saldo_informado) + (s_c - float(aj_c.saldo_erp_referencia)) if aj_c else s_c
            saldo_f_v = float(aj_v.saldo_informado) + (s_v - float(aj_v.saldo_erp_referencia)) if aj_v else s_v

            res.append({
                "id": pid, 
                "nome": p.get("Nome"), 
                "marca": p.get("Marca"),
                "fornecedor": p.get("NomeFornecedor") or p.get("Fornecedor") or p.get("RazaoSocialFornecedor") or p.get("Fabricante"),
                "categoria": p.get("NomeCategoria") or p.get("Categoria") or p.get("Grupo") or p.get("SubGrupo"),
                "codigo_nfe": p.get("CodigoNFe") or p.get("Codigo"), 
                "codigo_barras": p.get("CodigoBarras") or p.get("EAN_NFe"),
                "preco_venda": float(p.get("ValorVenda") or p.get("PrecoVenda") or 0),
                "saldo_centro": round(saldo_f_c, 2), 
                "saldo_vila": round(saldo_f_v, 2),
                "saldo_erp_centro": s_c,
                "saldo_erp_vila": s_v,
                "busca_texto": p.get("BuscaTexto", ""),
            })
        
        resultado_final = {"produtos": res}
        cache.set(cache_key, resultado_final, timeout=3600) # Aumentado para 1h
        return JsonResponse(resultado_final)
    except Exception as e: return JsonResponse({"erro": str(e)}, status=500)

def api_buscar_produto_id(request, id):
    client, db = obter_conexao_mongo()
    if db is None: return JsonResponse({"erro": "Erro conexao"}, status=500)
    try:
        from bson import ObjectId
        query = {"$or": [{"Id": id}]}
        try:
            query["$or"].append({"_id": ObjectId(id)})
        except Exception: pass

        p = db[client.col_p].find_one(query)
        if not p: return JsonResponse({"erro": "Produto nao encontrado"}, status=404)

        estoques = list(db[client.col_e].find({"ProdutoID": id}))
        
        ajustes_bd = AjusteRapidoEstoque.objects.filter(produto_externo_id=id).order_by('deposito', '-criado_em')
        ajustes_map = {}
        for aj in ajustes_bd:
            if aj.deposito not in ajustes_map:
                ajustes_map[aj.deposito] = aj

        s_c = 0.0; s_v = 0.0
        for est in estoques:
            val = float(est.get("Saldo") or 0)
            did = str(est.get("DepositoID") or "")
            if did == client.DEPOSITO_CENTRO: s_c += val
            elif did == client.DEPOSITO_VILA_ELIAS: s_v += val

        aj_c = ajustes_map.get('centro')
        aj_v = ajustes_map.get('vila')
        saldo_f_c = float(aj_c.saldo_informado) + (s_c - float(aj_c.saldo_erp_referencia)) if aj_c else s_c
        saldo_f_v = float(aj_v.saldo_informado) + (s_v - float(aj_v.saldo_erp_referencia)) if aj_v else s_v
        
        mapa_img = {}
        query_ids = [id]
        if p.get("Codigo"): query_ids.append(str(p.get("Codigo")))
        try:
            for img in db["DtoImagemProduto"].find({"ProdutoID": {"$in": query_ids}}):
                val = img.get("Url") or img.get("UrlImagem") or img.get("Imagem") or img.get("ImagemBase64") or img.get("Base64") or ""
                if val: mapa_img[str(img.get("ProdutoID"))] = val
        except: pass
        
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
    except Exception as e: return JsonResponse({"erro": str(e)}, status=500)