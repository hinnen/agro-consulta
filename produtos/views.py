import json, unicodedata, re
from decimal import Decimal
from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.http import require_GET, require_POST
from base.models import Empresa, PerfilUsuario
from estoque.models import AjusteRapidoEstoque
from integracoes.venda_erp_mongo import VendaERPMongoClient

# --- CONEXÃO ---
def obter_conexao_mongo():
    try:
        client = VendaERPMongoClient()
        client.client.admin.command('ping')
        return client, client.db
    except Exception as e:
        print(f"--- ERRO MONGO: {e} ---")
        return None, None

def normalizar_termo(txt):
    if not txt: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', txt)
                  if unicodedata.category(c) != 'Mn').lower()

# --- PÁGINAS ---
def consulta_produtos(request):
    return render(request, "produtos/consulta_produtos.html")

def historico_ajustes(request):
    ajustes = AjusteRapidoEstoque.objects.all().order_by('-criado_em')
    return render(request, "produtos/historico_ajustes.html", {"ajustes": ajustes})

def sugestao_transferencia(request):
    return render(request, "produtos/transferencias.html")

# --- APIs ---

@require_GET
def api_buscar_produtos(request):
    termo_original = request.GET.get("q", "").strip()
    client, db = obter_conexao_mongo()
    if db is None: return JsonResponse({"erro": "Erro conexao"}, status=500)

    termo_norm = normalizar_termo(termo_original)
    termo_limpo = termo_original.replace(" ", "")
    palavras = termo_norm.split()
    
    try:
        regex_parts = [f"(?=.*{re.escape(p)})" for p in palavras]
        regex_final = "".join(regex_parts) + ".*"

        query = {"$or": [
            {"BuscaTexto": {"$regex": regex_final, "$options": "i"}},
            {"Nome": {"$regex": regex_final, "$options": "i"}},
            {"CodigoNFe": termo_limpo}, {"CodigoBarras": termo_limpo}, {"EAN_NFe": termo_limpo}
        ], "CadastroInativo": {"$ne": True}}

        produtos = list(db[client.col_p].find(query).limit(15))
        p_ids = [str(p.get("Id") or p["_id"]) for p in produtos]
        estoques = list(db[client.col_e].find({"ProdutoID": {"$in": p_ids}}))

        # Pre-fetch adjustments to prevent N+1 query problem inside the loop
        ajustes_bd = AjusteRapidoEstoque.objects.filter(produto_externo_id__in=p_ids).order_by('produto_externo_id', 'deposito', '-criado_em')
        ajustes_map = {}
        for aj in ajustes_bd:
            if (aj.produto_externo_id, aj.deposito) not in ajustes_map:
                ajustes_map[(aj.produto_externo_id, aj.deposito)] = aj

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
            
            res.append({
                "id": pid, "nome": p.get("Nome"), "marca": p.get("Marca") or "",
                "codigo_nfe": p.get("CodigoNFe") or "", 
                "preco_venda": float(p.get("ValorVenda") or p.get("PrecoVenda") or 0),
                "saldo_centro": round(saldo_f_c, 2), "saldo_vila": round(saldo_f_v, 2)
            })
        return JsonResponse({"produtos": res})
    except Exception as e: return JsonResponse({"erro": str(e)}, status=500)

@require_GET
def api_autocomplete_produtos(request):
    termo = request.GET.get("q", "").strip()
    client, db = obter_conexao_mongo()
    if len(termo) < 2 or db is None: return JsonResponse({"sugestoes": []})
    termo_norm = normalizar_termo(termo)
    try:
        query = {"$or": [{"BuscaTexto": {"$regex": f"^{termo_norm}", "$options": "i"}}, {"Nome": {"$regex": f"^{termo_norm}", "$options": "i"}}], "CadastroInativo": {"$ne": True}}
        sugestoes = list(db[client.col_p].find(query, {"Nome": 1, "Marca": 1, "ValorVenda": 1, "PrecoVenda": 1, "Id": 1}).limit(8))
        res = [{"id": str(s.get("Id") or s["_id"]), "nome": s.get("Nome"), "marca": s.get("Marca") or "", "preco_venda": float(s.get("PrecoVenda") or s.get("ValorVenda") or 0)} for s in sugestoes]
        return JsonResponse({"sugestoes": res})
    except Exception: return JsonResponse({"sugestoes": []})

@require_POST
def api_ajustar_estoque(request):
    pin = request.POST.get("pin")
    perfil = PerfilUsuario.objects.filter(senha_rapida=pin).first()
    if not perfil: return JsonResponse({"ok": False, "erro": "PIN INCORRETO"}, status=403)
    try:
        empresa = Empresa.objects.filter(nome_fantasia="Agro Mais").first()
        AjusteRapidoEstoque.objects.create(
            empresa=empresa, produto_externo_id=request.POST.get("produto_id"),
            deposito=request.POST.get("deposito", "centro"), nome_produto=request.POST.get("nome_produto"),
            saldo_erp_referencia=Decimal(request.POST.get("saldo_atual", "0")),
            saldo_informado=Decimal(request.POST.get("novo_saldo", "0"))
        )
        return JsonResponse({"ok": True})
    except Exception as e: return JsonResponse({"ok": False, "erro": str(e)}, status=500)

@require_GET
def api_buscar_clientes(request):
    termo = request.GET.get("q", "").strip()
    client, db = obter_conexao_mongo()
    if not termo or db is None: return JsonResponse({"clientes": []})
    try:
        query = {"$or": [{"Nome": {"$regex": termo, "$options": "i"}}, {"CpfCnpj": {"$regex": termo, "$options": "i"}}]}
        clientes = list(db[client.col_c].find(query).limit(10))
        res = [{"nome": c.get("Nome"), "documento": c.get("CpfCnpj") or "Sem Doc"} for c in clientes]
        return JsonResponse({"clientes": res})
    except Exception as e: return JsonResponse({"erro": str(e)}, status=500)

@require_POST
def api_deletar_ajuste(request, id):
    pin = request.POST.get("pin")
    if pin != "1234": 
        return JsonResponse({"ok": False, "erro": "PIN INCORRETO"}, status=403)
    try:
        AjusteRapidoEstoque.objects.filter(id=id).delete()
        return JsonResponse({"ok": True})
    except Exception as e: 
        return JsonResponse({"ok": False, "erro": str(e)}, status=500)

@require_POST
def api_limpar_historico(request):
    pin = request.POST.get("pin")
    if pin != "1234": 
        return JsonResponse({"ok": False, "erro": "PIN INCORRETO"}, status=403)
    try:
        AjusteRapidoEstoque.objects.all().delete()
        return JsonResponse({"ok": True})
    except Exception as e: 
        return JsonResponse({"ok": False, "erro": str(e)}, status=500)