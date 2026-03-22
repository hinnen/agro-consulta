import json
import unicodedata
import re
from datetime import datetime, timedelta
from decimal import Decimal
from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.http import require_GET, require_POST
from base.models import Empresa, PerfilUsuario
from estoque.models import AjusteRapidoEstoque
from integracoes.venda_erp_mongo import VendaERPMongoClient

# --- CACHE EM MEMÓRIA ---
CACHE_PRODUTOS = []
ULTIMA_SINCRO = None

def obter_produtos_erp():
    """ Busca todos os produtos do ERP e guarda em memória por 10 min """
    global CACHE_PRODUTOS, ULTIMA_SINCRO
    agora = datetime.now()
    
    if not CACHE_PRODUTOS or not ULTIMA_SINCRO or (agora - ULTIMA_SINCRO) > timedelta(minutes=10):
        try:
            client = VendaERPMongoClient()
            # Puxa apenas os campos necessários para ser rápido
            campos = {"Nome": 1, "Marca": 1, "ValorVenda": 1, "Id": 1, "CodigoProduto": 1, "CodigoNFe": 1, "CodigoBarras": 1}
            CACHE_PRODUTOS = list(client.db[client.col_p].find({}, campos))
            ULTIMA_SINCRO = agora
            print(f"--- CACHE ATUALIZADO: {len(CACHE_PRODUTOS)} produtos ---")
        except Exception as e:
            print(f"Erro ao sincronizar cache: {e}")
    return CACHE_PRODUTOS

def criar_regex_flexivel(termo):
    t = termo.lower()
    t = t.replace('a', '[a,á,à,ã,â]').replace('e', '[e,é,ê]').replace('i', '[i,í]')
    t = t.replace('o', '[o,ó,ô,õ]').replace('u', '[u,ú]').replace('c', '[c,ç]')
    return t

# --- VIEWS ---

def consulta_produtos(request):
    return render(request, "produtos/consulta_produtos.html")

@require_GET
def api_buscar_produtos(request):
    termo = request.GET.get("q", "").strip().lower()
    if not termo: return JsonResponse({"produtos": []})
    
    produtos_todos = obter_produtos_erp()
    palavras = termo.split()
    
    # 1. Filtro rápido na memória (Substitui o Regex lento do Mongo)
    regex_parts = [criar_regex_flexivel(p) for p in palavras]
    
    resultado_ids = []
    matches = []
    
    for p in produtos_todos:
        nome_marca = f"{(p.get('Nome') or '').lower()} {(p.get('Marca') or '').lower()} {p.get('CodigoNFe') or ''}"
        # Verifica se todas as palavras da busca estão no nome/marca
        if all(re.search(reg, nome_marca) for reg in regex_parts):
            matches.append(p)
            resultado_ids.append(str(p.get("Id") or p["_id"]))
            if len(matches) >= 15: break

    # 2. Busca Estoque (Apenas dos 15 filtrados - Isso é instantâneo)
    try:
        client = VendaERPMongoClient()
        estoque_list = list(client.db[client.col_e].find({"ProdutoID": {"$in": resultado_ids}}))
        
        res = []
        for p in matches:
            pid = str(p.get("Id") or p["_id"])
            s_centro = 0.0
            s_vila = 0.0
            
            for est in estoque_list:
                if str(est.get("ProdutoID")) == pid:
                    val = float(est.get("Saldo") or 0)
                    did = str(est.get("DepositoID") or "")
                    if did == client.DEPOSITO_CENTRO: s_centro = val
                    elif did == client.DEPOSITO_VILA_ELIAS: s_vila = val

            # Lógica de Ajuste Local
            ajuste_c = AjusteRapidoEstoque.objects.filter(produto_externo_id=pid, deposito='centro').order_by('-criado_em').first()
            ajuste_v = AjusteRapidoEstoque.objects.filter(produto_externo_id=pid, deposito='vila').order_by('-criado_em').first()

            saldo_f_c = float(ajuste_c.saldo_informado) + (s_centro - float(ajuste_c.saldo_erp_referencia)) if ajuste_c else s_centro
            saldo_f_v = float(ajuste_v.saldo_informado) + (s_vila - float(ajuste_v.saldo_erp_referencia)) if ajuste_v else s_vila
            
            res.append({
                "id": pid, "nome": p.get("Nome"), "marca": p.get("Marca") or "",
                "codigo_nfe": p.get("CodigoNFe") or "", "preco_venda": float(p.get("ValorVenda") or 0),
                "saldo_centro": round(saldo_f_c, 2), "saldo_vila": round(saldo_f_v, 2)
            })
        return JsonResponse({"produtos": res})
    except Exception as e:
        return JsonResponse({"erro": str(e)}, status=500)

@require_GET
def api_autocomplete_produtos(request):
    # Usa o mesmo cache para sugerir nomes instantaneamente
    termo = request.GET.get("q", "").strip().lower()
    if len(termo) < 2: return JsonResponse({"sugestoes": []})
    
    produtos = obter_produtos_erp()
    sugestoes = []
    for p in produtos:
        if termo in (p.get("Nome") or "").lower():
            sugestoes.append({
                "id": str(p.get("Id") or p["_id"]),
                "nome": p.get("Nome"),
                "marca": p.get("Marca") or "",
                "preco": float(p.get("ValorVenda") or 0)
            })
            if len(sugestoes) >= 8: break
    return JsonResponse({"sugestoes": sugestoes})

@require_POST
def api_ajustar_estoque(request):
    pin = request.POST.get("pin")
    if not PerfilUsuario.objects.filter(senha_rapida=pin).exists():
        return JsonResponse({"ok": False, "erro": "PIN INCORRETO"}, status=403)
    try:
        empresa = Empresa.objects.filter(nome_fantasia="Agro Mais").first()
        AjusteRapidoEstoque.objects.create(
            empresa=empresa, produto_externo_id=request.POST.get("produto_id"),
            deposito=request.POST.get("deposito", "centro"), nome_produto=request.POST.get("nome_produto"),
            saldo_erp_referencia=Decimal(request.POST.get("saldo_atual", "0")),
            saldo_informado=Decimal(request.POST.get("novo_saldo", "0"))
        )
        return JsonResponse({"ok": True})
    except Exception as e:
        return JsonResponse({"ok": False, "erro": str(e)}, status=500)

def historico_ajustes(request):
    ajustes = AjusteRapidoEstoque.objects.all().order_by('-criado_em')
    return render(request, "produtos/historico_ajustes.html", {"ajustes": ajustes})

def sugestao_transferencia(request):
    return render(request, "produtos/transferencias.html")

def api_buscar_clientes(request):
    # Clientes são menos frequentes, busca direto no Mongo
    termo = request.GET.get("q", "").strip()
    client = VendaERPMongoClient()
    clientes = list(client.db[client.col_c].find({"Nome": {"$regex": termo, "$options": "i"}}).limit(10))
    res = [{"nome": c.get("Nome"), "documento": c.get("CpfCnpj")} for c in clientes]
    return JsonResponse({"clientes": res})