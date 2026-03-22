import json
import unicodedata
import re
from datetime import datetime
from decimal import Decimal
from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.http import require_GET, require_POST
from base.models import Empresa, Loja, PerfilUsuario
from estoque.models import AjusteRapidoEstoque
from integracoes.venda_erp_mongo import VendaERPMongoClient

# Instância Global para evitar lentidão de reconexão no deploy
try:
    CLIENT_MONGO = VendaERPMongoClient()
    DB_MONGO = CLIENT_MONGO.db
except:
    CLIENT_MONGO = None
    DB_MONGO = None

# --- UTILITÁRIOS ---

def criar_regex_flexivel(termo):
    """ Converte 'rac' em uma regex que aceita 'rac', 'raç', 'ração', etc. """
    t = termo.lower()
    t = t.replace('a', '[a,á,à,ã,â]')
    t = t.replace('e', '[e,é,ê]')
    t = t.replace('i', '[i,í]')
    t = t.replace('o', '[o,ó,ô,õ]')
    t = t.replace('u', '[u,ú]')
    t = t.replace('c', '[c,ç]')
    return t

# --- VIEWS DE PÁGINA ---

def consulta_produtos(request):
    return render(request, "produtos/consulta_produtos.html")

def historico_ajustes(request):
    ajustes = AjusteRapidoEstoque.objects.all().order_by('-criado_em')
    return render(request, "produtos/historico_ajustes.html", {"ajustes": ajustes})

def sugestao_transferencia(request):
    return render(request, "produtos/transferencias.html")

# --- API DE PRODUTOS E ESTOQUE ---

@require_GET
def api_buscar_produtos(request):
    termo_original = request.GET.get("q", "").strip()
    if not termo_original or not DB_MONGO: return JsonResponse({"produtos": []})
    
    palavras = termo_original.split()
    
    try:
        # Monta regex split search
        regex_parts = [f"(?=.*{criar_regex_flexivel(p)})" for p in palavras]
        regex_final = "".join(regex_parts) + ".*"

        query = {"$or": [
            {"Nome": {"$regex": regex_final, "$options": "i"}},
            {"Marca": {"$regex": regex_final, "$options": "i"}},
            {"CodigoProduto": {"$regex": termo_original, "$options": "i"}},
            {"CodigoNFe": {"$regex": termo_original, "$options": "i"}},
            {"CodigoBarras": termo_original}
        ]}

        # Busca otimizada usando conexão global
        produtos_mongo = list(DB_MONGO[CLIENT_MONGO.col_p].find(query).limit(15))
        p_ids = [str(p.get("Id") or p["_id"]) for p in produtos_mongo]
        estoque_list = list(DB_MONGO[CLIENT_MONGO.col_e].find({"ProdutoID": {"$in": p_ids}}))

        res = []
        for p in produtos_mongo:
            pid = str(p.get("Id") or p["_id"])
            s_centro_erp = 0.0
            s_vila_erp = 0.0
            
            for est in estoque_list:
                if str(est.get("ProdutoID")) == pid:
                    val = float(est.get("Saldo") or 0)
                    did = str(est.get("DepositoID") or "")
                    if did == CLIENT_MONGO.DEPOSITO_CENTRO: s_centro_erp = val
                    elif did == CLIENT_MONGO.DEPOSITO_VILA_ELIAS: s_vila_erp = val

            # Inteligência de Saldo (Ajuste + Variação ERP)
            ajuste_centro = AjusteRapidoEstoque.objects.filter(produto_externo_id=pid, deposito='centro').order_by('-criado_em').first()
            ajuste_vila = AjusteRapidoEstoque.objects.filter(produto_externo_id=pid, deposito='vila').order_by('-criado_em').first()

            saldo_f_centro = float(ajuste_centro.saldo_informado) + (s_centro_erp - float(ajuste_centro.saldo_erp_referencia)) if ajuste_centro else s_centro_erp
            saldo_f_vila = float(ajuste_vila.saldo_informado) + (s_vila_erp - float(ajuste_vila.saldo_erp_referencia)) if ajuste_vila else s_vila_erp
            
            res.append({
                "id": pid, 
                "nome": p.get("Nome") or "Sem Nome", 
                "marca": p.get("Marca") or "",
                "codigo_interno": p.get("CodigoProduto") or p.get("Codigo") or "",
                "codigo_nfe": p.get("CodigoNFe") or p.get("CodigoNfe") or "",
                "preco_venda": float(p.get("ValorVenda") or p.get("PrecoVenda") or 0),
                "saldo_centro": round(saldo_f_centro, 2),
                "saldo_vila": round(saldo_f_vila, 2)
            })
            
        return JsonResponse({"produtos": res})
    except Exception as e:
        return JsonResponse({"erro": str(e)}, status=500)

@require_POST
def api_ajustar_estoque(request):
    pin = request.POST.get("pin")
    perfil = PerfilUsuario.objects.filter(senha_rapida=pin).first()
    if not perfil: return JsonResponse({"ok": False, "erro": "PIN INCORRETO"}, status=403)
    
    try:
        empresa = Empresa.objects.filter(nome_fantasia="Agro Mais").first()
        AjusteRapidoEstoque.objects.create(
            empresa=empresa,
            produto_externo_id=request.POST.get("produto_id"),
            deposito=request.POST.get("deposito", "centro"),
            nome_produto=request.POST.get("nome_produto"),
            codigo_interno=request.POST.get("codigo_interno"),
            saldo_erp_referencia=Decimal(request.POST.get("saldo_atual", "0")),
            saldo_informado=Decimal(request.POST.get("novo_saldo", "0"))
        )
        return JsonResponse({"ok": True})
    except Exception as e:
        return JsonResponse({"ok": False, "erro": str(e)}, status=500)

@require_GET
def api_autocomplete_produtos(request):
    termo = request.GET.get("q", "").strip()
    if len(termo) < 2 or not DB_MONGO: return JsonResponse({"sugestoes": []})
    
    try:
        p_flex = criar_regex_flexivel(termo)
        query = {"Nome": {"$regex": f"^{p_flex}", "$options": "i"}}
        sugestoes = list(DB_MONGO[CLIENT_MONGO.col_p].find(query, {"Nome": 1, "Marca": 1, "ValorVenda": 1, "Id": 1}).limit(8))
        
        res = [{
            "id": str(s.get("Id") or s["_id"]), 
            "nome": s.get("Nome"),
            "marca": s.get("Marca") or "",
            "preco": float(s.get("ValorVenda") or 0)
        } for s in sugestoes]
        return JsonResponse({"sugestoes": res})
    except:
        return JsonResponse({"sugestoes": []})

@require_GET
def api_buscar_clientes(request):
    termo = request.GET.get("q", "").strip()
    if not termo or not DB_MONGO: return JsonResponse({"clientes": []})
    try:
        query = {"$or": [{"Nome": {"$regex": termo, "$options": "i"}}, {"CpfCnpj": {"$regex": termo, "$options": "i"}}]}
        clientes = list(DB_MONGO[CLIENT_MONGO.col_c].find(query).limit(10))
        res = [{"id": str(c["_id"]), "nome": c.get("Nome"), "documento": c.get("CpfCnpj")} for c in clientes]
        return JsonResponse({"clientes": res})
    except Exception as e:
        return JsonResponse({"erro": str(e)}, status=500)