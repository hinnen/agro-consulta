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

# Instância Global com proteção para falha de conexão
try:
    CLIENT_MONGO = VendaERPMongoClient()
    DB_MONGO = CLIENT_MONGO.db
except Exception as e:
    print(f"ERRO CRÍTICO MONGO: {e}")
    CLIENT_MONGO = None
    DB_MONGO = None

def normalizar_termo(txt):
    """ Normaliza o termo de busca para bater com o BuscaTexto do banco """
    if not txt: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', txt)
                  if unicodedata.category(c) != 'Mn').lower()

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
    if not termo_original or not DB_MONGO: 
        return JsonResponse({"produtos": []})
    
    termo_norm = normalizar_termo(termo_original)
    palavras = termo_norm.split()
    
    try:
        # Monta regex para o campo BuscaTexto (usa seus índices)
        regex_parts = [f"(?=.*{re.escape(p)})" for p in palavras]
        regex_final = "".join(regex_parts) + ".*"

        query = {"$or": [
            {"BuscaTexto": {"$regex": regex_final, "$options": "i"}},
            {"CodigoNFe": termo_limpo},
            {"CodigoBarras": termo_limpo},
            {"EAN_NFe": termo_limpo}, # Campo extra para código de barras
            {"CodigoProduto": termo_limpo}
        ]}

        # Filtro de ativos (essencial para performance com o índice que você criou)
        query["CadastroInativo"] = {"$ne": True}

        col_p = DB_MONGO[CLIENT_MONGO.col_p]
        produtos_mongo = list(col_p.find(query).limit(15))
        
        p_ids = [str(p.get("Id") or p["_id"]) for p in produtos_mongo]
        
        col_e = DB_MONGO[CLIENT_MONGO.col_e]
        estoque_list = list(col_e.find({"ProdutoID": {"$in": p_ids}}))

        res = []
        for p in produtos_mongo:
            pid = str(p.get("Id") or p["_id"])
            s_centro = 0.0
            s_vila = 0.0
            
            for est in estoque_list:
                if str(est.get("ProdutoID")) == pid:
                    val = float(est.get("Saldo") or 0)
                    did = str(est.get("DepositoID") or "")
                    if did == CLIENT_MONGO.DEPOSITO_CENTRO: s_centro = val
                    elif did == CLIENT_MONGO.DEPOSITO_VILA_ELIAS: s_vila = val

            # Lógica de Saldo Local + Variação ERP
            ajuste_c = AjusteRapidoEstoque.objects.filter(produto_externo_id=pid, deposito='centro').order_by('-criado_em').first()
            ajuste_v = AjusteRapidoEstoque.objects.filter(produto_externo_id=pid, deposito='vila').order_by('-criado_em').first()

            saldo_f_c = float(ajuste_c.saldo_informado) + (s_centro - float(ajuste_c.saldo_erp_referencia)) if ajuste_c else s_centro
            saldo_f_v = float(ajuste_v.saldo_informado) + (s_vila - float(ajuste_v.saldo_erp_referencia)) if ajuste_v else s_vila
            
            res.append({
                "id": pid, 
                "nome": p.get("Nome") or "Sem Nome", 
                "marca": p.get("Marca") or "",
                "codigo_nfe": p.get("CodigoNFe") or "",
                "preco_venda": float(p.get("ValorVenda") or p.get("PrecoVenda") or 0),
                "saldo_centro": round(saldo_f_c, 2),
                "saldo_vila": round(saldo_f_v, 2)
            })
            
        return JsonResponse({"produtos": res})
    except Exception as e:
        return JsonResponse({"erro": str(e)}, status=500)

@require_GET
def api_autocomplete_produtos(request):
    termo = request.GET.get("q", "").strip()
    if len(termo) < 2 or not DB_MONGO: 
        return JsonResponse({"sugestoes": []})
    
    termo_norm = normalizar_termo(termo)
    try:
        # Busca por prefixo no BuscaTexto (usa índice de forma otimizada)
        query = {
            "BuscaTexto": {"$regex": f"^{termo_norm}", "$options": "i"}, 
            "CadastroInativo": {"$ne": True}
        }
        sugestoes = list(DB_MONGO[CLIENT_MONGO.col_p].find(query, {"Nome": 1, "Marca": 1, "ValorVenda": 1, "PrecoVenda": 1, "Id": 1}).limit(8))
        
        res = [{
            "id": str(s.get("Id") or s["_id"]), 
            "nome": s.get("Nome"),
            "marca": s.get("Marca") or "",
            "preco_venda": float(s.get("PrecoVenda") or s.get("ValorVenda") or 0)
        } for s in sugestoes]
        return JsonResponse({"sugestoes": res})
    except:
        return JsonResponse({"sugestoes": []})

@require_POST
def api_ajustar_estoque(request):
    pin = request.POST.get("pin")
    perfil = PerfilUsuario.objects.filter(senha_rapida=pin).first()
    if not perfil: 
        return JsonResponse({"ok": False, "erro": "PIN INCORRETO"}, status=403)
    
    try:
        empresa = Empresa.objects.filter(nome_fantasia="Agro Mais").first()
        AjusteRapidoEstoque.objects.create(
            empresa=empresa,
            produto_externo_id=request.POST.get("produto_id"),
            deposito=request.POST.get("deposito", "centro"),
            nome_produto=request.POST.get("nome_produto"),
            saldo_erp_referencia=Decimal(request.POST.get("saldo_atual", "0")),
            saldo_informado=Decimal(request.POST.get("novo_saldo", "0"))
        )
        return JsonResponse({"ok": True})
    except Exception as e:
        return JsonResponse({"ok": False, "erro": str(e)}, status=500)

@require_GET
def api_buscar_clientes(request):
    termo = request.GET.get("q", "").strip()
    if not termo or not DB_MONGO: 
        return JsonResponse({"clientes": []})
    try:
        query = {"Nome": {"$regex": termo, "$options": "i"}}
        clientes = list(DB_MONGO[CLIENT_MONGO.col_c].find(query).limit(10))
        res = [{"nome": c.get("Nome"), "documento": c.get("CpfCnpj")} for c in clientes]
        return JsonResponse({"clientes": res})
    except Exception as e:
        return JsonResponse({"erro": str(e)}, status=500)