import json
from datetime import datetime
from decimal import Decimal
from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.http import require_GET, require_POST
from base.models import Empresa, Loja, PerfilUsuario
from estoque.models import AjusteRapidoEstoque
from integracoes.venda_erp_mongo import VendaERPMongoClient

def consulta_produtos(request):
    return render(request, "produtos/consulta_produtos.html")

def historico_ajustes(request):
    ajustes = AjusteRapidoEstoque.objects.all().order_by('-criado_em')
    return render(request, "produtos/historico_ajustes.html", {"ajustes": ajustes})

def sugestao_transferencia(request):
    return render(request, "produtos/transferencias.html")

@require_GET
def api_buscar_produtos(request):
    termo = request.GET.get("q", "").strip()
    if not termo: return JsonResponse({"produtos": []})
    
    try:
        client = VendaERPMongoClient()
        db = client.db
        
        # 1. Busca produtos
        query = {"$or": [
            {"Nome": {"$regex": termo, "$options": "i"}},
            {"CodigoProduto": {"$regex": termo, "$options": "i"}},
            {"CodigoBarras": termo}
        ]}
        produtos_mongo = list(db[client.col_p].find(query).limit(15))
        
        # Mapeia IDs (O Venda ERP usa o campo 'Id' como string para o estoque)
        p_ids = [str(p.get("Id") or p["_id"]) for p in produtos_mongo]

        # 2. Busca estoque com os campos exatos: ProdutoID e Saldo
        estoque_list = list(db[client.col_e].find({"ProdutoID": {"$in": p_ids}}))

        res = []
        for p in produtos_mongo:
            pid = str(p.get("Id") or p["_id"])
            s_centro = 0.0
            s_vila = 0.0
            
            for est in estoque_list:
                if str(est.get("ProdutoID")) == pid:
                    val = float(est.get("Saldo") or 0)
                    did = str(est.get("DepositoID") or "")
                    
                    if did == client.DEPOSITO_CENTRO:
                        s_centro = val
                    elif did == client.DEPOSITO_VILA_ELIAS:
                        s_vila = val
                    elif s_centro == 0:
                        s_centro = val # Se o ID não bater, joga no Centro
            
            res.append({
                "id": str(p["_id"]), 
                "nome": p.get("Nome") or "Sem Nome", 
                "codigo_interno": p.get("CodigoProduto") or p.get("Codigo") or "",
                "preco_venda": float(p.get("ValorVenda") or p.get("PrecoVenda") or 0),
                "saldo_centro": round(s_centro, 2),
                "saldo_vila": round(s_vila, 2)
            })
            
        return JsonResponse({"produtos": res})
    except Exception as e:
        print(f"ERRO BUSCA: {str(e)}")
        return JsonResponse({"erro": str(e)}, status=500)

@require_GET
def api_buscar_clientes(request):
    termo = request.GET.get("q", "").strip()
    if not termo: return JsonResponse({"clientes": []})
    try:
        client = VendaERPMongoClient()
        clientes = client.buscar_clientes(termo)
        res = [{"id": str(c["_id"]), "nome": c.get("Nome"), "documento": c.get("CpfCnpj")} for c in clientes]
        return JsonResponse({"clientes": res})
    except Exception as e:
        return JsonResponse({"erro": str(e)}, status=500)

@require_POST
def api_ajustar_estoque(request):
    pin = request.POST.get("pin")
    vendedor = PerfilUsuario.objects.filter(senha_rapida=pin).first()
    if not vendedor: return JsonResponse({"ok": False, "erro": "PIN"}, status=403)
    try:
        empresa = Empresa.objects.filter(nome_fantasia="Agro Mais").first()
        loja = Loja.objects.filter(nome__icontains="centro").first()
        AjusteRapidoEstoque.objects.create(
            empresa=empresa, loja=loja, 
            produto_externo_id=request.POST.get("produto_id"),
            deposito="centro",
            nome_produto=request.POST.get("nome_produto"),
            codigo_interno=request.POST.get("codigo_interno"),
            saldo_erp_referencia=Decimal(request.POST.get("saldo_atual", "0")),
            saldo_informado=Decimal(request.POST.get("novo_saldo", "0"))
        )
        return JsonResponse({"ok": True})
    except Exception as e:
        return JsonResponse({"ok": False, "erro": str(e)}, status=500)