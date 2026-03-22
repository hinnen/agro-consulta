import json
from datetime import datetime
from decimal import Decimal
from django.shortcuts import render, get_object_or_404
from django.http import JsonResponse
from django.views.decorators.http import require_GET, require_POST
from base.models import Empresa, Loja
from estoque.models import AjusteRapidoEstoque, ConfiguracaoProduto
from integracoes.venda_erp_mongo import VendaERPMongoClient
from integracoes.venda_erp_api import VendaERPAPIClient

def consulta_produtos(request):
    return render(request, "produtos/consulta_produtos.html")

def historico_ajustes(request):
    ajustes = AjusteRapidoEstoque.objects.all().order_by('-criado_em')
    return render(request, "produtos/historico_ajustes.html", {"ajustes": ajustes})

def sugestao_transferencia(request):
    try:
        client = VendaERPMongoClient()
        produtos = client.buscar_produtos("", limite=500) 
        p_ids = [str(p["_id"]) for p in produtos]
        estoques = client.buscar_estoques_por_produto_ids(p_ids)
        
        # Pega ajustes de hoje (inclui o retroativo de 12:00)
        ajustes_hoje = AjusteRapidoEstoque.objects.filter(
            criado_em__date=datetime.now().date()
        )
        
        dif_map = {}
        for a in ajustes_hoje:
            pid = a.produto_externo_id
            if pid not in dif_map: 
                dif_map[pid] = {"centro": Decimal(0), "vila": Decimal(0)}
            dif_map[pid][a.deposito] += a.diferenca_saldo

        # Carrega limites personalizados do banco de dados
        configs = {c.codigo_interno: c for c in ConfiguracaoProduto.objects.all()}

        relatorio = []
        for p in produtos:
            pid, cod = str(p["_id"]), p.get("CodigoNFe")
            centro, vila = Decimal(0), Decimal(0)
            
            for est in estoques:
                if str(est.get("ProdutoID")) == pid:
                    did, val = str(est.get("DepositoID")), Decimal(str(est.get("Saldo", 0)))
                    if did == client.DEPOSITO_CENTRO: centro = val
                    elif did == client.DEPOSITO_VILA_ELIAS: vila = val
            
            # Aplica correções manuais sobre o saldo vivo do ERP
            difs = dif_map.get(pid, {"centro": Decimal(0), "vila": Decimal(0)})
            centro += difs["centro"]
            vila += difs["vila"]

            # Parâmetros Logísticos (Segurança e Máximo)
            c = configs.get(cod)
            min_seguranca = c.estoque_seguranca if c else Decimal("1.0")
            max_centro = c.estoque_maximo_centro if c else Decimal("15.0")
            
            falta_no_centro = max_centro - centro
            status, cor, sugestao = "OK", "text-slate-400", 0

            # Lógica da aba SISTEMA
            if centro < min_seguranca:
                if vila >= falta_no_centro:
                    status, cor, sugestao = "TRANSFERIR", "text-blue-600", falta_no_centro
                elif vila > 0:
                    status, cor, sugestao = "TRANSFERIR + COMPRAR", "text-orange-600", vila
                else:
                    status, cor, sugestao = "COMPRAR", "text-red-600", 0
            
            if status != "OK" or centro > 0 or vila > 0:
                relatorio.append({
                    "nome": p.get("Nome"), "codigo": cod,
                    "centro": float(centro), "vila": float(vila),
                    "status": status, "cor": cor, "sugestao": float(sugestao),
                    "min": float(min_seguranca), "max": float(max_centro)
                })
        
        return render(request, "produtos/transferencias.html", {"relatorio": relatorio})
    except Exception as e:
        return render(request, "produtos/transferencias.html", {"erro": str(e)})

@require_GET
def api_sugestoes_produtos(request):
    termo = request.GET.get("q", "").strip()
    if len(termo) < 2: return JsonResponse({"sugestoes": []})
    try:
        client = VendaERPMongoClient()
        produtos = client.buscar_produtos(termo, limite=8)
        sugestoes = [{"id": str(p["_id"]), "nome": p.get("Nome", ""), "codigo_interno": p.get("CodigoNFe") or str(p.get("Codigo", ""))} for p in produtos]
        return JsonResponse({"sugestoes": sugestoes})
    except Exception as e: return JsonResponse({"erro": str(e)}, status=500)

@require_GET
def api_buscar_produtos(request):
    termo = request.GET.get("q", "").strip()
    try:
        client = VendaERPMongoClient()
        produtos = client.buscar_produtos(termo)
        p_ids = [str(p["_id"]) for p in produtos]
        estoques = client.buscar_estoques_por_produto_ids(p_ids)
        
        # Importante: filtro por __date para aceitar ajustes manuais do shell
        ajustes_hoje = AjusteRapidoEstoque.objects.filter(
            produto_externo_id__in=p_ids, 
            criado_em__date=datetime.now().date()
        )
        
        dif_map = {}
        for a in ajustes_hoje:
            pid = a.produto_externo_id
            if pid not in dif_map: dif_map[pid] = {"centro": Decimal(0), "vila": Decimal(0)}
            dif_map[pid][a.deposito] += a.diferenca_saldo

        res = []
        for p in produtos:
            pid = str(p["_id"])
            saldos_erp = {"centro": Decimal(0), "vila": Decimal(0)}
            for est in estoques:
                if str(est.get("ProdutoID")) == pid:
                    did, val = str(est.get("DepositoID")), Decimal(str(est.get("Saldo", 0)))
                    if did == client.DEPOSITO_CENTRO: saldos_erp["centro"] = val
                    elif did == client.DEPOSITO_VILA_ELIAS: saldos_erp["vila"] = val
            
            difs = dif_map.get(pid, {"centro": Decimal(0), "vila": Decimal(0)})
            res.append({
                "id": pid, "nome": p.get("Nome", ""), "marca": p.get("Marca", ""),
                "codigo_interno": p.get("CodigoNFe") or str(p.get("Codigo", "")),
                "preco_venda": float(p.get("PrecoVenda") or 0),
                "saldo_centro": float(saldos_erp["centro"] + difs["centro"]),
                "saldo_vila": float(saldos_erp["vila"] + difs["vila"])
            })
        return JsonResponse({"produtos": res})
    except Exception as e: return JsonResponse({"erro": str(e)}, status=500)

@require_POST
def api_ajustar_estoque(request):
    if request.POST.get("pin") != "1234": return JsonResponse({"ok": False, "erro": "PIN INVÁLIDO"}, status=403)
    try:
        deposito_slug = request.POST.get("deposito", "centro")
        empresa = Empresa.objects.filter(nome_fantasia="Agro Mais").first()
        loja = Loja.objects.filter(nome__icontains=deposito_slug).first()
        AjusteRapidoEstoque.objects.create(
            empresa=empresa, loja=loja, produto_externo_id=request.POST.get("produto_id"),
            deposito=deposito_slug, nome_produto=request.POST.get("nome_produto", "Produto"),
            codigo_interno=request.POST.get("codigo_interno", ""),
            saldo_erp_referencia=Decimal(request.POST.get("saldo_atual", "0")),
            saldo_informado=Decimal(request.POST.get("novo_saldo", "0"))
        )
        return JsonResponse({"ok": True})
    except Exception as e: return JsonResponse({"ok": False, "erro": str(e)}, status=500)

@require_POST
def api_salvar_config_logistica(request):
    if request.POST.get("pin") != "1234": return JsonResponse({"ok": False}, status=403)
    cod = request.POST.get("codigo")
    ConfiguracaoProduto.objects.update_or_create(
        codigo_interno=cod,
        defaults={
            'estoque_seguranca': Decimal(request.POST.get("min")),
            'estoque_maximo_centro': Decimal(request.POST.get("max"))
        }
    )
    return JsonResponse({"ok": True})

@require_POST
def api_deletar_ajuste(request, ajuste_id):
    if request.POST.get("pin") != "1234": return JsonResponse({"ok": False, "erro": "PIN INVÁLIDO"}, status=403)
    get_object_or_404(AjusteRapidoEstoque, id=ajuste_id).delete()
    return JsonResponse({"ok": True})

@require_POST
def api_limpar_historico(request):
    if request.POST.get("pin") != "1234": return JsonResponse({"ok": False, "erro": "PIN INVÁLIDO"}, status=403)
    AjusteRapidoEstoque.objects.all().delete()
    return JsonResponse({"ok": True})

@require_POST
def api_gerar_orcamento(request):
    try:
        itens = json.loads(request.POST.get("itens", "[]"))
        client = VendaERPAPIClient()
        payload = {"status": "Aberto", "data": datetime.now().isoformat(), "descricao": f"Orcamento: {request.POST.get('cliente_id', 'Balcao')}", "itens": [{"produtoID": i['id'], "quantidade": float(i['quantidade']), "valorUnitario": float(i['preco'])} for i in itens]}
        ok, code, res = client.salvar_operacao_pdv(payload)
        return JsonResponse({"ok": ok, "mensagem": "Orçamento Gerado!" if ok else f"Erro {code}"})
    except Exception as e: return JsonResponse({"ok": False, "mensagem": str(e)})