from django.shortcuts import render

from integracoes.venda_erp_mongo import VendaERPMongoClient
from django.http import JsonResponse
from integracoes.venda_erp_mongo import VendaERPMongoClient


def api_buscar_produtos(request):
    termo = request.GET.get("q", "").strip()

    if not termo:
        return JsonResponse({"produtos": []})

    client = VendaERPMongoClient()

    produtos = client.buscar_produtos(termo)

    produto_ids = [p["_id"] for p in produtos]
    estoques = client.buscar_estoques_por_produto_ids(produto_ids)

    estoques_por_produto = {}

    for estoque in estoques:
        pid = str(estoque.get("ProdutoID"))

        if pid not in estoques_por_produto:
            estoques_por_produto[pid] = []

        estoques_por_produto[pid].append(estoque)

    resultados = []

    for produto in produtos:
        saldo_centro = 0
        saldo_vila = 0

        for estoque in estoques_por_produto.get(str(produto["_id"]), []):
            deposito = str(estoque.get("Deposito", "")).lower()
            saldo = estoque.get("Saldo", 0) or 0

            if "centro" in deposito:
                saldo_centro = saldo
            elif "vila" in deposito:
                saldo_vila = saldo

        resultados.append({
            "id": str(produto["_id"]),
            "nome": produto.get("Nome") or "",
            "marca": produto.get("Marca") or "",
            "preco": produto.get("PrecoVenda") or 0,
            "saldo_centro": saldo_centro,
            "saldo_vila": saldo_vila,
        })

    return JsonResponse({"produtos": resultados})

def consulta_produtos(request):
    termo = request.GET.get('q', '').strip()
    resultados = []
    erro_api = ''

    if termo:
        try:
            client = VendaERPMongoClient()
            produtos = client.buscar_produtos(termo)

            produto_ids = [produto["_id"] for produto in produtos]
            estoques = client.buscar_estoques_por_produto_ids(produto_ids)

            estoques_por_produto = {}
            for estoque in estoques:
                produto_id = str(estoque.get("ProdutoID"))
                if produto_id not in estoques_por_produto:
                    estoques_por_produto[produto_id] = []
                estoques_por_produto[produto_id].append(estoque)

            for produto in produtos:
                saldo_centro = 0
                saldo_vila = 0

                for estoque in estoques_por_produto.get(str(produto["_id"]), []):
                    deposito = str(estoque.get("Deposito", "")).strip().lower()
                    saldo = float(estoque.get("Saldo", 0) or 0)

                    if "centro" in deposito:
                        saldo_centro += saldo
                    elif "vila" in deposito:
                        saldo_vila += saldo

                resultados.append({
                    "id": str(produto.get("_id")),
                    "codigo_interno": produto.get("CodigoNFe") or str(produto.get("Codigo") or ""),
                    "codigo_barras": produto.get("EAN_NFe") or "",
                    "nome": produto.get("Nome") or "",
                    "marca": produto.get("Marca") or "",
                    "categoria": produto.get("Categoria") or "",
                    "preco_venda": float(produto.get("PrecoVenda") or 0),
                    "saldo_centro": saldo_centro,
                    "saldo_vila": saldo_vila,
                    "saldo_total": saldo_centro + saldo_vila,
                })

        except Exception as exc:
            erro_api = f'Erro ao consultar MongoDB do Venda ERP: {exc}'

    produto_destaque = resultados[0] if resultados else None

    contexto = {
        'termo': termo,
        'resultados': resultados,
        'produto_destaque': produto_destaque,
        'erro_api': erro_api,
    }

    return render(request, 'produtos/consulta_produtos.html', contexto)