from decimal import Decimal, InvalidOperation

from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_protect
from django.views.decorators.http import require_GET, require_POST

from base.models import Empresa, Loja
from estoque.models import AjusteRapidoEstoque
from integracoes.venda_erp_mongo import VendaERPMongoClient


def consulta_produtos(request):
    return render(request, 'produtos/consulta_produtos.html')


def _normalizar_decimal(valor):
    texto = str(valor).strip().replace(' ', '')

    if ',' in texto and '.' in texto:
        if texto.rfind(',') > texto.rfind('.'):
            texto = texto.replace('.', '').replace(',', '.')
        else:
            texto = texto.replace(',', '')
    else:
        texto = texto.replace(',', '.')

    return Decimal(texto)


def _buscar_ajustes_mais_recentes(produto_ids):
    ajustes = (
        AjusteRapidoEstoque.objects
        .filter(produto_externo_id__in=produto_ids)
        .order_by('produto_externo_id', 'deposito', '-criado_em')
    )

    mapa = {}
    for ajuste in ajustes:
        chave = (ajuste.produto_externo_id, ajuste.deposito)
        if chave not in mapa:
            mapa[chave] = ajuste

    return mapa


@require_GET
def api_buscar_produtos(request):
    termo = request.GET.get('q', '').strip()
    produtos_json = []
    erro_api = ''

    if termo:
        try:
            client = VendaERPMongoClient()
            produtos = client.buscar_produtos(termo)

            produto_ids = [str(produto["_id"]) for produto in produtos]
            estoques = client.buscar_estoques_por_produto_ids(produto_ids)
            ajustes = _buscar_ajustes_mais_recentes(produto_ids)

            estoques_por_produto = {}
            for estoque in estoques:
                produto_id = str(estoque.get("ProdutoID"))
                estoques_por_produto.setdefault(produto_id, []).append(estoque)

            for produto in produtos:
                produto_id = str(produto.get("_id"))
                saldo_centro_erp = Decimal('0')
                saldo_vila_erp = Decimal('0')

                for estoque in estoques_por_produto.get(produto_id, []):
                    deposito = str(estoque.get("Deposito", "")).strip().lower()
                    saldo = Decimal(str(estoque.get("Saldo", 0) or 0))

                    if "centro" in deposito:
                        saldo_centro_erp += saldo
                    elif "vila" in deposito:
                        saldo_vila_erp += saldo

                ajuste_centro = ajustes.get((produto_id, 'centro'))
                ajuste_vila = ajustes.get((produto_id, 'vila'))

                saldo_centro = saldo_centro_erp
                saldo_vila = saldo_vila_erp

                if ajuste_centro:
                    saldo_centro = saldo_centro_erp + ajuste_centro.diferenca_saldo

                if ajuste_vila:
                    saldo_vila = saldo_vila_erp + ajuste_vila.diferenca_saldo

                produtos_json.append({
                    "id": produto_id,
                    "codigo_interno": produto.get("CodigoNFe") or str(produto.get("Codigo") or ""),
                    "codigo_barras": produto.get("EAN_NFe") or "",
                    "nome": produto.get("Nome") or "",
                    "marca": produto.get("Marca") or "",
                    "categoria": produto.get("Categoria") or "",
                    "preco_venda": float(produto.get("PrecoVenda") or 0),
                    "saldo_centro": float(saldo_centro),
                    "saldo_vila": float(saldo_vila),
                    "saldo_total": float(saldo_centro + saldo_vila),
                    "saldo_centro_erp": float(saldo_centro_erp),
                    "saldo_vila_erp": float(saldo_vila_erp),
                })

        except Exception as exc:
            erro_api = f'Erro ao consultar MongoDB do Venda ERP: {exc}'

    return JsonResponse({
        'produtos': produtos_json,
        'erro_api': erro_api,
    })


@require_POST
@csrf_protect
def api_ajustar_estoque(request):
    try:
        produto_id = request.POST.get('produto_id', '').strip()
        codigo_interno = request.POST.get('codigo_interno', '').strip()
        nome_produto = request.POST.get('nome_produto', '').strip()
        deposito = request.POST.get('deposito', '').strip().lower()
        saldo_atual = request.POST.get('saldo_atual', '0').strip()
        novo_saldo = request.POST.get('novo_saldo', '').strip()
        observacao = request.POST.get('observacao', '').strip()

        if not produto_id:
            return JsonResponse({'ok': False, 'erro': 'Produto inválido.'}, status=400)

        if deposito not in ['centro', 'vila']:
            return JsonResponse({'ok': False, 'erro': 'Depósito inválido.'}, status=400)

        if novo_saldo == '':
            return JsonResponse({'ok': False, 'erro': 'Informe o novo saldo.'}, status=400)

        saldo_erp_referencia = _normalizar_decimal(saldo_atual)
        saldo_informado = _normalizar_decimal(novo_saldo)
        diferenca_saldo = saldo_informado - saldo_erp_referencia

        empresa = Empresa.objects.filter(nome_fantasia="Agro Mais").first()

        loja = None
        if empresa:
            if deposito == 'centro':
                loja = Loja.objects.filter(empresa=empresa, nome__icontains='centro').first()
            elif deposito == 'vila':
                loja = Loja.objects.filter(empresa=empresa, nome__icontains='vila').first()

        ajuste = AjusteRapidoEstoque.objects.create(
            empresa=empresa,
            loja=loja,
            produto_externo_id=produto_id,
            codigo_interno=codigo_interno,
            nome_produto=nome_produto,
            deposito=deposito,
            saldo_erp_referencia=saldo_erp_referencia,
            saldo_informado=saldo_informado,
            diferenca_saldo=diferenca_saldo,
            observacao=observacao,
        )

        return JsonResponse({
            'ok': True,
            'mensagem': 'Ajuste salvo com sucesso.',
            'ajuste_id': ajuste.id,
            'saldo_erp_referencia': float(ajuste.saldo_erp_referencia),
            'saldo_informado': float(ajuste.saldo_informado),
            'diferenca_saldo': float(ajuste.diferenca_saldo),
            'empresa': empresa.nome_fantasia if empresa else '',
            'loja': loja.nome if loja else '',
        })

    except InvalidOperation:
        return JsonResponse({'ok': False, 'erro': 'Número inválido.'}, status=400)
    except Exception as exc:
        return JsonResponse({'ok': False, 'erro': f'Erro ao salvar ajuste: {exc}'}, status=500)