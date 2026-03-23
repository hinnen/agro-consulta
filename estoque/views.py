import csv
import io
from decimal import Decimal, InvalidOperation

from django.http import JsonResponse
from django.core.cache import cache
from django.shortcuts import render
from django.views.decorators.csrf import csrf_protect
from django.views.decorators.http import require_GET, require_POST

from base.models import Empresa, Loja, PerfilUsuario
from estoque.models import AjusteRapidoEstoque, ConfiguracaoTransferencia
from integracoes.venda_erp_mongo import VendaERPMongoClient


def consulta_produtos(request):
    return render(request, 'produtos/consulta_produtos.html')


def _normalizar_decimal(valor):
    texto = str(valor).strip().replace(' ', '')
    if not texto:
        return Decimal('0')

    if ',' in texto and '.' in texto:
        if texto.rfind(',') > texto.rfind('.'):
            texto = texto.replace('.', '').replace(',', '.')
        else:
            texto = texto.replace(',', '')
    else:
        texto = texto.replace(',', '.')

    return Decimal(texto)


def _buscar_ajustes_mais_recentes(produto_ids=None):
    # Trava de segurança para não explodir o limite do banco SQLite
    if produto_ids is not None and len(produto_ids) <= 900:
        ajustes = (
            AjusteRapidoEstoque.objects
            .filter(produto_externo_id__in=produto_ids)
            .order_by('produto_externo_id', 'deposito', '-criado_em')
        )
    else:
        ajustes = AjusteRapidoEstoque.objects.all().order_by('produto_externo_id', 'deposito', '-criado_em')

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


@require_GET
def api_listar_usuarios(request):
    try:
        perfis = PerfilUsuario.objects.all()
        
        # Trava de sobrevivência: Se o Render apagar o banco, cria um usuário automaticamente
        if not perfis.exists():
            from django.contrib.auth.models import User
            user, _ = User.objects.get_or_create(username='caixa', defaults={'first_name': 'Caixa', 'is_staff': True})
            PerfilUsuario.objects.get_or_create(user=user, codigo_vendedor='0001', defaults={'senha_rapida': '1234'})
            perfis = PerfilUsuario.objects.all()

        lista = []
        for p in perfis:
            # Tenta descobrir o nome amigável do usuário atrelado ao perfil
            nome = "Usuário Desconhecido"
            if hasattr(p, 'user') and p.user:
                nome = f"{p.codigo_vendedor} - {p.user.get_full_name() or p.user.username}"
            elif hasattr(p, 'nome'):
                nome = p.nome
            else:
                nome = str(p)
            lista.append({"id": p.id, "nome": nome})
        
        # Ordena a lista em ordem alfabética para facilitar
        lista.sort(key=lambda x: x['nome'])
        return JsonResponse({'ok': True, 'usuarios': lista})
    except Exception as exc:
        return JsonResponse({'ok': False, 'erro': f'Erro: {exc}'}, status=500)

@require_POST
@csrf_protect
def api_atualizar_pin(request):
    try:
        perfil_id = request.POST.get('perfil_id', '').strip()
        pin_atual = request.POST.get('pin_atual', '').strip()
        novo_pin = request.POST.get('novo_pin', '').strip()
        
        perfil = PerfilUsuario.objects.filter(id=perfil_id, senha_rapida=pin_atual).first()
        if not perfil:
            return JsonResponse({'ok': False, 'erro': 'PIN atual incorreto para o usuário selecionado.'}, status=403)
            
        perfil.senha_rapida = novo_pin
        perfil.save()
        
        return JsonResponse({'ok': True, 'mensagem': 'PIN atualizado com sucesso!'})
    except Exception as exc:
        return JsonResponse({'ok': False, 'erro': f'Erro ao atualizar PIN: {exc}'}, status=500)

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


@require_POST
@csrf_protect
def api_salvar_config_transferencia(request):
    try:
        pin = request.POST.get('pin', '').strip()
        perfil = PerfilUsuario.objects.filter(senha_rapida=pin).first()
        if not perfil:
            return JsonResponse({'ok': False, 'erro': 'PIN INCORRETO'}, status=403)

        produto_id = request.POST.get('produto_id', '').strip()
        if not produto_id:
            return JsonResponse({'ok': False, 'erro': 'Produto inválido.'}, status=400)

        config, _ = ConfiguracaoTransferencia.objects.get_or_create(
            produto_externo_id=produto_id,
            defaults={'nome_produto': request.POST.get('nome_produto', '').strip()}
        )
        
        if 'capacidade_maxima' in request.POST:
            config.capacidade_maxima = _normalizar_decimal(request.POST.get('capacidade_maxima', '0'))
        if 'estoque_seguranca' in request.POST:
            config.estoque_seguranca = _normalizar_decimal(request.POST.get('estoque_seguranca', '0'))
        if 'dias_cobertura' in request.POST:
            config.dias_cobertura = int(request.POST.get('dias_cobertura', '1'))
        if 'venda_media_diaria' in request.POST:
            config.venda_media_diaria = _normalizar_decimal(request.POST.get('venda_media_diaria', '0'))
            
        config.save()

        return JsonResponse({'ok': True, 'mensagem': 'Regras de transferência salvas.'})
    except InvalidOperation:
        return JsonResponse({'ok': False, 'erro': 'Número inválido.'}, status=400)
    except Exception as exc:
        return JsonResponse({'ok': False, 'erro': f'Erro ao salvar regra: {exc}'}, status=500)


@require_POST
@csrf_protect
def api_importar_planilha_transferencia(request):
    try:
        pin = request.POST.get('pin', '').strip()
        perfil = PerfilUsuario.objects.filter(senha_rapida=pin).first()
        if not perfil:
            return JsonResponse({'ok': False, 'erro': 'PIN INCORRETO'}, status=403)

        arquivo = request.FILES.get('arquivo')
        if not arquivo:
            return JsonResponse({'ok': False, 'erro': 'Nenhum arquivo enviado.'}, status=400)

        decoded_file = arquivo.read().decode('utf-8-sig', errors='replace')
        io_string = io.StringIO(decoded_file)
        
        # Tenta ler com ponto e vírgula (padrão Excel Brasil), se não der, tenta vírgula
        reader = csv.DictReader(io_string, delimiter=';')
        if not reader.fieldnames or len(reader.fieldnames) <= 1:
            io_string.seek(0)
            reader = csv.DictReader(io_string, delimiter=',')
            
        if not reader.fieldnames:
            return JsonResponse({'ok': False, 'erro': 'Planilha vazia ou formato inválido.'}, status=400)

        client = VendaERPMongoClient()
        sucesso = 0
        
        # 1. Lê a planilha inteira e separa os códigos
        linhas_validas = []
        codigos_buscados = set()

        for row in reader:
            row_norm = {k.strip().lower(): str(v).strip() for k, v in row.items() if k and v}
            
            codigo = row_norm.get('codigo') or row_norm.get('id') or row_norm.get('produto')
            p_seg = row_norm.get('seguranca') or row_norm.get('min') or row_norm.get('minimo')
            p_max = row_norm.get('maximo') or row_norm.get('max') or '-1'
            
            # Agora exigimos apenas o código e a segurança. Máximo vazio vira 0 (Infinito)
            if not codigo or not p_seg:
                continue
                
            linhas_validas.append({'codigo': codigo, 'p_seg': p_seg, 'p_max': p_max})
            codigos_buscados.add(codigo)

        # 2. Faz UMA ÚNICA viagem ao Banco de Dados buscando todos os códigos juntos!
        mapa_produtos = {}
        if codigos_buscados:
            from bson.objectid import ObjectId
            obj_ids = []
            for c in codigos_buscados:
                if len(c) == 24:
                    try: obj_ids.append(ObjectId(c))
                    except: pass
            
            query = {"$or": [
                {"CodigoNFe": {"$in": list(codigos_buscados)}},
                {"Codigo": {"$in": list(codigos_buscados)}},
                {"CodigoBarras": {"$in": list(codigos_buscados)}},
                {"EAN_NFe": {"$in": list(codigos_buscados)}}
            ]}
            if obj_ids: query["$or"].append({"_id": {"$in": obj_ids}})
            
            produtos_mongo = client.db[client.col_p].find(query, {"_id": 1, "Id": 1, "Nome": 1, "CodigoNFe": 1, "Codigo": 1, "CodigoBarras": 1, "EAN_NFe": 1})
            
            for p in produtos_mongo:
                pid = str(p.get('_id') or p.get('Id'))
                info = {"id": pid, "nome": p.get('Nome', f"Produto {pid}")}
                if p.get("CodigoNFe"): mapa_produtos[str(p.get("CodigoNFe"))] = info
                if p.get("Codigo"): mapa_produtos[str(p.get("Codigo"))] = info
                if p.get("CodigoBarras"): mapa_produtos[str(p.get("CodigoBarras"))] = info
                if p.get("EAN_NFe"): mapa_produtos[str(p.get("EAN_NFe"))] = info
                mapa_produtos[pid] = info

        # 3. Salva no Cockpit na velocidade da luz
        for linha in linhas_validas:
            p = mapa_produtos.get(linha['codigo'])
            if p:
                
                config, _ = ConfiguracaoTransferencia.objects.get_or_create(produto_externo_id=p['id'])
                config.nome_produto = p['nome']
                config.capacidade_maxima = _normalizar_decimal(linha['p_max'])
                config.estoque_seguranca = _normalizar_decimal(linha['p_seg'])
                config.save()
                sucesso += 1

        return JsonResponse({'ok': True, 'mensagem': f'{sucesso} regras importadas/atualizadas com sucesso!'})
    except Exception as exc:
        return JsonResponse({'ok': False, 'erro': f'Erro ao processar: {exc}'}, status=500)


@require_GET
def api_sugestoes_transferencia(request):
    try:
        client = VendaERPMongoClient()

        # 1. Pegar IDs dos produtos que possuem regras configuradas
        regras = ConfiguracaoTransferencia.objects.all()
        mapa_regras = {str(r.produto_externo_id): r for r in regras}
        ids_configurados = list(mapa_regras.keys())

        # 2. Descobrir quais produtos (não configurados) têm saldo na Vila Elias (Prioridade Alta)
        query_vila = {"Saldo": {"$gt": 0}}
        if hasattr(client, 'DEPOSITO_VILA_ELIAS') and client.DEPOSITO_VILA_ELIAS:
            query_vila["DepositoID"] = client.DEPOSITO_VILA_ELIAS
        else:
            query_vila["Deposito"] = {"$regex": "vila", "$options": "i"}

        estoques_vila = list(client.db[client.col_e].find(query_vila, {"ProdutoID": 1, "_id": 0}))
        ids_com_saldo_vila = [str(e.get("ProdutoID")) for e in estoques_vila if e.get("ProdutoID")]

        # 3. Unir tudo (Só vamos baixar do ERP o que realmente importa e ignorar o resto)
        ids_alvo = list(set(ids_configurados + ids_com_saldo_vila))

        if not ids_alvo:
            return JsonResponse({'sugestoes': []})

        # 4. Busca os detalhes APENAS desses produtos no ERP (Carga Ultra Leve)
        produtos = []
        chunk_size = 3000
        for i in range(0, len(ids_alvo), chunk_size):
            chunk = ids_alvo[i:i+chunk_size]
            
            from bson.objectid import ObjectId
            obj_ids = []
            str_ids = []
            for pid in chunk:
                if len(pid) == 24:
                    try: obj_ids.append(ObjectId(pid))
                    except: str_ids.append(pid)
                else:
                    str_ids.append(pid)

            q_prod = {"$or": []}
            if obj_ids: q_prod["$or"].append({"_id": {"$in": obj_ids}})
            if str_ids: q_prod["$or"].append({"Id": {"$in": str_ids}})

            if q_prod["$or"]:
                produtos.extend(list(client.db[client.col_p].find(
                    q_prod, 
                    {"Id": 1, "_id": 1, "Nome": 1, "CodigoNFe": 1, "Codigo": 1, "EAN_NFe": 1, "CodigoBarras": 1}
                )))

        mapa_produtos = {}
        p_ids_encontrados = []
        for p in produtos:
            pid = str(p.get("Id") or p.get("_id"))
            p_ids_encontrados.append(pid)
            mapa_produtos[pid] = {
                "nome": p.get("Nome", f"Produto {pid}"),
                "codigo": p.get("CodigoNFe") or p.get("Codigo") or pid,
                "codigo_barras": p.get("EAN_NFe") or p.get("CodigoBarras") or "",
                "saldo_c": 0.0,
                "saldo_v": 0.0
            }

        # 5. Busca os saldos totais APENAS desses produtos específicos
        estoques = []
        for i in range(0, len(p_ids_encontrados), chunk_size):
            chunk = p_ids_encontrados[i:i+chunk_size]
            estoques.extend(list(client.db[client.col_e].find(
                {"ProdutoID": {"$in": chunk}, "Saldo": {"$ne": 0}},
                {"ProdutoID": 1, "DepositoID": 1, "Deposito": 1, "Saldo": 1, "_id": 0}
            )))

        # 6. Consolida saldos
        for estoque in estoques:
            pid = str(estoque.get("ProdutoID"))
            if pid in mapa_produtos:
                deposito_id = str(estoque.get("DepositoID", ""))
                deposito_nome = str(estoque.get("Deposito", "")).strip().lower()
                saldo = float(str(estoque.get("Saldo", 0) or 0))

                if hasattr(client, 'DEPOSITO_CENTRO') and deposito_id == client.DEPOSITO_CENTRO:
                    mapa_produtos[pid]["saldo_c"] += saldo
                elif hasattr(client, 'DEPOSITO_VILA_ELIAS') and deposito_id == client.DEPOSITO_VILA_ELIAS:
                    mapa_produtos[pid]["saldo_v"] += saldo
                elif "centro" in deposito_nome: 
                    mapa_produtos[pid]["saldo_c"] += saldo
                elif "vila" in deposito_nome: 
                    mapa_produtos[pid]["saldo_v"] += saldo

        # 7. Cruzar com ajustes e montar sugestões
        ajustes = _buscar_ajustes_mais_recentes(p_ids_encontrados)

        sugestoes = []

        for pid, p_info in mapa_produtos.items():
            saldo_centro_erp = Decimal(str(p_info["saldo_c"]))
            saldo_vila_erp = Decimal(str(p_info["saldo_v"]))

            ajuste_centro = ajustes.get((pid, 'centro'))
            ajuste_vila = ajustes.get((pid, 'vila'))

            saldo_centro = saldo_centro_erp + (ajuste_centro.diferenca_saldo if ajuste_centro else Decimal('0'))
            saldo_vila = saldo_vila_erp + (ajuste_vila.diferenca_saldo if ajuste_vila else Decimal('0'))

            regra = mapa_regras.get(pid)

            if regra:
                # PRODUTO JÁ CONFIGURADO
                qtde_transferir = Decimal('0')
                qtde_comprar = Decimal('0')
                status = "OK"

                if saldo_centro <= regra.capacidade_minima:
                    if regra.capacidade_maxima == Decimal('-1'):
                        qtde_transferir = max(Decimal('0'), saldo_vila)
                        falta_para_minimo = regra.capacidade_minima - saldo_centro
                        qtde_comprar = max(Decimal('0'), falta_para_minimo - qtde_transferir)
                        if qtde_transferir > 0 and qtde_comprar > 0: status = "TRANSFERIR_COMPRAR"
                        elif qtde_transferir > 0: status = "TRANSFERIR"
                        elif qtde_comprar > 0: status = "COMPRAR"
                    else:
                        qtde_necessaria = regra.capacidade_maxima - saldo_centro
                        if qtde_necessaria > 0:
                            qtde_transferir = qtde_necessaria if saldo_vila >= qtde_necessaria else max(Decimal('0'), saldo_vila)
                            qtde_comprar = qtde_necessaria - qtde_transferir
                            status = "COMPRAR" if qtde_transferir == 0 else ("TRANSFERIR" if qtde_comprar == 0 else "TRANSFERIR_COMPRAR")

                sugestoes.append({
                    "produto_id": pid, "codigo": p_info["codigo"], "codigo_barras": p_info["codigo_barras"],
                    "nome": p_info["nome"] or regra.nome_produto,
                    "saldo_centro": float(saldo_centro), "saldo_vila": float(saldo_vila),
                    "status": status, "qtde_transferir": float(qtde_transferir), "qtde_comprar": float(qtde_comprar),
                    "capacidade_maxima": float(regra.capacidade_maxima), "estoque_seguranca": float(regra.estoque_seguranca),
                    "capacidade_minima": float(regra.capacidade_minima), "configurado": True, "prioridade": 3
                })
            else:
                # PRODUTO NÃO CONFIGURADO (Prioridade 1 = Alta)
                sugestoes.append({
                    "produto_id": pid, "codigo": p_info["codigo"], "codigo_barras": p_info["codigo_barras"],
                    "nome": p_info["nome"],
                    "saldo_centro": float(saldo_centro), "saldo_vila": float(saldo_vila),
                    "status": "ALTA", "qtde_transferir": 0.0, "qtde_comprar": 0.0,
                    "configurado": False, "prioridade": 1
                })

        # Ordenação mágica: Prioridade 1 (Alta) -> 3 (Configurados), e dentro delas em ordem alfabética.
        sugestoes.sort(key=lambda x: (x["prioridade"], x["nome"]))

        return JsonResponse({'sugestoes': sugestoes})
    except Exception as exc:
        return JsonResponse({'ok': False, 'erro': f'Erro: {exc}'}, status=500)