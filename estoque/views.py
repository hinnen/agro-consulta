import csv
import io
import uuid
from decimal import Decimal, InvalidOperation

from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.http import JsonResponse
from django.core.cache import cache
from django.shortcuts import render
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import csrf_protect
from django.views.decorators.http import require_GET, require_POST
from django.utils import timezone
from django.utils.timezone import localtime

from base.models import Empresa, Loja, PerfilUsuario
from estoque.models import (
    AjusteRapidoEstoque,
    ConfiguracaoTransferencia,
    HistoricoTransferencia,
    OrigemAjusteEstoque,
    PedidoTransferencia,
)
from integracoes.venda_erp_mongo import VendaERPMongoClient
from atualizar_medias import calcular
import json

_cached_mongo_client = None
def get_mongo_client():
    global _cached_mongo_client
    if _cached_mongo_client is None:
        _cached_mongo_client = VendaERPMongoClient()
    return _cached_mongo_client


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


def _pedido_transferencia_extra(pedido):
    if not pedido:
        return {
            "pedido_quantidade": 0.0,
            "pedido_lote_uuid": None,
            "pedido_status": None,
            "pedido_impresso_em": None,
        }
    return {
        "pedido_quantidade": float(pedido.quantidade),
        "pedido_lote_uuid": str(pedido.lote_uuid) if pedido.lote_uuid else None,
        "pedido_status": (pedido.status or "IMPRESSO").strip(),
        "pedido_impresso_em": localtime(pedido.impresso_em).strftime("%d/%m/%Y %H:%M")
        if pedido.impresso_em
        else None,
    }


def _rotulo_usuario_pin(pin):
    pin = (pin or "").strip()
    if not pin or pin == "1234":
        return ""
    perfil = (
        PerfilUsuario.objects.filter(senha_rapida=pin)
        .select_related("user")
        .first()
    )
    if not perfil:
        return ""
    u = perfil.user
    nome = (u.get_full_name() or u.first_name or u.username or "").strip()
    return f"{perfil.codigo_vendedor} — {nome}".strip(" —")[:200]


def _rotulo_usuario_request(request):
    if not request.user.is_authenticated:
        return ""
    u = request.user
    nome = (u.get_full_name() or u.first_name or u.username or "").strip()
    return nome[:200]


def _historico_transferencia(
    tipo,
    *,
    usuario_label="",
    lote_uuid=None,
    produto_externo_id="",
    quantidade=None,
    observacao="",
):
    try:
        HistoricoTransferencia.objects.create(
            tipo=tipo,
            lote_uuid=lote_uuid,
            produto_externo_id=(produto_externo_id or "")[:100],
            quantidade=quantidade,
            usuario_label=(usuario_label or "")[:200],
            observacao=observacao or "",
        )
    except Exception:
        pass


def _transferir_vila_para_centro_exec(
    request,
    pin,
    produto_id,
    qtd,
    nome_produto,
    codigo_interno,
    obs_extra,
    registrar_historico=True,
    invalidar_cache=True,
):
    """
    Executa uma transferência Vila→Centro (Agro). Retorna dict com ok/erro e códigos HTTP sugeridos.
    """
    if pin == "1234":
        return {"ok": False, "erro": "Senha padrão (1234) bloqueada. Troque seu PIN.", "status": 403}
    if not PerfilUsuario.objects.filter(senha_rapida=pin).exists():
        return {"ok": False, "erro": "PIN incorreto.", "status": 403}

    produto_id = (produto_id or "").strip()[:100]
    if not produto_id:
        return {"ok": False, "erro": "Produto inválido.", "status": 400}

    if not isinstance(qtd, Decimal):
        try:
            qtd = Decimal(str(qtd))
        except Exception:
            return {"ok": False, "erro": "Quantidade inválida.", "status": 400}

    if qtd <= 0:
        return {"ok": False, "erro": "Informe quantidade maior que zero.", "status": 400}

    nome_produto = (nome_produto or "").strip()[:255] or "Produto"
    codigo_interno = (codigo_interno or "").strip()[:100]
    obs_extra = (obs_extra or "").strip()[:500]

    ped_row = PedidoTransferencia.objects.filter(produto_externo_id=produto_id).first()
    lote_ref = ped_row.lote_uuid if ped_row else None

    from produtos.views import (
        _empresa_loja_padrao_agro_estoque,
        _invalidar_caches_apos_ajuste_pin,
        _saldo_erp_produto_deposito_mongo,
        _saldo_final_agro_com_pin,
        obter_conexao_mongo,
    )

    client_m, db = obter_conexao_mongo()
    if db is None:
        return {"ok": False, "erro": "Mongo indisponível.", "status": 503}

    saldo_erp_v = _saldo_erp_produto_deposito_mongo(db, client_m, produto_id, "vila")
    saldo_erp_c = _saldo_erp_produto_deposito_mongo(db, client_m, produto_id, "centro")
    saldo_ag_v = _saldo_final_agro_com_pin(produto_id, "vila", saldo_erp_v)
    saldo_ag_c = _saldo_final_agro_com_pin(produto_id, "centro", saldo_erp_c)

    if qtd > saldo_ag_v:
        return {
            "ok": False,
            "erro": f"Quantidade maior que o saldo na Vila ({float(saldo_ag_v):.3f}).",
            "status": 400,
        }

    novo_v = (saldo_ag_v - qtd).quantize(Decimal("0.001"))
    novo_c = (saldo_ag_c + qtd).quantize(Decimal("0.001"))

    empresa_v, loja_v = _empresa_loja_padrao_agro_estoque("vila")
    empresa_c, loja_c = _empresa_loja_padrao_agro_estoque("centro")
    empresa = Empresa.objects.filter(nome_fantasia="Agro Mais").first()

    ref_obs = f"Vila→Centro {qtd}"
    if obs_extra:
        ref_obs = f"{ref_obs} · {obs_extra}"

    nome_v = f"{nome_produto} · Transferência {ref_obs}"[:255]
    nome_c = nome_v

    with transaction.atomic():
        AjusteRapidoEstoque.objects.create(
            empresa=empresa_v or empresa,
            loja=loja_v,
            produto_externo_id=produto_id,
            codigo_interno=codigo_interno,
            nome_produto=nome_v,
            deposito="vila",
            saldo_erp_referencia=saldo_erp_v,
            saldo_informado=novo_v,
            observacao=ref_obs,
            origem=OrigemAjusteEstoque.TRANSFERENCIA_UI,
            usuario=request.user if request.user.is_authenticated else None,
        )
        AjusteRapidoEstoque.objects.create(
            empresa=empresa_c or empresa,
            loja=loja_c,
            produto_externo_id=produto_id,
            codigo_interno=codigo_interno,
            nome_produto=nome_c,
            deposito="centro",
            saldo_erp_referencia=saldo_erp_c,
            saldo_informado=novo_c,
            observacao=ref_obs,
            origem=OrigemAjusteEstoque.TRANSFERENCIA_UI,
            usuario=request.user if request.user.is_authenticated else None,
        )

    if invalidar_cache:
        _invalidar_caches_apos_ajuste_pin()
    PedidoTransferencia.objects.filter(produto_externo_id=produto_id).delete()

    if registrar_historico:
        rotulo = _rotulo_usuario_pin(pin) or _rotulo_usuario_request(request)
        _historico_transferencia(
            HistoricoTransferencia.TIPO_TRANSFER_ITEM,
            usuario_label=rotulo,
            lote_uuid=lote_ref,
            produto_externo_id=produto_id,
            quantidade=qtd,
            observacao=nome_produto[:500],
        )

    return {
        "ok": True,
        "saldo_vila": float(novo_v),
        "saldo_centro": float(novo_c),
        "quantidade": float(qtd),
    }


@require_GET
def api_buscar_produtos(request):
    termo = request.GET.get('q', '').strip()
    produtos_json = []
    erro_api = ''

    if termo:
        try:
            client = get_mongo_client()
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
            pin_raw = (getattr(p, "senha_rapida", None) or "").strip()
            pin_personalizado = bool(pin_raw) and pin_raw != "1234"
            lista.append({"id": p.id, "nome": nome, "pin_personalizado": pin_personalizado})
        
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


@login_required(login_url="/admin/login/")
@require_POST
@csrf_protect
def api_definir_pin_rh(request):
    """Define ou redefine o PIN de um usuário via painel de RH (sem pedir PIN atual)."""
    try:
        perfil_id = request.POST.get('perfil_id', '').strip()
        novo_pin = request.POST.get('novo_pin', '').strip()

        if not perfil_id:
            return JsonResponse({'ok': False, 'erro': 'Perfil inválido.'}, status=400)
        if not novo_pin:
            return JsonResponse({'ok': False, 'erro': 'Informe o novo PIN.'}, status=400)
        if (not novo_pin.isdigit()) or len(novo_pin) != 4:
            return JsonResponse({'ok': False, 'erro': 'O PIN deve ter exatamente 4 dígitos numéricos.'}, status=400)
        if novo_pin == '1234':
            return JsonResponse({'ok': False, 'erro': 'Escolha um PIN diferente de 1234.'}, status=400)

        try:
            perfil = PerfilUsuario.objects.get(id=perfil_id)
        except PerfilUsuario.DoesNotExist:
            return JsonResponse({'ok': False, 'erro': 'Perfil não encontrado.'}, status=404)

        perfil.senha_rapida = novo_pin
        perfil.save()

        return JsonResponse({'ok': True, 'mensagem': 'PIN atualizado com sucesso.'})
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
            origem=OrigemAjusteEstoque.TRANSFERENCIA_UI,
            usuario=request.user if request.user.is_authenticated else None,
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
def api_transferir_vila_para_centro(request):
    """
    Registra transferência Vila Elias → Centro na **camada Agro** (dois ``AjusteRapidoEstoque``).
    Não grava movimento no ERP/Mongo do WL; o PDV passa a enxergar os saldos atualizados pela fórmula habitual.
    """
    try:
        pin = request.POST.get("pin", "").strip()
        produto_id = (request.POST.get("produto_id") or "").strip()[:100]
        qtd = _normalizar_decimal(request.POST.get("quantidade", "0"))
        nome_produto = (request.POST.get("nome_produto") or "").strip()[:255] or "Produto"
        codigo_interno = (request.POST.get("codigo_interno") or "").strip()[:100]
        obs_extra = (request.POST.get("observacao") or "").strip()[:500]

        out = _transferir_vila_para_centro_exec(
            request,
            pin,
            produto_id,
            qtd,
            nome_produto,
            codigo_interno,
            obs_extra,
            registrar_historico=True,
            invalidar_cache=True,
        )
        if not out.get("ok"):
            return JsonResponse({"ok": False, "erro": out.get("erro", "Erro.")}, status=int(out.get("status") or 400))
        return JsonResponse(
            {
                "ok": True,
                "saldo_vila": out["saldo_vila"],
                "saldo_centro": out["saldo_centro"],
                "quantidade": out["quantidade"],
            }
        )
    except InvalidOperation:
        return JsonResponse({"ok": False, "erro": "Quantidade inválida."}, status=400)
    except Exception as exc:
        return JsonResponse({"ok": False, "erro": str(exc)}, status=500)


@require_POST
@csrf_protect
def api_transferir_lote_vila_para_centro(request):
    """Transfere todos os itens de um lote (pedido IMPRESSO) com um único PIN."""
    try:
        data = json.loads(request.body)
        pin = str(data.get("pin") or "").strip()
        lote_raw = str(data.get("lote_uuid") or "").strip()
        if not lote_raw:
            return JsonResponse({"ok": False, "erro": "lote_uuid obrigatório."}, status=400)
        try:
            lote_uuid = uuid.UUID(lote_raw)
        except ValueError:
            return JsonResponse({"ok": False, "erro": "lote_uuid inválido."}, status=400)

        if pin == "1234" or not PerfilUsuario.objects.filter(senha_rapida=pin).exists():
            return JsonResponse({"ok": False, "erro": "PIN incorreto ou bloqueado."}, status=403)

        linhas_raw = list(
            PedidoTransferencia.objects.filter(lote_uuid=lote_uuid, status="IMPRESSO")
            .order_by("produto_externo_id")
            .values_list("produto_externo_id", "quantidade")
        )
        if not linhas_raw:
            return JsonResponse({"ok": False, "erro": "Lote vazio ou já encerrado."}, status=404)

        from produtos.views import _invalidar_caches_apos_ajuste_pin

        pids = [str(r[0]) for r in linhas_raw]
        nomes_map = {
            str(k): (v or "").strip()
            for k, v in ConfiguracaoTransferencia.objects.filter(produto_externo_id__in=pids).values_list(
                "produto_externo_id", "nome_produto"
            )
        }

        rotulo = _rotulo_usuario_pin(pin) or _rotulo_usuario_request(request)
        resultados_ok = []
        resultados_erro = []

        for produto_id_raw, qtd_raw in linhas_raw:
            pid = str(produto_id_raw).strip()[:100]
            try:
                qtd = Decimal(str(qtd_raw))
            except Exception:
                resultados_erro.append({"produto_id": pid, "erro": "Quantidade inválida."})
                continue
            nome = (nomes_map.get(str(pid)) or "").strip() or f"Produto {pid}"
            cod = pid
            out = _transferir_vila_para_centro_exec(
                request,
                pin,
                pid,
                qtd,
                nome,
                cod,
                "lote",
                registrar_historico=False,
                invalidar_cache=False,
            )
            if out.get("ok"):
                resultados_ok.append(
                    {"produto_id": pid, "quantidade": float(out.get("quantidade", qtd))}
                )
            else:
                resultados_erro.append({"produto_id": pid, "erro": out.get("erro", "Erro")})

        _invalidar_caches_apos_ajuste_pin()

        _historico_transferencia(
            HistoricoTransferencia.TIPO_TRANSFER_LOTE,
            usuario_label=rotulo,
            lote_uuid=lote_uuid,
            observacao=json.dumps(
                {"ok": resultados_ok, "erro": resultados_erro},
                ensure_ascii=False,
            )[:8000],
        )

        return JsonResponse(
            {
                "ok": len(resultados_erro) == 0,
                "transferidos": resultados_ok,
                "falhas": resultados_erro,
                "mensagem": f"{len(resultados_ok)} transferido(s), {len(resultados_erro)} falha(s).",
            }
        )
    except Exception as exc:
        return JsonResponse({"ok": False, "erro": str(exc)}, status=500)


@require_GET
def api_listar_historico_transferencia(request):
    """Últimos eventos de impressão / transferência / cancelamento."""
    try:
        lim = int(request.GET.get("limit", "80"))
        lim = max(1, min(lim, 200))
        qs = HistoricoTransferencia.objects.all()[:lim]
        evs = []
        for h in qs:
            evs.append(
                {
                    "tipo": h.tipo,
                    "criado_em": localtime(h.criado_em).strftime("%d/%m/%Y %H:%M")
                    if h.criado_em
                    else "",
                    "lote_uuid": str(h.lote_uuid) if h.lote_uuid else None,
                    "produto_id": h.produto_externo_id or None,
                    "quantidade": float(h.quantidade) if h.quantidade is not None else None,
                    "usuario": h.usuario_label or "—",
                    "observacao": (h.observacao or "")[:500],
                }
            )
        return JsonResponse({"ok": True, "eventos": evs})
    except Exception as exc:
        return JsonResponse({"ok": False, "erro": str(exc)}, status=500)


@require_POST
@csrf_protect
def api_salvar_config_transferencia(request):
    try:
        pin = request.POST.get('pin', '').strip()
        if pin == '1234':
            return JsonResponse({'ok': False, 'erro': 'Senha padrão (1234) bloqueada. Troque seu PIN.'}, status=403)
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
def api_registrar_impressao(request):
    try:
        data = json.loads(request.body)
        itens = data.get("itens", [])
        substituir = bool(data.get("substituir"))
        if not itens:
            return JsonResponse({"ok": False, "erro": "Nenhum item."}, status=400)

        ids_impressos = [str(item["id"]).strip()[:100] for item in itens if item.get("id")]
        if not substituir:
            conflitos = list(
                PedidoTransferencia.objects.filter(
                    produto_externo_id__in=ids_impressos, status="IMPRESSO"
                ).values_list("produto_externo_id", flat=True)
            )
            if conflitos:
                return JsonResponse(
                    {
                        "ok": False,
                        "codigo": "PEDIDO_ABERTO",
                        "produtos_em_aberto": conflitos,
                        "erro": "Um ou mais produtos já estão em separação (impresso).",
                    },
                    status=409,
                )

        pin_opt = str(data.get("pin") or "").strip()
        usuario_lbl = _rotulo_usuario_request(request)
        if pin_opt:
            if pin_opt == "1234":
                return JsonResponse({"ok": False, "erro": "PIN padrão bloqueado."}, status=403)
            rot = _rotulo_usuario_pin(pin_opt)
            if not rot:
                return JsonResponse(
                    {"ok": False, "erro": "PIN inválido para identificação no histórico."},
                    status=403,
                )
            usuario_lbl = rot
        elif not usuario_lbl:
            usuario_lbl = "—"

        lote = uuid.uuid4()
        agora = timezone.now()
        criados = 0
        for item in itens:
            pid = str(item["id"]).strip()[:100]
            qt = Decimal(str(item.get("qtde") or 0))
            if not pid or qt <= 0:
                continue
            PedidoTransferencia.objects.filter(produto_externo_id=pid).delete()
            PedidoTransferencia.objects.create(
                produto_externo_id=pid,
                quantidade=qt,
                lote_uuid=lote,
                status="IMPRESSO",
                impresso_em=agora,
            )
            criados += 1
        if criados == 0:
            return JsonResponse({"ok": False, "erro": "Nenhuma quantidade válida nos itens."}, status=400)

        snap = []
        for item in itens:
            pid = str(item.get("id") or "").strip()[:100]
            qt = Decimal(str(item.get("qtde") or 0))
            if not pid or qt <= 0:
                continue
            snap.append({"id": pid, "qt": float(qt)})

        _historico_transferencia(
            HistoricoTransferencia.TIPO_LOTE_IMPRESSO,
            usuario_label=usuario_lbl,
            lote_uuid=lote,
            observacao=json.dumps({"itens": snap, "n": criados}, ensure_ascii=False)[:8000],
        )
        return JsonResponse({"ok": True, "lote_uuid": str(lote)})
    except Exception as exc:
        return JsonResponse({"ok": False, "erro": str(exc)}, status=500)


@require_GET
def api_listar_lotes_transferencia(request):
    """Lista lotes com itens em separação (status IMPRESSO), agrupados por lote_uuid."""
    try:
        rows = list(
            PedidoTransferencia.objects.filter(status="IMPRESSO")
            .order_by("-impresso_em", "-id")
        )
        por_lote = {}
        for r in rows:
            key = str(r.lote_uuid) if r.lote_uuid else f"legacy-{r.pk}"
            por_lote.setdefault(key, []).append(r)

        lotes_out = []
        for _lote_key, linhas in por_lote.items():
            linhas.sort(key=lambda x: (x.impresso_em or x.criado_em, x.produto_externo_id))
            primeiro = linhas[0]
            ref_dt = primeiro.impresso_em or primeiro.criado_em
            lote_uuid_str = str(primeiro.lote_uuid) if primeiro.lote_uuid else None
            lotes_out.append(
                {
                    "lote_uuid": lote_uuid_str,
                    "impresso_em": localtime(ref_dt).strftime("%d/%m/%Y %H:%M") if ref_dt else None,
                    "n_itens": len(linhas),
                    "itens": [
                        {
                            "produto_id": str(x.produto_externo_id),
                            "quantidade": float(x.quantidade),
                        }
                        for x in linhas
                    ],
                }
            )
        lotes_out.sort(key=lambda x: x.get("impresso_em") or "", reverse=True)
        return JsonResponse({"ok": True, "lotes": lotes_out[:50]})
    except Exception as exc:
        return JsonResponse({"ok": False, "erro": str(exc)}, status=500)


@require_POST
@csrf_protect
def api_atualizar_pedido_transferencia(request):
    try:
        data = json.loads(request.body)
        pin = str(data.get("pin") or "").strip()
        if pin == "1234":
            return JsonResponse({"ok": False, "erro": "Senha padrão (1234) bloqueada. Troque seu PIN."}, status=403)
        if not PerfilUsuario.objects.filter(senha_rapida=pin).exists():
            return JsonResponse({"ok": False, "erro": "PIN incorreto."}, status=403)

        produto_id = str(data.get("produto_id") or "").strip()[:100]
        if not produto_id:
            return JsonResponse({"ok": False, "erro": "Produto inválido."}, status=400)
        qtd = _normalizar_decimal(data.get("quantidade", "0"))
        if qtd <= 0:
            return JsonResponse({"ok": False, "erro": "Quantidade deve ser maior que zero."}, status=400)

        ped = PedidoTransferencia.objects.filter(
            produto_externo_id=produto_id, status="IMPRESSO"
        ).first()
        if not ped:
            return JsonResponse({"ok": False, "erro": "Nenhum pedido em aberto para este produto."}, status=404)

        ped.quantidade = qtd
        ped.save(update_fields=["quantidade"])
        return JsonResponse({"ok": True, "quantidade": float(qtd)})
    except InvalidOperation:
        return JsonResponse({"ok": False, "erro": "Quantidade inválida."}, status=400)
    except Exception as exc:
        return JsonResponse({"ok": False, "erro": str(exc)}, status=500)


@require_POST
@csrf_protect
def api_adicionar_pedido_transferencia(request):
    try:
        data = json.loads(request.body)
        pin = str(data.get("pin") or "").strip()
        if pin == "1234":
            return JsonResponse({"ok": False, "erro": "Senha padrão (1234) bloqueada. Troque seu PIN."}, status=403)
        if not PerfilUsuario.objects.filter(senha_rapida=pin).exists():
            return JsonResponse({"ok": False, "erro": "PIN incorreto."}, status=403)

        produto_id = str(data.get("produto_id") or "").strip()[:100]
        lote_raw = str(data.get("lote_uuid") or "").strip()
        if not produto_id or not lote_raw:
            return JsonResponse({"ok": False, "erro": "produto_id e lote_uuid são obrigatórios."}, status=400)
        try:
            lote_uuid = uuid.UUID(lote_raw)
        except ValueError:
            return JsonResponse({"ok": False, "erro": "lote_uuid inválido."}, status=400)

        qtd = _normalizar_decimal(data.get("quantidade", "0"))
        if qtd <= 0:
            return JsonResponse({"ok": False, "erro": "Quantidade deve ser maior que zero."}, status=400)

        if not PedidoTransferencia.objects.filter(lote_uuid=lote_uuid, status="IMPRESSO").exists():
            return JsonResponse({"ok": False, "erro": "Lote não encontrado ou já encerrado."}, status=404)

        outro = (
            PedidoTransferencia.objects.filter(produto_externo_id=produto_id, status="IMPRESSO")
            .exclude(lote_uuid=lote_uuid)
            .first()
        )
        if outro:
            return JsonResponse(
                {
                    "ok": False,
                    "erro": "Este produto já está em outro lote de separação. Cancele o outro antes.",
                },
                status=400,
            )

        mesmo_lote = PedidoTransferencia.objects.filter(
            produto_externo_id=produto_id, lote_uuid=lote_uuid, status="IMPRESSO"
        ).first()
        if mesmo_lote:
            mesmo_lote.quantidade = qtd
            mesmo_lote.save(update_fields=["quantidade"])
            return JsonResponse({"ok": True, "quantidade": float(mesmo_lote.quantidade)})

        PedidoTransferencia.objects.create(
            produto_externo_id=produto_id,
            quantidade=qtd,
            lote_uuid=lote_uuid,
            status="IMPRESSO",
            impresso_em=timezone.now(),
        )
        return JsonResponse({"ok": True, "quantidade": float(qtd)})
    except InvalidOperation:
        return JsonResponse({"ok": False, "erro": "Quantidade inválida."}, status=400)
    except Exception as exc:
        return JsonResponse({"ok": False, "erro": str(exc)}, status=500)

@require_POST
@csrf_protect
def api_cancelar_separacao(request, id):
    try:
        pid = str(id).strip()[:100]
        ex = PedidoTransferencia.objects.filter(produto_externo_id=pid).first()
        lote_ref = ex.lote_uuid if ex else None
        qant = ex.quantidade if ex else None
        pin_opt = (request.POST.get("pin") or "").strip()
        usuario_lbl = _rotulo_usuario_pin(pin_opt) if pin_opt else _rotulo_usuario_request(request)
        if not usuario_lbl:
            usuario_lbl = "—"

        PedidoTransferencia.objects.filter(produto_externo_id=pid).delete()

        _historico_transferencia(
            HistoricoTransferencia.TIPO_CANCEL_SEP,
            usuario_label=usuario_lbl,
            lote_uuid=lote_ref,
            produto_externo_id=pid,
            quantidade=qant,
            observacao="",
        )
        return JsonResponse({"ok": True})
    except Exception as e:
        return JsonResponse({"ok": False, "erro": str(e)})

@require_POST
@csrf_protect
def api_importar_planilha_transferencia(request):
    try:
        pin = request.POST.get('pin', '').strip()
        if pin == '1234':
            return JsonResponse({'ok': False, 'erro': 'Senha padrão (1234) bloqueada. Troque seu PIN.'}, status=403)
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

        client = get_mongo_client()
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


@require_POST
@csrf_protect
def api_atualizar_medias(request):
    try:
        pin = request.POST.get('pin', '').strip()
        if pin == '1234':
            return JsonResponse({'ok': False, 'erro': 'Senha padrão (1234) bloqueada. Troque seu PIN.'}, status=403)
        perfil = PerfilUsuario.objects.filter(senha_rapida=pin).first()
        if not perfil:
            return JsonResponse({'ok': False, 'erro': 'PIN INCORRETO'}, status=403)

        calcular()

        return JsonResponse({'ok': True, 'mensagem': 'Médias de vendas atualizadas com sucesso!'})
    except Exception as exc:
        return JsonResponse({'ok': False, 'erro': f'Erro ao calcular médias: {exc}'}, status=500)

@require_GET
def api_sugestoes_transferencia(request):
    try:
        client = get_mongo_client()

        # 1. Pegar IDs dos produtos que possuem regras configuradas
        regras = ConfiguracaoTransferencia.objects.all()
        mapa_regras = {str(r.produto_externo_id): r for r in regras}
        ids_configurados = list(mapa_regras.keys())

        # Pegar Lotes de Separação
        pedidos_sep = PedidoTransferencia.objects.filter(status="IMPRESSO")
        mapa_pedidos = {str(p.produto_externo_id): p for p in pedidos_sep}
        ids_pedidos = list(mapa_pedidos.keys())

        # 2. Descobrir quais produtos (não configurados) têm saldo na Vila Elias (Prioridade Alta)
        query_vila = {"Saldo": {"$gt": 0}}
        if hasattr(client, 'DEPOSITO_VILA_ELIAS') and client.DEPOSITO_VILA_ELIAS:
            query_vila["DepositoID"] = client.DEPOSITO_VILA_ELIAS
        else:
            query_vila["Deposito"] = {"$regex": "vila", "$options": "i"}

        estoques_vila = list(client.db[client.col_e].find(query_vila, {"ProdutoID": 1, "_id": 0}))
        ids_com_saldo_vila = [str(e.get("ProdutoID")) for e in estoques_vila if e.get("ProdutoID")]

        # 3. Unir tudo (Só vamos baixar do ERP o que realmente importa e ignorar o resto)
        ids_alvo = list(set(ids_configurados + ids_com_saldo_vila + ids_pedidos))

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
            pedido = mapa_pedidos.get(pid)

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
                        
                        if pedido:
                            status = "SEPARANDO"
                            qtde_transferir = pedido.quantidade
                        else:
                            if qtde_transferir > 0 and qtde_comprar > 0: status = "TRANSFERIR_COMPRAR"
                            elif qtde_transferir > 0: status = "TRANSFERIR"
                            elif qtde_comprar > 0: status = "COMPRAR"
                    else:
                        qtde_necessaria = regra.capacidade_maxima - saldo_centro
                        if qtde_necessaria > 0:
                            qtde_transferir = qtde_necessaria if saldo_vila >= qtde_necessaria else max(Decimal('0'), saldo_vila)
                            qtde_comprar = qtde_necessaria - qtde_transferir
                            if pedido:
                                status = "SEPARANDO"
                                qtde_transferir = pedido.quantidade
                            else:
                                status = "COMPRAR" if qtde_transferir == 0 else ("TRANSFERIR" if qtde_comprar == 0 else "TRANSFERIR_COMPRAR")
                else:
                    # Auto-limpeza do pedido caso o saldo tenha subido no ERP
                    if pedido:
                        pedido.delete()
                        status = "OK"

                sugestoes.append(
                    {
                        "produto_id": pid,
                        "codigo": p_info["codigo"],
                        "codigo_barras": p_info["codigo_barras"],
                        "nome": p_info["nome"] or regra.nome_produto,
                        "saldo_centro": float(saldo_centro),
                        "saldo_vila": float(saldo_vila),
                        "saldo_centro_erp": float(saldo_centro_erp),
                        "saldo_vila_erp": float(saldo_vila_erp),
                        "status": status,
                        "qtde_transferir": float(qtde_transferir),
                        "qtde_comprar": float(qtde_comprar),
                        "capacidade_maxima": float(regra.capacidade_maxima),
                        "estoque_seguranca": float(regra.estoque_seguranca),
                        "capacidade_minima": float(regra.capacidade_minima),
                        "configurado": True,
                        "prioridade": 3 if status != "SEPARANDO" else 4,
                        **_pedido_transferencia_extra(pedido),
                    }
                )
            else:
                # PRODUTO NÃO CONFIGURADO
                if pedido:
                    status = "SEPARANDO"
                    qtde_transferir = pedido.quantidade
                else:
                    status = "ALTA" if saldo_vila > 0 else "MEDIA"
                    qtde_transferir = Decimal('0')

                sugestoes.append(
                    {
                        "produto_id": pid,
                        "codigo": p_info["codigo"],
                        "codigo_barras": p_info["codigo_barras"],
                        "nome": p_info["nome"],
                        "saldo_centro": float(saldo_centro),
                        "saldo_vila": float(saldo_vila),
                        "saldo_centro_erp": float(saldo_centro_erp),
                        "saldo_vila_erp": float(saldo_vila_erp),
                        "status": status,
                        "qtde_transferir": float(qtde_transferir),
                        "qtde_comprar": 0.0,
                        "configurado": False,
                        "prioridade": 4 if status == "SEPARANDO" else 1,
                        **_pedido_transferencia_extra(pedido),
                    }
                )

        # Ordenação mágica: Prioridade 1 (Alta) -> 3 (Configurados), e dentro delas em ordem alfabética.
        sugestoes.sort(key=lambda x: (x["prioridade"], x["nome"]))

        ultima_atualizacao = "Nunca"
        ultima_regra = ConfiguracaoTransferencia.objects.order_by('-atualizado_em').first()
        if ultima_regra and ultima_regra.atualizado_em:
            ultima_atualizacao = localtime(ultima_regra.atualizado_em).strftime("%d/%m às %H:%M")

        return JsonResponse({'sugestoes': sugestoes, 'ultima_atualizacao': ultima_atualizacao})
    except Exception as exc:
        return JsonResponse({'ok': False, 'erro': f'Erro: {exc}'}, status=500)


@never_cache
@login_required(login_url="/admin/login/")
@require_GET
def api_estoque_sync_health(request):
    """JSON: heartbeat leitura Mongo + build catálogo (ver ``EstoqueSyncHealth``)."""
    from estoque.sync_health import snapshot_health_dict

    return JsonResponse({"ok": True, **snapshot_health_dict()})


@never_cache
@login_required(login_url="/admin/login/")
@require_GET
def api_estoque_divergencia_ajustes(request):
    """
    Lista o último ajuste por (produto, depósito): camada Agro sobre o espelho ERP no Mongo.
    Útil para auditoria (não é “erro” — é o delta operacional intencional).
    """
    try:
        lim = int(request.GET.get("limit") or 200)
    except (TypeError, ValueError):
        lim = 200
    lim = max(1, min(500, lim))

    rows = AjusteRapidoEstoque.objects.all().order_by("produto_externo_id", "deposito", "-criado_em")
    seen = set()
    itens = []
    for r in rows:
        if len(itens) >= lim:
            break
        k = (r.produto_externo_id, r.deposito)
        if k in seen:
            continue
        seen.add(k)
        itens.append(
            {
                "produto_externo_id": r.produto_externo_id,
                "deposito": r.deposito,
                "nome_produto": (r.nome_produto or "")[:200],
                "saldo_erp_referencia": float(r.saldo_erp_referencia),
                "saldo_informado": float(r.saldo_informado),
                "diferenca_saldo": float(r.diferenca_saldo),
                "origem": r.origem,
                "criado_em": r.criado_em.isoformat() if r.criado_em else None,
            }
        )

    return JsonResponse({"ok": True, "total_listados": len(itens), "itens": itens})