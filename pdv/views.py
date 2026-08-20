from django.conf import settings
from django.shortcuts import render
from django.templatetags.static import static
from django.urls import reverse
from django.middleware.csrf import get_token
from django.views.decorators.csrf import ensure_csrf_cookie

from produtos.entrega_bairros_data import BAIRROS_JACUPI_RURAIS, BAIRROS_JACUPI_URBANOS
from produtos.caixa_util import (
    PONTO_CAIXA_NOTEBOOK,
    adotar_sessao_caixa_unica_aberta,
    filtrar_maquininhas_pdv_sem_mp,
    filtrar_maquininhas_por_loja,
    mp_point_host_conta,
    navegador_pode_mp_point_automatico,
    obter_sessao_caixa_aberta_request,
    ponto_operacao_browser,
    rotulo_caixa_browser,
)
from produtos.agro_fonte_config import agro_staging_readonly
from produtos.nfce_config_util import nfce_config_resumo
from produtos.mercado_pago_point import (
    MAQUININHAS_MP_POINT_AUTO_CENTRO,
    MAQUININHAS_MP_POINT_AUTO_VILA,
    mp_point_conta_configurada,
)

_DEFAULT_MAQUININHAS_CARTAO_PDV = [
    {
        "id": "mp_balcao",
        "nome": "Mercado Pago Centro (automático)",
        "rede": "mp",
        "lojas": ["centro"],
    },
    {"id": "cielo_1", "nome": "Cielo", "rede": "cielo", "lojas": ["centro"]},
    {"id": "mp_renan", "nome": "Mercado Pago Renan", "rede": "mp", "lojas": ["centro"]},
    {"id": "mp_vila", "nome": "Mercado Pago Vila", "rede": "mp", "lojas": ["vila"]},
    {"id": "sicredi_1", "nome": "Sicredi", "rede": "sicredi", "lojas": ["vila"]},
]

_DEFAULT_MAQUININHAS_PIX_PDV = [
    {
        "id": "pix_mp_qr",
        "nome": "Mercado Pago Centro — Pix (automático)",
        "rede": "mp",
        "lojas": ["centro"],
    },
    {"id": "pix_cielo", "nome": "Cielo — Pix", "rede": "cielo", "lojas": ["centro"]},
    {
        "id": "pix_mp_renan",
        "nome": "Mercado Pago Renan — Pix",
        "rede": "mp",
        "lojas": ["centro"],
    },
    {
        "id": "pix_mp_vila",
        "nome": "Mercado Pago Vila — Pix",
        "rede": "mp",
        "lojas": ["vila"],
    },
    {"id": "pix_sicredi", "nome": "Sicredi — Pix", "rede": "sicredi", "lojas": ["vila"]},
    {
        "id": "pix_sicoob_chave",
        "nome": "Sicoob — Chave Pix (WhatsApp)",
        "rede": "sicoob",
        "lojas": ["centro", "vila"],
    },
]


def _maquininhas_cartao_effective():
    raw = getattr(settings, "PDV_WIZARD_MAQUININHAS_CARTAO", None)
    if raw:
        return raw
    legacy = getattr(settings, "PDV_WIZARD_MAQUININHAS", None)
    if legacy:
        return [m for m in legacy if str(m.get("id", "") or "").strip() != "mp_loja"]
    return _DEFAULT_MAQUININHAS_CARTAO_PDV


def _maquininhas_pix_effective():
    return getattr(settings, "PDV_WIZARD_MAQUININHAS_PIX", None) or _DEFAULT_MAQUININHAS_PIX_PDV


def _safe_float_ptbr(val, default=0.0):
    try:
        if val is None:
            return default
        s = str(val).strip()
        if not s:
            return default
        if "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".")
        elif "," in s:
            s = s.replace(",", ".")
        return float(s)
    except (TypeError, ValueError):
        return default


@ensure_csrf_cookie
def pdv_home(request):
    caixa_aberto = obter_sessao_caixa_aberta_request(request) or adotar_sessao_caixa_unica_aberta(request)
    host_conta = mp_point_host_conta(request)
    centro_cfg = mp_point_conta_configurada("centro")
    vila_cfg = mp_point_conta_configurada("vila")
    centro_ok = centro_cfg and host_conta == "centro"
    vila_ok = vila_cfg and host_conta == "vila"
    mp_point_nav = navegador_pode_mp_point_automatico(request)
    mp_point_enabled = centro_ok or vila_ok
    ponto_nav = ponto_operacao_browser(request)
    pdv_reabrir_from_consulta = None
    if request.GET.get("reabrir") == "1":
        chk = request.session.get("pdv_checkout")
        if chk and chk.get("itens"):
            pdv_reabrir_from_consulta = chk
    u_pdv = ""
    if getattr(request, "user", None) and request.user.is_authenticated:
        u_pdv = (request.user.get_full_name() or "").strip() or (
            request.user.get_username() if hasattr(request.user, "get_username") else ""
        )
    origens_maps = [
        {
            "id": "centro",
            "label": "Centro — Av. Adhemar de Barros, 230",
            "q": (getattr(settings, "LOJA_MAPS_ORIGEM_CENTRO", None) or "").strip(),
            "link_loja": (getattr(settings, "LOJA_MAPS_LINK_CENTRO", None) or "").strip(),
        },
        {
            "id": "vila",
            "label": "Vila Elias",
            "q": (getattr(settings, "LOJA_MAPS_ORIGEM_VILA", None) or "").strip(),
            "link_loja": (getattr(settings, "LOJA_MAPS_LINK_VILA", None) or "").strip(),
        },
    ]
    from produtos.pdv_deposito_util import bootstrap_deposito
    from produtos.campanha_pdv_util import bootstrap_campanha

    dep_boot = bootstrap_deposito(request)
    dep_loja = str(dep_boot.get("deposito") or "centro").strip().lower()
    maq_cartao = filtrar_maquininhas_por_loja(_maquininhas_cartao_effective(), dep_loja)
    maq_pix = filtrar_maquininhas_por_loja(_maquininhas_pix_effective(), dep_loja)
    if not centro_ok:
        maq_cartao = filtrar_maquininhas_pdv_sem_mp(maq_cartao, MAQUININHAS_MP_POINT_AUTO_CENTRO)
        maq_pix = filtrar_maquininhas_pdv_sem_mp(maq_pix, MAQUININHAS_MP_POINT_AUTO_CENTRO)
    if vila_cfg and not vila_ok:
        maq_cartao = filtrar_maquininhas_pdv_sem_mp(maq_cartao, MAQUININHAS_MP_POINT_AUTO_VILA)
        maq_pix = filtrar_maquininhas_pdv_sem_mp(maq_pix, MAQUININHAS_MP_POINT_AUTO_VILA)
    if vila_ok:
        maq_cartao = [dict(m) if isinstance(m, dict) else m for m in maq_cartao]
        maq_pix = [dict(m) if isinstance(m, dict) else m for m in maq_pix]
        for lista in (maq_cartao, maq_pix):
            for m in lista:
                if not isinstance(m, dict):
                    continue
                mid = str(m.get("id") or "").strip().lower()
                if mid == "mp_vila":
                    m["nome"] = "Mercado Pago Vila (automático)"
                elif mid == "pix_mp_vila":
                    m["nome"] = "Mercado Pago Vila — Pix (automático)"
    ctx = {
        "caixa_aberto": caixa_aberto,
        "caixa_rotulo": rotulo_caixa_browser(request, caixa_aberto) if caixa_aberto else "Caixa fechado",
        "pdv_deposito": dep_boot.get("deposito") or "centro",
        "pdv_deposito_label": dep_boot.get("depositoLabel") or "Centro",
        "pdv_estoque_ativo_label": dep_boot.get("estoqueAtivoLabel") or "Esto: Centro",
        "pdv_bootstrap": {
            "csrfToken": get_token(request),
            "usuarioSalvamento": u_pdv,
            "clientePadraoNome": "CONSUMIDOR NÃO IDENTIFICADO...",
            "pdvEntregaWhatsapp": getattr(settings, "PDV_ENTREGA_WHATSAPP", "") or "",
            "origensMaps": origens_maps,
            "pdvDeposito": dep_boot,
            "campanhaPdv": bootstrap_campanha(dep_boot.get("deposito")),
            "urls": {
                "apiBuscarProdutos": reverse("api_buscar_mobile"),
                "apiBuscarClientes": reverse("api_buscar_clientes"),
                "apiPdvClienteRapido": reverse("api_pdv_cliente_rapido"),
                "apiPdvClienteEditarPattern": reverse("api_pdv_cliente_editar", args=[0]).replace(
                    "/0/", "/__pk__/"
                ),
                "apiClienteWhatsappDuplicado": reverse("api_cliente_whatsapp_duplicado"),
                "apiClienteLimparWhatsappPattern": reverse(
                    "api_cliente_limpar_whatsapp", args=[0]
                ).replace("/0/", "/__pk__/"),
                "apiClienteExclusaoPreviewPattern": reverse(
                    "api_cliente_exclusao_preview", args=[0]
                ).replace("/0/", "/__pk__/"),
                "apiClienteTransferirSaldosPattern": reverse(
                    "api_cliente_transferir_saldos", args=[0]
                ).replace("/0/", "/__pk__/"),
                "apiClienteExcluirPattern": reverse("api_cliente_excluir", args=[0]).replace(
                    "/0/", "/__pk__/"
                ),
                "apiClienteValeManualPattern": reverse(
                    "api_cliente_vale_credito_manual", args=[0]
                ).replace("/0/", "/__pk__/"),
                "apiClienteEventosPattern": reverse("api_cliente_eventos", args=[0]).replace(
                    "/0/", "/__pk__/"
                ),
                "apiPdvGeocodePlus": reverse("api_pdv_geocode_plus"),
                "apiListCustomers": reverse("api_list_customers"),
                "apiPdvSalvarCheckoutDraft": reverse("api_pdv_salvar_checkout_draft"),
                "apiPdvLimparCheckoutDraft": reverse("api_pdv_limpar_checkout_draft"),
                "apiEnviarPedidoErp": reverse("api_enviar_pedido_erp"),
                "apiPdvMpPointCriar": reverse("api_pdv_mp_point_criar"),
                "apiPdvMpPointStatus": reverse("api_pdv_mp_point_status"),
                "apiPdvMpPointConfirmarTranche": reverse("api_pdv_mp_point_confirmar_tranche"),
                "apiPdvMpPointFinalizar": reverse("api_pdv_mp_point_finalizar"),
                "apiPdvMpPointAbandon": reverse("api_pdv_mp_point_abandon"),
                "apiPdvMpPointForcarLiberar": reverse("api_pdv_mp_point_forcar_liberar"),
                "apiEntregaRegistrar": reverse("api_entrega_registrar"),
                "apiPdvClienteCreditoFiado": reverse("api_pdv_cliente_credito_fiado"),
                "apiPdvRelacionamentoCliente": reverse("api_pdv_relacionamento_cliente"),
                "apiPdvRelacionamentoClienteExtras": reverse("api_pdv_relacionamento_cliente_extras"),
                "apiPdvOrcamentos": reverse("api_pdv_orcamentos"),
                "apiPdvEntregasPendentes": reverse("api_pdv_entregas_pendentes"),
                "apiPdvEntregaPendenteDetalhe": reverse("api_pdv_entrega_pendente_detalhe", args=[0]).replace(
                    "/0/", "/__pk__/"
                ),
                "apiPdvEntregaPendenteAssumir": reverse("api_pdv_entrega_pendente_assumir", args=[0]).replace(
                    "/0/", "/__pk__/"
                ),
                "apiPdvEntregaPendenteFinalizar": reverse("api_pdv_entrega_pendente_finalizar", args=[0]).replace(
                    "/0/", "/__pk__/"
                ),
                "apiPdvEntregaPendenteCancelar": reverse("api_pdv_entrega_pendente_cancelar", args=[0]).replace(
                    "/0/", "/__pk__/"
                ),
                "apiVendaReenviarErp": reverse("api_venda_agro_reenviar_erp", args=[0]).replace(
                    "/0/", "/__pk__/"
                ),
                "apiLoginMobile": reverse("api_login_mobile"),
                "pdvCheckout": reverse("pdv_checkout"),
                "pdvWizardHome": reverse("pdv_home"),
                "consultaLegacy": reverse("consulta_produtos"),
                "home": reverse("home"),
                "vendasLista": reverse("vendas_lista"),
                "clientesLista": reverse("clientes_lista"),
                "clienteNovo": reverse("cliente_novo"),
                "clienteEditarPattern": reverse("cliente_editar", args=[0]).replace("/0/editar/", "/__pk__/editar/"),
                "entregasPainel": reverse("entregas_painel"),
                "caixaPainel": reverse("caixa_painel"),
                "fiadoGestao": reverse("fiado_gestao"),
                "apiFiadoCobrancaPdv": reverse("api_fiado_cobranca_pdv"),
                "apiFiadoBaixaPdv": reverse("api_fiado_baixa_pdv"),
                "apiFiadoRecibo": reverse("api_fiado_recibo_baixas"),
                "apiFiadoRecibos": reverse("api_fiado_recibos"),
                "apiPromocoesAtivasPdv": reverse("api_promocoes_ativas_pdv"),
                "apiPdvProdutoEdicaoRapidaPattern": reverse(
                    "api_pdv_produto_edicao_rapida", args=["__PID__"]
                ),
                "apiPdvProdutoAjusteEstoque": reverse("api_pdv_produto_ajuste_estoque"),
                "apiPdvCadastroRapidoChecar": reverse("api_pdv_cadastro_rapido_checar"),
                "apiPdvCadastroRapidoGmPreview": reverse("api_pdv_cadastro_rapido_gm_preview"),
                "apiPdvCadastroRapidoCriar": reverse("api_pdv_cadastro_rapido_criar"),
                "apiProdutosGestaoOverlaySalvar": reverse("api_produtos_gestao_overlay_salvar"),
                "apiComprasRelatorioDim": reverse("api_compras_relatorio_dim_sugestao"),
                "apiProdutosCadastroFacetaNova": reverse("api_produtos_cadastro_faceta_nova"),
                "apiPdvDeposito": reverse("api_pdv_deposito"),
                "apiPdvUsoLojaMeta": reverse("api_pdv_uso_loja_meta"),
                "apiPdvUsoLojaConfirmar": reverse("api_pdv_uso_loja_confirmar"),
                "apiPdvUsoLojaHistorico": reverse("api_pdv_uso_loja_historico"),
                "apiPdvUsoLojaEstornarPattern": reverse(
                    "api_pdv_uso_loja_estornar", args=[0]
                ).replace("/0/", "/__pk__/"),
                "apiPdvTransfLojaResumo": reverse("api_pdv_transf_loja_resumo"),
                "apiPdvTransfLojaSaldos": reverse("api_pdv_transf_loja_saldos"),
                "apiPdvTransfLojaLista": reverse("api_pdv_transf_loja_lista"),
                "apiPdvTransfLojaCriar": reverse("api_pdv_transf_loja_criar"),
                "apiPdvTransfLojaAjustar": reverse("api_pdv_transf_loja_ajustar"),
                "apiPdvTransfLojaAcaoPattern": reverse(
                    "api_pdv_transf_loja_acao", args=[0]
                ).replace("/0/", "/__pk__/"),
            },
            "search": {
                "mode": "wizard",
                "stagingReadonly": agro_staging_readonly(),
            },
            "stagingReadonly": agro_staging_readonly(),
            "erpEnvioAssincrono": bool(getattr(settings, "PDV_ERP_ENVIO_ASSINCRONO", True)),
            "assets": {
                "placeholderProduto": static("img/agro-mais-logo-buscador.png"),
            },
            "caixa": {
                "aberto": bool(caixa_aberto),
                "id": caixa_aberto.pk if caixa_aberto else None,
                "pontoOperacao": ponto_nav,
                "rotulo": rotulo_caixa_browser(request, caixa_aberto)
                if caixa_aberto
                else "Caixa fechado",
            },
            "bairrosEntrega": {
                "urbanos": list(BAIRROS_JACUPI_URBANOS),
                "rurais": list(BAIRROS_JACUPI_RURAIS),
            },
            "pagamentoUi": {
                "mpPointEnabled": mp_point_enabled,
                "mpPointCentroEnabled": centro_ok,
                "mpPointVilaEnabled": vila_ok,
                "mpPointMotivoBloqueio": (
                    (
                        "Mercado Pago automático da Vila só no computador do Caixa Vila Elias. "
                        "Neste PDV use Sicredi."
                        if vila_cfg and dep_loja == "vila"
                        else (
                            "Mercado Pago automático só no computador do Caixa Gaveta (aberto primeiro). "
                            "Neste PDV use as máquinas manuais da loja."
                        )
                    )
                    if (centro_cfg or vila_cfg)
                    and not mp_point_nav
                    and ponto_nav == PONTO_CAIXA_NOTEBOOK
                    else (
                        (
                            "Abra o Caixa Vila Elias neste computador para usar Mercado Pago automático."
                            if vila_cfg and dep_loja == "vila"
                            else "Abra o Caixa Gaveta neste computador para usar Mercado Pago automático."
                        )
                        if (centro_cfg or vila_cfg) and not mp_point_nav
                        else ""
                    )
                ),
                "qrMercadoPagoUrl": settings.PDV_QR_MERCADOPAGO_URL,
                "qrSicrediUrl": settings.PDV_QR_SICREDI_URL,
                "chavePixSicob": settings.PDV_CHAVE_PIX_SICOB,
                "saldoValeCredito": _safe_float_ptbr(settings.PDV_WIZARD_SALDO_VALE_CREDITO, 0.0),
                "saldoCashback": _safe_float_ptbr(settings.PDV_WIZARD_SALDO_CASHBACK, 0.0),
                "maquininhasCartao": maq_cartao,
                "maquininhasPix": maq_pix,
            },
            "nfce": nfce_config_resumo(),
        },
        "pdv_reabrir_from_consulta": pdv_reabrir_from_consulta,
        "agro_pdv_assets_v": (
            f"{getattr(settings, 'AGRO_PDV_ASSETS_V', '') or 'dev'}-{int(__import__('time').time())}"
            if settings.DEBUG
            else (getattr(settings, "AGRO_PDV_ASSETS_V", "") or "")
        ),
    }
    return render(request, "produtos/pdv_wizard.html", ctx)
