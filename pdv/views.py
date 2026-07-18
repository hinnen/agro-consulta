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
    navegador_pode_mp_point_automatico,
    obter_sessao_caixa_aberta_request,
    ponto_operacao_browser,
    rotulo_caixa_browser,
)
from produtos.agro_fonte_config import agro_staging_readonly
from produtos.nfce_config_util import nfce_config_resumo

_DEFAULT_MAQUININHAS_CARTAO_PDV = [
    {"id": "mp_balcao", "nome": "Mercado Pago — Balcão (automático)", "rede": "mp"},
    {"id": "cielo_1", "nome": "Cielo", "rede": "cielo"},
    {"id": "sicredi_1", "nome": "Sicredi", "rede": "sicredi"},
]

_DEFAULT_MAQUININHAS_PIX_PDV = [
    {"id": "pix_mp_qr", "nome": "Mercado Pago — Pix (automático)", "rede": "mp"},
    {"id": "pix_cielo", "nome": "Cielo — Pix", "rede": "cielo"},
    {"id": "pix_sicoob_chave", "nome": "Sicoob — Chave Pix", "rede": "sicoob"},
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
    mp_point_configurado = bool(
        getattr(settings, "MP_POINT_ENABLED", False)
        and (getattr(settings, "MP_POINT_ACCESS_TOKEN", "") or "").strip()
        and (getattr(settings, "MP_POINT_TERMINAL_ID", "") or "").strip()
    )
    mp_point_nav = navegador_pode_mp_point_automatico(request)
    mp_point_enabled = mp_point_configurado and mp_point_nav
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

    dep_boot = bootstrap_deposito(request)
    ctx = {
        "caixa_aberto": caixa_aberto,
        "caixa_rotulo": rotulo_caixa_browser(request, caixa_aberto) if caixa_aberto else "Caixa fechado",
        "pdv_deposito": dep_boot.get("deposito") or "centro",
        "pdv_deposito_label": dep_boot.get("depositoLabel") or "Centro",
        "pdv_estoque_ativo_label": dep_boot.get("estoqueAtivoLabel") or "Estoque: Centro",
        "pdv_bootstrap": {
            "csrfToken": get_token(request),
            "usuarioSalvamento": u_pdv,
            "clientePadraoNome": "CONSUMIDOR NÃO IDENTIFICADO...",
            "pdvEntregaWhatsapp": getattr(settings, "PDV_ENTREGA_WHATSAPP", "") or "",
            "origensMaps": origens_maps,
            "pdvDeposito": dep_boot,
            "urls": {
                "apiBuscarProdutos": reverse("api_buscar_mobile"),
                "apiBuscarClientes": reverse("api_buscar_clientes"),
                "apiPdvClienteRapido": reverse("api_pdv_cliente_rapido"),
                "apiPdvClienteEditarPattern": reverse("api_pdv_cliente_editar", args=[0]).replace(
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
                "apiEntregaRegistrar": reverse("api_entrega_registrar"),
                "apiPdvClienteCreditoFiado": reverse("api_pdv_cliente_credito_fiado"),
                "apiPdvRelacionamentoCliente": reverse("api_pdv_relacionamento_cliente"),
                "apiPdvRelacionamentoClienteExtras": reverse("api_pdv_relacionamento_cliente_extras"),
                "apiPdvOrcamentos": reverse("api_pdv_orcamentos"),
                "apiPdvEntregasPendentes": reverse("api_pdv_entregas_pendentes"),
                "apiVendaReenviarErp": reverse("api_venda_agro_reenviar_erp", args=[0]).replace(
                    "/0/", "/__pk__/"
                ),
                "vendasLista": reverse("vendas_lista"),
                "apiPdvEntregaPendenteDetalhe": reverse("api_pdv_entrega_pendente_detalhe", args=[0]).replace(
                    "/0/", "/__pk__/"
                ),
                "apiPdvEntregaPendenteFinalizar": reverse("api_pdv_entrega_pendente_finalizar", args=[0]).replace(
                    "/0/", "/__pk__/"
                ),
                "apiPdvEntregaPendenteCancelar": reverse("api_pdv_entrega_pendente_cancelar", args=[0]).replace(
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
                "apiPromocoesAtivasPdv": reverse("api_promocoes_ativas_pdv"),
                "apiPdvProdutoEdicaoRapidaPattern": reverse(
                    "api_pdv_produto_edicao_rapida", args=["__PID__"]
                ),
                "apiPdvProdutoAjusteEstoque": reverse("api_pdv_produto_ajuste_estoque"),
                "apiProdutosGestaoOverlaySalvar": reverse("api_produtos_gestao_overlay_salvar"),
                "apiComprasRelatorioDim": reverse("api_compras_relatorio_dim_sugestao"),
                "apiPdvDeposito": reverse("api_pdv_deposito"),
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
            },
            "bairrosEntrega": {
                "urbanos": list(BAIRROS_JACUPI_URBANOS),
                "rurais": list(BAIRROS_JACUPI_RURAIS),
            },
            "pagamentoUi": {
                "mpPointEnabled": mp_point_enabled,
                "mpPointMotivoBloqueio": (
                    "Mercado Pago automático só no computador do Caixa Gaveta (aberto primeiro). "
                    "Neste PDV use Cielo, Sicredi ou Sicoob."
                    if mp_point_configurado
                    and not mp_point_nav
                    and ponto_nav == PONTO_CAIXA_NOTEBOOK
                    else (
                        "Abra o Caixa Gaveta neste computador para usar Mercado Pago automático."
                        if mp_point_configurado and not mp_point_nav
                        else ""
                    )
                ),
                "qrMercadoPagoUrl": settings.PDV_QR_MERCADOPAGO_URL,
                "qrSicrediUrl": settings.PDV_QR_SICREDI_URL,
                "chavePixSicob": settings.PDV_CHAVE_PIX_SICOB,
                "saldoValeCredito": _safe_float_ptbr(settings.PDV_WIZARD_SALDO_VALE_CREDITO, 0.0),
                "saldoCashback": _safe_float_ptbr(settings.PDV_WIZARD_SALDO_CASHBACK, 0.0),
                "maquininhasCartao": (
                    _maquininhas_cartao_effective()
                    if mp_point_enabled
                    else filtrar_maquininhas_pdv_sem_mp(_maquininhas_cartao_effective())
                ),
                "maquininhasPix": (
                    _maquininhas_pix_effective()
                    if mp_point_enabled
                    else filtrar_maquininhas_pdv_sem_mp(_maquininhas_pix_effective())
                ),
            },
            "nfce": nfce_config_resumo(),
        },
        "pdv_reabrir_from_consulta": pdv_reabrir_from_consulta,
        "agro_pdv_assets_v": getattr(settings, "AGRO_PDV_ASSETS_V", "") or (
            str(int(__import__("time").time())) if settings.DEBUG else ""
        ),
    }
    return render(request, "produtos/pdv_wizard.html", ctx)
