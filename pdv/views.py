from django.conf import settings
from django.shortcuts import render
from django.templatetags.static import static
from django.urls import reverse

from produtos.entrega_bairros_data import BAIRROS_JACUPI_RURAIS, BAIRROS_JACUPI_URBANOS
from produtos.views import _obter_sessao_caixa_aberta

_DEFAULT_MAQUININHAS_CARTAO_PDV = [
    {"id": "mp_balcao", "nome": "Mercado Pago — Balcão", "rede": "mp"},
    {"id": "sicredi_1", "nome": "Sicredi — Terminal 1", "rede": "sicredi"},
    {"id": "sicredi_2", "nome": "Sicredi — Terminal 2", "rede": "sicredi"},
]

_DEFAULT_MAQUININHAS_PIX_PDV = [
    {"id": "pix_mp_qr", "nome": "Mercado Pago — QR", "rede": "mp"},
    {"id": "pix_sicredi_qr", "nome": "Sicredi — QR", "rede": "sicredi"},
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


def pdv_home(request):
    caixa_aberto = _obter_sessao_caixa_aberta(request)
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
    ctx = {
        "caixa_aberto": caixa_aberto,
        "pdv_bootstrap": {
            "csrfToken": request.META.get("CSRF_COOKIE", "") or "",
            "clientePadraoNome": "CONSUMIDOR NÃO IDENTIFICADO...",
            "pdvEntregaWhatsapp": getattr(settings, "PDV_ENTREGA_WHATSAPP", "") or "",
            "origensMaps": origens_maps,
            "urls": {
                "apiBuscarProdutos": reverse("api_buscar_mobile"),
                "apiBuscarClientes": reverse("api_buscar_clientes"),
                "apiListCustomers": reverse("api_list_customers"),
                "apiPdvSalvarCheckoutDraft": reverse("api_pdv_salvar_checkout_draft"),
                "apiPdvLimparCheckoutDraft": reverse("api_pdv_limpar_checkout_draft"),
                "apiEnviarPedidoErp": reverse("api_enviar_pedido_erp"),
                "apiEntregaRegistrar": reverse("api_entrega_registrar"),
                "apiLoginMobile": reverse("api_login_mobile"),
                "pdvCheckout": reverse("pdv_checkout"),
                "consultaLegacy": reverse("consulta_produtos"),
                "home": reverse("home"),
                "vendasLista": reverse("vendas_lista"),
                "clientesLista": reverse("clientes_lista"),
                "clienteNovo": reverse("cliente_novo"),
                "clienteEditarPattern": reverse("cliente_editar", args=[0]).replace("/0/editar/", "/__pk__/editar/"),
                "entregasPainel": reverse("entregas_painel"),
            },
            "search": {
                "mode": "wizard",
            },
            "assets": {
                "placeholderProduto": static("img/agro-mais-logo-buscador.png"),
            },
            "caixa": {
                "aberto": bool(caixa_aberto),
                "id": caixa_aberto.pk if caixa_aberto else None,
            },
            "bairrosEntrega": {
                "urbanos": list(BAIRROS_JACUPI_URBANOS),
                "rurais": list(BAIRROS_JACUPI_RURAIS),
            },
            "pagamentoUi": {
                "qrMercadoPagoUrl": settings.PDV_QR_MERCADOPAGO_URL,
                "qrSicrediUrl": settings.PDV_QR_SICREDI_URL,
                "chavePixSicob": settings.PDV_CHAVE_PIX_SICOB,
                "saldoValeCredito": _safe_float_ptbr(settings.PDV_WIZARD_SALDO_VALE_CREDITO, 0.0),
                "saldoCashback": _safe_float_ptbr(settings.PDV_WIZARD_SALDO_CASHBACK, 0.0),
                "maquininhasCartao": _maquininhas_cartao_effective(),
                "maquininhasPix": _maquininhas_pix_effective(),
            },
        },
    }
    return render(request, "produtos/pdv_wizard.html", ctx)
