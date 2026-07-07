(function () {
    'use strict';

    var bootstrapEl =
        document.getElementById('agro-pdv-wizard-bootstrap') ||
        document.getElementById('agro-pdv-bootstrap');
    var bootstrap = {};
    try {
        bootstrap = bootstrapEl ? JSON.parse(bootstrapEl.textContent || '{}') : {};
    } catch (err) {
        bootstrap = {};
    }

    var urls = bootstrap.urls || {};
    var assets = bootstrap.assets || {};
    var pagamentoUi = bootstrap.pagamentoUi || {};
    var MSG_CAIXA_FECHADO_VENDA = 'Abra o caixa antes de registrar vendas.';

    function caixaAbertoParaVenda() {
        var cx = bootstrap.caixa || {};
        return !!(cx.aberto && cx.id);
    }

    function atualizarUiAvisoCaixa() {
        var aviso = document.getElementById('pdv-caixa-fechado-aviso');
        var link = document.getElementById('pdv-topbar-caixa-link');
        var aberto = caixaAbertoParaVenda();
        if (aviso) {
            if (aberto) {
                aviso.classList.add('hidden');
                aviso.setAttribute('aria-hidden', 'true');
            } else {
                aviso.classList.remove('hidden');
                aviso.setAttribute('aria-hidden', 'false');
            }
        }
        if (link) {
            if (aberto) {
                link.textContent = 'Caixa ' + bootstrap.caixa.id;
                link.title = 'Painel do caixa — turno aberto';
            } else {
                link.textContent = 'Caixa fechado';
                link.title = 'Caixa fechado — abra o turno para vender';
            }
        }
    }

    function refreshCaixaBootstrap() {
        var home = String(urls.pdvWizardHome || '').trim();
        if (!home) return Promise.resolve(caixaAbertoParaVenda());
        return fetch(home, { credentials: 'same-origin', headers: { Accept: 'text/html' } })
            .then(function (r) {
                return r.ok ? r.text() : '';
            })
            .then(function (html) {
                if (!html) return false;
                var doc = new DOMParser().parseFromString(html, 'text/html');
                var el = doc.getElementById('agro-pdv-wizard-bootstrap');
                if (!el) return false;
                var data = JSON.parse(el.textContent || '{}');
                if (data && data.caixa) {
                    bootstrap.caixa = data.caixa;
                    atualizarUiAvisoCaixa();
                }
                return caixaAbertoParaVenda();
            })
            .catch(function () {
                return caixaAbertoParaVenda();
            });
    }

    function ensureCaixaAbertoParaVenda() {
        if (caixaAbertoParaVenda()) return Promise.resolve(true);
        return refreshCaixaBootstrap().then(function (ok) {
            if (!ok) {
                showPdvAviso(MSG_CAIXA_FECHADO_VENDA, { title: 'Caixa fechado', tone: 'error' });
            }
            return ok;
        });
    }
    /** Trava duplo clique / Enter+F9 repetido enquanto a confirmação de venda está em andamento. */
    var isProcessingSale = false;
    var isProcessingMpTranche = false;

    var MP_POINT_WAIT_ABORT_MSG =
        'Espera cancelada.\n\nSe o valor ainda estiver na maquininha, cancele a operação no terminal também.\n\nNo PDV: em «Pagamentos lançados», use Alterar ou Excluir e tente de novo.';
    var MP_POINT_POLL_MAX = 90;
    var MP_POINT_POLL_MS = 2000;

    var mpPointWaitControl = {
        orderId: null,
        cancelRequested: false,
        cancelouMaquininha: false,
        reset: function () {
            this.orderId = null;
            this.cancelRequested = false;
            this.cancelouMaquininha = false;
        }
    };

    function mpPointWaitAbortMessage() {
        if (mpPointWaitControl.cancelouMaquininha) {
            return (
                'Cobrança cancelada no PDV e na maquininha.\n\n' +
                'Em «Pagamentos lançados», altere ou exclua e tente de novo.'
            );
        }
        return MP_POINT_WAIT_ABORT_MSG;
    }
    /** Rascunho da quantidade enquanto o operador digita (evita perder foco a cada tecla). */
    var qtyEditDraft = { id: null, raw: '' };
    var qtyInputRestore = { id: null, selStart: null, selEnd: null };
    var qtySkipCommitOnce = false;
    var priceEditDraft = { id: null, raw: '' };
    var priceInputRestore = { id: null, selStart: null, selEnd: null };
    var priceSkipCommitOnce = false;

    function pdvMpPointBeep(kind) {
        try {
            var Ctx = window.AudioContext || window.webkitAudioContext;
            if (!Ctx) return;
            var ctx = new Ctx();
            var o = ctx.createOscillator();
            var g = ctx.createGain();
            o.connect(g);
            g.connect(ctx.destination);
            if (kind === 'ok') {
                o.frequency.value = 880;
                g.gain.value = 0.12;
                o.start();
                o.stop(ctx.currentTime + 0.12);
                setTimeout(function () {
                    var o2 = ctx.createOscillator();
                    var g2 = ctx.createGain();
                    o2.connect(g2);
                    g2.connect(ctx.destination);
                    o2.frequency.value = 1175;
                    g2.gain.value = 0.1;
                    o2.start();
                    o2.stop(ctx.currentTime + 0.14);
                }, 130);
            } else {
                o.frequency.value = 220;
                g.gain.value = 0.14;
                o.start();
                o.stop(ctx.currentTime + 0.35);
            }
        } catch (eBeep) {}
    }

    function showPdvAviso(msg, opts) {
        opts = opts || {};
        var texto = String(msg || '').replace(/\n+/g, ' ').trim();
        if (!texto) return;
        showSaleDoneFeedback(texto, opts.tone || 'warn', {
            title: opts.title || 'Atenção',
            placementTop: true,
            durationMs: opts.durationMs || 12000
        });
    }

    function showMpPointInlineAviso(msg, tone) {
        var el = document.getElementById('pdv-mp-point-inline-aviso');
        if (!el) return;
        var texto = String(msg || '').trim();
        if (!texto) {
            el.textContent = '';
            el.classList.add('hidden');
            el.classList.remove('pdv-mp-point-inline-aviso--ok');
            return;
        }
        el.textContent = texto;
        el.classList.toggle('pdv-mp-point-inline-aviso--ok', tone === 'ok');
        el.classList.remove('hidden');
    }

    function showMpPointAviso(msg, opts) {
        opts = opts || {};
        var texto = opts.keepNewlines ? String(msg || '').trim() : String(msg || '').replace(/\n+/g, ' ').trim();
        if (!texto) return;
        var mpOpts = {
            title: opts.title || 'Mercado Pago',
            placementTop: !opts.prominent,
            prominent: !!opts.prominent,
            persistent: !!opts.persistent,
            keepNewlines: !!opts.keepNewlines,
            durationMs: opts.persistent ? 0 : opts.durationMs || 16000
        };
        if (opts.tone) mpOpts.tone = opts.tone;
        showSaleDoneFeedback(texto, opts.tone || 'warn', mpOpts);
    }

    function showMpPointCancelFeedback() {
        var msg = mpPointWaitAbortMessage();
        var tone = mpPointWaitControl.cancelouMaquininha ? 'ok' : 'warn';
        showMpPointInlineAviso(msg, tone);
        showMpPointAviso(msg, {
            prominent: true,
            persistent: true,
            keepNewlines: true,
            tone: mpPointWaitControl.cancelouMaquininha ? 'info' : 'warn'
        });
    }

    function setMpPointWaitStatus(text) {
        var el = document.getElementById('pdv-mp-point-wait-status');
        if (el) el.textContent = String(text || '');
    }

    function showMpPointWaitBar(amount, formaLabel) {
        var overlay = document.getElementById('pdv-mp-point-wait-overlay');
        var amtEl = document.getElementById('pdv-mp-point-wait-amount');
        var formaEl = document.getElementById('pdv-mp-point-wait-forma');
        if (amtEl && amount != null) {
            var n = State.toNumber(amount);
            amtEl.textContent = typeof formatMoney === 'function' ? formatMoney(n) : 'R$ ' + n.toFixed(2).replace('.', ',');
        }
        if (formaEl) {
            var fl = String(formaLabel || '').trim();
            formaEl.textContent = fl ? 'Forma no PDV: ' + fl : '';
            formaEl.classList.toggle('hidden', !fl);
        }
        if (overlay) overlay.classList.remove('hidden');
        document.body.classList.add('pdv-mp-point-wait-active');
        mpPointWaitControl.cancelouMaquininha = false;
        setMpPointWaitStatus('Enviando cobrança à maquininha…');
    }

    function hideMpPointWaitBar() {
        var overlay = document.getElementById('pdv-mp-point-wait-overlay');
        if (overlay) overlay.classList.add('hidden');
        document.body.classList.remove('pdv-mp-point-wait-active');
        mpPointWaitControl.reset();
    }
    var bairrosEntrega = bootstrap.bairrosEntrega || { urbanos: [], rurais: [] };
    var State = window.AgroPdvState;
    if (!State) return;

    var dom = {
        panels: document.querySelectorAll('[data-step-panel]'),
        stepNavs: document.querySelectorAll('[data-step-nav]'),
        stepHint: document.getElementById('pdv-step-hint'),
        mainFooter: document.getElementById('pdv-main-footer'),
        btnPrev: document.getElementById('pdv-btn-prev'),
        btnNext: document.getElementById('pdv-btn-next'),
        summaryClient: document.getElementById('pdv-summary-client'),
        summaryMode: document.getElementById('pdv-summary-mode'),
        summaryItems: document.getElementById('pdv-summary-items'),
        summarySubtotal: document.getElementById('pdv-summary-subtotal'),
        summaryDiscount: document.getElementById('pdv-summary-discount'),
        summaryShipping: document.getElementById('pdv-summary-shipping'),
        summaryTotal: document.getElementById('pdv-summary-total'),
        summaryDelivery: document.getElementById('pdv-summary-delivery'),
        summaryPayment: document.getElementById('pdv-summary-payment'),
        summaryNote: document.getElementById('pdv-summary-note'),
        summaryCurrentStep: document.getElementById('pdv-summary-current-step'),
        quickClientName: document.getElementById('pdv-quick-client-name'),
        quickClientMeta: document.getElementById('pdv-quick-client-meta'),
        quickClientSearch: document.getElementById('pdv-quick-client-search'),
        quickClientResults: document.getElementById('pdv-quick-client-results'),
        quickClientModal: document.getElementById('pdv-quick-client-modal'),
        quickClientPicker: document.getElementById('pdv-quick-client-picker'),
        quickClientPickerHint: document.getElementById('pdv-quick-client-picker-hint'),
        quickClientModalFechar: document.getElementById('pdv-quick-client-modal-fechar'),
        quickClientCadastrar: document.getElementById('pdv-quick-client-cadastrar'),
        quickClientEditOverlay: document.getElementById('pdv-quick-client-edit-overlay'),
        quickClientEditTitle: document.getElementById('pdv-quick-client-edit-title'),
        quickClientEditNome: document.getElementById('pdv-quick-client-edit-nome'),
        quickClientEditWhatsapp: document.getElementById('pdv-quick-client-edit-whatsapp'),
        quickClientEditCpf: document.getElementById('pdv-quick-client-edit-cpf'),
        quickClientEditLogradouro: document.getElementById('pdv-quick-client-edit-logradouro'),
        quickClientEditNumero: document.getElementById('pdv-quick-client-edit-numero'),
        quickClientEditBairro: document.getElementById('pdv-quick-client-edit-bairro'),
        quickClientEditCidade: document.getElementById('pdv-quick-client-edit-cidade'),
        quickClientEditUf: document.getElementById('pdv-quick-client-edit-uf'),
        quickClientEditCep: document.getElementById('pdv-quick-client-edit-cep'),
        quickClientEditComplemento: document.getElementById('pdv-quick-client-edit-complemento'),
        quickClientEditPluscode: document.getElementById('pdv-quick-client-edit-pluscode'),
        quickClientEditReferencia: document.getElementById('pdv-quick-client-edit-referencia'),
        quickClientEditErro: document.getElementById('pdv-quick-client-edit-erro'),
        quickClientEditSalvar: document.getElementById('pdv-quick-client-edit-salvar'),
        quickClientEditCancelar: document.getElementById('pdv-quick-client-edit-cancelar'),
        quickClientEditFechar: document.getElementById('pdv-quick-client-edit-fechar'),
        step1ClientBar: document.getElementById('pdv-step1-client-bar'),
        quickClientChange: document.getElementById('pdv-quick-client-change'),
        quickClientEditStep1: document.getElementById('pdv-quick-client-edit-step1'),
        wizardCliRapidoModal: document.getElementById('pdv-wizard-cli-rapido-modal'),
        wizardCliRapidoPanel: document.querySelector('[data-pdv-wizard-cli-rapido-panel]'),
        wizardCliRapidoNome: document.getElementById('pdv-wizard-cli-rapido-nome'),
        wizardCliRapidoWhatsapp: document.getElementById('pdv-wizard-cli-rapido-whatsapp'),
        wizardCliRapidoCpf: document.getElementById('pdv-wizard-cli-rapido-cpf'),
        wizardCliRapidoErro: document.getElementById('pdv-wizard-cli-rapido-erro'),
        wizardCliRapidoSalvar: document.getElementById('pdv-wizard-cli-rapido-salvar'),
        wizardCliRapidoCancelar: document.getElementById('pdv-wizard-cli-rapido-cancelar'),
        wizardCliRapidoFechar: document.getElementById('pdv-wizard-cli-rapido-fechar'),
        clientPurchaseHistory: document.getElementById('pdv-client-purchase-history'),
        productSearch: document.getElementById('pdv-product-search'),
        productSearchWrap: document.getElementById('pdv-product-search-wrap'),
        productSearchFeedback: document.getElementById('pdv-product-search-feedback'),
        productSearchMeta: document.getElementById('pdv-product-search-meta'),
        productAutocomplete: document.getElementById('pdv-product-autocomplete'),
        productCartList: document.getElementById('pdv-product-cart-list'),
        quickClientHit: document.getElementById('pdv-quick-client-hit'),
        productSubtotal: document.getElementById('pdv-product-subtotal'),
        productSubtotalItems: document.getElementById('pdv-product-subtotal-items'),
        productCreditBalance: document.getElementById('pdv-product-credit-balance'),
        productCashbackBalance: document.getElementById('pdv-product-cashback-balance'),
        productFiadoBalance: document.getElementById('pdv-product-fiado-balance'),
        fiadoGestaoOpen: document.getElementById('pdv-fiado-gestao-open'),
        topbarCaixaLink: document.getElementById('pdv-topbar-caixa-link'),
        topbarFiadoLink: document.getElementById('pdv-topbar-fiado-link'),
        fiadoVencidosModal: document.getElementById('pdv-fiado-vencidos-modal'),
        fiadoVencidosCliente: document.getElementById('pdv-fiado-vencidos-cliente'),
        fiadoVencidosTotal: document.getElementById('pdv-fiado-vencidos-total'),
        fiadoVencidosTbody: document.getElementById('pdv-fiado-vencidos-tbody'),
        fiadoVencidosGestao: document.getElementById('pdv-fiado-vencidos-gestao'),
        fiadoVencidosFechar: document.getElementById('pdv-fiado-vencidos-fechar'),
        productStepCount: document.getElementById('pdv-product-step-count'),
        clearItems: document.getElementById('pdv-clear-items'),
        step1Advance: document.getElementById('pdv-step1-advance'),
        step1Payment: document.getElementById('pdv-step1-payment'),
        step1BudgetVerMais: document.getElementById('pdv-step1-budget-ver-mais'),
        step1SalvarOrcamentoBtn: document.getElementById('pdv-step1-salvar-orcamento-btn'),
        topbarEntregasBtn: document.getElementById('pdv-topbar-entregas-btn'),
        topbarEntregasCount: document.getElementById('pdv-topbar-entregas-count'),
        entregasPendentesModal: document.getElementById('pdv-entregas-pendentes-modal'),
        entregasPendentesList: document.getElementById('pdv-entregas-pendentes-list'),
        entregasPendentesClose: document.getElementById('pdv-entregas-pendentes-close'),
        openBudgetHistory: document.getElementById('pdv-open-budget-history'),
        modalStart: document.getElementById('pdv-cliente-start-modal'),
        startSearchClient: document.getElementById('pdv-start-search-client'),
        startConsumidorFinal: document.getElementById('pdv-start-consumidor-final'),
        budgetHistoryModal: document.getElementById('pdv-budget-history-modal'),
        budgetHistoryList: document.getElementById('pdv-budget-history-list'),
        budgetHistoryClose: document.getElementById('pdv-budget-history-close'),
        step2ClientName: document.getElementById('pdv-step2-client-name'),
        step2ClientDoc: document.getElementById('pdv-step2-client-doc'),
        step2TelView: document.getElementById('pdv-step2-client-tel-view'),
        step2EndView: document.getElementById('pdv-step2-client-end-view'),
        step2OpenClienteEdit: document.getElementById('pdv-step2-open-cliente-edit'),
        entregaClienteNome: document.getElementById('pdv-entrega-cliente-nome'),
        entregaClienteTelefone: document.getElementById('pdv-entrega-cliente-telefone'),
        clienteTelefone: document.getElementById('pdv-cliente-telefone'),
        clienteCpf: document.getElementById('pdv-cliente-cpf'),
        clienteLogradouro: document.getElementById('pdv-cliente-logradouro'),
        clienteNumero: document.getElementById('pdv-cliente-numero'),
        clienteBairro: document.getElementById('pdv-cliente-bairro'),
        clientePluscode: document.getElementById('pdv-cliente-pluscode'),
        clienteAdvancedEdit: document.getElementById('pdv-cliente-advanced-edit-modal'),
        clienteEditModal: document.getElementById('pdv-cliente-edit-modal'),
        clienteEditClose: document.getElementById('pdv-cliente-edit-close'),
        clienteZapWhatsapp: document.getElementById('pdv-step2-cliente-zap'),
        entregaMain: document.getElementById('pdv-entrega-main'),
        entregaResumo: document.getElementById('pdv-entrega-resumo'),
        entregaWizard: document.getElementById('pdv-entrega-wizard'),
        vendaObservacao: document.getElementById('pdv-venda-observacao'),
        entregaLogradouro: document.getElementById('pdv-entrega-logradouro'),
        entregaNumero: document.getElementById('pdv-entrega-numero'),
        entregaBairro: document.getElementById('pdv-entrega-bairro'),
        entregaPluscode: document.getElementById('pdv-entrega-pluscode'),
        entregaComplemento: document.getElementById('pdv-entrega-complemento'),
        entregaReferencia: document.getElementById('pdv-entrega-referencia'),
        entregaHorario: document.getElementById('pdv-entrega-horario'),
        entregaTroco: document.getElementById('pdv-entrega-troco'),
        entregaObservacao: document.getElementById('pdv-entrega-observacao'),
        paymentMethod: document.getElementById('pdv-payment-method'),
        paymentDiscount: document.getElementById('pdv-payment-discount'),
        paymentShipping: document.getElementById('pdv-payment-shipping'),
        paymentReceived: document.getElementById('pdv-payment-received'),
        paymentChange: document.getElementById('pdv-payment-change'),
        paymentValorForma: document.getElementById('pdv-pay-valor-tranche'),
        paymentValorTotalRef: document.getElementById('pdv-payment-valor-total-ref'),
        paymentValorRestante: document.getElementById('pdv-payment-valor-restante'),
        paymentSubtotal: document.getElementById('pdv-payment-subtotal'),
        paymentDiscountView: document.getElementById('pdv-payment-discount-view'),
        paymentShippingView: document.getElementById('pdv-payment-shipping-view'),
        paymentTotal: document.getElementById('pdv-payment-total'),
        paymentPaidAccum: document.getElementById('pdv-payment-pago-acum'),
        paymentRemainingTop: document.getElementById('pdv-payment-restante-top'),
        paymentRestanteHero: document.getElementById('pdv-payment-restante-hero'),
        paymentRestanteHeroLabel: document.getElementById('pdv-payment-restante-hero-label'),
        paymentRestanteHeroVal: document.getElementById('pdv-payment-restante-hero-val'),
        paymentRestanteHeroSub: document.getElementById('pdv-payment-restante-hero-sub'),
        paymentTotalInline: document.getElementById('pdv-payment-total-inline'),
        paymentTotaisDetalhe: document.getElementById('pdv-payment-totais-detalhe'),
        paymentFormaResumo: document.getElementById('pdv-payment-forma-resumo'),
        payCommitTranche: document.getElementById('pdv-pay-commit-tranche'),
        payCommitTrancheHint: document.getElementById('pdv-pay-commit-tranche-hint'),
        payStepChips: document.getElementById('pdv-pay-step-chips'),
        paymentFeedback: document.getElementById('pdv-payment-feedback'),
        paymentLancamentosBox: document.getElementById('pdv-payment-lancamentos-box'),
        paymentLancamentosList: document.getElementById('pdv-payment-lancamentos-list'),
        confirmSaleNoPrint: document.getElementById('pdv-confirm-sale-no-print'),
        confirmSalePrint: document.getElementById('pdv-confirm-sale-print'),
        paymentModalCards: document.querySelectorAll('[data-payment-modal-card]'),
        paymentFormaModal: document.getElementById('pdv-payment-forma-modal'),
        paymentFormaModalBackdrop: document.getElementById('pdv-payment-forma-modal-backdrop'),
        paymentFormaModalClose: document.getElementById('pdv-payment-forma-modal-close'),
        btnConfirmDiscount: document.getElementById('pdv-confirm-discount'),
        btnFormaGotoDesconto: document.getElementById('pdv-payment-forma-goto-desconto'),
        btnPopConfirmDiscount: document.getElementById('pdv-pay-pop-confirm-discount'),
        btnOpenPaymentForma: document.getElementById('pdv-open-payment-forma'),
        btnTrocarPaymentForma: document.getElementById('pdv-trocar-payment-forma'),
        paymentFormaLabel: document.getElementById('pdv-payment-forma-label'),
        paymentFlowHeading: document.getElementById('pdv-payment-flow-heading'),
        paymentFlowArea: document.getElementById('pdv-payment-flow-area'),
        paymentNoFormaHint: document.getElementById('pdv-payment-no-forma-hint'),
        paymentParcelasCredito: document.getElementById('pdv-payment-parcelas'),
        flowParcelasPanel: document.getElementById('pdv-flow-parcelas'),
        fiadoParcelasInput: document.getElementById('pdv-fiado-parcelas'),
        fiadoDiasInput: document.getElementById('pdv-fiado-dias'),
        fiadoResumo: document.getElementById('pdv-fiado-resumo'),
        valeSaldoView: document.getElementById('pdv-vale-saldo-view'),
        cashbackSaldoView: document.getElementById('pdv-cashback-saldo-view'),
        pixMpQr: document.getElementById('pdv-pix-mp-qr'),
        cardSicrediLink: document.getElementById('pdv-card-sicredi-link'),
        pixSicobKey: document.getElementById('pdv-pix-sicob-key'),
        pixCopyKey: document.getElementById('pdv-pix-copy-key'),
        outroValidarPin: document.getElementById('pdv-outro-validar-pin'),
        outroPinMsg: document.getElementById('pdv-outro-pin-msg'),
        outroDetalhes: document.getElementById('pdv-outro-detalhes'),
        stepPagamentoRoot: document.getElementById('pdv-step-pagamento-root')
    };

    var lastProducts = [];
    var searchTimer = null;
    var searchClientTimer = null;
    var filterSeq = 0;
    var barcodeTimer = null;
    var lastInputAt = 0;
    /** Bloqueia atalhos +/- e F4 logo após bip (hífen do GM1546-5S não pode remover item). */
    var wizardScannerBloqueioTeclasAte = 0;
    var productSelectionIndex = -1;
    var clientListSelectIdx = -1;
    var quickClientEditPk = null;
    var quickClientEditListIdx = -1;
    var quickClientEditBairroRuralExpandido = false;
    var quickClientGeocodeTimer = null;
    var quickClientGeocodeSeq = 0;
    var quickClientGeocodeLastQ = '';
    var entregaPlusGeocodeTimer = null;
    var entregaPlusGeocodeSeq = 0;
    var entregaPlusGeocodeLastQ = '';
    var entregaClienteSnapshot = null;
    var entregaEnderecoEditadoPeloUsuario = false;
    var entregaClienteSnapshotTimer = null;
    var entregaWizardAguardandoTroco = false;
    var _ensuringEntregaModo = false;
    var entregaPendingAfterSaveCliente = null;
    var clientSearchSeq = 0;
    var lastClientSearchQuery = '';
    var PDV_CLIENTES_LS_KEY = 'agro_pdv_clientes_cache_v1';
    var ENTREGAS_PENDENTES_LS_KEY = 'agro_pdv_entregas_pendentes_v1';
    var wizardClientesCache = [];
    var wizardClientesCacheReady = false;
    var wizardClientesCacheLoading = false;
    var AUTOCOMPLETE_PAGE_SIZE = 5;
    var AUTOCOMPLETE_SCROLL_THRESHOLD = 10;
    var autocompleteVisibleLimit = AUTOCOMPLETE_PAGE_SIZE;
    var productSearchAwaitingServer = false;
    var productSearchMayHaveMore = false;
    var productSearchPointerInside = false;
    var productSearchPointerTimer = null;
    var productSearchSuppressDismissUntil = 0;
    var productSearchDismissedSnapshot = null;
    var MAX_LOCAL_RESULTS = 48;
    var CATALOG_STORAGE_KEY = 'agro_pdv_wizard_catalog_v10';
    /** Mesma chave da Consulta — sobrevive fechar o navegador. */
    var PDV_SHARED_CATALOG_LS_KEY = 'agro_pdv_catalog_cache_v2';
    var PDV_PATCH_QUEUE_KEY = 'agro_pdv_catalog_patch_queue_v1';

    function agroPdvEnqueuePatchesRespostaVenda(data) {
        var patches = data && data.pdv_catalog_patches;
        if (!patches || !patches.length) return;
        try {
            var raw = localStorage.getItem(PDV_PATCH_QUEUE_KEY);
            var q = raw ? JSON.parse(raw) : { items: [] };
            if (!q.items) q.items = [];
            patches.forEach(function (p) {
                if (p && p.id != null) q.items.push({ patch: p, at: Date.now() });
            });
            if (q.items.length > 24) q.items = q.items.slice(q.items.length - 24);
            localStorage.setItem(PDV_PATCH_QUEUE_KEY, JSON.stringify(q));
        } catch (_) {}
        patches.forEach(function (patch) {
            if (!patch || patch.id == null) return;
            var pid = String(patch.id);
            for (var i = 0; i < wizardProductCatalog.length; i++) {
                var row = wizardProductCatalog[i];
                if (String(row.id || row.Id || '') === pid) {
                    if (patch.saldo_centro != null) row.saldo_centro = patch.saldo_centro;
                    if (patch.saldo_vila != null) row.saldo_vila = patch.saldo_vila;
                }
            }
        });
    }

    var WIZARD_CATALOG_TTL_MS = 1000 * 60 * 60 * 8;
    var stagingReadonly = !!(
        bootstrap.stagingReadonly ||
        (bootstrap.search && bootstrap.search.stagingReadonly)
    );
    var wizardProductCatalog = [];
    var catalogReady = false;
    var catalogLoadPromise = null;
    var WIZARD_FOCUS_DELTA_MIN_MS = 5 * 60 * 1000;
    var wizardCatalogBootAt = 0;
    var wizardCatalogLastFocusDeltaAt = 0;
    var wizardCatalogFocusDeltaBusy = false;
    var wizardStoragePatchTimer = null;
    var prevStepCache = '';
    var entregasPendentesPollTimer = null;
    var entregasPendentesCache = { total: 0, itens: [] };
    var creditoFiadoCliente = null;
    var creditoFiadoClienteId = '';
    var fiadoVencidosAlertShownKey = '';

    function entregaPendenteApiUrl(template, pk) {
        return String(template || '').replace('__pk__', String(pk));
    }

    function escapeHtml(value) {
        var div = document.createElement('div');
        div.textContent = value == null ? '' : String(value);
        return div.innerHTML;
    }

    function stripAccents(s) {
        return String(s || '')
            .toLowerCase()
            .normalize('NFD')
            .replace(/[\u0300-\u036f]/g, '');
    }

    function onlyDigits(s) {
        return String(s || '').replace(/\D/g, '');
    }

    function displayCodigoGm(p) {
        var n = String((p && p.codigo_nfe) || '').trim();
        if (n) return n;
        var c = String((p && p.codigo) || '').trim();
        if (c) return c;
        var e = String((p && p.codigo_barras) || '').trim();
        return e || '—';
    }

    function cartCodigoGm(item) {
        var g = String((item && item.codigoGm) || '').trim();
        if (g) return g;
        return String((item && item.codigo) || '').trim() || '—';
    }

    function allowLocalQuery(q) {
        if (q.length >= 2) return true;
        return /^\d{6,}$/.test(q);
    }

    function marcarWizardScannerAtivo(ms) {
        wizardScannerBloqueioTeclasAte = Date.now() + (ms != null && ms > 0 ? ms : 1500);
    }

    function wizardScannerTeclasBloqueadas() {
        return Date.now() < wizardScannerBloqueioTeclasAte;
    }

    function pareceCodigoGmWizard(q) {
        var s = String(q || '').trim().replace(/\s/g, '');
        if (/^GMORC\d{10,20}$/i.test(s)) return false;
        return /^GM[\dA-Za-z-]{3,}$/i.test(s);
    }

    function pareceLeituraCodigoWizard(q) {
        var s = String(q || '').trim();
        if (!s) return false;
        if (/^\d{6,}$/.test(s)) return true;
        return pareceCodigoGmWizard(s);
    }

    /** Código GM/SKU em digitação — "-" não pode ser atalho de qty (ex. GM1546-5S). */
    function skuEmDigitacaoNoCampoBusca(val) {
        var v = String(val || '').trim();
        if (!v) return false;
        if (pareceCodigoGmWizard(v)) return true;
        return /^[a-zA-Z]{1,6}[\w.\-]*\d/.test(v) || (/^[a-zA-Z0-9.\-]+$/.test(v) && v.length >= 3);
    }

    function deveIgnorarAtalhoQtyBusca(val) {
        if (wizardScannerTeclasBloqueadas()) return true;
        return skuEmDigitacaoNoCampoBusca(val);
    }

    function resolveProdutoId(produto) {
        return State.resolveProdutoId ? State.resolveProdutoId(produto) : String((produto && produto.id) || '').trim();
    }

    function normalizeWizardCatalogProduct(p) {
        if (!p || typeof p !== 'object') return null;
        var id = resolveProdutoId(p);
        if (!id) return null;
        var out = Object.assign({}, p, { id: id });
        if (!out.codigo_nfe && out.codigo) out.codigo_nfe = out.codigo;
        return out;
    }

    function normalizeWizardCatalogList(arr) {
        var out = [];
        var seen = {};
        (arr || []).forEach(function (p) {
            var n = normalizeWizardCatalogProduct(p);
            if (!n || seen[n.id]) return;
            seen[n.id] = true;
            out.push(n);
        });
        return out;
    }

    function localSkuCacheSufficient(localList, ql) {
        if (!localList.length || !ql) return false;
        var prefixNoCache = countCatalogSkuPrefix(ql);
        if (prefixNoCache <= 0 || prefixNoCache !== localList.length) return false;
        return localList.every(function (p) {
            return !!resolveProdutoId(p) && productMatchesSkuPrefix(p, ql);
        });
    }

    function localTextCacheSufficient(localList) {
        return catalogReady && (localList || []).length >= AUTOCOMPLETE_PAGE_SIZE;
    }

    function finishLocalProductSearch(localList, message) {
        productSearchAwaitingServer = false;
        productSearchMayHaveMore = localList.length > AUTOCOMPLETE_PAGE_SIZE;
        renderProductResults(localList);
        dom.productSearchFeedback.textContent =
            message || 'Cache local (' + wizardProductCatalog.length + ' produtos).';
    }

    function productQueryAlnum(q) {
        return String(q || '')
            .trim()
            .toLowerCase()
            .replace(/[^a-z0-9]/g, '');
    }

    function productMatchesQueryExact(p, query) {
        var ql = String(query || '').trim().toLowerCase();
        if (!ql || !p) return false;
        var qAl = productQueryAlnum(ql);
        var fields = [p.codigo_nfe, p.codigo, p.codigo_barras];
        var i;
        for (i = 0; i < fields.length; i++) {
            var v = String(fields[i] == null ? '' : fields[i]).trim().toLowerCase();
            if (!v) continue;
            if (v === ql) return true;
            if (qAl && productQueryAlnum(v) === qAl) return true;
        }
        if (Array.isArray(p.index_codigos)) {
            for (i = 0; i < p.index_codigos.length; i++) {
                var x = String(p.index_codigos[i] == null ? '' : p.index_codigos[i]).trim().toLowerCase();
                if (!x) continue;
                if (x === ql) return true;
                if (qAl && productQueryAlnum(x) === qAl) return true;
            }
        }
        return false;
    }

    function productRowLookupCode(produto) {
        if (!produto) return '';
        return String(
            produto.codigo_nfe || produto.codigo || produto.codigo_barras || ''
        ).trim();
    }

    function findProductInListById(list, produto) {
        var pid = resolveProdutoId(produto);
        if (!pid) return null;
        var norm = normalizeWizardCatalogList(list);
        var i;
        for (i = 0; i < norm.length; i++) {
            if (resolveProdutoId(norm[i]) === pid) return norm[i];
        }
        return null;
    }

    function pickProductForQuery(list, query, opts) {
        opts = opts || {};
        var norm = normalizeWizardCatalogList(list);
        if (!norm.length) return null;
        if (opts.preferProduto) {
            var byId = findProductInListById(norm, opts.preferProduto);
            if (byId) return byId;
        }
        var ql = String(query || '').trim();
        var i;
        if (ql) {
            for (i = 0; i < norm.length; i++) {
                if (productMatchesQueryExact(norm[i], ql)) return norm[i];
            }
        }
        if (norm.length === 1) return norm[0];
        return null;
    }

    function mergeProductRowForAdd(local, remote) {
        var loc = local ? normalizeWizardCatalogProduct(local) : null;
        var rem = remote ? normalizeWizardCatalogProduct(remote) : null;
        if (!loc) return rem;
        if (!rem) return loc;
        var id = resolveProdutoId(rem) || resolveProdutoId(loc);
        return Object.assign({}, loc, rem, {
            id: id,
            nome: rem.nome || loc.nome,
            marca: rem.marca || loc.marca,
            codigo_nfe: rem.codigo_nfe || loc.codigo_nfe,
            codigo: rem.codigo || loc.codigo,
            codigo_barras: rem.codigo_barras || loc.codigo_barras,
            preco_venda: rem.preco_venda != null ? rem.preco_venda : loc.preco_venda,
            imagem: rem.imagem || loc.imagem,
            index_codigos: Array.isArray(rem.index_codigos) && rem.index_codigos.length
                ? rem.index_codigos
                : loc.index_codigos,
        });
    }

    function tryAddProductFromSearch(produto, opts) {
        opts = opts || {};
        invalidatePendingProductSearch();
        var qty = opts.qty != null ? opts.qty : 1;
        var explicitPick = !!opts.explicitPick;
        var rowCode = productRowLookupCode(produto);
        var queryHint = String(
            opts.query != null ? opts.query : (dom.productSearch && dom.productSearch.value) || ''
        ).trim();
        var forceServer = !!opts.forceServer || (!explicitPick && looksLikeSkuCode(queryHint));

        function finishOk(msg) {
            if (!opts.skipSearchUiReset) {
                resetProductSearchUi(msg || opts.okMsg || 'Item adicionado à venda.');
            }
            return true;
        }

        function tryAdd(p) {
            var row = normalizeWizardCatalogProduct(p);
            if (!row) return false;
            if (!String(row.nome || '').trim()) return false;
            return !!State.addItem(row, qty);
        }

        function failMsg(text) {
            if (dom.productSearchFeedback) dom.productSearchFeedback.textContent = text;
            return false;
        }

        if (tryAdd(produto)) {
            return Promise.resolve(finishOk());
        }

        if (!forceServer && !explicitPick && !resolveProdutoId(produto)) {
            return Promise.resolve(
                failMsg('Não foi possível adicionar — produto sem ID no cache. Atualize a página (F5).')
            );
        }

        var code = explicitPick && rowCode ? rowCode : queryHint;
        if (!code) code = rowCode;
        if (!code) {
            return Promise.resolve(
                failMsg('Não foi possível adicionar — produto sem ID no cache. Atualize a página (F5).')
            );
        }

        if (!opts.skipSearchUiReset && dom.productSearchFeedback) {
            dom.productSearchFeedback.textContent = 'Conferindo código no servidor…';
        }

        return fetchWizardServerSearch(code).then(function (srv) {
            var picked = pickProductForQuery(srv.produtos, code, { preferProduto: produto });
            if (explicitPick && produto && picked) {
                var locId = resolveProdutoId(produto);
                var pickId = resolveProdutoId(picked);
                if (locId && pickId && locId !== pickId) {
                    picked = findProductInListById(srv.produtos, produto)
                        || pickProductForQuery(srv.produtos, rowCode, { preferProduto: produto });
                }
            }
            var merged = picked ? mergeProductRowForAdd(produto, picked) : produto;
            if (tryAdd(merged)) return finishOk();
            if (picked && picked !== merged && tryAdd(picked)) return finishOk();
            if (explicitPick && tryAdd(produto)) return finishOk();
            return failMsg(
                'Não foi possível adicionar este produto. Pressione F5 e busque de novo.'
            );
        }).catch(function () {
            return failMsg('Falha ao conferir produto. Verifique a rede ou atualize a página (F5).');
        });
    }

    function lerWizardCatalogSharedCache() {
        try {
            var raw = localStorage.getItem(PDV_SHARED_CATALOG_LS_KEY);
            if (!raw) return null;
            var parsed = JSON.parse(raw);
            if (!parsed || !Array.isArray(parsed.produtos) || !parsed.produtos.length) return null;
            var age = Date.now() - Number(parsed.saved_at || 0);
            if (age > WIZARD_CATALOG_TTL_MS) return null;
            return parsed;
        } catch (err) {
            return null;
        }
    }

    function salvarWizardCatalogSharedCache(payload) {
        try {
            localStorage.setItem(
                PDV_SHARED_CATALOG_LS_KEY,
                JSON.stringify({
                    saved_at: Date.now(),
                    catalog_version: payload.catalog_version || '',
                    catalog_updated_at: payload.catalog_updated_at || '',
                    produtos: Array.isArray(payload.produtos) ? payload.produtos : wizardProductCatalog,
                })
            );
        } catch (err2) {}
        try {
            sessionStorage.setItem(
                CATALOG_STORAGE_KEY,
                JSON.stringify({ produtos: wizardProductCatalog, t: Date.now() })
            );
        } catch (err3) {}
    }

    function aplicarWizardCatalogRows(produtos, silent) {
        wizardProductCatalog = normalizeWizardCatalogList(produtos);
        catalogReady = wizardProductCatalog.length > 0;
        if (!silent && catalogReady) {
            updateSearchAwaitingPulse();
        }
        return catalogReady;
    }

    function syncWizardCatalogDelta(since, silent) {
        var u = new URL('/api/todos-produtos/delta/', window.location.origin);
        if (since) u.searchParams.set('since', since);
        return fetch(u.toString(), { credentials: 'same-origin' })
            .then(function (res) {
                return res.json();
            })
            .then(function (d) {
                if (!d) return;
                if (d.unchanged) return;
                if (d.delta && wizardProductCatalog.length) {
                    var map = {};
                    wizardProductCatalog.forEach(function (p) {
                        map[String(p.id)] = p;
                    });
                    (d.changed || []).forEach(function (row) {
                        if (!row || row.id == null) return;
                        var pid = String(row.id);
                        var prev = map[pid];
                        map[pid] = normalizeWizardCatalogProduct(
                            prev ? Object.assign({}, prev, row) : row
                        );
                    });
                    (d.removed_ids || []).forEach(function (pid) {
                        delete map[String(pid)];
                    });
                    wizardProductCatalog = normalizeWizardCatalogList(Object.keys(map).map(function (k) {
                        return map[k];
                    }));
                    catalogReady = wizardProductCatalog.length > 0;
                    salvarWizardCatalogSharedCache({
                        produtos: wizardProductCatalog,
                        catalog_version: d.catalog_version || '',
                        catalog_updated_at: d.catalog_updated_at || '',
                    });
                    return;
                }
                if (Array.isArray(d.produtos) && d.produtos.length) {
                    aplicarWizardCatalogRows(d.produtos, silent);
                    salvarWizardCatalogSharedCache(d);
                }
            })
            .catch(function () {});
    }

    function aplicarWizardPatchesProdutos(patches) {
        var rows = Array.isArray(patches) ? patches : [];
        if (!rows.length || !wizardProductCatalog.length) return false;
        var map = {};
        wizardProductCatalog.forEach(function (p) {
            map[String(p.id)] = p;
        });
        var touched = false;
        rows.forEach(function (patch) {
            if (!patch || patch.id == null) return;
            var pid = String(patch.id);
            var prev = map[pid];
            if (prev) {
                map[pid] = normalizeWizardCatalogProduct(Object.assign({}, prev, patch));
                touched = true;
            }
        });
        if (!touched) return false;
        wizardProductCatalog = normalizeWizardCatalogList(
            Object.keys(map).map(function (k) {
                return map[k];
            })
        );
        catalogReady = wizardProductCatalog.length > 0;
        return true;
    }

    function agroWizardAplicarFilaPatchLocal(clearQueue) {
        if (clearQueue === undefined) clearQueue = true;
        if (!wizardProductCatalog.length) return false;
        var items = [];
        try {
            var raw = localStorage.getItem(PDV_PATCH_QUEUE_KEY);
            if (raw) {
                var q = JSON.parse(raw);
                if (q && Array.isArray(q.items)) items = q.items;
            }
        } catch (_) {}
        if (!items.length) return false;
        var patches = items
            .map(function (it) {
                return it && it.patch ? it.patch : it;
            })
            .filter(function (p) {
                return p && p.id != null;
            });
        if (!patches.length) return false;
        var ok = aplicarWizardPatchesProdutos(patches);
        if (ok && clearQueue) {
            try {
                localStorage.removeItem(PDV_PATCH_QUEUE_KEY);
            } catch (_) {}
        }
        return ok;
    }

    function agroWizardCatalogoRefreshNoFoco() {
        if (document.hidden || !wizardProductCatalog.length) return;
        if (wizardCatalogBootAt && Date.now() - wizardCatalogBootAt < 2500) return;

        agroWizardAplicarFilaPatchLocal(true);

        var now = Date.now();
        if (now - wizardCatalogLastFocusDeltaAt < WIZARD_FOCUS_DELTA_MIN_MS) return;
        if (wizardCatalogFocusDeltaBusy) return;
        wizardCatalogFocusDeltaBusy = true;
        wizardCatalogLastFocusDeltaAt = now;
        var shared = lerWizardCatalogSharedCache();
        syncWizardCatalogDelta(shared && shared.catalog_version ? shared.catalog_version : '', true).finally(
            function () {
                wizardCatalogFocusDeltaBusy = false;
            }
        );
    }

    function fetchWizardCatalogFallback() {
        var url = (urls.apiBuscarProdutos || '/api/buscar/') + '?wizard=1&wizard_catalog=1';
        return fetch(url, { credentials: 'same-origin' })
            .then(function (res) {
                return res.text().then(function (text) {
                    if (!res.ok) {
                        var hint = (text || '').trim().slice(0, 200);
                        throw new Error(
                            'servidor HTTP ' + res.status + (hint ? ' — ' + hint : '')
                        );
                    }
                    try {
                        return JSON.parse(text);
                    } catch (eJson2) {
                        throw new Error('resposta do catálogo não é JSON válido (dados corrompidos no servidor?)');
                    }
                });
            })
            .then(function (data) {
                aplicarWizardCatalogRows(Array.isArray(data.produtos) ? data.produtos : [], false);
                salvarWizardCatalogSharedCache({
                    produtos: wizardProductCatalog,
                    catalog_version: data.catalog_version || '',
                    catalog_updated_at: data.catalog_updated_at || '',
                });
            });
    }

    function pdvCatalogBootShow() {
        if (window.AgroPdvCatalogSplash) window.AgroPdvCatalogSplash.show();
        else if (window.gmLoader) window.gmLoader.show('🐭 carregando catálogo...');
        else if (window.gmLoadingBar) window.gmLoadingBar.show();
    }

    function pdvCatalogBootHide() {
        if (window.AgroPdvCatalogSplash) window.AgroPdvCatalogSplash.hide(0);
        else if (window.gmLoader) window.gmLoader.hide(180);
        else if (window.gmLoadingBar) window.gmLoadingBar.hide();
    }

    function loadWizardCatalog() {
        if (catalogReady) return Promise.resolve();
        if (!wizardCatalogBootAt) wizardCatalogBootAt = Date.now();
        if (catalogLoadPromise) {
            pdvCatalogBootShow();
            return catalogLoadPromise;
        }

        try {
            var raw = sessionStorage.getItem(CATALOG_STORAGE_KEY);
            if (raw) {
                var parsed = JSON.parse(raw);
                if (parsed && Array.isArray(parsed.produtos) && parsed.produtos.length) {
                    if (aplicarWizardCatalogRows(parsed.produtos, true)) {
                        var shared = lerWizardCatalogSharedCache();
                        syncWizardCatalogDelta(shared && shared.catalog_version ? shared.catalog_version : '');
                        return Promise.resolve();
                    }
                }
            }
        } catch (err) {}

        var sharedCache = lerWizardCatalogSharedCache();
        if (sharedCache && sharedCache.produtos.length) {
            aplicarWizardCatalogRows(sharedCache.produtos, true);
            syncWizardCatalogDelta(sharedCache.catalog_version || '');
            return Promise.resolve();
        }

        var urlDelta = new URL('/api/todos-produtos/delta/', window.location.origin);
        pdvCatalogBootShow();
        catalogLoadPromise = fetch(urlDelta.toString(), { credentials: 'same-origin' })
            .then(function (res) {
                return res.text().then(function (text) {
                    if (!res.ok) {
                        throw new Error('delta HTTP ' + res.status);
                    }
                    try {
                        return JSON.parse(text);
                    } catch (eJson) {
                        throw new Error('delta JSON inválido');
                    }
                });
            })
            .then(function (d) {
                if (d && Array.isArray(d.produtos) && d.produtos.length) {
                    aplicarWizardCatalogRows(d.produtos, false);
                    salvarWizardCatalogSharedCache(d);
                    return;
                }
                return fetchWizardCatalogFallback();
            })
            .catch(function () {
                return fetchWizardCatalogFallback();
            })
            .finally(function () {
                catalogLoadPromise = null;
                updateSearchAwaitingPulse();
                pdvCatalogBootHide();
            });
        return catalogLoadPromise;
    }

    function matchQueryAgainstIndexCodigos(qt, qd, p) {
        if (!Array.isArray(p.index_codigos) || !p.index_codigos.length) return false;
        var ql = String(qt || '').trim().toLowerCase();
        if (!ql) return false;
        for (var i = 0; i < p.index_codigos.length; i++) {
            var x = String(p.index_codigos[i] == null ? '' : p.index_codigos[i]).trim();
            if (!x) continue;
            var xl = x.toLowerCase();
            if (xl === ql) return true;
            if (ql.length >= 3 && xl.indexOf(ql) === 0) return true;
            if (qd.length >= 6 && onlyDigits(x) === qd) return true;
        }
        return false;
    }

    function looksLikeSkuCode(q) {
        var s = String(q || '').trim();
        if (!s || /\s/.test(s)) return false;
        if (/^\d{6,}$/.test(onlyDigits(s))) return true;
        return /^[a-zA-Z]{1,6}[\w.\-]*\d/i.test(s);
    }

    function productMatchesSkuPrefix(p, ql) {
        if (!p || !ql || ql.length < 2) return false;
        var cod = stripAccents(String(p.codigo || '').trim().toLowerCase());
        var nfe = stripAccents(String(p.codigo_nfe || '').trim().toLowerCase());
        if ((cod && cod.indexOf(ql) === 0) || (nfe && nfe.indexOf(ql) === 0)) return true;
        if (!Array.isArray(p.index_codigos)) return false;
        for (var i = 0; i < p.index_codigos.length; i++) {
            var xs = String(p.index_codigos[i] == null ? '' : p.index_codigos[i]).trim().toLowerCase();
            if (xs && xs.indexOf(ql) === 0) return true;
        }
        return false;
    }

    function countCatalogSkuPrefix(ql) {
        var n = 0;
        wizardProductCatalog.forEach(function (p) {
            if (productMatchesSkuPrefix(p, ql)) n++;
        });
        return n;
    }

    function mergeProductsById(primary, extra) {
        var seen = {};
        var out = [];
        function addList(arr) {
            normalizeWizardCatalogList(arr || []).forEach(function (p) {
                if (seen[p.id]) return;
                seen[p.id] = true;
                out.push(p);
            });
        }
        addList(primary);
        addList(extra);
        return out;
    }

    function findUniqueBarcodeMatch(q) {
        var qt = String(q || '').trim();
        if (!qt) return null;
        var qd = onlyDigits(qt);
        var seen = {};
        var hits = [];
        var eanOnly = qd.length >= 8;
        wizardProductCatalog.forEach(function (p) {
            var ean = String(p.codigo_barras || '').trim();
            var nfe = String(p.codigo_nfe || '').trim();
            var cod = String(p.codigo || '').trim();
            var match = false;
            if (qt && (ean === qt || nfe === qt || cod === qt)) match = true;
            else if (productMatchesQueryExact(p, qt)) match = true;
            else if (qd.length >= 6 && onlyDigits(ean) && onlyDigits(ean) === qd) match = true;
            else if (qd.length >= 6 && onlyDigits(nfe) && onlyDigits(nfe) === qd) match = true;
            else if (!eanOnly && matchQueryAgainstIndexCodigos(qt, qd, p)) match = true;
            if (match) {
                var id = resolveProdutoId(p);
                if (id && !seen[id]) {
                    seen[id] = true;
                    hits.push(p);
                }
            }
        });
        return hits.length === 1 ? hits[0] : null;
    }

    function scoreProduct(p, qRaw, barcodeMode) {
        var q = stripAccents(qRaw.trim()).toLowerCase();
        if (!q) return 0;
        var nome = stripAccents(p.nome || '').toLowerCase();
        var marca = stripAccents(p.marca || '').toLowerCase();
        var buscaTxt = stripAccents(p.busca_texto || '').toLowerCase();
        var nfe = stripAccents(String(p.codigo_nfe || '')).toLowerCase();
        var cod = stripAccents(String(p.codigo || '')).toLowerCase();
        var ean = stripAccents(String(p.codigo_barras || '').replace(/\s/g, '')).toLowerCase();
        var qDigits = onlyDigits(q);
        var eanD = onlyDigits(ean);

        var score = 0;
        if (nfe === q || cod === q || ean === q) score += 2500;
        if (qDigits.length >= 6 && eanD && eanD === qDigits) score += 2400;
        if (matchQueryAgainstIndexCodigos(qRaw, qDigits, p)) score += 2600;
        if (barcodeMode) {
            if (ean.indexOf(q) !== -1 || nfe.indexOf(q) !== -1 || cod.indexOf(q) !== -1) score += 500;
        }
        if (nfe.indexOf(q) === 0) score += 900;
        else if (nfe.indexOf(q) !== -1) score += 450;
        if (cod.indexOf(q) === 0) score += 850;
        else if (cod.indexOf(q) !== -1) score += 400;
        if (ean && q && ean.indexOf(q) === 0) score += 880;
        else if (ean && q && ean.indexOf(q) !== -1) score += 420;

        if (nome.indexOf(q) !== -1) score += 200;
        if (buscaTxt && buscaTxt.indexOf(q) !== -1) score += 220;

        var tokens = q.split(/\s+/).filter(function (t) { return t.length > 0; });
        if (tokens.length > 1) {
            var allIn = tokens.every(function (t) { return nome.indexOf(t) !== -1; });
            if (allIn) score += 320;
            tokens.forEach(function (t) {
                if (nome.split(/\s+/).some(function (w) { return w.indexOf(t) === 0; })) score += 70;
            });
            var hitPart = 0;
            tokens.forEach(function (t) {
                if (t.length < 2) return;
                if (nome.indexOf(t) !== -1 || (marca && marca.indexOf(t) !== -1)) hitPart++;
            });
            if (hitPart > 0) score += hitPart * 130;
        } else if (tokens.length === 1 && tokens[0].length >= 2) {
            var t0 = tokens[0];
            if (nome.indexOf(t0) === 0) score += 180;
            else if (nome.split(/\s+/).some(function (w) { return w.indexOf(t0) === 0; })) score += 120;
        }

        if (marca && marca.indexOf(q) !== -1) score += 90;
        return score;
    }

    function filterCatalogLocal(query, mode) {
        var q = String(query || '').trim();
        if (!allowLocalQuery(q)) {
            return { list: [], message: 'Digite ao menos 2 letras ou 6+ dígitos do código.' };
        }
        var barcodeMode = mode === 'barcode';
        if (barcodeMode) {
            var oneBc = findUniqueBarcodeMatch(q);
            if (oneBc) return { list: [], barcodeHit: oneBc, message: '' };
        }
        var qDigitsOnly = onlyDigits(q);
        // EAN 8+ dígitos: busca no servidor (catálogo local pode não ter o item).
        if (qDigitsOnly.length >= 8 && !barcodeMode) {
            return { list: [], message: '', barcodeHit: null };
        }
        if (qDigitsOnly.length >= 8 && barcodeMode) {
            return { list: [], message: '', barcodeHit: null };
        }
        var scored = wizardProductCatalog
            .map(function (p) {
                return { p: p, s: scoreProduct(p, q, barcodeMode) };
            })
            .filter(function (x) { return x.s > 0; })
            .sort(function (a, b) {
                if (b.s !== a.s) return b.s - a.s;
                return stripAccents(a.p.nome || '').localeCompare(stripAccents(b.p.nome || ''));
            })
            .map(function (x) { return x.p; })
            .slice(0, MAX_LOCAL_RESULTS);
        return {
            list: scored,
            message: scored.length ? '' : 'Nenhum produto no cache para este termo.'
        };
    }

    function fetchWizardServerSearch(query) {
        var u =
            (urls.apiBuscarProdutos || '/api/buscar/') + '?wizard=1&q=' + encodeURIComponent(String(query || '').trim());
        return fetch(u, { credentials: 'same-origin' })
            .then(function (res) {
                if (!res.ok) throw new Error('search_http');
                return res.json();
            })
            .then(function (data) {
                return {
                    produtos: Array.isArray(data.produtos) ? data.produtos : [],
                    exactBarcode: !!data.exact_barcode_match,
                };
            });
    }

    function tryAutoAddBarcodeHit(product, message) {
        if (!product) return false;
        if (!String(product.nome || '').trim()) return false;
        invalidatePendingProductSearch();
        marcarWizardScannerAtivo(1500);
        var q = dom.productSearch ? String(dom.productSearch.value || '').trim() : '';
        if (State.addItem(product, 1)) {
            resetProductSearchUi(message || 'Item adicionado pela leitura do código.');
            return true;
        }
        tryAddProductFromSearch(product, {
            okMsg: message || 'Item adicionado pela leitura do código.',
            query: q,
            forceServer: true,
        });
        return false;
    }

    function whatsappHrefLoose(raw) {
        var d = String(raw || '').replace(/\D/g, '');
        if (!d) return '';
        if (d.indexOf('55') === 0) return 'https://wa.me/' + d;
        return 'https://wa.me/55' + d;
    }

    function formatMoney(value) {
        return new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(Number(value || 0));
    }

    function formatQty(value) {
        return State.formatQtyDisplay ? State.formatQtyDisplay(value) : String(value == null ? '' : value);
    }

    function formatPriceEdit(value) {
        return State.formatPriceDisplay ? State.formatPriceDisplay(value) : formatMoney(value).replace(/^R\$\s*/, '').trim();
    }

    function lineSubtotal(item) {
        return State.toNumber(item.qtd) * State.toNumber(item.preco);
    }

    function restorePriceInputDisplay(input) {
        if (!input) return;
        var id = input.getAttribute('data-item-price-input');
        if (!id) return;
        var item = State.getState().itens.find(function (it) {
            return String(it.id) === String(id);
        });
        if (!item) return;
        input.value = formatPriceEdit(lineSubtotal(item));
        input.setAttribute('aria-label', 'Total da linha');
        input.title = 'Toque para alterar o preço unitário';
    }

    function applyQtyDelta(itemId, direction) {
        var current = State.getState().itens.find(function (item) {
            return String(item.id) === String(itemId);
        });
        if (!current) return;
        qtyEditDraft = { id: null, raw: '' };
        var step = State.qtyStepFor ? State.qtyStepFor(current.qtd) : 1;
        var nextQty = State.toNumber(current.qtd) + direction * step;
        if (nextQty < (State.QTD_MIN || 0.001)) {
            State.removeItem(itemId);
        } else {
            State.updateItemQuantity(itemId, nextQty);
        }
    }

    function commitQtyInput(input) {
        if (!input) return;
        var id = input.getAttribute('data-item-qty-input');
        if (!id) return;
        var parsed = State.normalizeQty ? State.normalizeQty(input.value, null) : State.toNumber(input.value);
        qtyEditDraft = { id: null, raw: '' };
        qtyInputRestore = { id: null, selStart: null, selEnd: null };
        if (!parsed) {
            State.removeItem(id);
            return;
        }
        var current = State.getState().itens.find(function (item) {
            return String(item.id) === String(id);
        });
        if (current && Math.abs(State.toNumber(current.qtd) - parsed) < 0.0000001) return;
        State.updateItemQuantity(id, parsed);
    }

    function commitPriceInput(input) {
        if (!input) return;
        var id = input.getAttribute('data-item-price-input');
        if (!id) return;
        var parsed = State.normalizePrice ? State.normalizePrice(input.value, null) : State.toNumber(input.value);
        priceEditDraft = { id: null, raw: '' };
        priceInputRestore = { id: null, selStart: null, selEnd: null };
        if (!parsed) {
            restorePriceInputDisplay(input);
            return;
        }
        var cur = State.getState().itens.find(function (item) {
            return String(item.id) === String(id);
        });
        if (cur && Math.abs(State.toNumber(cur.preco) - parsed) < 0.0000001) {
            restorePriceInputDisplay(input);
            return;
        }
        State.updateItemPrice(id, parsed);
    }

    function compactText(value, fallback) {
        var txt = String(value || '').trim();
        return txt || fallback || '—';
    }

    function buildLinhaEnderecoEntrega(state) {
        var e = state.entrega || {};
        var c = state.cliente || {};
        var log = String(e.logradouro || c.logradouro || '').trim();
        var num = String(e.numero || c.numero || '').trim();
        var bai = String(e.bairro || c.bairro || '').trim();
        var pc = String(e.plusCode || c.plus_code || '').trim();
        var parts = [];
        if (log || num) {
            var ln = [log, num].filter(Boolean).join(', ');
            if (ln) parts.push(ln);
        }
        if (bai) parts.push(bai);
        if (pc) parts.push('Plus ' + pc);
        if (parts.length) return parts.join(' — ') + ' — Jacupiranga/SP';
        return compactText(e.endereco || composeClienteEnderecoLinha(c) || c.endereco || '', '');
    }

    function composeEndereco(state) {
        var structured = buildLinhaEnderecoEntrega(state);
        var parts = [];
        var enderecoBase =
            structured ||
            composeClienteEnderecoLinha(state.cliente) ||
            compactText(
                (state.entrega && state.entrega.endereco) ||
                    (state.cliente && state.cliente.endereco),
                ''
            );
        if (enderecoBase) parts.push(enderecoBase);
        if (state.entrega.complemento) parts.push(state.entrega.complemento);
        if (state.entrega.referencia) parts.push('Ref.: ' + state.entrega.referencia);
        return parts.join(' • ');
    }

    function syncEntregaEnderecoFromCliente(st) {
        st = st || State.getState();
        var c = st.cliente || {};
        var patch = {
            logradouro: String(c.logradouro || '').trim(),
            numero: String(c.numero || '').trim(),
            bairro: String(c.bairro || '').trim(),
            plusCode: String(c.plus_code || '').trim(),
            complemento: '',
            referencia: String(c.referencia_rural || '').trim()
        };
        var line = buildLinhaEnderecoEntrega({ entrega: patch, cliente: c });
        State.setEntregaPatch({
            logradouro: patch.logradouro,
            numero: patch.numero,
            bairro: patch.bairro,
            plusCode: patch.plusCode,
            complemento: patch.complemento,
            referencia: patch.referencia,
            endereco: line,
            enderecoPassoConcluido: false
        });
        entregaPlusGeocodeLastQ = String(patch.plusCode || '').trim();
        resetEntregaClienteSnapshot();
    }

    function composeClienteEnderecoLinha(c) {
        if (!c || typeof c !== 'object') return '';
        var log = String(c.logradouro || '').trim();
        var num = String(c.numero || '').trim();
        var bai = String(c.bairro || '').trim();
        var pc = String(c.plus_code || '').trim();
        var parts = [];
        if (log || num) {
            var ln = [log, num].filter(Boolean).join(', ');
            if (ln) parts.push(ln);
        }
        if (bai) parts.push(bai);
        if (pc) parts.push('Plus ' + pc);
        if (parts.length) return parts.join(' — ') + ' — Jacupiranga/SP';
        return String(c.endereco || '').trim();
    }

    function destinoQueryParaMaps(state) {
        var e = state.entrega || {};
        var c = state.cliente || {};
        var pc = String(e.plusCode || c.plus_code || '').trim();
        if (pc) return pc;
        var linha = buildLinhaEnderecoEntrega(state);
        if (linha) return linha;
        return compactText(e.endereco || c.endereco || '', '');
    }

    function shortNote(state) {
        return compactText(state.pagamento.observacaoFinal || state.venda.observacao || state.entrega.observacao, 'Sem observações até o momento.');
    }

    function currentClientName(state) {
        if (state.cliente && state.cliente.nome) return state.cliente.nome;
        return bootstrap.clientePadraoNome || 'CONSUMIDOR NÃO IDENTIFICADO...';
    }

    function clienteComSaldoAgro(state) {
        return (
            state &&
            state.clienteMode !== 'consumidor_final' &&
            state.cliente &&
            state.cliente.cliente_agro_pk != null
        );
    }

    function saldoValeAtual(state) {
        if (clienteComSaldoAgro(state)) {
            if (
                creditoFiadoCliente &&
                clienteFiadoQueryKey(state) === creditoFiadoClienteId &&
                creditoFiadoCliente.saldo_vale_credito != null
            ) {
                return State.toNumber(creditoFiadoCliente.saldo_vale_credito);
            }
            return State.toNumber(state.cliente.saldo_vale_credito);
        }
        return State.toNumber(pagamentoUi.saldoValeCredito || 0);
    }

    function saldoCashbackAtual(state) {
        if (clienteComSaldoAgro(state)) {
            if (
                creditoFiadoCliente &&
                clienteFiadoQueryKey(state) === creditoFiadoClienteId &&
                creditoFiadoCliente.saldo_cashback != null
            ) {
                return State.toNumber(creditoFiadoCliente.saldo_cashback);
            }
            return State.toNumber(state.cliente.saldo_cashback);
        }
        return State.toNumber(pagamentoUi.saldoCashback || 0);
    }

    function whatsappHrefCliente(telefone) {
        var d = String(telefone || '').replace(/\D/g, '');
        if (d.length < 10) return '';
        if (d.length <= 11) d = '55' + d;
        return 'https://wa.me/' + d;
    }

    function flowIndex(flow, step) {
        return flow.indexOf(step);
    }

    function nextStep(state, computed) {
        var flow = computed.flow;
        var idx = flowIndex(flow, state.currentStep);
        return idx >= 0 && idx < flow.length - 1 ? flow[idx + 1] : null;
    }

    function prevStep(state, computed) {
        var flow = computed.flow;
        var idx = flowIndex(flow, state.currentStep);
        return idx > 0 ? flow[idx - 1] : null;
    }

    function totalNumberFromComputed(computed) {
        return Number(computed.total || 0);
    }

    function sumValorLancamentos(state) {
        var arr = (state.pagamento && state.pagamento.lancamentos) || [];
        var s = 0;
        arr.forEach(function (L) {
            s += State.toNumber(L && L.valor);
        });
        return s;
    }

    function saldoRestantePagamento(state, computed) {
        return Math.max(0, totalNumberFromComputed(computed) - sumValorLancamentos(state));
    }

    function effectiveValorDestaForma(state, computed) {
        var total = totalNumberFromComputed(computed);
        var raw = String((state.pagamento && state.pagamento.valorDestaForma) || '').trim();
        if (!raw) return total;
        var v = State.toNumber(raw);
        if (!Number.isFinite(v) || v <= 0) return total;
        return v;
    }

    function maquinaRedeClass(item) {
        var r = String((item && item.rede) || '').toLowerCase();
        var id = String((item && item.id) || '').toLowerCase();
        if (r === 'mp' || id.indexOf('mp_') === 0 || id.indexOf('mercado') === 0 || id.indexOf('pix_mp') === 0)
            return 'pdv-pay-maquina-card-mp';
        if (r === 'sicredi' || id.indexOf('sicredi') === 0 || id.indexOf('pix_sicredi') === 0)
            return 'pdv-pay-maquina-card-sicredi';
        if (r === 'cielo' || id.indexOf('cielo') === 0)
            return 'pdv-pay-maquina-card-cielo';
        if (r === 'sicoob' || id.indexOf('sicoob') === 0 || id.indexOf('pix_sicoob') === 0)
            return 'pdv-pay-maquina-card-sicoob';
        return 'pdv-pay-maquina-card-outro';
    }

    function afterCommitTrancheFlow() {
        setTimeout(function () {
            var inp = document.getElementById('pdv-pay-valor-tranche');
            if (inp) inp.value = '';
            var st = State.getState();
            var comp = State.getComputed();
            var rest = saldoRestantePagamento(st, comp);
            if (rest > 0.009) {
                openPaymentFormaModal();
            } else {
                var n = document.getElementById('pdv-confirm-sale-no-print');
                if (n) n.focus();
            }
        }, 0);
    }

    function erroCommitTranche(state, computed, T) {
        var forma = state.pagamento.forma || '';
        var rest = saldoRestantePagamento(state, computed);
        if (T <= 0.009) return 'Informe um valor maior que zero.';
        if (T > rest + 0.009) return 'Valor acima do restante (' + formatMoney(rest) + ').';
        if (requiresMaquina(forma) && !String(state.pagamento.maquinaId || '').trim()) {
            return 'Selecione a máquina (Pix ou cartão).';
        }
        if (forma === 'Cartão de crédito parcelado') {
            var par = parseInt(state.pagamento.creditoParcelas, 10) || 0;
            if (par < 2) return 'Informe 2 ou mais parcelas.';
        }
        if (forma === 'Fiado') {
            var msgFi = validarFiadoPermitido(state);
            if (msgFi) return msgFi;
            var fp = parseInt(state.pagamento.fiadoParcelas, 10) || 0;
            var fd = parseInt(state.pagamento.fiadoDiasVencimento, 10) || 0;
            if (fp < 1 || fp > 6) return 'Fiado: use de 1 a 6 parcelas.';
            if (fd < 1) return 'Fiado: prazo em dias inválido.';
            var vFiado = State.toNumber(state.pagamento.valorDestaForma);
            if (!(vFiado > 0.009)) vFiado = T - sumValorLancamentos(state);
            if (creditoFiadoCliente && creditoFiadoCliente.excede) {
                return (
                    'Fiado acima do limite. Disponível ' +
                    (creditoFiadoCliente.disponivel_texto || '') +
                    '.'
                );
            }
        }
        if (forma === 'Vale crédito') {
            var sv = saldoValeAtual(state);
            if (sv <= 0) return 'Sem saldo de vale crédito configurado.';
            if (T > sv + 0.009) return 'Valor acima do saldo do vale.';
        }
        if (forma === 'Cashback') {
            var sc = saldoCashbackAtual(state);
            if (sc <= 0) return 'Sem saldo de cashback configurado.';
            if (T > sc + 0.009) return 'Valor acima do saldo de cashback.';
        }
        if (forma === 'Outro') {
            if (!state.pagamento.outroPinVerificado) return 'Valide o PIN do operador em “Outro”.';
            if (!String(state.pagamento.outroDetalhes || '').trim()) return 'Descreva o pagamento em “Outro”.';
        }
        return '';
    }

    function snapshotLancamentoFromState(state, T, dinheiroExtra) {
        dinheiroExtra = dinheiroExtra || {};
        var forma = state.pagamento.forma || '';
        var mid = String(state.pagamento.maquinaId || '').trim();
        var mpModo = isMaquinaMpPointAuto(mid, forma) ? 'point' : '';
        return {
            forma: forma,
            valor: T,
            maquinaId: state.pagamento.maquinaId || '',
            maquinaNome: state.pagamento.maquinaNome || '',
            mpBalcaoModo: mpModo,
            cobrarNoPointMp: mpModo === 'point',
            creditoParcelas: forma === 'Cartão de crédito parcelado' ? parseInt(state.pagamento.creditoParcelas, 10) || 2 : null,
            fiadoParcelas: forma === 'Fiado' ? parseInt(state.pagamento.fiadoParcelas, 10) || 1 : null,
            fiadoDiasVencimento: forma === 'Fiado' ? parseInt(state.pagamento.fiadoDiasVencimento, 10) || 30 : null,
            valorRecebido: dinheiroExtra.valorRecebido || '',
            trocoCalculado: dinheiroExtra.trocoCalculado || '',
            outroDetalhes: forma === 'Outro' ? String(state.pagamento.outroDetalhes || '').trim() : '',
            mpPointOrderId: '',
            mpPointPago: false
        };
    }

    function lancamentoSnapParaErpRow(L, state) {
        state = state || State.getState();
        var fn = lancamentoFormaErpLabel(L);
        var v = State.toNumber(L.valor);
        if (!fn && !(v > 0.0001)) return null;
        if (!fn) fn = 'Não informado';
        var row = {
            formaPagamento: fn.slice(0, 200),
            valorPagamento: Math.round((v + Number.EPSILON) * 100) / 100,
            quitar: fn !== 'Fiado'
        };
        if (L.forma === 'Cartão de crédito parcelado') {
            row.creditoParcelas = Math.min(
                24,
                Math.max(2, parseInt(L.creditoParcelas, 10) || parseInt(state.pagamento.creditoParcelas, 10) || 2)
            );
        }
        var midL = String(L.maquinaId || '').trim();
        if (midL) {
            row.maquinaId = midL;
            if (L.maquinaNome) row.maquinaNome = String(L.maquinaNome).slice(0, 120);
            if (L.mpBalcaoModo) row.mpBalcaoModo = String(L.mpBalcaoModo);
            if (L.cobrarNoPointMp) row.cobrarNoPointMp = true;
        }
        return row;
    }

    function buildErpPayloadParaTrancheMp(state, computed, trancheValor) {
        var snap = snapshotLancamentoFromState(state, trancheValor);
        var payload = buildErpPayload(state, computed);
        var row = lancamentoSnapParaErpRow(snap, state);
        payload.pagamentos = row ? [row] : [];
        payload.valor_cobranca_tranche = Math.round((trancheValor + Number.EPSILON) * 100) / 100;
        return payload;
    }

    function deveCobrarMpPointNaTranche(state, trancheValor) {
        if (!pagamentoUi.mpPointEnabled || !String(urls.apiPdvMpPointCriar || '').trim()) return false;
        if (!(trancheValor > 0.009)) return false;
        var forma = String(state.pagamento.forma || '').trim();
        var mid = String(state.pagamento.maquinaId || '').trim();
        return isMaquinaMpPointAuto(mid, forma);
    }

    function aplicarReconMpPointNoSnap(snap, data) {
        if (!snap || !data) return snap;
        var formaMp = String(data.mp_point_forma_confirmada || '').trim();
        if (formaMp) {
            snap.forma = formaMp;
            var low = formaMp.toLowerCase();
            if (low.indexOf('parcelado') >= 0) {
                var m = formaMp.match(/(\d+)\s*x/i);
                if (m) snap.creditoParcelas = parseInt(m[1], 10) || snap.creditoParcelas;
            }
        }
        return snap;
    }

    function mpPointOrderIdsFromLancamentos(state) {
        var ids = [];
        (state.pagamento.lancamentos || []).forEach(function (L) {
            if (L.mpPointOrderId && L.mpPointPago) ids.push(String(L.mpPointOrderId));
        });
        return ids;
    }

    function vendaPrecisaFinalizarMpPoint(state) {
        return mpPointOrderIdsFromLancamentos(state).length > 0;
    }

    function finishMpTrancheBusy() {
        isProcessingMpTranche = false;
        var trancheInp = document.getElementById('pdv-pay-valor-tranche');
        if (trancheInp) trancheInp.disabled = false;
    }

    function cobrarMpPointNaTranche(st, comp, cur) {
        if (isProcessingMpTranche) return;
        isProcessingMpTranche = true;
        showMpPointInlineAviso('');
        var trancheInp = document.getElementById('pdv-pay-valor-tranche');
        if (trancheInp) trancheInp.disabled = true;

        ensureCaixaAbertoParaVenda()
            .then(function (caixaOk) {
                if (!caixaOk) {
                    finishMpTrancheBusy();
                    return;
                }
                var payload = buildErpPayloadParaTrancheMp(st, comp, cur);
                var formaWait = lancamentoFormaErpLabel(snapshotLancamentoFromState(st, cur));
                return jsonPost(urls.apiPdvSalvarCheckoutDraft, buildCheckoutDraftPayload(st, comp)).then(function (draftRes) {
                    if (!draftRes.ok || !draftRes.data.ok) {
                        throw new Error(
                            (draftRes.data && (draftRes.data.erro || draftRes.data.mensagem)) ||
                                'Falha ao salvar rascunho.'
                        );
                    }
                    return jsonPost(urls.apiPdvMpPointCriar, payload);
                }).then(function (criarRes) {
                    if (!criarRes.ok || !criarRes.data.ok) {
                        throw new Error((criarRes.data && criarRes.data.erro) || 'Falha ao enviar valor ao terminal MP.');
                    }
                    var oid = criarRes.data.order_id;
                    if (!oid) throw new Error('Resposta sem order_id.');
                    mpPointWaitControl.cancelRequested = false;
                    mpPointWaitControl.orderId = oid;
                    showMpPointWaitBar(
                        criarRes.data.amount != null ? criarRes.data.amount : cur,
                        formaWait
                    );
                    return pollMpPointUntilPaid(oid);
                }).then(function (pack) {
                    var confirmUrl = urls.apiPdvMpPointConfirmarTranche || '';
                    if (!String(confirmUrl).trim()) throw new Error('API confirmar tranche MP indisponível.');
                    return jsonPost(confirmUrl, { order_id: pack.order_id }).then(function (confRes) {
                        return { pack: pack, confRes: confRes };
                    });
                }).then(function (result) {
                    var confRes = result.confRes;
                    if (!confRes.ok || !confRes.data.ok) {
                        throw new Error(
                            (confRes.data && (confRes.data.erro || confRes.data.mensagem)) ||
                                'Falha ao confirmar pagamento no terminal.'
                        );
                    }
                    var snap = snapshotLancamentoFromState(State.getState(), cur);
                    snap = aplicarReconMpPointNoSnap(snap, confRes.data);
                    snap.mpPointOrderId = confRes.data.order_id || mpPointWaitControl.orderId || result.pack.order_id;
                    snap.mpPointPago = true;
                    if (confRes.data.mp_point_forma_divergencia && confRes.data.mp_point_aviso) {
                        showMpPointAviso(confRes.data.mp_point_aviso);
                    }
                    State.addPagamentoLancamento(snap);
                    pdvMpPointBeep('ok');
                    afterCommitTrancheFlow();
                });
            })
            .catch(function (err) {
                if (err && err.mpPointUserAbort) {
                    showMpPointCancelFeedback();
                } else if (err && err.mpPointUi) {
                    pdvMpPointBeep('err');
                    showMpPointAviso(err.message || 'Operação cancelada na maquininha.');
                } else {
                    pdvMpPointBeep('err');
                    showMpPointAviso(
                        (err && err.message) || 'Falha ao cobrar na maquininha Mercado Pago.',
                        { tone: 'error' }
                    );
                }
            })
            .finally(function () {
                hideMpPointWaitBar();
                finishMpTrancheBusy();
            });
    }

    function renderPayStepChips(mpPoint) {
        var el = dom.payStepChips || document.getElementById('pdv-pay-step-chips');
        if (!el) return;
        var steps = mpPoint
            ? ['Valor', 'Botão', 'Maquininha', 'Confirmar']
            : ['Valor', 'Lançar', 'Confirmar'];
        el.innerHTML = steps
            .map(function (label, i) {
                return (
                    '<span class="pdv-pay-step-chip" role="listitem"><b>' +
                    (i + 1) +
                    '</b>' +
                    escapeHtml(label) +
                    '</span>'
                );
            })
            .join('');
    }

    function commitTrancheFlow(st, comp, cur) {
        var err = erroCommitTranche(st, comp, cur);
        if (err) {
            showPdvAviso(err);
            return;
        }
        if (deveCobrarMpPointNaTranche(st, cur)) {
            cobrarMpPointNaTranche(st, comp, cur);
            return;
        }
        State.addPagamentoLancamento(snapshotLancamentoFromState(st, cur));
        afterCommitTrancheFlow();
    }

    function fillQrSlot(el, url, emptyMsg) {
        if (!el) return;
        var u = String(url || '').trim();
        if (u) {
            el.innerHTML =
                '<img src="' +
                escapeHtml(u) +
                '" alt="QR Code" class="mx-auto max-h-[min(20vh,9rem)] w-auto max-w-full object-contain">';
        } else {
            var msg =
                (emptyMsg && String(emptyMsg).trim()) ||
                'O QR desta forma de pagamento é gerado na maquininha selecionada.';
            el.innerHTML =
                '<p class="px-2 py-4 text-center text-[10px] font-bold leading-snug text-slate-600 sm:text-xs">' +
                escapeHtml(msg) +
                '</p>';
        }
    }

    function wireSicrediLink(anchor, url) {
        if (!anchor) return;
        var u = String(url || '').trim();
        if (u) {
            anchor.href = u;
            anchor.classList.remove('pointer-events-none', 'opacity-50');
            anchor.onclick = null;
            anchor.removeAttribute('title');
        } else {
            anchor.href = '#';
            anchor.classList.add('pointer-events-none');
            anchor.classList.remove('opacity-50');
            anchor.setAttribute('title', 'QR gerado na maquininha Sicredi — use o terminal.');
            anchor.onclick = function (e) {
                e.preventDefault();
            };
        }
    }

    function requiresMaquina(forma) {
        return (
            forma === 'PIX' ||
            forma === 'Cartão de débito' ||
            forma === 'Cartão de crédito' ||
            forma === 'Cartão de crédito parcelado'
        );
    }

    function getMaquininhasList(forma) {
        var m =
            forma === 'PIX'
                ? pagamentoUi.maquininhasPix
                : pagamentoUi.maquininhasCartao;
        if (!Array.isArray(m) || !m.length) return [];
        return m
            .map(function (x) {
                return {
                    id: String((x && x.id) || '').trim(),
                    nome: String((x && (x.nome || x.label)) || '').trim() || String((x && x.id) || '').trim(),
                    rede: String((x && x.rede) || '').trim()
                };
            })
            .filter(function (x) {
                return !!x.id;
            });
    }

    function rebuildMaquinasList(forma) {
        var wrap = document.getElementById('pdv-pay-maquinas-list');
        if (!wrap) return;
        var items = getMaquininhasList(forma);
        var avisoMp = String(pagamentoUi.mpPointMotivoBloqueio || '').trim();
        var avisoHtml = avisoMp
            ? '<p class="mb-3 rounded-xl border-2 border-amber-200 bg-amber-50 px-3 py-2 text-xs font-bold leading-snug text-amber-950">' +
              escapeHtml(avisoMp) +
              '</p>'
            : '';
        if (!items.length) {
            wrap.innerHTML =
                avisoHtml +
                '<p class="p-2 text-center text-sm font-bold text-slate-500">Nenhuma máquina configurada (PDV_WIZARD_MAQUININHAS_' +
                (forma === 'PIX' ? 'PIX' : 'CARTAO') +
                ').</p>';
            return;
        }
        wrap.innerHTML = avisoHtml + items
            .map(function (it, idx) {
                var k = idx < 9 ? String(idx + 1) : '';
                var cardClass = maquinaRedeClass(it);
                return (
                    '<button type="button" class="pdv-action-btn pdv-pay-maquina-card-btn mb-2 w-full min-h-[3.5rem] justify-between gap-3 rounded-2xl border-2 px-4 py-3.5 text-left text-sm font-black shadow-md transition hover:scale-[1.01] active:scale-[0.99] sm:min-h-[4rem] sm:text-base ' +
                    cardClass +
                    '" data-maquina-id="' +
                    escapeHtml(it.id) +
                    '" data-maquina-nome="' +
                    escapeHtml(it.nome) +
                    '" data-maquina-idx="' +
                    idx +
                    '"><span class="min-w-0 flex-1 leading-tight">' +
                    escapeHtml(it.nome) +
                    '</span>' +
                    (k
                        ? '<kbd class="shrink-0 rounded-lg border-2 border-black/10 bg-black/10 px-2 py-1 font-mono text-sm font-black">' +
                          k +
                          '</kbd>'
                        : '') +
                    '</button>'
                );
            })
            .join('');
    }

    function isMaquinaMpPointAuto(maquinaId, forma) {
        if (!pagamentoUi.mpPointEnabled) return false;
        var mid = String(maquinaId || '').trim();
        var f = String(forma || '').trim();
        if (mid === 'mp_balcao' && f !== 'PIX') return true;
        if (mid === 'pix_mp_qr' && f === 'PIX') return true;
        return false;
    }

    function finishMaquinaSelection(id, nome) {
        var st = State.getState();
        var formaM = st.pagamento.forma || '';
        var patch = { maquinaId: id, maquinaNome: nome, mpBalcaoModo: '' };
        if (isMaquinaMpPointAuto(id, formaM)) {
            patch.mpBalcaoModo = 'point';
        }
        State.setPagamentoPatch(patch);
        var md = document.getElementById('pdv-pay-pop-maquinas');
        if (md && typeof md.close === 'function') {
            try {
                md.close();
            } catch (errM) {}
        }
        focusFirstFlowFieldForForma(State.getState().pagamento.forma);
    }

    function openMaquinasDialog() {
        var dlg = document.getElementById('pdv-pay-pop-maquinas');
        var st = State.getState();
        var forma = st.pagamento.forma || '';
        var titleEl = document.getElementById('pdv-pay-pop-maquinas-title');
        if (titleEl) {
            titleEl.textContent = forma === 'PIX' ? 'Pix — qual máquina?' : 'Cartão — qual máquina?';
        }
        rebuildMaquinasList(forma);
        showPayFlowDialog(dlg);
        setTimeout(function () {
            var w = document.getElementById('pdv-pay-maquinas-list');
            if (!w) return;
            var b = w.querySelector('[data-maquina-id]');
            if (b) b.focus();
        }, 50);
    }

    function pagamentoResumoExtra(state, computed) {
        var parts = [];
        var arr = state.pagamento.lancamentos || [];
        var total = totalNumberFromComputed(computed);
        if (arr.length) {
            parts.push(
                'Pagamentos: ' +
                    arr
                        .map(function (L) {
                            var bits = [(L.forma || '') + ' ' + formatMoney(L.valor)];
                            if (L.maquinaNome) bits.push(L.maquinaNome);
                            if (L.forma === 'Cartão de crédito parcelado' && L.creditoParcelas) bits.push(L.creditoParcelas + 'x');
                            return bits.join(' · ');
                        })
                        .join(' | ')
            );
        }
        if (Math.abs(sumValorLancamentos(state) - total) > 0.02) {
            parts.push('Total venda ' + formatMoney(total));
        }
        return parts.filter(Boolean).join(' | ');
    }

    /** Linha detalhada (valores / troco) — cupom, resumo na tela, separação. */
    function lancamentoFormaErpLine(L) {
        if (!L) return '';
        var f = L.forma || '';
        var line = f + ' ' + formatMoney(L.valor);
        if (f === 'Cartão de crédito parcelado' && L.creditoParcelas) line += ' ' + L.creditoParcelas + 'x';
        if (L.maquinaNome) line += ' [' + L.maquinaNome + ']';
        if (L.trocoCalculado) line += ' (troco ' + formatMoney(State.toNumber(L.trocoCalculado)) + ')';
        return line;
    }

    /** Só rótulo da forma (sem valor) — ERP lista valor na outra coluna; evita "Dinheiro R$ 4,00". */
    function lancamentoFormaErpLabel(L) {
        if (!L) return '';
        var f = String(L.forma || '').trim();
        if (!f) return '';
        var bits = [f];
        if (f === 'Cartão de crédito parcelado' && L.creditoParcelas) bits.push(String(L.creditoParcelas).trim() + 'x');
        if (L.maquinaNome) bits.push(String(L.maquinaNome).trim());
        return bits.join(' ');
    }

    /** Texto gravado no Agro / campo formaPagamento do pedido (sem valores embutidos). */
    function formaPagamentoParaErp(state, computed) {
        var arr = state.pagamento.lancamentos || [];
        if (!arr.length) return state.pagamento.forma || '';
        return arr.map(lancamentoFormaErpLabel).filter(Boolean).join(' + ');
    }

    /** Resumo com valores para UI interna e cupom. */
    function formaPagamentoResumoUi(state, computed) {
        var arr = state.pagamento.lancamentos || [];
        if (!arr.length) return state.pagamento.forma || '';
        return arr.map(lancamentoFormaErpLine).filter(Boolean).join(' + ');
    }

    /** Uma linha em ``pagamentos`` por lançamento (Pedidos/Salvar). */
    function buildFiadoCronograma(valor, numParcelas, diasPrimeira) {
        var n = Math.min(6, Math.max(1, parseInt(numParcelas, 10) || 1));
        var diasBase = Math.max(1, parseInt(diasPrimeira, 10) || 30);
        var total = Math.round((State.toNumber(valor) + Number.EPSILON) * 100) / 100;
        if (!(total > 0.009)) return [];
        var base = Math.round(((total / n) + Number.EPSILON) * 100) / 100;
        var out = [];
        var acum = 0;
        var ref = new Date();
        for (var i = 0; i < n; i++) {
            var parcelaVal = i === n - 1 ? Math.round((total - acum + Number.EPSILON) * 100) / 100 : base;
            acum += parcelaVal;
            var dias = diasBase * (i + 1);
            var venc = new Date(ref.getTime());
            venc.setDate(venc.getDate() + dias);
            out.push({
                parcela: i + 1,
                dias: dias,
                vencimento: venc.toISOString().slice(0, 10),
                valor: parcelaVal
            });
        }
        return out;
    }

    function clientePodeFiado(state) {
        if (!state || state.clienteMode === 'consumidor_final') return false;
        var c = state.cliente;
        if (!c || !String(c.nome || '').trim()) return false;
        if (/consumidor\s+n[aã]o\s+identificado/i.test(String(c.nome || ''))) return false;
        if (c.cliente_agro_pk != null && String(c.cliente_agro_pk).trim() !== '') return true;
        var id = String(c.id || '').trim();
        if (!id) return false;
        if (/^erp-doc:/i.test(id)) return false;
        return true;
    }

    function clienteFiadoQueryKey(state) {
        var c = state.cliente || {};
        var pk = c.cliente_agro_pk != null ? String(c.cliente_agro_pk) : '';
        var id = String(c.id || '').trim();
        return pk ? 'pk:' + pk : 'id:' + id;
    }

    function buildFiadoGestaoUrl(state) {
        var base = urls.fiadoGestao || '/fiado/';
        var sep = base.indexOf('?') >= 0 ? '&' : '?';
        var u = base + sep + 'from=pdv';
        var st = state || State.getState();
        if (st && st.cliente && st.cliente.cliente_agro_pk != null) {
            u += '&cliente=' + encodeURIComponent(String(st.cliente.cliente_agro_pk));
        }
        try {
            return new URL(u, window.location.origin).href;
        } catch (eUrl) {
            return u;
        }
    }

    /** Abre consulta no painel do balcão ou na janela gestão (conforme origem). */
    function navegarAgroInApp(href) {
        var url = String(href || '').trim();
        if (!url) return;
        try {
            if (!/^https?:\/\//i.test(url)) {
                url = new URL(url, window.location.origin).href;
            }
        } catch (eAbs) {
            return;
        }
        var dw = window.AgroDualWindow;
        var onPdv = dw && typeof dw.isPdvHost === 'function' && dw.isPdvHost();
        try {
            if (dw && typeof dw.openPdvPanel === 'function' && onPdv) {
                dw.openPdvPanel(url);
                return;
            }
            if (dw && typeof dw.navigateGestao === 'function') {
                if (dw.inEmbed && dw.inEmbed()) {
                    dw.navigateGestao(url);
                    return;
                }
            }
            if (dw && dw.enabled && dw.enabled()) {
                dw.navigateGestao(url);
                return;
            }
        } catch (eDw) {}
        try {
            if (window.top && window.top !== window) {
                window.top.postMessage({ type: 'agro-open-inapp-tab', href: url }, window.location.origin);
                return;
            }
        } catch (ePm) {}
        try {
            if (window.top && window.top !== window && typeof window.top.__agroInAppAddTab === 'function') {
                window.top.__agroInAppAddTab(url);
                return;
            }
        } catch (eTab) {}
        window.location.href = url;
    }

    function openFiadoGestao() {
        navegarAgroInApp(buildFiadoGestaoUrl(State.getState()));
    }

    function mensagemBloqueioFiadoPendencia() {
        if (!creditoFiadoCliente) return '';
        if (!creditoFiadoCliente.tem_vencido && !creditoFiadoCliente.bloqueado_nova_venda) return '';
        return (
            creditoFiadoCliente.bloqueado_motivo ||
            'Cliente com fiado vencido (' +
                (creditoFiadoCliente.total_vencido_texto ||
                    formatMoney(creditoFiadoCliente.total_vencido || 0)) +
                '). Quite os vencidos antes de nova venda fiado.'
        );
    }

    function validarFiadoPermitido(state) {
        if (!clientePodeFiado(state)) {
            return 'Fiado exige cliente cadastrado (não use consumidor final).';
        }
        var pend = mensagemBloqueioFiadoPendencia();
        if (pend) return pend;
        return '';
    }

    function renderProductFiadoBalance(state) {
        if (!dom.productFiadoBalance) return;
        var cf = creditoFiadoCliente;
        var cidKey = clienteFiadoQueryKey(state);
        if (cf && creditoFiadoClienteId === cidKey) {
            var saldoDevedor = State.toNumber(cf.usado);
            if (!isFinite(saldoDevedor) || saldoDevedor < 0) saldoDevedor = 0;
            dom.productFiadoBalance.textContent = formatMoney(saldoDevedor);
            var tip = 'Saldo devedor (fiado em aberto) · clique para gerir';
            if (cf.tem_vencido || cf.bloqueado_nova_venda) {
                tip = mensagemBloqueioFiadoPendencia() + ' · clique para gerir';
            } else if (saldoDevedor > 0.009) {
                tip = 'Saldo fiado em aberto · clique para gerir';
            } else {
                tip = 'Sem saldo fiado em aberto';
            }
            dom.productFiadoBalance.title = tip;
        } else {
            dom.productFiadoBalance.textContent = clientePodeFiado(state) ? '…' : 'R$ 0,00';
            dom.productFiadoBalance.title = clientePodeFiado(state)
                ? 'Carregando saldo fiado…'
                : 'Selecione um cliente cadastrado';
        }
        if (dom.fiadoGestaoOpen) {
            dom.fiadoGestaoOpen.title = dom.productFiadoBalance.title;
        }
        if (dom.topbarFiadoLink) {
            dom.topbarFiadoLink.href = buildFiadoGestaoUrl(state);
        }
    }

    function closeFiadoVencidosModal() {
        if (!dom.fiadoVencidosModal) return;
        dom.fiadoVencidosModal.classList.add('hidden');
        dom.fiadoVencidosModal.classList.remove('flex');
        try {
            document.body.style.overflow = '';
        } catch (errFv) {}
    }

    function openFiadoVencidosModal(state) {
        if (!dom.fiadoVencidosModal || !creditoFiadoCliente || !creditoFiadoCliente.tem_vencido) return;
        var st = state || State.getState();
        var titulos = creditoFiadoCliente.titulos_vencidos || [];
        if (!titulos.length) return;
        if (dom.fiadoVencidosCliente) {
            dom.fiadoVencidosCliente.textContent = (st.cliente && st.cliente.nome) || 'Cliente';
        }
        if (dom.fiadoVencidosTotal) {
            dom.fiadoVencidosTotal.textContent =
                creditoFiadoCliente.total_vencido_texto ||
                formatMoney(creditoFiadoCliente.total_vencido || 0);
        }
        if (dom.fiadoVencidosTbody) {
            dom.fiadoVencidosTbody.innerHTML = titulos
                .map(function (t) {
                    return (
                        '<tr class="border-t border-red-100">' +
                        '<td class="py-2 pr-2 font-bold max-w-[10rem] truncate" title="' +
                        escapeHtml(t.numero_documento || '') +
                        '">' +
                        escapeHtml(t.numero_documento || '—') +
                        '</td>' +
                        '<td class="py-2 pr-2 font-black text-red-800 whitespace-nowrap">' +
                        escapeHtml(t.vencimento_texto || '—') +
                        '</td>' +
                        '<td class="py-2 text-right font-black tabular-nums text-red-900">' +
                        escapeHtml(formatMoney(t.saldo_aberto || 0)) +
                        '</td></tr>'
                    );
                })
                .join('');
        }
        if (dom.fiadoVencidosGestao) {
            dom.fiadoVencidosGestao.href = buildFiadoGestaoUrl(st);
            dom.fiadoVencidosGestao.title = 'Abrir gestão fiado (nova aba no menu lateral)';
        }
        dom.fiadoVencidosModal.classList.remove('hidden');
        dom.fiadoVencidosModal.classList.add('flex');
        try {
            document.body.style.overflow = 'hidden';
        } catch (errFv2) {}
        if (dom.fiadoVencidosFechar) dom.fiadoVencidosFechar.focus();
    }

    function maybeShowFiadoVencidosAlert(state, opts) {
        opts = opts || {};
        if (!opts.showVencidosAlert) return;
        var key = clienteFiadoQueryKey(state);
        if (!key || key === fiadoVencidosAlertShownKey) return;
        if (!creditoFiadoCliente || !creditoFiadoCliente.tem_vencido) return;
        fiadoVencidosAlertShownKey = key;
        openFiadoVencidosModal(state);
    }

    function refreshCreditoFiadoCliente(valorFiadoPendente, opts) {
        opts = opts || {};
        var url = urls.apiPdvClienteCreditoFiado;
        if (!url) return Promise.resolve();
        var state = State.getState();
        if (!clientePodeFiado(state)) {
            creditoFiadoCliente = null;
            creditoFiadoClienteId = '';
            fiadoVencidosAlertShownKey = '';
            return Promise.resolve();
        }
        var c = state.cliente;
        var cidKey = clienteFiadoQueryKey(state);
        if (!opts.force && creditoFiadoClienteId === cidKey && creditoFiadoCliente) {
            return Promise.resolve();
        }
        creditoFiadoClienteId = cidKey;
        creditoFiadoCliente = null;
        renderProductFiadoBalance(state);
        var q = url + (url.indexOf('?') >= 0 ? '&' : '?');
        if (c.cliente_agro_pk != null) {
            q += 'cliente_agro_pk=' + encodeURIComponent(String(c.cliente_agro_pk));
            if (String(c.id || '').trim()) {
                q += '&cliente_id=' + encodeURIComponent(String(c.id).trim());
            }
        } else {
            q += 'cliente_id=' + encodeURIComponent(String(c.id || '').trim());
        }
        if (String(c.nome || '').trim()) {
            q += '&cliente_nome=' + encodeURIComponent(String(c.nome).trim());
        }
        if (valorFiadoPendente != null && State.toNumber(valorFiadoPendente) > 0.009) {
            q += '&valor_fiado=' + encodeURIComponent(String(valorFiadoPendente).replace('.', ','));
        }
        return jsonGet(q)
            .then(function (res) {
                if (clienteFiadoQueryKey(State.getState()) !== cidKey) {
                    return;
                }
                if (res.ok && res.data && res.data.ok !== false) {
                    creditoFiadoCliente = res.data;
                } else {
                    creditoFiadoCliente = null;
                }
                maybeShowFiadoVencidosAlert(State.getState(), opts);
            })
            .catch(function () {
                if (clienteFiadoQueryKey(State.getState()) === cidKey) {
                    creditoFiadoCliente = null;
                }
            })
            .then(function () {
                renderProductFiadoBalance(State.getState());
            });
    }

    function valorFiadoNosLancamentos(state) {
        var sum = 0;
        (state.pagamento.lancamentos || []).forEach(function (L) {
            if (String(L.forma || '') === 'Fiado') sum += State.toNumber(L.valor);
        });
        return sum;
    }

    function pagamentosDetalheParaErp(state) {
        var arr = state.pagamento.lancamentos || [];
        if (!arr.length) return null;
        var out = [];
        for (var i = 0; i < arr.length; i++) {
            var L = arr[i];
            var fn = lancamentoFormaErpLabel(L);
            var v = State.toNumber(L.valor);
            if (!fn && !(v > 0.0001)) continue;
            if (!fn) fn = 'Não informado';
            var row = {
                formaPagamento: fn.slice(0, 200),
                valorPagamento: Math.round((v + Number.EPSILON) * 100) / 100,
                quitar: fn !== 'Fiado'
            };
            if (fn === 'Fiado') {
                var fp = Math.min(6, Math.max(1, parseInt(L.fiadoParcelas, 10) || 1));
                var fd = Math.max(1, parseInt(L.fiadoDiasVencimento, 10) || 30);
                row.fiadoParcelas = fp;
                row.fiadoDiasVencimento = fd;
                row.fiadoCronograma = buildFiadoCronograma(v, fp, fd);
            }
            if (L.forma === 'Cartão de crédito parcelado') {
                row.creditoParcelas = Math.min(
                    24,
                    Math.max(
                        2,
                        parseInt(L.creditoParcelas, 10) ||
                            parseInt(state.pagamento.creditoParcelas, 10) ||
                            2
                    )
                );
            }
            var midL = String(L.maquinaId || '').trim();
            if (midL) {
                row.maquinaId = midL;
                if (L.maquinaNome) row.maquinaNome = String(L.maquinaNome).slice(0, 120);
                if (L.mpBalcaoModo) row.mpBalcaoModo = String(L.mpBalcaoModo);
                if (L.cobrarNoPointMp) row.cobrarNoPointMp = true;
            }
            out.push(row);
        }
        return out.length ? out : null;
    }

    function deveUsarMpPointNoFechar(state, computed) {
        if (!pagamentoUi.mpPointEnabled || !String(urls.apiPdvMpPointCriar || '').trim()) return false;
        var arr = state.pagamento.lancamentos || [];
        if (arr.length !== 1) return false;
        var L = arr[0];
        var mid = String(L.maquinaId || '').trim();
        var forma = String(L.forma || state.pagamento.forma || '').trim();
        if (!isMaquinaMpPointAuto(mid, forma)) return false;
        var total = totalNumberFromComputed(computed);
        var vL = Math.round((State.toNumber(L.valor) + Number.EPSILON) * 100) / 100;
        var vT = Math.round((total + Number.EPSILON) * 100) / 100;
        if (Math.abs(vL - vT) > 0.1) return false;
        return true;
    }

    function erroValidacaoPagamento(state, computed) {
        var forma = String(state.pagamento.forma || '').trim();
        if (forma) return '';
        var arr = state.pagamento.lancamentos || [];
        if (!arr.length) return 'Escolha formas de pagamento até cobrir o total.';
        var total = totalNumberFromComputed(computed);
        var sum = sumValorLancamentos(state);
        if (sum + 0.009 < total) {
            return 'Falta ' + formatMoney(total - sum) + '. Escolha outra forma.';
        }
        if (sum > total + 0.009) return 'Soma dos pagamentos passou do total. Ajuste os lançamentos.';
        var temFiado = arr.some(function (L) {
            return String(L.forma || '') === 'Fiado';
        });
        if (temFiado) {
            var msgFi = validarFiadoPermitido(state);
            if (msgFi) return msgFi;
            if (creditoFiadoCliente && creditoFiadoCliente.excede) {
                return (
                    'Fiado acima do limite. Disponível ' +
                    (creditoFiadoCliente.disponivel_texto || '') +
                    '.'
                );
            }
        }
        return '';
    }

    function canAdvance(state, computed) {
        if (state.currentStep === 'produtos') {
            if (!state.itens.length) return 'Adicione ao menos 1 item antes de continuar.';
            if (state.clienteMode === 'unset') return 'Defina o cliente ou consumidor final antes de continuar.';
        }
        if (state.currentStep === 'entrega') {
            if (!enderecoEntregaMinimoOk(state)) return 'Informe o endereço básico da entrega (logradouro e bairro ou endereço legível).';
        }
        if (state.currentStep === 'pagamento') {
            var ep = erroValidacaoPagamento(state, computed);
            if (ep) return ep;
        }
        return '';
    }

    function setInputValue(el, value) {
        if (!el) return;
        var next = value == null ? '' : String(value);
        if (el.value !== next) el.value = next;
    }

    function setInputValueUnlessFocused(el, value) {
        if (!el) return;
        if (document.activeElement === el) return;
        setInputValue(el, value);
    }

    function moneyFieldDisplay(value) {
        return State.formatMoneyInputDisplay ? State.formatMoneyInputDisplay(value) : String(value == null ? '' : value);
    }

    function bindMoneyInputField(el, onStore) {
        if (!el || typeof onStore !== 'function') return;
        el.addEventListener('input', function () {
            var next = State.sanitizeMoneyInputTyping
                ? State.sanitizeMoneyInputTyping(el.value)
                : el.value;
            if (next !== el.value) el.value = next;
            onStore(next);
        });
        el.addEventListener('blur', function () {
            var n = State.toNumber(el.value);
            var fmt = n > 0.009 ? moneyFieldDisplay(n) : '';
            if (fmt !== el.value) el.value = fmt;
            onStore(fmt);
        });
    }

    function setSelectValue(el, value, fallback) {
        if (!el) return;
        var next = value == null || value === '' ? (fallback || '') : String(value);
        if (el.value !== next) el.value = next;
    }

    function showElement(el, show, displayValue) {
        if (!el) return;
        if (show) {
            el.hidden = false;
            el.classList.remove('hidden');
            if (displayValue) el.classList.add(displayValue);
        } else {
            el.hidden = true;
            el.classList.add('hidden');
            el.classList.remove('flex');
        }
    }

    function esconderSubpainelsEntregaForaDaEtapa() {
        var onEntrega = State.getState().currentStep === 'entrega';
        if (onEntrega) return;
        ['pdv-entrega-partida-bar', 'pdv-entrega-main', 'pdv-entrega-resumo'].forEach(function (id) {
            var el = document.getElementById(id);
            if (el) showElement(el, false);
        });
        if (dom.entregaWizard) showElement(dom.entregaWizard, false);
    }

    function renderStepPanels(state, computed) {
        var flow = computed.flow;
        dom.panels.forEach(function (panel) {
            var step = panel.getAttribute('data-step-panel');
            var visible = step === state.currentStep;
            panel.hidden = !visible;
            panel.classList.toggle('hidden', !visible);
        });
        esconderSubpainelsEntregaForaDaEtapa();
        dom.stepNavs.forEach(function (btn) {
            var step = btn.getAttribute('data-step-nav');
            var idx = flowIndex(flow, step);
            var currentIdx = flowIndex(flow, state.currentStep);
            btn.classList.remove(
                'pdv-step-badge-active',
                'pdv-step-badge-done',
                'pdv-step-nav-retracted',
                'border-emerald-200',
                'bg-emerald-50'
            );
            btn.removeAttribute('aria-current');
            btn.style.zIndex = '';
            if (idx === currentIdx) {
                btn.classList.add('pdv-step-badge-active');
                btn.setAttribute('aria-current', 'step');
                btn.style.zIndex = '20';
            } else {
                btn.classList.add('pdv-step-nav-retracted');
                if (idx > -1 && idx < currentIdx) {
                    btn.classList.add('pdv-step-badge-done', 'border-emerald-200');
                }
                btn.style.zIndex = String(idx > -1 ? (idx < currentIdx ? idx + 1 : 12 - idx) : 3);
            }
            btn.disabled = idx === -1;
        });

        var prev = prevStep(state, computed);
        var next = nextStep(state, computed);
        dom.btnPrev.disabled = !prev;
        dom.btnPrev.classList.toggle('opacity-40', !prev);
        if (dom.btnPrev) {
            if (state.currentStep === 'entrega') {
                dom.btnPrev.innerHTML =
                    'Voltar <kbd class="pointer-events-none ml-1 rounded border border-slate-200 bg-slate-100 px-1 font-mono text-[8px] text-slate-600">F1</kbd>';
                dom.btnPrev.title = 'Volta um passo (tela ou pergunta anterior) · F1';
            } else {
                dom.btnPrev.textContent = 'Voltar';
                dom.btnPrev.title = 'Etapa anterior · Alt+←';
            }
        }
        if (dom.btnNext) {
            dom.btnNext.style.display = state.currentStep === 'pagamento' ? 'none' : '';
            var nextLabel = next === 'pagamento' ? 'Ir para pagamento' : 'Continuar';
            var kbdF7 =
                '<kbd class="pointer-events-none ml-1 rounded border border-emerald-800/40 bg-emerald-700 px-1 font-mono text-[8px] text-white sm:ml-1.5">F7</kbd>';
            var kbdCtrl =
                '<kbd class="pointer-events-none ml-1 hidden rounded border border-emerald-800/40 bg-emerald-700 px-1 font-mono text-[8px] text-white sm:inline">Ctrl+Enter</kbd>';
            if (state.currentStep === 'entrega') {
                var lp = String((state.entrega && state.entrega.localPagamento) || '');
                var endOk = enderecoEntregaMinimoOkParaUi(state);
                var faseEnt = entregaFaseAtual(state);
                dom.btnNext.classList.remove('opacity-40');
                if (faseEnt === 'pagamento_local') {
                    dom.btnNext.innerHTML = 'Pagamento entrega ou loja' + kbdF7;
                    dom.btnNext.disabled = false;
                    dom.btnNext.title = 'Escolha na tela acima · F7';
                } else if (faseEnt === 'endereco') {
                    dom.btnNext.innerHTML = 'Continuar para taxa' + kbdF7;
                    dom.btnNext.disabled = !endOk;
                    if (!endOk) dom.btnNext.classList.add('opacity-40');
                    dom.btnNext.title = 'Endereço preenchido · abre taxa e horário · F7';
                } else if (faseEnt === 'detalhes') {
                    dom.btnNext.innerHTML = 'Confirmar taxa e horário' + kbdF7;
                    dom.btnNext.disabled = false;
                    dom.btnNext.title = 'Confirma frete e horário · F7';
                } else if (faseEnt === 'meio') {
                    dom.btnNext.innerHTML = 'Dinheiro ou cartão' + kbdF7;
                    dom.btnNext.disabled = false;
                    dom.btnNext.title = 'Escolha na tela acima · F7';
                } else if (faseEnt === 'troco') {
                    dom.btnNext.innerHTML = 'Confirmar troco' + kbdF7;
                    dom.btnNext.disabled = false;
                    dom.btnNext.title = 'Confirma valor em dinheiro · F7';
                } else if (faseEnt === 'done' && lp === 'loja') {
                    dom.btnNext.innerHTML = 'Ir para pagamento' + kbdF7;
                    dom.btnNext.disabled = false;
                    dom.btnNext.title = 'Imprime as vias e abre a etapa Pagamento · F7';
                } else if (faseEnt === 'done' && lp === 'entrega') {
                    dom.btnNext.innerHTML = 'Enviar entrega' + kbdF7;
                    dom.btnNext.disabled = false;
                    dom.btnNext.title = 'Imprime, registra no painel Entregas e reinicia o PDV · F7';
                } else if (lp === 'loja') {
                    dom.btnNext.innerHTML = 'Ir para pagamento' + kbdF7;
                    dom.btnNext.disabled = !endOk;
                    if (!endOk) dom.btnNext.classList.add('opacity-40');
                    dom.btnNext.title = 'Imprime as vias e abre a etapa Pagamento · F7';
                } else if (lp === 'entrega') {
                    dom.btnNext.innerHTML = 'Enviar entrega' + kbdF7;
                    dom.btnNext.disabled = !endOk;
                    if (!endOk) dom.btnNext.classList.add('opacity-40');
                    dom.btnNext.title = 'Imprime, registra no painel Entregas e reinicia o PDV · F7';
                } else {
                    dom.btnNext.innerHTML = nextLabel + kbdF7;
                    dom.btnNext.disabled = false;
                    dom.btnNext.title = 'Próxima etapa · F7';
                }
            } else {
                dom.btnNext.innerHTML = nextLabel + kbdCtrl;
                dom.btnNext.disabled = false;
                dom.btnNext.title = 'Próxima etapa · Ctrl+Enter ou Alt+Enter';
            }
        }
        if (dom.mainFooter) {
            dom.mainFooter.style.display = state.currentStep === 'produtos' ? 'none' : '';
        }

        var hints = {
            produtos: 'Monte os itens e defina o cliente base da venda.',
            entrega: '',
            pagamento: ''
        };
        if (dom.stepHint) {
            dom.stepHint.textContent = hints[state.currentStep] || '';
            dom.stepHint.style.display = hints[state.currentStep] ? '' : 'none';
        }
        if (document.body) {
            document.body.setAttribute('data-pdv-step', state.currentStep || 'produtos');
        }
    }

    function irParaPagamentoFromProdutos() {
        if (typeof hideSaleDoneToast === 'function') hideSaleDoneToast();
        var state = State.getState();
        if (!state.itens.length) {
            alert('Adicione ao menos 1 item antes de ir para pagamento.');
            return;
        }
        if (state.clienteMode === 'unset') {
            alert('Defina o cliente ou consumidor final antes de ir para pagamento.');
            return;
        }
        State.setCurrentStep('pagamento');
    }

    function renderSummary(state, computed) {
        if (!dom.summaryClient) return;
        dom.summaryClient.textContent = currentClientName(state);
        dom.summaryMode.textContent = state.clienteMode === 'consumidor_final'
            ? 'Consumidor final'
            : compactText((state.cliente && (state.cliente.documento || state.cliente.telefone)) || '', 'Cliente selecionado');
        dom.summaryItems.textContent = formatQty(computed.itemCount);
        if (dom.summarySubtotal) dom.summarySubtotal.textContent = formatMoney(computed.subtotal);
        if (dom.summaryDiscount) dom.summaryDiscount.textContent = formatMoney(computed.desconto);
        if (dom.summaryShipping) dom.summaryShipping.textContent = formatMoney(computed.frete);
        dom.summaryTotal.textContent = formatMoney(computed.total);
        dom.summaryDelivery.textContent =
            state.entrega.modoRetiradaEntrega === 'entrega' || state.entrega.ativa ? 'Entrega' : 'Retirada';
        var currentStepLabel = ({
            produtos: 'Produtos',
            entrega: 'Entrega',
            pagamento: 'Pagamento'
        })[state.currentStep] || 'Produtos';
        if (state.currentStep === 'produtos') {
            dom.summaryPayment.textContent = '';
            dom.summaryNote.textContent = '';
            dom.summaryCurrentStep.textContent = '';
            var stepExtra = document.getElementById('pdv-summary-step-extra');
            if (stepExtra) stepExtra.classList.add('hidden');
        } else {
            var stepExtraShow = document.getElementById('pdv-summary-step-extra');
            if (stepExtraShow) stepExtraShow.classList.remove('hidden');
            var compS = State.getComputed();
            var larr = state.pagamento.lancamentos || [];
            if (larr.length) {
                dom.summaryPayment.textContent = 'Pagamento: ' + formaPagamentoResumoUi(state, compS);
            } else if (state.pagamento.forma) {
                dom.summaryPayment.textContent = 'Pagamento: ' + state.pagamento.forma + ' (pendente)';
            } else {
                dom.summaryPayment.textContent = 'Pagamento: em aberto';
            }
            dom.summaryNote.textContent = shortNote(state);
            dom.summaryCurrentStep.textContent = currentStepLabel;
        }
    }

    function openProductPhotoPop(url) {
        var dlg = document.getElementById('pdv-product-photo-pop');
        var img = document.getElementById('pdv-product-photo-pop-img');
        if (!dlg || !img) return;
        var u = String(url || '').trim() || String(assets.placeholderProduto || '');
        img.src = u;
        if (typeof dlg.showModal === 'function') {
            try {
                dlg.showModal();
            } catch (errD) {}
        }
        setTimeout(function () {
            try {
                img.focus();
            } catch (errF) {}
        }, 80);
    }

    function updateSearchAwaitingPulse() {
        var wrap = document.getElementById('pdv-product-search-wrap');
        if (!wrap || !dom.productSearch) return;
        if (State.getState().currentStep !== 'produtos') {
            wrap.classList.remove('pdv-search-awaiting');
            return;
        }
        var v = String(dom.productSearch.value || '').trim();
        var acHidden = !dom.productAutocomplete || dom.productAutocomplete.classList.contains('hidden');
        var fb = String((dom.productSearchFeedback && dom.productSearchFeedback.textContent) || '');
        var loading = /Carregando|Filtrando|Buscando/i.test(fb);
        var on = !v && acHidden && !loading;
        wrap.classList.toggle('pdv-search-awaiting', on);
    }

    function bumpLastCartItem(delta) {
        var st = State.getState();
        var arr = st.itens || [];
        if (!arr.length) return;
        var last = arr[arr.length - 1];
        applyQtyDelta(last.id, delta > 0 ? 1 : -1);
    }

    function clienteAgroPkFromCliente(cliente) {
        if (!cliente) return null;
        if (cliente.cliente_agro_pk != null && cliente.cliente_agro_pk !== '') {
            var n = Number(cliente.cliente_agro_pk);
            return Number.isFinite(n) ? n : null;
        }
        var s = String(cliente.id || '');
        if (s.indexOf('local:') === 0) {
            var pk = parseInt(s.slice(6), 10);
            return Number.isFinite(pk) ? pk : null;
        }
        return null;
    }

    function cartRowMixClass(item) {
        if (!item || item.preco_manual) return '';
        var cor = item.promo_mix_cor;
        if (cor == null || cor === '') return '';
        var cls = ' pdv-cart-row--mix pdv-cart-row--mix-' + String(cor);
        if (item.promo_mix_pendente) cls += ' pdv-cart-row--mix-pendente';
        return cls;
    }

    function renderCartMixNameTag(item) {
        if (!item || item.preco_manual || item.promo_mix_cor == null) return '';
        var titulo = 'Mesma promo mix — linhas com a mesma cor formam o bloco juntas';
        return (
            '<span class="pdv-cart-mix-tag" title="' +
            escapeHtml(titulo) +
            '">MIX</span>'
        );
    }

    function renderCartPromoBadges(item, itens) {
        var empty = '<span class="pdv-cart-promo-wrap pdv-cart-promo--empty" aria-hidden="true"></span>';
        if (!item || item.preco_manual) return empty;
        var promo = item.promocao;
        if (typeof window.AgroPdvPromocoes === 'undefined') return empty;
        if (!promo) promo = window.AgroPdvPromocoes.getPromo(item.id);
        if (!promo) return empty;
        var padrao = parseFloat(item.preco_padrao != null ? item.preco_padrao : item.preco);
        if (!isFinite(padrao)) padrao = 0;
        var ctx =
            window.AgroPdvPromocoes.poolContextoFromCarrinho &&
            window.AgroPdvPromocoes.poolContextoFromCarrinho(item, itens);
        if (!ctx) ctx = {};
        if (
            window.AgroPdvPromocoes.mixBlocoContextoCarrinho &&
            itens &&
            itens.length
        ) {
            var mixBloco = window.AgroPdvPromocoes.mixBlocoContextoCarrinho(item, itens);
            if (mixBloco) {
                ctx.mixContinuacao = mixBloco.mixContinuacao;
                ctx.mixCabecalho = mixBloco.mixCabecalho;
            }
        }
        var resumo = window.AgroPdvPromocoes.resumoIndicadorPromo(promo, item.qtd, padrao, ctx);
        if (!resumo || !resumo.badges || !resumo.badges.length) return empty;
        var html =
            '<span class="pdv-cart-promo-wrap pdv-cart-promo--' +
            escapeHtml(resumo.state || 'ativo') +
            '">';
        resumo.badges.forEach(function (badge) {
            if (badge.stack) {
                html +=
                    '<span class="pdv-cart-promo-badge pdv-cart-promo-badge--stack" title="' +
                    escapeHtml(badge.title || badge.lineBottom || '') +
                    '">' +
                    '<span class="pdv-cart-promo-line pdv-cart-promo-line-top">' +
                    escapeHtml(badge.lineTop || 'PROMO') +
                    '</span>' +
                    '<span class="pdv-cart-promo-line pdv-cart-promo-line-sub">' +
                    escapeHtml(badge.lineBottom || '') +
                    '</span>' +
                    '</span>';
                return;
            }
            html +=
                '<span class="pdv-cart-promo-badge' +
                (badge.mixLinha ? ' pdv-cart-promo-badge--mix-linha' : '') +
                '" title="' +
                escapeHtml(badge.title || badge.text || '') +
                '">' +
                escapeHtml(badge.text || '') +
                '</span>';
        });
        html += '</span>';
        return html;
    }

    function renderQuickClient(state) {
        if (dom.quickClientName) dom.quickClientName.textContent = currentClientName(state);
        if (dom.quickClientEditStep1) {
            var podeEditar =
                state.clienteMode !== 'consumidor_final' &&
                state.cliente &&
                clienteAgroPkFromCliente(state.cliente) != null;
            dom.quickClientEditStep1.disabled = !podeEditar;
        }
        if (!dom.quickClientMeta) return;
        if (state.clienteMode === 'consumidor_final') {
            dom.quickClientMeta.textContent = 'Consumidor final definido para venda rápida.';
        } else if (state.cliente) {
            dom.quickClientMeta.textContent = compactText(state.cliente.telefone || state.cliente.endereco, 'Cliente carregado no wizard.');
        } else {
            dom.quickClientMeta.textContent = 'Você pode ajustar os dados na próxima etapa.';
        }
    }

    function renderProducts(state, computed) {
        dom.productStepCount.textContent = String(state.itens.length);
        dom.productSubtotal.textContent = formatMoney(computed.subtotal);
        var lineCount = state.itens.length;
        dom.productSubtotalItems.textContent = lineCount + (lineCount === 1 ? ' item' : ' itens');
        if (dom.productCreditBalance) dom.productCreditBalance.textContent = formatMoney(saldoValeAtual(state));
        if (dom.productCashbackBalance) dom.productCashbackBalance.textContent = formatMoney(saldoCashbackAtual(state));
        renderProductFiadoBalance(state);
        if (!state.itens.length) {
            dom.productCartList.innerHTML =
                '<div class="pdv-cart-empty">' +
                '  <span class="pdv-cart-empty-icon" aria-hidden="true">' +
                '    <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M2.25 3h1.386c.51 0 .955.343 1.087.835l.383 1.437M7.5 14.25a3 3 0 0 0-3 3h15.75m-12.75-3h11.218c1.121-2.3 2.1-4.684 2.924-7.138a60.114 60.114 0 0 0-16.536-1.84M7.5 14.25 5.106 5.272M6 20.25a.75.75 0 1 0 0-1.5.75.75 0 0 0 0 1.5Zm12 0a.75.75 0 1 0 0-1.5.75.75 0 0 0 0 1.5Z" /></svg>' +
                '  </span>' +
                '  <p class="pdv-cart-empty-title">Nenhum item ainda — busque acima.</p>' +
                '  <p class="pdv-cart-empty-sub">Os produtos adicionados aparecerão aqui.</p>' +
                '</div>';
        } else {
            dom.productCartList.innerHTML = state.itens.map(function (item) {
                var imgUrl = String(item.imagem || assets.placeholderProduto || '').trim();
                var itemId = String(item.id);
                var qtyVal =
                    qtyEditDraft.id === itemId
                        ? qtyEditDraft.raw
                        : formatQty(item.qtd);
                var isEditingPrice = priceEditDraft.id === itemId;
                var priceVal = isEditingPrice
                    ? priceEditDraft.raw
                    : formatPriceEdit(lineSubtotal(item));
                return (
                    '' +
                    '<div class="pdv-cart-row rounded-xl border-2 border-slate-200 bg-white px-2 py-2 shadow-sm sm:px-2.5' +
                    cartRowMixClass(item) +
                    '">' +
                    '  <span class="relative h-12 w-12 shrink-0 cursor-zoom-in overflow-hidden rounded-lg border-2 border-slate-200 bg-slate-50 outline-none focus-visible:ring-2 focus-visible:ring-emerald-400" data-pdv-photo-zoom="' +
                    escapeHtml(imgUrl) +
                    '" tabindex="0" role="button" title="Ampliar foto (Enter)">' +
                    '    <img src="' +
                    escapeHtml(imgUrl) +
                    '" alt="" class="pointer-events-none h-full w-full object-cover">' +
                    '  </span>' +
                    '  <div class="pdv-cart-line overflow-hidden">' +
                    '    <span class="pdv-cart-nome">' +
                    renderCartMixNameTag(item) +
                    escapeHtml(item.nome) +
                    '</span>' +
                    '  </div>' +
                    '  <div class="pdv-cart-row-tools">' +
                    '    <span class="pdv-cart-gm" title="Código GM">' +
                    escapeHtml(cartCodigoGm(item)) +
                    '</span>' +
                    renderCartPromoBadges(item, state.itens) +
                    '    <div class="pdv-cart-qty-wrap">' +
                    '      <button type="button" data-item-qty="' +
                    escapeHtml(itemId) +
                    '" data-item-delta="-1" title="Menos">−</button>' +
                    '      <input type="text" inputmode="decimal" autocomplete="off" spellcheck="false" aria-label="Quantidade" title="Toque para digitar (ex.: 0,350 kg)" class="pdv-cart-qty-input" data-item-qty-input="' +
                    escapeHtml(itemId) +
                    '" value="' +
                    escapeHtml(qtyVal) +
                    '">' +
                    '      <button type="button" data-item-qty="' +
                    escapeHtml(itemId) +
                    '" data-item-delta="1" title="Mais">+</button>' +
                    '    </div>' +
                    '    <div class="pdv-cart-price-wrap">' +
                    '      <div class="pdv-cart-price-box">' +
                    '        <span class="pdv-cart-price-prefix" aria-hidden="true">R$</span>' +
                    '        <input type="text" inputmode="decimal" autocomplete="off" spellcheck="false" aria-label="Total da linha" title="Toque para alterar o preço unitário" class="pdv-cart-price-input" data-item-price-input="' +
                    escapeHtml(itemId) +
                    '" value="' +
                    escapeHtml(priceVal) +
                    '">' +
                    '      </div>' +
                    '    </div>' +
                    '    <button type="button" class="pdv-cart-remove" data-remove-item="' +
                    escapeHtml(itemId) +
                    '" aria-label="Remover item" title="Remover">' +
                    '      <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" d="m14.74 9-.346 9m-4.788 0L9.26 9m9.968-3.21c.342.052.682.107 1.022.166m-1.022-.165L18.16 19.673a2.25 2.25 0 0 1-2.244 2.077H8.084a2.25 2.25 0 0 1-2.244-2.077L4.772 5.79m14.456 0a48.108 48.108 0 0 0-3.478-.397m-12 .562c.34-.059.68-.114 1.022-.165m0 0a48.11 48.11 0 0 1 3.478-.397m7.5 0v-.916c0-1.18-.91-2.164-2.09-2.201a51.964 51.964 0 0 0-3.32 0c-1.18.037-2.09 1.022-2.09 2.201v.916m7.5 0a48.667 48.667 0 0 0-7.5 0" /></svg>' +
                    '    </button>' +
                    '  </div>' +
                    '</div>'
                );
            }).join('');
            if (qtyInputRestore.id) {
                var restoreInputs = dom.productCartList.querySelectorAll('[data-item-qty-input]');
                var restoreInput = null;
                for (var ri = 0; ri < restoreInputs.length; ri++) {
                    if (restoreInputs[ri].getAttribute('data-item-qty-input') === qtyInputRestore.id) {
                        restoreInput = restoreInputs[ri];
                        break;
                    }
                }
                if (restoreInput) {
                    restoreInput.focus();
                    try {
                        restoreInput.setSelectionRange(qtyInputRestore.selStart, qtyInputRestore.selEnd);
                    } catch (errSel) {
                        restoreInput.select();
                    }
                } else {
                    qtyInputRestore = { id: null, selStart: null, selEnd: null };
                }
            }
            if (priceInputRestore.id) {
                var restorePriceInputs = dom.productCartList.querySelectorAll('[data-item-price-input]');
                var restorePriceInput = null;
                for (var pi = 0; pi < restorePriceInputs.length; pi++) {
                    if (restorePriceInputs[pi].getAttribute('data-item-price-input') === priceInputRestore.id) {
                        restorePriceInput = restorePriceInputs[pi];
                        break;
                    }
                }
                if (restorePriceInput) {
                    restorePriceInput.focus();
                    try {
                        restorePriceInput.setSelectionRange(priceInputRestore.selStart, priceInputRestore.selEnd);
                    } catch (errSelP) {
                        restorePriceInput.select();
                    }
                } else {
                    priceInputRestore = { id: null, selStart: null, selEnd: null };
                }
            }
        }
        updateSearchAwaitingPulse();
        var budgetKeyNow = budgetClienteKeyFromState(state);
        if (state.clienteMode !== 'unset' && budgetKeyNow !== lastBudgetSyncKey) {
            lastBudgetSyncKey = budgetKeyNow;
            syncHistoricoOrcamentosCliente(budgetKeyNow);
        } else {
            renderRecentBudgetsSnippet();
        }
        var cidKeyNow = clienteFiadoQueryKey(state);
        if (
            clientePodeFiado(state) &&
            cidKeyNow &&
            (cidKeyNow !== creditoFiadoClienteId || !creditoFiadoCliente)
        ) {
            refreshCreditoFiadoCliente(valorFiadoNosLancamentos(state), {
                force: !creditoFiadoCliente,
                showVencidosAlert: true,
            });
        } else if (!clientePodeFiado(state) && creditoFiadoClienteId) {
            creditoFiadoCliente = null;
            creditoFiadoClienteId = '';
            renderProductFiadoBalance(state);
        }
    }

    function pdvCodigoBarrasOrcamento(orcId) {
        return 'GMORC' + String(orcId);
    }

    function pdvCsrfTokenOrcamentos() {
        try {
            if (bootstrap && bootstrap.csrfToken) return bootstrap.csrfToken;
        } catch (eCs) {}
        var m = document.querySelector('meta[name=csrfmiddlewaretoken]');
        if (m && m.getAttribute('content')) return m.getAttribute('content');
        return '';
    }

    function apiPdvOrcamentosUrl() {
        try {
            return bootstrap && bootstrap.urls && bootstrap.urls.apiPdvOrcamentos
                ? bootstrap.urls.apiPdvOrcamentos
                : '';
        } catch (eUrl) {
            return '';
        }
    }

    function apiPdvOrcamentoDetalheUrl(orcId) {
        var base = apiPdvOrcamentosUrl();
        if (!base) return '';
        return base.replace(/\/?$/, '/') + encodeURIComponent(String(orcId)) + '/';
    }

    var lastBudgetSyncKey = '';
    var _orcamentoSyncSeq = 0;

    function readHistoricoOrcamentos() {
        try {
            var historico = JSON.parse(localStorage.getItem('historicoOrcamentos') || '[]');
            return Array.isArray(historico) ? historico : [];
        } catch (errH) {
            return [];
        }
    }

    function writeHistoricoOrcamentos(historico) {
        try {
            localStorage.setItem('historicoOrcamentos', JSON.stringify(historico));
        } catch (errW) {}
    }

    function mergeOrcamentoIntoHistorico(entry) {
        if (!entry || entry.id == null) return;
        var historico = readHistoricoOrcamentos();
        historico = historico.filter(function (item) {
            return String(item.id) !== String(entry.id);
        });
        historico.unshift(entry);
        var perKey = {};
        historico = historico.filter(function (item) {
            var k = entryClienteKey(item);
            perKey[k] = (perKey[k] || 0) + 1;
            if (perKey[k] > 30) return false;
            return true;
        });
        if (historico.length > 300) historico.length = 300;
        writeHistoricoOrcamentos(historico);
    }

    function syncHistoricoOrcamentosCliente(clienteKey, opts) {
        opts = opts || {};
        var url = apiPdvOrcamentosUrl();
        if (!url) {
            if (!opts.silent) renderRecentBudgetsSnippet();
            return Promise.resolve();
        }
        var key = String(clienteKey || 'consumidor_final');
        var seq = ++_orcamentoSyncSeq;
        return fetch(url + '?cliente_key=' + encodeURIComponent(key), { credentials: 'same-origin' })
            .then(function (r) {
                return r.json();
            })
            .then(function (data) {
                if (seq !== _orcamentoSyncSeq || !data || !data.ok || !Array.isArray(data.items)) return;
                var map = {};
                readHistoricoOrcamentos().forEach(function (item) {
                    map[String(item.id)] = item;
                });
                data.items.forEach(function (item) {
                    map[String(item.id)] = item;
                });
                var merged = Object.keys(map)
                    .map(function (k) {
                        return map[k];
                    })
                    .sort(function (a, b) {
                        return Number(b.id) - Number(a.id);
                    });
                if (merged.length > 300) merged.length = 300;
                writeHistoricoOrcamentos(merged);
            })
            .catch(function () {})
            .finally(function () {
                if (!opts.silent) renderRecentBudgetsSnippet();
            });
    }

    function fetchOrcamentoFromServer(orcId) {
        var url = apiPdvOrcamentoDetalheUrl(orcId);
        if (!url) return Promise.resolve(null);
        return fetch(url, { credentials: 'same-origin' })
            .then(function (r) {
                return r.json();
            })
            .then(function (data) {
                if (data && data.ok && data.item) return data.item;
                return null;
            })
            .catch(function () {
                return null;
            });
    }

    function budgetClienteKeyFromState(state) {
        if (!state) return 'consumidor_final';
        if (state.clienteMode === 'consumidor_final') return 'consumidor_final';
        var c = state.cliente;
        if (!c) return 'consumidor_final';
        var pk = c.cliente_agro_pk != null ? String(c.cliente_agro_pk).trim() : '';
        if (pk) return 'pk:' + pk;
        var id = String(c.id || '').trim();
        if (id) return 'id:' + id;
        var tel = String(c.telefone || '').replace(/\D/g, '');
        var nome = String(c.nome || '').trim().toLowerCase();
        return 'tmp:' + nome + ':' + tel;
    }

    function entryClienteKey(entry) {
        if (!entry || typeof entry !== 'object') return 'consumidor_final';
        if (entry.cliente_key) return String(entry.cliente_key);
        if (entry.cliente_mode === 'consumidor_final') return 'consumidor_final';
        var ex = entry.cliente_extra;
        if (ex && ex.cliente_agro_pk != null) return 'pk:' + String(ex.cliente_agro_pk).trim();
        var nome = String(entry.cliente || '').trim();
        if (/consumidor\s+n[aã]o\s+identificado/i.test(nome)) return 'consumidor_final';
        var tel = ex && ex.telefone ? String(ex.telefone).replace(/\D/g, '') : '';
        return 'tmp:' + nome.toLowerCase() + ':' + tel;
    }

    function filterHistoricoPorCliente(historico, clienteKey) {
        historico = Array.isArray(historico) ? historico : [];
        var key = String(clienteKey || 'consumidor_final');
        return historico.filter(function (item) {
            return entryClienteKey(item) === key;
        });
    }

    var PDV_BUDGET_CARD_VISIBLE = 3;

    function formatBudgetCardDate(dataStr) {
        if (!dataStr) return '—';
        var raw = String(dataStr).trim();
        var parsed = new Date(raw);
        if (!isNaN(parsed.getTime())) {
            var dd = String(parsed.getDate()).padStart(2, '0');
            var mm = String(parsed.getMonth() + 1).padStart(2, '0');
            var yy = String(parsed.getFullYear()).slice(-2);
            return dd + '/' + mm + '/' + yy;
        }
        var m = raw.match(/(\d{1,2})\/(\d{1,2})\/(\d{2,4})/);
        if (m) {
            var y = m[3].length === 4 ? m[3].slice(-2) : m[3];
            return String(m[1]).padStart(2, '0') + '/' + String(m[2]).padStart(2, '0') + '/' + y;
        }
        return raw.slice(0, 8) || '—';
    }

    function reopenBudgetById(budgetId, onDone) {
        if (budgetId == null || budgetId === '') {
            if (onDone) onDone(false);
            return false;
        }
        var historico = readHistoricoOrcamentos();
        var entry = historico.find(function (item) {
            return String(item.id) === String(budgetId);
        });
        if (entry) {
            State.hydrateFromBudget(entry);
            State.setCurrentStep('produtos');
            if (onDone) onDone(true);
            return true;
        }
        fetchOrcamentoFromServer(budgetId).then(function (remote) {
            if (!remote) {
                alert('Orçamento não encontrado.');
                if (onDone) onDone(false);
                return;
            }
            mergeOrcamentoIntoHistorico(remote);
            State.hydrateFromBudget(remote);
            State.setCurrentStep('produtos');
            renderRecentBudgetsSnippet();
            if (onDone) onDone(true);
        });
        return false;
    }

    function reopenBudgetFromBarcode(raw) {
        var m = String(raw || '')
            .replace(/\s/g, '')
            .toUpperCase()
            .match(/^GMORC(\d{10,20})$/);
        if (!m) return false;
        var oid = parseInt(m[1], 10);
        if (!Number.isFinite(oid)) return false;
        reopenBudgetById(oid, function (ok) {
            if (ok && dom.productSearch) dom.productSearch.value = '';
        });
        return true;
    }

    function salvarOrcamentoWizard() {
        var state = State.getState();
        if (state.clienteMode === 'unset') {
            alert('Defina o cliente ou consumidor final antes de salvar o orçamento.');
            return;
        }
        if (!state.itens || !state.itens.length) {
            alert('Adicione itens ao carrinho antes de salvar o orçamento.');
            return;
        }
        var computed = State.getComputed();
        var historico = readHistoricoOrcamentos();
        var idOrc = Date.now();
        var clienteNome =
            state.cliente && state.cliente.nome
                ? String(state.cliente.nome).trim()
                : state.clienteMode === 'consumidor_final'
                  ? 'Consumidor não identificado'
                  : 'Cliente';
        var key = budgetClienteKeyFromState(state);
        var operadorSalvo = '';
        try {
            operadorSalvo = (localStorage.getItem('gm_sspin_operador') || '').trim();
        } catch (eOp) {}
        var usuarioSalvo = '';
        try {
            usuarioSalvo =
                bootstrap && bootstrap.usuarioSalvamento
                    ? String(bootstrap.usuarioSalvamento).trim()
                    : '';
        } catch (eUs) {}
        if (!usuarioSalvo) usuarioSalvo = operadorSalvo;
        var novo = {
            id: idOrc,
            orc_barcode: pdvCodigoBarrasOrcamento(idOrc),
            data: new Date().toLocaleString('pt-BR'),
            cliente: clienteNome,
            cliente_key: key,
            cliente_mode: state.clienteMode || 'cliente',
            total: formatMoney(computed.subtotal != null ? computed.subtotal : computed.total || 0),
            itens: state.itens.map(function (item) {
                return JSON.parse(JSON.stringify(item));
            }),
            forma_pagamento: state.pagamento && state.pagamento.forma ? state.pagamento.forma : '',
            entrega: !!(state.entrega && state.entrega.ativa),
            usuario: usuarioSalvo || undefined,
            cliente_extra: state.cliente ? JSON.parse(JSON.stringify(state.cliente)) : null,
        };
        historico.unshift(novo);
        var perKey = {};
        historico = historico.filter(function (item) {
            var k = entryClienteKey(item);
            perKey[k] = (perKey[k] || 0) + 1;
            if (perKey[k] > 30) return false;
            return true;
        });
        if (historico.length > 300) historico.length = 300;
        writeHistoricoOrcamentos(historico);
        renderRecentBudgetsSnippet();
        var doneFeedback = function () {
            showSaleDoneFeedback('Orçamento salvo para ' + clienteNome + '.', 'success', {
                title: 'Orçamento salvo',
                placementTop: true,
            });
        };
        var urlSave = apiPdvOrcamentosUrl();
        if (!urlSave) {
            doneFeedback();
            return;
        }
        if (dom.step1SalvarOrcamentoBtn) dom.step1SalvarOrcamentoBtn.disabled = true;
        fetch(urlSave, {
            method: 'POST',
            credentials: 'same-origin',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': pdvCsrfTokenOrcamentos(),
            },
            body: JSON.stringify({ entry: novo }),
        })
            .then(function (r) {
                return r.json().then(function (data) {
                    return { okHttp: r.ok, data: data };
                });
            })
            .then(function (res) {
                if (res && res.data && res.data.ok && res.data.item) {
                    mergeOrcamentoIntoHistorico(res.data.item);
                    renderRecentBudgetsSnippet();
                    doneFeedback();
                    return;
                }
                renderRecentBudgetsSnippet();
                alert(
                    (res && res.data && res.data.erro) ||
                        'Orçamento salvo só neste navegador — servidor não confirmou. Tente de novo.'
                );
            })
            .catch(function () {
                renderRecentBudgetsSnippet();
                alert('Falha de rede ao gravar orçamento no servidor. Ficou só neste navegador.');
            })
            .finally(function () {
                if (dom.step1SalvarOrcamentoBtn) dom.step1SalvarOrcamentoBtn.disabled = false;
            });
    }

    function renderRecentBudgetsSnippet() {
        var el = document.getElementById('pdv-step1-budget-snippet');
        if (!el) return;
        var state = State.getState();
        var key = budgetClienteKeyFromState(state);
        var historico = filterHistoricoPorCliente(readHistoricoOrcamentos(), key);
        var slice = historico.slice(0, PDV_BUDGET_CARD_VISIBLE);
        if (dom.step1BudgetVerMais) {
            if (historico.length > PDV_BUDGET_CARD_VISIBLE) {
                dom.step1BudgetVerMais.hidden = false;
                dom.step1BudgetVerMais.disabled = false;
            } else {
                dom.step1BudgetVerMais.hidden = true;
                dom.step1BudgetVerMais.disabled = true;
            }
        }
        if (!slice.length) {
            el.innerHTML =
                '<p class="py-0.5 text-center text-[10px] font-semibold text-slate-400">Nenhum ainda</p>';
            return;
        }
        el.innerHTML =
            '<div role="list" class="text-left text-[11px] leading-snug">' +
            slice
                .map(function (item, idx) {
                    var sep =
                        idx < slice.length - 1
                            ? ' border-b border-dashed border-slate-300'
                            : '';
                    return (
                        '<button type="button" role="listitem" data-budget-id="' +
                        escapeHtml(String(item.id)) +
                        '" class="flex w-full items-center justify-between gap-2 px-1 py-1.5 text-left transition hover:bg-emerald-50 focus:outline-none focus-visible:ring-2 focus-visible:ring-emerald-400' +
                        sep +
                        '" title="Reabrir orçamento · ' +
                        escapeHtml(item.data || '') +
                        '">' +
                        '<span class="shrink-0 font-bold tabular-nums text-slate-700">' +
                        escapeHtml(formatBudgetCardDate(item.data)) +
                        '</span>' +
                        '<span class="min-w-0 truncate text-right font-mono font-black tabular-nums text-slate-800">' +
                        escapeHtml(item.total || '—') +
                        '</span>' +
                        '</button>'
                    );
                })
                .join('') +
            '</div>';
    }

    function applyEntregasPendentesButton() {
        var n = entregasPendentesCache.total || 0;
        var apiOk = !!String(urls.apiPdvEntregasPendentes || '').trim();
        var discreteTop =
            'pdv-action-btn pdv-wiz-topbar-btn pdv-wiz-topbar-btn--slate relative';
        var alertTop =
            'pdv-action-btn pdv-wiz-topbar-btn pdv-wiz-topbar-btn--slate pdv-wiz-topbar-btn--entregas-pendente relative';

        if (dom.topbarEntregasBtn) {
            dom.topbarEntregasBtn.hidden = !apiOk;
            dom.topbarEntregasBtn.className = n > 0 ? alertTop : discreteTop;
        }
        if (dom.topbarEntregasCount) {
            if (n > 0) {
                dom.topbarEntregasCount.textContent = String(n);
                dom.topbarEntregasCount.classList.remove('hidden');
            } else {
                dom.topbarEntregasCount.classList.add('hidden');
            }
        }
    }

    function renderEntregasPendentesList() {
        var el = dom.entregasPendentesList;
        if (!el) return;
        var itens = entregasPendentesCache.itens || [];
        if (!itens.length) {
            el.innerHTML =
                '<p class="py-8 text-center text-sm font-bold text-slate-500">Nenhuma pendência agora.</p>';
            return;
        }
        el.innerHTML = itens
            .map(function (row) {
                var id = row.id;
                var nome = escapeHtml(row.cliente_nome || '—');
                var total = escapeHtml(row.total_texto || '—');
                var forma = escapeHtml(row.forma_pagamento || '');
                var cod = escapeHtml(row.retomar_codigo || '');
                var caixaLbl = escapeHtml(row.sessao_caixa_label || '');
                return (
                    '<article class="mb-2 rounded-xl border-2 border-orange-200 bg-orange-50/40 p-3">' +
                    '<div class="font-black text-slate-900">' +
                    nome +
                    '</div>' +
                    '<div class="mt-1 text-xs font-bold text-slate-600">' +
                    total +
                    (forma ? ' · ' + forma : '') +
                    '</div>' +
                    (caixaLbl
                        ? '<div class="mt-0.5 text-[10px] font-bold uppercase text-orange-800">' +
                          caixaLbl +
                          '</div>'
                        : '') +
                    (cod ? '<div class="mt-0.5 text-[10px] font-mono text-slate-500">' + cod + '</div>' : '') +
                    '<div class="mt-3 flex flex-wrap gap-2">' +
                    '<button type="button" class="pdv-entrega-retomar rounded-lg bg-emerald-600 px-3 py-2 text-[10px] font-black uppercase text-white" data-entrega-id="' +
                    id +
                    '">Retomar pagamento</button>' +
                    '<button type="button" class="pdv-entrega-cancelar rounded-lg border-2 border-red-300 bg-white px-3 py-2 text-[10px] font-black uppercase text-red-800" data-entrega-id="' +
                    id +
                    '">Cancelar</button>' +
                    '</div>' +
                    '</article>'
                );
            })
            .join('');
        el.querySelectorAll('.pdv-entrega-retomar').forEach(function (btn) {
            btn.addEventListener('click', function () {
                var pk = btn.getAttribute('data-entrega-id');
                if (pk) retomarEntregaPendente(pk);
            });
        });
        el.querySelectorAll('.pdv-entrega-cancelar').forEach(function (btn) {
            btn.addEventListener('click', function () {
                var pk = btn.getAttribute('data-entrega-id');
                if (pk) cancelarEntregaPendente(pk);
            });
        });
    }

    function openEntregasPendentesModal() {
        refreshEntregasPendentesUi(false).then(function () {
            if (!dom.entregasPendentesModal) return;
            renderEntregasPendentesList();
            if (typeof dom.entregasPendentesModal.showModal === 'function') {
                dom.entregasPendentesModal.showModal();
            } else {
                dom.entregasPendentesModal.setAttribute('open', 'open');
            }
            syncPdvSspinIdlePause();
        });
    }

    function closeEntregasPendentesModal() {
        if (!dom.entregasPendentesModal) return;
        if (typeof dom.entregasPendentesModal.close === 'function') {
            dom.entregasPendentesModal.close();
        } else {
            dom.entregasPendentesModal.removeAttribute('open');
        }
        syncPdvSspinIdlePause();
    }

    function refreshEntregasPendentesUi(silent) {
        var url = urls.apiPdvEntregasPendentes;
        if (!url) return Promise.resolve();
        if (window.AgroPdvOfflineCache) {
            var cached = window.AgroPdvOfflineCache.readPayload(ENTREGAS_PENDENTES_LS_KEY);
            if (cached && Array.isArray(cached.itens)) {
                entregasPendentesCache.total = cached.total || 0;
                entregasPendentesCache.itens = cached.itens;
                applyEntregasPendentesButton();
                if (
                    !silent &&
                    dom.entregasPendentesModal &&
                    dom.entregasPendentesModal.open
                ) {
                    renderEntregasPendentesList();
                }
                if (!window.AgroPdvOfflineCache.isStale(ENTREGAS_PENDENTES_LS_KEY, window.AgroPdvOfflineCache.TTL.ENTREGAS_MS)) {
                    return Promise.resolve();
                }
            }
        }
        return jsonGet(url)
            .then(function (res) {
                if (!res.ok || !res.data || !res.data.ok) return;
                entregasPendentesCache.total = res.data.total || 0;
                entregasPendentesCache.itens = res.data.itens || [];
                if (window.AgroPdvOfflineCache) {
                    window.AgroPdvOfflineCache.writePayload(ENTREGAS_PENDENTES_LS_KEY, {
                        total: entregasPendentesCache.total,
                        itens: entregasPendentesCache.itens,
                    });
                }
                applyEntregasPendentesButton();
                if (
                    !silent &&
                    dom.entregasPendentesModal &&
                    dom.entregasPendentesModal.open
                ) {
                    renderEntregasPendentesList();
                }
            })
            .catch(function () {});
    }

    function retomarEntregaPendente(pk) {
        var url = entregaPendenteApiUrl(urls.apiPdvEntregaPendenteDetalhe, pk);
        if (!url) return;
        if (window.gmLoadingBar) window.gmLoadingBar.show();
        jsonGet(url)
            .then(function (res) {
                if (!res.ok || !res.data || !res.data.ok || !res.data.entrega) {
                    throw new Error(
                        (res.data && (res.data.erro || res.data.mensagem)) ||
                            'Não foi possível carregar a entrega.'
                    );
                }
                var ent = res.data.entrega;
                var snap = ent.pdv_wizard_state;
                if (!snap || typeof snap !== 'object') {
                    throw new Error('Estado da venda não encontrado para retomar.');
                }
                closeEntregasPendentesModal();
                closeStartModal();
                State.hydrateFromEntregaPendente(snap, { id: ent.id });
                showSaleDoneFeedback(
                    'Venda retomada (entrega #' + ent.id + '). Registre o pagamento e confirme a venda.',
                    'info'
                );
            })
            .catch(function (err) {
                showSaleDoneFeedback(
                    err && err.message ? err.message : 'Falha ao retomar entrega.',
                    'warn'
                );
            })
            .finally(function () {
                if (window.gmLoadingBar) window.gmLoadingBar.hide();
            });
    }

    function cancelarEntregaPendente(pk) {
        var motivo = window.prompt('Motivo do cancelamento (opcional):', 'Cancelado no PDV');
        if (motivo === null) return;
        var url = entregaPendenteApiUrl(urls.apiPdvEntregaPendenteCancelar, pk);
        if (!url) return;
        if (window.gmLoadingBar) window.gmLoadingBar.show();
        jsonPost(url, { motivo: motivo || 'Cancelado no PDV' })
            .then(function (res) {
                if (!res.ok || !res.data || !res.data.ok) {
                    throw new Error(
                        (res.data && (res.data.erro || res.data.mensagem)) ||
                            'Não foi possível cancelar.'
                    );
                }
                return refreshEntregasPendentesUi(false);
            })
            .then(function () {
                renderEntregasPendentesList();
                if (!(entregasPendentesCache.total || 0)) closeEntregasPendentesModal();
            })
            .catch(function (err) {
                alert(err && err.message ? err.message : 'Falha ao cancelar entrega.');
            })
            .finally(function () {
                if (window.gmLoadingBar) window.gmLoadingBar.hide();
            });
    }

    function finalizarEntregaPendenteAposVenda(pendenteId, vendaId) {
        var url = entregaPendenteApiUrl(urls.apiPdvEntregaPendenteFinalizar, pendenteId);
        if (!url) return Promise.resolve({ ok: true, data: { ok: true } });
        return jsonPost(url, { venda_id: vendaId != null ? vendaId : null });
    }

    function productAutocompleteHeaderHtml() {
        return (
            '<div class="pdv-ac-head" aria-hidden="true">' +
            '<span></span><span>Produto</span><span>GM</span><span>Marca</span><span>Preço</span>' +
            '</div>'
        );
    }

    function shouldShowAutocompleteLoadMore() {
        if (lastProducts.length > autocompleteVisibleLimit) return true;
        if (productSearchAwaitingServer && lastProducts.length >= autocompleteVisibleLimit) return true;
        return productSearchMayHaveMore && lastProducts.length >= AUTOCOMPLETE_PAGE_SIZE;
    }

    function autocompleteViewportCap() {
        var ac = dom.productAutocomplete;
        if (!ac || ac.classList.contains('hidden')) return Infinity;
        var top = ac.getBoundingClientRect().top;
        var vh = window.visualViewport ? window.visualViewport.height : window.innerHeight;
        return Math.max(160, vh - top - 16);
    }

    function syncAutocompletePanelHeight() {
        var ac = dom.productAutocomplete;
        if (!ac || ac.classList.contains('hidden') || !lastProducts.length) return;

        var scrollEl = ac.querySelector('.pdv-ac-scroll');
        if (!scrollEl) return;

        var visibleCount = Math.min(lastProducts.length, autocompleteVisibleLimit);
        var hasLoadMore = shouldShowAutocompleteLoadMore();
        var needsScroll = visibleCount > AUTOCOMPLETE_SCROLL_THRESHOLD;
        var measureRowCount = needsScroll ? AUTOCOMPLETE_SCROLL_THRESHOLD : visibleCount;

        ac.style.maxHeight = 'none';
        scrollEl.style.maxHeight = 'none';
        scrollEl.style.overflowY = 'hidden';
        ac.style.overflowY = 'hidden';

        var head = scrollEl.querySelector('.pdv-ac-head');
        var rows = scrollEl.querySelectorAll('.pdv-ac-row');
        var loadMore = ac.querySelector('.pdv-ac-load-more');

        var scrollPart = 0;
        if (head) scrollPart += head.getBoundingClientRect().height;
        for (var i = 0; i < measureRowCount && i < rows.length; i++) {
            scrollPart += rows[i].getBoundingClientRect().height;
        }

        var loadMorePart = 0;
        if (hasLoadMore && loadMore) {
            loadMorePart = loadMore.getBoundingClientRect().height;
        }

        var totalHeight = Math.ceil(scrollPart + loadMorePart + 8);
        var viewportCap = autocompleteViewportCap();
        var scrollOverflow = needsScroll;

        if (totalHeight > viewportCap) {
            scrollPart = Math.max(96, viewportCap - loadMorePart - 8);
            totalHeight = Math.ceil(scrollPart + loadMorePart + 8);
            scrollOverflow = true;
        }

        ac.setAttribute('data-pdv-ac-layout', scrollOverflow ? 'scroll' : 'fit');
        ac.style.setProperty('max-height', Math.min(totalHeight, viewportCap) + 'px', 'important');
        scrollEl.style.maxHeight = Math.ceil(scrollPart) + 'px';
        scrollEl.style.overflowY = scrollOverflow ? 'auto' : 'hidden';

        if (productSelectionIndex >= 0) {
            var sel = scrollEl.querySelector(
                '[data-autocomplete-index="' + productSelectionIndex + '"]'
            );
            if (sel && sel.scrollIntoView) {
                sel.scrollIntoView({ block: 'nearest' });
            }
        }
    }

    var autocompleteResizeTimer = null;
    function onAutocompleteViewportChange() {
        if (
            !lastProducts.length ||
            !dom.productAutocomplete ||
            dom.productAutocomplete.classList.contains('hidden')
        ) {
            return;
        }
        clearTimeout(autocompleteResizeTimer);
        autocompleteResizeTimer = setTimeout(function () {
            syncAutocompletePanelHeight();
        }, 40);
    }

    function clearAutocompletePanelHeight() {
        var ac = dom.productAutocomplete;
        if (!ac) return;
        ac.removeAttribute('data-pdv-ac-layout');
        ac.style.removeProperty('max-height');
        ac.style.removeProperty('overflow-y');
    }

    function productAutocompleteLoadMoreHtml() {
        var waiting = productSearchAwaitingServer && lastProducts.length <= autocompleteVisibleLimit;
        return (
            '<button type="button" class="pdv-ac-load-more" data-autocomplete-load-more="1"' +
            (waiting ? ' disabled' : '') +
            '>' +
            (waiting ? 'carregando…' : 'carregar mais...') +
            '</button>'
        );
    }

    function productAutocompleteHtml(produto, index) {
        var selected = index === productSelectionIndex;
        var gm = displayCodigoGm(produto);
        var marca = String(produto.marca || '').trim() || '—';
        var imgUrl = String(produto.imagem || assets.placeholderProduto || '').trim();
        var pid = resolveProdutoId(produto);
        return (
            '' +
            '<button type="button" class="pdv-ac-row ' +
            (selected ? 'pdv-ac-row-selected' : '') +
            '" data-add-product="' +
            escapeHtml(pid) +
            '" data-autocomplete-index="' +
            index +
            '">' +
            '  <span class="pdv-ac-thumb" data-pdv-photo-zoom="' +
            escapeHtml(imgUrl) +
            '" tabindex="-1" role="presentation" title="Ampliar foto">' +
            '    <img src="' +
            escapeHtml(imgUrl) +
            '" alt="">' +
            '  </span>' +
            '  <span class="pdv-ac-nome">' +
            escapeHtml(produto.nome || '') +
            '</span>' +
            '  <span class="pdv-ac-gm">' +
            escapeHtml(gm) +
            '</span>' +
            '  <span class="pdv-ac-marca">' +
            escapeHtml(marca) +
            '</span>' +
            '  <span class="pdv-ac-preco"><span class="pdv-ac-preco-box">' +
            escapeHtml(formatMoney(produto.preco_venda || 0)) +
            '</span></span>' +
            '</button>'
        );
    }

    function invalidatePendingProductSearch() {
        clearTimeout(searchTimer);
        searchTimer = null;
        clearTimeout(barcodeTimer);
        barcodeTimer = null;
        filterSeq += 1;
    }

    function hideProductAutocomplete(opts) {
        opts = opts || {};
        if (!opts.skipSnapshot && lastProducts.length) {
            productSearchDismissedSnapshot = {
                products: lastProducts.slice(),
                index: Math.max(productSelectionIndex, 0),
                query: String((dom.productSearch && dom.productSearch.value) || '').trim(),
            };
        }
        productSelectionIndex = -1;
        lastProducts = [];
        autocompleteVisibleLimit = AUTOCOMPLETE_PAGE_SIZE;
        productSearchAwaitingServer = false;
        productSearchMayHaveMore = false;
        clearAutocompletePanelHeight();
        if (dom.productAutocomplete) {
            dom.productAutocomplete.innerHTML = '';
            dom.productAutocomplete.classList.add('hidden');
        }
    }

    function clearProductSearchDismissedSnapshot() {
        productSearchDismissedSnapshot = null;
    }

    function isProductAutocompleteOpen() {
        return !!(dom.productAutocomplete && !dom.productAutocomplete.classList.contains('hidden'));
    }

    function resolveEnterProductPick(qEnter) {
        if (isProductAutocompleteOpen() && lastProducts.length) {
            var liveIdx = Math.max(productSelectionIndex, 0);
            if (liveIdx >= lastProducts.length) liveIdx = 0;
            return lastProducts[liveIdx];
        }
        var snap = productSearchDismissedSnapshot;
        if (
            snap &&
            snap.products &&
            snap.products.length &&
            String(snap.query || '') === String(qEnter || '')
        ) {
            var snapIdx = Math.max(snap.index, 0);
            if (snapIdx >= snap.products.length) snapIdx = 0;
            return snap.products[snapIdx];
        }
        return null;
    }

    function isInsideProductSearchZone(node) {
        if (!node || typeof node.closest !== 'function') return false;
        var wrap = dom.productSearchWrap || document.getElementById('pdv-product-search-wrap');
        return !!(wrap && wrap.contains(node));
    }

    function markProductSearchPointerInside() {
        productSearchPointerInside = true;
        productSearchSuppressDismissUntil = Date.now() + 600;
        clearTimeout(productSearchPointerTimer);
        productSearchPointerTimer = setTimeout(function () {
            productSearchPointerInside = false;
        }, 600);
    }

    function shouldSuppressProductAutocompleteDismiss() {
        return Date.now() < productSearchSuppressDismissUntil || productSearchPointerInside;
    }

    function dismissProductAutocomplete() {
        if (shouldSuppressProductAutocompleteDismiss()) return;
        if (!dom.productAutocomplete || dom.productAutocomplete.classList.contains('hidden')) return;
        hideProductAutocomplete();
        if (dom.productSearchMeta) {
            var q = String((dom.productSearch && dom.productSearch.value) || '').trim();
            dom.productSearchMeta.textContent = q
                ? 'Lista recolhida · Enter ou digite de novo'
                : '↑↓ Enter · +/− último';
        }
        updateSearchAwaitingPulse();
    }

    function expandProductAutocomplete() {
        if (!lastProducts.length) return;
        if (
            autocompleteVisibleLimit >= lastProducts.length &&
            productSearchAwaitingServer
        ) {
            return;
        }
        if (autocompleteVisibleLimit >= lastProducts.length) return;
        autocompleteVisibleLimit = Math.min(
            autocompleteVisibleLimit + AUTOCOMPLETE_PAGE_SIZE,
            lastProducts.length
        );
        renderProductResults(lastProducts, { preserveLimit: true });
    }

    function renderProductResults(produtos, opts) {
        opts = opts || {};
        var normalized = normalizeWizardCatalogList(produtos);
        if (!opts.preserveLimit) {
            autocompleteVisibleLimit = AUTOCOMPLETE_PAGE_SIZE;
            clearProductSearchDismissedSnapshot();
        }
        lastProducts = normalized;
        var visibleCount = Math.min(lastProducts.length, autocompleteVisibleLimit);
        if (lastProducts.length) {
            if (productSelectionIndex < 0 || productSelectionIndex >= visibleCount) {
                productSelectionIndex = 0;
            }
        } else {
            productSelectionIndex = -1;
        }
        if (dom.productAutocomplete) {
            if (lastProducts.length) {
                var rowsHtml = lastProducts
                    .slice(0, autocompleteVisibleLimit)
                    .map(function (produto, index) {
                        return productAutocompleteHtml(produto, index);
                    })
                    .join('');
                var loadMoreHtml = shouldShowAutocompleteLoadMore()
                    ? productAutocompleteLoadMoreHtml()
                    : '';
                dom.productAutocomplete.innerHTML =
                    '<div class="pdv-ac-scroll">' +
                    productAutocompleteHeaderHtml() +
                    rowsHtml +
                    '</div>' +
                    loadMoreHtml;
                dom.productAutocomplete.classList.remove('hidden');
            } else {
                dom.productAutocomplete.innerHTML = '';
                dom.productAutocomplete.classList.add('hidden');
            }
        }
        if (!lastProducts.length) {
            var qEmpty = String((dom.productSearch && dom.productSearch.value) || '').trim();
            dom.productSearchMeta.textContent = qEmpty ? 'Sem resultados' : '↑↓ Enter · +/− último';
            updateSearchAwaitingPulse();
            return;
        }
        if (shouldShowAutocompleteLoadMore()) {
            dom.productSearchMeta.textContent =
                visibleCount +
                ' de ' +
                (lastProducts.length > visibleCount ? lastProducts.length : visibleCount + '+') +
                ' na lista · carregar mais abaixo · Enter · +/− último';
        } else {
            dom.productSearchMeta.textContent = lastProducts.length + ' na lista · Enter · +/− último';
        }
        updateSearchAwaitingPulse();
        requestAnimationFrame(function () {
            requestAnimationFrame(function () {
                syncAutocompletePanelHeight();
            });
        });
    }

    function renderEntregaClienteCampos(state) {
        var cl = state.cliente || {};
        setInputValueUnlessFocused(dom.entregaClienteNome, currentClientName(state));
        setInputValueUnlessFocused(dom.entregaClienteTelefone, cl.telefone || '');
        if (dom.clienteZapWhatsapp) {
            var tel = dom.entregaClienteTelefone
                ? dom.entregaClienteTelefone.value
                : cl.telefone;
            var wz = whatsappHrefCliente(tel);
            if (wz) {
                dom.clienteZapWhatsapp.href = wz;
                dom.clienteZapWhatsapp.classList.remove('hidden');
            } else {
                dom.clienteZapWhatsapp.href = '#';
                dom.clienteZapWhatsapp.classList.add('hidden');
            }
        }
    }

    function clienteEntregaSnapshotFromDom() {
        return {
            nome: dom.entregaClienteNome ? String(dom.entregaClienteNome.value || '').trim() : '',
            telefone: dom.entregaClienteTelefone ? String(dom.entregaClienteTelefone.value || '').trim() : '',
            logradouro: dom.entregaLogradouro ? String(dom.entregaLogradouro.value || '').trim() : '',
            numero: dom.entregaNumero ? String(dom.entregaNumero.value || '').trim() : '',
            bairro: dom.entregaBairro ? String(dom.entregaBairro.value || '').trim() : '',
            plus_code: dom.entregaPluscode ? String(dom.entregaPluscode.value || '').trim() : '',
            complemento: dom.entregaComplemento ? String(dom.entregaComplemento.value || '').trim() : '',
            referencia: dom.entregaReferencia ? String(dom.entregaReferencia.value || '').trim() : ''
        };
    }

    function clienteEntregaTelefoneNormalizado(tel) {
        return String(tel || '').replace(/\D/g, '');
    }

    function clienteEntregaSnapshotsIguais(a, b) {
        if (!a || !b) return true;
        var keys = [
            'nome',
            'telefone',
            'logradouro',
            'numero',
            'bairro',
            'plus_code',
            'complemento',
            'referencia'
        ];
        for (var i = 0; i < keys.length; i++) {
            var k = keys[i];
            var va = String(a[k] || '').trim();
            var vb = String(b[k] || '').trim();
            if (k === 'telefone') {
                if (clienteEntregaTelefoneNormalizado(va) !== clienteEntregaTelefoneNormalizado(vb)) {
                    return false;
                }
            } else if (va !== vb) {
                return false;
            }
        }
        return true;
    }

    function clienteEntregaSnapshotFromState(state) {
        state = state || State.getState();
        var c = state.cliente || {};
        var e = state.entrega || {};
        return {
            nome: String(c.nome || '').trim(),
            telefone: String(c.telefone || '').trim(),
            logradouro: String(e.logradouro || c.logradouro || '').trim(),
            numero: String(e.numero || c.numero || '').trim(),
            bairro: String(e.bairro || c.bairro || '').trim(),
            plus_code: String(e.plusCode || c.plus_code || '').trim(),
            complemento: String(e.complemento || '').trim(),
            referencia: String(e.referencia || c.referencia_rural || '').trim()
        };
    }

    function resetEntregaClienteSnapshot() {
        entregaClienteSnapshot = null;
        entregaEnderecoEditadoPeloUsuario = false;
        if (entregaClienteSnapshotTimer) {
            clearTimeout(entregaClienteSnapshotTimer);
            entregaClienteSnapshotTimer = null;
        }
        agendarCapturaEntregaClienteSnapshot();
    }

    function marcarEntregaEnderecoEditadoPeloUsuario() {
        entregaEnderecoEditadoPeloUsuario = true;
    }

    function agendarCapturaEntregaClienteSnapshot() {
        if (entregaClienteSnapshotTimer) clearTimeout(entregaClienteSnapshotTimer);
        if (entregaFaseAtual() !== 'endereco') return;
        entregaClienteSnapshotTimer = setTimeout(function () {
            entregaClienteSnapshotTimer = null;
            if (entregaFaseAtual() !== 'endereco') return;
            if (entregaEnderecoEditadoPeloUsuario || entregaClienteSnapshot) return;
            entregaClienteSnapshot = clienteEntregaSnapshotFromDom();
        }, 400);
    }

    function garantirEntregaClienteSnapshotInicial() {
        if (!entregaClienteSnapshot && entregaFaseAtual() === 'endereco') {
            entregaClienteSnapshot = clienteEntregaSnapshotFromDom();
        }
    }

    function clienteEntregaDadosAlterados() {
        if (!entregaClienteSnapshot) return false;
        return !clienteEntregaSnapshotsIguais(
            entregaClienteSnapshot,
            clienteEntregaSnapshotFromDom()
        );
    }

    function commitEntregaClienteCamposFromDom() {
        var st = State.getState();
        if (!st.cliente) return;
        var nome = dom.entregaClienteNome ? String(dom.entregaClienteNome.value || '').trim() : '';
        var tel = dom.entregaClienteTelefone
            ? String(dom.entregaClienteTelefone.value || '').trim()
            : '';
        var c = Object.assign({}, st.cliente);
        if (nome) c.nome = nome;
        if (tel || dom.entregaClienteTelefone) c.telefone = tel;
        c.endereco = composeClienteEnderecoLinha(c) || c.endereco;
        State.setCliente(c, st.clienteMode);
    }

    function aplicarEntregaEnderecoNoClienteMemoria() {
        var st = State.getState();
        if (!st.cliente) return;
        var e = st.entrega || {};
        var c = Object.assign({}, st.cliente);
        if (e.logradouro) c.logradouro = e.logradouro;
        if (e.numero) c.numero = e.numero;
        if (e.bairro) c.bairro = e.bairro;
        if (e.plusCode) c.plus_code = e.plusCode;
        if (e.referencia) c.referencia_rural = e.referencia;
        c.endereco = composeClienteEnderecoLinha(c) || buildLinhaEnderecoEntrega(st) || c.endereco;
        State.setCliente(c, st.clienteMode);
    }

    function buildPayloadSalvarClienteEntrega(state) {
        commitEntregaClienteCamposFromDom();
        commitEntregaCamposEndereco({ trimEnds: true });
        state = state || State.getState();
        aplicarEntregaEnderecoNoClienteMemoria();
        state = State.getState();
        var c = state.cliente || {};
        var e = state.entrega || {};
        return {
            nome: String(c.nome || '').trim(),
            whatsapp: String(c.telefone || '').trim(),
            logradouro: String(e.logradouro || c.logradouro || '').trim(),
            numero: String(e.numero || c.numero || '').trim(),
            bairro: String(e.bairro || c.bairro || '').trim(),
            cidade: String(c.cidade || '').trim(),
            uf: String(c.uf || '').trim(),
            cep: String(c.cep || '').trim(),
            complemento: String(e.complemento || '').trim(),
            plus_code: String(e.plusCode || c.plus_code || '').trim(),
            referencia_rural: String(e.referencia || c.referencia_rural || '').trim(),
            cpf: clienteCpfEffective(c)
        };
    }

    function isEntregaSalvarClienteModalOpen() {
        var r = document.getElementById('modal-pdv-entrega-salvar-cliente');
        return !!(r && !r.classList.contains('hidden'));
    }

    function closeEntregaSalvarClienteModal() {
        var root = document.getElementById('modal-pdv-entrega-salvar-cliente');
        if (root) {
            root.classList.add('hidden');
            root.classList.remove('flex');
        }
        pdvTryRemoveModalOpenBody();
    }

    function openEntregaSalvarClienteModal() {
        var root = document.getElementById('modal-pdv-entrega-salvar-cliente');
        if (!root) {
            if (entregaPendingAfterSaveCliente) entregaPendingAfterSaveCliente();
            entregaPendingAfterSaveCliente = null;
            return;
        }
        root.classList.remove('hidden');
        root.classList.add('flex');
        pdvEnsureModalOpenBody();
    }

    function continuarAposEnderecoEntrega() {
        var state = State.getState();
        if (entregaTaxaDevePularAuto(state)) {
            aplicarEntregaTaxaGratisAuto();
        }
        syncEntregaDetalhesModalUi();
        scrollEntregaWizardIntoView();
    }

    function tentarModalSalvarClienteAposEndereco() {
        garantirEntregaClienteSnapshotInicial();
        var state = State.getState();
        if (!clienteEntregaDadosAlterados() || !clienteAgroPkFromCliente(state.cliente)) {
            return false;
        }
        commitEntregaClienteCamposFromDom();
        commitEntregaCamposEndereco({ trimEnds: true });
        entregaPendingAfterSaveCliente = continuarAposEnderecoEntrega;
        openEntregaSalvarClienteModal();
        return true;
    }

    function finalizarEscolhaSalvarCliente(salvarNoCadastro) {
        var continuar = entregaPendingAfterSaveCliente;
        entregaPendingAfterSaveCliente = null;
        if (!salvarNoCadastro) {
            aplicarEntregaEnderecoNoClienteMemoria();
            entregaClienteSnapshot = clienteEntregaSnapshotFromDom();
            closeEntregaSalvarClienteModal();
            if (continuar) continuar();
            return;
        }
        var st = State.getState();
        var pk = clienteAgroPkFromCliente(st.cliente);
        var pattern = urls.apiPdvClienteEditarPattern;
        if (!pk || !pattern) {
            alert('Não foi possível salvar no cadastro (cliente sem vínculo local).');
            closeEntregaSalvarClienteModal();
            if (continuar) continuar();
            return;
        }
        var payload = buildPayloadSalvarClienteEntrega(st);
        if (payload.nome.length < 2) {
            alert('Informe o nome do cliente (mínimo 2 caracteres).');
            entregaPendingAfterSaveCliente = continuar;
            openEntregaSalvarClienteModal();
            return;
        }
        var waDigits = String(payload.whatsapp || '').replace(/\D/g, '');
        if (waDigits.length < 10) {
            alert('Informe o telefone com DDD (mínimo 10 dígitos).');
            entregaPendingAfterSaveCliente = continuar;
            openEntregaSalvarClienteModal();
            return;
        }
        aplicarEntregaEnderecoNoClienteMemoria();
        entregaClienteSnapshot = clienteEntregaSnapshotFromDom();
        closeEntregaSalvarClienteModal();
        if (continuar) continuar();
        jsonPost(pattern.replace('__pk__', String(pk)), payload)
            .then(function (res) {
                if (!res.ok || !res.data || !res.data.ok) {
                    showSaleDoneFeedback(
                        (res.data && (res.data.erro || res.data.error)) ||
                            'Não foi possível salvar o cadastro do cliente. Confira no cadastro depois.',
                        'warn',
                        { placementTop: true, title: 'Cadastro do cliente' }
                    );
                    return;
                }
                patchClienteInSearchResults(res.data.cliente);
                syncEntregaEnderecoFromCliente(State.getState());
                entregaClienteSnapshot = clienteEntregaSnapshotFromDom();
            })
            .catch(function () {
                showSaleDoneFeedback(
                    'Erro de rede ao salvar o cadastro. Confira no cadastro depois.',
                    'warn',
                    { placementTop: true, title: 'Cadastro do cliente' }
                );
            });
    }

    function tryProsseguirEntregaStep() {
        if (abrirFluxoPagamentoEntregaSePendente()) return;
        var state = State.getState();
        var lp = String((state.entrega && state.entrega.localPagamento) || '');
        if (lp === 'loja') {
            wizardIrParaPagamentoComImpressao();
            return;
        }
        if (lp === 'entrega') {
            wizardEnviarEntregaPainel();
        }
    }

    function prepararEntregaAoSairDeProdutos() {
        State.setEntregaPatch({
            modoRetiradaEntrega: 'entrega',
            ativa: true
        });
        syncEntregaEnderecoFromCliente();
    }

    function ensureEntregaModoNaEtapa() {
        if (_ensuringEntregaModo) return;
        var state = State.getState();
        if (state.currentStep !== 'entrega') return;
        var modo = String((state.entrega && state.entrega.modoRetiradaEntrega) || '').trim();
        if (modo !== 'entrega') {
            _ensuringEntregaModo = true;
            prepararEntregaAoSairDeProdutos();
            _ensuringEntregaModo = false;
        }
    }

    function entregaModoEfetivo(state) {
        return String((state.entrega && state.entrega.modoRetiradaEntrega) || '').trim();
    }

    function onEntregaBtnNext() {
        commitEntregaClienteCamposFromDom();
        commitEntregaCamposEndereco({ trimEnds: true });
        commitEntregaObsFromDom();
        var state = State.getState();
        var computed = State.getComputed();
        var fase = entregaFaseAtual(state);
        if (fase === 'pagamento_local' || fase === 'meio') {
            abrirFluxoPagamentoEntregaSePendente();
            return;
        }
        if (fase === 'detalhes') {
            confirmarEntregaDetalhesModal();
            return;
        }
        if (fase === 'troco') {
            confirmarEntregaTrocoModal();
            return;
        }
        if (fase === 'endereco') {
            if (!enderecoEntregaMinimoOkParaUi(state)) {
                alert('Informe o endereço básico da entrega (logradouro e bairro ou endereço legível).');
                return;
            }
            commitEntregaCamposEndereco({ trimEnds: true });
            State.setEntregaPatch({ enderecoPassoConcluido: true });
            if (tentarModalSalvarClienteAposEndereco()) return;
            continuarAposEnderecoEntrega();
            return;
        }
        var validation = canAdvance(state, computed);
        if (validation) {
            alert(validation);
            return;
        }
        tryProsseguirEntregaStep();
    }

    function commitEntregaCamposEndereco(opts) {
        opts = opts || {};
        var trimEnds = !!opts.trimEnds;
        function campoEndereco(el, sempreTrim) {
            if (!el) return '';
            var v = String(el.value != null ? el.value : '');
            return sempreTrim || trimEnds ? v.trim() : v;
        }
        var st = State.getState();
        var e0 = st.entrega || {};
        var e = Object.assign({}, e0, {
            logradouro: campoEndereco(dom.entregaLogradouro, false),
            numero: campoEndereco(dom.entregaNumero, true),
            bairro: dom.entregaBairro ? dom.entregaBairro.value : '',
            plusCode: campoEndereco(dom.entregaPluscode, false)
        });
        var line = buildLinhaEnderecoEntrega({ entrega: e, cliente: st.cliente });
        State.setEntregaPatch({
            logradouro: e.logradouro,
            numero: e.numero,
            bairro: e.bairro,
            plusCode: e.plusCode,
            endereco: line
        });
    }

    function applyEntregaPlusGeocodeEndereco(endereco, overwrite) {
        endereco = endereco || {};
        function setIf(el, val) {
            if (!el || val == null || val === '') return;
            if (overwrite || !String(el.value || '').trim()) el.value = val;
        }
        setIf(dom.entregaLogradouro, endereco.logradouro);
        setIf(dom.entregaNumero, endereco.numero);
        if (dom.entregaBairro && endereco.bairro) {
            var bai = bairroListaJacupiOuVazio(endereco.bairro);
            if (bai && (overwrite || !String(dom.entregaBairro.value || '').trim())) {
                dom.entregaBairro.value = bai;
            }
        }
        commitEntregaCamposEndereco({ trimEnds: true });
        if (!entregaEnderecoEditadoPeloUsuario && entregaFaseAtual() === 'endereco') {
            entregaClienteSnapshot = clienteEntregaSnapshotFromDom();
        }
        var st = State.getState();
        if (!st.cliente) return;
        var pc = dom.entregaPluscode ? String(dom.entregaPluscode.value || '').trim() : '';
        var c = Object.assign({}, st.cliente);
        if (endereco.logradouro && (overwrite || !String(c.logradouro || '').trim())) {
            c.logradouro = endereco.logradouro;
        }
        if (endereco.numero && (overwrite || !String(c.numero || '').trim())) c.numero = endereco.numero;
        if (endereco.bairro) {
            var b2 = bairroListaJacupiOuVazio(endereco.bairro);
            if (b2 && (overwrite || !String(c.bairro || '').trim())) c.bairro = b2;
        }
        if (endereco.cidade && (overwrite || !String(c.cidade || '').trim())) c.cidade = endereco.cidade;
        if (endereco.uf && (overwrite || !String(c.uf || '').trim())) c.uf = endereco.uf;
        if (endereco.cep && (overwrite || !String(c.cep || '').trim())) c.cep = endereco.cep;
        if (pc) c.plus_code = pc;
        c.endereco = composeClienteEnderecoLinha(c) || c.endereco;
        State.setCliente(c, st.clienteMode);
    }

    function scheduleEntregaPlusGeocode(force) {
        if (!dom.entregaPluscode) return;
        var q = String(dom.entregaPluscode.value || '').trim();
        if (q.length < 4) return;
        if (!force && q === entregaPlusGeocodeLastQ) return;
        clearTimeout(entregaPlusGeocodeTimer);
        entregaPlusGeocodeTimer = setTimeout(function () {
            runEntregaPlusGeocode(q);
        }, force ? 0 : 650);
    }

    function runEntregaPlusGeocode(q) {
        var apiUrl = urls.apiPdvGeocodePlus;
        if (!apiUrl) return;
        q = String(q || '').trim();
        if (q.length < 4) return;
        entregaPlusGeocodeLastQ = q;
        var seq = ++entregaPlusGeocodeSeq;
        var sep = apiUrl.indexOf('?') >= 0 ? '&' : '?';
        fetch(apiUrl + sep + 'q=' + encodeURIComponent(q), {
            credentials: 'same-origin',
            headers: { Accept: 'application/json' }
        })
            .then(function (r) {
                return r.json();
            })
            .then(function (data) {
                if (seq !== entregaPlusGeocodeSeq) return;
                if (!data || !data.ok || !data.endereco) return;
                applyEntregaPlusGeocodeEndereco(data.endereco, false);
            })
            .catch(function () {});
    }

    function renderStep2(state) {
        if (dom.step2ClientName) dom.step2ClientName.textContent = currentClientName(state);
        if (dom.step2ClientDoc) {
            var cpfView = clienteCpfEffective(state.cliente);
            dom.step2ClientDoc.textContent = cpfView
                ? pdvFormatCpfInput(cpfView)
                : 'Sem documento informado';
        }
        if (dom.step2TelView) {
            dom.step2TelView.textContent = compactText(state.cliente && state.cliente.telefone, '—');
        }
        if (dom.step2EndView) {
            var c0 = state.cliente || {};
            var endLinha = composeClienteEnderecoLinha(c0) || compactText(c0.endereco, '');
            dom.step2EndView.textContent = compactText(endLinha, '—');
        }
        setInputValue(dom.clienteTelefone, state.cliente && state.cliente.telefone);
        setInputValue(dom.clienteCpf, clienteCpfParaExibir(state.cliente));
        initBairroSelectsOnce();
        var cl = state.cliente || {};
        if (!String(cl.logradouro || '').trim() && !String(cl.bairro || '').trim() && String(cl.endereco || '').trim()) {
            setInputValue(dom.clienteLogradouro, cl.endereco);
        } else {
            setInputValue(dom.clienteLogradouro, cl.logradouro || '');
        }
        setInputValue(dom.clienteNumero, cl.numero || '');
        setSelectValue(dom.clienteBairro, cl.bairro || '', '');
        setInputValue(dom.clientePluscode, cl.plus_code || '');
        setInputValue(dom.vendaObservacao, state.venda.observacao);
        if (dom.clienteZapWhatsapp) {
            var wz = whatsappHrefCliente(cl.telefone);
            if (wz) {
                dom.clienteZapWhatsapp.href = wz;
                dom.clienteZapWhatsapp.classList.remove('hidden');
            } else {
                dom.clienteZapWhatsapp.href = '#';
                dom.clienteZapWhatsapp.classList.add('hidden');
            }
        }
    }

    function openClienteEditModal() {
        if (!dom.clienteEditModal) return;
        renderStep2(State.getState());
        dom.clienteEditModal.classList.remove('hidden');
        dom.clienteEditModal.classList.add('flex');
        setTimeout(function () {
            if (dom.clienteTelefone) dom.clienteTelefone.focus();
        }, 50);
    }

    function closeClienteEditModal() {
        if (!dom.clienteEditModal) return;
        if (!persistClienteEditModalSilencioso()) return;
        dom.clienteEditModal.classList.add('hidden');
        dom.clienteEditModal.classList.remove('flex');
    }

    function persistClienteEditModalSilencioso() {
        commitClienteEditCampos();
        var cpfCheck = pdvValidarCpfOpcional(dom.clienteCpf ? dom.clienteCpf.value : '');
        if (!cpfCheck.ok) {
            alert(cpfCheck.msg || 'CPF inválido.');
            if (dom.clienteCpf) dom.clienteCpf.focus();
            return false;
        }
        var state = State.getState();
        var pk = clienteAgroPkFromCliente(state.cliente);
        var pattern = urls.apiPdvClienteEditarPattern;
        if (!pk || !pattern || !state.cliente) return true;
        var c = state.cliente;
        var nome = String(c.nome || '').trim();
        if (nome.length < 2) return true;
        var waDigits = String(c.telefone || '').replace(/\D/g, '');
        if (waDigits.length < 10) return true;
        jsonPost(pattern.replace('__pk__', String(pk)), {
            nome: nome,
            whatsapp: String(c.telefone || '').trim(),
            cpf: cpfCheck.cpf,
            logradouro: c.logradouro || '',
            numero: c.numero || '',
            bairro: c.bairro || '',
            plus_code: c.plus_code || ''
        })
            .then(function (res) {
                if (res.ok && res.data && res.data.ok && res.data.cliente) {
                    var st = State.getState();
                    State.setCliente(
                        res.data.cliente,
                        st.clienteMode === 'consumidor_final' ? 'consumidor_final' : 'cliente'
                    );
                }
            })
            .catch(function () {});
        return true;
    }

    function isClienteEditModalOpen() {
        return !!(dom.clienteEditModal && !dom.clienteEditModal.classList.contains('hidden'));
    }

    function initEntregaToolbarOnce() {
        var sel = document.getElementById('pdv-entrega-origem-maps');
        if (!sel || sel.getAttribute('data-pdv-inited') === '1') return;
        var origens = bootstrap.origensMaps || [];
        sel.innerHTML = origens
            .map(function (o) {
                return (
                    '<option value="' +
                    escapeHtml(String(o.id || '')) +
                    '">' +
                    escapeHtml(String(o.label || o.id || '')) +
                    '</option>'
                );
            })
            .join('');
        if (origens.length) sel.value = String(origens[0].id || '');
        sel.setAttribute('data-pdv-inited', '1');
        sel.addEventListener('change', function () {
            syncEntregaToolbarLinks(State.getState());
        });
        var painel = document.getElementById('pdv-entrega-painel-btn');
        if (painel && urls.entregasPainel) painel.href = urls.entregasPainel;
    }

    function syncEntregaToolbarLinks(state) {
        initEntregaToolbarOnce();
        var sel = document.getElementById('pdv-entrega-origem-maps');
        var lojaA = document.getElementById('pdv-entrega-loja-maps');
        var origens = bootstrap.origensMaps || [];
        var curId = sel && sel.value;
        var o = null;
        for (var i = 0; i < origens.length; i++) {
            if (String(origens[i].id) === String(curId)) {
                o = origens[i];
                break;
            }
        }
        if (!o && origens.length) o = origens[0];
        if (lojaA) {
            var st = state || State.getState();
            var destQ = destinoQueryParaMaps(st);
            var origQ = '';
            if (o) {
                origQ = String(o.q || '').trim();
                if (!origQ) {
                    var lkO = String(o.link_loja || '').trim();
                    if (lkO && !/^https?:\/\//i.test(lkO)) origQ = lkO;
                }
            }
            if (destQ && origQ) {
                lojaA.href =
                    'https://www.google.com/maps/dir/?api=1&origin=' +
                    encodeURIComponent(origQ) +
                    '&destination=' +
                    encodeURIComponent(destQ) +
                    '&travelmode=driving';
                lojaA.classList.remove('pointer-events-none', 'opacity-40');
            } else if (destQ) {
                lojaA.href =
                    'https://www.google.com/maps/search/?api=1&query=' + encodeURIComponent(destQ);
                lojaA.classList.remove('pointer-events-none', 'opacity-40');
            } else if (o) {
                var lk = String(o.link_loja || '').trim();
                if (lk) {
                    lojaA.href = lk;
                    lojaA.classList.remove('pointer-events-none', 'opacity-40');
                } else if (origQ) {
                    lojaA.href =
                        'https://www.google.com/maps/search/?api=1&query=' + encodeURIComponent(origQ);
                    lojaA.classList.remove('pointer-events-none', 'opacity-40');
                } else {
                    lojaA.href = '#';
                    lojaA.classList.add('pointer-events-none', 'opacity-40');
                }
            } else {
                lojaA.href = '#';
                lojaA.classList.add('pointer-events-none', 'opacity-40');
            }
        }
        var wbtn = document.getElementById('pdv-entrega-whats-btn');
        var wu = whatsappHrefLoose(bootstrap.pdvEntregaWhatsapp);
        if (wbtn) {
            if (wu) {
                wbtn.href = wu;
                wbtn.classList.remove('hidden');
            } else {
                wbtn.href = '#';
                wbtn.classList.add('hidden');
            }
        }
    }

    function tryNavigateToStep(stepName) {
        var state = State.getState();
        var computed = State.getComputed();
        var flow = computed.flow;
        if (flowIndex(flow, stepName) === -1) return false;
        if (flowIndex(flow, stepName) > flowIndex(flow, state.currentStep)) return false;
        State.setCurrentStep(stepName);
        return true;
    }

    function initBairroSelectsOnce() {
        var urban = bairrosEntrega.urbanos || [];
        var rural = bairrosEntrega.rurais || [];
        function fill(sel) {
            if (!sel || sel.getAttribute('data-pdv-bairros') === '1') return;
            var cur = sel.value;
            var h =
                '<option value="">Selecione</option>' +
                '<optgroup label="Urbanos">' +
                urban
                    .map(function (b) {
                        return '<option value="' + escapeHtml(b) + '">' + escapeHtml(b) + '</option>';
                    })
                    .join('') +
                '</optgroup>' +
                '<optgroup label="Rurais">' +
                rural
                    .map(function (b) {
                        return '<option value="' + escapeHtml(b) + '">' + escapeHtml(b) + '</option>';
                    })
                    .join('') +
                '</optgroup>';
            sel.innerHTML = h;
            sel.setAttribute('data-pdv-bairros', '1');
            if (cur && (urban.indexOf(cur) >= 0 || rural.indexOf(cur) >= 0)) sel.value = cur;
        }
        fill(dom.entregaBairro);
        fill(dom.clienteBairro);
    }

    function normalizarBuscaBairro(s) {
        return String(s || '')
            .toLowerCase()
            .normalize('NFD')
            .replace(/[\u0300-\u036f]/g, '')
            .replace(/\s+/g, ' ')
            .trim();
    }

    function todosBairrosJacupiOrdem() {
        var urban = bairrosEntrega.urbanos || [];
        var rural = bairrosEntrega.rurais || [];
        return urban.concat(rural);
    }

    function filtrarBairrosJacupiPorTexto(arr, q) {
        var nq = normalizarBuscaBairro(q);
        if (!nq) return arr.slice();
        return arr.filter(function (nome) {
            return normalizarBuscaBairro(nome).indexOf(nq) >= 0;
        });
    }

    function canonicalBairroJacupiSePossivel(txt) {
        txt = String(txt || '').trim();
        if (!txt) return '';
        var nt = normalizarBuscaBairro(txt);
        var all = todosBairrosJacupiOrdem();
        for (var i = 0; i < all.length; i++) {
            if (normalizarBuscaBairro(all[i]) === nt) return all[i];
        }
        return txt;
    }

    /** Só aceita bairro da lista interna; fora dela → vazio (ex.: retorno do Plus Code). */
    function bairroListaJacupiOuVazio(txt) {
        txt = String(txt || '').trim();
        if (!txt) return '';
        var nt = normalizarBuscaBairro(txt);
        var all = todosBairrosJacupiOrdem();
        for (var i = 0; i < all.length; i++) {
            if (normalizarBuscaBairro(all[i]) === nt) return all[i];
        }
        return '';
    }

    function quickClientEditMissingInputs() {
        return [
            dom.quickClientEditNome,
            dom.quickClientEditWhatsapp,
            dom.quickClientEditLogradouro,
            dom.quickClientEditNumero,
            dom.quickClientEditBairro,
            dom.quickClientEditCidade,
            dom.quickClientEditUf,
            dom.quickClientEditCep,
        ];
    }

    function clearQuickClientEditMissingHighlights() {
        if (!dom.quickClientEditOverlay) return;
        dom.quickClientEditOverlay.querySelectorAll('.pdv-client-edit-missing').forEach(function (node) {
            node.classList.remove('pdv-client-edit-missing');
        });
    }

    function refreshQuickClientEditMissingHighlights() {
        if (!dom.quickClientEditOverlay || dom.quickClientEditOverlay.classList.contains('hidden')) return;
        quickClientEditMissingInputs().forEach(function (el) {
            if (!el) return;
            var wrap = el.closest('.pdv-client-edit-field');
            if (!wrap) return;
            wrap.classList.toggle('pdv-client-edit-missing', !String(el.value || '').trim());
        });
    }

    function initQuickClientEditMissingListenersOnce() {
        if (!dom.quickClientEditOverlay || dom.quickClientEditOverlay.dataset.missingBound === '1') return;
        dom.quickClientEditOverlay.dataset.missingBound = '1';
        quickClientEditMissingInputs().forEach(function (el) {
            if (!el) return;
            el.addEventListener('input', refreshQuickClientEditMissingHighlights);
            el.addEventListener('change', refreshQuickClientEditMissingHighlights);
        });
    }

    function fecharQuickClientEditBairroDd() {
        var dd = document.getElementById('pdv-quick-client-edit-bairro-dd');
        var inp = dom.quickClientEditBairro;
        if (dd) {
            dd.classList.add('hidden');
            dd.innerHTML = '';
        }
        if (inp) inp.setAttribute('aria-expanded', 'false');
    }

    function renderQuickClientEditBairroDd() {
        var dd = document.getElementById('pdv-quick-client-edit-bairro-dd');
        var inp = dom.quickClientEditBairro;
        if (!dd || !inp) return;
        var urban = bairrosEntrega.urbanos || [];
        var rural = bairrosEntrega.rurais || [];
        var q = inp.value;
        var nq = normalizarBuscaBairro(q);
        var fu;
        var fr;
        if (nq) {
            fu = filtrarBairrosJacupiPorTexto(urban, q);
            fr = filtrarBairrosJacupiPorTexto(rural, q);
        } else {
            fu = urban.slice();
            fr = quickClientEditBairroRuralExpandido ? rural.slice() : [];
        }
        var showVerMais = !nq && !quickClientEditBairroRuralExpandido && rural.length;
        var showingRuraisBloco = fr.length && (nq || quickClientEditBairroRuralExpandido);
        var h = '';
        function row(nome) {
            h +=
                '<li role="option" data-bairro="' +
                escapeHtml(nome) +
                '" class="cursor-pointer border-b border-slate-50 text-slate-800 hover:bg-emerald-50 last:border-b-0">' +
                escapeHtml(nome) +
                '</li>';
        }
        fu.forEach(row);
        if (showVerMais) {
            h +=
                '<li role="button" data-qc-bairro-ver-mais="1" class="cursor-pointer border-t border-slate-200 px-3 py-2 text-center text-[clamp(0.75rem,0.35vw+0.7rem,0.95rem)] font-black uppercase tracking-wide text-orange-600 hover:bg-orange-50">Ver mais (rurais)</li>';
        }
        if (showingRuraisBloco) {
            h +=
                '<li class="pointer-events-none border-t border-slate-200 bg-slate-50 px-3 py-1 text-[clamp(0.65rem,0.3vw+0.6rem,0.85rem)] font-black uppercase text-slate-400">Rurais</li>';
            fr.forEach(row);
        }
        if (nq && !fu.length && !fr.length) {
            h =
                '<li class="pointer-events-none px-3 py-2 italic text-slate-500">Nenhum na lista — pode salvar o texto digitado</li>';
        }
        dd.innerHTML = h;
    }

    function abrirQuickClientEditBairroDd() {
        var dd = document.getElementById('pdv-quick-client-edit-bairro-dd');
        var inp = dom.quickClientEditBairro;
        if (!dd || !inp) return;
        renderQuickClientEditBairroDd();
        dd.classList.remove('hidden');
        inp.setAttribute('aria-expanded', 'true');
    }

    function toggleQuickClientEditBairroDd() {
        var dd = document.getElementById('pdv-quick-client-edit-bairro-dd');
        if (!dd) return;
        if (dd.classList.contains('hidden')) abrirQuickClientEditBairroDd();
        else fecharQuickClientEditBairroDd();
    }

    function initQuickClientEditBairroComboboxOnce() {
        var wrap = document.querySelector('.pdv-quick-client-edit-bairro-wrap');
        if (!wrap || wrap.dataset.agroBairroComboBound) return;
        wrap.dataset.agroBairroComboBound = '1';
        var inp = dom.quickClientEditBairro;
        var dd = document.getElementById('pdv-quick-client-edit-bairro-dd');
        var toggle = document.getElementById('pdv-quick-client-edit-bairro-toggle');
        if (!inp || !dd) return;

        dd.addEventListener('mousedown', function (ev) {
            ev.preventDefault();
        });
        dd.addEventListener('click', function (ev) {
            var vm = ev.target.closest('[data-qc-bairro-ver-mais]');
            if (vm) {
                quickClientEditBairroRuralExpandido = true;
                renderQuickClientEditBairroDd();
                return;
            }
            var li = ev.target.closest('[data-bairro]');
            if (li) {
                inp.value = li.getAttribute('data-bairro') || '';
                fecharQuickClientEditBairroDd();
                refreshQuickClientEditMissingHighlights();
            }
        });
        if (toggle) {
            toggle.addEventListener('click', function (ev) {
                ev.preventDefault();
                toggleQuickClientEditBairroDd();
            });
        }
        inp.addEventListener('focus', function () {
            abrirQuickClientEditBairroDd();
        });
        inp.addEventListener('input', function () {
            quickClientEditBairroRuralExpandido = !!normalizarBuscaBairro(inp.value);
            abrirQuickClientEditBairroDd();
        });
        inp.addEventListener('keydown', function (ev) {
            if (ev.key === 'Escape') {
                fecharQuickClientEditBairroDd();
                return;
            }
            if (ev.key === 'ArrowDown' || ev.key === 'ArrowUp') {
                ev.preventDefault();
                abrirQuickClientEditBairroDd();
            }
        });
        inp.addEventListener('blur', function () {
            window.setTimeout(function () {
                var active = document.activeElement;
                if (active === toggle || (dd && dd.contains(active))) return;
                fecharQuickClientEditBairroDd();
                var canon = canonicalBairroJacupiSePossivel(inp.value);
                if (canon !== inp.value) inp.value = canon;
            }, 120);
        });
        document.addEventListener('click', function (ev) {
            if (!wrap.contains(ev.target)) fecharQuickClientEditBairroDd();
        });
    }

    function applyQuickClientGeocodeEndereco(endereco, overwrite) {
        endereco = endereco || {};
        function setIf(el, val) {
            if (!el || !val) return;
            if (overwrite || !String(el.value || '').trim()) el.value = val;
        }
        setIf(dom.quickClientEditLogradouro, endereco.logradouro);
        setIf(dom.quickClientEditNumero, endereco.numero);
        if (dom.quickClientEditBairro && endereco.bairro) {
            var bai = bairroListaJacupiOuVazio(endereco.bairro);
            if (bai && (overwrite || !String(dom.quickClientEditBairro.value || '').trim())) {
                dom.quickClientEditBairro.value = bai;
            }
        }
        setIf(dom.quickClientEditCidade, endereco.cidade);
        setIf(dom.quickClientEditUf, endereco.uf);
        setIf(dom.quickClientEditCep, endereco.cep);
        refreshQuickClientEditMissingHighlights();
    }

    function scheduleQuickClientPlusGeocode(force) {
        if (!dom.quickClientEditPluscode) return;
        var q = String(dom.quickClientEditPluscode.value || '').trim();
        if (q.length < 4) return;
        if (!force && q === quickClientGeocodeLastQ) return;
        clearTimeout(quickClientGeocodeTimer);
        quickClientGeocodeTimer = setTimeout(function () {
            runQuickClientPlusGeocode(q);
        }, force ? 0 : 650);
    }

    function runQuickClientPlusGeocode(q) {
        var apiUrl = urls.apiPdvGeocodePlus;
        if (!apiUrl) return;
        q = String(q || '').trim();
        if (q.length < 4) return;
        quickClientGeocodeLastQ = q;
        var seq = ++quickClientGeocodeSeq;
        var sep = apiUrl.indexOf('?') >= 0 ? '&' : '?';
        fetch(apiUrl + sep + 'q=' + encodeURIComponent(q), {
            credentials: 'same-origin',
            headers: { Accept: 'application/json' },
        })
            .then(function (r) {
                return r.json();
            })
            .then(function (data) {
                if (seq !== quickClientGeocodeSeq) return;
                if (!data || !data.ok || !data.endereco) return;
                applyQuickClientGeocodeEndereco(data.endereco, false);
            })
            .catch(function () {});
    }

    function enderecoEntregaMinimoOk(state) {
        var e = state.entrega || {};
        if (String(e.logradouro || '').trim() && String(e.bairro || '').trim()) return true;
        if (String(e.endereco || '').trim().length > 4) return true;
        var c = state.cliente || {};
        return String(c.endereco || '').trim().length > 4;
    }

    /** Lê endereço do DOM (sem gravar estado) para habilitar o botão ao digitar. */
    function enderecoEntregaMinimoOkParaUi(state) {
        state = state || State.getState();
        var log = dom.entregaLogradouro ? String(dom.entregaLogradouro.value || '').trim() : '';
        var bai = dom.entregaBairro ? String(dom.entregaBairro.value || '').trim() : '';
        if (log && bai) return true;
        var num = dom.entregaNumero ? String(dom.entregaNumero.value || '').trim() : '';
        var pc = dom.entregaPluscode ? String(dom.entregaPluscode.value || '').trim() : '';
        if (log || num || bai || pc) {
            var linha = buildLinhaEnderecoEntrega({
                entrega: { logradouro: log, numero: num, bairro: bai, plusCode: pc },
                cliente: state.cliente || {}
            });
            if (String(linha || '').trim().length > 4) return true;
        }
        return enderecoEntregaMinimoOk(state);
    }

    function entregaTaxaDevePularAuto(state) {
        state = state || State.getState();
        // Futuro: frete grátis automático por bairro/endereço do cliente.
        return false;
    }

    function entregaTaxaConsideradaOk(state) {
        state = state || State.getState();
        if (entregaTaxaDevePularAuto(state)) return true;
        return !!(state.entrega && state.entrega.taxaEntregaRespondida);
    }

    function entregaFaseAtual(state) {
        state = state || State.getState();
        if (entregaModoEfetivo(state) !== 'entrega') return 'pagamento_local';
        var e = state.entrega || {};
        var lp = String(e.localPagamento || '').trim();
        if (!lp) return 'pagamento_local';
        if (!e.enderecoPassoConcluido || !enderecoEntregaMinimoOk(state)) return 'endereco';
        if (!entregaTaxaConsideradaOk(state)) return 'detalhes';
        if (lp === 'loja') return 'done';
        if (entregaWizardAguardandoTroco) return 'troco';
        if (!String(e.meioNaEntrega || '').trim()) return 'meio';
        return 'done';
    }

    function aplicarEntregaTaxaGratisAuto() {
        State.setPagamentoField('frete', 0);
        State.setEntregaPatch({
            taxaEntregaModo: 'nao',
            taxaEntregaRespondida: true,
            detalhesEntregaRespondidos: true
        });
    }

    function scrollEntregaWizardIntoView() {
        if (!dom.entregaWizard || dom.entregaWizard.classList.contains('hidden')) return;
        var painel = entregaWizardPainelAtual();
        var foco =
            (painel === 'detalhes' && document.getElementById('pdv-entrega-horario')) ||
            (painel === 'troco' && document.getElementById('pdv-ef3-troco-input')) ||
            document.getElementById('pdv-ed-shell');
        if (!foco) return;
        setTimeout(function () {
            try {
                foco.focus();
            } catch (eFoco) {}
        }, 80);
    }

    function entregaWizardPrecisaExibir(state) {
        state = state || State.getState();
        if (state.currentStep !== 'entrega') return false;
        if (entregaModoEfetivo(state) !== 'entrega') return false;
        var fase = entregaFaseAtual(state);
        return fase === 'pagamento_local' || fase === 'detalhes' || fase === 'meio' || fase === 'troco';
    }

    function atualizarEntregaWizardVisibilidade(state) {
        state = state || State.getState();
        if (state.currentStep !== 'entrega') {
            if (dom.entregaWizard) showElement(dom.entregaWizard, false);
            if (dom.entregaMain) showElement(dom.entregaMain, false);
            if (dom.entregaResumo) showElement(dom.entregaResumo, false);
            return;
        }
        var needsWizard = entregaWizardPrecisaExibir(state);
        var modo = entregaModoEfetivo(state);
        var fase = entregaFaseAtual(state);
        if (dom.entregaWizard) showElement(dom.entregaWizard, needsWizard, 'flex');
        if (dom.entregaMain) {
            showElement(dom.entregaMain, !needsWizard && modo === 'entrega' && fase === 'endereco', 'flex');
        }
        if (dom.entregaResumo) {
            showElement(dom.entregaResumo, !needsWizard && modo === 'entrega' && fase === 'done', 'flex');
        }
        var partidaBar = document.getElementById('pdv-entrega-partida-bar');
        if (partidaBar) {
            showElement(partidaBar, modo === 'entrega');
        }
        var entregaShell = document.querySelector('.pdv-step-entrega-shell');
        if (entregaShell) {
            entregaShell.classList.toggle('pdv-step-entrega-shell--resumo', modo === 'entrega' && fase === 'done');
        }
        if (state.currentStep === 'entrega' && dom.entregaWizard && dom.entregaMain && dom.entregaResumo) {
            var wizOff =
                dom.entregaWizard.hidden || dom.entregaWizard.classList.contains('hidden');
            var mainOff = dom.entregaMain.hidden || dom.entregaMain.classList.contains('hidden');
            var resumoOff =
                dom.entregaResumo.hidden || dom.entregaResumo.classList.contains('hidden');
            if (wizOff && mainOff && resumoOff) {
                var faseFb = entregaFaseAtual(state);
                if (faseFb === 'endereco') {
                    showElement(dom.entregaMain, true, 'flex');
                } else if (faseFb === 'done') {
                    showElement(dom.entregaResumo, true, 'flex');
                    renderEntregaResumo(state, State.getComputed());
                } else {
                    showElement(dom.entregaWizard, true, 'flex');
                    syncEntregaDetalhesModalUi();
                }
            }
        }
        if (modo === 'entrega' && fase === 'done') {
            renderEntregaResumo(state, State.getComputed());
        }
    }

    function fecharModaisEntregaAntesImpressao() {
        closeEntregaSalvarClienteModal();
        entregaWizardAguardandoTroco = false;
        entregaPendingAfterSaveCliente = null;
    }

    function entregaFluxoPagamentoCompleto(state) {
        return entregaFaseAtual(state) === 'done';
    }

    function entregaWizardPainelAtual(state) {
        var fase = entregaFaseAtual(state);
        if (fase === 'pagamento_local' || fase === 'detalhes' || fase === 'meio' || fase === 'troco') {
            return fase;
        }
        return 'done';
    }

    function aplicarEntregaWizardHeader(painel) {
        var shell = document.getElementById('pdv-ed-shell');
        var header = document.getElementById('pdv-ed-header');
        var body = document.getElementById('pdv-ed-body');
        var etapa = document.getElementById('pdv-ed-etapa');
        var titulo = document.getElementById('pdv-ed-titulo');
        var subtitulo = document.getElementById('pdv-ed-subtitulo');
        var themes = {
            pagamento_local: {
                border: 'border-emerald-400',
                header: 'bg-gradient-to-r from-emerald-600 to-teal-600 text-white',
                etapa: 'Pagamento da entrega',
                titulo: 'Onde será o pagamento?',
                sub: 'Escolha uma opção para continuar.'
            },
            detalhes: {
                border: 'border-amber-400',
                header: 'bg-gradient-to-r from-amber-600 to-orange-600 text-white',
                etapa: 'Taxa e horário',
                titulo: 'Cobrar frete nesta entrega?',
                sub: 'Confira o horário se o cliente pediu.'
            },
            meio: {
                border: 'border-orange-400',
                header: 'bg-gradient-to-r from-orange-600 to-amber-600 text-white',
                etapa: 'Pagamento na entrega',
                titulo: 'Como o cliente vai pagar?',
                sub: ''
            },
            troco: {
                border: 'border-slate-400',
                header: 'bg-slate-800 text-white',
                etapa: 'Pagamento na entrega',
                titulo: 'Troco para quanto?',
                sub: 'Pergunte ao cliente se precisa de troco.'
            }
        };
        var t = themes[painel] || themes.pagamento_local;
        if (shell) {
            shell.setAttribute('data-pdv-ed-painel', painel || 'pagamento_local');
            shell.className =
                'pdv-ed-wizard-panel rounded-[1.2rem] border-[3px] bg-white shadow-2xl ' + t.border;
        }
        if (header) {
            header.className =
                'pdv-ed-header rounded-t-[1.05rem] border-b border-white/15 ' +
                t.header;
        }
        if (body) {
            body.className = 'rounded-b-[1.05rem] bg-white';
        }
        if (etapa) etapa.textContent = t.etapa;
        if (titulo) titulo.textContent = t.titulo;
        if (subtitulo) {
            subtitulo.textContent = t.sub;
            subtitulo.classList.toggle('hidden', !String(t.sub || '').trim());
        }
    }

    function renderEntregaTrocoPainelUi() {
        var comp = State.getComputed();
        var elTotal = document.getElementById('pdv-ed-troco-total');
        if (elTotal) elTotal.textContent = formatMoney(Number(comp.total || 0));
    }

    function confirmarEntregaDetalhesModal() {
        var taxaChecked = document.querySelector('input[name="pdv-entrega-taxa-modo"]:checked');
        if (!taxaChecked) {
            alert('Escolha se cobra frete, não cobra ou decide depois.');
            return;
        }
        commitEntregaTaxaModo(taxaChecked.value);
        commitEntregaTaxaValorInput();
        var horEl = document.getElementById('pdv-entrega-horario');
        var hor = horEl ? horEl.value : '';
        State.setEntregaPatch({
            horario: hor,
            detalhesEntregaRespondidos: true
        });
        wizardSyncLembretesFromEntregaHorario();
        if (dom.entregaHorario) dom.entregaHorario.value = hor;
        syncEntregaDetalhesModalUi();
        scrollEntregaWizardIntoView();
    }

    function confirmarEntregaTrocoModal() {
        var inp = document.getElementById('pdv-ef3-troco-input');
        var val = inp ? String(inp.value || '').trim() : '';
        if (!val) {
            alert('Informe o valor para troco (use 0 ou 0,00 se não precisar).');
            return false;
        }
        entregaWizardAguardandoTroco = false;
        State.setEntregaPatch({ localPagamento: 'entrega', meioNaEntrega: 'dinheiro', troco: val });
        State.setEntregaField('maquininha', 'nao');
        if (dom.entregaTroco) dom.entregaTroco.value = val;
        syncEntregaDetalhesModalUi();
        return true;
    }

    function syncEntregaDetalhesModalUi() {
        var st = State.getState();
        if (!entregaWizardPrecisaExibir(st)) {
            atualizarEntregaWizardVisibilidade(st);
            if (entregaFaseAtual(st) === 'done') {
                entregaWizardAguardandoTroco = false;
                aposConcluirFluxoPagamentoEntrega();
            }
            return;
        }
        var painel = entregaFaseAtual(st);
        if (entregaWizardAguardandoTroco) painel = 'troco';
        var map = {
            pagamento_local: document.getElementById('pdv-ed-pagamento-local-panel'),
            detalhes: document.getElementById('pdv-ed-detalhes-panel'),
            meio: document.getElementById('pdv-ed-meio-panel'),
            troco: document.getElementById('pdv-ed-troco-panel')
        };
        Object.keys(map).forEach(function (k) {
            if (map[k]) map[k].classList.toggle('hidden', k !== painel);
        });
        if (painel === 'detalhes') {
            var stDet = State.getState();
            if (!String((stDet.entrega && stDet.entrega.taxaEntregaModo) || '').trim()) {
                commitEntregaTaxaModo('sim', { draft: true });
            } else {
                renderEntregaTaxaCard(st);
            }
        }
        if (painel === 'troco') renderEntregaTrocoPainelUi();
        aplicarEntregaWizardHeader(painel);
        atualizarEntregaWizardVisibilidade(st);
    }

    function focarPrimeiroCampoEnderecoEntrega() {
        var alvo =
            (dom.entregaLogradouro && !String(dom.entregaLogradouro.value || '').trim() && dom.entregaLogradouro) ||
            (dom.entregaBairro && !String(dom.entregaBairro.value || '').trim() && dom.entregaBairro) ||
            dom.entregaLogradouro ||
            dom.entregaClienteNome;
        if (!alvo) return;
        setTimeout(function () {
            try {
                alvo.focus();
            } catch (errF) {}
        }, 120);
    }

    function aposConcluirFluxoPagamentoEntrega() {
        if (!entregaFluxoPagamentoCompleto()) return;
        renderEntregaResumo(State.getState(), State.getComputed());
    }

    function entregaResumoLabelTaxa(state) {
        var modo = entregaTaxaModoEfetivo(state);
        if (modo === 'nao') return 'Sem frete';
        if (modo === 'depois') return 'A definir depois';
        if (modo === 'sim') {
            return formatMoney(State.toNumber((state.pagamento && state.pagamento.frete) || 0));
        }
        return '—';
    }

    function entregaResumoHorarioTexto(hor) {
        hor = String(hor || '').trim();
        if (!hor) return 'Horário não informado';
        var parts = hor.split(':');
        if (parts.length >= 2) return 'Entregar às ' + parts[0] + ':' + parts[1];
        return 'Horário: ' + hor;
    }

    function renderEntregaResumo(state, computed) {
        state = state || State.getState();
        computed = computed || State.getComputed();
        var e = state.entrega || {};
        var c = state.cliente || {};
        var lp = String(e.localPagamento || '').trim();
        var meio = String(e.meioNaEntrega || '').trim();
        var nome =
            currentClientName(state) ||
            (dom.entregaClienteNome && dom.entregaClienteNome.value) ||
            '—';
        var tel =
            String((c && c.telefone) || '').trim() ||
            (dom.entregaClienteTelefone && dom.entregaClienteTelefone.value) ||
            '';
        var linha = buildLinhaEnderecoEntrega({ entrega: e, cliente: c });
        var elLocal = document.getElementById('pdv-resumo-pagamento-local');
        var elMeio = document.getElementById('pdv-resumo-pagamento-meio');
        var elTroco = document.getElementById('pdv-resumo-pagamento-troco');
        if (elLocal) {
            elLocal.textContent =
                lp === 'entrega' ? 'Pagamento na entrega' : lp === 'loja' ? 'Pagamento na loja' : '—';
        }
        if (elMeio) {
            var meioRow = elMeio.closest('.pdv-entrega-review-row');
            if (lp === 'entrega' && meio) {
                elMeio.textContent =
                    meio === 'dinheiro' ? 'Dinheiro' : meio === 'cartao' ? 'Cartão (maquininha)' : '—';
                elMeio.classList.remove('hidden');
                if (meioRow) meioRow.classList.remove('hidden');
            } else {
                elMeio.classList.add('hidden');
                if (meioRow) meioRow.classList.add('hidden');
            }
        }
        if (elTroco) {
            var trocoRow = elTroco.closest('.pdv-entrega-review-row');
            if (lp === 'entrega' && meio === 'dinheiro' && String(e.troco || '').trim()) {
                elTroco.textContent = 'R$ ' + String(e.troco).trim();
                elTroco.classList.remove('hidden');
                if (trocoRow) trocoRow.classList.remove('hidden');
            } else {
                elTroco.classList.add('hidden');
                if (trocoRow) trocoRow.classList.add('hidden');
            }
        }
        var elNome = document.getElementById('pdv-resumo-cliente-nome');
        var elTel = document.getElementById('pdv-resumo-cliente-tel');
        var elLinha = document.getElementById('pdv-resumo-endereco-linha');
        var elExtra = document.getElementById('pdv-resumo-endereco-extra');
        if (elNome) elNome.textContent = nome;
        if (elTel) elTel.textContent = tel || 'Telefone não informado';
        if (elLinha) elLinha.textContent = String(linha || e.endereco || '').trim() || 'Endereço incompleto';
        if (elExtra) {
            var extraRow = elExtra.closest('.pdv-entrega-review-row');
            var extras = [];
            if (String(e.plusCode || '').trim()) extras.push('Plus Code: ' + e.plusCode);
            if (String(e.complemento || '').trim()) extras.push('Compl.: ' + e.complemento);
            if (String(e.referencia || '').trim()) extras.push('Ref.: ' + e.referencia);
            if (extras.length) {
                elExtra.textContent = extras.join(' · ');
                elExtra.classList.remove('hidden');
                if (extraRow) extraRow.classList.remove('hidden');
            } else {
                elExtra.classList.add('hidden');
                if (extraRow) extraRow.classList.add('hidden');
            }
        }
        var elTaxa = document.getElementById('pdv-resumo-taxa');
        var elHor = document.getElementById('pdv-resumo-horario');
        if (elTaxa) elTaxa.textContent = entregaResumoLabelTaxa(state);
        if (elHor) elHor.textContent = entregaResumoHorarioTexto(e.horario);
        var aside = document.getElementById('pdv-resumo-aside');
        var elTotal = document.getElementById('pdv-resumo-total');
        if (aside && elTotal) {
            var showTotal = lp === 'entrega';
            aside.classList.toggle('hidden', !showTotal);
            if (showTotal) elTotal.textContent = formatMoney(Number(computed.total || 0));
        }
        var resumoVendaObs = document.getElementById('pdv-resumo-venda-observacao');
        var resumoEntObs = document.getElementById('pdv-resumo-entrega-observacao');
        if (resumoVendaObs) setInputValue(resumoVendaObs, state.venda.observacao);
        if (resumoEntObs) setInputValue(resumoEntObs, e.observacao);
    }

    function commitEntregaObsFromDom() {
        var fase = entregaFaseAtual();
        var resumoVendaObs = document.getElementById('pdv-resumo-venda-observacao');
        var resumoEntObs = document.getElementById('pdv-resumo-entrega-observacao');
        if (fase === 'done' && resumoVendaObs) {
            State.setVendaField('observacao', resumoVendaObs.value);
            if (resumoEntObs) State.setEntregaField('observacao', resumoEntObs.value);
            return;
        }
        if (dom.vendaObservacao) State.setVendaField('observacao', dom.vendaObservacao.value);
        if (dom.entregaObservacao) State.setEntregaField('observacao', dom.entregaObservacao.value);
    }

    function entregaIrEditarFase(destino) {
        entregaWizardAguardandoTroco = false;
        if (destino === 'pagamento') {
            var st = State.getState();
            var e = st.entrega || {};
            var lp = String(e.localPagamento || '').trim();
            var meio = String(e.meioNaEntrega || '').trim();
            if (lp === 'entrega' && meio === 'dinheiro') destino = 'troco';
            else if (lp === 'entrega' && meio) destino = 'meio';
            else destino = 'pagamento_local';
        }
        if (destino === 'pagamento_local') {
            State.setEntregaPatch({
                localPagamento: '',
                meioNaEntrega: '',
                troco: '',
                taxaEntregaRespondida: false,
                taxaEntregaModo: '',
                detalhesEntregaRespondidos: false,
                enderecoPassoConcluido: false
            });
            State.setPagamentoField('frete', 0);
            syncEntregaDetalhesModalUi();
            scrollEntregaWizardIntoView();
            return;
        }
        if (destino === 'endereco') {
            resetEntregaClienteSnapshot();
            State.setEntregaPatch({ enderecoPassoConcluido: false });
            atualizarEntregaWizardVisibilidade(State.getState());
            focarPrimeiroCampoEnderecoEntrega();
            return;
        }
        if (destino === 'detalhes') {
            State.setEntregaPatch({
                taxaEntregaRespondida: false,
                detalhesEntregaRespondidos: false,
                meioNaEntrega: '',
                troco: ''
            });
            State.setPagamentoField('frete', 0);
            syncEntregaDetalhesModalUi();
            scrollEntregaWizardIntoView();
            return;
        }
        if (destino === 'meio') {
            State.setEntregaPatch({ meioNaEntrega: '', troco: '' });
            syncEntregaDetalhesModalUi();
            scrollEntregaWizardIntoView();
            return;
        }
        if (destino === 'troco') {
            State.setEntregaPatch({ meioNaEntrega: '', troco: '' });
            entregaWizardAguardandoTroco = true;
            syncEntregaDetalhesModalUi();
            scrollEntregaWizardIntoView();
        }
    }

    function abrirFluxoPagamentoEntregaSePendente() {
        var fase = entregaFaseAtual();
        if (fase === 'done' || fase === 'endereco') return false;
        syncEntregaDetalhesModalUi();
        scrollEntregaWizardIntoView();
        return true;
    }

    function entregaTaxaModoEfetivo(state) {
        var e = state.entrega || {};
        var m = String(e.taxaEntregaModo || '');
        if (m === 'nao' || m === 'sim' || m === 'depois') return m;
        if (e.taxaEntregaRespondida) {
            var f = State.toNumber((state.pagamento && state.pagamento.frete) || 0);
            if (f > 0.009) return 'sim';
            return 'nao';
        }
        return '';
    }

    function commitEntregaTaxaModo(modo, opts) {
        opts = opts || {};
        var draft = !!opts.draft;
        if (modo === 'nao') {
            State.setPagamentoField('frete', 0);
            State.setEntregaField('taxaEntregaModo', 'nao');
            if (!draft) State.setEntregaField('taxaEntregaRespondida', true);
        } else if (modo === 'depois') {
            State.setEntregaField('taxaEntregaModo', 'depois');
            if (!draft) State.setEntregaField('taxaEntregaRespondida', true);
        } else if (modo === 'sim') {
            var st = State.getState();
            var f = State.toNumber((st.pagamento && st.pagamento.frete) || 0);
            if (f <= 0.009) State.setPagamentoField('frete', 10);
            State.setEntregaField('taxaEntregaModo', 'sim');
            if (!draft) State.setEntregaField('taxaEntregaRespondida', true);
        }
        renderEntregaTaxaCard(State.getState());
    }

    function commitEntregaTaxaValorInput() {
        var el = document.getElementById('pdv-entrega-taxa-valor');
        if (!el) return;
        var st = State.getState();
        if (entregaTaxaModoEfetivo(st) !== 'sim') return;
        var raw = String(el.value || '').trim();
        if (!raw) {
            var cur = State.toNumber(st.pagamento && st.pagamento.frete);
            if (cur <= 0.009) State.setPagamentoField('frete', 10);
            return;
        }
        State.setPagamentoField('frete', State.toNumber(raw));
    }

    function renderEntregaTaxaCard(state) {
        var modo = entregaTaxaModoEfetivo(state);
        var frete = State.toNumber((state.pagamento && state.pagamento.frete) || 0);
        var wrap = document.getElementById('pdv-entrega-taxa-valor-wrap');
        var inpVal = document.getElementById('pdv-entrega-taxa-valor');
        if (!modo && entregaWizardPainelAtual(state) === 'detalhes') {
            modo = 'sim';
        }
        document.querySelectorAll('input[name="pdv-entrega-taxa-modo"]').forEach(function (r) {
            r.checked = modo !== '' && r.value === modo;
        });
        if (wrap) wrap.classList.toggle('hidden', modo !== 'sim');
        var campos = document.getElementById('pdv-ed-detalhes-campos');
        if (campos) campos.classList.toggle('pdv-ed-detalhes-campos--so-horario', modo !== 'sim');
        if (inpVal && modo === 'sim') {
            var display = frete > 0.009 ? String(frete.toFixed(2)).replace('.', ',') : '10,00';
            setInputValue(inpVal, display);
        }
    }

    function voltarUmPassoEntrega() {
        var state = State.getState();
        if (state.currentStep !== 'entrega') return false;
        var fase = entregaFaseAtual(state);
        var e = state.entrega || {};
        var lp = String(e.localPagamento || '').trim();
        var meio = String(e.meioNaEntrega || '').trim();
        if (fase === 'troco') {
            entregaWizardAguardandoTroco = false;
            syncEntregaDetalhesModalUi();
            scrollEntregaWizardIntoView();
            return true;
        }
        if (fase === 'meio') {
            entregaWizardAguardandoTroco = false;
            State.setEntregaPatch({
                meioNaEntrega: '',
                troco: '',
                taxaEntregaRespondida: false,
                detalhesEntregaRespondidos: false
            });
            syncEntregaDetalhesModalUi();
            scrollEntregaWizardIntoView();
            return true;
        }
        if (fase === 'detalhes') {
            resetEntregaClienteSnapshot();
            State.setEntregaPatch({
                taxaEntregaRespondida: false,
                taxaEntregaModo: '',
                detalhesEntregaRespondidos: false,
                enderecoPassoConcluido: false
            });
            State.setPagamentoField('frete', 0);
            syncEntregaDetalhesModalUi();
            return true;
        }
        if (fase === 'endereco') {
            entregaWizardAguardandoTroco = false;
            resetEntregaClienteSnapshot();
            State.setEntregaPatch({
                localPagamento: '',
                meioNaEntrega: '',
                troco: '',
                taxaEntregaRespondida: false,
                taxaEntregaModo: '',
                detalhesEntregaRespondidos: false,
                enderecoPassoConcluido: false
            });
            State.setPagamentoField('frete', 0);
            syncEntregaDetalhesModalUi();
            scrollEntregaWizardIntoView();
            return true;
        }
        if (fase === 'done') {
            if (lp === 'loja') {
                State.setEntregaPatch({
                    taxaEntregaRespondida: false,
                    detalhesEntregaRespondidos: false
                });
                syncEntregaDetalhesModalUi();
                scrollEntregaWizardIntoView();
                return true;
            }
            if (meio === 'dinheiro') {
                State.setEntregaPatch({ meioNaEntrega: '', troco: '' });
                entregaWizardAguardandoTroco = true;
                if (dom.entregaTroco) dom.entregaTroco.value = '';
                syncEntregaDetalhesModalUi();
                scrollEntregaWizardIntoView();
                return true;
            }
            if (meio === 'cartao') {
                State.setEntregaPatch({ meioNaEntrega: '', troco: '' });
                State.setEntregaField('maquininha', '');
                syncEntregaDetalhesModalUi();
                scrollEntregaWizardIntoView();
                return true;
            }
        }
        if (fase === 'pagamento_local') {
            resetEntregaModoAoVoltarProdutos();
            State.setCurrentStep('produtos');
            return true;
        }
        return false;
    }

    function renderEntrega(state) {
        if (state.currentStep !== 'entrega') {
            esconderSubpainelsEntregaForaDaEtapa();
            return;
        }
        syncEntregaToolbarLinks(state);
        initBairroSelectsOnce();
        var e = state.entrega || {};
        var c = state.cliente || {};
        var log = String(e.logradouro || '').trim();
        var num = String(e.numero || '').trim();
        var bai = String(e.bairro || '').trim();
        if (!log && !num && !bai && String(e.endereco || '').trim()) {
            setInputValueUnlessFocused(dom.entregaLogradouro, e.endereco);
        } else {
            setInputValueUnlessFocused(dom.entregaLogradouro, e.logradouro || c.logradouro || '');
        }
        setInputValueUnlessFocused(dom.entregaNumero, e.numero || c.numero || '');
        setSelectValue(dom.entregaBairro, e.bairro || c.bairro || '', '');
        setInputValueUnlessFocused(dom.entregaPluscode, e.plusCode || c.plus_code || '');
        setInputValueUnlessFocused(dom.entregaComplemento, e.complemento);
        setInputValueUnlessFocused(dom.entregaReferencia, e.referencia || (c && c.referencia_rural) || '');
        setInputValue(dom.entregaHorario, e.horario);
        setInputValue(dom.entregaTroco, e.troco);
        setInputValue(dom.entregaObservacao, e.observacao);
        setInputValue(dom.vendaObservacao, state.venda.observacao);
        renderEntregaTaxaCard(state);
        renderEntregaClienteCampos(state);
        entregaPlusGeocodeLastQ = String(e.plusCode || c.plus_code || '').trim();
        atualizarEntregaWizardVisibilidade(state);
        if (entregaWizardPrecisaExibir(state)) {
            syncEntregaDetalhesModalUi();
        } else if (entregaFaseAtual(state) === 'done') {
            renderEntregaResumo(state, State.getComputed());
        }
        if (entregaFaseAtual(state) === 'endereco') {
            agendarCapturaEntregaClienteSnapshot();
        }
    }

    function renderPagamento(state, computed) {
        var forma = state.pagamento.forma || '';
        setSelectValue(dom.paymentMethod, forma, '');
        setInputValueUnlessFocused(dom.paymentDiscount, moneyFieldDisplay(state.pagamento.descontoGeral));
        setInputValueUnlessFocused(dom.paymentShipping, moneyFieldDisplay(state.pagamento.frete));
        setInputValueUnlessFocused(dom.paymentReceived, moneyFieldDisplay(state.pagamento.valorRecebido));
        setInputValue(dom.paymentChange, state.pagamento.trocoCalculado);
        if (dom.paymentValorForma) {
            setInputValueUnlessFocused(dom.paymentValorForma, moneyFieldDisplay(state.pagamento.valorDestaForma));
        }
        if (dom.paymentParcelasCredito) {
            setInputValue(dom.paymentParcelasCredito, String(state.pagamento.creditoParcelas || 2));
        }
        if (dom.fiadoParcelasInput) setInputValue(dom.fiadoParcelasInput, String(state.pagamento.fiadoParcelas || 1));
        if (dom.fiadoDiasInput) setInputValue(dom.fiadoDiasInput, String(state.pagamento.fiadoDiasVencimento || 30));
        if (dom.outroDetalhes) setInputValue(dom.outroDetalhes, state.pagamento.outroDetalhes);
        if (dom.fiadoResumo) {
            var fpR = parseInt(state.pagamento.fiadoParcelas, 10) || 1;
            var fdR = parseInt(state.pagamento.fiadoDiasVencimento, 10) || 30;
            dom.fiadoResumo.innerHTML =
                '<strong>' +
                fpR +
                'x</strong> · 1º venc. <strong>' +
                fdR +
                ' dias</strong> <span class="text-orange-700/80">(editar abaixo)</span>';
        }
        if (dom.valeSaldoView) dom.valeSaldoView.textContent = formatMoney(saldoValeAtual(state));
        if (dom.cashbackSaldoView) dom.cashbackSaldoView.textContent = formatMoney(saldoCashbackAtual(state));
        if (dom.outroPinMsg) {
            dom.outroPinMsg.textContent = state.pagamento.outroPinVerificado
                ? 'PIN ok — descreva o pagamento.'
                : 'Valide o PIN antes de descrever.';
            dom.outroPinMsg.classList.toggle('text-emerald-700', !!state.pagamento.outroPinVerificado);
            dom.outroPinMsg.classList.toggle('text-slate-500', !state.pagamento.outroPinVerificado);
        }

        if (dom.paymentFormaLabel) {
            dom.paymentFormaLabel.textContent = forma || 'Nenhuma selecionada';
        }
        if (dom.paymentFlowHeading) {
            dom.paymentFlowHeading.textContent = forma ? forma : '—';
        }
        if (dom.paymentFlowArea) dom.paymentFlowArea.classList.toggle('hidden', !forma);
        if (dom.paymentNoFormaHint) dom.paymentNoFormaHint.classList.toggle('hidden', !!forma);

        dom.paymentModalCards.forEach(function (btn) {
            var v = btn.getAttribute('data-payment-modal-card');
            var on = v === forma;
            btn.classList.toggle('ring-4', on);
            btn.classList.toggle('ring-white', on);
            btn.classList.toggle('ring-offset-2', on);
            btn.classList.toggle('ring-offset-slate-900', on);
            btn.setAttribute('aria-pressed', on ? 'true' : 'false');
            if (v === 'Fiado') {
                var bloqFi = validarFiadoPermitido(state);
                btn.disabled = !!bloqFi;
                btn.classList.toggle('opacity-40', !!bloqFi);
                btn.classList.toggle('cursor-not-allowed', !!bloqFi);
                if (bloqFi) btn.title = bloqFi;
                else btn.removeAttribute('title');
            }
        });

        var show = function (id, yes) {
            var el = document.getElementById(id);
            if (el) el.classList.toggle('hidden', !yes);
        };
        var hasMaquina = !!(state.pagamento.maquinaId && String(state.pagamento.maquinaId).trim());
        var needMaquinaBar = requiresMaquina(forma);
        var barMaquina = document.getElementById('pdv-pay-maquina-bar');
        var lblMaquina = document.getElementById('pdv-pay-maquina-label');
        if (barMaquina) barMaquina.classList.toggle('hidden', !needMaquinaBar || !hasMaquina);
        if (lblMaquina) {
            lblMaquina.textContent =
                String(state.pagamento.maquinaNome || state.pagamento.maquinaId || '').trim() || '—';
        }

        show('pdv-flow-dinheiro', forma === 'Dinheiro');
        show('pdv-flow-pix', forma === 'PIX');
        var cartao =
            forma === 'Cartão de débito' || forma === 'Cartão de crédito' || forma === 'Cartão de crédito parcelado';
        show('pdv-flow-cartao', cartao);

        var pixGate = document.getElementById('pdv-pix-machine-gate');
        var pixSteps = document.getElementById('pdv-pix-steps-wrap');
        if (forma === 'PIX') {
            var pg = !hasMaquina;
            if (pixGate) {
                pixGate.classList.toggle('hidden', !pg);
                pixGate.classList.toggle('flex', pg);
            }
            if (pixSteps) {
                pixSteps.classList.toggle('hidden', pg);
                pixSteps.classList.toggle('flex', !pg);
            }
            var pixMpRow = document.getElementById('pdv-pix-row-mp');
            var pixCieloRow = document.getElementById('pdv-pix-row-cielo');
            var pixScoRow = document.getElementById('pdv-pix-row-sicoob');
            var rowVisPix = function (el, on) {
                if (el) el.classList.toggle('hidden', !on);
            };
            if (hasMaquina) {
                var pMid = String(state.pagamento.maquinaId || '').trim();
                var narrowMp = pMid === 'pix_mp_qr';
                var narrowCielo = pMid === 'pix_cielo';
                var narrowSco = pMid === 'pix_sicoob_chave';
                var narrow = narrowMp || narrowCielo || narrowSco;
                if (narrow) {
                    rowVisPix(pixMpRow, narrowMp);
                    rowVisPix(pixCieloRow, narrowCielo);
                    rowVisPix(pixScoRow, narrowSco);
                } else {
                    rowVisPix(pixMpRow, true);
                    rowVisPix(pixCieloRow, true);
                    rowVisPix(pixScoRow, true);
                }
            } else {
                rowVisPix(pixMpRow, true);
                rowVisPix(pixCieloRow, true);
                rowVisPix(pixScoRow, true);
            }
        } else {
            if (pixGate) {
                pixGate.classList.add('hidden');
                pixGate.classList.remove('flex');
            }
            if (pixSteps) {
                pixSteps.classList.add('hidden');
                pixSteps.classList.remove('flex');
            }
        }

        var cardGate = document.getElementById('pdv-card-machine-gate');
        var cardSteps = document.getElementById('pdv-card-steps-wrap');
        var cardScrRow = document.getElementById('pdv-card-row-sicredi');
        if (cartao) {
            var cg = !hasMaquina;
            if (cardGate) {
                cardGate.classList.toggle('hidden', !cg);
                cardGate.classList.toggle('flex', cg);
            }
            if (cardSteps) {
                cardSteps.classList.toggle('hidden', cg);
                cardSteps.classList.toggle('flex', !cg);
            }
            if (cardScrRow) {
                var showCardSicredi = true;
                if (hasMaquina) {
                    var cMid = String(state.pagamento.maquinaId || '').trim();
                    var cRede = '';
                    getMaquininhasList(forma).forEach(function (it) {
                        if (it.id === cMid) cRede = String(it.rede || '').toLowerCase();
                    });
                    showCardSicredi =
                        cRede === 'sicredi' ||
                        cMid.indexOf('sicredi') === 0;
                }
                cardScrRow.classList.toggle('hidden', !showCardSicredi);
            }
        } else {
            if (cardGate) {
                cardGate.classList.add('hidden');
                cardGate.classList.remove('flex');
            }
            if (cardSteps) {
                cardSteps.classList.add('hidden');
                cardSteps.classList.remove('flex');
            }
        }

        if (dom.flowParcelasPanel) dom.flowParcelasPanel.classList.toggle('hidden', forma !== 'Cartão de crédito parcelado');
        show('pdv-flow-fiado', forma === 'Fiado');
        show('pdv-flow-vale', forma === 'Vale crédito');
        show('pdv-flow-cashback', forma === 'Cashback');
        show('pdv-flow-outro', forma === 'Outro');

        var trBar = document.getElementById('pdv-pay-valor-tranche-bar');
        var showTranche =
            !!forma &&
            forma !== 'Dinheiro' &&
            (!requiresMaquina(forma) || hasMaquina);
        if (trBar) trBar.classList.toggle('hidden', !showTranche);

        var mpPixAuto =
            pagamentoUi.mpPointEnabled &&
            hasMaquina &&
            String(state.pagamento.maquinaId || '').trim() === 'pix_mp_qr';
        var mpPixHint = mpPixAuto
            ? 'Use o botão verde «Cobrar na maquininha» ao lado do valor.'
            : 'QR Pix Mercado Pago — use o display do terminal ou “Ampliar QR” para orientar o cliente.';
        var btnAmplifyPix = document.getElementById('pdv-pay-open-qr-pix');
        if (btnAmplifyPix) btnAmplifyPix.classList.toggle('hidden', !!mpPixAuto);
        fillQrSlot(dom.pixMpQr, mpPixAuto ? '' : pagamentoUi.qrMercadoPagoUrl, mpPixHint);
        wireSicrediLink(dom.cardSicrediLink, pagamentoUi.qrSicrediUrl);
        if (dom.cardSicrediLink) {
            var uCardScr = String(pagamentoUi.qrSicrediUrl || '').trim();
            dom.cardSicrediLink.textContent = uCardScr ? 'QR Sicredi' : 'QR na maquininha Sicredi';
        }
        if (dom.pixSicobKey) {
            var key = String(pagamentoUi.chavePixSicob || '').trim();
            dom.pixSicobKey.textContent =
                key || 'Chave não cadastrada — use Pix na maquininha ou configure a chave no painel.';
        }

        var total = totalNumberFromComputed(computed);
        var pagoAcum = sumValorLancamentos(state);
        var restFin = saldoRestantePagamento(state, computed);
        if (dom.paymentPaidAccum) dom.paymentPaidAccum.textContent = formatMoney(pagoAcum);
        if (dom.paymentRemainingTop) dom.paymentRemainingTop.textContent = formatMoney(restFin);
        var quitadoPay = restFin <= 0.009;
        if (dom.paymentRestanteHero) {
            dom.paymentRestanteHero.classList.toggle('pdv-pay-restante-hero--quitado', quitadoPay);
            dom.paymentRestanteHero.classList.toggle('pdv-pay-restante-hero--pendente', !quitadoPay);
        }
        if (dom.paymentRestanteHeroLabel) {
            dom.paymentRestanteHeroLabel.textContent = quitadoPay ? 'Tudo pago' : 'Resta pagar';
        }
        if (dom.paymentRestanteHeroVal) {
            dom.paymentRestanteHeroVal.textContent = quitadoPay ? 'Pode confirmar' : formatMoney(restFin);
        }
        if (dom.paymentTotalInline) dom.paymentTotalInline.textContent = formatMoney(total);
        if (dom.paymentTotaisDetalhe) {
            var showDetalhe = (computed.desconto > 0.009) || (computed.frete > 0.009);
            dom.paymentTotaisDetalhe.classList.toggle('hidden', !showDetalhe);
        }
        if (dom.paymentFormaResumo) {
            dom.paymentFormaResumo.textContent = '';
            dom.paymentFormaResumo.classList.add('hidden');
        }
        var midPay = String(state.pagamento.maquinaId || '').trim();
        var mpPointBtn = isMaquinaMpPointAuto(midPay, forma);
        if (dom.payCommitTranche) {
            dom.payCommitTranche.textContent = mpPointBtn ? 'Cobrar na maquininha' : 'Lançar pagamento';
            dom.payCommitTranche.disabled = !!isProcessingMpTranche || quitadoPay;
            dom.payCommitTranche.classList.toggle('opacity-40', dom.payCommitTranche.disabled);
        }
        renderPayStepChips(mpPointBtn && !!forma);
        if (dom.payCommitTrancheHint) {
            dom.payCommitTrancheHint.textContent = mpPointBtn
                ? 'Digite o valor, toque no botão verde e aguarde a maquininha.'
                : forma
                  ? 'Digite o valor e toque em lançar pagamento.'
                  : '';
        }
        if (dom.paymentValorTotalRef) dom.paymentValorTotalRef.textContent = formatMoney(total);
        if (dom.paymentValorRestante) dom.paymentValorRestante.textContent = formatMoney(restFin);

        var larr = state.pagamento.lancamentos || [];
        if (dom.paymentLancamentosBox) dom.paymentLancamentosBox.classList.toggle('hidden', !larr.length);
        if (dom.paymentLancamentosList) {
            dom.paymentLancamentosList.innerHTML = larr.length
                ? larr
                      .map(function (L, idx) {
                          var sub = [];
                          if (L.maquinaNome) sub.push(String(L.maquinaNome).trim());
                          var midL = String(L.maquinaId || '').trim();
                          if (L.mpPointPago) {
                              sub.unshift('Pago na maquininha');
                          } else if (isMaquinaMpPointAuto(midL, L.forma)) {
                              sub.push('Point automático');
                          }
                          if (L.forma === 'Cartão de crédito parcelado' && L.creditoParcelas) {
                              sub.push(String(L.creditoParcelas).trim() + 'x');
                          }
                          if (L.forma === 'Dinheiro' && L.trocoCalculado) {
                              sub.push('Troco ' + formatMoney(State.toNumber(L.trocoCalculado)));
                          }
                          if (L.forma === 'Outro' && L.outroDetalhes) {
                              sub.push(String(L.outroDetalhes).trim().slice(0, 80));
                          }
                          var subTxt = sub.filter(Boolean).join(' · ');
                          var podeEditar = !L.mpPointPago;
                          var metaLine = subTxt ? 'Pago · ' + subTxt : 'Pago';
                          var liClass =
                              'pdv-pay-lanc-item ' +
                              (L.mpPointPago ? 'pdv-pay-lanc-item--mp-pago border-emerald-300' : 'border-emerald-200/80');
                          var actionsHtml = podeEditar
                              ? '<span class="pdv-pay-lanc-actions">' +
                                '<button type="button" class="pdv-pay-lanc-btn pdv-pay-lanc-btn--edit" data-pdv-edit-lanc="' +
                                idx +
                                '">Alt.</button>' +
                                '<button type="button" class="pdv-pay-lanc-btn pdv-pay-lanc-btn--rm" data-pdv-remove-lanc="' +
                                idx +
                                '">Exc.</button></span>'
                              : '';
                          return (
                              '<li class="' +
                              liClass +
                              '">' +
                              '<div class="pdv-pay-lanc-row1">' +
                              '<span class="pdv-pay-lanc-forma">' +
                              escapeHtml(L.forma || '') +
                              '</span>' +
                              '<span class="pdv-pay-lanc-valor">' +
                              escapeHtml(formatMoney(L.valor)) +
                              '</span></div>' +
                              '<div class="pdv-pay-lanc-row2">' +
                              '<span class="pdv-pay-lanc-meta">' +
                              escapeHtml(metaLine) +
                              '</span>' +
                              actionsHtml +
                              '</div></li>'
                          );
                      })
                      .join('')
                : '';
        }

        dom.paymentSubtotal.textContent = formatMoney(computed.subtotal);
        dom.paymentDiscountView.textContent = formatMoney(computed.desconto);
        dom.paymentShippingView.textContent = formatMoney(computed.frete);
        dom.paymentTotal.textContent = formatMoney(computed.total);
        var err = erroValidacaoPagamento(state, computed);
        var readyConfirm = !err && !forma && larr.length && restFin <= 0.009;
        var cnp = dom.confirmSaleNoPrint;
        var cp = dom.confirmSalePrint;
        if (cnp) {
            cnp.disabled = !readyConfirm;
            cnp.classList.toggle('opacity-40', !readyConfirm);
        }
        if (cp) {
            cp.disabled = !readyConfirm;
            cp.classList.toggle('opacity-40', !readyConfirm);
        }
        if (err) {
            dom.paymentFeedback.textContent = err;
            dom.paymentFeedback.classList.remove('hidden');
        } else {
            var hintParcel = '';
            var totP = computed.total || 0;
            var formaP = String(state.pagamento.forma || '').trim();
            var temParcelado =
                formaP === 'Cartão de crédito parcelado' ||
                (larr || []).some(function (L) {
                    return String(L.forma || '') === 'Cartão de crédito parcelado';
                });
            if (temParcelado && totP + 0.009 < 10) {
                hintParcel =
                    'Parcelado na maquininha MP costuma funcionar a partir de R$ 10,00 (vendedor ou cliente).';
            }
            dom.paymentFeedback.textContent = hintParcel;
            if (dom.paymentFeedback) {
                dom.paymentFeedback.classList.toggle('hidden', !hintParcel);
            }
        }
    }

    function renderAll(state, computed) {
        var flow = computed.flow;
        if (flowIndex(flow, state.currentStep) === -1) {
            State.setCurrentStep(flow[flow.length - 1] || 'produtos');
            return;
        }
        var wasStep = prevStepCache;
        ensureEntregaModoNaEtapa();
        state = State.getState();
        computed = State.getComputed();
        if (state.currentStep === 'entrega' && wasStep !== 'entrega') {
            entregaWizardAguardandoTroco = false;
            if (window.gmLoadingBar && window.gmLoadingBar.hide) window.gmLoadingBar.hide();
        }
        renderStepPanels(state, computed);
        renderSummary(state, computed);
        renderQuickClient(state);
        renderProducts(state, computed);
        renderStep2(state);
        renderEntrega(state);
        renderPagamento(state, computed);
        if (state.currentStep === 'pagamento' && wasStep !== 'pagamento' && !state.pagamento.forma) {
            var compOpen = State.getComputed();
            if (saldoRestantePagamento(state, compOpen) > 0.009) {
                openPaymentFormaModal();
            }
        }
        prevStepCache = state.currentStep;
        syncPdvSspinIdlePause();
    }

    function focusProductSearch() {
        if (dom.productSearch) dom.productSearch.focus();
    }

    function openStartModal() {
        if (!dom.modalStart) return;
        dom.modalStart.classList.remove('hidden');
        dom.modalStart.classList.add('flex');
        pdvEnsureModalOpenBody();
        setTimeout(function () {
            if (dom.startConsumidorFinal) dom.startConsumidorFinal.focus();
        }, 50);
    }

    function closeStartModal() {
        if (!dom.modalStart) return;
        dom.modalStart.classList.add('hidden');
        dom.modalStart.classList.remove('flex');
        pdvTryRemoveModalOpenBody();
    }

    /** Nova venda: zera carrinho/cliente e reabre o pop-up inicial (consumidor / buscar cliente). */
    function resetWizardParaNovaVenda() {
        closePaymentFormaModal();
        hideMpPointWaitBar();
        if (dom.stepPagamentoRoot) {
            dom.stepPagamentoRoot.querySelectorAll('dialog[open]').forEach(function (dlg) {
                try {
                    dlg.close();
                } catch (errDlg) {}
            });
        }
        State.reset(false);
        State.setCurrentStep('produtos');
        openStartModal();
    }

    function openBudgetHistory() {
        var state = State.getState();
        var key = budgetClienteKeyFromState(state);
        var clienteNome =
            state.cliente && state.cliente.nome
                ? String(state.cliente.nome).trim()
                : state.clienteMode === 'consumidor_final'
                  ? 'Consumidor não identificado'
                  : 'Cliente';

        function paintModal() {
            var historico = filterHistoricoPorCliente(readHistoricoOrcamentos(), key);
            var older = historico.slice(PDV_BUDGET_CARD_VISIBLE);
            dom.budgetHistoryList.innerHTML = older.length
            ? older
                  .map(function (item) {
                      var itens = Array.isArray(item.itens) ? item.itens.length : 0;
                      return (
                          '' +
                          '<div class="mb-3 rounded-2xl border border-slate-200 bg-slate-50 p-4">' +
                          '  <div class="flex flex-wrap items-start justify-between gap-3">' +
                          '    <div>' +
                          '      <div class="text-sm font-black text-slate-900">' +
                          escapeHtml(formatBudgetCardDate(item.data)) +
                          ' <span class="text-[11px] font-semibold text-slate-500">' +
                          escapeHtml(item.data && String(item.data).indexOf(',') > -1 ? String(item.data).split(',')[1].trim() : '') +
                          '</span></div>' +
                          '      <div class="mt-1 text-[11px] font-bold text-slate-500">' +
                          escapeHtml(item.total || 'R$ 0,00') +
                          ' • ' +
                          itens +
                          ' item(ns)' +
                          (item.orc_barcode
                              ? ' • <span class="font-mono">' + escapeHtml(String(item.orc_barcode)) + '</span>'
                              : '') +
                          '</div>' +
                          '    </div>' +
                          '    <button type="button" class="rounded-xl bg-emerald-600 px-3 py-2 text-[11px] font-black uppercase text-white" data-budget-id="' +
                          escapeHtml(String(item.id)) +
                          '">Reabrir</button>' +
                          '  </div>' +
                          '</div>'
                      );
                  })
                  .join('')
            : '<div class="rounded-2xl border border-dashed border-slate-300 bg-slate-50 px-4 py-10 text-center text-sm font-bold text-slate-400">' +
              (historico.length
                  ? 'Não há orçamentos mais antigos para ' +
                    escapeHtml(clienteNome) +
                    '. Os ' +
                    PDV_BUDGET_CARD_VISIBLE +
                    ' mais recentes estão no card ao lado — clique na linha para reabrir.'
                  : 'Nenhum orçamento salvo para ' + escapeHtml(clienteNome) + '.') +
              '</div>';
            dom.budgetHistoryModal.classList.remove('hidden');
            dom.budgetHistoryModal.classList.add('flex');
        }

        syncHistoricoOrcamentosCliente(key, { silent: true }).finally(function () {
            paintModal();
        });
    }

    function closeBudgetHistory() {
        dom.budgetHistoryModal.classList.add('hidden');
        dom.budgetHistoryModal.classList.remove('flex');
    }

    function pdvEnsureModalOpenBody() {
        try {
            document.body.classList.add('modal-open');
        } catch (eM) {}
        syncPdvSspinIdlePause();
    }

    function isQuickClientModalOpen() {
        return !!(
            dom.quickClientModal &&
            !dom.quickClientModal.classList.contains('hidden')
        );
    }

    function pdvTryRemoveModalOpenBody() {
        var mdEsc = document.getElementById('modal-pdv-entrega-salvar-cliente');
        var mei = document.getElementById('modal-pdv-entrega-impressao');
        var meiOpen = mei && !mei.classList.contains('hidden');
        var escOpen = mdEsc && !mdEsc.classList.contains('hidden');
        var cliOpen = isQuickClientModalOpen();
        var startOpen = dom.modalStart && !dom.modalStart.classList.contains('hidden');
        var cadOpen =
            dom.wizardCliRapidoModal && !dom.wizardCliRapidoModal.classList.contains('hidden');
        try {
            if (!escOpen && !meiOpen && !cliOpen && !startOpen && !cadOpen) {
                document.body.classList.remove('modal-open');
            }
        } catch (eM2) {}
        syncPdvSspinIdlePause();
    }

    function syncPdvSspinIdlePause() {
        if (typeof window.gmSspinRecomputeFromDom === 'function') {
            window.gmSspinRecomputeFromDom();
        }
    }

    function fecharOverlayGenerico(el) {
        if (!el || el.id === 'sspin-root') return;
        try {
            if (String(el.tagName || '').toUpperCase() === 'DIALOG' && el.open) {
                el.close();
                return;
            }
        } catch (eDlg) {}
        el.classList.add('hidden');
        el.classList.remove('flex');
        try {
            el.setAttribute('aria-hidden', 'true');
        } catch (eAr) {}
    }

    function fecharModaisPdvAntesDescanso() {
        closeEntregasPendentesModal();
        closePaymentFormaModal();
        closeStartModal();
        closeClienteEditModal();
        closeFiadoVencidosModal();
        closeBudgetHistory();
        closeEntregaSalvarClienteModal();
        try {
            if (dom.quickClientEditOverlay && !dom.quickClientEditOverlay.classList.contains('hidden')) {
                dom.quickClientEditOverlay.classList.add('hidden');
                dom.quickClientEditOverlay.classList.remove('flex');
            }
        } catch (eQce) {}
        try {
            document.querySelectorAll('dialog[open]').forEach(function (dlg) {
                try {
                    dlg.close();
                } catch (eD) {}
            });
        } catch (eDlgAll) {}
        try {
            document.querySelectorAll('[aria-modal="true"]').forEach(fecharOverlayGenerico);
        } catch (eAm) {}
        try {
            var ew = document.getElementById('pdv-entrega-wizard');
            if (ew) fecharOverlayGenerico(ew);
        } catch (eEw) {}
        try {
            var drawer = document.getElementById('pdv-drawer-carrinho');
            var backdrop = document.getElementById('pdv-carrinho-backdrop');
            if (drawer) {
                drawer.classList.add('translate-x-full');
                drawer.setAttribute('aria-hidden', 'true');
            }
            if (backdrop) {
                backdrop.classList.add('opacity-0', 'pointer-events-none');
                backdrop.setAttribute('aria-hidden', 'true');
            }
        } catch (eDr) {}
        try {
            document.body.classList.remove('modal-open');
            document.body.style.overflow = '';
        } catch (eBody) {}
        syncPdvSspinIdlePause();
    }

    function isRetiradaEntregaModalOpen() {
        return false;
    }
    function isEntregaDetalhesModalOpen() {
        return !!(
            dom.entregaWizard &&
            !dom.entregaWizard.classList.contains('hidden') &&
            entregaWizardPrecisaExibir()
        );
    }
    function closeRetiradaEntregaModal() {}
    function openRetiradaEntregaModal() {
        scrollEntregaWizardIntoView();
    }
    function closeEntregaDetalhesModal() {}
    function openEntregaDetalhesModal() {
        renderEntregaTaxaCard(State.getState());
        syncEntregaDetalhesModalUi();
        scrollEntregaWizardIntoView();
    }
    function resetEntregaModoAoVoltarProdutos() {
        State.setEntregaPatch({
            modoRetiradaEntrega: '',
            detalhesEntregaRespondidos: false,
            ativa: false,
            localPagamento: '',
            meioNaEntrega: '',
            taxaEntregaRespondida: false,
            taxaEntregaModo: '',
            enderecoPassoConcluido: false,
            horario: '',
            troco: '',
            logradouro: '',
            numero: '',
            bairro: '',
            plusCode: '',
            complemento: '',
            referencia: '',
            endereco: ''
        });
        State.setPagamentoField('frete', 0);
        closeEntregaSalvarClienteModal();
        entregaWizardAguardandoTroco = false;
        entregaPendingAfterSaveCliente = null;
        entregaPlusGeocodeLastQ = '';
        resetEntregaClienteSnapshot();
    }
    function escolherRetiradaEntrega(modo) {
        if (modo === 'retirada') {
            State.setEntregaPatch({
                modoRetiradaEntrega: 'retirada',
                ativa: false,
                detalhesEntregaRespondidos: false,
                localPagamento: '',
                meioNaEntrega: ''
            });
            State.setPagamentoField('frete', 0);
            State.setCurrentStep('pagamento');
        }
    }

    function isEntregaFluxo1Open() {
        return isEntregaDetalhesModalOpen() && entregaWizardPainelAtual() === 'pagamento_local';
    }
    function isEntregaFluxo2Open() {
        return isEntregaDetalhesModalOpen() && entregaWizardPainelAtual() === 'meio';
    }
    function isEntregaFluxoDetalhesOpen() {
        return isEntregaDetalhesModalOpen() && entregaWizardPainelAtual() === 'detalhes';
    }
    function isEntregaFluxo3Open() {
        return isEntregaDetalhesModalOpen() && entregaWizardPainelAtual() === 'troco';
    }
    function isAnyEntregaFluxoModalOpen() {
        return isEntregaDetalhesModalOpen() || isEntregaSalvarClienteModalOpen();
    }

    function closeEntregaFluxoModal1() {}
    function openEntregaFluxoModal1() {
        openEntregaDetalhesModal();
    }
    function closeEntregaFluxoModal2() {}
    function openEntregaFluxoModal2() {
        openEntregaDetalhesModal();
    }
    function closeEntregaFluxoModal3() {
        entregaWizardAguardandoTroco = false;
        syncEntregaDetalhesModalUi();
    }
    function openEntregaFluxoModal3() {
        entregaWizardAguardandoTroco = true;
        var inp = document.getElementById('pdv-ef3-troco-input');
        var st = State.getState();
        if (inp) {
            inp.value = String((st.entrega && st.entrega.troco) || '').trim();
        }
        openEntregaDetalhesModal();
        if (inp) {
            setTimeout(function () {
                try {
                    inp.focus();
                    inp.select();
                } catch (eI) {}
            }, 80);
        }
    }

    function setQuickClientPickerHighlight(on) {
        if (dom.quickClientModal) {
            dom.quickClientModal.classList.toggle('pdv-client-modal-open', !!on);
        }
        if (dom.quickClientSearch) {
            dom.quickClientSearch.classList.toggle('pdv-client-search-hot', !!on);
            if (!on) dom.quickClientSearch.classList.remove('pdv-client-search-typed');
        }
    }

    function resetQuickClientResultsIdle() {
        if (!dom.quickClientResults) return;
        dom.quickClientResults.innerHTML =
            '<p class="px-4 py-6 text-center text-sm font-bold text-slate-500">Digite pelo menos 2 letras para buscar.</p>';
        delete dom.quickClientResults._clientes;
        clientListSelectIdx = -1;
    }

    function focusQuickClientSearchField() {
        if (!dom.quickClientSearch) return;
        try {
            dom.quickClientSearch.focus({ preventScroll: true });
        } catch (e) {
            dom.quickClientSearch.focus();
        }
        try {
            dom.quickClientSearch.select();
        } catch (e2) { /* ignore */ }
    }

    function openQuickClientPicker() {
        if (!dom.quickClientModal || !dom.quickClientSearch) return;
        clientListSelectIdx = -1;
        dom.quickClientSearch.value = '';
        resetQuickClientResultsIdle();
        dom.quickClientModal.classList.remove('hidden');
        dom.quickClientModal.classList.add('flex');
        pdvEnsureModalOpenBody();
        setQuickClientPickerHighlight(true);
        loadWizardClientesCache(false);
        window.setTimeout(focusQuickClientSearchField, 40);
        window.setTimeout(focusQuickClientSearchField, 180);
    }

    function closeQuickClientPicker() {
        if (!dom.quickClientModal) return;
        closeQuickClientEditOverlay();
        dom.quickClientModal.classList.add('hidden');
        dom.quickClientModal.classList.remove('flex');
        setQuickClientPickerHighlight(false);
        if (dom.quickClientSearch) dom.quickClientSearch.value = '';
        clientListSelectIdx = -1;
        pdvTryRemoveModalOpenBody();
        focusProductSearch();
    }

    function isPhoneLikeClientSearchTerm(term) {
        var t = String(term || '').trim();
        if (!t) return false;
        var digits = t.replace(/\D/g, '');
        var compact = t.replace(/\s/g, '');
        if (digits.length >= 8 && compact.length && digits.length >= compact.length * 0.65) {
            return true;
        }
        return /^\d[\d\s().+-]*$/.test(t);
    }

    function clearWizardQuickClientCadastroForm() {
        var ids = [
            'pdv-wizard-cli-rapido-nome',
            'pdv-wizard-cli-rapido-whatsapp',
            'pdv-wizard-cli-rapido-cpf',
            'pdv-wizard-cli-rapido-logradouro',
            'pdv-wizard-cli-rapido-numero',
            'pdv-wizard-cli-rapido-bairro',
            'pdv-wizard-cli-rapido-cidade',
            'pdv-wizard-cli-rapido-uf',
            'pdv-wizard-cli-rapido-cep'
        ];
        ids.forEach(function (id) {
            var el = document.getElementById(id);
            if (el) el.value = '';
        });
        if (dom.wizardCliRapidoErro) {
            dom.wizardCliRapidoErro.textContent = '';
            dom.wizardCliRapidoErro.classList.add('hidden');
        }
    }

    function closeWizardQuickClientCadastro() {
        if (!dom.wizardCliRapidoModal) return;
        dom.wizardCliRapidoModal.classList.add('hidden');
        dom.wizardCliRapidoModal.classList.remove('flex');
        clearWizardQuickClientCadastroForm();
        pdvTryRemoveModalOpenBody();
        if (isQuickClientModalOpen()) {
            window.setTimeout(focusQuickClientSearchField, 60);
        }
    }

    function openWizardQuickClientCadastro(prefillNome) {
        if (!dom.wizardCliRapidoModal) return;
        clearWizardQuickClientCadastroForm();
        var nome = String(prefillNome || '').trim();
        if (!nome && lastClientSearchQuery && !isPhoneLikeClientSearchTerm(lastClientSearchQuery)) {
            nome = lastClientSearchQuery.trim();
        }
        if (nome && dom.wizardCliRapidoNome) dom.wizardCliRapidoNome.value = nome;
        if (
            lastClientSearchQuery &&
            isPhoneLikeClientSearchTerm(lastClientSearchQuery) &&
            dom.wizardCliRapidoWhatsapp
        ) {
            dom.wizardCliRapidoWhatsapp.value = lastClientSearchQuery.trim();
        }
        dom.wizardCliRapidoModal.classList.remove('hidden');
        dom.wizardCliRapidoModal.classList.add('flex');
        pdvEnsureModalOpenBody();
        setTimeout(function () {
            if (dom.wizardCliRapidoNome && dom.wizardCliRapidoNome.value) {
                dom.wizardCliRapidoWhatsapp.focus();
            } else if (dom.wizardCliRapidoNome) {
                dom.wizardCliRapidoNome.focus();
            }
        }, 80);
    }

    function saveWizardQuickClientCadastro() {
        var url = urls.apiPdvClienteRapido;
        if (!url) {
            alert('Cadastro rápido indisponível (URL).');
            return;
        }
        var nome = dom.wizardCliRapidoNome
            ? String(dom.wizardCliRapidoNome.value || '').trim()
            : '';
        var wa = dom.wizardCliRapidoWhatsapp
            ? String(dom.wizardCliRapidoWhatsapp.value || '').trim()
            : '';
        if (dom.wizardCliRapidoErro) {
            dom.wizardCliRapidoErro.textContent = '';
            dom.wizardCliRapidoErro.classList.add('hidden');
        }
        if (nome.length < 2) {
            if (dom.wizardCliRapidoErro) {
                dom.wizardCliRapidoErro.textContent = 'Informe o nome do cliente (mínimo 2 caracteres).';
                dom.wizardCliRapidoErro.classList.remove('hidden');
            } else {
                alert('Informe o nome do cliente.');
            }
            if (dom.wizardCliRapidoNome) dom.wizardCliRapidoNome.focus();
            return;
        }
        var waDigits = wa.replace(/\D/g, '');
        if (waDigits.length < 10) {
            var msgTel = 'Informe o telefone ou WhatsApp com DDD (mínimo 10 dígitos).';
            if (dom.wizardCliRapidoErro) {
                dom.wizardCliRapidoErro.textContent = msgTel;
                dom.wizardCliRapidoErro.classList.remove('hidden');
            } else {
                alert(msgTel);
            }
            if (dom.wizardCliRapidoWhatsapp) dom.wizardCliRapidoWhatsapp.focus();
            return;
        }
        var cpfCheck = pdvValidarCpfOpcional(
            dom.wizardCliRapidoCpf ? dom.wizardCliRapidoCpf.value : ''
        );
        if (!cpfCheck.ok) {
            if (dom.wizardCliRapidoErro) {
                dom.wizardCliRapidoErro.textContent = cpfCheck.msg || 'CPF inválido.';
                dom.wizardCliRapidoErro.classList.remove('hidden');
            } else {
                alert(cpfCheck.msg || 'CPF inválido.');
            }
            if (dom.wizardCliRapidoCpf) dom.wizardCliRapidoCpf.focus();
            return;
        }
        function gv(id) {
            var el = document.getElementById(id);
            return el ? String(el.value || '').trim() : '';
        }
        var btn = dom.wizardCliRapidoSalvar;
        var prevLabel = btn ? btn.textContent : '';
        if (btn) {
            btn.disabled = true;
            btn.textContent = 'Salvando…';
        }
        jsonPost(url, {
            nome: nome,
            whatsapp: wa,
            cpf: cpfCheck.cpf,
            logradouro: gv('pdv-wizard-cli-rapido-logradouro'),
            numero: gv('pdv-wizard-cli-rapido-numero'),
            bairro: gv('pdv-wizard-cli-rapido-bairro'),
            cidade: gv('pdv-wizard-cli-rapido-cidade'),
            uf: gv('pdv-wizard-cli-rapido-uf'),
            cep: gv('pdv-wizard-cli-rapido-cep')
        })
            .then(function (res) {
                if (res.ok && res.data && res.data.ok && res.data.cliente) {
                    State.setCliente(res.data.cliente, 'cliente');
                    syncEntregaEnderecoFromCliente();
                    closeWizardQuickClientCadastro();
                    closeQuickClientPicker();
                    refreshCreditoFiadoCliente(null, { force: true, showVencidosAlert: true });
                    return;
                }
                var err =
                    (res.data && res.data.erro) ||
                    'Não foi possível salvar o cliente.';
                if (dom.wizardCliRapidoErro) {
                    dom.wizardCliRapidoErro.textContent = err;
                    dom.wizardCliRapidoErro.classList.remove('hidden');
                } else {
                    alert(err);
                }
            })
            .catch(function () {
                var msg = 'Erro de rede ao cadastrar cliente.';
                if (dom.wizardCliRapidoErro) {
                    dom.wizardCliRapidoErro.textContent = msg;
                    dom.wizardCliRapidoErro.classList.remove('hidden');
                } else {
                    alert(msg);
                }
            })
            .finally(function () {
                if (btn) {
                    btn.disabled = false;
                    btn.textContent = prevLabel || 'Salvar cliente';
                }
            });
    }

    function highlightClientListRow() {
        if (!dom.quickClientResults) return;
        var rows = dom.quickClientResults.querySelectorAll('[data-client-row-idx]');
        for (var i = 0; i < rows.length; i++) {
            var el = rows[i];
            var idx = parseInt(el.getAttribute('data-client-row-idx') || '-1', 10);
            var on = idx === clientListSelectIdx;
            el.classList.toggle('pdv-client-row-selected', on);
            el.setAttribute('aria-selected', on ? 'true' : 'false');
            if (on) {
                try {
                    el.scrollIntoView({ block: 'nearest' });
                } catch (errS) {}
            }
        }
    }

    function clientSearchResultRowHtml(cliente, idx) {
        var pk = cliente && cliente.cliente_agro_pk;
        var canEdit = pk != null && pk !== '';
        var nomeFull = String(cliente.nome || '');
        var tel = cliente && String(cliente.telefone || '').trim();
        var telLabel = tel || 'Sem telefone';
        var plusCode = cliente && String(cliente.plus_code || '').trim();
        var mapsHtml = plusCode
            ? '<span class="pdv-client-maps-ok">Maps ok</span>'
            : '<span class="pdv-client-maps-falta">Maps Falta</span>';
        return (
            '' +
            '<tr class="pdv-client-results-data-row" role="option" data-client-row-idx="' +
            idx +
            '" data-select-client="' +
            escapeHtml(cliente.id) +
            '" data-client-list-idx="' +
            idx +
            '" aria-selected="false" title="' +
            escapeHtml(nomeFull) +
            '">' +
            '<td class="pdv-client-cell-name"><span class="pdv-client-result-name">' +
            escapeHtml(nomeFull) +
            '</span></td>' +
            '<td class="pdv-client-cell-tel"><span class="pdv-client-result-tel">' +
            escapeHtml(telLabel) +
            '</span></td>' +
            '<td class="pdv-client-cell-maps">' +
            mapsHtml +
            '</td>' +
            '<td class="pdv-client-cell-edit">' +
            (canEdit
                ? '<button type="button" class="rounded-xl border-2 border-sky-300 bg-sky-50 px-3 py-2 text-[clamp(0.75rem,0.35vw+0.65rem,0.95rem)] font-black uppercase tracking-wide text-sky-900 hover:bg-sky-100 min-h-[2.75rem]" ' +
                  'data-edit-client="' +
                  escapeHtml(String(pk)) +
                  '" data-client-list-idx="' +
                  idx +
                  '" title="Editar cadastro do cliente">Editar</button>'
                : '') +
            '</td>' +
            '</tr>'
        );
    }

    function clientSearchResultsHeaderHtml() {
        return (
            '<thead><tr>' +
            '<th scope="col">Nome</th>' +
            '<th scope="col">Telefone</th>' +
            '<th scope="col">Maps</th>' +
            '<th scope="col"><span class="sr-only">Ações</span></th>' +
            '</tr></thead><tbody>'
        );
    }

    function renderClientSearchResults(clientes) {
        if (!dom.quickClientResults) return;
        dom.quickClientResults._clientes = clientes;
        if (!clientes.length) {
            dom.quickClientResults.innerHTML =
                '<p class="px-4 py-6 text-center text-sm font-bold text-slate-500">Nenhum cliente encontrado para este termo.</p>';
            clientListSelectIdx = -1;
            return;
        }
        if (clientListSelectIdx < 0 || clientListSelectIdx >= clientes.length) {
            clientListSelectIdx = 0;
        }
        dom.quickClientResults.innerHTML =
            '<table class="pdv-client-results-table">' +
            clientSearchResultsHeaderHtml() +
            clientes
            .map(function (cliente, idx) {
                return clientSearchResultRowHtml(cliente, idx);
            })
            .join('') +
            '</tbody></table>';
        highlightClientListRow();
    }

    function isQuickClientEditOpen() {
        return !!(
            dom.quickClientEditOverlay &&
            dom.quickClientEditOverlay.classList.contains('flex')
        );
    }

    function clearQuickClientEditForm() {
        quickClientEditPk = null;
        quickClientEditListIdx = -1;
        [
            dom.quickClientEditNome,
            dom.quickClientEditWhatsapp,
            dom.quickClientEditCpf,
            dom.quickClientEditLogradouro,
            dom.quickClientEditNumero,
            dom.quickClientEditBairro,
            dom.quickClientEditCidade,
            dom.quickClientEditUf,
            dom.quickClientEditCep,
            dom.quickClientEditComplemento,
            dom.quickClientEditPluscode,
            dom.quickClientEditReferencia,
        ].forEach(function (el) {
            if (el) el.value = '';
        });
        if (dom.quickClientEditErro) {
            dom.quickClientEditErro.textContent = '';
            dom.quickClientEditErro.classList.add('hidden');
        }
        clearQuickClientEditMissingHighlights();
    }

    function fillQuickClientEditForm(cliente) {
        if (!cliente) return;
        if (dom.quickClientEditNome) dom.quickClientEditNome.value = String(cliente.nome || '');
        if (dom.quickClientEditWhatsapp) {
            dom.quickClientEditWhatsapp.value = String(cliente.telefone || '');
        }
        if (dom.quickClientEditCpf) {
            dom.quickClientEditCpf.value = clienteCpfParaExibir(cliente);
        }
        if (dom.quickClientEditLogradouro) {
            dom.quickClientEditLogradouro.value = String(cliente.logradouro || '');
        }
        if (dom.quickClientEditNumero) dom.quickClientEditNumero.value = String(cliente.numero || '');
        if (dom.quickClientEditBairro) {
            dom.quickClientEditBairro.value = canonicalBairroJacupiSePossivel(cliente.bairro || '');
        }
        if (dom.quickClientEditCidade) dom.quickClientEditCidade.value = String(cliente.cidade || '');
        if (dom.quickClientEditUf) dom.quickClientEditUf.value = String(cliente.uf || '');
        if (dom.quickClientEditCep) dom.quickClientEditCep.value = String(cliente.cep || '');
        if (dom.quickClientEditComplemento) {
            dom.quickClientEditComplemento.value = String(cliente.complemento || '');
        }
        if (dom.quickClientEditPluscode) {
            dom.quickClientEditPluscode.value = String(cliente.plus_code || '');
        }
        if (dom.quickClientEditReferencia) {
            dom.quickClientEditReferencia.value = String(cliente.referencia_rural || '');
        }
        if (dom.quickClientEditTitle) {
            dom.quickClientEditTitle.textContent = String(cliente.nome || 'Cliente');
            dom.quickClientEditTitle.setAttribute('title', String(cliente.nome || ''));
        }
        refreshQuickClientEditMissingHighlights();
    }

    function openQuickClientEditOverlay(cliente, listIdx) {
        var pk = clienteAgroPkFromCliente(cliente);
        if (!dom.quickClientEditOverlay || !cliente || !pk) return;
        quickClientEditPk = pk;
        quickClientEditListIdx = listIdx != null ? listIdx : -1;
        if (dom.quickClientEditErro) {
            dom.quickClientEditErro.textContent = '';
            dom.quickClientEditErro.classList.add('hidden');
        }
        fillQuickClientEditForm(Object.assign({}, cliente, { cliente_agro_pk: pk }));
        quickClientEditBairroRuralExpandido = false;
        quickClientGeocodeLastQ = String(cliente.plus_code || '').trim();
        initQuickClientEditBairroComboboxOnce();
        initQuickClientEditMissingListenersOnce();
        dom.quickClientEditOverlay.classList.remove('hidden');
        dom.quickClientEditOverlay.classList.add('flex');
        window.setTimeout(function () {
            if (dom.quickClientEditWhatsapp) dom.quickClientEditWhatsapp.focus();
        }, 60);
    }

    function openStep1QuickClientEdit() {
        var state = State.getState();
        if (state.clienteMode === 'consumidor_final' || !state.cliente) {
            alert('Selecione um cliente cadastrado para editar.');
            return;
        }
        var pk = clienteAgroPkFromCliente(state.cliente);
        if (!pk) {
            alert('Este cliente não pode ser editado aqui (sem cadastro local).');
            return;
        }
        openQuickClientEditOverlay(Object.assign({}, state.cliente, { cliente_agro_pk: pk }), -1);
    }

    function closeQuickClientEditOverlay() {
        if (!dom.quickClientEditOverlay) return;
        var pickerOpen =
            dom.quickClientModal && !dom.quickClientModal.classList.contains('hidden');
        dom.quickClientEditOverlay.classList.add('hidden');
        dom.quickClientEditOverlay.classList.remove('flex');
        clearQuickClientEditForm();
        if (pickerOpen) window.setTimeout(focusQuickClientSearchField, 40);
    }

    function patchClienteInSearchResults(updated) {
        if (!updated || !dom.quickClientResults) return;
        var clientes = dom.quickClientResults._clientes || [];
        var pk = updated.cliente_agro_pk;
        var idx = -1;
        var i;
        for (i = 0; i < clientes.length; i++) {
            if (String(clientes[i].cliente_agro_pk) === String(pk)) {
                idx = i;
                break;
            }
        }
        if (idx < 0 && quickClientEditListIdx >= 0) idx = quickClientEditListIdx;
        if (idx >= 0) {
            clientes[idx] = Object.assign({}, clientes[idx], updated);
            renderClientSearchResults(clientes);
        }
        var st = State.getState();
        if (st.cliente && String(st.cliente.id) === String(updated.id)) {
            State.setCliente(updated, st.clienteMode === 'consumidor_final' ? 'consumidor_final' : 'cliente');
            syncEntregaEnderecoFromCliente();
        }
    }

    function saveQuickClientEditOverlay() {
        var pattern = urls.apiPdvClienteEditarPattern;
        if (!pattern || !quickClientEditPk) {
            alert('Edição indisponível (URL).');
            return;
        }
        var nome = dom.quickClientEditNome
            ? String(dom.quickClientEditNome.value || '').trim()
            : '';
        if (dom.quickClientEditErro) {
            dom.quickClientEditErro.textContent = '';
            dom.quickClientEditErro.classList.add('hidden');
        }
        if (nome.length < 2) {
            if (dom.quickClientEditErro) {
                dom.quickClientEditErro.textContent =
                    'Informe o nome do cliente (mínimo 2 caracteres).';
                dom.quickClientEditErro.classList.remove('hidden');
            }
            if (dom.quickClientEditNome) dom.quickClientEditNome.focus();
            return;
        }
        var wa = dom.quickClientEditWhatsapp
            ? String(dom.quickClientEditWhatsapp.value || '').trim()
            : '';
        var waDigits = wa.replace(/\D/g, '');
        if (waDigits.length < 10) {
            if (dom.quickClientEditErro) {
                dom.quickClientEditErro.textContent =
                    'Informe o WhatsApp com DDD (mínimo 10 dígitos).';
                dom.quickClientEditErro.classList.remove('hidden');
            }
            if (dom.quickClientEditWhatsapp) dom.quickClientEditWhatsapp.focus();
            return;
        }
        var cpfCheck = pdvValidarCpfOpcional(
            dom.quickClientEditCpf ? dom.quickClientEditCpf.value : ''
        );
        if (!cpfCheck.ok) {
            if (dom.quickClientEditErro) {
                dom.quickClientEditErro.textContent = cpfCheck.msg || 'CPF inválido.';
                dom.quickClientEditErro.classList.remove('hidden');
            }
            if (dom.quickClientEditCpf) dom.quickClientEditCpf.focus();
            return;
        }
        var url = pattern.replace('__pk__', String(quickClientEditPk));
        var payload = {
            nome: nome,
            whatsapp: wa,
            cpf: cpfCheck.cpf,
            logradouro: dom.quickClientEditLogradouro ? dom.quickClientEditLogradouro.value : '',
            numero: dom.quickClientEditNumero ? dom.quickClientEditNumero.value : '',
            bairro: dom.quickClientEditBairro ? dom.quickClientEditBairro.value : '',
            cidade: dom.quickClientEditCidade ? dom.quickClientEditCidade.value : '',
            uf: dom.quickClientEditUf ? dom.quickClientEditUf.value : '',
            cep: dom.quickClientEditCep ? dom.quickClientEditCep.value : '',
            complemento: dom.quickClientEditComplemento ? dom.quickClientEditComplemento.value : '',
            plus_code: dom.quickClientEditPluscode ? dom.quickClientEditPluscode.value : '',
            referencia_rural: dom.quickClientEditReferencia ? dom.quickClientEditReferencia.value : '',
        };
        var btn = dom.quickClientEditSalvar;
        var prevLabel = btn ? btn.textContent : '';
        if (btn) {
            btn.disabled = true;
            btn.textContent = 'Salvando…';
        }
        jsonPost(url, payload)
            .then(function (res) {
                if (!res.ok || !res.data || !res.data.ok) {
                    var err =
                        (res.data && (res.data.erro || res.data.error)) ||
                        'Não foi possível salvar o cliente.';
                    if (dom.quickClientEditErro) {
                        dom.quickClientEditErro.textContent = err;
                        dom.quickClientEditErro.classList.remove('hidden');
                    } else {
                        alert(err);
                    }
                    return;
                }
                patchClienteInSearchResults(res.data.cliente);
                closeQuickClientEditOverlay();
            })
            .catch(function () {
                var msg = 'Erro de rede ao salvar cliente.';
                if (dom.quickClientEditErro) {
                    dom.quickClientEditErro.textContent = msg;
                    dom.quickClientEditErro.classList.remove('hidden');
                } else {
                    alert(msg);
                }
            })
            .finally(function () {
                if (btn) {
                    btn.disabled = false;
                    btn.textContent = prevLabel || 'Salvar';
                }
            });
    }

    function resetProductSearchUi(message) {
        invalidatePendingProductSearch();
        clearProductSearchDismissedSnapshot();
        dom.productSearch.value = '';
        hideProductAutocomplete({ skipSnapshot: true });
        dom.productSearchMeta.textContent = 'Aguardando busca';
        dom.productSearchFeedback.textContent = message || 'Digite para filtrar o catálogo local.';
        focusProductSearch();
        updateSearchAwaitingPulse();
    }

    function runProductSearch(term, mode) {
        var query = String(term || '').trim();
        if (reopenBudgetFromBarcode(query)) return;
        if (!query) {
            renderProductResults([]);
            dom.productSearchMeta.textContent = 'Aguardando busca';
            dom.productSearchFeedback.textContent = 'Digite para filtrar o catálogo local.';
            updateSearchAwaitingPulse();
            return;
        }
        if (!allowLocalQuery(query)) {
            renderProductResults([]);
            dom.productSearchFeedback.textContent = 'Digite ao menos 2 letras ou 6+ dígitos do código.';
            updateSearchAwaitingPulse();
            return;
        }
        var seq = ++filterSeq;
        productSearchAwaitingServer = true;
        productSearchMayHaveMore = false;
        dom.productSearchFeedback.textContent = catalogReady ? 'Filtrando…' : 'Carregando catálogo local…';
        loadWizardCatalog()
            .then(function () {
                if (seq !== filterSeq) return;
                var r = filterCatalogLocal(query, mode);
                if (r.barcodeHit) {
                    productSearchAwaitingServer = false;
                    tryAddProductFromSearch(r.barcodeHit, {
                        okMsg: 'Item adicionado pela leitura do código.',
                        query: query,
                        forceServer: true,
                    });
                    return Promise.resolve();
                }
                var localList = normalizeWizardCatalogList(r.list || []);
                var skuCode = looksLikeSkuCode(query);
                var qlSku = String(query).trim().toLowerCase();
                if (mode === 'barcode' && localList.length === 1) {
                    productSearchAwaitingServer = false;
                    tryAutoAddBarcodeHit(localList[0]);
                    return null;
                }
                if (skuCode && localList.length && localSkuCacheSufficient(localList, qlSku)) {
                    finishLocalProductSearch(localList);
                    return null;
                }
                if (mode === 'manual' && !skuCode && localTextCacheSufficient(localList)) {
                    finishLocalProductSearch(localList);
                    return null;
                }
                if (localList.length >= AUTOCOMPLETE_PAGE_SIZE) {
                    productSearchMayHaveMore = true;
                }
                if (localList.length) {
                    renderProductResults(localList);
                }
                dom.productSearchFeedback.textContent = localList.length
                    ? 'Cache local · conferindo servidor…'
                    : skuCode
                      ? 'Buscando variantes do código…'
                      : 'Buscando no servidor…';
                return fetchWizardServerSearch(query)
                    .then(function (srv) {
                        return {
                            remote: srv.produtos,
                            exactBarcode: srv.exactBarcode,
                            localList: localList,
                            skuCode: skuCode,
                            mode: mode,
                        };
                    })
                    .catch(function () {
                        return {
                            remote: [],
                            exactBarcode: false,
                            localList: localList,
                            skuCode: skuCode,
                            mode: mode,
                        };
                    });
            })
            .then(function (payload) {
                if (seq !== filterSeq) return;
                if (!payload || !Array.isArray(payload.remote)) return;
                productSearchAwaitingServer = false;
                var remote = payload.remote;
                var merged = stagingReadonly
                    ? mergeProductsById(remote, payload.localList || [])
                    : mergeProductsById(payload.localList || [], remote);
                if (payload.mode === 'barcode' && merged.length === 1) {
                    tryAutoAddBarcodeHit(merged[0]);
                    return;
                }
                if (payload.mode === 'barcode' && payload.exactBarcode && merged.length >= 1) {
                    tryAutoAddBarcodeHit(merged[0]);
                    return;
                }
                if (merged.length) {
                    productSearchMayHaveMore = merged.length > AUTOCOMPLETE_PAGE_SIZE;
                    renderProductResults(merged);
                    if (payload.skuCode && remote.length) {
                        dom.productSearchFeedback.textContent =
                            merged.length +
                            ' encontrado(s) (cache + servidor, variantes de código).';
                    } else if (remote.length && !(payload.localList || []).length) {
                        dom.productSearchFeedback.textContent =
                            remote.length + ' encontrado(s) no servidor (fora do cache).';
                    } else if (remote.length) {
                        dom.productSearchFeedback.textContent =
                            merged.length +
                            ' encontrado(s) (cache + servidor).';
                    } else {
                        dom.productSearchFeedback.textContent =
                            'Cache local (' + wizardProductCatalog.length + ' produtos).';
                    }
                } else {
                    productSearchMayHaveMore = false;
                    renderProductResults([]);
                    dom.productSearchFeedback.textContent =
                        'Nenhum produto para este termo (cache e servidor).';
                }
            })
            .catch(function () {
                if (seq !== filterSeq) return;
                productSearchAwaitingServer = false;
                productSearchMayHaveMore = false;
                dom.productSearchFeedback.textContent =
                    'Não foi possível carregar o catálogo. Atualize a página.';
                renderProductResults([]);
            });
    }

    function normalizeClientSearchText(s) {
        return String(s || '')
            .toLowerCase()
            .normalize('NFD')
            .replace(/[\u0300-\u036f]/g, '')
            .replace(/\s+/g, ' ')
            .trim();
    }

    function filterWizardClientesLocal(q) {
        var n = normalizeClientSearchText(q);
        if (n.length < 2) return [];
        return wizardClientesCache
            .filter(function (c) {
                var nm = normalizeClientSearchText(c.nome || '');
                var tel = normalizeClientSearchText(c.telefone || '');
                var doc = normalizeClientSearchText(c.documento || c.cpf || '');
                var ed = normalizeClientSearchText(c.endereco || '');
                var pc = normalizeClientSearchText(c.plus_code || '');
                return (
                    nm.indexOf(n) >= 0 ||
                    tel.indexOf(n) >= 0 ||
                    doc.indexOf(n) >= 0 ||
                    ed.indexOf(n) >= 0 ||
                    pc.indexOf(n) >= 0
                );
            })
            .slice(0, 45);
    }

    function hydrateWizardClientesFromStorage() {
        try {
            var raw = localStorage.getItem(PDV_CLIENTES_LS_KEY);
            if (!raw) return false;
            var d = JSON.parse(raw);
            if (Array.isArray(d.clientes) && d.clientes.length) {
                wizardClientesCache = d.clientes;
                wizardClientesCacheReady = true;
                return true;
            }
        } catch (eHydr) {}
        return false;
    }

    function loadWizardClientesCache(force) {
        if (wizardClientesCacheLoading && !force) {
            return Promise.resolve(wizardClientesCache.length);
        }
        hydrateWizardClientesFromStorage();
        if (
            wizardClientesCacheReady &&
            !force &&
            window.AgroPdvOfflineCache &&
            !window.AgroPdvOfflineCache.isStale(PDV_CLIENTES_LS_KEY, window.AgroPdvOfflineCache.TTL.CLIENTES_MS)
        ) {
            return Promise.resolve(wizardClientesCache.length);
        }
        if (!urls.apiListCustomers) return Promise.resolve(wizardClientesCache.length);
        wizardClientesCacheLoading = true;
        return fetch(urls.apiListCustomers, { credentials: 'same-origin' })
            .then(function (res) {
                return res.json();
            })
            .then(function (data) {
                var list = Array.isArray(data.clientes) ? data.clientes : [];
                if (list.length) {
                    wizardClientesCache = list;
                    wizardClientesCacheReady = true;
                    try {
                        localStorage.setItem(
                            PDV_CLIENTES_LS_KEY,
                            JSON.stringify({ clientes: list, saved_at: Date.now() })
                        );
                    } catch (eLs) {}
                }
                return list.length;
            })
            .catch(function () {
                return wizardClientesCache.length;
            })
            .finally(function () {
                wizardClientesCacheLoading = false;
            });
    }

    function showClientSearchLoadingMessage() {
        if (!dom.quickClientResults) return;
        dom.quickClientResults.innerHTML =
            '<p class="px-4 py-6 text-center text-sm font-bold text-slate-500">Buscando clientes…</p>';
        dom.quickClientResults.classList.remove('hidden');
        delete dom.quickClientResults._clientes;
        clientListSelectIdx = -1;
    }

    function showClientSearchEmptyMessage() {
        if (!dom.quickClientResults) return;
        dom.quickClientResults.innerHTML =
            '<p class="px-4 py-6 text-center text-sm font-bold text-slate-500">Nenhum cliente para este termo.</p>';
        dom.quickClientResults.classList.remove('hidden');
        delete dom.quickClientResults._clientes;
        clientListSelectIdx = -1;
    }

    function runClientSearch(term) {
        var query = String(term || '').trim();
        lastClientSearchQuery = query;
        if (query.length < 2) {
            resetQuickClientResultsIdle();
            return;
        }
        var localHits = filterWizardClientesLocal(query);
        if (localHits.length) {
            renderClientSearchResults(localHits);
            dom.quickClientResults.classList.remove('hidden');
        } else if (wizardClientesCacheReady) {
            showClientSearchEmptyMessage();
        } else {
            showClientSearchLoadingMessage();
        }
        var seq = ++clientSearchSeq;
        fetch(urls.apiBuscarClientes + '?q=' + encodeURIComponent(query), { credentials: 'same-origin' })
            .then(function (res) {
                return res.json();
            })
            .then(function (data) {
                if (seq !== clientSearchSeq) return;
                var clientes = data.clientes || [];
                if (clientes.length) {
                    renderClientSearchResults(clientes);
                    dom.quickClientResults.classList.remove('hidden');
                } else if (!localHits.length) {
                    showClientSearchEmptyMessage();
                }
            })
            .catch(function () {
                if (seq !== clientSearchSeq) return;
                if (localHits.length) return;
                dom.quickClientResults.innerHTML =
                    '<div class="px-3 py-3 text-sm font-bold text-red-500">Falha ao buscar clientes.</div>';
                dom.quickClientResults.classList.remove('hidden');
                delete dom.quickClientResults._clientes;
                clientListSelectIdx = -1;
            });
    }

    function payloadItens(state) {
        return (state.itens || []).map(function (item) {
            return {
                id: item.id,
                nome: item.nome,
                qtd: item.qtd,
                preco: item.preco,
                codigo: item.codigo
            };
        });
    }

    function csrfToken() {
        return bootstrap.csrfToken || '';
    }

    function parseFetchJson(res) {
        return res.text().then(function (text) {
            var data = {};
            if (text) {
                try {
                    data = JSON.parse(text);
                } catch (parseErr) {
                    var hint =
                        res.status === 403
                            ? 'Sessão expirou ou falha de segurança. Recarregue a página (F5) e tente de novo.'
                            : 'O servidor respondeu com erro (HTTP ' + res.status + '). Tente F5; se persistir, avise o suporte.';
                    throw new Error(hint);
                }
            }
            return { ok: res.ok, status: res.status, data: data };
        });
    }

    function jsonPost(url, payload) {
        return fetch(url, {
            method: 'POST',
            credentials: 'same-origin',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': csrfToken()
            },
            body: JSON.stringify(payload || {})
        }).then(parseFetchJson);
    }

    function jsonGet(url) {
        return fetch(url, {
            method: 'GET',
            credentials: 'same-origin'
        }).then(parseFetchJson);
    }

    function pollMpPointUntilPaid(orderId) {
        var maxPoll = MP_POINT_POLL_MAX;
        var statusBase = urls.apiPdvMpPointStatus || '';
        var startedAt = Date.now();
        function userAbortError() {
            var e = new Error(mpPointWaitAbortMessage());
            e.mpPointUserAbort = true;
            return e;
        }
        function step(n) {
            if (mpPointWaitControl.cancelRequested) {
                return Promise.reject(userAbortError());
            }
            var secs = Math.floor((Date.now() - startedAt) / 1000);
            setMpPointWaitStatus('Aguardando maquininha… ' + secs + 's');
            if (n >= maxPoll) {
                return Promise.reject(
                    new Error(
                        'A maquininha não respondeu a tempo (~' +
                            Math.round((maxPoll * MP_POINT_POLL_MS) / 1000) +
                            ' s). Cancele na maquininha se o valor ainda estiver lá e tente de novo.'
                    )
                );
            }
            var sep = statusBase.indexOf('?') >= 0 ? '&' : '?';
            return jsonGet(statusBase + sep + 'order_id=' + encodeURIComponent(orderId)).then(function (stRes) {
                if (mpPointWaitControl.cancelRequested) {
                    return Promise.reject(userAbortError());
                }
                if (!stRes.ok) {
                    throw new Error((stRes.data && (stRes.data.erro || stRes.data.message)) || 'Falha ao consultar Point.');
                }
                if (!stRes.data.ok) {
                    throw new Error((stRes.data && stRes.data.erro) || 'Falha ao consultar Point.');
                }
                if (stRes.data.abandoned) {
                    return Promise.reject(userAbortError());
                }
                if (stRes.data.canceled) {
                    return Promise.reject({
                        mpPointUi: true,
                        message:
                            'Pagamento cancelado na maquininha. Em «Pagamentos lançados», altere ou exclua e tente de novo.'
                    });
                }
                if (stRes.data.failed) {
                    var fmsg =
                        (stRes.data.failed_msg && String(stRes.data.failed_msg).trim()) ||
                        'Pagamento recusado ou não concluído na maquininha.';
                    return Promise.reject({
                        mpPointUi: true,
                        message: fmsg + ' Em «Pagamentos lançados», altere ou exclua e tente de novo.'
                    });
                }
                if (stRes.data.finalized && stRes.data.venda_id) {
                    return { jaFinalizado: true, venda_id: stRes.data.venda_id };
                }
                if (stRes.data.paid) {
                    return { jaFinalizado: false, order_id: orderId };
                }
                return new Promise(function (resolve) {
                    setTimeout(function () {
                        resolve(step(n + 1));
                    }, MP_POINT_POLL_MS);
                });
            });
        }
        return step(0);
    }

    function buildCheckoutDraftPayload(state, computed) {
        var cliente = state.cliente || {};
        var draft = {
            itens: payloadItens(state),
            cliente: currentClientName(state),
            cliente_extra: {
                id: cliente.id || '',
                documento: cliente.documento || '',
                telefone: cliente.telefone || '',
                nome: cliente.nome || ''
            },
            forma_pagamento: formaPagamentoParaErp(state, computed || State.getComputed())
        };
        var pagDraft = pagamentosDetalheParaErp(state);
        if (pagDraft && pagDraft.length) draft.pagamentos = pagDraft;
        return injetarOperadorNoPayload(draft);
    }

    function operadorPdvAtual() {
        var st = State.getState();
        if (st && st.pagamento && st.pagamento.operadorPdv) {
            return String(st.pagamento.operadorPdv).trim();
        }
        try {
            return (localStorage.getItem('gm_sspin_operador') || '').trim();
        } catch (e0) {
            return '';
        }
    }

    function injetarOperadorNoPayload(payload) {
        var op = operadorPdvAtual();
        if (op) {
            payload.operador_pdv = op;
            payload.operador = op;
        }
        return payload;
    }

    function sincronizarOperadorPdvNoState() {
        var op = '';
        try {
            op = (localStorage.getItem('gm_sspin_operador') || '').trim();
        } catch (e0) {
            op = '';
        }
        if (op) State.setPagamentoField('operadorPdv', op);
    }

    function buildErpPayload(state, computed) {
        var cliente = state.cliente || {};
        var payload = {
            cliente: currentClientName(state),
            itens: payloadItens(state),
            forma_pagamento: formaPagamentoParaErp(state, computed || State.getComputed())
        };
        var pag = pagamentosDetalheParaErp(state);
        if (pag && pag.length) payload.pagamentos = pag;
        if (cliente.cliente_agro_pk != null) {
            payload.cliente_agro_pk = cliente.cliente_agro_pk;
        }
        if (cliente.id && !/^erp-doc:/i.test(cliente.id)) {
            payload.cliente_id = cliente.id;
        }
        var cpfCli = clienteCpfEffective(cliente);
        if (cpfCli) payload.cliente_documento = cpfCli;
        else if (cliente.documento && cliente.documento !== '—') {
            payload.cliente_documento = cliente.documento;
        }
        var nfceOpts = (state.pagamento && state.pagamento.nfceOpts) || {};
        if (nfceOpts.cpf) {
            payload.nfce_cpf = nfceOpts.cpf;
            payload.cliente_documento = nfceOpts.cpf;
        }
        if (nfceOpts.semIdentificacao) payload.nfce_sem_identificacao = true;
        if (nfceUsuarioQuerEmitir(state)) payload.nfce_emitir = true;
        if (nfceDeveSerSincrona(state)) {
            payload.nfce_sincrona = true;
            if (nfceOpts.semIdentificacao || nfceOpts.cpf) payload.nfce_escolha_explicita = true;
        }
        var comp = computed || State.getComputed();
        if (comp && comp.desconto > 0.009) {
            payload.desconto_geral = comp.desconto;
        }
        if (comp && comp.frete > 0.009) {
            payload.frete = comp.frete;
        }
        var idem = String((state.pagamento && state.pagamento.clientRequestId) || '').trim();
        if (idem) payload.client_request_id = idem;
        var cx = bootstrap.caixa || {};
        if (cx.id != null && String(cx.id).trim() !== '') {
            payload.sessao_caixa_id = parseInt(cx.id, 10) || cx.id;
        }
        if (state.entrega && state.entrega.pedidoEntregaPendenteId) {
            payload.pedido_entrega_pendente_id = state.entrega.pedidoEntregaPendenteId;
        }
        return injetarOperadorNoPayload(payload);
    }

    function buildEntregaPayload(state, computed, extras) {
        extras = extras || {};
        var cliente = state.cliente || {};
        var e = state.entrega || {};
        var extraPag = pagamentoResumoExtra(state, computed);
        var obsParts = [
            state.venda.observacao || '',
            state.entrega.observacao || '',
            state.pagamento.observacaoFinal || '',
            extraPag,
            state.entrega.maquininha ? 'Maquininha: ' + state.entrega.maquininha : '',
            extras.obsExtra || ''
        ].filter(Boolean);
        var observacoes = obsParts.join(' | ');
        var plus =
            extras.plus_code != null && String(extras.plus_code).trim()
                ? String(extras.plus_code).trim()
                : String(e.plusCode || cliente.plus_code || '').trim();
        var out = {
            cliente_nome: currentClientName(state),
            telefone: cliente.telefone || '',
            endereco_linha: composeEndereco(state),
            plus_code: plus,
            referencia_rural: state.entrega.referencia || cliente.referencia_rural || '',
            maps_url_manual: cliente.maps_url_manual || '',
            itens: payloadItens(state),
            total_texto: formatMoney(computed.total),
            retomar_codigo: extras.retomar_codigo != null ? String(extras.retomar_codigo) : '',
            operador: operadorPdvAtual(),
            hora_prevista: state.entrega.horario || '',
            forma_pagamento: formaPagamentoResumoUi(state, computed),
            troco_precisa: (function () {
                var arr = state.pagamento.lancamentos || [];
                var any = arr.some(function (L) {
                    return L.forma === 'Dinheiro' && String(L.trocoCalculado || '').trim();
                });
                return any || !!String(state.entrega.troco || '').trim();
            })(),
            observacoes: observacoes
        };
        if (extras.orc_local_id != null && String(extras.orc_local_id).trim() !== '') {
            out.orc_local_id = parseInt(extras.orc_local_id, 10);
        }
        if (extras.venda_id != null && String(extras.venda_id).trim() !== '') {
            out.venda_id = parseInt(extras.venda_id, 10);
        }
        return out;
    }

    function buildCupomPayloadFromWizard(state, computed, extras) {
        extras = extras || {};
        var agora = new Date();
        var dt =
            extras.criado_em ||
            agora.toLocaleString('pt-BR', {
                day: '2-digit',
                month: '2-digit',
                year: 'numeric',
                hour: '2-digit',
                minute: '2-digit'
            });
        var itens = (state.itens || []).map(function (item) {
            return {
                nome: item.nome,
                qtd: State.toNumber(item.qtd),
                preco: State.toNumber(item.preco),
                subtotal: lineSubtotal(item)
            };
        });
        var formaTxt = formaPagamentoResumoUi(state, computed);
        var fiadoDias = parseInt(state.pagamento.fiadoDiasVencimento, 10) || 30;
        var ehFiado =
            /fiado/i.test(formaTxt || '') ||
            ((state.pagamento.lancamentos || []).some(function (L) {
                return String(L.forma || '').toLowerCase() === 'fiado';
            }));
        var vencDt = new Date(agora.getTime());
        vencDt.setDate(vencDt.getDate() + fiadoDias);
        var vencStr = vencDt.toLocaleDateString('pt-BR', {
            day: '2-digit',
            month: '2-digit',
            year: 'numeric'
        });
        return {
            venda_id: extras.venda_id || null,
            criado_em: dt,
            segunda_via: !!extras.segunda_via,
            cliente_nome: currentClientName(state),
            forma_pagamento: formaTxt,
            total: computed.total,
            total_texto: formatMoney(computed.total),
            operador: operadorPdvAtual(),
            caixa_id: (bootstrap.caixa && bootstrap.caixa.id) || null,
            devolvida: false,
            eh_fiado: ehFiado,
            fiado_dias: fiadoDias,
            vencimento: ehFiado ? vencStr : '',
            itens: itens
        };
    }

    function buildSaleReceiptHtml(state, computed) {
        var payload = buildCupomPayloadFromWizard(state, computed, { segunda_via: false });
        if (typeof window.agroBuildCupomVenda80mmHtml === 'function') {
            return window.agroBuildCupomVenda80mmHtml(payload);
        }
        return (
            '<!DOCTYPE html><html><head><meta charset="utf-8"><title>Cupom</title></head><body><p>Recarregue a página (F5) — módulo de cupom não carregou.</p></body></html>'
        );
    }

    function pdvReservarJanelaCupomFallback(withPrint) {
        if (!withPrint) return null;
        if (typeof window.agroImprimirCupomVenda80mm === 'function') return null;
        try {
            return window.open('about:blank', 'pdv_cupom_venda', 'width=480,height=720,scrollbars=yes');
        } catch (eWin) {
            return null;
        }
    }

    function printSaleReceiptWindow(win, state, computed, extras) {
        extras = extras || {};
        var payload = buildCupomPayloadFromWizard(state, computed, extras);
        if (typeof window.agroImprimirCupomVenda80mm === 'function') {
            window.agroImprimirCupomVenda80mm(payload);
            if (win && !win.closed) {
                try {
                    win.close();
                } catch (errC) {}
            }
            return true;
        }
        if (!win || win.closed) return false;
        try {
            win.document.open();
            win.document.write(buildSaleReceiptHtml(state, computed));
            win.document.close();
            win.focus();
            setTimeout(function () {
                try {
                    win.print();
                } catch (errP) {}
            }, 200);
            return true;
        } catch (errW) {
            return false;
        }
    }

    function setConfirmButtonsBusy(busy) {
        var n = dom.confirmSaleNoPrint;
        var p = dom.confirmSalePrint;
        if (n) {
            n.disabled = !!busy;
            n.textContent = busy ? 'Confirmando…' : '';
            n.classList.toggle('opacity-50', !!busy);
            n.classList.toggle('cursor-not-allowed', !!busy);
        }
        if (p) {
            p.disabled = !!busy;
            p.textContent = busy ? 'Confirmando…' : '';
            p.classList.toggle('opacity-50', !!busy);
            p.classList.toggle('cursor-not-allowed', !!busy);
        }
        if (!busy) {
            if (n) {
                n.innerHTML =
                    'Confirmar sem impressão <kbd class="ml-1 rounded border border-emerald-300 bg-emerald-50 px-1.5 py-0.5 font-mono text-[10px]">Enter</kbd>';
            }
            if (p) {
                p.innerHTML =
                    'Confirmar com impressão <kbd class="ml-1 rounded bg-emerald-500 px-1.5 py-0.5 font-mono text-[10px] text-white">F9</kbd>';
            }
            State.setPagamentoField('observacaoFinal', State.getState().pagamento.observacaoFinal || '');
        }
    }

    function showSaleDoneFeedback(msg, tone, opts) {
        tone = tone || 'success';
        opts = opts || {};
        var host = document.getElementById('pdv-sale-toast');
        if (!host) {
            host = document.createElement('div');
            host.id = 'pdv-sale-toast';
            host.setAttribute('role', 'status');
            host.setAttribute('aria-live', 'polite');
            document.body.appendChild(host);
        }
        var prominent = !!opts.prominent;
        var persistent = !!opts.persistent || opts.durationMs === 0;
        var placementTop = !!opts.placementTop && !prominent;
        host.className =
            'pointer-events-auto fixed z-[9999] transition-all duration-300 ease-out opacity-0 ' +
            (prominent
                ? 'pdv-sale-toast--prominent'
                : 'w-[min(26rem,calc(100vw-2rem))] translate-y-3 ' +
                  (placementTop ? 'top-4 left-1/2 -translate-x-1/2' : 'bottom-4 right-4 translate-x-0'));
        var palette =
            tone === 'error'
                ? 'border-rose-500 bg-rose-50 text-rose-950 shadow-rose-300/60'
                : tone === 'warn'
                ? 'border-amber-500 bg-amber-50 text-amber-950 shadow-amber-400/70'
                : tone === 'info'
                  ? 'border-sky-400 bg-sky-50 text-sky-950 shadow-sky-200/50'
                  : 'border-emerald-400 bg-emerald-50 text-emerald-950 shadow-emerald-200/50';
        var icon =
            tone === 'error' ? '✕' : tone === 'warn' ? '⚠' : tone === 'info' ? 'ℹ' : '✓';
        var titleHtml = opts.title
            ? '<p class="pdv-sale-toast-title text-base font-black leading-tight">' + escapeHtml(opts.title) + '</p>'
            : '';
        var bodyClass =
            'pdv-sale-toast-body ' +
            (opts.title ? 'mt-1 text-sm font-semibold leading-snug' : 'text-sm font-bold leading-snug pt-1');
        if (opts.keepNewlines) bodyClass += ' whitespace-pre-line';
        var bodyHtml =
            '<p class="' + bodyClass + '">' + escapeHtml(msg || 'Venda confirmada.') + '</p>';
        var dismissBtn =
            tone === 'warn' || tone === 'error' || tone === 'info' || persistent
                ? '<button type="button" class="mt-3 rounded-xl border-2 border-current/25 bg-white px-4 py-2 text-xs font-black uppercase tracking-wide hover:bg-white/80" data-pdv-toast-dismiss>Entendi</button>'
                : '';
        host.innerHTML =
            '<div class="pdv-sale-toast-panel rounded-2xl border-2 px-4 py-3 shadow-2xl ' +
            palette +
            '">' +
            '<div class="flex items-start gap-3">' +
            '<span class="pdv-sale-toast-icon mt-0.5 flex h-9 w-9 shrink-0 items-center justify-center rounded-full bg-white/90 text-lg font-black" aria-hidden="true">' +
            icon +
            '</span>' +
            '<div class="min-w-0 flex-1">' +
            titleHtml +
            bodyHtml +
            dismissBtn +
            '</div></div></div>';
        var dismissEl = host.querySelector('[data-pdv-toast-dismiss]');
        if (dismissEl) {
            dismissEl.addEventListener('click', function () {
                hideSaleDoneToast();
            });
        }
        if (prominent) {
            host.addEventListener(
                'click',
                function onBackdrop(ev) {
                    if (ev.target === host) hideSaleDoneToast();
                },
                { once: true }
            );
        }
        host.classList.remove('opacity-0', 'translate-y-3', 'pointer-events-none');
        host.classList.add('opacity-100', 'translate-y-0');
        if (showSaleDoneFeedback._timer) clearTimeout(showSaleDoneFeedback._timer);
        if (!persistent) {
            var ms = opts.durationMs || (tone === 'warn' || tone === 'error' ? 14000 : 5200);
            showSaleDoneFeedback._timer = setTimeout(function () {
                hideSaleDoneToast();
            }, ms);
        }
    }

    function hideSaleDoneToast() {
        var host = document.getElementById('pdv-sale-toast');
        if (!host) return;
        host.classList.add('opacity-0', 'translate-y-3', 'pointer-events-none');
        host.classList.remove('opacity-100', 'translate-y-0');
    }

    function nfceAtivoNoPdv() {
        return !!(bootstrap.nfce && bootstrap.nfce.ativo);
    }

    function nfceFormasAutoLista() {
        if (bootstrap.nfce && Array.isArray(bootstrap.nfce.formasAuto) && bootstrap.nfce.formasAuto.length) {
            return bootstrap.nfce.formasAuto;
        }
        return ['PIX', 'Cartão de débito', 'Cartão de crédito', 'Cartão de crédito parcelado'];
    }

    function nfceFormasPagamentoVenda(state) {
        state = state || State.getState();
        var arr = (state.pagamento && state.pagamento.lancamentos) || [];
        return arr
            .map(function (L) {
                return String((L && L.forma) || '').trim();
            })
            .filter(Boolean);
    }

    function nfceVendaTemFormaAuto(state) {
        var auto = nfceFormasAutoLista();
        var formas = nfceFormasPagamentoVenda(state);
        if (!formas.length) return false;
        return formas.some(function (f) {
            return auto.indexOf(f) >= 0;
        });
    }

    function nfceModoGlobalAuto() {
        return !!(bootstrap.nfce && bootstrap.nfce.modo === 'auto');
    }

    function nfceUsuarioQuerEmitir(state) {
        if (!nfceAtivoNoPdv()) return false;
        if (nfceModoGlobalAuto()) return true;
        state = state || State.getState();
        if (state.pagamento && state.pagamento.nfceEmitir === true) return true;
        if (state.pagamento && state.pagamento.nfceEmitir === false) return false;
        return nfceVendaTemFormaAuto(state);
    }

    function prepararNfceSemImpressao() {
        State.setPagamentoField('cupomImpressao', '');
        if (nfceModoGlobalAuto() || nfceVendaTemFormaAuto()) {
            State.setPagamentoField('nfceEmitir', true);
        } else {
            State.setPagamentoField('nfceEmitir', false);
            State.setPagamentoField('nfceOpts', {});
        }
    }

    function prepararNfceComImpressao(escolha) {
        escolha = escolha === 'nfce' ? 'nfce' : 'venda';
        State.setPagamentoField('cupomImpressao', escolha);
        if (escolha === 'nfce') {
            State.setPagamentoField('nfceEmitir', true);
        } else {
            State.setPagamentoField('nfceEmitir', false);
            State.setPagamentoField('nfceOpts', {});
        }
    }

    function abrirModalEscolhaImpressao(callback) {
        var modal = document.getElementById('modal-pdv-escolha-impressao');
        var btnNfc = document.getElementById('pdv-escolha-impressao-nfce');
        var btnVenda = document.getElementById('pdv-escolha-impressao-venda');
        var btnCancel = document.getElementById('pdv-escolha-impressao-cancelar');
        if (!modal || !btnNfc || !btnVenda) {
            callback('venda');
            return;
        }
        function fechar() {
            modal.classList.add('hidden');
            modal.classList.remove('flex');
            btnNfc.removeEventListener('click', onNfc);
            btnVenda.removeEventListener('click', onVenda);
            if (btnCancel) btnCancel.removeEventListener('click', onCancel);
            document.removeEventListener('keydown', onKey);
        }
        function onNfc() {
            fechar();
            callback('nfce');
        }
        function onVenda() {
            fechar();
            callback('venda');
        }
        function onCancel() {
            fechar();
            callback(null);
        }
        function onKey(ev) {
            if (ev.key === 'Escape') {
                ev.preventDefault();
                onCancel();
            }
        }
        modal.classList.remove('hidden');
        modal.classList.add('flex');
        btnNfc.addEventListener('click', onNfc);
        btnVenda.addEventListener('click', onVenda);
        if (btnCancel) btnCancel.addEventListener('click', onCancel);
        document.addEventListener('keydown', onKey);
    }

    function pdvFormatCpfInput(raw) {
        var d = nfceNormalizarCpf(raw);
        if (d.length <= 3) return d;
        if (d.length <= 6) return d.slice(0, 3) + '.' + d.slice(3);
        if (d.length <= 9) return d.slice(0, 3) + '.' + d.slice(3, 6) + '.' + d.slice(6);
        return d.slice(0, 3) + '.' + d.slice(3, 6) + '.' + d.slice(6, 9) + '-' + d.slice(9);
    }

    function clienteCpfEffective(c) {
        if (!c) return '';
        var cpf = nfceNormalizarCpf(c.cpf);
        if (nfceCpfValido(cpf)) return cpf;
        var doc = nfceNormalizarCpf(c.documento);
        if (nfceCpfValido(doc)) return doc;
        return '';
    }

    function clienteCpfParaExibir(c) {
        var eff = clienteCpfEffective(c);
        return eff ? pdvFormatCpfInput(eff) : '';
    }

    function pdvValidarCpfOpcional(raw) {
        var norm = nfceNormalizarCpf(raw);
        if (!norm) return { ok: true, cpf: '' };
        if (nfceCpfValido(norm)) return { ok: true, cpf: norm };
        return { ok: false, msg: 'CPF inválido.' };
    }

    function nfceNormalizarCpf(raw) {
        return String(raw || '').replace(/\D/g, '').slice(0, 11);
    }

    function nfceCpfValido(cpf) {
        cpf = nfceNormalizarCpf(cpf);
        if (cpf.length !== 11 || /^(\d)\1+$/.test(cpf)) return false;
        var s = 0;
        var i;
        for (i = 0; i < 9; i++) s += parseInt(cpf.charAt(i), 10) * (10 - i);
        var d1 = s % 11 < 2 ? 0 : 11 - (s % 11);
        if (parseInt(cpf.charAt(9), 10) !== d1) return false;
        s = 0;
        for (i = 0; i < 10; i++) s += parseInt(cpf.charAt(i), 10) * (11 - i);
        var d2 = s % 11 < 2 ? 0 : 11 - (s % 11);
        return parseInt(cpf.charAt(10), 10) === d2;
    }

    function nfceErroDaResposta(data) {
        if (data && data.nfce && data.nfce.ok === false) {
            if (data.nfce.pendente_retry) return '';
            var e = data.nfce.erro || 'Falha na NFC-e';
            if (data.nfce.c_stat) e = '[' + data.nfce.c_stat + '] ' + e;
            return e;
        }
        return '';
    }

    function abrirModalNfceCpf(callback) {
        var modal = document.getElementById('modal-pdv-nfce-cpf');
        var input = document.getElementById('pdv-nfce-cpf-input');
        var errEl = document.getElementById('pdv-nfce-cpf-erro');
        var btnOk = document.getElementById('pdv-nfce-cpf-confirmar');
        var btnCancel = document.getElementById('pdv-nfce-cpf-cancelar');
        var btnSemId = document.getElementById('pdv-nfce-sem-id-rapido');
        if (!modal || !input || !btnOk || !btnCancel) {
            callback(null);
            return;
        }
        input.value = '';
        if (errEl) {
            errEl.textContent = '';
            errEl.classList.add('hidden');
        }
        modal.classList.remove('hidden');
        modal.classList.add('flex');
        setTimeout(function () {
            if (btnSemId) btnSemId.focus();
        }, 40);

        function fechar(res) {
            modal.classList.add('hidden');
            modal.classList.remove('flex');
            btnOk.removeEventListener('click', onOk);
            btnCancel.removeEventListener('click', onCancel);
            if (btnSemId) btnSemId.removeEventListener('click', onSemId);
            document.removeEventListener('keydown', onKey);
            callback(res);
        }

        function onCancel() {
            fechar(null);
        }

        function onSemId() {
            fechar({ cpf: '', semIdentificacao: true });
        }

        function onOk() {
            var cpf = nfceNormalizarCpf(input.value);
            if (!cpf) {
                if (errEl) {
                    errEl.textContent = 'Digite o CPF ou toque em «Sem CPF na nota».';
                    errEl.classList.remove('hidden');
                }
                input.focus();
                return;
            }
            if (!nfceCpfValido(cpf)) {
                if (errEl) {
                    errEl.textContent = 'CPF inválido. Corrija ou use «Sem CPF na nota».';
                    errEl.classList.remove('hidden');
                }
                input.focus();
                return;
            }
            fechar({ cpf: cpf, semIdentificacao: false });
        }

        function onKey(ev) {
            if (ev.key === 'Escape') {
                ev.preventDefault();
                onCancel();
            }
        }

        btnOk.addEventListener('click', onOk);
        btnCancel.addEventListener('click', onCancel);
        if (btnSemId) btnSemId.addEventListener('click', onSemId);
        document.addEventListener('keydown', onKey);
    }

    function nfceFluxoAutomatico(state) {
        return nfceModoGlobalAuto() || nfceVendaTemFormaAuto(state);
    }

    function nfceVendaComImpressaoFiscal(state) {
        state = state || State.getState();
        var pag = state.pagamento || {};
        return !!pag.imprimirCupom && String(pag.cupomImpressao || '') === 'nfce';
    }

    function nfceDeveSerSincrona(state) {
        return nfceUsuarioQuerEmitir(state) && nfceVendaComImpressaoFiscal(state);
    }

    function resolverNfceAntesConfirmar(withPrint) {
        if (!nfceUsuarioQuerEmitir()) {
            State.setPagamentoField('nfceOpts', {});
            confirmSaleProsseguir(withPrint);
            return;
        }
        var state = State.getState();
        var cpfCad = clienteCpfEffective(state.cliente);
        if (nfceCpfValido(cpfCad)) {
            State.setPagamentoField('nfceOpts', { cpf: cpfCad, semIdentificacao: false });
            confirmSaleProsseguir(withPrint);
            return;
        }
        if (!withPrint && nfceFluxoAutomatico(state)) {
            State.setPagamentoField('nfceOpts', { cpf: '', semIdentificacao: true });
            confirmSaleProsseguir(withPrint);
            return;
        }
        abrirModalNfceCpf(function (opts) {
            if (!opts) return;
            State.setPagamentoField('nfceOpts', opts);
            if (opts.cpf) {
                var stCl = State.getState();
                State.setCliente(
                    Object.assign({}, stCl.cliente || {}, { cpf: opts.cpf, documento: opts.cpf }),
                    stCl.clienteMode
                );
            }
            confirmSaleProsseguir(withPrint);
        });
    }

    function saleDoneMessage(opts) {
        opts = opts || {};
        if (opts.fiadoAguardaErp) {
            return (
                'Venda fiado registrada (#' +
                (opts.vendaId || '—') +
                '). Envie ao ERP em Vendas → Fiado pendente ERP.'
            );
        }
        if (opts.erpPendente) {
            return opts.entregaOk
                ? 'Venda e entrega registradas — ERP em segundo plano.'
                : 'Venda registrada — ERP em segundo plano.';
        }
        if (opts.entregaOk) return 'Venda confirmada e entrega registrada com sucesso.';
        return 'Venda confirmada com sucesso.';
    }

    function nfceSucessoDaResposta(data) {
        if (data && data.nfce && data.nfce.ok === true) {
            var n = data.nfce.numero != null ? String(data.nfce.numero) : '';
            var s = data.nfce.serie != null ? String(data.nfce.serie) : '';
            if (n) return 'NFC-e nº ' + n + (s ? ' · série ' + s : '') + ' autorizada.';
            return 'NFC-e autorizada.';
        }
        return '';
    }

    function nfceAvisoPosVenda(opts) {
        if (!opts || !opts.nfceErro) return '';
        return opts.nfceErro;
    }

    function mostrarAvisoNfcePendente(opts, prefixo) {
        if (!opts || !opts.nfceErro) return;
        var detalhe = String(opts.nfceErro || '').trim();
        var corpo =
            (prefixo ? prefixo + ' ' : '') +
            'A venda foi salva, mas o cupom fiscal (NFC-e) não saiu.' +
            (detalhe ? ' Motivo: ' + detalhe + '.' : '') +
            ' Depois vá em Consultar vendas e use Reemitir NFC-e.';
        showSaleDoneFeedback(corpo, 'warn', {
            placementTop: true,
            title: 'Cupom fiscal pendente',
            durationMs: 16000
        });
    }

    function aguardarPosImpressao(ms) {
        ms = ms || 0;
        if (!ms) return Promise.resolve();
        return new Promise(function (resolve) {
            setTimeout(resolve, ms);
        });
    }

    function imprimirCupomAposVenda(withPrint, printWin, vendaId, cupomImpressao) {
        var stP = State.getState();
        var compP = State.getComputed();
        if (!withPrint) return Promise.resolve(false);
        var interno = cupomImpressao === 'venda';
        if (vendaId && typeof agroCarregarEImprimirCupomVenda === 'function') {
            return agroCarregarEImprimirCupomVenda(vendaId, { segunda_via: false, interno: interno })
                .then(function () {
                    if (printWin && !printWin.closed) {
                        try {
                            printWin.close();
                        } catch (errC) {}
                    }
                    return false;
                })
                .catch(function () {
                    return !printSaleReceiptWindow(printWin, stP, compP, {
                        venda_id: vendaId,
                        segunda_via: false
                    });
                });
        }
        return Promise.resolve(
            !printSaleReceiptWindow(printWin, stP, compP, {
                venda_id: vendaId || null,
                segunda_via: false
            })
        );
    }

    function finalizeConfirmedSale(withPrint, printWin, opts) {
        opts = opts || {};
        var cupomImpressao = opts.cupomImpressao || '';
        var imprimir = !!withPrint;
        if (opts.nfceErro && cupomImpressao === 'nfce') {
            imprimir = false;
            if (printWin && !printWin.closed) {
                try {
                    printWin.close();
                } catch (errC) {}
            }
        }
        imprimirCupomAposVenda(imprimir, printWin, opts.vendaId, cupomImpressao).then(function (printFail) {
            return aguardarPosImpressao(imprimir ? 900 : 0).then(function () {
                return printFail;
            });
        }).then(function (printFail) {
            jsonPost(urls.apiPdvLimparCheckoutDraft, {}).catch(function () {});
            resetWizardParaNovaVenda();
            refreshEntregasPendentesUi(true);
            if (opts.nfceErro) {
                mostrarAvisoNfcePendente(opts, saleDoneMessage(opts));
            } else if (printFail) {
                showSaleDoneFeedback(
                    'Venda registrada — falha na impressão. Reimprima pela lista de vendas, se precisar.',
                    'warn',
                    { placementTop: true }
                );
            } else if (opts.nfceOk) {
                showSaleDoneFeedback(saleDoneMessage(opts) + ' ' + opts.nfceOk, 'success');
            } else {
                var msg = saleDoneMessage(opts);
                var kind = opts.erpPendente ? 'info' : 'success';
                showSaleDoneFeedback(msg, kind);
            }
        });
    }

    function confirmSale(withPrint) {
        if (isProcessingSale) return;
        var state = State.getState();
        var computed = State.getComputed();
        var validation = canAdvance(Object.assign({}, state, { currentStep: 'pagamento' }), computed);
        if (validation) {
            alert(validation);
            return;
        }
        ensureCaixaAbertoParaVenda().then(function (caixaOk) {
            if (!caixaOk) return;
            if (withPrint && nfceAtivoNoPdv()) {
                if (nfceModoGlobalAuto() || nfceVendaTemFormaAuto(state)) {
                    prepararNfceComImpressao('nfce');
                    resolverNfceAntesConfirmar(true);
                    return;
                }
                abrirModalEscolhaImpressao(function (escolha) {
                    if (!escolha) return;
                    prepararNfceComImpressao(escolha);
                    resolverNfceAntesConfirmar(true);
                });
                return;
            }
            if (withPrint) {
                prepararNfceComImpressao('venda');
            } else {
                prepararNfceSemImpressao();
            }
            resolverNfceAntesConfirmar(!!withPrint);
        });
    }

    function confirmSaleProsseguir(withPrint) {
        if (isProcessingSale) return;
        var state = State.getState();
        var computed = State.getComputed();
        (function setSaleClientRequestId() {
            var uuid =
                typeof crypto !== 'undefined' && crypto.randomUUID
                    ? crypto.randomUUID()
                    : 'req-' +
                      Date.now().toString(36) +
                      '-' +
                      Math.random().toString(36).slice(2, 11);
            State.setPagamentoField('clientRequestId', uuid);
        })();
        if (vendaPrecisaFinalizarMpPoint(state)) {
            confirmSaleFinalizarMpPointOrders(!!withPrint);
            return;
        }
        if (deveUsarMpPointNoFechar(state, computed)) {
            confirmSaleMercadoPagoPoint(!!withPrint);
            return;
        }
        isProcessingSale = true;
        setConfirmButtonsBusy(true);
        var printWin = pdvReservarJanelaCupomFallback(withPrint);
        if (withPrint && !printWin && typeof window.agroImprimirCupomVenda80mm !== 'function') {
            showPdvAviso(
                'Não foi possível abrir a janela do cupom. Permita pop-ups para este site e use de novo “Confirmar com impressão”.',
                { title: 'Impressão' }
            );
        }
        State.setPagamentoField('imprimirCupom', !!withPrint);
        state = State.getState();
        var cupomImpressao = withPrint
            ? String((state.pagamento && state.pagamento.cupomImpressao) || 'venda')
            : '';
        if (window.gmLoadingBar) window.gmLoadingBar.show();

        jsonPost(urls.apiPdvSalvarCheckoutDraft, buildCheckoutDraftPayload(state, computed))
            .then(function (draftRes) {
                if (!draftRes.ok || !draftRes.data.ok) throw new Error((draftRes.data && (draftRes.data.erro || draftRes.data.mensagem)) || 'Falha ao salvar rascunho.');
                return jsonPost(urls.apiEnviarPedidoErp, buildErpPayload(state, computed));
            })
            .then(function (erpRes) {
                if (!erpRes.ok || !erpRes.data.ok) {
                    throw new Error(
                        (erpRes.data && (erpRes.data.erro || erpRes.data.mensagem)) || 'Falha ao confirmar venda.'
                    );
                }
                if (typeof window.agroPdvAplicarPatchesRespostaVenda === 'function') {
                    window.agroPdvAplicarPatchesRespostaVenda(erpRes.data);
                } else {
                    agroPdvEnqueuePatchesRespostaVenda(erpRes.data);
                }
                var erpPendente = !!erpRes.data.erp_pendente;
                var fiadoAguardaErp = !!erpRes.data.fiado_aguarda_erp;
                var vendaId = erpRes.data && erpRes.data.venda_id;
                var nfceErro = nfceErroDaResposta(erpRes.data);
                var nfceOk = nfceSucessoDaResposta(erpRes.data);
                if (fiadoAguardaErp) {
                    finalizeConfirmedSale(withPrint, printWin, {
                        fiadoAguardaErp: true,
                        vendaId: vendaId,
                        nfceErro: nfceErro,
                        nfceOk: nfceOk,
                        cupomImpressao: cupomImpressao
                    });
                    return;
                }
                var pendenteId =
                    state.entrega && state.entrega.pedidoEntregaPendenteId
                        ? state.entrega.pedidoEntregaPendenteId
                        : null;
                if (pendenteId) {
                    return finalizarEntregaPendenteAposVenda(pendenteId, vendaId).then(function (finRes) {
                        if (!finRes.ok || !finRes.data || !finRes.data.ok) {
                            throw new Error(
                                (finRes.data && (finRes.data.erro || finRes.data.mensagem)) ||
                                    'Venda salva, mas falhou ao encerrar pendência da entrega.'
                            );
                        }
                        finalizeConfirmedSale(withPrint, printWin, {
                            erpPendente: erpPendente,
                            entregaOk: true,
                            vendaId: vendaId,
                            nfceErro: nfceErro,
                            nfceOk: nfceOk,
                            cupomImpressao: cupomImpressao
                        });
                    });
                }
                if (state.entrega.ativa) {
                    var entPayload = buildEntregaPayload(state, computed, { venda_id: vendaId });
                    if (erpPendente) {
                        jsonPost(urls.apiEntregaRegistrar, entPayload).catch(function () {});
                        finalizeConfirmedSale(withPrint, printWin, {
                            erpPendente: true,
                            entregaOk: true,
                            vendaId: vendaId,
                            nfceErro: nfceErro,
                            nfceOk: nfceOk,
                            cupomImpressao: cupomImpressao
                        });
                        return;
                    }
                    return jsonPost(urls.apiEntregaRegistrar, entPayload).then(function (entRes) {
                        if (!entRes.ok || !entRes.data.ok) {
                            throw new Error(
                                (entRes.data && (entRes.data.erro || entRes.data.mensagem)) ||
                                    'Venda salva, mas falhou ao registrar entrega.'
                            );
                        }
                        finalizeConfirmedSale(withPrint, printWin, {
                            entregaOk: true,
                            vendaId: vendaId,
                            nfceErro: nfceErro,
                            nfceOk: nfceOk,
                            cupomImpressao: cupomImpressao
                        });
                    });
                }
                finalizeConfirmedSale(withPrint, printWin, {
                    erpPendente: erpPendente,
                    vendaId: vendaId,
                    nfceErro: nfceErro,
                    nfceOk: nfceOk,
                    cupomImpressao: cupomImpressao
                });
            })
            .catch(function (err) {
                if (printWin && !printWin.closed) {
                    try {
                        printWin.close();
                    } catch (errC) {}
                }
                showPdvAviso(err && err.message ? err.message : 'Falha ao confirmar venda.', { tone: 'error' });
            })
            .finally(function () {
                if (window.gmLoadingBar) window.gmLoadingBar.hide();
                isProcessingSale = false;
                setConfirmButtonsBusy(false);
            });
    }

    function confirmSaleFinalizarMpPointOrders(withPrint) {
        withPrint = !!withPrint;
        if (isProcessingSale) return;
        if (!pagamentoUi.mpPointEnabled || !String(urls.apiPdvMpPointFinalizar || '').trim()) {
            showMpPointAviso('Mercado Pago Point não está configurado no servidor.', { tone: 'error' });
            return;
        }
        var state = State.getState();
        var computed = State.getComputed();
        var orderIds = mpPointOrderIdsFromLancamentos(state);
        if (!orderIds.length) {
            showMpPointAviso('Pagamento MP não encontrado nos lançamentos.', { tone: 'error' });
            isProcessingSale = false;
            setConfirmButtonsBusy(false);
            return;
        }
        isProcessingSale = true;
        setConfirmButtonsBusy(true);
        var printWin = pdvReservarJanelaCupomFallback(withPrint);
        if (withPrint && !printWin && typeof window.agroImprimirCupomVenda80mm !== 'function') {
            showPdvAviso(
                'Não foi possível abrir a janela do cupom. Permita pop-ups ou use “Confirmar sem impressão”.',
                { title: 'Impressão' }
            );
        }
        State.setPagamentoField('imprimirCupom', withPrint);
        state = State.getState();
        if (window.gmLoadingBar) window.gmLoadingBar.show();

        var erpPayload = buildErpPayload(state, computed);
        var primaryOrderId = orderIds[0];

        jsonPost(urls.apiPdvSalvarCheckoutDraft, buildCheckoutDraftPayload(state, computed))
            .then(function (draftRes) {
                if (!draftRes.ok || !draftRes.data.ok) {
                    throw new Error(
                        (draftRes.data && (draftRes.data.erro || draftRes.data.mensagem)) || 'Falha ao salvar rascunho.'
                    );
                }
                return jsonPost(urls.apiPdvMpPointFinalizar, {
                    order_id: primaryOrderId,
                    erp_payload: erpPayload
                });
            })
            .then(function (finRes) {
                if (!finRes.ok || !finRes.data.ok) {
                    throw new Error(
                        (finRes.data && (finRes.data.erro || finRes.data.mensagem)) ||
                            'Falha ao registrar venda após pagamento na maquininha.'
                    );
                }
                var mpPointFormaDivergiu =
                    !!(finRes.data && finRes.data.mp_point_forma_divergencia && finRes.data.mp_point_aviso);
                if (mpPointFormaDivergiu) {
                    showMpPointAviso(finRes.data.mp_point_aviso);
                }
                var st = State.getState();
                var comp = State.getComputed();
                var pendenteId =
                    st.entrega && st.entrega.pedidoEntregaPendenteId
                        ? st.entrega.pedidoEntregaPendenteId
                        : null;
                var vendaId = finRes.data && finRes.data.venda_id;
                if (pendenteId) {
                    return finalizarEntregaPendenteAposVenda(pendenteId, vendaId).then(function (finEntRes) {
                        if (!finEntRes.ok || !finEntRes.data || !finEntRes.data.ok) {
                            throw new Error(
                                (finEntRes.data && (finEntRes.data.erro || finEntRes.data.mensagem)) ||
                                    'Venda salva, mas falhou ao encerrar pendência da entrega.'
                            );
                        }
                        return {
                            entrega: finEntRes.data,
                            erp: finRes.data,
                            mpPointFormaDivergiu: mpPointFormaDivergiu
                        };
                    });
                }
                if (!st.entrega.ativa) {
                    return { entrega: null, erp: finRes.data, mpPointFormaDivergiu: mpPointFormaDivergiu };
                }
                return jsonPost(
                    urls.apiEntregaRegistrar,
                    buildEntregaPayload(st, comp, { venda_id: vendaId })
                ).then(function (entRes) {
                    if (!entRes.ok || !entRes.data.ok) {
                        throw new Error(
                            (entRes.data && (entRes.data.erro || entRes.data.mensagem)) ||
                                'Venda salva, mas falhou ao registrar entrega.'
                        );
                    }
                    return {
                        entrega: entRes.data,
                        erp: finRes.data,
                        mpPointFormaDivergiu: mpPointFormaDivergiu
                    };
                });
            })
            .then(function (result) {
                var erpData = (result && result.erp) || {};
                var mpPointFormaDivergiu = !!(result && result.mpPointFormaDivergiu);
                var nfceErro = nfceErroDaResposta(erpData);
                var nfceOk = nfceSucessoDaResposta(erpData);
                var vIdMp = erpData && erpData.venda_id != null ? erpData.venda_id : null;
                var stMp = State.getState();
                var cupomImpMp = withPrint
                    ? String((stMp.pagamento && stMp.pagamento.cupomImpressao) || 'venda')
                    : '';
                if (nfceErro && cupomImpMp === 'nfce') {
                    cupomImpMp = 'venda';
                }
                return imprimirCupomAposVenda(withPrint, printWin, vIdMp, cupomImpMp)
                    .then(function (printFail) {
                        return aguardarPosImpressao(withPrint ? 900 : 0).then(function () {
                            return printFail;
                        });
                    })
                    .then(function (printFail) {
                        jsonPost(urls.apiPdvLimparCheckoutDraft, {}).catch(function () {});
                        pdvMpPointBeep(mpPointFormaDivergiu ? 'err' : 'ok');
                        resetWizardParaNovaVenda();
                        if (nfceErro) {
                            mostrarAvisoNfcePendente({ nfceErro: nfceErro }, 'Venda confirmada.');
                            return;
                        }
                        if (printFail) {
                            showSaleDoneFeedback(
                                'Venda registrada — falha na impressão. Reimprima pela lista de vendas, se precisar.',
                                'warn',
                                { placementTop: true }
                            );
                        } else if (nfceOk) {
                            showSaleDoneFeedback(
                                (result.entrega ? 'Venda e entrega OK. ' : 'Venda confirmada. ') + nfceOk,
                                'success'
                            );
                        } else {
                            showSaleDoneFeedback(
                                result.entrega
                                    ? 'Venda registrada e entrega lançada.'
                                    : 'Venda registrada com sucesso.',
                                'success'
                            );
                        }
                    });
            })
            .catch(function (err) {
                if (printWin && !printWin.closed) {
                    try {
                        printWin.close();
                    } catch (errC) {}
                }
                pdvMpPointBeep('err');
                showMpPointAviso(
                    (err && err.message) || 'Falha ao confirmar venda com pagamento MP.',
                    { tone: 'error' }
                );
            })
            .finally(function () {
                if (window.gmLoadingBar) window.gmLoadingBar.hide();
                isProcessingSale = false;
                setConfirmButtonsBusy(false);
            });
    }

    function confirmSaleMercadoPagoPoint(withPrint) {
        withPrint = !!withPrint;
        if (isProcessingSale) return;
        if (!pagamentoUi.mpPointEnabled || !String(urls.apiPdvMpPointCriar || '').trim()) {
            showMpPointAviso('Mercado Pago Point não está configurado no servidor.', { tone: 'error' });
            return;
        }
        var state = State.getState();
        var computed = State.getComputed();
        var validation = canAdvance(Object.assign({}, state, { currentStep: 'pagamento' }), computed);
        if (validation) {
            showPdvAviso(validation);
            return;
        }
        if (!deveUsarMpPointNoFechar(state, computed)) {
            showMpPointAviso(
                'O envio automático ao Point só vale para Mercado Pago (cartão ou Pix) com pagamento único cobrindo o total.'
            );
            return;
        }
        ensureCaixaAbertoParaVenda().then(function (caixaOk) {
            if (!caixaOk) return;
            confirmSaleMercadoPagoPointProsseguir(withPrint);
        });
    }

    function confirmSaleMercadoPagoPointProsseguir(withPrint) {
        withPrint = !!withPrint;
        if (isProcessingSale) return;
        var state = State.getState();
        var computed = State.getComputed();
        isProcessingSale = true;
        setConfirmButtonsBusy(true);
        var printWin = pdvReservarJanelaCupomFallback(withPrint);
        if (withPrint && !printWin && typeof window.agroImprimirCupomVenda80mm !== 'function') {
            showPdvAviso(
                'Não foi possível abrir a janela do cupom. Permita pop-ups ou use “Confirmar sem impressão”.',
                { title: 'Impressão' }
            );
        }
        State.setPagamentoField('imprimirCupom', withPrint);
        state = State.getState();
        if (window.gmLoadingBar) window.gmLoadingBar.show();

        jsonPost(urls.apiPdvSalvarCheckoutDraft, buildCheckoutDraftPayload(state, computed))
            .then(function (draftRes) {
                if (!draftRes.ok || !draftRes.data.ok) {
                    throw new Error(
                        (draftRes.data && (draftRes.data.erro || draftRes.data.mensagem)) || 'Falha ao salvar rascunho.'
                    );
                }
                return jsonPost(urls.apiPdvMpPointCriar, buildErpPayload(state, computed));
            })
            .then(function (criarRes) {
                if (!criarRes.ok || !criarRes.data.ok) {
                    throw new Error((criarRes.data && criarRes.data.erro) || 'Falha ao enviar valor ao terminal MP.');
                }
                var oid = criarRes.data.order_id;
                if (!oid) throw new Error('Resposta sem order_id.');
                mpPointWaitControl.cancelRequested = false;
                mpPointWaitControl.orderId = oid;
                var stWait = State.getState();
                var compWait = State.getComputed();
                var arrWait = (stWait.pagamento && stWait.pagamento.lancamentos) || [];
                var formaWait = arrWait[0]
                    ? lancamentoFormaErpLabel(arrWait[0])
                    : String((stWait.pagamento && stWait.pagamento.forma) || '');
                showMpPointWaitBar(
                    criarRes.data.amount != null ? criarRes.data.amount : totalNumberFromComputed(compWait),
                    formaWait
                );
                return pollMpPointUntilPaid(oid);
            })
            .then(function (pack) {
                if (pack.jaFinalizado) {
                    return { ok: true, data: { ok: true, venda_id: pack.venda_id } };
                }
                return jsonPost(urls.apiPdvMpPointFinalizar, { order_id: pack.order_id });
            })
            .then(function (finRes) {
                if (!finRes.ok || !finRes.data.ok) {
                    throw new Error((finRes.data && (finRes.data.erro || finRes.data.mensagem)) || 'Falha ao registrar venda após o Point.');
                }
                var mpPointFormaDivergiu =
                    !!(finRes.data && finRes.data.mp_point_forma_divergencia && finRes.data.mp_point_aviso);
                if (mpPointFormaDivergiu) {
                    showMpPointAviso(finRes.data.mp_point_aviso);
                }
                var st = State.getState();
                var comp = State.getComputed();
                var pendenteId =
                    st.entrega && st.entrega.pedidoEntregaPendenteId
                        ? st.entrega.pedidoEntregaPendenteId
                        : null;
                var vendaId = finRes.data && finRes.data.venda_id;
                if (pendenteId) {
                    return finalizarEntregaPendenteAposVenda(pendenteId, vendaId).then(function (finEntRes) {
                        if (!finEntRes.ok || !finEntRes.data || !finEntRes.data.ok) {
                            throw new Error(
                                (finEntRes.data && (finEntRes.data.erro || finEntRes.data.mensagem)) ||
                                    'Venda salva, mas falhou ao encerrar pendência da entrega.'
                            );
                        }
                        return {
                            entrega: finEntRes.data,
                            erp: finRes.data,
                            mpPointFormaDivergiu: mpPointFormaDivergiu
                        };
                    });
                }
                if (!st.entrega.ativa) {
                    return { entrega: null, erp: finRes.data, mpPointFormaDivergiu: mpPointFormaDivergiu };
                }
                return jsonPost(
                    urls.apiEntregaRegistrar,
                    buildEntregaPayload(st, comp, { venda_id: vendaId })
                ).then(function (entRes) {
                    if (!entRes.ok || !entRes.data.ok) {
                        throw new Error(
                            (entRes.data && (entRes.data.erro || entRes.data.mensagem)) ||
                                'Venda salva, mas falhou ao registrar entrega.'
                        );
                    }
                    return {
                        entrega: entRes.data,
                        erp: finRes.data,
                        mpPointFormaDivergiu: mpPointFormaDivergiu
                    };
                });
            })
            .then(function (result) {
                var erpData = (result && result.erp) || {};
                var mpPointFormaDivergiu = !!(result && result.mpPointFormaDivergiu);
                var nfceErro = nfceErroDaResposta(erpData);
                var nfceOk = nfceSucessoDaResposta(erpData);
                var vIdMp =
                    erpData && erpData.venda_id != null ? erpData.venda_id : null;
                var stMp = State.getState();
                var cupomImpMp = withPrint
                    ? String((stMp.pagamento && stMp.pagamento.cupomImpressao) || 'venda')
                    : '';
                if (nfceErro && cupomImpMp === 'nfce') {
                    cupomImpMp = 'venda';
                }
                return imprimirCupomAposVenda(withPrint, printWin, vIdMp, cupomImpMp).then(function (printFail) {
                    return aguardarPosImpressao(withPrint ? 900 : 0).then(function () {
                        return printFail;
                    });
                }).then(function (printFail) {
                    jsonPost(urls.apiPdvLimparCheckoutDraft, {}).catch(function () {});
                    pdvMpPointBeep(mpPointFormaDivergiu ? 'err' : 'ok');
                    resetWizardParaNovaVenda();
                    if (nfceErro) {
                        mostrarAvisoNfcePendente(
                            { nfceErro: nfceErro },
                            'Pagamento no Point confirmado.'
                        );
                        return;
                    }
                    if (printFail) {
                        showSaleDoneFeedback(
                            'Venda registrada — falha na impressão. Reimprima pela lista de vendas, se precisar.',
                            'warn',
                            { placementTop: true }
                        );
                    } else if (nfceOk) {
                        showSaleDoneFeedback(
                            (result.entrega
                                ? 'Pagamento no Point confirmado, venda e entrega OK. '
                                : 'Pagamento no Point confirmado. ') + nfceOk,
                            'success'
                        );
                    } else {
                        showSaleDoneFeedback(
                            result.entrega
                                ? 'Pagamento no Point confirmado, venda registrada e entrega lançada.'
                                : 'Pagamento no Point confirmado e venda registrada com sucesso.',
                            'success'
                        );
                    }
                });
            })
            .catch(function (err) {
                if (printWin && !printWin.closed) {
                    try {
                        printWin.close();
                    } catch (errC) {}
                }
                if (err && err.mpPointUserAbort) {
                    jsonPost(urls.apiPdvLimparCheckoutDraft, {}).catch(function () {});
                    showMpPointCancelFeedback();
                } else if (err && err.mpPointUi) {
                    jsonPost(urls.apiPdvLimparCheckoutDraft, {}).catch(function () {});
                    pdvMpPointBeep('err');
                    showMpPointAviso(err.message || 'Operação cancelada na maquininha.');
                } else {
                    pdvMpPointBeep('err');
                    showMpPointAviso(
                        (err && err.message) || 'Falha no fluxo Mercado Pago Point.',
                        { tone: 'error' }
                    );
                }
            })
            .finally(function () {
                hideMpPointWaitBar();
                if (window.gmLoadingBar) window.gmLoadingBar.hide();
                isProcessingSale = false;
                setConfirmButtonsBusy(false);
            });
    }

    function isPaymentFormaModalOpen() {
        return dom.paymentFormaModal && !dom.paymentFormaModal.classList.contains('hidden');
    }

    function openPaymentFormaModal() {
        if (!dom.paymentFormaModal) return;
        dom.paymentFormaModal.classList.remove('hidden');
        dom.paymentFormaModal.classList.add('flex');
        try {
            document.body.style.overflow = 'hidden';
        } catch (err) {}
        syncPdvSspinIdlePause();
        var first = dom.paymentFormaModal.querySelector('[data-payment-modal-card]');
        if (first) first.focus();
    }

    function closePaymentFormaModal() {
        if (!dom.paymentFormaModal) return;
        dom.paymentFormaModal.classList.add('hidden');
        dom.paymentFormaModal.classList.remove('flex');
        try {
            document.body.style.overflow = '';
        } catch (err2) {}
        syncPdvSspinIdlePause();
    }

    function commitDiscountField() {
        if (!dom.paymentDiscount) return;
        var raw = dom.paymentDiscount.value;
        if (State.sanitizeMoneyInputTyping) raw = State.sanitizeMoneyInputTyping(raw);
        var n = State.toNumber(raw);
        var fmt = n > 0.009 ? moneyFieldDisplay(n) : '';
        State.setPagamentoField('descontoGeral', fmt);
        setInputValue(dom.paymentDiscount, fmt);
    }

    function focusDiscountField() {
        closePaymentFormaModal();
        var dlgDin = document.getElementById('pdv-pay-pop-dinheiro');
        if (dlgDin && dlgDin.open && typeof dlgDin.close === 'function') {
            try {
                dlgDin.close();
            } catch (errDin) {}
        }
        var footer = dom.mainFooter;
        if (footer) {
            try {
                footer.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
            } catch (errScroll) {}
        }
        setTimeout(function () {
            if (dom.paymentDiscount) {
                dom.paymentDiscount.focus();
                try {
                    dom.paymentDiscount.select();
                } catch (errSel) {}
            }
        }, 100);
    }

    function confirmDiscountAndOpenFormas() {
        commitDiscountField();
        var dlgDin = document.getElementById('pdv-pay-pop-dinheiro');
        if (dlgDin && dlgDin.open && typeof dlgDin.close === 'function') {
            try {
                dlgDin.close();
            } catch (errClose) {}
        }
        closePaymentFormaModal();
        var comp = State.getComputed();
        if (dom.paymentFeedback) {
            dom.paymentFeedback.textContent =
                comp.desconto > 0.009
                    ? 'Desconto ' +
                      formatMoney(comp.desconto) +
                      ' aplicado. Escolha a forma de pagamento (F3).'
                    : 'Escolha a forma de pagamento (F3).';
        }
        openPaymentFormaModal();
    }

    function refreshDinheiroPopTrocoDisplay(st, comp) {
        var trocoEl = document.getElementById('pdv-pay-pop-din-troco');
        if (!trocoEl) return;
        var restDin = saldoRestantePagamento(st, comp);
        var tr = String(st.pagamento.trocoCalculado || '').trim();
        if (tr) {
            trocoEl.textContent = formatMoney(State.toNumber(tr));
            return;
        }
        var recN = State.toNumber(st.pagamento.valorRecebido);
        if (recN > restDin + 0.009) trocoEl.textContent = formatMoney(recN - restDin);
        else trocoEl.textContent = '—';
    }

    function syncDinheiroRecebidoValue(raw, syncPopInput) {
        var val = raw == null ? '' : String(raw);
        State.setPagamentoField('valorRecebido', val);
        setInputValue(dom.paymentReceived, val);
        var st = State.getState();
        var comp = State.getComputed();
        var recebido = State.toNumber(val);
        var rest = saldoRestantePagamento(st, comp);
        var trocoStr = '';
        if (recebido > rest + 0.009) {
            trocoStr = String((recebido - rest).toFixed(2)).replace('.', ',');
        }
        State.setPagamentoField('trocoCalculado', trocoStr);
        setInputValue(dom.paymentChange, trocoStr);
        refreshDinheiroPopTrocoDisplay(State.getState(), comp);
        if (syncPopInput) {
            var recInp = document.getElementById('pdv-pay-pop-din-recebido');
            if (recInp) setInputValue(recInp, val);
        }
    }

    function commitDinheiroRecebido(opts) {
        opts = opts || {};
        var st = State.getState();
        if (st.currentStep !== 'pagamento' || st.pagamento.forma !== 'Dinheiro') return false;
        var comp = State.getComputed();
        var rest = saldoRestantePagamento(st, comp);
        var raw = String(
            opts.raw != null
                ? opts.raw
                : (dom.paymentReceived && dom.paymentReceived.value) || ''
        ).trim();
        var cur = raw ? State.toNumber(raw) : 0;
        if (!raw || cur <= 0.009) {
            if (rest <= 0.009) return false;
            var fmt = String(rest.toFixed(2)).replace('.', ',');
            syncDinheiroRecebidoValue(fmt, true);
            return false;
        }
        var R = cur;
        var T = Math.min(R, rest);
        var err = erroCommitTranche(st, comp, T);
        if (err) {
            alert(err);
            return false;
        }
        var trocoVal = R > rest + 0.009 ? R - rest : 0;
        State.addPagamentoLancamento(
            snapshotLancamentoFromState(st, T, {
                valorRecebido: String(R.toFixed(2)).replace('.', ','),
                trocoCalculado: trocoVal > 0.009 ? String(trocoVal.toFixed(2)).replace('.', ',') : ''
            })
        );
        if (opts.closeDialog) {
            var dlg = document.getElementById('pdv-pay-pop-dinheiro');
            if (dlg && typeof dlg.close === 'function') {
                try {
                    dlg.close();
                } catch (errClose) {}
            }
        }
        afterCommitTrancheFlow();
        return true;
    }

    function focusFirstFlowFieldForForma(forma) {
        setTimeout(function () {
            var st = State.getState();
            var mid = String((st.pagamento && st.pagamento.maquinaId) || '').trim();
            var tr = document.getElementById('pdv-pay-valor-tranche');
            if (forma === 'Dinheiro') {
                openPayPopDinheiroResumo({ autofocus: true });
                return;
            }
            if (forma === 'PIX') {
                if (!mid) {
                    var bp = document.getElementById('pdv-pay-open-maquinas-pix');
                    if (bp) bp.focus();
                    return;
                }
                if (tr) tr.focus();
            } else if (
                forma === 'Cartão de débito' ||
                forma === 'Cartão de crédito' ||
                forma === 'Cartão de crédito parcelado'
            ) {
                if (!mid) {
                    var bc = document.getElementById('pdv-pay-open-maquinas-card');
                    if (bc) bc.focus();
                    return;
                }
                if (forma === 'Cartão de crédito parcelado' && dom.paymentParcelasCredito) dom.paymentParcelasCredito.focus();
                else if (tr) tr.focus();
            } else if (forma === 'Outro' && dom.outroValidarPin) dom.outroValidarPin.focus();
            else if (tr) tr.focus();
        }, 80);
    }

    function choosePaymentFormaFromModal(forma) {
        if (!forma) return;
        selectPaymentForma(forma);
        closePaymentFormaModal();
        if (requiresMaquina(forma)) {
            openMaquinasDialog();
        } else {
            focusFirstFlowFieldForForma(forma);
        }
    }

    function selectPaymentForma(forma) {
        var st = State.getState();
        var comp = State.getComputed();
        var rest = saldoRestantePagamento(st, comp);
        var patch = {
            forma: forma,
            maquinaId: '',
            maquinaNome: '',
            mpBalcaoModo: '',
            outroPinVerificado: forma === 'Outro' ? !!st.pagamento.outroPinVerificado : false
        };
        if (forma === 'Vale crédito') {
            var sv = Math.min(saldoValeAtual(st), rest);
            patch.valorDestaForma = sv > 0 ? String(sv.toFixed(2)).replace('.', ',') : '';
        } else if (forma === 'Cashback') {
            var scb = Math.min(saldoCashbackAtual(st), rest);
            patch.valorDestaForma = scb > 0 ? String(scb.toFixed(2)).replace('.', ',') : '';
        } else if (forma === 'Fiado') {
            var msgFiado = validarFiadoPermitido(st);
            if (msgFiado) {
                showSaleDoneFeedback(msgFiado, 'error');
                return;
            }
            patch.valorDestaForma = rest > 0 ? String(rest.toFixed(2)).replace('.', ',') : '';
        } else if (forma !== 'Dinheiro') {
            patch.valorDestaForma = rest > 0.009 ? String(rest.toFixed(2)).replace('.', ',') : '';
        } else {
            patch.valorDestaForma = '';
        }
        State.setPagamentoPatch(patch);
    }

    function normalizeDigitKeyCode(code) {
        if (/^Numpad[1-9]$/.test(code)) return 'Digit' + code.slice(6);
        return code;
    }

    function paymentShortcutForma(code) {
        var c = normalizeDigitKeyCode(code);
        var map = {
            Digit1: 'Dinheiro',
            Digit2: 'PIX',
            Digit3: 'Cartão de débito',
            Digit4: 'Cartão de crédito',
            Digit5: 'Cartão de crédito parcelado',
            Digit6: 'Fiado',
            Digit7: 'Vale crédito',
            Digit8: 'Cashback',
            Digit9: 'Outro'
        };
        return map[c] || '';
    }

    function runCommitTrancheFromInput() {
        var inp = document.getElementById('pdv-pay-valor-tranche');
        if (!inp) return;
        var st = State.getState();
        if (st.currentStep !== 'pagamento') return;
        var comp = State.getComputed();
        var rest = saldoRestantePagamento(st, comp);
        var raw = String(inp.value || '').trim();
        var cur = raw ? State.toNumber(raw) : 0;
        if (!raw || cur <= 0.009) {
            if (rest <= 0.009) {
                if (dom.confirmSaleNoPrint && !dom.confirmSaleNoPrint.disabled) confirmSale(false);
                return;
            }
            var fmt = String(rest.toFixed(2)).replace('.', ',');
            inp.value = fmt;
            State.setPagamentoField('valorDestaForma', fmt);
            showPdvAviso('Valor preenchido com o que falta. Toque no botão verde para continuar.');
            return;
        }
        var err = erroCommitTranche(st, comp, cur);
        if (err) {
            showPdvAviso(err);
            return;
        }
        commitTrancheFlow(st, comp, cur);
    }

    function handleValorTrancheEnter(event) {
        if (event.key !== 'Enter') return;
        var tag = (event.target && event.target.tagName) || '';
        if (tag === 'TEXTAREA') return;
        var inp = document.getElementById('pdv-pay-valor-tranche');
        if (!inp || event.target !== inp) return;
        event.preventDefault();
        runCommitTrancheFromInput();
    }

    function handlePaymentReceivedEnter(event) {
        if (event.key !== 'Enter') return;
        var tag = (event.target && event.target.tagName) || '';
        if (tag === 'TEXTAREA') return;
        var popRec = document.getElementById('pdv-pay-pop-din-recebido');
        var fromPop = popRec && event.target === popRec;
        if (!fromPop && (!dom.paymentReceived || event.target !== dom.paymentReceived)) return;
        event.preventDefault();
        var raw = fromPop ? popRec.value : dom.paymentReceived.value;
        commitDinheiroRecebido({ raw: raw, closeDialog: fromPop });
    }

    function focusParcelasThenTranche(event) {
        if (event.key !== 'Enter') return;
        if (!dom.paymentParcelasCredito || event.target !== dom.paymentParcelasCredito) return;
        event.preventDefault();
        var tr = document.getElementById('pdv-pay-valor-tranche');
        if (tr) tr.focus();
    }

    function payFlowDialogOpen() {
        return !!(dom.stepPagamentoRoot && dom.stepPagamentoRoot.querySelector('dialog[open]'));
    }

    function showPayFlowDialog(dlg) {
        if (dlg && typeof dlg.showModal === 'function') {
            try {
                dlg.showModal();
            } catch (err) {}
        }
    }

    function openPayPopQr(title) {
        var dlg = document.getElementById('pdv-pay-pop-qr');
        var body = document.getElementById('pdv-pay-pop-qr-body');
        var tEl = document.getElementById('pdv-pay-pop-qr-title');
        if (!dlg || !body) return;
        if (tEl) tEl.textContent = title || 'QR Code';
        var u = String(pagamentoUi.qrMercadoPagoUrl || '').trim();
        if (u) {
            body.innerHTML =
                '<img src="' +
                escapeHtml(u) +
                '" alt="" class="mx-auto max-h-[min(68vh,500px)] w-auto max-w-full object-contain">';
        } else {
            body.innerHTML =
                '<p class="p-6 text-center text-sm font-bold text-slate-600">O QR aparece na maquininha. Amplie no terminal do cliente, se precisar.</p>';
        }
        showPayFlowDialog(dlg);
    }

    function openPayPopDinheiroResumo(opts) {
        opts = opts || {};
        var dlg = document.getElementById('pdv-pay-pop-dinheiro');
        if (!dlg) return;
        var st = State.getState();
        var comp = State.getComputed();
        var totalEl = document.getElementById('pdv-pay-pop-din-total');
        var recInp = document.getElementById('pdv-pay-pop-din-recebido');
        var restDin = saldoRestantePagamento(st, comp);
        if (totalEl) totalEl.textContent = formatMoney(restDin);
        if (recInp) setInputValue(recInp, String(st.pagamento.valorRecebido || '').trim());
        refreshDinheiroPopTrocoDisplay(st, comp);
        showPayFlowDialog(dlg);
        if (opts.autofocus !== false && recInp) {
            setTimeout(function () {
                try {
                    recInp.focus();
                    if (recInp.select) recInp.select();
                } catch (errFocus) {}
            }, 60);
        }
    }

    function openPayPopFiado() {
        var dlg = document.getElementById('pdv-pay-pop-fiado');
        var body = document.getElementById('pdv-pay-pop-fiado-body');
        if (!dlg || !body) return;
        var st = State.getState();
        var fp = parseInt(st.pagamento.fiadoParcelas, 10) || 1;
        var fd = parseInt(st.pagamento.fiadoDiasVencimento, 10) || 30;
        body.innerHTML =
            'Conta a receber em <strong>' +
            fp +
            'x</strong><br>1º vencimento em <strong>' +
            fd +
            ' dias</strong>.';
        showPayFlowDialog(dlg);
    }

    function openPayPopSaldo(titulo, valorFmt, hint) {
        var dlg = document.getElementById('pdv-pay-pop-saldo');
        if (!dlg) return;
        var t = document.getElementById('pdv-pay-pop-saldo-title');
        var v = document.getElementById('pdv-pay-pop-saldo-valor');
        var h = document.getElementById('pdv-pay-pop-saldo-hint');
        if (t) t.textContent = titulo || 'Saldo';
        if (v) v.textContent = valorFmt || formatMoney(0);
        if (h) h.textContent = hint || '';
        showPayFlowDialog(dlg);
    }

    function openPayPopOutroHelp() {
        var dlg = document.getElementById('pdv-pay-pop-outro-help');
        showPayFlowDialog(dlg);
    }

    function validarPinOutro() {
        if (!urls.apiLoginMobile) {
            alert('Rota de PIN não configurada.');
            return;
        }
        var pin = window.prompt('PIN do operador:');
        if (pin == null || String(pin).trim() === '') return;
        var fd = new FormData();
        fd.append('pin', String(pin).trim());
        fetch(urls.apiLoginMobile, {
            method: 'POST',
            credentials: 'same-origin',
            headers: { 'X-CSRFToken': csrfToken() },
            body: fd
        })
            .then(function (res) {
                return res.json().then(function (data) {
                    return { ok: res.ok && data && data.ok, data: data };
                });
            })
            .then(function (r) {
                if (r.ok) {
                    State.setPagamentoField('outroPinVerificado', true);
                    var op = (r.data && r.data.operador) ? String(r.data.operador).trim() : '';
                    if (op) State.setPagamentoField('operadorPdv', op);
                } else {
                    alert((r.data && r.data.erro) || 'PIN inválido.');
                }
            })
            .catch(function () {
                alert('Falha ao validar PIN.');
            });
    }

    function hhmmMinusMinutes(hhmm, mins) {
        var p = String(hhmm || '').trim().split(':');
        var h = parseInt(p[0], 10);
        var m = parseInt(p[1], 10);
        if (!isFinite(h) || !isFinite(m)) return '';
        var t = h * 60 + m - mins;
        while (t < 0) t += 24 * 60;
        t = t % (24 * 60);
        var nh = Math.floor(t / 60);
        var nm = t % 60;
        return String(nh).padStart(2, '0') + ':' + String(nm).padStart(2, '0');
    }

    function obterLembretesLocal() {
        try {
            var d = JSON.parse(localStorage.getItem('gmLembretesCaixa') || '[]');
            return Array.isArray(d) ? d : [];
        } catch (e0) {
            return [];
        }
    }

    function salvarLembretesLocal(lista) {
        try {
            localStorage.setItem('gmLembretesCaixa', JSON.stringify(lista));
        } catch (e1) {}
    }

    function tocarSomLembreteWizard() {
        try {
            var AC = window.AudioContext || window.webkitAudioContext;
            if (!AC) return;
            var audioCtx = new AC();
            [1320, 980, 1320].forEach(function (freq, idx) {
                var osc = audioCtx.createOscillator();
                var gain = audioCtx.createGain();
                osc.frequency.setValueAtTime(freq, audioCtx.currentTime);
                osc.connect(gain);
                gain.connect(audioCtx.destination);
                var start = audioCtx.currentTime + idx * 0.12;
                gain.gain.setValueAtTime(0.2, start);
                gain.gain.exponentialRampToValueAtTime(0.001, start + 0.15);
                osc.start(start);
                osc.stop(start + 0.16);
            });
        } catch (e2) {}
    }

    function exibirAlertaLembreteWizard(lembrete) {
        var t = document.getElementById('alerta-lembrete-texto');
        var box = document.getElementById('alerta-lembrete');
        if (t && box && lembrete) {
            t.textContent = (lembrete.hora || '') + ' · ' + (lembrete.texto || '');
            box.classList.remove('hidden');
            tocarSomLembreteWizard();
        } else if (lembrete) {
            alert((lembrete.hora || '') + ' — ' + (lembrete.texto || ''));
        }
    }

    function verificarLembretesWizardTick() {
        var agora = new Date();
        var hoje = agora.toISOString().slice(0, 10);
        var hh = String(agora.getHours()).padStart(2, '0');
        var mm = String(agora.getMinutes()).padStart(2, '0');
        var horaAtual = hh + ':' + mm;
        var lista = obterLembretesLocal();
        var alterou = false;
        lista.forEach(function (item) {
            if (item.data !== hoje) {
                item.data = hoje;
                item.disparado = false;
                alterou = true;
            }
            if (!item.concluido && !item.disparado && String(item.hora || '') <= horaAtual) {
                item.disparado = true;
                alterou = true;
                exibirAlertaLembreteWizard(item);
            }
        });
        if (alterou) salvarLembretesLocal(lista);
    }

    function wizardSyncLembretesFromEntregaHorario() {
        var horarioVal = dom.entregaHorario ? dom.entregaHorario.value : '';
        var lista = obterLembretesLocal().filter(function (x) {
            return String(x.id || '').indexOf('pdv_wiz_ent_') !== 0;
        });
        if (!horarioVal) {
            salvarLembretesLocal(lista);
            return;
        }
        var nome = currentClientName(State.getState());
        var h20 = hhmmMinusMinutes(horarioVal, 20);
        var d = new Date().toISOString().slice(0, 10);
        if (h20 && h20 !== horarioVal) {
            lista.push({
                id: 'pdv_wiz_ent_warn_' + horarioVal,
                texto: 'Entrega — ' + nome + ' (faltam 20 min)',
                hora: h20,
                disparado: false,
                data: d,
                concluido: false
            });
        }
        lista.push({
            id: 'pdv_wiz_ent_at_' + horarioVal,
            texto: 'Entrega — ' + nome + ' (horário)',
            hora: horarioVal,
            disparado: false,
            data: d,
            concluido: false
        });
        salvarLembretesLocal(lista);
    }

    function wizardItemMetaPdv(item) {
        var cg = String((item && item.codigoGm) || (item && item.codigo) || '').trim();
        return { codigo_gm: cg || '—', prateleira: '' };
    }

    /** Mesmo critério de moeda do cupom no painel Entregas (toLocaleString BRL). */
    function wizardPrintMoedaCupom(n) {
        var x = Number(n);
        if (!isFinite(x)) return '—';
        try {
            return x.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
        } catch (e0) {
            return '—';
        }
    }

    function wizardPrintCodigoBarrasEntrega(e) {
        var c = String(e.retomar_codigo || '').trim();
        if (c) return c;
        if (e.orc_local_id != null) return 'GMORC' + String(e.orc_local_id);
        return 'ENT' + String(e.id || '');
    }

    /** Espelha urlClienteMapsParaQr do painel (http manual, senão busca por plus/end/texto extra). */
    function wizardPrintUrlMapsQr(e) {
        var m = String(e.maps_url_manual || '').trim();
        if (/^https?:\/\//i.test(m)) return m;
        var extra = m;
        var pc = String(e.plus_code || '').trim();
        var end = String(e.endereco_linha || '').trim();
        var q = [pc, extra, end].filter(Boolean).join(' ').trim() || String(e.cliente_nome || '');
        if (!q.trim()) return '';
        return 'https://www.google.com/maps/search/?api=1&query=' + encodeURIComponent(q);
    }

    function wizardPrintPayloadEntrega(state, computed, orcId) {
        var e = state.entrega || {};
        var c = state.cliente || {};
        var dh = new Date().toISOString().replace('T', ' ').slice(0, 19);
        var trocoPrecisa = (function () {
            var arr = state.pagamento.lancamentos || [];
            var any = arr.some(function (L) {
                return L.forma === 'Dinheiro' && String(L.trocoCalculado || '').trim();
            });
            return !!(any || String(e.troco || '').trim());
        })();
        var itensJson = (state.itens || []).map(function (it) {
            var cg = String((it.codigoGm || it.codigo || '')).trim();
            return {
                codigo_gm: cg,
                codigo: String(it.codigo || ''),
                nome: String(it.nome || ''),
                qtd: it.qtd,
                preco: Number(it.preco || 0),
                prateleira: ''
            };
        });
        return {
            id: orcId,
            orc_local_id: orcId,
            retomar_codigo: 'GMORC' + String(orcId),
            criado_em: dh,
            cliente_nome: currentClientName(state),
            telefone: c.telefone || '',
            plus_code: String(e.plusCode || c.plus_code || '').trim(),
            endereco_linha: composeEndereco(state),
            referencia_rural: String(e.referencia || c.referencia_rural || '').trim(),
            forma_pagamento: String(formaPagamentoResumoUi(state, computed) || ''),
            troco_precisa: trocoPrecisa,
            maps_url_manual: String(c.maps_url_manual || '').trim(),
            itens_json: itensJson,
            total_texto: formatMoney(computed.total)
        };
    }

    function wizardPrintNomeClienteHtml(nome) {
        if (typeof window.agroCupomNomeClienteHtml === 'function') {
            return window.agroCupomNomeClienteHtml(nome);
        }
        return (
            '<div class="nome-cliente" style="font-weight:900;font-size:32px;line-height:1.15;word-break:break-word;overflow-wrap:break-word;text-align:center;white-space:pre-wrap;margin:8px 0 6px;letter-spacing:-0.01em;">' +
            escapeHtml(nome || '—') +
            '</div>'
        );
    }

    function wizardPrintPgCorteHtml() {
        if (typeof window.agroCupomPgCorteHtml === 'function') {
            return window.agroCupomPgCorteHtml();
        }
        return '<div class="pg-avanco-corte" aria-hidden="true">&nbsp;</div>';
    }

    function wizardPrintRodapeSistvaleHtml() {
        if (typeof window.agroCupomRodapeSistvaleHtml === 'function') {
            return window.agroCupomRodapeSistvaleHtml();
        }
        return (
            '<div class="rodape-sistvale" style="text-align:center;font-size:11px;font-weight:900;letter-spacing:.16em;margin-top:10px;padding:5px 4px 4px;background:#000;color:#fff;">SISTVALE</div>'
        );
    }

    /** Igual htmlPagSeparacao do painel Entregas. */
    function wizardPrintHtmlSeparacao(e) {
        var items = Array.isArray(e.itens_json) ? e.itens_json : [];
        var dh = String(e.criado_em || '').replace('T', ' ').slice(0, 19);
        var h = '<div class="pg">';
        h += '<div style="text-align:center;font-weight:900;font-size:15px;letter-spacing:0.04em;">SEPARAÇÃO</div>';
        h += '<div style="font-size:9px;margin:6px 0 8px;color:#333;">' + escapeHtml(dh) + '</div>';
        h += wizardPrintNomeClienteHtml(e.cliente_nome);
        if (e.telefone) h += '<div style="margin-top:3px;">Tel ' + escapeHtml(e.telefone) + '</div>';
        if (e.plus_code) h += '<div style="margin-top:2px;">Plus ' + escapeHtml(e.plus_code) + '</div>';
        if (e.endereco_linha) h += '<div style="margin-top:2px;line-height:1.3;">' + escapeHtml(e.endereco_linha) + '</div>';
        if (e.referencia_rural) h += '<div style="margin-top:2px;">Ref. ' + escapeHtml(e.referencia_rural) + '</div>';
        if (e.forma_pagamento) {
            var fp = escapeHtml(e.forma_pagamento);
            if (e.forma_pagamento === 'Dinheiro') {
                if (e.troco_precisa === true) fp += ' · troco: sim';
                else if (e.troco_precisa === false) fp += ' · troco: não';
            }
            h += '<div style="font-size:10px;margin-top:6px;"><b>Pag.</b> ' + fp + '</div>';
        }
        h += '<div style="border-top:2px solid #000;margin:10px 0 8px;"></div>';
        items.forEach(function (it) {
            var cod = it.codigo_gm != null ? String(it.codigo_gm) : it.codigo != null ? String(it.codigo) : '';
            h += '<div style="border-top:1px dashed #000;margin-top:10px;padding-top:8px;">';
            if (cod) h += '<div style="font-size:10px;"><b>GM</b> ' + escapeHtml(cod) + '</div>';
            h += '<div style="font-weight:bold;line-height:1.25;margin-top:2px;">' + escapeHtml(it.nome || '') + '</div>';
            h += '<div style="font-size:20px;font-weight:900;margin-top:6px;">QTD ' + escapeHtml(String(it.qtd != null ? it.qtd : '')) + '</div>';
            if (it.prateleira) h += '<div style="font-size:10px;margin-top:2px;"><b>Prat.</b> ' + escapeHtml(String(it.prateleira)) + '</div>';
            h += '</div>';
        });
        if (e.total_texto) {
            h +=
                '<div style="border-top:3px solid #000;margin:12px 0 6px;padding-top:6px;font-weight:900;display:flex;justify-content:space-between;align-items:baseline;gap:4px;">' +
                '<span style="font-size:20px;">TOTAL</span>' +
                '<span style="font-size:34px;letter-spacing:-0.03em;">' +
                escapeHtml(String(e.total_texto)) +
                '</span></div>';
        }
        h += '<div style="margin-top:12px;text-align:center;">';
        h += '<svg id="barc-orc" xmlns="http://www.w3.org/2000/svg"></svg>';
        h += '<div style="font-size:9px;margin-top:6px;">Bipe no PDV para retomar o orçamento</div></div>';
        h += wizardPrintRodapeSistvaleHtml();
        h += wizardPrintPgCorteHtml();
        h += '</div>';
        return h;
    }

    /** Igual htmlPagEntregador do painel Entregas. */
    function wizardPrintHtmlEntregador(e) {
        var nomeCli = String(e.cliente_nome || '');
        var primeiro = (nomeCli.split(/\s+/)[0] || nomeCli || '—').toUpperCase();
        var dh = String(e.criado_em || '').replace('T', ' ').slice(0, 19);
        var end = String(e.endereco_linha || '').trim();
        var mapsUrl = wizardPrintUrlMapsQr(e);
        var qrImg = mapsUrl
            ? '<img src="https://api.qrserver.com/v1/create-qr-code/?size=200x200&margin=1&data=' +
              encodeURIComponent(mapsUrl) +
              '" alt="" style="width:36mm;height:auto;display:block;margin:6px auto 0;" />'
            : '<div style="text-align:center;margin-top:6px;font-size:10px;">(sem destino no Maps)</div>';
        var items = Array.isArray(e.itens_json) ? e.itens_json : [];
        var entItems = '';
        items.forEach(function (it) {
            entItems +=
                '<div style="margin-top:6px;line-height:1.35;font-size:14px;font-weight:800;">' +
                escapeHtml(String(it.qtd != null ? it.qtd : '') + '× ' + String(it.nome || '')) +
                '</div>';
        });
        var h = '<div class="pg">';
        h += '<div style="text-align:center;font-weight:900;font-size:14px;">ENTREGA</div>';
        h += '<div style="font-size:26px;font-weight:900;text-align:center;line-height:1;margin:10px 0 8px;letter-spacing:-0.02em;">' + escapeHtml(primeiro) + '</div>';
        h += '<div style="font-size:10px;">' + escapeHtml(dh) + '</div>';
        h += wizardPrintNomeClienteHtml(nomeCli);
        if (e.telefone) {
            h += '<div style="font-size:12px;font-weight:800;margin-top:4px;">Tel ' + escapeHtml(e.telefone) + '</div>';
        }
        if (e.total_texto) {
            h += '<div style="border-top:3px solid #000;margin:10px 0 8px;"></div>';
            h +=
                '<div style="font-weight:900;display:flex;justify-content:space-between;align-items:baseline;gap:4px;">' +
                '<span style="font-size:20px;">TOTAL</span>' +
                '<span style="font-size:34px;letter-spacing:-0.03em;">' +
                escapeHtml(String(e.total_texto)) +
                '</span></div>';
        }
        h += '<div style="border-top:1px dashed #000;margin:10px 0 8px;"></div>';
        h += entItems;
        h += '<div style="margin-top:10px;font-size:13px;font-weight:900;">Endereço</div>';
        h += '<div style="font-size:18px;font-weight:900;word-break:break-word;line-height:1.32;margin-top:4px;">' + escapeHtml(end || '—') + '</div>';
        h += qrImg;
        h += wizardPrintRodapeSistvaleHtml();
        h += wizardPrintPgCorteHtml();
        h += '</div>';
        return h;
    }

    /** Cupom do pacote entrega/orçamento — via do cliente (3ª folha), cabeçalho igual cupom de venda. */
    function wizardPrintHtmlCupom(e) {
        var items = Array.isArray(e.itens_json) ? e.itens_json : [];
        var mapped = items.map(function (it) {
            var q = Number(it.qtd != null ? it.qtd : 0);
            var preco = Number(it.preco != null ? it.preco : 0);
            return {
                nome: String(it.nome || ''),
                qtd: q,
                preco: preco,
                subtotal: isFinite(q) && isFinite(preco) ? q * preco : 0
            };
        });
        var bc = wizardPrintCodigoBarrasEntrega(e);
        var rodapeExtra =
            'Retomar: ' +
            escapeHtml(bc) +
            '<br><span style="font-size:8px;">Bipe no PDV para retomar o orçamento</span>';
        var cab =
            typeof window.agroCupomCabecalhoHtml === 'function'
                ? window.agroCupomCabecalhoHtml()
                : typeof window.agroCupomLogoHtml === 'function'
                  ? window.agroCupomLogoHtml()
                  : '';
        if (typeof window.agroCupomInnerHtml === 'function') {
            var ehFiadoCup = /fiado/i.test(String(e.forma_pagamento || ''));
            return window.agroCupomInnerHtml({
                criado_em: String(e.criado_em || '').replace('T', ' ').slice(0, 19),
                cliente_nome: e.cliente_nome,
                telefone: e.telefone || '',
                endereco_linha: e.endereco_linha || '',
                forma_pagamento: e.forma_pagamento,
                total_texto: String(e.total_texto || '—'),
                itens: mapped,
                subtitulo: ehFiadoCup ? 'COMPROVANTE FIADO' : 'ORÇAMENTO / ENTREGA',
                via_rotulo: 'VIA DO CLIENTE',
                eh_fiado: ehFiadoCup,
                fiado_dias: 30,
                com_assinatura: false,
                mostrar_cabecalho: true,
                endereco_grande: false,
                rodape_extra: rodapeExtra
            });
        }
        var dh = String(e.criado_em || '').replace('T', ' ').slice(0, 19);
        var h = '<div class="pg">' + cab;
        h += '<div style="text-align:center;font-weight:900;font-size:10px;margin:4px 0;">ORÇAMENTO / ENTREGA · VIA DO CLIENTE</div>';
        h += wizardPrintNomeClienteHtml(e.cliente_nome);
        h += '<div>' + escapeHtml(dh) + ' · ' + escapeHtml(String(e.total_texto || '')) + '</div></div>';
        return h;
    }

    /**
     * Mesmo fluxo do painel Entregas (imprimirPacotePainel): iframe + documento próprio + JsBarcode no iframe.
     * Evita impressão em branco: o pack no DOM do wizard não é filho direto de body, então o CSS antigo
     * body.print-pdv-entrega-pack > * escondia tudo.
     */
    function wizardImprimirPacoteEntrega(orcId, opt) {
        opt = opt || { sep: true, ent: true, cup: true };
        var state = State.getState();
        var computed = State.getComputed();
        var e = wizardPrintPayloadEntrega(state, computed, orcId);
        var parts = [];
        if (opt.sep) parts.push(wizardPrintHtmlSeparacao(e));
        if (opt.ent) parts.push(wizardPrintHtmlEntregador(e));
        if (opt.cup) parts.push(wizardPrintHtmlCupom(e));
        if (!parts.length) return;
        var barcodeVal = wizardPrintCodigoBarrasEntrega(e);
        var packStyles =
            typeof window.agroCupomStyles === 'function'
                ? window.agroCupomStyles()
                : '@page{margin:0;size:80mm auto}html,body{margin:0;padding:0;width:80mm}body{font-family:system-ui,sans-serif}.pg{width:80mm;margin:0 auto;padding:0;page-break-inside:avoid;break-inside:avoid-page;overflow:visible;box-sizing:border-box}.pg + .pg{page-break-before:always;break-before:page}.pg-avanco-corte{display:block;height:14mm;min-height:14mm;line-height:14mm;font-size:1px;color:transparent;overflow:hidden}.cupom-cabecalho,.cupom-logo{width:100%}.cupom-logo img{width:100%;max-width:100%;height:auto;display:block;margin:0}.cupom-zap{width:100%;display:flex;align-items:center;justify-content:center;gap:7px;font-size:16px;font-weight:900}.nome-cliente{font-weight:900;font-size:32px;line-height:1.15;word-break:break-word;overflow-wrap:break-word;text-align:center;white-space:pre-wrap;margin:8px 0 6px;letter-spacing:-0.01em}.rodape-sistvale{text-align:center;font-size:11px;font-weight:900;letter-spacing:.16em;margin-top:10px;padding:5px 4px 4px;background:#000;color:#fff;-webkit-print-color-adjust:exact;print-color-adjust:exact}';

        var pages = [];
        if (opt.sep) pages.push({ html: wizardPrintHtmlSeparacao(e), barcodeVal: barcodeVal });
        if (opt.ent) pages.push({ html: wizardPrintHtmlEntregador(e) });
        if (opt.cup) pages.push({ html: wizardPrintHtmlCupom(e) });

        if (typeof window.agroCupomImprimirPaginasSequencial === 'function') {
            window.agroCupomImprimirPaginasSequencial(pages, {
                iframeId: 'agro-print-iframe-entregas-pdv',
                title: 'Entrega PDV',
                styles: packStyles,
                gapMs: 420,
                readyDelay: 80
            });
            return;
        }

        if (typeof window.agroCupomPrintBodyInIframe === 'function') {
            window.agroCupomPrintBodyInIframe({
                iframeId: 'agro-print-iframe-entregas-pdv',
                title: 'Entrega PDV',
                styles: packStyles,
                bodyHtml: parts.join(''),
                barcodeVal: opt.sep ? barcodeVal : '',
                readyDelay: 80
            });
        }
    }

    function wizardModalEscolhaImpressaoEntrega() {
        return new Promise(function (resolve) {
            var root = document.getElementById('modal-pdv-entrega-impressao');
            if (!root) {
                resolve({ sep: true, ent: true, cup: true });
                return;
            }
            var btnImp = document.getElementById('mei-imprimir');
            var btnCan = document.getElementById('mei-cancelar');
            var done = false;
            function finish(v) {
                if (done) return;
                done = true;
                root.classList.add('hidden');
                root.classList.remove('flex');
                root.onclick = null;
                if (btnImp) btnImp.onclick = null;
                if (btnCan) btnCan.onclick = null;
                pdvTryRemoveModalOpenBody();
                resolve(v);
            }
            if (btnImp) {
                btnImp.onclick = function () {
                    var sep = document.getElementById('mei-chk-sep');
                    var ent = document.getElementById('mei-chk-ent');
                    var cup = document.getElementById('mei-chk-cup');
                    var s = sep && sep.checked;
                    var en = ent && ent.checked;
                    var c = cup && cup.checked;
                    if (!s && !en && !c) {
                        alert('Marque ao menos uma via para imprimir.');
                        return;
                    }
                    finish({ sep: s, ent: en, cup: c });
                };
            }
            if (btnCan) btnCan.onclick = function () {
                finish(null);
            };
            root.onclick = function (ev) {
                if (ev.target === root) finish(null);
            };
            root.classList.remove('hidden');
            root.classList.add('flex');
            pdvEnsureModalOpenBody();
        });
    }

    function obsFluxoEntregaResumo(state) {
        var e = state.entrega || {};
        var lp = String(e.localPagamento || '');
        var m = String(e.meioNaEntrega || '');
        if (lp === 'entrega' && m === 'dinheiro') {
            return (
                'Pagamento na entrega — dinheiro. Troco: ' +
                (String(e.troco || '').trim() || '—') +
                '. Registrado pelo PDV (Enviar entrega).'
            );
        }
        if (lp === 'entrega' && m === 'cartao') {
            return 'Pagamento na entrega — cartão (maquininha). Registrado pelo PDV (Enviar entrega).';
        }
        if (lp === 'loja') {
            return 'Pagamento na loja (fluxo PDV).';
        }
        return '';
    }

    function wizardEnviarEntregaPainel() {
        var state = State.getState();
        var computed = State.getComputed();
        if (!state.itens.length) {
            alert('Adicione itens à venda antes de enviar a entrega.');
            return;
        }
        commitEntregaCamposEndereco({ trimEnds: true });
        commitEntregaObsFromDom();
        if (!enderecoEntregaMinimoOk(state)) {
            alert('Preencha logradouro e bairro (ou endereço legível) para entrega.');
            return;
        }
        var lp = String((state.entrega && state.entrega.localPagamento) || '');
        var meio = String((state.entrega && state.entrega.meioNaEntrega) || '');
        if (lp !== 'entrega' || !meio) {
            abrirFluxoPagamentoEntregaSePendente();
            return;
        }
        fecharModaisEntregaAntesImpressao();
        wizardModalEscolhaImpressaoEntrega().then(function (opt) {
            if (!opt) return;
            var orcId = Date.now();
            wizardImprimirPacoteEntrega(orcId, opt);
            var state2 = State.getState();
            var computed2 = State.getComputed();
            var snapshot = State.exportWizardStateSnapshot
                ? State.exportWizardStateSnapshot()
                : null;
            var body = buildEntregaPayload(state2, computed2, {
                orc_local_id: orcId,
                retomar_codigo: 'GMORC' + String(orcId),
                obsExtra: obsFluxoEntregaResumo(state2)
            });
            body.aguarda_pagamento_pdv = true;
            body.pdv_wizard_state = snapshot || {};
            if (bootstrap.caixa && bootstrap.caixa.id) {
                body.sessao_caixa_id = bootstrap.caixa.id;
            }
            if (window.gmLoadingBar) window.gmLoadingBar.show();
            jsonPost(urls.apiEntregaRegistrar || '', body)
                .then(function (res) {
                    if (!res.ok || !res.data || !res.data.ok) {
                        throw new Error((res.data && (res.data.erro || res.data.mensagem)) || 'Falha ao registrar no painel Entregas.');
                    }
                    resetWizardParaNovaVenda();
                    showSaleDoneFeedback(
                        'Entrega enviada. Quando o entregador voltar, use Entregas para registrar o pagamento.',
                        'success'
                    );
                    return refreshEntregasPendentesUi(true);
                })
                .catch(function (err) {
                    alert(err && err.message ? err.message : 'Não foi possível registrar no painel Entregas.');
                })
                .finally(function () {
                    if (window.gmLoadingBar) window.gmLoadingBar.hide();
                });
        });
    }

    function wizardIrParaPagamentoComImpressao() {
        var state = State.getState();
        if (!state.itens.length) {
            alert('Adicione itens à venda antes de continuar.');
            return;
        }
        commitEntregaCamposEndereco({ trimEnds: true });
        commitEntregaObsFromDom();
        if (!enderecoEntregaMinimoOk(state)) {
            alert('Preencha logradouro e bairro (ou endereço legível) para entrega.');
            return;
        }
        if (String((state.entrega && state.entrega.localPagamento) || '') !== 'loja') {
            alert('Esta ação é para pagamento na loja. Escolha essa opção no pop-up da etapa Entrega.');
            return;
        }
        fecharModaisEntregaAntesImpressao();
        wizardModalEscolhaImpressaoEntrega().then(function (opt) {
            if (!opt) return;
            var orcId = Date.now();
            wizardImprimirPacoteEntrega(orcId, opt);
            State.setCurrentStep('pagamento');
        });
    }

    function reiniciarFluxoPagamentoEntregaUi() {
        entregaWizardAguardandoTroco = false;
        resetEntregaClienteSnapshot();
        State.setEntregaPatch({
            localPagamento: '',
            meioNaEntrega: '',
            troco: '',
            taxaEntregaRespondida: false,
            taxaEntregaModo: '',
            detalhesEntregaRespondidos: false,
            enderecoPassoConcluido: false
        });
        State.setPagamentoField('frete', 0);
        State.setEntregaField('maquininha', '');
        if (dom.entregaTroco) dom.entregaTroco.value = '';
        syncEntregaDetalhesModalUi();
        scrollEntregaWizardIntoView();
    }

    function bindEvents() {
        dom.stepNavs.forEach(function (btn) {
            btn.addEventListener('click', function () {
                var step = btn.getAttribute('data-step-nav');
                var state = State.getState();
                var computed = State.getComputed();
                if (computed.flow.indexOf(step) === -1) return;
                if (flowIndex(computed.flow, step) <= flowIndex(computed.flow, state.currentStep)) {
                    State.setCurrentStep(step);
                }
            });
        });

        dom.btnPrev.addEventListener('click', function () {
            var state = State.getState();
            if (state.currentStep === 'entrega' && voltarUmPassoEntrega()) {
                return;
            }
            var computed = State.getComputed();
            var target = prevStep(state, computed);
            if (state.currentStep === 'entrega' && target === 'produtos') {
                resetEntregaModoAoVoltarProdutos();
            }
            if (target) State.setCurrentStep(target);
        });

        dom.btnNext.addEventListener('click', function () {
            var state = State.getState();
            var computed = State.getComputed();
            if (state.currentStep === 'entrega') {
                onEntregaBtnNext();
                return;
            }
            var validation = canAdvance(state, computed);
            if (validation) {
                alert(validation);
                return;
            }
            var target = nextStep(state, computed);
            if (target) State.setCurrentStep(target);
        });

        dom.quickClientChange.addEventListener('click', function () {
            openQuickClientPicker();
        });
        if (dom.quickClientEditStep1) {
            dom.quickClientEditStep1.addEventListener('click', function () {
                openStep1QuickClientEdit();
            });
        }

        function openQuickClientPickerFromHit() {
            openQuickClientPicker();
        }
        if (dom.quickClientHit) {
            dom.quickClientHit.addEventListener('click', openQuickClientPickerFromHit);
            dom.quickClientHit.addEventListener('keydown', function (event) {
                if (event.key === 'Enter' || event.key === ' ') {
                    event.preventDefault();
                    openQuickClientPickerFromHit();
                }
            });
        }

        if (dom.quickClientModalFechar) {
            dom.quickClientModalFechar.addEventListener('click', closeQuickClientPicker);
        }
        if (dom.quickClientModal) {
            dom.quickClientModal.addEventListener('click', function (event) {
                if (event.target !== dom.quickClientModal) return;
                if (isQuickClientEditOpen()) {
                    closeQuickClientEditOverlay();
                    return;
                }
                closeQuickClientPicker();
            });
        }
        if (dom.quickClientCadastrar) {
            dom.quickClientCadastrar.addEventListener('click', function () {
                openWizardQuickClientCadastro();
            });
        }
        if (dom.quickClientEditSalvar) {
            dom.quickClientEditSalvar.addEventListener('click', saveQuickClientEditOverlay);
        }
        if (dom.quickClientEditCancelar) {
            dom.quickClientEditCancelar.addEventListener('click', closeQuickClientEditOverlay);
        }
        if (dom.quickClientEditFechar) {
            dom.quickClientEditFechar.addEventListener('click', closeQuickClientEditOverlay);
        }
        if (dom.quickClientEditOverlay) {
            dom.quickClientEditOverlay.addEventListener('click', function (event) {
                if (event.target === dom.quickClientEditOverlay) closeQuickClientEditOverlay();
            });
        }
        if (dom.quickClientEditPluscode) {
            dom.quickClientEditPluscode.addEventListener('input', function () {
                scheduleQuickClientPlusGeocode(false);
            });
            dom.quickClientEditPluscode.addEventListener('blur', function () {
                scheduleQuickClientPlusGeocode(true);
            });
            dom.quickClientEditPluscode.addEventListener('keydown', function (event) {
                if (event.key === 'Enter') {
                    event.preventDefault();
                    scheduleQuickClientPlusGeocode(true);
                }
            });
        }

        if (dom.wizardCliRapidoSalvar) {
            dom.wizardCliRapidoSalvar.addEventListener('click', saveWizardQuickClientCadastro);
        }
        if (dom.wizardCliRapidoCancelar) {
            dom.wizardCliRapidoCancelar.addEventListener('click', closeWizardQuickClientCadastro);
        }
        if (dom.wizardCliRapidoFechar) {
            dom.wizardCliRapidoFechar.addEventListener('click', closeWizardQuickClientCadastro);
        }
        if (dom.wizardCliRapidoModal) {
            dom.wizardCliRapidoModal.addEventListener('click', function (event) {
                if (event.target === dom.wizardCliRapidoModal) {
                    closeWizardQuickClientCadastro();
                }
            });
        }
        if (dom.wizardCliRapidoNome) {
            dom.wizardCliRapidoNome.addEventListener('keydown', function (event) {
                if (event.key === 'Enter') {
                    event.preventDefault();
                    if (dom.wizardCliRapidoWhatsapp) dom.wizardCliRapidoWhatsapp.focus();
                }
            });
        }
        if (dom.wizardCliRapidoWhatsapp) {
            dom.wizardCliRapidoWhatsapp.addEventListener('keydown', function (event) {
                if (event.key === 'Enter') {
                    event.preventDefault();
                    saveWizardQuickClientCadastro();
                }
            });
        }

        dom.quickClientSearch.addEventListener('input', function () {
            if ((dom.quickClientSearch.value || '').trim()) {
                dom.quickClientSearch.classList.add('pdv-client-search-typed');
            } else {
                dom.quickClientSearch.classList.remove('pdv-client-search-typed');
            }
            clearTimeout(searchClientTimer);
            searchClientTimer = setTimeout(function () {
                runClientSearch(dom.quickClientSearch.value);
            }, 180);
        });

        dom.quickClientSearch.addEventListener('keydown', function (event) {
            var vis = dom.quickClientResults && !dom.quickClientResults.classList.contains('hidden');
            var clientes = dom.quickClientResults._clientes || [];
            if (vis && clientes.length && (event.key === 'ArrowDown' || event.key === 'ArrowUp')) {
                event.preventDefault();
                if (clientListSelectIdx < 0) clientListSelectIdx = 0;
                else if (event.key === 'ArrowDown') {
                    clientListSelectIdx = Math.min(clientListSelectIdx + 1, clientes.length - 1);
                } else {
                    clientListSelectIdx = Math.max(clientListSelectIdx - 1, 0);
                }
                highlightClientListRow();
                return;
            }
            if (vis && clientes.length && event.key === 'Enter') {
                event.preventDefault();
                if (clientListSelectIdx >= 0 && clientListSelectIdx < clientes.length) {
                    State.setCliente(clientes[clientListSelectIdx], 'cliente');
                    syncEntregaEnderecoFromCliente();
                    refreshCreditoFiadoCliente(null, { force: true, showVencidosAlert: true });
                    closeQuickClientPicker();
                }
            }
        });

        dom.quickClientResults.addEventListener('click', function (event) {
            if (event.target.closest('[data-wizard-cadastrar-cliente]')) {
                openWizardQuickClientCadastro();
                return;
            }
            var editBtn = event.target.closest('[data-edit-client]');
            if (editBtn) {
                event.preventDefault();
                event.stopPropagation();
                var editIdx = parseInt(editBtn.getAttribute('data-client-list-idx') || '-1', 10);
                var clientesEdit = dom.quickClientResults._clientes || [];
                var clienteEdit = editIdx >= 0 ? clientesEdit[editIdx] : null;
                if (clienteEdit) openQuickClientEditOverlay(clienteEdit, editIdx);
                return;
            }
            var btn = event.target.closest('[data-select-client]');
            if (!btn) return;
            var idxAttr = btn.getAttribute('data-client-list-idx');
            if (idxAttr != null && idxAttr !== '') {
                clientListSelectIdx = parseInt(idxAttr, 10);
            }
            var id = btn.getAttribute('data-select-client');
            var clientes = dom.quickClientResults._clientes || [];
            var cliente = clientes.find(function (item) { return String(item.id) === String(id); });
            if (!cliente) return;
            State.setCliente(cliente, 'cliente');
            syncEntregaEnderecoFromCliente();
            refreshCreditoFiadoCliente(null, { force: true, showVencidosAlert: true });
            closeQuickClientPicker();
        });

        dom.productSearch.addEventListener('keydown', function (event) {
            if (event.key === 'Escape') {
                if (dom.productAutocomplete && !dom.productAutocomplete.classList.contains('hidden')) {
                    event.preventDefault();
                    dismissProductAutocomplete();
                    return;
                }
            }
            if (event.key === 'ArrowDown') {
                if (!lastProducts.length) return;
                event.preventDefault();
                var acCap = Math.min(lastProducts.length, autocompleteVisibleLimit);
                productSelectionIndex = Math.min(productSelectionIndex + 1, Math.max(acCap - 1, 0));
                renderProductResults(lastProducts, { preserveLimit: true });
            } else if (event.key === 'ArrowUp') {
                if (!lastProducts.length) return;
                event.preventDefault();
                productSelectionIndex = Math.max(productSelectionIndex - 1, 0);
                renderProductResults(lastProducts, { preserveLimit: true });
            } else if (event.key === 'Enter') {
                event.preventDefault();
                clearTimeout(barcodeTimer);
                clearTimeout(searchTimer);
                var qEnter = String(dom.productSearch.value || '').trim();
                if (qEnter) marcarWizardScannerAtivo(1500);
                var pick = resolveEnterProductPick(qEnter);
                if (pick) {
                    tryAddProductFromSearch(pick, {
                        query: productRowLookupCode(pick) || qEnter,
                        explicitPick: true,
                    });
                    return;
                }
                if (/^\d{8,}$/.test(qEnter) || pareceCodigoGmWizard(qEnter)) {
                    runProductSearch(qEnter, 'barcode');
                    return;
                }
                if (reopenBudgetFromBarcode(qEnter)) return;
                if (qEnter) {
                    runProductSearch(qEnter, 'manual');
                }
            } else if (event.key === '+' || event.key === '=' || event.code === 'NumpadAdd') {
                if (deveIgnorarAtalhoQtyBusca(dom.productSearch.value)) return;
                event.preventDefault();
                bumpLastCartItem(1);
            } else if (event.key === '-' || event.code === 'NumpadSubtract') {
                if (deveIgnorarAtalhoQtyBusca(dom.productSearch.value)) return;
                event.preventDefault();
                bumpLastCartItem(-1);
            }
        });

        dom.productSearch.addEventListener('input', function () {
            var value = String(dom.productSearch.value || '');
            var trimmed = value.trim();
            var now = Date.now();
            var delta = now - lastInputAt;
            lastInputAt = now;
            clearTimeout(searchTimer);
            clearTimeout(barcodeTimer);
            if (reopenBudgetFromBarcode(trimmed)) return;
            if (pareceLeituraCodigoWizard(trimmed) && (delta < 45 || trimmed.length >= 8)) {
                marcarWizardScannerAtivo(1500);
            }
            if (/^\d{8,}$/.test(trimmed)) {
                var waitMs = trimmed.length >= 13 ? 12 : 40;
                barcodeTimer = setTimeout(function () {
                    runProductSearch(trimmed, 'barcode');
                }, waitMs);
                return;
            }
            if (/^\d{6,7}$/.test(trimmed) && delta < 40) {
                barcodeTimer = setTimeout(function () {
                    runProductSearch(trimmed, 'barcode');
                }, 35);
                return;
            }
            if (pareceCodigoGmWizard(trimmed)) {
                barcodeTimer = setTimeout(function () {
                    runProductSearch(trimmed, 'barcode');
                }, 200);
                return;
            }
            searchTimer = setTimeout(function () {
                runProductSearch(value, 'manual');
            }, 220);
        });

        dom.productSearch.addEventListener('blur', function (event) {
            if (shouldSuppressProductAutocompleteDismiss()) return;
            var related = event.relatedTarget;
            if (related && isInsideProductSearchZone(related)) return;
            window.setTimeout(function () {
                if (shouldSuppressProductAutocompleteDismiss()) return;
                if (!dom.productAutocomplete || dom.productAutocomplete.classList.contains('hidden')) return;
                var ae = document.activeElement;
                if (ae && isInsideProductSearchZone(ae)) return;
                dismissProductAutocomplete();
            }, 120);
        });

        if (dom.productSearchWrap) {
            dom.productSearchWrap.addEventListener('mousedown', markProductSearchPointerInside, true);
        }

        document.addEventListener('mousedown', function (event) {
            if (shouldSuppressProductAutocompleteDismiss()) return;
            if (isInsideProductSearchZone(event.target)) return;
            dismissProductAutocomplete();
        });

        if (dom.productAutocomplete) {
            dom.productAutocomplete.addEventListener('mousedown', function (event) {
                if (event.target.closest('[data-autocomplete-load-more]')) {
                    event.preventDefault();
                    event.stopPropagation();
                    markProductSearchPointerInside();
                    return;
                }
                markProductSearchPointerInside();
                var zoom = event.target.closest('[data-pdv-photo-zoom]');
                if (zoom) return;
                var btn = event.target.closest('[data-add-product]');
                if (!btn) return;
                event.preventDefault();
                var idx = parseInt(btn.getAttribute('data-autocomplete-index') || '-1', 10);
                if (idx < 0 || idx >= lastProducts.length) return;
                tryAddProductFromSearch(lastProducts[idx], {
                    query: productRowLookupCode(lastProducts[idx])
                        || (dom.productSearch ? dom.productSearch.value : ''),
                    explicitPick: true,
                });
            });
            dom.productAutocomplete.addEventListener('click', function (event) {
                var loadMore = event.target.closest('[data-autocomplete-load-more]');
                if (loadMore) {
                    event.preventDefault();
                    event.stopPropagation();
                    markProductSearchPointerInside();
                    if (
                        productSearchAwaitingServer &&
                        lastProducts.length <= autocompleteVisibleLimit
                    ) {
                        return;
                    }
                    expandProductAutocomplete();
                    if (dom.productSearch) dom.productSearch.focus();
                    return;
                }
                var zoom = event.target.closest('[data-pdv-photo-zoom]');
                if (zoom) {
                    event.preventDefault();
                    event.stopPropagation();
                    openProductPhotoPop(zoom.getAttribute('data-pdv-photo-zoom') || '');
                }
            });
        }

        dom.productCartList.addEventListener('keydown', function (event) {
            var zEl = event.target.closest('[data-pdv-photo-zoom]');
            if (zEl && (event.key === 'Enter' || event.key === ' ')) {
                event.preventDefault();
                openProductPhotoPop(zEl.getAttribute('data-pdv-photo-zoom') || '');
            }
        });
        dom.productCartList.addEventListener('click', function (event) {
            var zoomC = event.target.closest('[data-pdv-photo-zoom]');
            if (zoomC) {
                event.preventDefault();
                openProductPhotoPop(zoomC.getAttribute('data-pdv-photo-zoom') || '');
                return;
            }
            var removeBtn = event.target.closest('[data-remove-item]');
            if (removeBtn) {
                State.removeItem(removeBtn.getAttribute('data-remove-item'));
                return;
            }
            var qtyBtn = event.target.closest('[data-item-qty]');
            if (qtyBtn) {
                var id = qtyBtn.getAttribute('data-item-qty');
                var delta = parseInt(qtyBtn.getAttribute('data-item-delta') || '0', 10);
                applyQtyDelta(id, delta > 0 ? 1 : -1);
            }
        });

        dom.productCartList.addEventListener('focusin', function (event) {
            var priceInput = event.target.closest('[data-item-price-input]');
            if (priceInput) {
                var pid = priceInput.getAttribute('data-item-price-input');
                var pItem = State.getState().itens.find(function (item) {
                    return String(item.id) === String(pid);
                });
                var unitRaw = pItem ? formatPriceEdit(pItem.preco) : priceInput.value;
                priceInput.value = unitRaw;
                priceInput.setAttribute('aria-label', 'Preço unitário');
                priceInput.title = 'Altere o preço unitário deste item';
                priceEditDraft = { id: pid, raw: unitRaw };
                setTimeout(function () {
                    try {
                        priceInput.select();
                    } catch (errSelP) {}
                }, 0);
                return;
            }
            var input = event.target.closest('[data-item-qty-input]');
            if (!input) return;
            qtyEditDraft = { id: input.getAttribute('data-item-qty-input'), raw: input.value };
            setTimeout(function () {
                try {
                    input.select();
                } catch (errSel) {}
            }, 0);
        });

        dom.productCartList.addEventListener('input', function (event) {
            var priceInput = event.target.closest('[data-item-price-input]');
            if (priceInput) {
                var pid = priceInput.getAttribute('data-item-price-input');
                priceEditDraft = { id: pid, raw: priceInput.value };
                priceInputRestore = {
                    id: pid,
                    selStart: priceInput.selectionStart,
                    selEnd: priceInput.selectionEnd
                };
                return;
            }
            var input = event.target.closest('[data-item-qty-input]');
            if (!input) return;
            var id = input.getAttribute('data-item-qty-input');
            qtyEditDraft = { id: id, raw: input.value };
            qtyInputRestore = {
                id: id,
                selStart: input.selectionStart,
                selEnd: input.selectionEnd
            };
        });

        dom.productCartList.addEventListener('keydown', function (event) {
            var priceInput = event.target.closest('[data-item-price-input]');
            if (priceInput) {
                if (event.key === 'Enter') {
                    event.preventDefault();
                    commitPriceInput(priceInput);
                    priceInput.blur();
                    if (dom.productSearch) dom.productSearch.focus();
                } else if (event.key === 'Escape') {
                    event.preventDefault();
                    restorePriceInputDisplay(priceInput);
                    priceEditDraft = { id: null, raw: '' };
                    priceInputRestore = { id: null, selStart: null, selEnd: null };
                    priceSkipCommitOnce = true;
                    priceInput.blur();
                }
                return;
            }
            var input = event.target.closest('[data-item-qty-input]');
            if (!input) return;
            if (event.key === 'Enter') {
                event.preventDefault();
                commitQtyInput(input);
                input.blur();
                if (dom.productSearch) dom.productSearch.focus();
            } else if (event.key === 'Escape') {
                event.preventDefault();
                var escId = input.getAttribute('data-item-qty-input');
                var escItem = State.getState().itens.find(function (item) {
                    return String(item.id) === String(escId);
                });
                if (escItem) input.value = formatQty(escItem.qtd);
                qtyEditDraft = { id: null, raw: '' };
                qtyInputRestore = { id: null, selStart: null, selEnd: null };
                qtySkipCommitOnce = true;
                input.blur();
            }
        });

        dom.productCartList.addEventListener('focusout', function (event) {
            var priceInput = event.target.closest('[data-item-price-input]');
            if (priceInput) {
                if (priceSkipCommitOnce) {
                    priceSkipCommitOnce = false;
                    return;
                }
                commitPriceInput(priceInput);
                return;
            }
            var input = event.target.closest('[data-item-qty-input]');
            if (!input) return;
            if (qtySkipCommitOnce) {
                qtySkipCommitOnce = false;
                return;
            }
            commitQtyInput(input);
        });

        dom.clearItems.addEventListener('click', function () {
            if (!State.getState().itens.length) return;
            if (confirm('Limpar todos os itens desta venda?')) State.clearItems();
        });

        if (dom.step1Advance) {
            dom.step1Advance.addEventListener('click', function () {
                var state = State.getState();
                var computed = State.getComputed();
                var validation = canAdvance(state, computed);
                if (validation) {
                    alert(validation);
                    return;
                }
                prepararEntregaAoSairDeProdutos();
                var target = nextStep(state, computed);
                if (target) State.setCurrentStep(target);
            });
        }

        if (dom.step1Payment) {
            dom.step1Payment.addEventListener('click', irParaPagamentoFromProdutos);
        }

        if (dom.openBudgetHistory) dom.openBudgetHistory.addEventListener('click', openBudgetHistory);
        if (dom.step1BudgetVerMais) dom.step1BudgetVerMais.addEventListener('click', openBudgetHistory);
        if (dom.step1SalvarOrcamentoBtn) {
            dom.step1SalvarOrcamentoBtn.addEventListener('click', salvarOrcamentoWizard);
        }
        if (dom.topbarEntregasBtn) {
            dom.topbarEntregasBtn.addEventListener('click', openEntregasPendentesModal);
        }
        if (dom.fiadoGestaoOpen) {
            dom.fiadoGestaoOpen.addEventListener('click', openFiadoGestao);
        }
        if (dom.topbarFiadoLink) {
            dom.topbarFiadoLink.addEventListener('click', function (ev) {
                ev.preventDefault();
                openFiadoGestao();
            });
        }
        if (dom.topbarCaixaLink) {
            dom.topbarCaixaLink.addEventListener('click', function (ev) {
                ev.preventDefault();
                navegarAgroInApp(dom.topbarCaixaLink.href);
            });
        }
        document.querySelectorAll('#pdv-topbar-compact a[href*="vendas"]').forEach(function (link) {
            link.addEventListener('click', function (ev) {
                ev.preventDefault();
                navegarAgroInApp(link.href);
            });
        });
        if (dom.fiadoVencidosGestao) {
            dom.fiadoVencidosGestao.addEventListener('click', function (ev) {
                ev.preventDefault();
                openFiadoGestao();
            });
        }
        if (dom.fiadoVencidosFechar) {
            dom.fiadoVencidosFechar.addEventListener('click', closeFiadoVencidosModal);
        }
        if (dom.fiadoVencidosModal) {
            dom.fiadoVencidosModal.addEventListener('click', function (ev) {
                if (ev.target === dom.fiadoVencidosModal) closeFiadoVencidosModal();
            });
        }
        if (dom.entregasPendentesClose) {
            dom.entregasPendentesClose.addEventListener('click', closeEntregasPendentesModal);
        }
        if (dom.entregasPendentesModal) {
            dom.entregasPendentesModal.addEventListener('click', function (ev) {
                if (ev.target === dom.entregasPendentesModal) closeEntregasPendentesModal();
            });
        }
        dom.budgetHistoryClose.addEventListener('click', closeBudgetHistory);
        dom.budgetHistoryModal.addEventListener('click', function (event) {
            if (event.target === dom.budgetHistoryModal) closeBudgetHistory();
        });
        dom.budgetHistoryList.addEventListener('click', function (event) {
            var btn = event.target.closest('[data-budget-id]');
            if (!btn) return;
            reopenBudgetById(btn.getAttribute('data-budget-id'), function (ok) {
                if (ok) closeBudgetHistory();
            });
        });
        var step1BudgetSnippet = document.getElementById('pdv-step1-budget-snippet');
        if (step1BudgetSnippet) {
            step1BudgetSnippet.addEventListener('click', function (event) {
                var row = event.target.closest('[data-budget-id]');
                if (!row) return;
                reopenBudgetById(row.getAttribute('data-budget-id'));
            });
        }

        if (dom.clientPurchaseHistory) {
            dom.clientPurchaseHistory.addEventListener('click', function () {
                if (window.AgroPdvRelacionamento && typeof window.AgroPdvRelacionamento.open === 'function') {
                    window.AgroPdvRelacionamento.open();
                    return;
                }
                var state = State.getState();
                var url = urls.vendasLista || '/vendas/';
                if (state.cliente && state.cliente.nome) {
                    url += (url.indexOf('?') === -1 ? '?' : '&') + 'cliente=' + encodeURIComponent(state.cliente.nome);
                }
                window.open(url, '_blank', 'noopener,noreferrer');
            });
        }

        dom.startSearchClient.addEventListener('click', function () {
            closeStartModal();
            openQuickClientPicker();
        });

        dom.startConsumidorFinal.addEventListener('click', function () {
            State.setConsumidorFinal(bootstrap.clientePadraoNome);
            closeStartModal();
            setTimeout(focusProductSearch, 30);
        });

        dom.clienteTelefone.addEventListener('input', function () {
            var state = State.getState();
            if (!state.cliente) return;
            state.cliente.telefone = dom.clienteTelefone.value;
            State.setCliente(state.cliente, state.clienteMode === 'consumidor_final' ? 'consumidor_final' : 'cliente');
        });

        function commitClienteEditCampos() {
            var state = State.getState();
            if (!state.cliente) return;
            var cpfCheck = pdvValidarCpfOpcional(dom.clienteCpf ? dom.clienteCpf.value : '');
            var cpfNorm = cpfCheck.ok ? cpfCheck.cpf : nfceNormalizarCpf(dom.clienteCpf ? dom.clienteCpf.value : '');
            var c = Object.assign({}, state.cliente, {
                logradouro: dom.clienteLogradouro ? dom.clienteLogradouro.value.trim() : '',
                numero: dom.clienteNumero ? dom.clienteNumero.value.trim() : '',
                bairro: dom.clienteBairro ? dom.clienteBairro.value : '',
                plus_code: dom.clientePluscode ? dom.clientePluscode.value.trim() : '',
                cpf: cpfNorm,
                documento: cpfNorm || (state.cliente.documento === '—' ? '—' : state.cliente.documento || '')
            });
            c.endereco = composeClienteEnderecoLinha(c);
            State.setCliente(c, state.clienteMode === 'consumidor_final' ? 'consumidor_final' : 'cliente');
            syncEntregaEnderecoFromCliente(State.getState());
            if (dom.step2ClientDoc) {
                dom.step2ClientDoc.textContent = cpfNorm
                    ? pdvFormatCpfInput(cpfNorm)
                    : 'Sem documento informado';
            }
        }

        function bindPdvCpfInputMask(el, onInput) {
            if (!el) return;
            el.addEventListener('input', function () {
                var pos = el.selectionStart;
                var prevLen = el.value.length;
                el.value = pdvFormatCpfInput(el.value);
                var delta = el.value.length - prevLen;
                try {
                    el.setSelectionRange(Math.max(0, (pos || 0) + delta), Math.max(0, (pos || 0) + delta));
                } catch (eMask) {}
                if (onInput) onInput();
            });
        }

        [dom.clienteLogradouro, dom.clienteNumero, dom.clientePluscode].forEach(function (el) {
            if (el) el.addEventListener('input', commitClienteEditCampos);
        });
        bindPdvCpfInputMask(dom.clienteCpf, commitClienteEditCampos);
        bindPdvCpfInputMask(dom.quickClientEditCpf);
        bindPdvCpfInputMask(dom.wizardCliRapidoCpf);
        if (dom.clienteBairro) dom.clienteBairro.addEventListener('change', commitClienteEditCampos);

        if (dom.step2OpenClienteEdit) {
            dom.step2OpenClienteEdit.addEventListener('click', openClienteEditModal);
        }
        if (dom.clienteEditClose) {
            dom.clienteEditClose.addEventListener('click', closeClienteEditModal);
        }
        if (dom.clienteEditModal) {
            dom.clienteEditModal.addEventListener('click', function (event) {
                if (event.target === dom.clienteEditModal) closeClienteEditModal();
            });
        }
        if (dom.clienteAdvancedEdit) {
            dom.clienteAdvancedEdit.addEventListener('click', function () {
                var state = State.getState();
                var cliente = state.cliente || {};
                if (cliente.cliente_agro_pk && urls.clienteEditarPattern) {
                    window.open(
                        urls.clienteEditarPattern.replace('__pk__', String(cliente.cliente_agro_pk)),
                        '_blank',
                        'noopener,noreferrer'
                    );
                } else {
                    window.open(urls.clientesLista || urls.clienteNovo || '/', '_blank', 'noopener,noreferrer');
                }
            });
        }

        [dom.entregaLogradouro, dom.entregaNumero, dom.entregaPluscode].forEach(function (el) {
            if (el) {
                el.addEventListener('input', function () {
                    marcarEntregaEnderecoEditadoPeloUsuario();
                    commitEntregaCamposEndereco();
                });
                el.addEventListener('blur', function () {
                    commitEntregaCamposEndereco({ trimEnds: true });
                });
            }
        });
        if (dom.entregaBairro) {
            dom.entregaBairro.addEventListener('change', function () {
                marcarEntregaEnderecoEditadoPeloUsuario();
                commitEntregaCamposEndereco();
            });
            dom.entregaBairro.addEventListener('input', function () {
                marcarEntregaEnderecoEditadoPeloUsuario();
                commitEntregaCamposEndereco();
            });
        }
        if (dom.entregaPluscode) {
            dom.entregaPluscode.addEventListener('input', function () {
                marcarEntregaEnderecoEditadoPeloUsuario();
                commitEntregaCamposEndereco();
                scheduleEntregaPlusGeocode(false);
            });
            dom.entregaPluscode.addEventListener('blur', function () {
                scheduleEntregaPlusGeocode(true);
            });
            dom.entregaPluscode.addEventListener('keydown', function (ev) {
                if (ev.key === 'Enter') {
                    ev.preventDefault();
                    scheduleEntregaPlusGeocode(true);
                }
            });
        }
        if (dom.entregaClienteNome) {
            dom.entregaClienteNome.addEventListener('input', function () {
                marcarEntregaEnderecoEditadoPeloUsuario();
                commitEntregaClienteCamposFromDom();
            });
        }
        if (dom.entregaClienteTelefone) {
            dom.entregaClienteTelefone.addEventListener('input', function () {
                marcarEntregaEnderecoEditadoPeloUsuario();
                commitEntregaClienteCamposFromDom();
                renderEntregaClienteCampos(State.getState());
            });
        }

        var btnEscSalvar = document.getElementById('pdv-esc-salvar-cadastro');
        if (btnEscSalvar) {
            btnEscSalvar.addEventListener('click', function () {
                finalizarEscolhaSalvarCliente(true);
            });
        }
        var btnEscSoEntrega = document.getElementById('pdv-esc-so-entrega');
        if (btnEscSoEntrega) {
            btnEscSoEntrega.addEventListener('click', function () {
                finalizarEscolhaSalvarCliente(false);
            });
        }
        var btnEscCancelar = document.getElementById('pdv-esc-cancelar');
        if (btnEscCancelar) {
            btnEscCancelar.addEventListener('click', function () {
                entregaPendingAfterSaveCliente = null;
                closeEntregaSalvarClienteModal();
            });
        }
        var mdEscModal = document.getElementById('modal-pdv-entrega-salvar-cliente');
        if (mdEscModal) {
            mdEscModal.addEventListener('click', function (ev) {
                if (ev.target === mdEscModal) {
                    entregaPendingAfterSaveCliente = null;
                    closeEntregaSalvarClienteModal();
                }
            });
        }

        [
            [dom.vendaObservacao, function () { State.setVendaField('observacao', dom.vendaObservacao.value); }],
            [dom.entregaComplemento, function () {
                marcarEntregaEnderecoEditadoPeloUsuario();
                State.setEntregaField('complemento', dom.entregaComplemento.value);
            }],
            [dom.entregaReferencia, function () {
                marcarEntregaEnderecoEditadoPeloUsuario();
                State.setEntregaField('referencia', dom.entregaReferencia.value);
            }],
            [
                dom.entregaHorario,
                function () {
                    State.setEntregaField('horario', dom.entregaHorario.value);
                    wizardSyncLembretesFromEntregaHorario();
                }
            ],
            [dom.entregaTroco, function () { State.setEntregaField('troco', dom.entregaTroco.value); }],
            [dom.entregaObservacao, function () { State.setEntregaField('observacao', dom.entregaObservacao.value); }],
            [
                document.getElementById('pdv-resumo-venda-observacao'),
                function () {
                    var el = document.getElementById('pdv-resumo-venda-observacao');
                    if (el) State.setVendaField('observacao', el.value);
                }
            ],
            [
                document.getElementById('pdv-resumo-entrega-observacao'),
                function () {
                    var el = document.getElementById('pdv-resumo-entrega-observacao');
                    if (el) State.setEntregaField('observacao', el.value);
                }
            ],
            [dom.paymentChange, function () { State.setPagamentoField('trocoCalculado', dom.paymentChange.value); }]
        ].forEach(function (entry) {
            if (entry[0]) entry[0].addEventListener('input', entry[1]);
            if (entry[0] && entry[0].tagName === 'SELECT') entry[0].addEventListener('change', entry[1]);
        });

        bindMoneyInputField(dom.paymentDiscount, function (raw) {
            State.setPagamentoField('descontoGeral', raw);
        });
        bindMoneyInputField(dom.paymentShipping, function (raw) {
            State.setPagamentoField('frete', raw);
        });
        bindMoneyInputField(dom.paymentValorForma, function (raw) {
            State.setPagamentoField('valorDestaForma', raw);
        });
        if (dom.paymentReceived) {
            bindMoneyInputField(dom.paymentReceived, function (raw) {
                syncDinheiroRecebidoValue(raw, true);
            });
        }
        var dinPopRecBind = document.getElementById('pdv-pay-pop-din-recebido');
        if (dinPopRecBind) {
            bindMoneyInputField(dinPopRecBind, function (raw) {
                syncDinheiroRecebidoValue(raw, false);
            });
        }

        if (dom.paymentMethod) {
            dom.paymentMethod.addEventListener('change', function () {
                var v = dom.paymentMethod.value;
                selectPaymentForma(v);
                if (requiresMaquina(v)) {
                    openMaquinasDialog();
                } else {
                    focusFirstFlowFieldForForma(v);
                }
            });
        }

        if (dom.btnOpenPaymentForma) {
            dom.btnOpenPaymentForma.addEventListener('click', openPaymentFormaModal);
        }
        if (dom.btnTrocarPaymentForma) {
            dom.btnTrocarPaymentForma.addEventListener('click', openPaymentFormaModal);
        }
        if (dom.btnConfirmDiscount) {
            dom.btnConfirmDiscount.addEventListener('click', confirmDiscountAndOpenFormas);
        }
        if (dom.btnFormaGotoDesconto) {
            dom.btnFormaGotoDesconto.addEventListener('click', focusDiscountField);
        }
        if (dom.btnPopConfirmDiscount) {
            dom.btnPopConfirmDiscount.addEventListener('click', confirmDiscountAndOpenFormas);
        }
        if (dom.paymentDiscount) {
            dom.paymentDiscount.addEventListener('keydown', function (ev) {
                if (ev.key !== 'Enter') return;
                ev.preventDefault();
                confirmDiscountAndOpenFormas();
            });
        }
        if (dom.paymentFormaModalClose) {
            dom.paymentFormaModalClose.addEventListener('click', closePaymentFormaModal);
        }
        if (dom.paymentFormaModalBackdrop) {
            dom.paymentFormaModalBackdrop.addEventListener('click', closePaymentFormaModal);
        }
        dom.paymentModalCards.forEach(function (btn) {
            btn.addEventListener('click', function () {
                choosePaymentFormaFromModal(btn.getAttribute('data-payment-modal-card') || '');
            });
        });

        if (dom.paymentValorForma) {
            dom.paymentValorForma.addEventListener('keydown', handleValorTrancheEnter);
        }
        if (dom.payCommitTranche) {
            dom.payCommitTranche.addEventListener('click', function () {
                runCommitTrancheFromInput();
            });
        }

        if (dom.paymentParcelasCredito) {
            dom.paymentParcelasCredito.addEventListener('input', function () {
                var n = parseInt(dom.paymentParcelasCredito.value, 10);
                State.setPagamentoField('creditoParcelas', Number.isFinite(n) && n >= 2 ? n : 2);
            });
            dom.paymentParcelasCredito.addEventListener('keydown', focusParcelasThenTranche);
        }

        if (dom.fiadoParcelasInput) {
            dom.fiadoParcelasInput.addEventListener('input', function () {
                var n = parseInt(dom.fiadoParcelasInput.value, 10);
                if (Number.isFinite(n) && n > 6) n = 6;
                State.setPagamentoField('fiadoParcelas', Number.isFinite(n) && n >= 1 ? n : 1);
                refreshCreditoFiadoCliente(valorFiadoNosLancamentos(State.getState()), { force: true }).then(
                    function () {
                        renderProductFiadoBalance(State.getState());
                    }
                );
            });
        }
        if (dom.fiadoDiasInput) {
            dom.fiadoDiasInput.addEventListener('input', function () {
                var n = parseInt(dom.fiadoDiasInput.value, 10);
                State.setPagamentoField('fiadoDiasVencimento', Number.isFinite(n) && n >= 1 ? n : 30);
            });
        }

        if (dom.outroDetalhes) {
            dom.outroDetalhes.addEventListener('input', function () {
                State.setPagamentoField('outroDetalhes', dom.outroDetalhes.value);
            });
        }

        if (dom.outroValidarPin) {
            dom.outroValidarPin.addEventListener('click', validarPinOutro);
        }

        var btnQrPix = document.getElementById('pdv-pay-open-qr-pix');
        if (btnQrPix) btnQrPix.addEventListener('click', function () { openPayPopQr('Mercado Pago — Pix'); });
        var btnDinResumo = document.getElementById('pdv-pay-open-dinheiro-resumo');
        if (btnDinResumo) btnDinResumo.addEventListener('click', openPayPopDinheiroResumo);
        var btnFiadoPop = document.getElementById('pdv-pay-open-fiado-pop');
        if (btnFiadoPop) btnFiadoPop.addEventListener('click', openPayPopFiado);
        var btnValePop = document.getElementById('pdv-pay-open-vale-pop');
        if (btnValePop) {
            btnValePop.addEventListener('click', function () {
                openPayPopSaldo(
                    'Vale crédito',
                    formatMoney(saldoValeAtual(State.getState())),
                    'O valor usado na venda não pode passar deste saldo.'
                );
            });
        }
        var btnCbPop = document.getElementById('pdv-pay-open-cashback-pop');
        if (btnCbPop) {
            btnCbPop.addEventListener('click', function () {
                openPayPopSaldo(
                    'Cashback',
                    formatMoney(saldoCashbackAtual(State.getState())),
                    'O valor usado na venda não pode passar deste saldo.'
                );
            });
        }
        var btnOutroAjuda = document.getElementById('pdv-pay-open-outro-ajuda');
        if (btnOutroAjuda) btnOutroAjuda.addEventListener('click', openPayPopOutroHelp);

        if (dom.pixCopyKey && dom.pixSicobKey) {
            dom.pixCopyKey.addEventListener('click', function () {
                var t = (dom.pixSicobKey.textContent || '').trim();
                if (!t || t.indexOf('não cadas') !== -1) {
                    alert('Sem chave Pix para copiar — use a maquininha ou cadastre a chave no painel.');
                    return;
                }
                if (navigator.clipboard && navigator.clipboard.writeText) {
                    navigator.clipboard.writeText(t).then(function () {
                        alert('Chave copiada.');
                    }).catch(function () {
                        alert('Não foi possível copiar.');
                    });
                } else {
                    alert(t);
                }
            });
        }

        if (dom.paymentReceived) dom.paymentReceived.addEventListener('keydown', handlePaymentReceivedEnter);
        var dinPopRec = document.getElementById('pdv-pay-pop-din-recebido');
        if (dinPopRec) dinPopRec.addEventListener('keydown', handlePaymentReceivedEnter);

        if (dom.paymentLancamentosList) {
            dom.paymentLancamentosList.addEventListener('click', function (event) {
                var rm = event.target.closest('[data-pdv-remove-lanc]');
                if (rm) {
                    event.preventDefault();
                    var rmIdx = parseInt(rm.getAttribute('data-pdv-remove-lanc'), 10);
                    var stRm = State.getState();
                    var Lrm = (stRm.pagamento.lancamentos || [])[rmIdx];
                    if (Lrm && Lrm.mpPointPago) {
                        showPdvAviso(
                            'Pagamento já confirmado na maquininha. Finalize a venda ou fale com o gerente.'
                        );
                        return;
                    }
                    State.removePagamentoLancamentoAt(rm.getAttribute('data-pdv-remove-lanc'));
                    return;
                }
                var ed = event.target.closest('[data-pdv-edit-lanc]');
                if (ed) {
                    event.preventDefault();
                    var edIdx = parseInt(ed.getAttribute('data-pdv-edit-lanc'), 10);
                    var stEd = State.getState();
                    var Led = (stEd.pagamento.lancamentos || [])[edIdx];
                    if (Led && Led.mpPointPago) {
                        showPdvAviso(
                            'Pagamento já confirmado na maquininha. Finalize a venda ou fale com o gerente.'
                        );
                        return;
                    }
                    State.beginEditPagamentoLancamento(ed.getAttribute('data-pdv-edit-lanc'));
                    var stE = State.getState();
                    focusFirstFlowFieldForForma(stE.pagamento.forma);
                }
            });
        }

        var maquinasListEl = document.getElementById('pdv-pay-maquinas-list');
        if (maquinasListEl) {
            maquinasListEl.addEventListener('click', function (event) {
                var btn = event.target.closest('[data-maquina-id]');
                if (!btn) return;
                var id = btn.getAttribute('data-maquina-id') || '';
                var nome = btn.getAttribute('data-maquina-nome') || id;
                var stM = State.getState();
                var formaM = stM.pagamento.forma || '';
                finishMaquinaSelection(id, nome);
                return;
            });
        }

        var btnMpPointCancelWait = document.getElementById('pdv-mp-point-cancel-wait');
        if (btnMpPointCancelWait) {
            btnMpPointCancelWait.addEventListener('click', function () {
                if (mpPointWaitControl.cancelRequested) return;
                var oid = mpPointWaitControl.orderId;
                var abandonUrl = String(urls.apiPdvMpPointAbandon || '').trim();
                if (!oid || !abandonUrl) {
                    mpPointWaitControl.cancelRequested = true;
                    return;
                }
                btnMpPointCancelWait.disabled = true;
                setMpPointWaitStatus('Cancelando na maquininha…');
                jsonPost(abandonUrl, { order_id: oid })
                    .then(function (res) {
                        mpPointWaitControl.cancelouMaquininha = !!(res.ok && res.data && res.data.cancelou_maquininha);
                        mpPointWaitControl.cancelRequested = true;
                        if (res.ok && res.data) {
                            if (res.data.cancelou_maquininha) {
                                setMpPointWaitStatus('Cancelado no PDV e na maquininha.');
                            } else if (res.data.aviso) {
                                setMpPointWaitStatus(res.data.aviso);
                            }
                        }
                    })
                    .catch(function () {
                        mpPointWaitControl.cancelouMaquininha = false;
                        mpPointWaitControl.cancelRequested = true;
                        setMpPointWaitStatus('Falha ao cancelar — confira a maquininha.');
                    })
                    .finally(function () {
                        btnMpPointCancelWait.disabled = false;
                    });
            });
        }
        var btnMaquinaPix = document.getElementById('pdv-pay-open-maquinas-pix');
        if (btnMaquinaPix) btnMaquinaPix.addEventListener('click', openMaquinasDialog);
        var btnMaquinaCard = document.getElementById('pdv-pay-open-maquinas-card');
        if (btnMaquinaCard) btnMaquinaCard.addEventListener('click', openMaquinasDialog);
        var btnTrocarMaquina = document.getElementById('pdv-pay-trocar-maquina');
        if (btnTrocarMaquina) btnTrocarMaquina.addEventListener('click', openMaquinasDialog);

        if (dom.confirmSaleNoPrint) {
            dom.confirmSaleNoPrint.addEventListener('click', function () {
                confirmSale(false);
            });
        }
        if (dom.confirmSalePrint) {
            dom.confirmSalePrint.addEventListener('click', function () {
                confirmSale(true);
            });
        }

        initEntregaToolbarOnce();
        atualizarUiAvisoCaixa();

        document.querySelectorAll('input[name="pdv-entrega-taxa-modo"]').forEach(function (radio) {
            radio.addEventListener('change', function () {
                if (!radio.checked) return;
                commitEntregaTaxaModo(radio.value, { draft: true });
            });
        });
        var inpTaxaValor = document.getElementById('pdv-entrega-taxa-valor');
        if (inpTaxaValor) {
            inpTaxaValor.addEventListener('input', commitEntregaTaxaValorInput);
            inpTaxaValor.addEventListener('blur', commitEntregaTaxaValorInput);
        }

        var btnReiniciarFluxoPagamento = document.getElementById('pdv-entrega-reiniciar-fluxo-pagamento');
        if (btnReiniciarFluxoPagamento) {
            btnReiniciarFluxoPagamento.addEventListener('click', reiniciarFluxoPagamentoEntregaUi);
        }
        var btnReiniciarResumo = document.getElementById('pdv-entrega-reiniciar-fluxo-pagamento-resumo');
        if (btnReiniciarResumo) {
            btnReiniciarResumo.addEventListener('click', reiniciarFluxoPagamentoEntregaUi);
        }
        document.querySelectorAll('.pdv-entrega-resumo-edit').forEach(function (btn) {
            btn.addEventListener('click', function () {
                var dest = btn.getAttribute('data-pdv-edit-fase');
                if (dest) entregaIrEditarFase(dest);
            });
        });

        var btnEf1Entrega = document.getElementById('pdv-ef1-entrega');
        if (btnEf1Entrega) {
            btnEf1Entrega.addEventListener('click', function () {
                entregaWizardAguardandoTroco = false;
                resetEntregaClienteSnapshot();
                State.setEntregaPatch({
                    localPagamento: 'entrega',
                    meioNaEntrega: '',
                    troco: '',
                    taxaEntregaRespondida: false,
                    taxaEntregaModo: '',
                    detalhesEntregaRespondidos: false,
                    enderecoPassoConcluido: false
                });
                State.setPagamentoField('frete', 0);
                syncEntregaDetalhesModalUi();
            });
        }
        var btnEf1Loja = document.getElementById('pdv-ef1-loja');
        if (btnEf1Loja) {
            btnEf1Loja.addEventListener('click', function () {
                entregaWizardAguardandoTroco = false;
                resetEntregaClienteSnapshot();
                State.setEntregaPatch({
                    localPagamento: 'loja',
                    meioNaEntrega: '',
                    troco: '',
                    taxaEntregaRespondida: false,
                    taxaEntregaModo: '',
                    detalhesEntregaRespondidos: false,
                    enderecoPassoConcluido: false
                });
                State.setPagamentoField('frete', 0);
                State.setEntregaField('maquininha', '');
                syncEntregaDetalhesModalUi();
            });
        }
        var btnEf2Din = document.getElementById('pdv-ef2-dinheiro');
        if (btnEf2Din) {
            btnEf2Din.addEventListener('click', function () {
                entregaWizardAguardandoTroco = true;
                var inpTroco = document.getElementById('pdv-ef3-troco-input');
                var stTroco = State.getState();
                if (inpTroco) {
                    inpTroco.value = String((stTroco.entrega && stTroco.entrega.troco) || '').trim();
                }
                syncEntregaDetalhesModalUi();
                if (inpTroco) {
                    setTimeout(function () {
                        try {
                            inpTroco.focus();
                            inpTroco.select();
                        } catch (eTroco) {}
                    }, 80);
                }
            });
        }
        var btnEf2Card = document.getElementById('pdv-ef2-cartao');
        if (btnEf2Card) {
            btnEf2Card.addEventListener('click', function () {
                entregaWizardAguardandoTroco = false;
                State.setEntregaPatch({ localPagamento: 'entrega', meioNaEntrega: 'cartao' });
                State.setEntregaField('maquininha', 'sim');
                syncEntregaDetalhesModalUi();
            });
        }
        var btnEf3Ok = document.getElementById('pdv-ef3-ok');
        if (btnEf3Ok) {
            btnEf3Ok.addEventListener('click', confirmarEntregaTrocoModal);
        }
        var inpEf3Troco = document.getElementById('pdv-ef3-troco-input');
        if (inpEf3Troco && btnEf3Ok) {
            inpEf3Troco.addEventListener('keydown', function (ev) {
                if (ev.key === 'Enter') {
                    ev.preventDefault();
                    btnEf3Ok.click();
                }
            });
        }

        document.addEventListener('keydown', function (event) {
            var inField = event.target && event.target.closest && event.target.closest('input,textarea,select');
            if (dom.modalStart && !dom.modalStart.classList.contains('hidden') && !event.altKey && !event.ctrlKey && !event.metaKey) {
                if (event.code === 'Enter' && dom.startConsumidorFinal) {
                    event.preventDefault();
                    dom.startConsumidorFinal.click();
                    return;
                }
                if (event.code === 'F2' && dom.startSearchClient) {
                    event.preventDefault();
                    dom.startSearchClient.click();
                    return;
                }
            }
            if (event.key === 'Escape') {
                if (isEntregaSalvarClienteModalOpen()) {
                    event.preventDefault();
                    entregaPendingAfterSaveCliente = null;
                    closeEntregaSalvarClienteModal();
                    return;
                }
                if (isEntregaDetalhesModalOpen()) {
                    event.preventDefault();
                    var painelEsc = entregaWizardPainelAtual();
                    if (painelEsc === 'troco') {
                        entregaWizardAguardandoTroco = false;
                        syncEntregaDetalhesModalUi();
                        return;
                    }
                    if (painelEsc === 'meio') {
                        State.setEntregaPatch({ meioNaEntrega: '', troco: '' });
                        entregaWizardAguardandoTroco = false;
                        syncEntregaDetalhesModalUi();
                        return;
                    }
                    if (painelEsc === 'detalhes') {
                        State.setEntregaPatch({
                            taxaEntregaRespondida: false,
                            taxaEntregaModo: '',
                            detalhesEntregaRespondidos: false,
                            enderecoPassoConcluido: false
                        });
                        State.setPagamentoField('frete', 0);
                        syncEntregaDetalhesModalUi();
                        return;
                    }
                    if (painelEsc === 'pagamento_local') {
                        resetEntregaModoAoVoltarProdutos();
                        State.setCurrentStep('produtos');
                        return;
                    }
                }
                if (isClienteEditModalOpen()) {
                    event.preventDefault();
                    closeClienteEditModal();
                    return;
                }
                if (
                    dom.wizardCliRapidoModal &&
                    !dom.wizardCliRapidoModal.classList.contains('hidden')
                ) {
                    event.preventDefault();
                    closeWizardQuickClientCadastro();
                    return;
                }
                if (isQuickClientEditOpen()) {
                    event.preventDefault();
                    closeQuickClientEditOverlay();
                    return;
                }
                if (dom.budgetHistoryModal && !dom.budgetHistoryModal.classList.contains('hidden')) {
                    event.preventDefault();
                    closeBudgetHistory();
                    return;
                }
                if (isQuickClientModalOpen()) {
                    event.preventDefault();
                    closeQuickClientPicker();
                    return;
                }
                if (dom.modalStart && !dom.modalStart.classList.contains('hidden')) {
                    event.preventDefault();
                    closeStartModal();
                    return;
                }
            }
            var stProdutos = State.getState();
            var startModalOpen = dom.modalStart && !dom.modalStart.classList.contains('hidden');
            var mdEntregaImp = document.getElementById('modal-pdv-entrega-impressao');
            var modalEntregaImpOpen = mdEntregaImp && !mdEntregaImp.classList.contains('hidden');
            if (
                stProdutos.currentStep === 'entrega' &&
                event.code === 'F1' &&
                !event.altKey &&
                !event.ctrlKey &&
                !event.metaKey &&
                !modalEntregaImpOpen
            ) {
                event.preventDefault();
                voltarUmPassoEntrega();
                return;
            }
            if (
                stProdutos.currentStep === 'produtos' &&
                !payFlowDialogOpen() &&
                !isPaymentFormaModalOpen() &&
                !startModalOpen
            ) {
                var pickerOpen = isQuickClientModalOpen();
                if (event.code === 'F2' && !event.altKey && !event.ctrlKey && !event.metaKey) {
                    event.preventDefault();
                    focusProductSearch();
                    return;
                }
                if (event.code === 'F3' && !pickerOpen && !event.altKey && !event.ctrlKey && !event.metaKey) {
                    event.preventDefault();
                    if (dom.step1Advance) dom.step1Advance.click();
                    return;
                }
                if (event.code === 'F4' && !event.altKey && !event.ctrlKey && !event.metaKey) {
                    if (wizardScannerTeclasBloqueadas()) return;
                    if (
                        dom.productSearch &&
                        document.activeElement === dom.productSearch &&
                        String(dom.productSearch.value || '').trim()
                    ) {
                        return;
                    }
                    event.preventDefault();
                    if (dom.quickClientChange) dom.quickClientChange.click();
                    return;
                }
                if (event.code === 'F8' && !event.altKey && !event.ctrlKey && !event.metaKey) {
                    event.preventDefault();
                    if (dom.clientPurchaseHistory) dom.clientPurchaseHistory.click();
                    return;
                }
                if (event.code === 'F7' && !pickerOpen && !event.altKey && !event.ctrlKey && !event.metaKey) {
                    event.preventDefault();
                    if (dom.step1Payment) dom.step1Payment.click();
                    return;
                }
            }
            if (event.code === 'F6') {
                event.preventDefault();
                openBudgetHistory();
                return;
            }
            if (
                stProdutos.currentStep === 'entrega' &&
                !payFlowDialogOpen() &&
                !isPaymentFormaModalOpen() &&
                !startModalOpen &&
                !isClienteEditModalOpen()
            ) {
                if (event.code === 'F7' && !event.altKey && !event.ctrlKey && !event.metaKey) {
                    event.preventDefault();
                    if (dom.btnNext && dom.btnNext.style.display !== 'none') {
                        if (!dom.btnNext.disabled) dom.btnNext.click();
                        else onEntregaBtnNext();
                    }
                    return;
                }
                if (!inField && isEntregaFluxo1Open()) {
                    var d1e = event.code === 'Digit1' || event.code === 'Numpad1';
                    var d2e = event.code === 'Digit2' || event.code === 'Numpad2';
                    if (d1e) {
                        event.preventDefault();
                        var bEnt = document.getElementById('pdv-ef1-entrega');
                        if (bEnt) bEnt.click();
                        return;
                    }
                    if (d2e) {
                        event.preventDefault();
                        var bLoja = document.getElementById('pdv-ef1-loja');
                        if (bLoja) bLoja.click();
                        return;
                    }
                }
            }
            if (event.altKey && event.code === 'ArrowLeft' && !event.ctrlKey && !event.metaKey && !inField) {
                event.preventDefault();
                if (dom.btnPrev && !dom.btnPrev.disabled) dom.btnPrev.click();
                return;
            }
            if (event.ctrlKey && event.code === 'Enter' && !event.altKey && !event.metaKey && !inField) {
                event.preventDefault();
                if (dom.btnNext && dom.btnNext.style.display !== 'none' && !dom.btnNext.disabled) dom.btnNext.click();
                return;
            }
            if (event.altKey && event.code === 'Enter' && !event.ctrlKey && !event.metaKey && !inField) {
                event.preventDefault();
                if (dom.btnNext && dom.btnNext.style.display !== 'none' && !dom.btnNext.disabled) dom.btnNext.click();
                return;
            }
            if (event.altKey && !event.ctrlKey && !event.metaKey && !inField) {
                var stepByDigit = { Digit1: 'produtos', Digit2: 'entrega', Digit3: 'pagamento' };
                if (stepByDigit[event.code]) {
                    event.preventDefault();
                    tryNavigateToStep(stepByDigit[event.code]);
                    return;
                }
            }
            if (event.code === 'KeyE' && !event.altKey && !event.ctrlKey && !event.metaKey && !inField) {
                var stE = State.getState();
                if (stE.currentStep === 'entrega' && dom.entregaClienteNome) {
                    event.preventDefault();
                    dom.entregaClienteNome.focus();
                    return;
                }
            }
            var st = State.getState();
            var md = document.getElementById('pdv-pay-pop-maquinas');
            if (st.currentStep === 'pagamento' && md && md.open && !event.altKey && !event.ctrlKey && !event.metaKey) {
                var c0 = normalizeDigitKeyCode(event.code);
                var mapIdx = { Digit1: 0, Digit2: 1, Digit3: 2, Digit4: 3, Digit5: 4, Digit6: 5, Digit7: 6, Digit8: 7, Digit9: 8 };
                if (mapIdx[c0] != null) {
                    var sel = document.querySelector(
                        '#pdv-pay-maquinas-list [data-maquina-idx="' + mapIdx[c0] + '"]'
                    );
                    if (sel) {
                        event.preventDefault();
                        sel.click();
                    }
                    return;
                }
            }
            if (st.currentStep === 'pagamento' && !payFlowDialogOpen() && !isPaymentFormaModalOpen()) {
                if (event.code === 'F3') {
                    event.preventDefault();
                    openPaymentFormaModal();
                    return;
                }
                if (
                    event.key === 'Enter' &&
                    !event.ctrlKey &&
                    !event.altKey &&
                    !event.metaKey &&
                    !event.shiftKey &&
                    !inField
                ) {
                    event.preventDefault();
                    if (dom.confirmSaleNoPrint && !dom.confirmSaleNoPrint.disabled) confirmSale(false);
                    return;
                }
                if (event.code === 'F9') {
                    event.preventDefault();
                    if (dom.confirmSalePrint && !dom.confirmSalePrint.disabled) confirmSale(true);
                    return;
                }
                if (
                    event.code === 'KeyT' &&
                    !event.ctrlKey &&
                    !event.metaKey &&
                    !event.altKey &&
                    !event.target.closest('input,textarea,select')
                ) {
                    var fa = document.getElementById('pdv-payment-flow-area');
                    if (fa && !fa.classList.contains('hidden')) {
                        event.preventDefault();
                        openPaymentFormaModal();
                    }
                    return;
                }
                if (
                    event.code === 'KeyM' &&
                    !event.ctrlKey &&
                    !event.metaKey &&
                    !event.altKey &&
                    !event.target.closest('input,textarea,select')
                ) {
                    var mb = document.getElementById('pdv-pay-maquina-bar');
                    if (mb && !mb.classList.contains('hidden')) {
                        event.preventDefault();
                        openMaquinasDialog();
                    }
                    return;
                }
            }
            if (event.key === 'Escape' && isPaymentFormaModalOpen()) {
                event.preventDefault();
                closePaymentFormaModal();
                return;
            }
            if (isPaymentFormaModalOpen() && st.currentStep === 'pagamento' && !payFlowDialogOpen()) {
                if (!event.altKey && !event.ctrlKey && !event.metaKey) {
                    var sf = paymentShortcutForma(event.code);
                    if (sf) {
                        event.preventDefault();
                        choosePaymentFormaFromModal(sf);
                    }
                }
            }
        });

        var alertaOk = document.getElementById('alerta-lembrete-ok');
        if (alertaOk) {
            alertaOk.addEventListener('click', function () {
                var box = document.getElementById('alerta-lembrete');
                if (box) box.classList.add('hidden');
            });
        }
        setInterval(verificarLembretesWizardTick, 25000);
    }

    var hydratedFromConsulta = false;
    var reabrirDraftEl = document.getElementById('pdv-wizard-reabrir-draft');
    if (reabrirDraftEl && typeof State.hydrateFromSessionDraft === 'function') {
        try {
            hydratedFromConsulta = !!State.hydrateFromSessionDraft(JSON.parse(reabrirDraftEl.textContent || 'null'));
        } catch (eReab) {
            hydratedFromConsulta = false;
        }
    }

    State.subscribe(renderAll);
    bindEvents();
    window.addEventListener('resize', onAutocompleteViewportChange);
    if (window.visualViewport) {
        window.visualViewport.addEventListener('resize', onAutocompleteViewportChange);
    }
    sincronizarOperadorPdvNoState();
    window.addEventListener('gm-sspin-operador', function (ev) {
        var nome = ev && ev.detail && ev.detail.nome ? String(ev.detail.nome).trim() : '';
        if (nome) State.setPagamentoField('operadorPdv', nome);
    });
    window.addEventListener('gm-sspin-before-lock', fecharModaisPdvAntesDescanso);

    function maybeOpenEntregasFromQuery() {
        try {
            var p = new URLSearchParams(window.location.search || '');
            if (p.get('entregas') !== '1' && p.get('abrir_entregas') !== '1') return;
            setTimeout(function () {
                openEntregasPendentesModal();
            }, 800);
        } catch (_) {}
    }

    function carregarDadosSecundariosPdv() {
        if (window.AgroPdvPromocoes && urls.apiPromocoesAtivasPdv) {
            window.AgroPdvPromocoes.setApiUrl(urls.apiPromocoesAtivasPdv);
            window.AgroPdvPromocoes.carregar({ empresa: 'centro' });
        }
        loadWizardClientesCache(false);
        setTimeout(function () {
            refreshEntregasPendentesUi(true).then(function () {
                maybeOpenEntregasFromQuery();
            });
            if (entregasPendentesPollTimer) clearInterval(entregasPendentesPollTimer);
            entregasPendentesPollTimer = setInterval(function () {
                refreshEntregasPendentesUi(true);
            }, 45000);
        }, 1200);
    }

    window.addEventListener('storage', function (ev) {
        if (ev.key !== PDV_PATCH_QUEUE_KEY && ev.key !== PDV_SHARED_CATALOG_LS_KEY) return;
        if (!wizardProductCatalog.length) return;
        clearTimeout(wizardStoragePatchTimer);
        wizardStoragePatchTimer = setTimeout(function () {
            if (agroWizardAplicarFilaPatchLocal(true)) return;
            if (ev.key !== PDV_SHARED_CATALOG_LS_KEY) return;
            try {
                var cached = lerWizardCatalogSharedCache();
                if (!cached || !cached.produtos.length) return;
                aplicarWizardCatalogRows(cached.produtos, true);
            } catch (_) {}
        }, 40);
    });

    document.addEventListener('visibilitychange', function () {
        if (!document.hidden) agroWizardCatalogoRefreshNoFoco();
    });
    window.addEventListener('focus', agroWizardCatalogoRefreshNoFoco);
    window.addEventListener('pageshow', function (ev) {
        if (ev.persisted) agroWizardCatalogoRefreshNoFoco();
    });

    loadWizardCatalog()
        .then(function () {
            if (dom.productSearchFeedback) {
                dom.productSearchFeedback.textContent = 'Catálogo local pronto. Digite para filtrar.';
            }
        })
        .catch(function (err) {
            if (dom.productSearchFeedback) {
                var msg = err && err.message ? String(err.message) : 'falha de rede ou servidor.';
                if (msg.length > 220) msg = msg.slice(0, 217) + '…';
                dom.productSearchFeedback.textContent =
                    'Catálogo: ' + msg + ' Atualize a página; se persistir, limpe o cache do navegador para este site.';
            }
            try {
                sessionStorage.removeItem(CATALOG_STORAGE_KEY);
            } catch (errRm) {}
        })
        .finally(function () {
            carregarDadosSecundariosPdv();
        });

    window.AgroPdvAddProductByCode = function (code) {
        var c = String(code || '').trim();
        if (!c) return Promise.resolve(false);
        var relOpts = { query: c, explicitPick: true, forceServer: false, okMsg: '', skipSearchUiReset: true };
        var picked = pickProductForQuery(wizardProductCatalog, c);
        if (picked) {
            var localResult = tryAddProductFromSearch(picked, relOpts);
            return Promise.resolve(localResult).then(function (ok) {
                if (ok) return ok;
                return tryAddProductFromSearch({}, { query: c, forceServer: true, okMsg: '', skipSearchUiReset: true });
            });
        }
        return tryAddProductFromSearch({}, { query: c, forceServer: true, okMsg: '', skipSearchUiReset: true });
    };

    var currentState = State.getState();
    if (!hydratedFromConsulta && (!currentState.clienteMode || currentState.clienteMode === 'unset')) {
        openStartModal();
    }
    focusProductSearch();
})();
