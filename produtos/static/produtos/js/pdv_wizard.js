(function () {
    'use strict';

    var bootstrapEl = document.getElementById('agro-pdv-wizard-bootstrap');
    var bootstrap = {};
    try {
        bootstrap = bootstrapEl ? JSON.parse(bootstrapEl.textContent || '{}') : {};
    } catch (err) {
        bootstrap = {};
    }

    var urls = bootstrap.urls || {};
    var assets = bootstrap.assets || {};
    var pagamentoUi = bootstrap.pagamentoUi || {};
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
        quickClientPicker: document.getElementById('pdv-quick-client-picker'),
        quickClientPickerClose: document.getElementById('pdv-quick-client-picker-close'),
        quickClientChange: document.getElementById('pdv-quick-client-change'),
        clientPurchaseHistory: document.getElementById('pdv-client-purchase-history'),
        productSearch: document.getElementById('pdv-product-search'),
        productSearchFeedback: document.getElementById('pdv-product-search-feedback'),
        productSearchMeta: document.getElementById('pdv-product-search-meta'),
        productAutocomplete: document.getElementById('pdv-product-autocomplete'),
        productCartList: document.getElementById('pdv-product-cart-list'),
        quickClientHit: document.getElementById('pdv-quick-client-hit'),
        voltarHome: document.getElementById('pdv-wizard-voltar-home'),
        productSubtotal: document.getElementById('pdv-product-subtotal'),
        productSubtotalItems: document.getElementById('pdv-product-subtotal-items'),
        productCreditBalance: document.getElementById('pdv-product-credit-balance'),
        productCashbackBalance: document.getElementById('pdv-product-cashback-balance'),
        productFiadoBalance: document.getElementById('pdv-product-fiado-balance'),
        productStepCount: document.getElementById('pdv-product-step-count'),
        clearItems: document.getElementById('pdv-clear-items'),
        step1Advance: document.getElementById('pdv-step1-advance'),
        step1Payment: document.getElementById('pdv-step1-payment'),
        step1BudgetVerMais: document.getElementById('pdv-step1-budget-ver-mais'),
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
        clienteTelefone: document.getElementById('pdv-cliente-telefone'),
        clienteLogradouro: document.getElementById('pdv-cliente-logradouro'),
        clienteNumero: document.getElementById('pdv-cliente-numero'),
        clienteBairro: document.getElementById('pdv-cliente-bairro'),
        clientePluscode: document.getElementById('pdv-cliente-pluscode'),
        clienteAdvancedEdit: document.getElementById('pdv-cliente-advanced-edit-modal'),
        clienteEditModal: document.getElementById('pdv-cliente-edit-modal'),
        clienteEditClose: document.getElementById('pdv-cliente-edit-close'),
        clienteZapWhatsapp: document.getElementById('pdv-step2-cliente-zap'),
        entregaRadios: document.querySelectorAll('input[name="pdv-entrega-tipo"]'),
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
        paymentNote: document.getElementById('pdv-payment-note'),
        paymentValorForma: document.getElementById('pdv-pay-valor-tranche'),
        paymentValorTotalRef: document.getElementById('pdv-payment-valor-total-ref'),
        paymentValorRestante: document.getElementById('pdv-payment-valor-restante'),
        paymentSubtotal: document.getElementById('pdv-payment-subtotal'),
        paymentDiscountView: document.getElementById('pdv-payment-discount-view'),
        paymentShippingView: document.getElementById('pdv-payment-shipping-view'),
        paymentTotal: document.getElementById('pdv-payment-total'),
        paymentPaidAccum: document.getElementById('pdv-payment-pago-acum'),
        paymentRemainingTop: document.getElementById('pdv-payment-restante-top'),
        paymentFeedback: document.getElementById('pdv-payment-feedback'),
        paymentLancamentosBox: document.getElementById('pdv-payment-lancamentos-box'),
        paymentLancamentosList: document.getElementById('pdv-payment-lancamentos-list'),
        confirmSaleNoPrint: document.getElementById('pdv-confirm-sale-no-print'),
        confirmSalePrint: document.getElementById('pdv-confirm-sale-print'),
        paymentModalCards: document.querySelectorAll('[data-payment-modal-card]'),
        paymentFormaModal: document.getElementById('pdv-payment-forma-modal'),
        paymentFormaModalBackdrop: document.getElementById('pdv-payment-forma-modal-backdrop'),
        paymentFormaModalClose: document.getElementById('pdv-payment-forma-modal-close'),
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
        cardMpQr: document.getElementById('pdv-card-mp-qr'),
        pixSicrediLink: document.getElementById('pdv-pix-sicredi-link'),
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
    var productSelectionIndex = -1;
    var clientListSelectIdx = -1;
    var clientSearchSeq = 0;
    var AUTOCOMPLETE_LIMIT = 8;
    var MAX_LOCAL_RESULTS = 48;
    var CATALOG_STORAGE_KEY = 'agro_pdv_wizard_catalog_v2';
    var wizardProductCatalog = [];
    var catalogReady = false;
    var catalogLoadPromise = null;
    var prevStepCache = '';

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

    function loadWizardCatalog() {
        if (catalogReady) return Promise.resolve();
        if (catalogLoadPromise) return catalogLoadPromise;
        try {
            var raw = sessionStorage.getItem(CATALOG_STORAGE_KEY);
            if (raw) {
                var parsed = JSON.parse(raw);
                if (parsed && Array.isArray(parsed.produtos) && parsed.produtos.length) {
                    wizardProductCatalog = parsed.produtos;
                    catalogReady = true;
                    return Promise.resolve();
                }
            }
        } catch (err) {}
        var url = (urls.apiBuscarProdutos || '/api/buscar/') + '?wizard=1&wizard_catalog=1';
        catalogLoadPromise = fetch(url, { credentials: 'same-origin' })
            .then(function (res) {
                if (!res.ok) throw new Error('catalog_http');
                return res.json();
            })
            .then(function (data) {
                wizardProductCatalog = Array.isArray(data.produtos) ? data.produtos : [];
                catalogReady = true;
                try {
                    sessionStorage.setItem(CATALOG_STORAGE_KEY, JSON.stringify({ produtos: wizardProductCatalog, t: Date.now() }));
                } catch (err2) {}
            })
            .finally(function () {
                catalogLoadPromise = null;
                updateSearchAwaitingPulse();
            });
        return catalogLoadPromise;
    }

    function findUniqueBarcodeMatch(q) {
        var qt = String(q || '').trim();
        if (!qt) return null;
        var qd = onlyDigits(qt);
        var seen = {};
        var hits = [];
        wizardProductCatalog.forEach(function (p) {
            var ean = String(p.codigo_barras || '').trim();
            var nfe = String(p.codigo_nfe || '').trim();
            var cod = String(p.codigo || '').trim();
            var match = false;
            if (qt && (ean === qt || nfe === qt || cod === qt)) match = true;
            else if (qd.length >= 6 && onlyDigits(ean) && onlyDigits(ean) === qd) match = true;
            if (match) {
                var id = String(p.id || '');
                if (id && !seen[id]) {
                    seen[id] = true;
                    hits.push(p);
                }
            }
        });
        return hits.length === 1 ? hits[0] : null;
    }

    function scoreProduct(p, qRaw, barcodeMode) {
        var q = stripAccents(qRaw.trim());
        if (!q) return 0;
        var nome = stripAccents(p.nome || '');
        var marca = stripAccents(p.marca || '');
        var nfe = stripAccents(String(p.codigo_nfe || ''));
        var cod = stripAccents(String(p.codigo || ''));
        var ean = stripAccents(String(p.codigo_barras || '').replace(/\s/g, ''));
        var qDigits = onlyDigits(q);
        var eanD = onlyDigits(ean);

        var score = 0;
        if (nfe === q || cod === q || ean === q) score += 2500;
        if (qDigits.length >= 6 && eanD && eanD === qDigits) score += 2400;
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
            var one = findUniqueBarcodeMatch(q);
            if (one) return { list: [], barcodeHit: one, message: '' };
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
                return Array.isArray(data.produtos) ? data.produtos : [];
            });
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

    function compactText(value, fallback) {
        var txt = String(value || '').trim();
        return txt || fallback || '—';
    }

    function buildLinhaEnderecoEntrega(state) {
        var e = state.entrega || {};
        var log = String(e.logradouro || '').trim();
        var num = String(e.numero || '').trim();
        var bai = String(e.bairro || '').trim();
        var pc = String(e.plusCode || '').trim();
        var parts = [];
        if (log || num) {
            var ln = [log, num].filter(Boolean).join(', ');
            if (ln) parts.push(ln);
        }
        if (bai) parts.push(bai);
        if (pc) parts.push('Plus ' + pc);
        if (parts.length) return parts.join(' — ') + ' — Jacupiranga/SP';
        return compactText(e.endereco || '', '');
    }

    function composeEndereco(state) {
        var structured = buildLinhaEnderecoEntrega(state);
        var parts = [];
        var enderecoBase = structured || compactText(state.entrega.endereco || (state.cliente && state.cliente.endereco), '');
        if (enderecoBase) parts.push(enderecoBase);
        if (state.entrega.complemento) parts.push(state.entrega.complemento);
        if (state.entrega.referencia) parts.push('Ref.: ' + state.entrega.referencia);
        return parts.join(' • ');
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
        if (forma === 'Crédito parcelado') {
            var par = parseInt(state.pagamento.creditoParcelas, 10) || 0;
            if (par < 2) return 'Informe 2 ou mais parcelas.';
        }
        if (forma === 'Fiado') {
            var fp = parseInt(state.pagamento.fiadoParcelas, 10) || 0;
            var fd = parseInt(state.pagamento.fiadoDiasVencimento, 10) || 0;
            if (fp < 1) return 'Fiado: parcelas inválidas.';
            if (fd < 1) return 'Fiado: prazo em dias inválido.';
        }
        if (forma === 'Vale crédito') {
            var sv = Number(pagamentoUi.saldoValeCredito || 0);
            if (sv <= 0) return 'Sem saldo de vale crédito configurado.';
            if (T > sv + 0.009) return 'Valor acima do saldo do vale.';
        }
        if (forma === 'Cashback') {
            var sc = Number(pagamentoUi.saldoCashback || 0);
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
        return {
            forma: forma,
            valor: T,
            maquinaId: state.pagamento.maquinaId || '',
            maquinaNome: state.pagamento.maquinaNome || '',
            creditoParcelas: forma === 'Crédito parcelado' ? parseInt(state.pagamento.creditoParcelas, 10) || 2 : null,
            fiadoParcelas: forma === 'Fiado' ? parseInt(state.pagamento.fiadoParcelas, 10) || 1 : null,
            fiadoDiasVencimento: forma === 'Fiado' ? parseInt(state.pagamento.fiadoDiasVencimento, 10) || 30 : null,
            valorRecebido: dinheiroExtra.valorRecebido || '',
            trocoCalculado: dinheiroExtra.trocoCalculado || '',
            outroDetalhes: forma === 'Outro' ? String(state.pagamento.outroDetalhes || '').trim() : ''
        };
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
            forma === 'Crédito parcelado'
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
        if (!items.length) {
            wrap.innerHTML =
                '<p class="p-2 text-center text-sm font-bold text-slate-500">Nenhuma máquina configurada (PDV_WIZARD_MAQUININHAS_' +
                (forma === 'PIX' ? 'PIX' : 'CARTAO') +
                ').</p>';
            return;
        }
        wrap.innerHTML = items
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
                            if (L.forma === 'Crédito parcelado' && L.creditoParcelas) bits.push(L.creditoParcelas + 'x');
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
        if (f === 'Crédito parcelado' && L.creditoParcelas) line += ' ' + L.creditoParcelas + 'x';
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
        if (f === 'Crédito parcelado' && L.creditoParcelas) bits.push(String(L.creditoParcelas).trim() + 'x');
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
            out.push({
                formaPagamento: fn.slice(0, 200),
                valorPagamento: Math.round((v + Number.EPSILON) * 100) / 100,
                quitar: true
            });
        }
        return out.length ? out : null;
    }

    function erroValidacaoPagamento(state, computed) {
        var forma = String(state.pagamento.forma || '').trim();
        if (forma) {
            return 'Finalize este meio com Enter no valor (dinheiro ou “Valor nesta forma”) ou use Trocar forma.';
        }
        var arr = state.pagamento.lancamentos || [];
        if (!arr.length) return 'Escolha formas de pagamento até cobrir o total.';
        var total = totalNumberFromComputed(computed);
        var sum = sumValorLancamentos(state);
        if (sum + 0.009 < total) {
            return 'Falta ' + formatMoney(total - sum) + '. Escolha outra forma.';
        }
        if (sum > total + 0.009) return 'Soma dos pagamentos passou do total. Ajuste os lançamentos.';
        return '';
    }

    function canAdvance(state, computed) {
        if (state.currentStep === 'produtos') {
            if (!state.itens.length) return 'Adicione ao menos 1 item antes de continuar.';
            if (state.clienteMode === 'unset') return 'Defina o cliente ou consumidor final antes de continuar.';
        }
        if (state.currentStep === 'cliente') {
            if (!state.cliente || !state.cliente.nome) return 'Defina o cliente da venda.';
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
        }
    }

    function renderStepPanels(state, computed) {
        var flow = computed.flow;
        dom.panels.forEach(function (panel) {
            var step = panel.getAttribute('data-step-panel');
            var visible = step === state.currentStep;
            panel.hidden = !visible;
        });
        dom.stepNavs.forEach(function (btn) {
            var step = btn.getAttribute('data-step-nav');
            var idx = flowIndex(flow, step);
            var currentIdx = flowIndex(flow, state.currentStep);
            btn.classList.remove('pdv-step-badge-active', 'pdv-step-badge-done', 'border-emerald-200', 'bg-emerald-50');
            if (idx === currentIdx) {
                btn.classList.add('pdv-step-badge-active');
            } else if (idx > -1 && idx < currentIdx) {
                btn.classList.add('pdv-step-badge-done', 'border-emerald-200');
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
                dom.btnPrev.title = 'Etapa anterior · F1';
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
                var meio = String((state.entrega && state.entrega.meioNaEntrega) || '');
                var endOk = enderecoEntregaMinimoOk(state);
                dom.btnNext.classList.remove('opacity-40');
                if (!lp) {
                    dom.btnNext.innerHTML = 'Escolha o local do pagamento' + kbdF7;
                    dom.btnNext.disabled = true;
                    dom.btnNext.classList.add('opacity-40');
                    dom.btnNext.title = 'Use o pop-up ao entrar nesta etapa (Pagamento na entrega ou na loja).';
                } else if (lp === 'loja') {
                    dom.btnNext.innerHTML = 'Ir para pagamento' + kbdF7;
                    dom.btnNext.disabled = !endOk;
                    if (!endOk) dom.btnNext.classList.add('opacity-40');
                    dom.btnNext.title = 'Imprime as vias e abre a etapa Pagamento · F7';
                } else if (lp === 'entrega') {
                    if (!meio) {
                        dom.btnNext.innerHTML = 'Dinheiro ou cartão' + kbdF7;
                        dom.btnNext.disabled = true;
                        dom.btnNext.classList.add('opacity-40');
                        dom.btnNext.title = 'Complete o pop-up (dinheiro ou cartão na entrega).';
                    } else {
                        dom.btnNext.innerHTML = 'Enviar entrega' + kbdF7;
                        dom.btnNext.disabled = !endOk;
                        if (!endOk) dom.btnNext.classList.add('opacity-40');
                        dom.btnNext.title = 'Imprime, registra no painel Entregas e reinicia o PDV · F7';
                    }
                } else {
                    dom.btnNext.innerHTML = nextLabel + kbdF7;
                    dom.btnNext.disabled = false;
                    dom.btnNext.title = 'Próxima etapa · F7';
                }
            } else if (state.currentStep === 'cliente') {
                dom.btnNext.innerHTML = nextLabel + kbdF7;
                dom.btnNext.disabled = false;
                dom.btnNext.title = next === 'pagamento' ? 'Ir para pagamento · F7' : 'Continuar · F7';
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
            cliente: 'Atalhos nos botões · E edita dados do cliente.',
            entrega: 'Pop-up: pagamento na entrega ou na loja. Rodapé: Enviar entrega ou Ir para pagamento · F7. F1 volta.',
            pagamento: 'Feche a venda e confirme o envio.'
        };
        dom.stepHint.textContent = hints[state.currentStep] || 'Siga o fluxo da venda.';
    }

    function renderSummary(state, computed) {
        if (!dom.summaryClient) return;
        dom.summaryClient.textContent = currentClientName(state);
        dom.summaryMode.textContent = state.clienteMode === 'consumidor_final'
            ? 'Consumidor final'
            : compactText((state.cliente && (state.cliente.documento || state.cliente.telefone)) || '', 'Cliente selecionado');
        dom.summaryItems.textContent = String(computed.itemCount);
        if (dom.summarySubtotal) dom.summarySubtotal.textContent = formatMoney(computed.subtotal);
        if (dom.summaryDiscount) dom.summaryDiscount.textContent = formatMoney(computed.desconto);
        if (dom.summaryShipping) dom.summaryShipping.textContent = formatMoney(computed.frete);
        dom.summaryTotal.textContent = formatMoney(computed.total);
        dom.summaryDelivery.textContent = state.entrega.ativa ? 'Entrega' : 'Retirada';
        var currentStepLabel = ({
            produtos: 'Produtos',
            cliente: 'Cliente',
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
        var id = last.id;
        var nextQty = State.toNumber(last.qtd) + delta;
        if (nextQty <= 0) {
            State.removeItem(id);
        } else {
            State.updateItemQuantity(id, nextQty);
        }
    }

    function renderQuickClient(state) {
        if (dom.quickClientName) dom.quickClientName.textContent = currentClientName(state);
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
        dom.productSubtotalItems.textContent = computed.itemCount + (computed.itemCount === 1 ? ' item' : ' itens');
        if (dom.productCreditBalance) dom.productCreditBalance.textContent = 'R$ 0,00';
        if (dom.productCashbackBalance) dom.productCashbackBalance.textContent = 'R$ 0,00';
        if (dom.productFiadoBalance) dom.productFiadoBalance.textContent = 'R$ 0,00';
        if (!state.itens.length) {
            dom.productCartList.innerHTML =
                '<div class="pdv-cart-empty rounded-2xl border border-dashed border-orange-200 bg-orange-50/40 px-4 py-8 text-center text-sm font-bold text-slate-500">Nenhum item ainda — busque acima.</div>';
        } else {
            dom.productCartList.innerHTML = state.itens.map(function (item) {
                var imgUrl = String(item.imagem || assets.placeholderProduto || '').trim();
                return (
                    '' +
                    '<div class="pdv-cart-row flex flex-nowrap items-center gap-2 rounded-xl border-2 border-slate-200 bg-white px-2 py-2 shadow-sm sm:gap-2.5 sm:px-2.5">' +
                    '  <span class="relative h-12 w-12 shrink-0 cursor-zoom-in overflow-hidden rounded-lg border-2 border-slate-200 bg-slate-50 outline-none focus-visible:ring-2 focus-visible:ring-emerald-400" data-pdv-photo-zoom="' +
                    escapeHtml(imgUrl) +
                    '" tabindex="0" role="button" title="Ampliar foto (Enter)">' +
                    '    <img src="' +
                    escapeHtml(imgUrl) +
                    '" alt="" class="pointer-events-none h-full w-full object-cover">' +
                    '  </span>' +
                    '  <div class="pdv-cart-line min-w-0 flex-1 overflow-hidden">' +
                    '    <span class="block truncate text-[12px] font-black leading-tight text-slate-900 sm:text-[13px]">' +
                    escapeHtml(item.nome) +
                    '</span>' +
                    '  </div>' +
                    '  <div class="flex shrink-0 flex-nowrap items-center gap-2 sm:gap-2.5">' +
                    '    <span class="w-[4.5rem] shrink-0 text-right font-mono text-[10px] font-bold leading-tight text-slate-500 tabular-nums sm:w-[4.75rem]" title="Código GM">' +
                    escapeHtml(cartCodigoGm(item)) +
                    '</span>' +
                    '    <div class="inline-flex shrink-0 items-center overflow-hidden rounded-xl border-2 border-slate-300 bg-slate-50 shadow-inner">' +
                    '      <button type="button" class="flex min-h-[2.85rem] min-w-[2.85rem] items-center justify-center text-xl font-black leading-none text-slate-800 active:bg-slate-200 sm:min-h-[3rem] sm:min-w-[3rem] sm:text-2xl" data-item-qty="' +
                    escapeHtml(item.id) +
                    '" data-item-delta="-1" title="Menos">−</button>' +
                    '      <span class="min-w-[2.25rem] px-1.5 text-center text-base font-black tabular-nums text-slate-900">' +
                    escapeHtml(item.qtd) +
                    '</span>' +
                    '      <button type="button" class="flex min-h-[2.85rem] min-w-[2.85rem] items-center justify-center text-xl font-black leading-none text-slate-800 active:bg-slate-200 sm:min-h-[3rem] sm:min-w-[3rem] sm:text-2xl" data-item-qty="' +
                    escapeHtml(item.id) +
                    '" data-item-delta="1" title="Mais">+</button>' +
                    '    </div>' +
                    '    <span class="w-[4.5rem] shrink-0 text-right text-[12px] font-black tabular-nums text-emerald-700 sm:w-[4.85rem]">' +
                    formatMoney(item.qtd * item.preco) +
                    '</span>' +
                    '    <button type="button" class="shrink-0 rounded-lg px-1 py-1 text-[9px] font-black uppercase text-red-700 underline decoration-red-300 sm:text-[10px]" data-remove-item="' +
                    escapeHtml(item.id) +
                    '">Remover</button>' +
                    '  </div>' +
                    '</div>'
                );
            }).join('');
        }
        updateSearchAwaitingPulse();
        renderRecentBudgetsSnippet();
    }

    function renderRecentBudgetsSnippet() {
        var el = document.getElementById('pdv-step1-budget-snippet');
        if (!el) return;
        var historico = [];
        try {
            historico = JSON.parse(localStorage.getItem('historicoOrcamentos') || '[]');
            if (!Array.isArray(historico)) historico = [];
        } catch (errH) {
            historico = [];
        }
        var slice = historico.slice(0, 3);
        if (!slice.length) {
            el.innerHTML =
                '<p class="py-0.5 text-center text-[10px] font-semibold text-slate-400">Nenhum ainda</p>';
            return;
        }
        el.innerHTML =
            '<ul class="space-y-2 text-left text-[10px] leading-snug">' +
            slice
                .map(function (item) {
                    return (
                        '<li class="border-b border-slate-200/60 pb-2 last:border-0">' +
                        '<div class="break-words font-bold text-slate-700" title="' +
                        escapeHtml(item.cliente || '') +
                        '">' +
                        escapeHtml(item.cliente || '—') +
                        '</div>' +
                        '<div class="mt-0.5 text-right font-mono text-[9px] text-slate-500">' +
                        escapeHtml(item.total || '—') +
                        '</div>' +
                        '</li>'
                    );
                })
                .join('') +
            '</ul>';
    }

    function productAutocompleteHtml(produto, index) {
        var selected = index === productSelectionIndex;
        var gm = displayCodigoGm(produto);
        var marca = String(produto.marca || '').trim();
        var sub = marca ? gm + ' · ' + marca : gm;
        var imgUrl = String(produto.imagem || assets.placeholderProduto || '').trim();
        return (
            '' +
            '<button type="button" class="flex w-full items-stretch gap-2 rounded-xl px-2 py-2 text-left ' +
            (selected ? 'bg-emerald-50 ring-2 ring-emerald-200' : 'hover:bg-emerald-50') +
            '" data-add-product="' +
            escapeHtml(produto.id) +
            '" data-autocomplete-index="' +
            index +
            '">' +
            '  <span class="relative h-11 w-11 shrink-0 cursor-zoom-in overflow-hidden rounded-lg border border-slate-200 bg-slate-50 outline-none focus-visible:ring-2 focus-visible:ring-emerald-400" data-pdv-photo-zoom="' +
            escapeHtml(imgUrl) +
            '" tabindex="-1" role="presentation" title="Ampliar foto">' +
            '    <img src="' +
            escapeHtml(imgUrl) +
            '" alt="" class="pointer-events-none h-full w-full object-cover">' +
            '  </span>' +
            '  <span class="flex min-w-0 flex-1 flex-col justify-center gap-0.5 overflow-hidden">' +
            '    <span class="truncate text-sm font-black text-slate-900">' +
            escapeHtml(produto.nome || '') +
            '</span>' +
            '    <span class="truncate text-[10px] font-bold text-slate-500">' +
            escapeHtml(sub) +
            '</span>' +
            '  </span>' +
            '  <span class="shrink-0 self-center text-sm font-black text-emerald-700">' +
            formatMoney(produto.preco_venda || 0) +
            '</span>' +
            '</button>'
        );
    }

    function renderProductResults(produtos) {
        lastProducts = Array.isArray(produtos) ? produtos : [];
        if (lastProducts.length) {
            if (productSelectionIndex < 0 || productSelectionIndex >= lastProducts.length) {
                productSelectionIndex = 0;
            }
        } else {
            productSelectionIndex = -1;
        }
        if (dom.productAutocomplete) {
            if (lastProducts.length) {
                dom.productAutocomplete.innerHTML = lastProducts.slice(0, AUTOCOMPLETE_LIMIT).map(function (produto, index) {
                    return productAutocompleteHtml(produto, index);
                }).join('');
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
        dom.productSearchMeta.textContent = lastProducts.length + ' na lista · Enter · +/− último';
        updateSearchAwaitingPulse();
    }

    function renderStep2(state) {
        if (dom.step2ClientName) dom.step2ClientName.textContent = currentClientName(state);
        if (dom.step2ClientDoc) {
            dom.step2ClientDoc.textContent = compactText(state.cliente && state.cliente.documento, 'Sem documento informado');
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
        dom.entregaRadios.forEach(function (radio) {
            radio.checked = state.entrega.ativa ? radio.value === 'entrega' : radio.value === 'retirada';
        });
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
        dom.clienteEditModal.classList.add('hidden');
        dom.clienteEditModal.classList.remove('flex');
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

    function enderecoEntregaMinimoOk(state) {
        var e = state.entrega || {};
        if (String(e.logradouro || '').trim() && String(e.bairro || '').trim()) return true;
        if (String(e.endereco || '').trim().length > 4) return true;
        var c = state.cliente || {};
        return String(c.endereco || '').trim().length > 4;
    }

    function entregaTaxaModoEfetivo(state) {
        var e = state.entrega || {};
        var m = String(e.taxaEntregaModo || '');
        if (m === 'nao' || m === 'sim' || m === 'depois') return m;
        if (e.taxaEntregaRespondida) {
            var f = Number((state.pagamento && state.pagamento.frete) || 0);
            if (f > 0.009) return 'sim';
            return 'nao';
        }
        return '';
    }

    function commitEntregaTaxaModo(modo) {
        if (modo === 'nao') {
            State.setPagamentoField('frete', 0);
            State.setEntregaField('taxaEntregaModo', 'nao');
            State.setEntregaField('taxaEntregaRespondida', true);
        } else if (modo === 'depois') {
            State.setEntregaField('taxaEntregaModo', 'depois');
            State.setEntregaField('taxaEntregaRespondida', true);
        } else if (modo === 'sim') {
            var st = State.getState();
            var f = Number((st.pagamento && st.pagamento.frete) || 0);
            if (f <= 0.009) State.setPagamentoField('frete', 10);
            State.setEntregaField('taxaEntregaModo', 'sim');
            State.setEntregaField('taxaEntregaRespondida', true);
        }
    }

    function commitEntregaTaxaValorInput() {
        var el = document.getElementById('pdv-entrega-taxa-valor');
        if (!el) return;
        var st = State.getState();
        if (entregaTaxaModoEfetivo(st) !== 'sim') return;
        State.setPagamentoField('frete', State.toNumber(el.value));
    }

    function renderEntregaTaxaCard(state) {
        var modo = entregaTaxaModoEfetivo(state);
        var frete = Number((state.pagamento && state.pagamento.frete) || 0);
        var wrap = document.getElementById('pdv-entrega-taxa-valor-wrap');
        var inpVal = document.getElementById('pdv-entrega-taxa-valor');
        document.querySelectorAll('input[name="pdv-entrega-taxa-modo"]').forEach(function (r) {
            r.checked = modo !== '' && r.value === modo;
        });
        if (wrap) wrap.classList.toggle('hidden', modo !== 'sim');
        if (inpVal && modo === 'sim') {
            var display = frete > 0.009 ? String(frete.toFixed(2)).replace('.', ',') : '10,00';
            setInputValue(inpVal, display);
        }
    }

    function renderEntrega(state) {
        syncEntregaToolbarLinks(state);
        initBairroSelectsOnce();
        var e = state.entrega || {};
        var c = state.cliente || {};
        var log = String(e.logradouro || '').trim();
        var num = String(e.numero || '').trim();
        var bai = String(e.bairro || '').trim();
        if (!log && !num && !bai && String(e.endereco || '').trim()) {
            setInputValue(dom.entregaLogradouro, e.endereco);
        } else {
            setInputValue(dom.entregaLogradouro, e.logradouro || c.logradouro || '');
        }
        setInputValue(dom.entregaNumero, e.numero || c.numero || '');
        setSelectValue(dom.entregaBairro, e.bairro || c.bairro || '', '');
        setInputValue(dom.entregaPluscode, e.plusCode || c.plus_code || '');
        setInputValue(dom.entregaComplemento, e.complemento);
        setInputValue(dom.entregaReferencia, e.referencia || (c && c.referencia_rural) || '');
        setInputValue(dom.entregaHorario, e.horario);
        setInputValue(dom.entregaTroco, e.troco);
        setInputValue(dom.entregaObservacao, e.observacao);
        renderEntregaTaxaCard(state);
    }

    function renderPagamento(state, computed) {
        var forma = state.pagamento.forma || '';
        setSelectValue(dom.paymentMethod, forma, '');
        setInputValue(dom.paymentDiscount, state.pagamento.descontoGeral ? String(state.pagamento.descontoGeral).replace('.', ',') : '');
        setInputValue(dom.paymentShipping, state.pagamento.frete ? String(state.pagamento.frete).replace('.', ',') : '');
        setInputValue(dom.paymentReceived, state.pagamento.valorRecebido);
        setInputValue(dom.paymentChange, state.pagamento.trocoCalculado);
        setInputValue(dom.paymentNote, state.pagamento.observacaoFinal);
        if (dom.paymentValorForma) setInputValue(dom.paymentValorForma, state.pagamento.valorDestaForma);
        if (dom.paymentParcelasCredito) {
            setInputValue(dom.paymentParcelasCredito, String(state.pagamento.creditoParcelas || 2));
        }
        if (dom.fiadoParcelasInput) setInputValue(dom.fiadoParcelasInput, String(state.pagamento.fiadoParcelas || 1));
        if (dom.fiadoDiasInput) setInputValue(dom.fiadoDiasInput, String(state.pagamento.fiadoDiasVencimento || 30));
        if (dom.outroDetalhes) setInputValue(dom.outroDetalhes, state.pagamento.outroDetalhes);
        if (dom.fiadoResumo) {
            dom.fiadoResumo.innerHTML =
                'Por padrão: conta a receber em <strong>' +
                (parseInt(state.pagamento.fiadoParcelas, 10) || 1) +
                'x</strong> com 1º vencimento em <strong>' +
                (parseInt(state.pagamento.fiadoDiasVencimento, 10) || 30) +
                ' dias</strong>.';
        }
        if (dom.valeSaldoView) dom.valeSaldoView.textContent = formatMoney(pagamentoUi.saldoValeCredito || 0);
        if (dom.cashbackSaldoView) dom.cashbackSaldoView.textContent = formatMoney(pagamentoUi.saldoCashback || 0);
        if (dom.outroPinMsg) {
            dom.outroPinMsg.textContent = state.pagamento.outroPinVerificado
                ? 'PIN validado. Descreva o pagamento.'
                : 'PIN obrigatório antes de descrever o pagamento.';
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
            forma === 'Cartão de débito' || forma === 'Cartão de crédito' || forma === 'Crédito parcelado';
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
            var pixScrRow = document.getElementById('pdv-pix-row-sicredi');
            var pixScoRow = document.getElementById('pdv-pix-row-sicoob');
            var rowVisPix = function (el, on) {
                if (el) el.classList.toggle('hidden', !on);
            };
            if (hasMaquina) {
                var pMid = String(state.pagamento.maquinaId || '').trim();
                var narrowMp = pMid === 'pix_mp_qr';
                var narrowScr = pMid === 'pix_sicredi_qr';
                var narrowSco = pMid === 'pix_sicoob_chave';
                var narrow = narrowMp || narrowScr || narrowSco;
                if (narrow) {
                    rowVisPix(pixMpRow, narrowMp);
                    rowVisPix(pixScrRow, narrowScr);
                    rowVisPix(pixScoRow, narrowSco);
                } else {
                    rowVisPix(pixMpRow, true);
                    rowVisPix(pixScrRow, true);
                    rowVisPix(pixScoRow, true);
                }
            } else {
                rowVisPix(pixMpRow, true);
                rowVisPix(pixScrRow, true);
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

        if (dom.flowParcelasPanel) dom.flowParcelasPanel.classList.toggle('hidden', forma !== 'Crédito parcelado');
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

        var mpPixHint =
            'QR Pix Mercado Pago aparece na maquininha — use o display do terminal ou “Ampliar QR” para orientar o cliente.';
        var mpCardHint =
            'Pagamento no cartão Mercado Pago é concluído na maquininha — não é necessário QR fixo nesta tela.';
        fillQrSlot(dom.pixMpQr, pagamentoUi.qrMercadoPagoUrl, mpPixHint);
        fillQrSlot(dom.cardMpQr, pagamentoUi.qrMercadoPagoUrl, mpCardHint);
        wireSicrediLink(dom.pixSicrediLink, pagamentoUi.qrSicrediUrl);
        wireSicrediLink(dom.cardSicrediLink, pagamentoUi.qrSicrediUrl);
        if (dom.pixSicrediLink) {
            var uPixScr = String(pagamentoUi.qrSicrediUrl || '').trim();
            dom.pixSicrediLink.textContent = uPixScr ? 'Abrir QR Sicredi' : 'QR na maquininha Sicredi';
        }
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
        if (dom.paymentValorTotalRef) dom.paymentValorTotalRef.textContent = formatMoney(total);
        if (dom.paymentValorRestante) dom.paymentValorRestante.textContent = formatMoney(restFin);

        var larr = state.pagamento.lancamentos || [];
        if (dom.paymentLancamentosBox) dom.paymentLancamentosBox.classList.toggle('hidden', !larr.length);
        if (dom.paymentLancamentosList) {
            dom.paymentLancamentosList.innerHTML = larr.length
                ? larr
                      .map(function (L) {
                          return (
                              '<li class="flex flex-wrap justify-between gap-2 border-b border-slate-100 pb-1">' +
                              '<span>' +
                              escapeHtml(L.forma || '') +
                              (L.maquinaNome ? ' · ' + escapeHtml(L.maquinaNome) : '') +
                              '</span><span class="tabular-nums">' +
                              escapeHtml(formatMoney(L.valor)) +
                              '</span></li>'
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
        if (!forma && !larr.length) {
            dom.paymentFeedback.textContent = 'F3 ou “Escolher forma” · teclas 1–9 no painel. Lance cada meio com Enter.';
        } else if (forma) {
            dom.paymentFeedback.textContent = '';
        } else if (err) {
            dom.paymentFeedback.textContent = err;
        } else {
            dom.paymentFeedback.textContent = '';
        }
    }

    function renderAll(state, computed) {
        var flow = computed.flow;
        if (flowIndex(flow, state.currentStep) === -1) {
            State.setCurrentStep(flow[flow.length - 1] || 'produtos');
            return;
        }
        var wasStep = prevStepCache;
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
        if (state.currentStep === 'entrega' && wasStep !== 'entrega' && state.entrega.ativa) {
            setTimeout(function () {
                var st2 = State.getState();
                if (st2.currentStep !== 'entrega' || !st2.entrega.ativa) return;
                if (!String(st2.entrega.localPagamento || '').trim()) {
                    openEntregaFluxoModal1();
                }
            }, 220);
        }
        prevStepCache = state.currentStep;
    }

    function focusProductSearch() {
        if (dom.productSearch) dom.productSearch.focus();
    }

    function openStartModal() {
        if (!dom.modalStart) return;
        dom.modalStart.classList.remove('hidden');
        dom.modalStart.classList.add('flex');
        setTimeout(function () {
            if (dom.startConsumidorFinal) dom.startConsumidorFinal.focus();
        }, 50);
    }

    function closeStartModal() {
        if (!dom.modalStart) return;
        dom.modalStart.classList.add('hidden');
        dom.modalStart.classList.remove('flex');
    }

    function openBudgetHistory() {
        var historico = [];
        try {
            historico = JSON.parse(localStorage.getItem('historicoOrcamentos') || '[]');
            if (!Array.isArray(historico)) historico = [];
        } catch (err) {}
        dom.budgetHistoryList.innerHTML = historico.length ? historico.map(function (item) {
            var itens = Array.isArray(item.itens) ? item.itens.length : 0;
            return '' +
                '<div class="mb-3 rounded-2xl border border-slate-200 bg-slate-50 p-4">' +
                '  <div class="flex flex-wrap items-start justify-between gap-3">' +
                '    <div>' +
                '      <div class="text-sm font-black text-slate-900">' + escapeHtml(item.cliente || 'Cliente não informado') + '</div>' +
                '      <div class="mt-1 text-[11px] font-bold text-slate-500">' + escapeHtml(item.data || '') + ' • ' + escapeHtml(item.total || 'R$ 0,00') + ' • ' + itens + ' item(ns)</div>' +
                '    </div>' +
                '    <button type="button" class="rounded-xl bg-emerald-600 px-3 py-2 text-[11px] font-black uppercase text-white" data-budget-index="' + historico.indexOf(item) + '">Reabrir</button>' +
                '  </div>' +
                '</div>';
        }).join('') : '<div class="rounded-2xl border border-dashed border-slate-300 bg-slate-50 px-4 py-10 text-center text-sm font-bold text-slate-400">Nenhum orçamento salvo neste aparelho.</div>';
        dom.budgetHistoryModal.classList.remove('hidden');
        dom.budgetHistoryModal.classList.add('flex');
    }

    function closeBudgetHistory() {
        dom.budgetHistoryModal.classList.add('hidden');
        dom.budgetHistoryModal.classList.remove('flex');
    }

    function pdvEnsureModalOpenBody() {
        try {
            document.body.classList.add('modal-open');
        } catch (eM) {}
    }

    function pdvTryRemoveModalOpenBody() {
        var md1 = document.getElementById('modal-pdv-entrega-fluxo-1');
        var md2 = document.getElementById('modal-pdv-entrega-fluxo-2');
        var md3 = document.getElementById('modal-pdv-entrega-fluxo-3-troco');
        var mei = document.getElementById('modal-pdv-entrega-impressao');
        var meiOpen = mei && !mei.classList.contains('hidden');
        var f1 = md1 && !md1.classList.contains('hidden');
        var f2 = md2 && !md2.classList.contains('hidden');
        var f3 = md3 && !md3.classList.contains('hidden');
        try {
            if (!f1 && !f2 && !f3 && !meiOpen) document.body.classList.remove('modal-open');
        } catch (eM2) {}
    }

    function isEntregaFluxo1Open() {
        var r = document.getElementById('modal-pdv-entrega-fluxo-1');
        return !!(r && !r.classList.contains('hidden'));
    }
    function isEntregaFluxo2Open() {
        var r = document.getElementById('modal-pdv-entrega-fluxo-2');
        return !!(r && !r.classList.contains('hidden'));
    }
    function isEntregaFluxo3Open() {
        var r = document.getElementById('modal-pdv-entrega-fluxo-3-troco');
        return !!(r && !r.classList.contains('hidden'));
    }
    function isAnyEntregaFluxoModalOpen() {
        return isEntregaFluxo1Open() || isEntregaFluxo2Open() || isEntregaFluxo3Open();
    }

    function closeEntregaFluxoModal1() {
        var root = document.getElementById('modal-pdv-entrega-fluxo-1');
        if (root) {
            root.classList.add('hidden');
            root.classList.remove('flex');
        }
        pdvTryRemoveModalOpenBody();
    }

    function openEntregaFluxoModal1() {
        var root = document.getElementById('modal-pdv-entrega-fluxo-1');
        if (!root) return;
        root.classList.remove('hidden');
        root.classList.add('flex');
        pdvEnsureModalOpenBody();
    }

    function closeEntregaFluxoModal2() {
        var root = document.getElementById('modal-pdv-entrega-fluxo-2');
        if (root) {
            root.classList.add('hidden');
            root.classList.remove('flex');
        }
        pdvTryRemoveModalOpenBody();
    }

    function openEntregaFluxoModal2() {
        var root = document.getElementById('modal-pdv-entrega-fluxo-2');
        if (!root) return;
        root.classList.remove('hidden');
        root.classList.add('flex');
        pdvEnsureModalOpenBody();
    }

    function closeEntregaFluxoModal3() {
        var root = document.getElementById('modal-pdv-entrega-fluxo-3-troco');
        if (root) {
            root.classList.add('hidden');
            root.classList.remove('flex');
        }
        pdvTryRemoveModalOpenBody();
    }

    function openEntregaFluxoModal3() {
        var inp = document.getElementById('pdv-ef3-troco-input');
        var st = State.getState();
        if (inp) {
            inp.value = String((st.entrega && st.entrega.troco) || '').trim();
            setTimeout(function () {
                try {
                    inp.focus();
                    inp.select();
                } catch (eI) {}
            }, 80);
        }
        var root = document.getElementById('modal-pdv-entrega-fluxo-3-troco');
        if (!root) return;
        root.classList.remove('hidden');
        root.classList.add('flex');
        pdvEnsureModalOpenBody();
    }

    function openQuickClientPicker() {
        if (!dom.quickClientPicker) return;
        clientListSelectIdx = -1;
        dom.quickClientPicker.classList.remove('hidden');
        dom.quickClientSearch.focus();
    }

    function closeQuickClientPicker() {
        if (!dom.quickClientPicker) return;
        dom.quickClientPicker.classList.add('hidden');
        if (dom.quickClientResults) dom.quickClientResults.classList.add('hidden');
        dom.quickClientSearch.value = '';
        clientListSelectIdx = -1;
        focusProductSearch();
    }

    function highlightClientListRow() {
        if (!dom.quickClientResults) return;
        var rows = dom.quickClientResults.querySelectorAll('[data-client-list-idx]');
        for (var i = 0; i < rows.length; i++) {
            var el = rows[i];
            var idx = parseInt(el.getAttribute('data-client-list-idx') || '-1', 10);
            var on = idx === clientListSelectIdx;
            el.classList.toggle('ring-2', on);
            el.classList.toggle('ring-emerald-400', on);
            el.classList.toggle('bg-emerald-50', on);
            el.setAttribute('aria-selected', on ? 'true' : 'false');
            if (on) {
                try {
                    el.scrollIntoView({ block: 'nearest' });
                } catch (errS) {}
            }
        }
    }

    function resetProductSearchUi(message) {
        dom.productSearch.value = '';
        productSelectionIndex = -1;
        renderProductResults([]);
        dom.productSearchMeta.textContent = 'Aguardando busca';
        dom.productSearchFeedback.textContent = message || 'Digite para filtrar o catálogo local.';
        focusProductSearch();
        updateSearchAwaitingPulse();
    }

    function runProductSearch(term, mode) {
        var query = String(term || '').trim();
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
        dom.productSearchFeedback.textContent = catalogReady ? 'Filtrando…' : 'Carregando catálogo local…';
        loadWizardCatalog()
            .then(function () {
                if (seq !== filterSeq) return;
                var r = filterCatalogLocal(query, mode);
                if (r.barcodeHit) {
                    State.addItem(r.barcodeHit, 1);
                    resetProductSearchUi('Item adicionado pela leitura do código.');
                    return Promise.resolve();
                }
                if (r.list.length) {
                    renderProductResults(r.list);
                    dom.productSearchFeedback.textContent =
                        'Cache local (' + wizardProductCatalog.length + ' produtos).';
                    return Promise.resolve();
                }
                dom.productSearchFeedback.textContent = 'Buscando no servidor…';
                return fetchWizardServerSearch(query);
            })
            .then(function (remote) {
                if (seq !== filterSeq) return;
                if (!Array.isArray(remote)) return;
                if (remote.length) {
                    renderProductResults(remote);
                    dom.productSearchFeedback.textContent =
                        remote.length + ' encontrado(s) no servidor (fora do cache).';
                } else {
                    renderProductResults([]);
                    dom.productSearchFeedback.textContent =
                        'Nenhum produto para este termo (cache e servidor).';
                }
            })
            .catch(function () {
                if (seq !== filterSeq) return;
                dom.productSearchFeedback.textContent =
                    'Não foi possível carregar o catálogo ou buscar. Atualize a página.';
                renderProductResults([]);
            });
    }

    function runClientSearch(term) {
        var query = String(term || '').trim();
        if (query.length < 2) {
            dom.quickClientResults.innerHTML = '';
            dom.quickClientResults.classList.add('hidden');
            delete dom.quickClientResults._clientes;
            clientListSelectIdx = -1;
            return;
        }
        var seq = ++clientSearchSeq;
        fetch(urls.apiBuscarClientes + '?q=' + encodeURIComponent(query), { credentials: 'same-origin' })
            .then(function (res) { return res.json(); })
            .then(function (data) {
                if (seq !== clientSearchSeq) return;
                var clientes = data.clientes || [];
                if (!clientes.length) {
                    dom.quickClientResults.innerHTML =
                        '<div class="px-3 py-3 text-sm font-bold text-slate-400">Nenhum cliente encontrado.</div>';
                    dom.quickClientResults.classList.remove('hidden');
                    delete dom.quickClientResults._clientes;
                    clientListSelectIdx = -1;
                    return;
                }
                dom.quickClientResults.innerHTML = clientes.map(function (cliente, idx) {
                    return '' +
                        '<button type="button" role="option" class="block w-full rounded-xl px-3 py-3 text-left hover:bg-emerald-50/80 focus:outline-none" ' +
                        'data-select-client="' +
                        escapeHtml(cliente.id) +
                        '" data-client-list-idx="' +
                        idx +
                        '" aria-selected="false">' +
                        '  <span class="block text-sm font-black text-slate-900">' +
                        escapeHtml(cliente.nome) +
                        '</span>' +
                        '  <span class="mt-1 block text-[11px] font-bold text-slate-500">' +
                        escapeHtml(cliente.documento || cliente.telefone || cliente.endereco || 'Sem dados extras') +
                        '</span>' +
                        '</button>';
                }).join('');
                dom.quickClientResults.classList.remove('hidden');
                dom.quickClientResults._clientes = clientes;
                clientListSelectIdx = 0;
                highlightClientListRow();
            })
            .catch(function () {
                if (seq !== clientSearchSeq) return;
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

    function jsonPost(url, payload) {
        return fetch(url, {
            method: 'POST',
            credentials: 'same-origin',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': csrfToken()
            },
            body: JSON.stringify(payload || {})
        }).then(function (res) {
            return res.json().then(function (data) {
                return { ok: res.ok, status: res.status, data: data };
            });
        });
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
        return draft;
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
        if (cliente.id && !/^local:/i.test(cliente.id) && !/^erp-doc:/i.test(cliente.id)) {
            payload.cliente_id = cliente.id;
        }
        if (cliente.documento) payload.cliente_documento = cliente.documento;
        return payload;
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
            operador: '',
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
        return out;
    }

    function buildSaleReceiptHtml(state, computed) {
        var formaTxt = formaPagamentoResumoUi(state, computed);
        var obs = String((state.pagamento && state.pagamento.observacaoFinal) || '').trim();
        var extraLinhas = '';
        if (computed.desconto > 0.009) {
            extraLinhas +=
                '<div class="line">Desconto: ' + escapeHtml(formatMoney(computed.desconto)) + '</div>';
        }
        if (computed.frete > 0.009) {
            extraLinhas += '<div class="line">Frete: ' + escapeHtml(formatMoney(computed.frete)) + '</div>';
        }
        return (
            '<!DOCTYPE html><html><head><meta charset="utf-8"><title>Cupom — PDV</title><style>' +
            'body{font-family:system-ui,Segoe UI,Arial,sans-serif;padding:18px;color:#111827;max-width:440px;margin:0 auto}' +
            'h1{font-size:18px;margin:0 0 6px;font-weight:900}' +
            '.sub{font-size:10px;color:#64748b;text-transform:uppercase;letter-spacing:.12em;margin-bottom:12px}' +
            '.line{font-size:12px;margin:6px 0}' +
            'table{width:100%;border-collapse:collapse;margin-top:10px}' +
            'td{padding:5px 0;border-bottom:1px dashed #cbd5e1;font-size:12px}' +
            '.tot{font-weight:900;font-size:20px;margin-top:14px;padding-top:10px;border-top:2px solid #94a3b8}' +
            '</style></head><body>' +
            '<h1>GM Agro</h1>' +
            '<div class="sub">Cupom de venda</div>' +
            '<div class="line"><strong>Cliente:</strong> ' + escapeHtml(currentClientName(state)) + '</div>' +
            '<div class="line"><strong>Pagamento:</strong> ' + escapeHtml(formaTxt || '—') + '</div>' +
            (obs ? '<div class="line"><strong>Obs.:</strong> ' + escapeHtml(obs) + '</div>' : '') +
            '<table><tbody>' +
            (state.itens || [])
                .map(function (item) {
                    return (
                        '<tr><td>' +
                        escapeHtml(item.qtd + '× ' + item.nome) +
                        '</td><td style="text-align:right">' +
                        escapeHtml(formatMoney(item.qtd * item.preco)) +
                        '</td></tr>'
                    );
                })
                .join('') +
            '</tbody></table>' +
            '<div class="line" style="margin-top:10px;font-size:11px;color:#64748b">Subtotal: ' +
            escapeHtml(formatMoney(computed.subtotal)) +
            '</div>' +
            extraLinhas +
            '<div class="tot">Total: ' + escapeHtml(formatMoney(computed.total)) + '</div>' +
            '</body></html>'
        );
    }

    function printSaleReceiptWindow(win, state, computed) {
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
        }
        if (p) {
            p.disabled = !!busy;
            p.textContent = busy ? 'Confirmando…' : '';
        }
        if (!busy) {
            if (n) {
                n.innerHTML =
                    'Confirmar sem impressão <kbd class="ml-1 rounded border border-emerald-300 bg-emerald-50 px-1.5 py-0.5 font-mono text-[10px]">F8</kbd>';
            }
            if (p) {
                p.innerHTML =
                    'Confirmar com impressão <kbd class="ml-1 rounded bg-emerald-500 px-1.5 py-0.5 font-mono text-[10px] text-white">F9</kbd>';
            }
            State.setPagamentoField('observacaoFinal', State.getState().pagamento.observacaoFinal || '');
        }
    }

    function confirmSale(withPrint) {
        var state = State.getState();
        var computed = State.getComputed();
        var validation = canAdvance(Object.assign({}, state, { currentStep: 'pagamento' }), computed);
        if (validation) {
            alert(validation);
            return;
        }
        var printWin = null;
        if (withPrint) {
            printWin = window.open('about:blank', 'pdv_cupom_venda', 'width=480,height=720,scrollbars=yes');
            if (!printWin) {
                alert(
                    'Não foi possível abrir a janela do cupom. Permita pop-ups para este site e use de novo “Confirmar com impressão”.'
                );
            }
        }
        State.setPagamentoField('imprimirCupom', !!withPrint);
        state = State.getState();
        setConfirmButtonsBusy(true);
        if (window.gmLoadingBar) window.gmLoadingBar.show();

        jsonPost(urls.apiPdvSalvarCheckoutDraft, buildCheckoutDraftPayload(state, computed))
            .then(function (draftRes) {
                if (!draftRes.ok || !draftRes.data.ok) throw new Error((draftRes.data && (draftRes.data.erro || draftRes.data.mensagem)) || 'Falha ao salvar rascunho.');
                return jsonPost(urls.apiEnviarPedidoErp, buildErpPayload(state, computed));
            })
            .then(function (erpRes) {
                if (!erpRes.ok || !erpRes.data.ok) throw new Error((erpRes.data && (erpRes.data.erro || erpRes.data.mensagem)) || 'Falha ao confirmar venda.');
                if (!state.entrega.ativa) return { entrega: null, erp: erpRes.data };
                return jsonPost(urls.apiEntregaRegistrar, buildEntregaPayload(state, computed)).then(function (entRes) {
                    if (!entRes.ok || !entRes.data.ok) throw new Error((entRes.data && (entRes.data.erro || entRes.data.mensagem)) || 'Venda salva, mas falhou ao registrar entrega.');
                    return { entrega: entRes.data, erp: erpRes.data };
                });
            })
            .then(function (result) {
                if (withPrint && printWin && !printWin.closed) {
                    var stP = State.getState();
                    var compP = State.getComputed();
                    if (!printSaleReceiptWindow(printWin, stP, compP)) {
                        alert('O cupom foi confirmado no sistema, mas a impressão falhou. Tente reimprimir pela lista de vendas, se disponível.');
                    }
                }
                jsonPost(urls.apiPdvLimparCheckoutDraft, {}).catch(function () {});
                alert(result.entrega
                    ? 'Venda confirmada e entrega registrada com sucesso.'
                    : 'Venda confirmada com sucesso.');
                State.reset(true);
                State.setCurrentStep('produtos');
            })
            .catch(function (err) {
                if (printWin && !printWin.closed) {
                    try {
                        printWin.close();
                    } catch (errC) {}
                }
                alert(err && err.message ? err.message : 'Falha ao confirmar venda.');
            })
            .finally(function () {
                if (window.gmLoadingBar) window.gmLoadingBar.hide();
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
    }

    function focusFirstFlowFieldForForma(forma) {
        setTimeout(function () {
            var st = State.getState();
            var mid = String((st.pagamento && st.pagamento.maquinaId) || '').trim();
            var tr = document.getElementById('pdv-pay-valor-tranche');
            if (forma === 'Dinheiro' && dom.paymentReceived) dom.paymentReceived.focus();
            else if (forma === 'PIX') {
                if (!mid) {
                    var bp = document.getElementById('pdv-pay-open-maquinas-pix');
                    if (bp) bp.focus();
                    return;
                }
                if (tr) tr.focus();
            } else if (
                forma === 'Cartão de débito' ||
                forma === 'Cartão de crédito' ||
                forma === 'Crédito parcelado'
            ) {
                if (!mid) {
                    var bc = document.getElementById('pdv-pay-open-maquinas-card');
                    if (bc) bc.focus();
                    return;
                }
                if (forma === 'Crédito parcelado' && dom.paymentParcelasCredito) dom.paymentParcelasCredito.focus();
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
            outroPinVerificado: forma === 'Outro' ? !!st.pagamento.outroPinVerificado : false
        };
        if (forma === 'Vale crédito') {
            var sv = Math.min(Number(pagamentoUi.saldoValeCredito || 0), rest);
            patch.valorDestaForma = sv > 0 ? String(sv.toFixed(2)).replace('.', ',') : '';
        } else if (forma === 'Cashback') {
            var scb = Math.min(Number(pagamentoUi.saldoCashback || 0), rest);
            patch.valorDestaForma = scb > 0 ? String(scb.toFixed(2)).replace('.', ',') : '';
        } else if (forma === 'Fiado') {
            patch.valorDestaForma = rest > 0 ? String(rest.toFixed(2)).replace('.', ',') : '';
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
            Digit5: 'Crédito parcelado',
            Digit6: 'Fiado',
            Digit7: 'Vale crédito',
            Digit8: 'Cashback',
            Digit9: 'Outro'
        };
        return map[c] || '';
    }

    function handleValorTrancheEnter(event) {
        if (event.key !== 'Enter') return;
        var tag = (event.target && event.target.tagName) || '';
        if (tag === 'TEXTAREA') return;
        var inp = document.getElementById('pdv-pay-valor-tranche');
        if (!inp || event.target !== inp) return;
        event.preventDefault();
        var st = State.getState();
        if (st.currentStep !== 'pagamento') return;
        var comp = State.getComputed();
        var rest = saldoRestantePagamento(st, comp);
        var raw = String(inp.value || '').trim();
        var cur = raw ? State.toNumber(raw) : 0;
        if (!raw || cur <= 0.009) {
            if (rest <= 0.009) return;
            var fmt = String(rest.toFixed(2)).replace('.', ',');
            inp.value = fmt;
            State.setPagamentoField('valorDestaForma', fmt);
            return;
        }
        var err = erroCommitTranche(st, comp, cur);
        if (err) {
            alert(err);
            return;
        }
        State.addPagamentoLancamento(snapshotLancamentoFromState(st, cur));
        afterCommitTrancheFlow();
    }

    function handlePaymentReceivedEnter(event) {
        if (event.key !== 'Enter') return;
        var tag = (event.target && event.target.tagName) || '';
        if (tag === 'TEXTAREA') return;
        if (!dom.paymentReceived || event.target !== dom.paymentReceived) return;
        event.preventDefault();
        var st = State.getState();
        if (st.currentStep !== 'pagamento' || st.pagamento.forma !== 'Dinheiro') return;
        var comp = State.getComputed();
        var rest = saldoRestantePagamento(st, comp);
        var raw = String(dom.paymentReceived.value || '').trim();
        var cur = raw ? State.toNumber(raw) : 0;
        if (!raw || cur <= 0.009) {
            if (rest <= 0.009) return;
            var fmt = String(rest.toFixed(2)).replace('.', ',');
            dom.paymentReceived.value = fmt;
            State.setPagamentoField('valorRecebido', fmt);
            State.setPagamentoField('trocoCalculado', '');
            return;
        }
        var R = cur;
        var T = Math.min(R, rest);
        var err = erroCommitTranche(st, comp, T);
        if (err) {
            alert(err);
            return;
        }
        var trocoVal = R > rest + 0.009 ? R - rest : 0;
        State.addPagamentoLancamento(
            snapshotLancamentoFromState(st, T, {
                valorRecebido: String(R.toFixed(2)).replace('.', ','),
                trocoCalculado: trocoVal > 0.009 ? String(trocoVal.toFixed(2)).replace('.', ',') : ''
            })
        );
        afterCommitTrancheFlow();
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

    function openPayPopDinheiroResumo() {
        var dlg = document.getElementById('pdv-pay-pop-dinheiro');
        if (!dlg) return;
        var st = State.getState();
        var comp = State.getComputed();
        var totalEl = document.getElementById('pdv-pay-pop-din-total');
        var recEl = document.getElementById('pdv-pay-pop-din-recebido');
        var trocoEl = document.getElementById('pdv-pay-pop-din-troco');
        var restDin = saldoRestantePagamento(st, comp);
        if (totalEl) totalEl.textContent = formatMoney(restDin);
        var recRaw = String(st.pagamento.valorRecebido || '').trim();
        if (recEl) recEl.textContent = recRaw ? formatMoney(State.toNumber(recRaw)) : '—';
        var tr = String(st.pagamento.trocoCalculado || '').trim();
        if (trocoEl) {
            if (tr) {
                trocoEl.textContent = formatMoney(State.toNumber(tr));
            } else {
                var recN = State.toNumber(st.pagamento.valorRecebido);
                if (recN > restDin + 0.009) trocoEl.textContent = formatMoney(recN - restDin);
                else trocoEl.textContent = '—';
            }
        }
        showPayFlowDialog(dlg);
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
                } else {
                    alert('PIN inválido.');
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

    /** Igual htmlPagSeparacao do painel Entregas. */
    function wizardPrintHtmlSeparacao(e) {
        var items = Array.isArray(e.itens_json) ? e.itens_json : [];
        var dh = String(e.criado_em || '').replace('T', ' ').slice(0, 19);
        var h = '<div class="pg">';
        h += '<div style="text-align:center;font-weight:900;font-size:14px;letter-spacing:0.04em;">SEPARAÇÃO</div>';
        h += '<div style="font-size:9px;margin:6px 0 8px;color:#333;">' + escapeHtml(dh) + '</div>';
        h += '<div style="font-weight:bold;font-size:12px;line-height:1.25;">' + escapeHtml(e.cliente_nome) + '</div>';
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
        h += '<div style="margin-top:12px;text-align:center;">';
        h += '<svg id="barc-orc" xmlns="http://www.w3.org/2000/svg"></svg>';
        h += '<div style="font-size:9px;margin-top:6px;">Bipe no PDV para retomar o orçamento</div></div>';
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
                '<div style="margin-top:6px;line-height:1.3;">' +
                escapeHtml(String(it.qtd != null ? it.qtd : '') + '× ' + String(it.nome || '')) +
                '</div>';
        });
        var h = '<div class="pg">';
        h += '<div style="text-align:center;font-weight:900;font-size:13px;">ENTREGA</div>';
        h += '<div style="font-size:26px;font-weight:900;text-align:center;line-height:1;margin:10px 0 8px;letter-spacing:-0.02em;">' + escapeHtml(primeiro) + '</div>';
        h += '<div style="font-size:10px;">' + escapeHtml(dh) + '</div>';
        h += '<div style="margin-top:8px;line-height:1.3;"><b>Cliente</b> ' + escapeHtml(nomeCli) + '</div>';
        h += '<div style="border-top:1px dashed #000;margin:8px 0;"></div>';
        h += entItems;
        h += '<div style="margin-top:8px;"><b>Endereço</b></div>';
        h += '<div style="font-size:10px;word-break:break-word;line-height:1.35;">' + escapeHtml(end || '—') + '</div>';
        h += qrImg;
        h += '</div>';
        return h;
    }

    /** Igual htmlPagCupom do painel Entregas (marca AGRO MAIS + linha de agradecimento). */
    function wizardPrintHtmlCupom(e) {
        var dh = String(e.criado_em || '').replace('T', ' ').slice(0, 19);
        var items = Array.isArray(e.itens_json) ? e.itens_json : [];
        var lines = '';
        items.forEach(function (it) {
            var q = Number(it.qtd != null ? it.qtd : 0);
            var preco = Number(it.preco != null ? it.preco : 0);
            var sub = isFinite(q) && isFinite(preco) ? q * preco : NaN;
            var subTxt = isFinite(sub) ? wizardPrintMoedaCupom(sub) : '—';
            lines +=
                '<div style="display:flex;justify-content:space-between;gap:4px;margin:3px 0;font-size:10px;">' +
                '<span style="flex:1;">' +
                escapeHtml(String(it.qtd != null ? it.qtd : '') + '× ' + String(it.nome || '').slice(0, 36)) +
                '</span><span style="white-space:nowrap;">' + escapeHtml(subTxt) + '</span></div>';
        });
        var bc = wizardPrintCodigoBarrasEntrega(e);
        var h = '<div class="pg">';
        h += '<div style="text-align:center;font-weight:900;font-size:14px;">AGRO MAIS</div>';
        h += '<div style="text-align:center;font-size:10px;margin:2px 0;">Orçamento (não fiscal)</div>';
        h += '<div style="font-size:10px;">' + escapeHtml(dh) + '</div>';
        h += '<div style="border-top:1px dashed #000;margin:6px 0;"></div>';
        h += '<div style="font-weight:bold;margin-bottom:4px;">' + escapeHtml(e.cliente_nome) + '</div>';
        h += lines;
        h += '<div style="border-top:2px solid #000;margin:8px 0 4px;padding-top:4px;font-weight:900;font-size:13px;display:flex;justify-content:space-between;">';
        h += '<span>TOTAL</span><span>' + escapeHtml(String(e.total_texto || '—')) + '</span></div>';
        h += '<div style="text-align:center;font-size:9px;margin-top:8px;">Obrigado — apresente na retirada</div>';
        h += '<div style="text-align:center;font-size:8px;margin-top:4px;word-break:break-all;">Retomar: ' + escapeHtml(bc) + '</div>';
        h += '</div>';
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
        var inner = parts.join('');
        var docHtml =
            '<!DOCTYPE html><html><head><meta charset="utf-8"><title>Entrega PDV</title><style>' +
            '@page{margin:0;size:80mm 200mm}' +
            'html,body{margin:0;padding:0}' +
            'body{font-family:system-ui,sans-serif;-webkit-print-color-adjust:exact;print-color-adjust:exact}' +
            '.pg{width:72mm;margin:0 auto;padding:4mm 3mm;font-size:11px;line-height:1.35;box-sizing:border-box;page-break-after:always;break-after:page}' +
            '.pg:last-child{page-break-after:auto;break-after:auto}' +
            '</style></head><body>' +
            inner +
            '</body></html>';

        var iframe = document.getElementById('agro-print-iframe-entregas-pdv');
        if (!iframe) {
            iframe = document.createElement('iframe');
            iframe.id = 'agro-print-iframe-entregas-pdv';
            iframe.title = 'Impressão entrega PDV';
            iframe.setAttribute('aria-hidden', 'true');
            iframe.style.cssText =
                'position:fixed;right:0;bottom:0;width:0;height:0;border:0;opacity:0;pointer-events:none;';
            document.body.appendChild(iframe);
        }
        var idoc = iframe.contentDocument || iframe.contentWindow.document;
        idoc.open();
        idoc.write(docHtml);
        idoc.close();

        var runPrint = function () {
            try {
                var svg = idoc.getElementById('barc-orc');
                var iw = iframe.contentWindow;
                if (svg && iw && typeof iw.JsBarcode !== 'undefined') {
                    iw.JsBarcode(svg, barcodeVal, {
                        format: 'CODE128',
                        width: 1.35,
                        height: 44,
                        displayValue: true,
                        fontSize: 11,
                        margin: 0,
                        marginTop: 4,
                        marginBottom: 2
                    });
                }
            } catch (eBr) {}
            setTimeout(function () {
                try {
                    iframe.contentWindow.focus();
                    iframe.contentWindow.print();
                } catch (ePr) {}
            }, 100);
        };
        var old = idoc.querySelector('script[data-agro-jsbarcode-pdv]');
        if (old) old.remove();
        var s = idoc.createElement('script');
        s.setAttribute('data-agro-jsbarcode-pdv', '1');
        s.src = 'https://cdn.jsdelivr.net/npm/jsbarcode@3.11.5/dist/JsBarcode.all.min.js';
        s.onload = runPrint;
        s.onerror = runPrint;
        idoc.head.appendChild(s);
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
        if (!enderecoEntregaMinimoOk(state)) {
            alert('Preencha logradouro e bairro (ou endereço legível) para entrega.');
            return;
        }
        var lp = String((state.entrega && state.entrega.localPagamento) || '');
        var meio = String((state.entrega && state.entrega.meioNaEntrega) || '');
        if (lp !== 'entrega' || !meio) {
            alert('Defina pagamento na entrega e escolha dinheiro ou cartão no pop-up.');
            return;
        }
        wizardModalEscolhaImpressaoEntrega().then(function (opt) {
            if (!opt) return;
            var orcId = Date.now();
            wizardImprimirPacoteEntrega(orcId, opt);
            var state2 = State.getState();
            var computed2 = State.getComputed();
            var body = buildEntregaPayload(state2, computed2, {
                orc_local_id: orcId,
                retomar_codigo: 'GMORC' + String(orcId),
                obsExtra: obsFluxoEntregaResumo(state2)
            });
            if (window.gmLoadingBar) window.gmLoadingBar.show();
            jsonPost(urls.apiEntregaRegistrar || '', body)
                .then(function (res) {
                    if (!res.ok || !res.data || !res.data.ok) {
                        throw new Error((res.data && (res.data.erro || res.data.mensagem)) || 'Falha ao registrar no painel Entregas.');
                    }
                    alert(
                        'Entrega registrada no painel (retomada GMORC' +
                            orcId +
                            '). O PDV voltou ao início para uma nova venda.'
                    );
                    State.reset(true);
                    State.setCurrentStep('produtos');
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
        if (!enderecoEntregaMinimoOk(state)) {
            alert('Preencha logradouro e bairro (ou endereço legível) para entrega.');
            return;
        }
        if (String((state.entrega && state.entrega.localPagamento) || '') !== 'loja') {
            alert('Esta ação é para pagamento na loja. Escolha essa opção no pop-up da etapa Entrega.');
            return;
        }
        wizardModalEscolhaImpressaoEntrega().then(function (opt) {
            if (!opt) return;
            var orcId = Date.now();
            wizardImprimirPacoteEntrega(orcId, opt);
            State.setCurrentStep('pagamento');
        });
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
            var computed = State.getComputed();
            var target = prevStep(state, computed);
            if (target) State.setCurrentStep(target);
        });

        dom.btnNext.addEventListener('click', function () {
            var state = State.getState();
            var computed = State.getComputed();
            if (state.currentStep === 'entrega') {
                var lp = String((state.entrega && state.entrega.localPagamento) || '');
                if (!lp) {
                    openEntregaFluxoModal1();
                    return;
                }
                if (lp === 'loja') {
                    wizardIrParaPagamentoComImpressao();
                    return;
                }
                if (lp === 'entrega') {
                    wizardEnviarEntregaPainel();
                    return;
                }
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

        if (dom.quickClientPickerClose) {
            dom.quickClientPickerClose.addEventListener('click', closeQuickClientPicker);
        }

        dom.quickClientSearch.addEventListener('input', function () {
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
                    closeQuickClientPicker();
                }
            }
        });

        dom.quickClientResults.addEventListener('click', function (event) {
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
            closeQuickClientPicker();
        });

        dom.productSearch.addEventListener('keydown', function (event) {
            if (event.key === 'ArrowDown') {
                if (!lastProducts.length) return;
                event.preventDefault();
                var acCap = Math.min(lastProducts.length, AUTOCOMPLETE_LIMIT);
                productSelectionIndex = Math.min(productSelectionIndex + 1, Math.max(acCap - 1, 0));
                renderProductResults(lastProducts);
            } else if (event.key === 'ArrowUp') {
                if (!lastProducts.length) return;
                event.preventDefault();
                productSelectionIndex = Math.max(productSelectionIndex - 1, 0);
                renderProductResults(lastProducts);
            } else if (event.key === 'Enter') {
                event.preventDefault();
                var target = lastProducts[Math.max(productSelectionIndex, 0)];
                if (target) {
                    State.addItem(target, 1);
                    dom.productSearch.value = '';
                    renderProductResults([]);
                    dom.productSearchFeedback.textContent = 'Item adicionado à venda.';
                } else {
                    runProductSearch(dom.productSearch.value, 'manual');
                }
            } else if (event.key === '+' || event.key === '=' || event.code === 'NumpadAdd') {
                event.preventDefault();
                bumpLastCartItem(1);
            } else if (event.key === '-' || event.code === 'NumpadSubtract') {
                event.preventDefault();
                bumpLastCartItem(-1);
            }
        });

        dom.productSearch.addEventListener('input', function () {
            var value = dom.productSearch.value;
            var now = Date.now();
            var delta = now - lastInputAt;
            lastInputAt = now;
            clearTimeout(searchTimer);
            clearTimeout(barcodeTimer);
            if (/^\d{6,}$/.test(String(value).trim()) && delta < 35) {
                barcodeTimer = setTimeout(function () {
                    runProductSearch(value, 'barcode');
                }, 60);
                return;
            }
            searchTimer = setTimeout(function () {
                runProductSearch(value, 'manual');
            }, 220);
        });

        if (dom.productAutocomplete) {
            dom.productAutocomplete.addEventListener('click', function (event) {
                var zoom = event.target.closest('[data-pdv-photo-zoom]');
                if (zoom) {
                    event.preventDefault();
                    event.stopPropagation();
                    openProductPhotoPop(zoom.getAttribute('data-pdv-photo-zoom') || '');
                    return;
                }
                var btn = event.target.closest('[data-add-product]');
                if (!btn) return;
                var id = btn.getAttribute('data-add-product');
                var produto = lastProducts.find(function (item) { return String(item.id) === String(id); });
                if (!produto) return;
                State.addItem(produto, 1);
                resetProductSearchUi('Item adicionado à venda.');
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
                var current = State.getState().itens.find(function (item) { return String(item.id) === String(id); });
                if (!current) return;
                var nextQty = current.qtd + parseInt(qtyBtn.getAttribute('data-item-delta') || '0', 10);
                if (nextQty <= 0) {
                    State.removeItem(id);
                } else {
                    State.updateItemQuantity(id, nextQty);
                }
            }
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
                var target = nextStep(state, computed);
                if (target) State.setCurrentStep(target);
            });
        }

        if (dom.step1Payment) {
            dom.step1Payment.addEventListener('click', function () {
                var state = State.getState();
                var computed = State.getComputed();
                if (!state.itens.length) {
                    alert('Adicione ao menos 1 item antes de ir para pagamento.');
                    return;
                }
                if (state.clienteMode === 'unset') {
                    alert('Defina o cliente ou consumidor final antes de ir para pagamento.');
                    return;
                }
                State.setCurrentStep('pagamento');
            });
        }

        if (dom.openBudgetHistory) dom.openBudgetHistory.addEventListener('click', openBudgetHistory);
        if (dom.step1BudgetVerMais) dom.step1BudgetVerMais.addEventListener('click', openBudgetHistory);
        dom.budgetHistoryClose.addEventListener('click', closeBudgetHistory);
        dom.budgetHistoryModal.addEventListener('click', function (event) {
            if (event.target === dom.budgetHistoryModal) closeBudgetHistory();
        });
        dom.budgetHistoryList.addEventListener('click', function (event) {
            var btn = event.target.closest('[data-budget-index]');
            if (!btn) return;
            var index = parseInt(btn.getAttribute('data-budget-index') || '-1', 10);
            var historico = [];
            try {
                historico = JSON.parse(localStorage.getItem('historicoOrcamentos') || '[]');
            } catch (err) {}
            if (historico[index]) {
                State.hydrateFromBudget(historico[index]);
                State.setCurrentStep('produtos');
                closeBudgetHistory();
            }
        });

        if (dom.clientPurchaseHistory) {
            dom.clientPurchaseHistory.addEventListener('click', function () {
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
            var c = Object.assign({}, state.cliente, {
                logradouro: dom.clienteLogradouro ? dom.clienteLogradouro.value.trim() : '',
                numero: dom.clienteNumero ? dom.clienteNumero.value.trim() : '',
                bairro: dom.clienteBairro ? dom.clienteBairro.value : '',
                plus_code: dom.clientePluscode ? dom.clientePluscode.value.trim() : ''
            });
            c.endereco = composeClienteEnderecoLinha(c);
            State.setCliente(c, state.clienteMode === 'consumidor_final' ? 'consumidor_final' : 'cliente');
        }

        [dom.clienteLogradouro, dom.clienteNumero, dom.clientePluscode].forEach(function (el) {
            if (el) el.addEventListener('input', commitClienteEditCampos);
        });
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

        dom.entregaRadios.forEach(function (radio) {
            radio.addEventListener('change', function () {
                State.setEntregaField('ativa', radio.value === 'entrega');
            });
        });

        function commitEntregaCamposEndereco() {
            var st = State.getState();
            var e0 = st.entrega || {};
            var e = Object.assign({}, e0, {
                logradouro: dom.entregaLogradouro ? dom.entregaLogradouro.value.trim() : '',
                numero: dom.entregaNumero ? dom.entregaNumero.value.trim() : '',
                bairro: dom.entregaBairro ? dom.entregaBairro.value : '',
                plusCode: dom.entregaPluscode ? dom.entregaPluscode.value.trim() : ''
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

        [dom.entregaLogradouro, dom.entregaNumero, dom.entregaPluscode].forEach(function (el) {
            if (el) el.addEventListener('input', commitEntregaCamposEndereco);
        });
        if (dom.entregaBairro) dom.entregaBairro.addEventListener('change', commitEntregaCamposEndereco);

        [
            [dom.vendaObservacao, function () { State.setVendaField('observacao', dom.vendaObservacao.value); }],
            [dom.entregaComplemento, function () { State.setEntregaField('complemento', dom.entregaComplemento.value); }],
            [dom.entregaReferencia, function () { State.setEntregaField('referencia', dom.entregaReferencia.value); }],
            [
                dom.entregaHorario,
                function () {
                    State.setEntregaField('horario', dom.entregaHorario.value);
                    wizardSyncLembretesFromEntregaHorario();
                }
            ],
            [dom.entregaTroco, function () { State.setEntregaField('troco', dom.entregaTroco.value); }],
            [dom.entregaObservacao, function () { State.setEntregaField('observacao', dom.entregaObservacao.value); }],
            [dom.paymentDiscount, function () { State.setPagamentoField('descontoGeral', State.toNumber(dom.paymentDiscount.value)); }],
            [dom.paymentShipping, function () { State.setPagamentoField('frete', State.toNumber(dom.paymentShipping.value)); }],
            [dom.paymentReceived, function () {
                State.setPagamentoField('valorRecebido', dom.paymentReceived.value);
                var state = State.getState();
                var computed = State.getComputed();
                var recebido = State.toNumber(dom.paymentReceived.value);
                var rest = saldoRestantePagamento(state, computed);
                if (recebido > rest + 0.009) {
                    State.setPagamentoField('trocoCalculado', String((recebido - rest).toFixed(2)).replace('.', ','));
                } else {
                    State.setPagamentoField('trocoCalculado', '');
                }
            }],
            [dom.paymentChange, function () { State.setPagamentoField('trocoCalculado', dom.paymentChange.value); }],
            [dom.paymentNote, function () { State.setPagamentoField('observacaoFinal', dom.paymentNote.value); }]
        ].forEach(function (entry) {
            if (entry[0]) entry[0].addEventListener('input', entry[1]);
            if (entry[0] && entry[0].tagName === 'SELECT') entry[0].addEventListener('change', entry[1]);
        });

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
            dom.paymentValorForma.addEventListener('input', function () {
                State.setPagamentoField('valorDestaForma', dom.paymentValorForma.value);
            });
            dom.paymentValorForma.addEventListener('keydown', handleValorTrancheEnter);
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
                State.setPagamentoField('fiadoParcelas', Number.isFinite(n) && n >= 1 ? n : 1);
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
        var btnQrCard = document.getElementById('pdv-pay-open-qr-card');
        if (btnQrCard) btnQrCard.addEventListener('click', function () { openPayPopQr('Mercado Pago — Cartão'); });
        var btnDinResumo = document.getElementById('pdv-pay-open-dinheiro-resumo');
        if (btnDinResumo) btnDinResumo.addEventListener('click', openPayPopDinheiroResumo);
        var btnFiadoPop = document.getElementById('pdv-pay-open-fiado-pop');
        if (btnFiadoPop) btnFiadoPop.addEventListener('click', openPayPopFiado);
        var btnValePop = document.getElementById('pdv-pay-open-vale-pop');
        if (btnValePop) {
            btnValePop.addEventListener('click', function () {
                openPayPopSaldo(
                    'Vale crédito',
                    formatMoney(pagamentoUi.saldoValeCredito || 0),
                    'O valor usado na venda não pode passar deste saldo.'
                );
            });
        }
        var btnCbPop = document.getElementById('pdv-pay-open-cashback-pop');
        if (btnCbPop) {
            btnCbPop.addEventListener('click', function () {
                openPayPopSaldo(
                    'Cashback',
                    formatMoney(pagamentoUi.saldoCashback || 0),
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

        var maquinasListEl = document.getElementById('pdv-pay-maquinas-list');
        if (maquinasListEl) {
            maquinasListEl.addEventListener('click', function (event) {
                var btn = event.target.closest('[data-maquina-id]');
                if (!btn) return;
                var id = btn.getAttribute('data-maquina-id') || '';
                var nome = btn.getAttribute('data-maquina-nome') || id;
                State.setPagamentoPatch({ maquinaId: id, maquinaNome: nome });
                var md = document.getElementById('pdv-pay-pop-maquinas');
                if (md && typeof md.close === 'function') {
                    try {
                        md.close();
                    } catch (errM) {}
                }
                focusFirstFlowFieldForForma(State.getState().pagamento.forma);
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

        document.querySelectorAll('input[name="pdv-entrega-taxa-modo"]').forEach(function (radio) {
            radio.addEventListener('change', function () {
                if (!radio.checked) return;
                commitEntregaTaxaModo(radio.value);
            });
        });
        var inpTaxaValor = document.getElementById('pdv-entrega-taxa-valor');
        if (inpTaxaValor) {
            inpTaxaValor.addEventListener('input', commitEntregaTaxaValorInput);
            inpTaxaValor.addEventListener('blur', commitEntregaTaxaValorInput);
        }

        var btnReiniciarFluxoPagamento = document.getElementById('pdv-entrega-reiniciar-fluxo-pagamento');
        if (btnReiniciarFluxoPagamento) {
            btnReiniciarFluxoPagamento.addEventListener('click', function () {
                closeEntregaFluxoModal3();
                closeEntregaFluxoModal2();
                closeEntregaFluxoModal1();
                State.setEntregaPatch({ localPagamento: '', meioNaEntrega: '', troco: '' });
                State.setEntregaField('maquininha', '');
                if (dom.entregaTroco) dom.entregaTroco.value = '';
                openEntregaFluxoModal1();
            });
        }

        var mdFlux1 = document.getElementById('modal-pdv-entrega-fluxo-1');
        if (mdFlux1) {
            mdFlux1.addEventListener('click', function (ev) {
                if (ev.target === mdFlux1) closeEntregaFluxoModal1();
            });
        }
        var mdFlux2 = document.getElementById('modal-pdv-entrega-fluxo-2');
        if (mdFlux2) {
            mdFlux2.addEventListener('click', function (ev) {
                if (ev.target === mdFlux2) {
                    closeEntregaFluxoModal2();
                    openEntregaFluxoModal1();
                }
            });
        }
        var mdFlux3 = document.getElementById('modal-pdv-entrega-fluxo-3-troco');
        if (mdFlux3) {
            mdFlux3.addEventListener('click', function (ev) {
                if (ev.target === mdFlux3) {
                    closeEntregaFluxoModal3();
                    openEntregaFluxoModal2();
                }
            });
        }
        var btnEf1Entrega = document.getElementById('pdv-ef1-entrega');
        if (btnEf1Entrega) {
            btnEf1Entrega.addEventListener('click', function () {
                closeEntregaFluxoModal1();
                openEntregaFluxoModal2();
            });
        }
        var btnEf1Loja = document.getElementById('pdv-ef1-loja');
        if (btnEf1Loja) {
            btnEf1Loja.addEventListener('click', function () {
                State.setEntregaPatch({ localPagamento: 'loja', meioNaEntrega: '' });
                State.setEntregaField('maquininha', '');
                closeEntregaFluxoModal1();
            });
        }
        var btnEf2Din = document.getElementById('pdv-ef2-dinheiro');
        if (btnEf2Din) {
            btnEf2Din.addEventListener('click', function () {
                closeEntregaFluxoModal2();
                openEntregaFluxoModal3();
            });
        }
        var btnEf2Card = document.getElementById('pdv-ef2-cartao');
        if (btnEf2Card) {
            btnEf2Card.addEventListener('click', function () {
                State.setEntregaPatch({ localPagamento: 'entrega', meioNaEntrega: 'cartao' });
                State.setEntregaField('maquininha', 'sim');
                closeEntregaFluxoModal2();
            });
        }
        var btnEf3Ok = document.getElementById('pdv-ef3-ok');
        if (btnEf3Ok) {
            btnEf3Ok.addEventListener('click', function () {
                var inp = document.getElementById('pdv-ef3-troco-input');
                var val = inp ? String(inp.value || '').trim() : '';
                if (!val) {
                    alert('Informe o valor para troco (use 0 ou 0,00 se não precisar).');
                    return;
                }
                State.setEntregaPatch({ localPagamento: 'entrega', meioNaEntrega: 'dinheiro', troco: val });
                State.setEntregaField('maquininha', 'nao');
                closeEntregaFluxoModal3();
            });
        }
        var btnEf3Can = document.getElementById('pdv-ef3-cancelar');
        if (btnEf3Can) {
            btnEf3Can.addEventListener('click', function () {
                closeEntregaFluxoModal3();
                openEntregaFluxoModal2();
            });
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
                if (isEntregaFluxo3Open()) {
                    event.preventDefault();
                    closeEntregaFluxoModal3();
                    openEntregaFluxoModal2();
                    return;
                }
                if (isEntregaFluxo2Open()) {
                    event.preventDefault();
                    closeEntregaFluxoModal2();
                    openEntregaFluxoModal1();
                    return;
                }
                if (isEntregaFluxo1Open()) {
                    event.preventDefault();
                    closeEntregaFluxoModal1();
                    return;
                }
                if (isClienteEditModalOpen()) {
                    event.preventDefault();
                    closeClienteEditModal();
                    return;
                }
                if (dom.budgetHistoryModal && !dom.budgetHistoryModal.classList.contains('hidden')) {
                    event.preventDefault();
                    closeBudgetHistory();
                    return;
                }
                if (dom.quickClientPicker && !dom.quickClientPicker.classList.contains('hidden')) {
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
                !modalEntregaImpOpen &&
                !isAnyEntregaFluxoModalOpen()
            ) {
                event.preventDefault();
                if (dom.btnPrev && !dom.btnPrev.disabled) dom.btnPrev.click();
                return;
            }
            if (
                stProdutos.currentStep === 'produtos' &&
                !payFlowDialogOpen() &&
                !isPaymentFormaModalOpen() &&
                !startModalOpen
            ) {
                var pickerOpen = dom.quickClientPicker && !dom.quickClientPicker.classList.contains('hidden');
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
                    event.preventDefault();
                    if (dom.quickClientChange) dom.quickClientChange.click();
                    return;
                }
                if (event.code === 'F5' && !event.altKey && !event.ctrlKey && !event.metaKey) {
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
                stProdutos.currentStep === 'cliente' &&
                !payFlowDialogOpen() &&
                !isPaymentFormaModalOpen() &&
                !startModalOpen &&
                !isClienteEditModalOpen()
            ) {
                if (event.code === 'F7' && !event.altKey && !event.ctrlKey && !event.metaKey) {
                    event.preventDefault();
                    if (dom.btnNext && dom.btnNext.style.display !== 'none' && !dom.btnNext.disabled) {
                        dom.btnNext.click();
                    }
                    return;
                }
                var focoTipoEntrega =
                    event.target &&
                    event.target.closest &&
                    event.target.closest('input[name="pdv-entrega-tipo"]');
                if (!inField || focoTipoEntrega) {
                    var d1 = event.code === 'Digit1' || event.code === 'Numpad1';
                    var d2 = event.code === 'Digit2' || event.code === 'Numpad2';
                    if (d1 || d2) {
                        event.preventDefault();
                        State.setEntregaField('ativa', !!d2);
                        return;
                    }
                }
            }
            if (
                stProdutos.currentStep === 'entrega' &&
                !payFlowDialogOpen() &&
                !isPaymentFormaModalOpen() &&
                !startModalOpen &&
                !isClienteEditModalOpen() &&
                !isAnyEntregaFluxoModalOpen() &&
                !modalEntregaImpOpen
            ) {
                if (event.code === 'F7' && !event.altKey && !event.ctrlKey && !event.metaKey) {
                    event.preventDefault();
                    if (dom.btnNext && dom.btnNext.style.display !== 'none' && !dom.btnNext.disabled) {
                        dom.btnNext.click();
                    }
                    return;
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
                var stepByDigit = { Digit1: 'produtos', Digit2: 'cliente', Digit3: 'entrega', Digit4: 'pagamento' };
                if (stepByDigit[event.code]) {
                    event.preventDefault();
                    tryNavigateToStep(stepByDigit[event.code]);
                    return;
                }
            }
            if (event.code === 'KeyE' && !event.altKey && !event.ctrlKey && !event.metaKey && !inField) {
                var stE = State.getState();
                if (stE.currentStep === 'cliente') {
                    event.preventDefault();
                    openClienteEditModal();
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
                if (event.code === 'F8') {
                    event.preventDefault();
                    if (dom.confirmSaleNoPrint && !dom.confirmSaleNoPrint.disabled) confirmSale(false);
                    return;
                }
                if (event.code === 'F9') {
                    event.preventDefault();
                    if (dom.confirmSalePrint && !dom.confirmSalePrint.disabled) confirmSale(true);
                    return;
                }
                if (event.code === 'KeyT' && !event.target.closest('input,textarea,select')) {
                    var fa = document.getElementById('pdv-payment-flow-area');
                    if (fa && !fa.classList.contains('hidden')) {
                        event.preventDefault();
                        openPaymentFormaModal();
                    }
                    return;
                }
                if (event.code === 'KeyM' && !event.target.closest('input,textarea,select')) {
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

    loadWizardCatalog()
        .then(function () {
            if (dom.productSearchFeedback) {
                dom.productSearchFeedback.textContent = 'Catálogo local pronto. Digite para filtrar.';
            }
        })
        .catch(function () {
            if (dom.productSearchFeedback) {
                dom.productSearchFeedback.textContent = 'Não foi possível carregar o catálogo. Atualize a página.';
            }
        });

    var currentState = State.getState();
    if (!hydratedFromConsulta && (!currentState.clienteMode || currentState.clienteMode === 'unset')) {
        openStartModal();
    }
    focusProductSearch();
})();
