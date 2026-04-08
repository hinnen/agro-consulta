(function () {
    'use strict';

    var STORAGE_KEY = 'agro_pdv_wizard_state_v1';
    var LAST_CLIENT_KEY = 'agro_pdv_wizard_last_client_v1';
    var STEP_ORDER = ['produtos', 'cliente', 'entrega', 'pagamento'];
    var listeners = [];

    function deepClone(obj) {
        return JSON.parse(JSON.stringify(obj));
    }

    function toNumber(value) {
        if (typeof value === 'number') return Number.isFinite(value) ? value : 0;
        var txt = String(value == null ? '' : value).trim();
        if (!txt) return 0;
        txt = txt.replace(/\./g, '').replace(',', '.').replace(/[^\d.-]/g, '');
        var num = parseFloat(txt);
        return Number.isFinite(num) ? num : 0;
    }

    function sanitizeCliente(raw) {
        if (!raw || typeof raw !== 'object') return null;
        return {
            id: String(raw.id || '').trim(),
            nome: String(raw.nome || '').trim(),
            documento: String(raw.documento || '').trim(),
            telefone: String(raw.telefone || '').trim(),
            endereco: String(raw.endereco || '').trim(),
            logradouro: String(raw.logradouro || '').trim(),
            numero: String(raw.numero || '').trim(),
            bairro: String(raw.bairro || '').trim(),
            cidade: String(raw.cidade || '').trim(),
            uf: String(raw.uf || '').trim(),
            cep: String(raw.cep || '').trim(),
            plus_code: String(raw.plus_code || '').trim(),
            referencia_rural: String(raw.referencia_rural || '').trim(),
            maps_url_manual: String(raw.maps_url_manual || '').trim(),
            cliente_agro_pk: raw.cliente_agro_pk != null ? raw.cliente_agro_pk : null
        };
    }

    function defaultState() {
        return {
            currentStep: 'produtos',
            clienteMode: 'unset',
            cliente: null,
            itens: [],
            entrega: {
                ativa: false,
                endereco: '',
                logradouro: '',
                numero: '',
                bairro: '',
                plusCode: '',
                complemento: '',
                referencia: '',
                horario: '',
                troco: '',
                statusPagamento: '',
                maquininha: '',
                observacao: '',
                taxaEntregaRespondida: false,
                taxaEntregaModo: '',
                localPagamento: '',
                meioNaEntrega: ''
            },
            pagamento: {
                forma: '',
                descontoGeral: 0,
                frete: 0,
                valorRecebido: '',
                trocoCalculado: '',
                imprimirCupom: false,
                observacaoFinal: '',
                valorDestaForma: '',
                creditoParcelas: 2,
                fiadoParcelas: 1,
                fiadoDiasVencimento: 30,
                outroDetalhes: '',
                outroPinVerificado: false,
                maquinaId: '',
                maquinaNome: '',
                lancamentos: []
            },
            venda: {
                observacao: ''
            }
        };
    }

    var state = loadState();

    function saveState() {
        try {
            sessionStorage.setItem(STORAGE_KEY, JSON.stringify(state));
        } catch (err) {}
    }

    function loadState() {
        try {
            var raw = sessionStorage.getItem(STORAGE_KEY);
            if (raw) {
                var parsed = JSON.parse(raw);
                var def = defaultState();
                var merged = Object.assign({}, def, parsed || {});
                merged.pagamento = Object.assign({}, def.pagamento, (parsed && parsed.pagamento) || {});
                if (!Array.isArray(merged.pagamento.lancamentos)) merged.pagamento.lancamentos = [];
                merged.entrega = Object.assign({}, def.entrega, (parsed && parsed.entrega) || {});
                ['logradouro', 'numero', 'bairro', 'plusCode'].forEach(function (k) {
                    if (merged.entrega[k] === undefined || merged.entrega[k] === null) {
                        merged.entrega[k] = '';
                    }
                });
                return merged;
            }
        } catch (err) {}
        return defaultState();
    }

    function notify() {
        saveState();
        listeners.forEach(function (listener) {
            try {
                listener(getState(), getComputed());
            } catch (err) {}
        });
    }

    function getState() {
        return deepClone(state);
    }

    function getComputed() {
        var subtotal = 0;
        var itemCount = 0;
        (state.itens || []).forEach(function (item) {
            var qtd = toNumber(item.qtd);
            var preco = toNumber(item.preco);
            var descontoItem = toNumber(item.desconto || 0);
            subtotal += Math.max(0, qtd * preco - descontoItem);
            itemCount += qtd;
        });
        var desconto = Math.max(0, toNumber(state.pagamento.descontoGeral));
        var frete = Math.max(0, toNumber(state.pagamento.frete));
        var total = Math.max(0, subtotal - desconto + frete);
        return {
            subtotal: subtotal,
            desconto: desconto,
            frete: frete,
            total: total,
            itemCount: itemCount,
            flow: resolveFlow(),
            isConsumidorFinal: state.clienteMode === 'consumidor_final',
            currentStep: state.currentStep
        };
    }

    function resolveFlow() {
        var flow = ['produtos'];
        if (state.clienteMode !== 'consumidor_final') flow.push('cliente');
        if (state.entrega.ativa) flow.push('entrega');
        flow.push('pagamento');
        return flow;
    }

    function setCurrentStep(step) {
        if (STEP_ORDER.indexOf(step) === -1) return;
        state.currentStep = step;
        notify();
    }

    function setCliente(cliente, mode) {
        state.cliente = sanitizeCliente(cliente);
        state.clienteMode = mode || 'cliente';
        if (state.cliente && state.clienteMode !== 'consumidor_final') {
            try {
                localStorage.setItem(LAST_CLIENT_KEY, JSON.stringify(state.cliente));
            } catch (err) {}
        }
        notify();
    }

    function setConsumidorFinal(nomePadrao) {
        state.clienteMode = 'consumidor_final';
        state.cliente = {
            id: '',
            nome: String(nomePadrao || 'CONSUMIDOR NÃO IDENTIFICADO...'),
            documento: '',
            telefone: '',
            endereco: '',
            logradouro: '',
            numero: '',
            bairro: '',
            cidade: '',
            uf: '',
            cep: '',
            plus_code: '',
            referencia_rural: '',
            maps_url_manual: '',
            cliente_agro_pk: null
        };
        notify();
    }

    function getLastClient() {
        try {
            return sanitizeCliente(JSON.parse(localStorage.getItem(LAST_CLIENT_KEY) || 'null'));
        } catch (err) {
            return null;
        }
    }

    function addItem(produto, quantidade) {
        if (!produto || !produto.id) return;
        var qtd = Math.max(1, toNumber(quantidade || 1));
        var existing = state.itens.find(function (item) { return String(item.id) === String(produto.id); });
        if (existing) {
            existing.qtd = toNumber(existing.qtd) + qtd;
        } else {
            state.itens.push({
                id: String(produto.id),
                nome: String(produto.nome || ''),
                preco: toNumber(produto.preco_venda || produto.preco || 0),
                qtd: qtd,
                codigo: String(produto.codigo || produto.codigo_nfe || produto.codigo_barras || ''),
                codigoGm: String(produto.codigo_nfe || produto.codigo || produto.codigo_barras || '').trim(),
                imagem: String(produto.imagem || ''),
                marca: String(produto.marca || ''),
                desconto: 0,
                observacao: ''
            });
        }
        notify();
    }

    function updateItemQuantity(itemId, nextQty) {
        state.itens = state.itens.map(function (item) {
            if (String(item.id) !== String(itemId)) return item;
            return Object.assign({}, item, { qtd: Math.max(1, toNumber(nextQty || 1)) });
        });
        notify();
    }

    function removeItem(itemId) {
        state.itens = state.itens.filter(function (item) { return String(item.id) !== String(itemId); });
        notify();
    }

    function clearItems() {
        state.itens = [];
        notify();
    }

    function setEntregaField(field, value) {
        if (!state.entrega || !(field in state.entrega)) return;
        state.entrega[field] = value;
        notify();
    }

    function setEntregaPatch(patch) {
        if (!state.entrega || !patch || typeof patch !== 'object') return;
        Object.keys(patch).forEach(function (k) {
            if (k in state.entrega) state.entrega[k] = patch[k];
        });
        notify();
    }

    function setPagamentoField(field, value) {
        if (!state.pagamento || !(field in state.pagamento)) return;
        state.pagamento[field] = value;
        notify();
    }

    function setPagamentoPatch(patch) {
        if (!state.pagamento || !patch || typeof patch !== 'object') return;
        Object.keys(patch).forEach(function (k) {
            if (k in state.pagamento) state.pagamento[k] = patch[k];
        });
        notify();
    }

    function setVendaField(field, value) {
        if (!state.venda || !(field in state.venda)) return;
        state.venda[field] = value;
        notify();
    }

    function hydrateFromBudget(entry) {
        if (!entry || typeof entry !== 'object') return;
        state.itens = Array.isArray(entry.itens) ? entry.itens.map(function (item) {
            var cod = String(item.codigo || '');
            var gm = String(item.codigoGm || item.codigo_nfe || '').trim();
            return {
                id: String(item.id || ''),
                nome: String(item.nome || ''),
                preco: toNumber(item.preco || 0),
                qtd: Math.max(1, toNumber(item.qtd || 1)),
                codigo: cod,
                codigoGm: gm || cod,
                imagem: '',
                marca: '',
                desconto: 0,
                observacao: ''
            };
        }) : [];
        if (entry.cliente && String(entry.cliente).trim()) {
            state.clienteMode = 'cliente';
            state.cliente = {
                id: '',
                nome: String(entry.cliente),
                documento: '',
                telefone: '',
                endereco: '',
                logradouro: '',
                numero: '',
                bairro: '',
                cidade: '',
                uf: '',
                cep: '',
                plus_code: '',
                referencia_rural: '',
                maps_url_manual: '',
                cliente_agro_pk: null
            };
        }
        if (entry.entrega) state.entrega.ativa = true;
        state.entrega.taxaEntregaRespondida = false;
        state.entrega.taxaEntregaModo = '';
        state.entrega.localPagamento = '';
        state.entrega.meioNaEntrega = '';
        state.pagamento.lancamentos = [];
        state.pagamento.forma = '';
        notify();
    }

    /** Hidrata o wizard a partir do rascunho salvo na sessão (consulta → FECHAR VENDA). */
    function hydrateFromSessionDraft(draft) {
        if (!draft || typeof draft !== 'object') return false;
        var itens = Array.isArray(draft.itens) ? draft.itens : [];
        if (!itens.length) return false;
        state.itens = itens.map(function (i) {
            var cod = String((i && i.codigo) || '').trim();
            return {
                id: String((i && i.id) || ''),
                nome: String((i && i.nome) || ''),
                preco: toNumber(i && i.preco),
                qtd: Math.max(1, toNumber(i && i.qtd)),
                codigo: cod,
                codigoGm: cod || '—',
                imagem: '',
                marca: '',
                desconto: 0,
                observacao: ''
            };
        });
        var nomeLinha = String(draft.cliente || '').trim();
        if (!nomeLinha) nomeLinha = 'CONSUMIDOR NÃO IDENTIFICADO...';
        var ex = draft.cliente_extra;
        if (ex && typeof ex === 'object' && Object.keys(ex).length) {
            var raw = Object.assign({}, ex);
            if (!String(raw.nome || '').trim() && String(raw.razao_social || '').trim()) {
                raw.nome = raw.razao_social;
            }
            if (!String(raw.nome || '').trim()) raw.nome = nomeLinha;
            state.cliente = sanitizeCliente(raw);
            state.clienteMode = 'cliente';
        } else if (/consumidor\s+n[aã]o\s+identificado/i.test(nomeLinha)) {
            state.clienteMode = 'consumidor_final';
            state.cliente = {
                id: '',
                nome: nomeLinha,
                documento: '',
                telefone: '',
                endereco: '',
                logradouro: '',
                numero: '',
                bairro: '',
                cidade: '',
                uf: '',
                cep: '',
                plus_code: '',
                referencia_rural: '',
                maps_url_manual: '',
                cliente_agro_pk: null
            };
        } else {
            state.clienteMode = 'cliente';
            state.cliente = sanitizeCliente({ nome: nomeLinha, id: '' });
        }
        state.entrega.ativa = false;
        state.entrega.endereco = '';
        state.entrega.logradouro = '';
        state.entrega.numero = '';
        state.entrega.bairro = '';
        state.entrega.plusCode = '';
        state.entrega.complemento = '';
        state.entrega.referencia = '';
        state.entrega.horario = '';
        state.entrega.troco = '';
        state.entrega.statusPagamento = '';
        state.entrega.maquininha = '';
        state.entrega.observacao = '';
        state.entrega.taxaEntregaRespondida = false;
        state.entrega.taxaEntregaModo = '';
        state.entrega.localPagamento = '';
        state.entrega.meioNaEntrega = '';
        var fp = String(draft.forma_pagamento || '').trim();
        var allowed = [
            '',
            'Dinheiro',
            'PIX',
            'Cartão de débito',
            'Cartão de crédito',
            'Crédito parcelado',
            'Fiado',
            'Vale crédito',
            'Cashback',
            'Outro'
        ];
        state.pagamento.forma = allowed.indexOf(fp) >= 0 ? fp : '';
        state.pagamento.lancamentos = [];
        state.pagamento.valorRecebido = '';
        state.pagamento.trocoCalculado = '';
        state.pagamento.valorDestaForma = '';
        state.pagamento.maquinaId = '';
        state.pagamento.maquinaNome = '';
        state.pagamento.outroDetalhes = '';
        state.pagamento.outroPinVerificado = false;
        state.currentStep = 'produtos';
        notify();
        return true;
    }

    function resetPagamentoTranche() {
        if (!state.pagamento) return;
        state.pagamento.forma = '';
        state.pagamento.maquinaId = '';
        state.pagamento.maquinaNome = '';
        state.pagamento.valorRecebido = '';
        state.pagamento.trocoCalculado = '';
        state.pagamento.valorDestaForma = '';
        state.pagamento.outroDetalhes = '';
        state.pagamento.outroPinVerificado = false;
        state.pagamento.creditoParcelas = 2;
        notify();
    }

    function addPagamentoLancamento(entry) {
        if (!state.pagamento) return;
        if (!Array.isArray(state.pagamento.lancamentos)) state.pagamento.lancamentos = [];
        state.pagamento.lancamentos.push(
            Object.assign(
                {
                    forma: '',
                    valor: 0,
                    maquinaId: '',
                    maquinaNome: '',
                    creditoParcelas: null,
                    fiadoParcelas: null,
                    fiadoDiasVencimento: null,
                    valorRecebido: '',
                    trocoCalculado: '',
                    outroDetalhes: ''
                },
                entry || {}
            )
        );
        resetPagamentoTranche();
    }

    function reset(keepClient) {
        var next = defaultState();
        if (keepClient && state.cliente) {
            next.cliente = deepClone(state.cliente);
            next.clienteMode = state.clienteMode;
            next.entrega.endereco = state.cliente.endereco || '';
        }
        state = next;
        notify();
    }

    window.AgroPdvState = {
        subscribe: function (listener) {
            if (typeof listener !== 'function') return function () {};
            listeners.push(listener);
            listener(getState(), getComputed());
            return function () {
                listeners = listeners.filter(function (item) { return item !== listener; });
            };
        },
        getState: getState,
        getComputed: getComputed,
        setCurrentStep: setCurrentStep,
        resolveFlow: resolveFlow,
        setCliente: setCliente,
        setConsumidorFinal: setConsumidorFinal,
        getLastClient: getLastClient,
        addItem: addItem,
        updateItemQuantity: updateItemQuantity,
        removeItem: removeItem,
        clearItems: clearItems,
        setEntregaField: setEntregaField,
        setEntregaPatch: setEntregaPatch,
        setPagamentoField: setPagamentoField,
        setPagamentoPatch: setPagamentoPatch,
        setVendaField: setVendaField,
        hydrateFromBudget: hydrateFromBudget,
        hydrateFromSessionDraft: hydrateFromSessionDraft,
        reset: reset,
        toNumber: toNumber,
        addPagamentoLancamento: addPagamentoLancamento,
        resetPagamentoTranche: resetPagamentoTranche
    };
})();
