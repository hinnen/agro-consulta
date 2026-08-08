(function () {
    'use strict';

    var STORAGE_KEY = 'agro_pdv_wizard_state_v1';
    var LAST_CLIENT_KEY = 'agro_pdv_wizard_last_client_v1';
    var STEP_ORDER = ['produtos', 'entrega', 'pagamento'];
    var listeners = [];

    function deepClone(obj) {
        return JSON.parse(JSON.stringify(obj));
    }

    var QTD_MIN = 0.001;
    var QTD_DECIMALS = 3;

    function toNumber(value) {
        if (typeof value === 'number') return Number.isFinite(value) ? value : 0;
        var txt = String(value == null ? '' : value).trim();
        if (!txt) return 0;
        if (txt.indexOf(',') >= 0) {
            txt = txt.replace(/\./g, '').replace(',', '.');
        }
        txt = txt.replace(/[^\d.-]/g, '');
        var num = parseFloat(txt);
        return Number.isFinite(num) ? num : 0;
    }

    function roundQty(value) {
        var n = toNumber(value);
        if (!n) return 0;
        var factor = Math.pow(10, QTD_DECIMALS);
        return Math.round(n * factor) / factor;
    }

    function normalizeQty(value, fallback) {
        var n = roundQty(value);
        if (n < QTD_MIN) {
            if (fallback != null) return normalizeQty(fallback, null);
            return 0;
        }
        return n;
    }

    function formatQtyDisplay(value) {
        var n = roundQty(value);
        if (!n) return '0';
        if (Math.abs(n - Math.round(n)) < 1 / Math.pow(10, QTD_DECIMALS)) {
            return String(Math.round(n));
        }
        var s = n.toFixed(QTD_DECIMALS).replace(/\.?0+$/, '');
        return s.replace('.', ',');
    }

    function qtyStepFor(currentQty) {
        var q = roundQty(currentQty);
        if (q >= 1 && Math.abs(q - Math.round(q)) < 1 / Math.pow(10, QTD_DECIMALS)) {
            return 1;
        }
        return 1 / Math.pow(10, QTD_DECIMALS);
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
            cliente_agro_pk: raw.cliente_agro_pk != null ? raw.cliente_agro_pk : null,
            saldo_vale_credito: toNumber(raw.saldo_vale_credito),
            saldo_cashback: toNumber(raw.saldo_cashback),
            limite_fiado_local: toNumber(raw.limite_fiado_local)
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
                /** '' | 'retirada' | 'entrega' — escolha no pop-up ao entrar na etapa Entrega */
                modoRetiradaEntrega: '',
                /** true após confirmar pop-up de taxa + horário + troco */
                detalhesEntregaRespondidos: false,
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
                /** true após F7 no form de endereço (mesmo com campos já preenchidos) */
                enderecoPassoConcluido: false,
                localPagamento: '',
                meioNaEntrega: '',
                pedidoEntregaPendenteId: null,
                /** true só após etapa Entrega (loja) ou retomar entrega pendente */
                entregaFreteLiberadoPagamento: false
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
                mpBalcaoModo: '',
                /** Chave por tentativa de confirmação (idempotência no servidor). */
                clientRequestId: '',
                lancamentos: [],
                nfceEmitir: false,
                nfceOpts: {},
                cupomImpressao: ''
            },
            venda: {
                observacao: ''
            },
            fiadoCobranca: {
                ativo: false,
                modo: '',
                tituloId: null,
                tituloIds: [],
                valorTotal: 0,
                saldoTotal: 0,
                parcial: false,
                resumoTexto: '',
                titulos: [],
                emOverlay: false
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
                Object.keys(def.pagamento).forEach(function (pk) {
                    if (merged.pagamento[pk] === undefined) merged.pagamento[pk] = def.pagamento[pk];
                });
                if (!Array.isArray(merged.pagamento.lancamentos)) merged.pagamento.lancamentos = [];
                merged.pagamento.lancamentos = merged.pagamento.lancamentos.map(function (L) {
                    if (!L || typeof L !== 'object') return L;
                    var row = Object.assign({}, L);
                    if (row.mpBalcaoModo == null) row.mpBalcaoModo = '';
                    var mid = String(row.maquinaId || '').trim();
                    var mpm = String(row.mpBalcaoModo || '').trim();
                    row.cobrarNoPointMp =
                        !!row.cobrarNoPointMp || (mid === 'mp_balcao' && mpm === 'point');
                    return row;
                });
                merged.entrega = Object.assign({}, def.entrega, (parsed && parsed.entrega) || {});
                merged.fiadoCobranca = Object.assign({}, def.fiadoCobranca, (parsed && parsed.fiadoCobranca) || {});
                ['logradouro', 'numero', 'bairro', 'plusCode'].forEach(function (k) {
                    if (merged.entrega[k] === undefined || merged.entrega[k] === null) {
                        merged.entrega[k] = '';
                    }
                });
                if (!merged.entrega.modoRetiradaEntrega) {
                    if (merged.entrega.ativa) merged.entrega.modoRetiradaEntrega = 'entrega';
                    else if (merged.entrega.modoRetiradaEntrega === undefined) merged.entrega.modoRetiradaEntrega = '';
                }
                if (merged.entrega.detalhesEntregaRespondidos === undefined) {
                    merged.entrega.detalhesEntregaRespondidos = !!merged.entrega.taxaEntregaRespondida;
                }
                if (merged.entrega.entregaFreteLiberadoPagamento === undefined) {
                    merged.entrega.entregaFreteLiberadoPagamento = false;
                }
                if (merged.currentStep === 'cliente') {
                    merged.currentStep = merged.entrega.ativa ? 'entrega' : 'produtos';
                }
                if (STEP_ORDER.indexOf(merged.currentStep) === -1) {
                    merged.currentStep = 'produtos';
                }
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
        if (state.fiadoCobranca && state.fiadoCobranca.ativo) {
            var vt = Math.max(0, toNumber(state.fiadoCobranca.valorTotal));
            return {
                subtotal: vt,
                desconto: 0,
                frete: 0,
                total: vt,
                itemCount: 0,
                flow: ['pagamento'],
                isConsumidorFinal: false,
                currentStep: state.currentStep,
                fiadoCobranca: true
            };
        }
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
        if (state.fiadoCobranca && state.fiadoCobranca.ativo) return ['pagamento'];
        var flow = ['produtos'];
        if (state.entrega.modoRetiradaEntrega !== 'retirada') flow.push('entrega');
        flow.push('pagamento');
        return flow;
    }

    function setCurrentStep(step) {
        if (STEP_ORDER.indexOf(step) === -1) return;
        state.currentStep = step;
        notify();
    }

    function clienteIdentityKey(cliente) {
        if (!cliente || typeof cliente !== 'object') return '';
        var pk = cliente.cliente_agro_pk != null ? String(cliente.cliente_agro_pk).trim() : '';
        if (pk) return 'pk:' + pk;
        var id = String(cliente.id || '').trim();
        if (id) return 'id:' + id;
        var tel = String(cliente.telefone || '').replace(/\D/g, '');
        var nome = String(cliente.nome || '').trim().toLowerCase();
        return 'tmp:' + nome + ':' + tel;
    }

    function composeEntregaEnderecoLinhaRapida(entregaPatch, cliente) {
        entregaPatch = entregaPatch || {};
        var log = String(entregaPatch.logradouro || '').trim();
        var num = String(entregaPatch.numero || '').trim();
        var bai = String(entregaPatch.bairro || '').trim();
        var pc = String(entregaPatch.plusCode || '').trim();
        var parts = [];
        if (log || num) {
            var ln = [log, num].filter(Boolean).join(', ');
            if (ln) parts.push(ln);
        }
        if (bai) parts.push(bai);
        if (pc) parts.push('Plus ' + pc);
        if (parts.length) return parts.join(' — ') + ' — Jacupiranga/SP';
        return String((cliente && cliente.endereco) || '').trim();
    }

    function aplicarEnderecoEntregaDoCliente(cliente) {
        if (!state.entrega) return;
        cliente = cliente || {};
        var patch = {
            logradouro: String(cliente.logradouro || '').trim(),
            numero: String(cliente.numero || '').trim(),
            bairro: String(cliente.bairro || '').trim(),
            plusCode: String(cliente.plus_code || '').trim(),
            complemento: '',
            referencia: String(cliente.referencia_rural || '').trim()
        };
        state.entrega.logradouro = patch.logradouro;
        state.entrega.numero = patch.numero;
        state.entrega.bairro = patch.bairro;
        state.entrega.plusCode = patch.plusCode;
        state.entrega.complemento = patch.complemento;
        state.entrega.referencia = patch.referencia;
        state.entrega.endereco = composeEntregaEnderecoLinhaRapida(patch, cliente);
        state.entrega.enderecoPassoConcluido = false;
    }

    function setCliente(cliente, mode) {
        var prevKey = clienteIdentityKey(state.cliente);
        state.cliente = sanitizeCliente(cliente);
        state.clienteMode = mode || 'cliente';
        var nextKey = clienteIdentityKey(state.cliente);
        if (prevKey !== nextKey) {
            aplicarEnderecoEntregaDoCliente(state.cliente);
        }
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
            nome: String(nomePadrao || 'CONSUMIDOR NÃO IDENTIFICADO...').trim() || 'CONSUMIDOR NÃO IDENTIFICADO...',
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
            cliente_agro_pk: null,
            saldo_vale_credito: 0,
            saldo_cashback: 0,
            limite_fiado_local: 0
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

    function resolveProdutoId(produto) {
        if (!produto || typeof produto !== 'object') return '';
        var raw = produto.id != null && produto.id !== ''
            ? produto.id
            : (produto.Id != null && produto.Id !== ''
                ? produto.Id
                : (produto.produto_id != null && produto.produto_id !== ''
                    ? produto.produto_id
                    : (produto.produto_externo_id != null ? produto.produto_externo_id : '')));
        var id = String(raw == null ? '' : raw).trim();
        if (!id || id === 'undefined' || id === 'null') return '';
        return id;
    }

    function aplicarPromocaoNoItem(item) {
        if (!item || item.preco_manual) return item;
        recalcularTodasPromocoes();
        return item;
    }

    function recalcularTodasPromocoes() {
        if (!state.itens || !state.itens.length) return;
        var fp = '';
        if (typeof window.AgroPrecosFormaPagamento !== 'undefined' && window.AgroPrecosFormaPagamento.obterFormaDoState) {
            fp = window.AgroPrecosFormaPagamento.obterFormaDoState(state);
        }
        if (typeof window.AgroPdvPromocoes !== 'undefined' && window.AgroPdvPromocoes.recalcCarrinhoComForma) {
            window.AgroPdvPromocoes.recalcCarrinhoComForma(state.itens, fp);
            return;
        }
        (state.itens || []).forEach(function (item) {
            if (!item || item.preco_manual) return;
            if (typeof window.AgroPrecosFormaPagamento !== 'undefined' && window.AgroPrecosFormaPagamento.aplicarPromocaoDepoisForma) {
                window.AgroPrecosFormaPagamento.aplicarPromocaoDepoisForma(item, fp);
            } else if (typeof window.AgroPdvPromocoes !== 'undefined' && window.AgroPdvPromocoes.aplicarNoItem) {
                if (item.preco_padrao == null) item.preco_padrao = toNumber(item.preco);
                window.AgroPdvPromocoes.aplicarNoItem(item);
            }
        });
        if (window.AgroPdvCampanha && window.AgroPdvCampanha.aplicarNosItens) {
            window.AgroPdvCampanha.aplicarNosItens(state.itens);
        }
    }

    function recalcularPrecosFormaItens(forma) {
        var fp = forma != null ? String(forma).trim() : '';
        if (!fp && typeof window.AgroPrecosFormaPagamento !== 'undefined' && window.AgroPrecosFormaPagamento.obterFormaDoState) {
            fp = window.AgroPrecosFormaPagamento.obterFormaDoState(state);
        }
        if (!state.itens || !state.itens.length) return;
        if (typeof window.AgroPdvPromocoes !== 'undefined' && window.AgroPdvPromocoes.recalcCarrinhoComForma) {
            window.AgroPdvPromocoes.recalcCarrinhoComForma(state.itens, fp);
            return;
        }
        recalcularTodasPromocoes();
    }

    function addItem(produto, quantidade) {
        var pid = resolveProdutoId(produto);
        if (!pid) return false;
        var qtd = normalizeQty(quantidade || 1, 1);
        var precoPadrao = toNumber(produto.preco_padrao != null ? produto.preco_padrao : (produto.preco_venda || produto.preco || 0));
        var existing = state.itens.find(function (item) { return String(item.id) === pid; });
        if (existing) {
            existing.qtd = normalizeQty(toNumber(existing.qtd) + qtd, qtd);
            if (existing.preco_padrao == null) existing.preco_padrao = precoPadrao;
            if (produto.precos_por_forma && typeof produto.precos_por_forma === 'object') {
                existing.precos_por_forma = Object.assign({}, produto.precos_por_forma);
            }
            if (produto.precos_grupos && typeof produto.precos_grupos === 'object') {
                existing.precos_grupos = Object.assign({}, produto.precos_grupos);
                if (Array.isArray(produto.precos_grupos.formas_a)) {
                    existing.precos_grupos.formas_a = produto.precos_grupos.formas_a.slice();
                }
                if (Array.isArray(produto.precos_grupos.formas_b)) {
                    existing.precos_grupos.formas_b = produto.precos_grupos.formas_b.slice();
                }
            }
            if (produto.precos_modo) {
                existing.precos_modo = String(produto.precos_modo).toLowerCase() === 'grupos' ? 'grupos' : 'por_forma';
            } else if (existing.precos_grupos) {
                existing.precos_modo = 'grupos';
            }
            if (!existing.preco_manual) recalcularTodasPromocoes();
        } else {
            var novo = {
                id: pid,
                nome: String(produto.nome || ''),
                preco: precoPadrao,
                preco_padrao: precoPadrao,
                qtd: qtd,
                codigo: String(produto.codigo || produto.codigo_nfe || produto.codigo_barras || ''),
                codigoGm: String(produto.codigo_nfe || produto.codigo || produto.codigo_barras || '').trim(),
                imagem: String(produto.imagem || ''),
                marca: String(produto.marca || ''),
                desconto: 0,
                observacao: ''
            };
            if (produto.precos_por_forma && typeof produto.precos_por_forma === 'object') {
                novo.precos_por_forma = Object.assign({}, produto.precos_por_forma);
            }
            if (produto.precos_grupos && typeof produto.precos_grupos === 'object') {
                novo.precos_grupos = Object.assign({}, produto.precos_grupos);
                if (Array.isArray(produto.precos_grupos.formas_a)) {
                    novo.precos_grupos.formas_a = produto.precos_grupos.formas_a.slice();
                }
                if (Array.isArray(produto.precos_grupos.formas_b)) {
                    novo.precos_grupos.formas_b = produto.precos_grupos.formas_b.slice();
                }
            }
            if (produto.precos_modo) {
                novo.precos_modo = String(produto.precos_modo).toLowerCase() === 'grupos' ? 'grupos' : 'por_forma';
            } else if (novo.precos_grupos) {
                novo.precos_modo = 'grupos';
            }
            state.itens.push(novo);
            recalcularTodasPromocoes();
        }
        notify();
        return true;
    }

    function updateItemQuantity(itemId, nextQty) {
        var q = normalizeQty(nextQty, null);
        if (!q) {
            removeItem(itemId);
            return;
        }
        state.itens = state.itens.map(function (item) {
            if (String(item.id) !== String(itemId)) return item;
            var next = Object.assign({}, item, { qtd: q });
            if (next.preco_padrao == null) next.preco_padrao = toNumber(next.preco);
            return next;
        });
        recalcularTodasPromocoes();
        notify();
    }

    var PRECO_MIN = 0.01;

    function normalizePrice(value, fallback) {
        var n = Math.round(toNumber(value) * 100) / 100;
        if (n < PRECO_MIN) {
            if (fallback != null) return normalizePrice(fallback, null);
            return 0;
        }
        return n;
    }

    function formatPriceDisplay(value) {
        var n = normalizePrice(value, null);
        if (!n) return '';
        return n.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    }

    /** Valor monetário em campo de texto (aceita vírgula; preserva digitação em andamento). */
    function formatMoneyInputDisplay(value) {
        if (value == null || value === '') return '';
        if (typeof value === 'string') {
            var t = value.trim();
            if (!t) return '';
            if (/,(\d{0,2})?$/.test(t)) return t;
            if (t.indexOf(',') < 0 && /\.\d{0,2}$/.test(t)) return t;
        }
        var n = toNumber(value);
        if (!n) return typeof value === 'string' ? String(value).trim() : '';
        return formatPriceDisplay(n);
    }

    function sanitizeMoneyInputTyping(raw) {
        var s = String(raw == null ? '' : raw).replace(/[^\d,.]/g, '');
        var iComma = s.indexOf(',');
        if (iComma >= 0) {
            var head = s.slice(0, iComma + 1).replace(/\./g, '');
            var tail = s.slice(iComma + 1).replace(/[,.]/g, '').slice(0, 2);
            return head + tail;
        }
        var iDot = s.lastIndexOf('.');
        if (iDot >= 0 && s.length - iDot - 1 <= 2) {
            var h = s.slice(0, iDot).replace(/\./g, '');
            var t = s.slice(iDot + 1).replace(/[,.]/g, '').slice(0, 2);
            return h + '.' + t;
        }
        return s.replace(/\./g, '');
    }

    function updateItemPrice(itemId, nextPrice) {
        var p = normalizePrice(nextPrice, null);
        if (!p) return;
        state.itens = state.itens.map(function (item) {
            if (String(item.id) !== String(itemId)) return item;
            return Object.assign({}, item, {
                preco: p,
                preco_manual: true,
                preco_grupo_preview: ''
            });
        });
        notify();
    }

    /** Prévia A/B no carrinho (etapa 1). Não trava preço: a forma na etapa 3 sempre manda. */
    function setItemPrecoGrupoPreview(itemId, grupo) {
        var gWant = String(grupo || '').toLowerCase() === 'b' ? 'b' : String(grupo || '').toLowerCase() === 'a' ? 'a' : '';
        if (!gWant) return;
        state.itens = state.itens.map(function (item) {
            if (String(item.id) !== String(itemId)) return item;
            if (String(item.precos_modo || '').toLowerCase() !== 'grupos') return item;
            var g = item.precos_grupos;
            if (!g || typeof g !== 'object') return item;
            var atual = String(item.preco_grupo_preview || '').toLowerCase();
            /* Segundo toque no mesmo chip volta ao preço padrão (sem escolha). */
            if (atual === gWant) {
                var padrao = toNumber(item.preco_padrao != null ? item.preco_padrao : item.preco);
                return Object.assign({}, item, {
                    preco_grupo_preview: '',
                    preco_manual: false,
                    preco: padrao
                });
            }
            var preco = gWant === 'a' ? toNumber(g.preco_a) : toNumber(g.preco_b);
            if (!(preco > 0)) return item;
            return Object.assign({}, item, {
                preco_grupo_preview: gWant,
                preco_manual: false,
                preco: preco
            });
        });
        recalcularTodasPromocoes();
        notify();
    }

    function patchItemCadastro(itemId, patch) {
        if (!patch || typeof patch !== 'object') return;
        state.itens = state.itens.map(function (item) {
            if (String(item.id) !== String(itemId)) return item;
            var next = Object.assign({}, item);
            if (patch.nome != null) next.nome = String(patch.nome || '');
            if (patch.codigo_nfe != null) {
                next.codigoGm = String(patch.codigo_nfe || '').trim();
                next.codigo_nfe = next.codigoGm;
            }
            if (patch.codigo_sistema != null) {
                next.codigo = String(patch.codigo_sistema || '').trim();
            } else if (patch.codigo != null && patch.codigo_nfe == null) {
                next.codigo = String(patch.codigo || '');
            }
            if (patch.codigo_barras != null) next.codigo_barras = String(patch.codigo_barras || '');
            if (patch.preco_custo != null) next.preco_custo = toNumber(patch.preco_custo);
            if (patch.unidade != null) next.unidade = String(patch.unidade || '');
            if (patch.precos_modo != null) {
                next.precos_modo = String(patch.precos_modo).toLowerCase() === 'grupos' ? 'grupos' : 'por_forma';
            }
            if (patch.precos_grupos && typeof patch.precos_grupos === 'object') {
                next.precos_grupos = Object.assign({}, patch.precos_grupos);
                if (Array.isArray(patch.precos_grupos.formas_a)) {
                    next.precos_grupos.formas_a = patch.precos_grupos.formas_a.slice();
                }
                if (Array.isArray(patch.precos_grupos.formas_b)) {
                    next.precos_grupos.formas_b = patch.precos_grupos.formas_b.slice();
                }
            }
            if (patch.preco_venda != null && !(toNumber(patch.preco_venda) < 0)) {
                var pv = toNumber(patch.preco_venda);
                next.preco_padrao = pv;
                if (!next.preco_manual) {
                    var preview = String(next.preco_grupo_preview || '').toLowerCase();
                    var g2 = next.precos_grupos;
                    if (preview === 'a' && g2 && toNumber(g2.preco_a) > 0) {
                        next.preco = toNumber(g2.preco_a);
                    } else if (preview === 'b' && g2 && toNumber(g2.preco_b) > 0) {
                        next.preco = toNumber(g2.preco_b);
                    } else {
                        next.preco = pv;
                    }
                }
            } else if (next.precos_grupos && !next.preco_manual) {
                var preview2 = String(next.preco_grupo_preview || '').toLowerCase();
                var g3 = next.precos_grupos;
                if (preview2 === 'a' && toNumber(g3.preco_a) > 0) next.preco = toNumber(g3.preco_a);
                else if (preview2 === 'b' && toNumber(g3.preco_b) > 0) next.preco = toNumber(g3.preco_b);
            }
            return next;
        });
        recalcularTodasPromocoes();
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
        if (!state.pagamento) return;
        var defPg = defaultState().pagamento;
        if (!(field in defPg)) return;
        state.pagamento[field] = value;
        if (field === 'forma') recalcularPrecosFormaItens(value);
        notify();
    }

    function setPagamentoPatch(patch) {
        if (!state.pagamento || !patch || typeof patch !== 'object') return;
        var defPg = defaultState().pagamento;
        var formaMudou = Object.prototype.hasOwnProperty.call(patch, 'forma');
        Object.keys(patch).forEach(function (k) {
            if (k in defPg) state.pagamento[k] = patch[k];
        });
        if (formaMudou) recalcularPrecosFormaItens(state.pagamento.forma);
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
            var row = {
                id: String(item.id || ''),
                nome: String(item.nome || ''),
                preco: toNumber(item.preco || 0),
                preco_padrao: item.preco_padrao != null ? toNumber(item.preco_padrao) : toNumber(item.preco || 0),
                qtd: normalizeQty(item.qtd || 1, 1),
                codigo: cod,
                codigoGm: gm || cod,
                imagem: '',
                marca: '',
                desconto: 0,
                observacao: ''
            };
            if (item.precos_modo) {
                row.precos_modo = String(item.precos_modo).toLowerCase() === 'grupos' ? 'grupos' : 'por_forma';
            }
            if (item.precos_grupos && typeof item.precos_grupos === 'object') {
                row.precos_grupos = Object.assign({}, item.precos_grupos);
                if (Array.isArray(item.precos_grupos.formas_a)) {
                    row.precos_grupos.formas_a = item.precos_grupos.formas_a.slice();
                }
                if (Array.isArray(item.precos_grupos.formas_b)) {
                    row.precos_grupos.formas_b = item.precos_grupos.formas_b.slice();
                }
                if (!row.precos_modo) row.precos_modo = 'grupos';
            }
            if (item.precos_por_forma && typeof item.precos_por_forma === 'object') {
                row.precos_por_forma = Object.assign({}, item.precos_por_forma);
            }
            return row;
        }) : [];
        var nomeLinha = String(entry.cliente || '').trim();
        var ex = entry.cliente_extra;
        if (ex && typeof ex === 'object' && Object.keys(ex).length) {
            var raw = Object.assign({}, ex);
            if (!String(raw.nome || '').trim() && String(raw.razao_social || '').trim()) {
                raw.nome = raw.razao_social;
            }
            if (!String(raw.nome || '').trim()) raw.nome = nomeLinha || 'Cliente';
            state.cliente = sanitizeCliente(raw);
            state.clienteMode = /consumidor\s+n[aã]o\s+identificado/i.test(state.cliente.nome || '')
                ? 'consumidor_final'
                : 'cliente';
        } else if (nomeLinha) {
            if (/consumidor\s+n[aã]o\s+identificado/i.test(nomeLinha)) {
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
        }
        if (entry.entrega) {
            state.entrega.ativa = true;
            state.entrega.modoRetiradaEntrega = 'entrega';
        }
        state.entrega.detalhesEntregaRespondidos = true;
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
            var gm = String((i && (i.codigoGm || i.codigo_nfe || i.codigo_gm)) || '').trim();
            var row = {
                id: String((i && i.id) || ''),
                nome: String((i && i.nome) || ''),
                preco: toNumber(i && i.preco),
                preco_padrao: i && i.preco_padrao != null ? toNumber(i.preco_padrao) : toNumber(i && i.preco),
                qtd: normalizeQty(i && i.qtd, 1),
                codigo: cod,
                codigoGm: gm || cod || '—',
                imagem: '',
                marca: '',
                desconto: 0,
                observacao: ''
            };
            if (i && i.precos_modo) {
                row.precos_modo = String(i.precos_modo).toLowerCase() === 'grupos' ? 'grupos' : 'por_forma';
            }
            if (i && i.precos_grupos && typeof i.precos_grupos === 'object') {
                row.precos_grupos = Object.assign({}, i.precos_grupos);
                if (Array.isArray(i.precos_grupos.formas_a)) {
                    row.precos_grupos.formas_a = i.precos_grupos.formas_a.slice();
                }
                if (Array.isArray(i.precos_grupos.formas_b)) {
                    row.precos_grupos.formas_b = i.precos_grupos.formas_b.slice();
                }
                if (!row.precos_modo) row.precos_modo = 'grupos';
            }
            if (i && i.precos_por_forma && typeof i.precos_por_forma === 'object') {
                row.precos_por_forma = Object.assign({}, i.precos_por_forma);
            }
            return row;
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
        state.entrega.modoRetiradaEntrega = '';
        state.entrega.detalhesEntregaRespondidos = false;
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
        state.entrega.pedidoEntregaPendenteId = null;
        var fp = String(draft.forma_pagamento || '').trim();
        var allowed = [
            '',
            'Dinheiro',
            'PIX',
            'Cartão de débito',
            'Cartão de crédito',
            'Cartão de crédito parcelado',
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
        state.pagamento.mpBalcaoModo = '';
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
        state.pagamento.mpBalcaoModo = '';
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
                    mpBalcaoModo: '',
                    cobrarNoPointMp: false,
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

    function removePagamentoLancamentoAt(index) {
        if (!state.pagamento || !Array.isArray(state.pagamento.lancamentos)) return;
        var i = parseInt(index, 10);
        if (!Number.isFinite(i) || i < 0 || i >= state.pagamento.lancamentos.length) return;
        state.pagamento.lancamentos.splice(i, 1);
        notify();
    }

    function beginEditPagamentoLancamento(index) {
        if (!state.pagamento || !Array.isArray(state.pagamento.lancamentos)) return;
        var i = parseInt(index, 10);
        if (!Number.isFinite(i) || i < 0 || i >= state.pagamento.lancamentos.length) return;
        var L = state.pagamento.lancamentos.splice(i, 1)[0];
        if (!L || typeof L !== 'object') {
            notify();
            return;
        }
        var v = toNumber(L.valor);
        var fmt = String(v.toFixed(2)).replace('.', ',');
        var forma = String(L.forma || '');
        var vr = String(L.valorRecebido || '').trim();
        var patch = {
            forma: forma,
            maquinaId: String(L.maquinaId || ''),
            maquinaNome: String(L.maquinaNome || ''),
            mpBalcaoModo: String(L.mpBalcaoModo || ''),
            creditoParcelas: L.creditoParcelas != null ? parseInt(L.creditoParcelas, 10) || 2 : 2,
            fiadoParcelas: L.fiadoParcelas != null ? parseInt(L.fiadoParcelas, 10) || 1 : 1,
            fiadoDiasVencimento: L.fiadoDiasVencimento != null ? parseInt(L.fiadoDiasVencimento, 10) || 30 : 30,
            outroDetalhes: String(L.outroDetalhes || ''),
            outroPinVerificado: forma === 'Outro',
            valorRecebido: forma === 'Dinheiro' ? (vr || fmt) : '',
            trocoCalculado: forma === 'Dinheiro' ? String(L.trocoCalculado || '') : '',
            valorDestaForma: forma === 'Dinheiro' ? '' : fmt
        };
        Object.assign(state.pagamento, patch);
        notify();
    }

    function reset(keepClient) {
        var next = defaultState();
        if (keepClient && state.cliente) {
            next.cliente = deepClone(state.cliente);
            next.clienteMode = state.clienteMode;
            var c = next.cliente;
            next.entrega.logradouro = String(c.logradouro || '').trim();
            next.entrega.numero = String(c.numero || '').trim();
            next.entrega.bairro = String(c.bairro || '').trim();
            next.entrega.plusCode = String(c.plus_code || '').trim();
            next.entrega.referencia = String(c.referencia_rural || '').trim();
            next.entrega.complemento = '';
            next.entrega.endereco = composeEntregaEnderecoLinhaRapida(next.entrega, c);
        }
        state = next;
        notify();
    }

    function hydrateFromFiadoCobranca(data, opts) {
        opts = opts || {};
        data = data && typeof data === 'object' ? data : {};
        var def = defaultState();
        state = def;
        var emOverlay = !!opts.emOverlay;
        if (!emOverlay) {
            try {
                emOverlay = !!(window.top && window.top !== window.self);
                if (!emOverlay) {
                    emOverlay =
                        new URLSearchParams(window.location.search || '').get('agro_pdv_overlay') === '1';
                }
            } catch (_) {
                emOverlay = false;
            }
        }
        state.clienteMode = data.cliente ? 'cliente' : 'unset';
        state.cliente = data.cliente ? sanitizeCliente(data.cliente) : null;
        state.fiadoCobranca = {
            ativo: true,
            modo: String(data.modo || 'titulo'),
            tituloId: data.titulo_id != null ? data.titulo_id : null,
            tituloIds: Array.isArray(data.titulo_ids) ? data.titulo_ids.slice() : [],
            valorTotal: toNumber(data.valor_total),
            saldoTotal: toNumber(
                data.saldo_total != null ? data.saldo_total : data.valor_total
            ),
            parcial: !!data.parcial,
            resumoTexto: String(data.resumo_texto || ''),
            titulos: Array.isArray(data.titulos) ? data.titulos.slice() : [],
            emOverlay: emOverlay
        };
        if (
            !state.fiadoCobranca.parcial &&
            state.fiadoCobranca.saldoTotal > state.fiadoCobranca.valorTotal + 0.009
        ) {
            state.fiadoCobranca.parcial = true;
        }
        var rotuloCobranca = state.fiadoCobranca.parcial
            ? 'Recebimento fiado (parcial)'
            : 'Quitação fiado';
        state.itens = [
            {
                id: 'fiado-cobranca',
                nome: rotuloCobranca + ' — ' + (state.fiadoCobranca.resumoTexto || 'cliente'),
                qtd: 1,
                preco: state.fiadoCobranca.valorTotal,
                desconto: 0,
                unidade: 'serv'
            }
        ];
        state.entrega = Object.assign({}, def.entrega);
        state.pagamento = Object.assign({}, def.pagamento);
        state.pagamento.lancamentos = [];
        state.venda = Object.assign({}, def.venda);
        state.currentStep = 'pagamento';
        notify();
        return true;
    }

    function exportWizardStateSnapshot() {
        var s = getState();
        return {
            currentStep: s.currentStep,
            clienteMode: s.clienteMode,
            cliente: s.cliente ? deepClone(s.cliente) : null,
            itens: (s.itens || []).map(function (item) {
                return deepClone(item);
            }),
            entrega: deepClone(s.entrega || defaultState().entrega),
            pagamento: deepClone(s.pagamento || defaultState().pagamento),
            venda: deepClone(s.venda || defaultState().venda)
        };
    }

    function hydrateFromEntregaPendente(snapshot, meta) {
        meta = meta || {};
        var def = defaultState();
        var snap = snapshot && typeof snapshot === 'object' ? snapshot : {};
        state.clienteMode = snap.clienteMode || def.clienteMode;
        state.cliente = snap.cliente ? sanitizeCliente(snap.cliente) : null;
        state.itens = Array.isArray(snap.itens)
            ? snap.itens.map(function (item) {
                  return Object.assign({}, item);
              })
            : [];
        state.entrega = Object.assign({}, def.entrega, snap.entrega || {});
        state.pagamento = Object.assign({}, def.pagamento, snap.pagamento || {});
        if (!Array.isArray(state.pagamento.lancamentos)) state.pagamento.lancamentos = [];
        state.venda = Object.assign({}, def.venda, snap.venda || {});
        state.entrega.pedidoEntregaPendenteId = meta.id != null ? meta.id : null;
        state.entrega.entregaFreteLiberadoPagamento = true;
        if (state.entrega.ativa) {
            state.entrega.modoRetiradaEntrega = 'entrega';
            state.entrega.localPagamento = 'loja';
            state.entrega.meioNaEntrega = '';
        }
        state.pagamento.forma = '';
        state.pagamento.lancamentos = [];
        state.pagamento.valorRecebido = '';
        state.pagamento.trocoCalculado = '';
        state.pagamento.valorDestaForma = '';
        state.pagamento.clientRequestId = '';
        state.currentStep = 'pagamento';
        notify();
        return true;
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
        resolveProdutoId: resolveProdutoId,
        addItem: addItem,
        updateItemQuantity: updateItemQuantity,
        updateItemPrice: updateItemPrice,
        setItemPrecoGrupoPreview: setItemPrecoGrupoPreview,
        patchItemCadastro: patchItemCadastro,
        normalizePrice: normalizePrice,
        formatMoneyInputDisplay: formatMoneyInputDisplay,
        sanitizeMoneyInputTyping: sanitizeMoneyInputTyping,
        formatPriceDisplay: formatPriceDisplay,
        removeItem: removeItem,
        clearItems: clearItems,
        setEntregaField: setEntregaField,
        setEntregaPatch: setEntregaPatch,
        setPagamentoField: setPagamentoField,
        setPagamentoPatch: setPagamentoPatch,
        setVendaField: setVendaField,
        hydrateFromBudget: hydrateFromBudget,
        hydrateFromSessionDraft: hydrateFromSessionDraft,
        hydrateFromEntregaPendente: hydrateFromEntregaPendente,
        hydrateFromFiadoCobranca: hydrateFromFiadoCobranca,
        exportWizardStateSnapshot: exportWizardStateSnapshot,
        reset: reset,
        toNumber: toNumber,
        addPagamentoLancamento: addPagamentoLancamento,
        removePagamentoLancamentoAt: removePagamentoLancamentoAt,
        beginEditPagamentoLancamento: beginEditPagamentoLancamento,
        resetPagamentoTranche: resetPagamentoTranche,
        roundQty: roundQty,
        normalizeQty: normalizeQty,
        formatQtyDisplay: formatQtyDisplay,
        qtyStepFor: qtyStepFor,
        recalcularTodasPromocoes: function () {
            recalcularTodasPromocoes();
            notify();
        },
        QTD_MIN: QTD_MIN
    };
})();
