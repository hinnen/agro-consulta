/**
 * PDV — Relacionamento com cliente (atalho F8).
 * Vendas/métricas via API; pets/saúde/anotações no Postgres (ClienteAgro).
 */
(function () {
    'use strict';

    var STORAGE_PREFIX = 'agro_rel_cliente_v1_';

    var TABS = [
        { id: 'resumo', label: 'Resumo' },
        { id: 'historico', label: 'Histórico' },
        { id: 'ciclo_racao', label: 'Ciclo ração', labelTab: 'Ciclo' },
        { id: 'cross_sell', label: 'Cross-sell', labelTab: 'Cross' },
        { id: 'fiado', label: 'Fiado' },
        { id: 'fidelidade', label: 'Cashback', labelTab: 'Cash' },
        { id: 'bonus', label: 'Bônus' },
        { id: 'pets', label: 'Pets' },
        { id: 'saude', label: 'Saúde' },
        { id: 'anotacoes', label: 'Anotações', labelTab: 'Anot.' },
        { id: 'contato', label: 'Contato' },
    ];

    var dom = {};
    var bootstrap = {};
    var apiData = null;
    var clientePk = null;
    var clienteExtra = { pets: [], lembretes: [], anotacoes: '' };
    var activeTab = 'resumo';
    var relCartAdded = {};
    var historicoOffset = 0;
    var historicoHasMore = false;
    var historicoLoadingMore = false;
    var secaoCarregada = { ciclo_racao: false, cross_sell: false };
    var secaoLoading = { ciclo_racao: false, cross_sell: false };
    var HISTORICO_PAGE = 12;

    function money(v) {
        var n = Number(v);
        if (!isFinite(n)) n = 0;
        return 'R$ ' + n.toFixed(2).replace('.', ',');
    }

    function esc(s) {
        if (s == null) return '';
        return String(s)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    function csrfToken() {
        return bootstrap.csrfToken || '';
    }

    function defaultExtra() {
        return { pets: [], lembretes: [], anotacoes: '' };
    }

    function extraFromApi(data) {
        var e = (data && data.extras) || {};
        return {
            pets: Array.isArray(e.pets) ? e.pets.slice() : [],
            lembretes: Array.isArray(e.lembretes) ? e.lembretes.slice() : [],
            anotacoes: typeof e.anotacoes === 'string' ? e.anotacoes : '',
        };
    }

    function extrasTemDados(extra) {
        if (!extra) return false;
        if ((extra.pets && extra.pets.length) || (extra.lembretes && extra.lembretes.length)) return true;
        return !!(extra.anotacoes && String(extra.anotacoes).trim());
    }

    function loadLegacyLocalExtra(pk) {
        try {
            var raw = localStorage.getItem(STORAGE_PREFIX + pk);
            return raw ? JSON.parse(raw) : defaultExtra();
        } catch (e) {
            return defaultExtra();
        }
    }

    function clearLegacyLocalExtra(pk) {
        try {
            localStorage.removeItem(STORAGE_PREFIX + pk);
        } catch (e) {}
    }

    function saveExtraUrl() {
        return (
            (bootstrap.urls && bootstrap.urls.apiPdvRelacionamentoClienteExtras) ||
            '/api/pdv/relacionamento-cliente/extras/'
        );
    }

    function saveExtraToServer(extra) {
        return fetch(saveExtraUrl(), {
            method: 'POST',
            credentials: 'same-origin',
            headers: {
                'Content-Type': 'application/json',
                Accept: 'application/json',
                'X-CSRFToken': csrfToken(),
            },
            body: JSON.stringify({
                cliente_agro_pk: clientePk,
                pets: extra.pets || [],
                lembretes: extra.lembretes || [],
                anotacoes: extra.anotacoes || '',
            }),
        })
            .then(function (r) {
                return r.json();
            })
            .then(function (data) {
                if (!data || !data.ok) throw new Error((data && data.erro) || 'Falha ao salvar');
                return extraFromApi({ extras: data.extras });
            });
    }

    function tryMigrateLocalExtras(pk, serverExtra) {
        var local = loadLegacyLocalExtra(pk);
        if (!extrasTemDados(local)) return Promise.resolve(serverExtra);
        if (extrasTemDados(serverExtra)) {
            clearLegacyLocalExtra(pk);
            return Promise.resolve(serverExtra);
        }
        return saveExtraToServer(local)
            .then(function (saved) {
                clearLegacyLocalExtra(pk);
                return saved;
            })
            .catch(function () {
                return serverExtra;
            });
    }

    function persistExtra(extra, onOk) {
        return saveExtraToServer(extra)
            .then(function (saved) {
                clienteExtra = saved;
                if (apiData) apiData.extras = saved;
                if (onOk) onOk(saved);
                return saved;
            })
            .catch(function (err) {
                alert(err.message || 'Não foi possível salvar no cadastro.');
                throw err;
            });
    }

    function getClienteFromPdv() {
        if (window.AgroPdvState && typeof window.AgroPdvState.getState === 'function') {
            var st = window.AgroPdvState.getState();
            if (st && st.cliente && st.clienteMode !== 'consumidor_final') {
                return st.cliente;
            }
        }
        return null;
    }

    function fillBuscaGm(codigo) {
        var inp = document.getElementById('pdv-product-search');
        if (!inp || !codigo) return;
        inp.value = String(codigo).trim();
        inp.focus();
        inp.dispatchEvent(new Event('input', { bubbles: true }));
    }

    function btnCartHtml(codigo, large, added, disponivel) {
        if (!codigo) return '';
        if (disponivel === false) {
            var baseOff =
                large
                    ? 'shrink-0 rounded-xl border-2 border-slate-200 bg-slate-50 px-3 py-1.5 text-xs font-black text-slate-400'
                    : 'shrink-0 rounded-lg border-2 border-slate-200 bg-slate-50 px-2.5 py-1 text-[10px] font-black text-slate-400';
            return (
                '<span class="' +
                baseOff +
                '" title="Produto não está no cadastro atual">Indisp.</span>'
            );
        }
        var base =
            large
                ? 'rel-add-gm shrink-0 rounded-xl border-2 px-3 py-1.5 text-xs font-black'
                : 'rel-add-gm shrink-0 rounded-lg border-2 px-2.5 py-1 text-[10px] font-black';
        if (added) {
            return (
                '<button type="button" class="' +
                base +
                ' rel-add-gm--done" disabled aria-label="Já no carrinho" data-gm="' +
                esc(codigo) +
                '"><span aria-hidden="true">✓</span> No carrinho</button>'
            );
        }
        return (
            '<button type="button" class="' +
            base +
            ' border-emerald-500 bg-emerald-50 text-emerald-900 hover:bg-emerald-100" aria-label="Adicionar 1 unidade ao carrinho agora" data-gm="' +
            esc(codigo) +
            '"><span aria-hidden="true">🛒</span> +1 un.</button>'
        );
    }

    function histCompraMeta(p, inline) {
        if (!p) return '';
        var parts = [];
        if (p.qtd_total != null && p.qtd_total !== '') {
            parts.push(
                'Já comprou <span class="font-black text-slate-600">' +
                    esc(String(p.qtd_total)) +
                    ' un.</span>'
            );
        }
        if (p.vezes != null && p.vezes !== '') {
            parts.push('<span class="font-black text-slate-600">' + esc(String(p.vezes)) + '×</span> na loja');
        }
        if (!parts.length) return '';
        var text = parts.join(' · ');
        if (inline) {
            return (
                '<span class="shrink-0 text-[10px] font-bold text-slate-500 sm:text-[11px]">· ' + text + '</span>'
            );
        }
        return (
            '<p class="mt-0.5 text-[10px] font-bold leading-snug text-slate-500 sm:text-[11px]">' +
            text +
            '</p>'
        );
    }

    function btnCartCol(codigo, large, disponivel) {
        return (
            '<div class="rel-add-gm-col flex shrink-0 items-center border-l border-slate-200 pl-2 sm:pl-3">' +
            btnCart(codigo, large, disponivel) +
            '</div>'
        );
    }

    function topProdutoValCol(valor) {
        return (
            '<div class="rel-top-prod-col rel-top-prod-col--stat">' +
            '<span class="rel-top-prod-val">' +
            esc(valor) +
            '</span></div>'
        );
    }

    function topProdutoRowHtml(p, largeBtn) {
        var vezes = p.vezes != null && p.vezes !== '' ? String(p.vezes) : '—';
        var total = p.qtd_total != null && p.qtd_total !== '' ? String(p.qtd_total) : '—';
        return (
            '<li class="rel-top-prod-row">' +
            '<div class="rel-top-prod-col rel-top-prod-col--nome">' +
            '<span class="rel-top-prod-nome" title="' +
            esc(p.descricao) +
            '">' +
            esc(p.descricao) +
            '</span></div>' +
            topProdutoValCol(vezes) +
            topProdutoValCol(total) +
            '<div class="rel-top-prod-col rel-top-prod-col--btn">' +
            btnCart(p.codigo, largeBtn, p.catalogo_disponivel !== false) +
            '</div></li>'
        );
    }

    function topProdutoListHtml(produtos, largeBtn) {
        if (!produtos || !produtos.length) return '';
        var html =
            '<div class="rel-top-prod-wrap">' +
            '<p class="rel-top-prod-titulo">Top produtos <span class="rel-top-prod-titulo-sub">— histórico do cliente</span></p>' +
            '<ul class="rel-top-prod-list">' +
            '<li class="rel-top-prod-head">' +
            '<div class="rel-top-prod-col rel-top-prod-col--nome">Produto</div>' +
            '<div class="rel-top-prod-col rel-top-prod-col--stat">Vezes comprada</div>' +
            '<div class="rel-top-prod-col rel-top-prod-col--stat">Total comprado</div>' +
            '<div class="rel-top-prod-col rel-top-prod-col--btn">Balcão</div></li>';
        produtos.slice(0, 5).forEach(function (p) {
            html += topProdutoRowHtml(p, largeBtn);
        });
        html += '</ul></div>';
        return html;
    }

    function btnCart(codigo, large, disponivel) {
        var key = String(codigo || '').trim();
        return btnCartHtml(key, large, !!relCartAdded[key], disponivel);
    }

    function markCartBtnDone(btn) {
        if (!btn) return;
        var gm = btn.getAttribute('data-gm') || '';
        relCartAdded[gm] = true;
        btn.classList.remove('border-emerald-500', 'bg-emerald-50', 'text-emerald-900', 'hover:bg-emerald-100', 'rel-add-gm--busy');
        btn.classList.add('rel-add-gm--done');
        btn.disabled = true;
        btn.setAttribute('aria-label', 'Já no carrinho');
        btn.innerHTML = '<span aria-hidden="true">✓</span> No carrinho';
    }

    function addProductToCartFromRel(btn) {
        var gm = (btn.getAttribute('data-gm') || '').trim();
        if (!gm || btn.classList.contains('rel-add-gm--done')) return;
        btn.classList.add('rel-add-gm--busy');
        btn.disabled = true;
        var prevHtml = btn.innerHTML;
        btn.innerHTML = '<span aria-hidden="true">…</span>';

        function done(ok) {
            if (ok) {
                markCartBtnDone(btn);
                return;
            }
            btn.classList.remove('rel-add-gm--busy');
            btn.disabled = false;
            btn.innerHTML = prevHtml;
            fillBuscaGm(gm);
        }

        if (typeof window.AgroPdvAddProductByCode === 'function') {
            Promise.resolve(window.AgroPdvAddProductByCode(gm)).then(done).catch(function () {
                done(false);
            });
            return;
        }
        fillBuscaGm(gm);
        markCartBtnDone(btn);
    }

    function moneyCompact(v) {
        var n = Number(v);
        if (!isFinite(n)) n = 0;
        return n.toFixed(2).replace('.', ',');
    }

    function tabDisplayLabel(t) {
        return (t && (t.labelTab || t.label)) || '';
    }

    function fiadoTabAlertMeta(d) {
        var f = (d && d.financeiro_fiado) || {};
        var total = Number(f.total_aberto) || 0;
        if (total <= 0) return null;
        var tit = f.titulos_abertos || [];
        var vencido = false;
        tit.forEach(function (t) {
            if (t.vencido) vencido = true;
        });
        return { total: total, vencido: vencido };
    }

    function resumoTabAlertMeta(d) {
        var m = (d && d.metricas) || {};
        if (m.ultima_visita_dias != null && m.ultima_visita_dias > 60) {
            return { extra: String(m.ultima_visita_dias) + 'd', title: 'Cliente sumido há ' + m.ultima_visita_dias + ' dias' };
        }
        return null;
    }

    function cicloTabAlertMeta(d) {
        var rows = (d && d.ciclo_racao) || [];
        var n = 0;
        rows.forEach(function (c) {
            if (c.status === 'atrasado') n += 1;
        });
        if (n <= 0) return null;
        return { extra: String(n), title: n + ' recompra(s) atrasada(s)' };
    }

    function fiadoGestaoClienteUrl(pk) {
        var base = (bootstrap.urls && bootstrap.urls.fiadoGestao) || '/fiado/';
        var join = base.indexOf('?') >= 0 ? '&' : '?';
        return base + join + 'from=pdv&cliente=' + encodeURIComponent(String(pk));
    }

    function formaBadge(forma) {
        var f = (forma || '').trim();
        if (!f) return '';
        return (
            '<span class="shrink-0 rounded-lg border border-slate-200 bg-slate-100 px-2 py-0.5 text-[11px] font-black uppercase text-slate-700">' +
            esc(f) +
            '</span>'
        );
    }

    function resumoMetricCard(label, valueHtml, valueClass) {
        return (
            '<div class="rel-resumo-card">' +
            '<p class="rel-resumo-card-label">' +
            esc(label) +
            '</p>' +
            '<div class="rel-resumo-card-val' +
            (valueClass ? ' ' + valueClass : '') +
            '">' +
            valueHtml +
            '</div></div>'
        );
    }

    function petsResumoNomes(extra) {
        var pets = (extra && extra.pets) || [];
        if (!pets.length) return '—';
        var nomes = pets
            .map(function (p) {
                return String((p && p.nome) || '').trim();
            })
            .filter(Boolean);
        return nomes.length ? esc(nomes.join(', ')) : '—';
    }

    function relacionamentoApiUrl(extraParams) {
        var url = (bootstrap.urls && bootstrap.urls.apiPdvRelacionamentoCliente) || '/api/pdv/relacionamento-cliente/';
        var qs = 'cliente_agro_pk=' + encodeURIComponent(String(clientePk));
        if (extraParams) {
            qs += '&' + extraParams;
        }
        return url + '?' + qs;
    }

    function fetchRelacionamentoJson(extraParams) {
        return fetch(relacionamentoApiUrl(extraParams), {
            credentials: 'same-origin',
            headers: { Accept: 'application/json' },
        }).then(function (r) {
            return r.json();
        });
    }

    function appendHistoricoVendas(data) {
        if (!apiData || !data || !data.historico_rapido) return;
        var hr = data.historico_rapido;
        if (!apiData.historico_rapido) apiData.historico_rapido = {};
        var prev = (apiData.historico_rapido.vendas || []).slice();
        var novas = hr.vendas || [];
        apiData.historico_rapido.vendas = prev.concat(novas);
        if (hr.has_more != null) apiData.historico_rapido.has_more = hr.has_more;
        if (hr.total != null) apiData.historico_rapido.total = hr.total;
    }

    function syncHistoricoMetaFromApi() {
        var hr = (apiData && apiData.historico_rapido) || {};
        historicoOffset = (hr.vendas && hr.vendas.length) || 0;
        historicoHasMore = !!hr.has_more;
    }

    function ensureSecaoCarregada(tabId) {
        if (!clientePk || !apiData) return Promise.resolve();
        if (tabId === 'ciclo_racao' && !secaoCarregada.ciclo_racao && !secaoLoading.ciclo_racao) {
            secaoLoading.ciclo_racao = true;
            return fetchRelacionamentoJson('secao=ciclo_racao')
                .then(function (data) {
                    if (!data || !data.ok) throw new Error((data && data.erro) || 'Falha ao carregar ciclo');
                    apiData.ciclo_racao = data.ciclo_racao || [];
                    secaoCarregada.ciclo_racao = true;
                    renderTabs();
                    if (activeTab === 'ciclo_racao' || activeTab === 'resumo') renderTabContent();
                })
                .catch(function (err) {
                    if (activeTab === 'ciclo_racao' && dom.panel) {
                        dom.panel.innerHTML =
                            '<p class="text-sm font-bold text-red-700">' + esc(err.message || 'Erro') + '</p>';
                    }
                })
                .finally(function () {
                    secaoLoading.ciclo_racao = false;
                });
        }
        if (tabId === 'cross_sell' && !secaoCarregada.cross_sell && !secaoLoading.cross_sell) {
            secaoLoading.cross_sell = true;
            return fetchRelacionamentoJson('secao=cross_sell')
                .then(function (data) {
                    if (!data || !data.ok) throw new Error((data && data.erro) || 'Falha ao carregar cross-sell');
                    apiData.cross_sell = data.cross_sell || [];
                    secaoCarregada.cross_sell = true;
                    if (activeTab === 'cross_sell') renderTabContent();
                })
                .catch(function (err) {
                    if (activeTab === 'cross_sell' && dom.panel) {
                        dom.panel.innerHTML =
                            '<p class="text-sm font-bold text-red-700">' + esc(err.message || 'Erro') + '</p>';
                    }
                })
                .finally(function () {
                    secaoLoading.cross_sell = false;
                });
        }
        return Promise.resolve();
    }

    function prefetchCicloParaResumo() {
        if (secaoCarregada.ciclo_racao || secaoLoading.ciclo_racao) return;
        ensureSecaoCarregada('ciclo_racao');
    }

    function renderVendaHistoricoBlock(v) {
        var origemBadge =
            v.origem === 'erp'
                ? '<span class="rounded-md border border-slate-300 bg-slate-100 px-2 py-0.5 text-[10px] font-black uppercase text-slate-600">ERP</span>'
                : '<span class="rounded-md border border-emerald-300 bg-emerald-50 px-2 py-0.5 text-[10px] font-black uppercase text-emerald-800">SisVale</span>';
        var html =
            '<details class="group rounded-2xl border-2 border-slate-200 bg-white shadow-sm">' +
            '<summary class="flex cursor-pointer list-none flex-wrap items-center gap-x-3 gap-y-2 px-4 py-3.5 sm:px-5 sm:py-4 [&::-webkit-details-marker]:hidden">' +
            '<span class="text-lg font-black text-slate-900 sm:text-xl">' +
            money(v.total) +
            '</span>' +
            origemBadge +
            formaBadge(v.forma) +
            '<span class="text-sm font-bold text-slate-500 sm:ml-auto">' +
            esc(v.data) +
            '</span>' +
            '<span class="w-full text-[11px] font-black uppercase text-emerald-700 group-open:hidden sm:w-auto sm:text-xs">Toque para ver itens ▾</span></summary>' +
            '<div class="border-t-2 border-slate-100 bg-slate-50/80 px-3 py-3 sm:px-5 sm:py-4">' +
            '<table class="w-full border-collapse text-left text-sm sm:text-base"><thead><tr class="text-[11px] font-black uppercase text-slate-500 sm:text-xs">' +
            '<th class="pb-2 pr-2">Produto</th><th class="pb-2 px-2 text-center">Qtd venda</th><th class="pb-2 px-2 text-right">Total</th><th class="pb-2 pl-2 text-right">Balcão</th></tr></thead><tbody>';
        (v.itens || []).forEach(function (it) {
            html +=
                '<tr class="border-t border-slate-200/80"><td class="py-2.5 pr-2 font-bold text-slate-900">' +
                esc(it.descricao) +
                '</td><td class="px-2 py-2.5 text-center font-black text-slate-800">' +
                it.qtd +
                '</td><td class="px-2 py-2.5 text-right font-black text-emerald-800">' +
                money(it.total) +
                '</td><td class="py-2.5 pl-2 text-right">' +
                btnCart(it.codigo, false, it.catalogo_disponivel !== false) +
                '</td></tr>';
        });
        html += '</tbody></table></div></details>';
        return html;
    }

    function renderResumoCards(d, extra) {
        var m = d.metricas || {};
        var fid = d.fidelidade || {};
        var c = d.cliente || {};
        var waUrl = (c.whatsapp_url || '').trim();
        var waBtn =
            waUrl
                ? '<a class="rel-resumo-wa" href="' +
                  esc(waUrl) +
                  '" target="_blank" rel="noopener noreferrer"><span aria-hidden="true">💬</span> Conversar</a>'
                : '<span class="text-xs font-bold text-slate-400">Sem nº</span>';
        return (
            '<div class="rel-resumo-cards">' +
            resumoMetricCard('Visitas', String(m.total_vendas || 0), 'rel-resumo-card-val--dark') +
            resumoMetricCard('Ticket méd.', money(m.ticket_medio), 'rel-resumo-card-val--dark') +
            resumoMetricCard('Cashback', money(fid.cashback), 'rel-resumo-card-val--cash') +
            resumoMetricCard('Vale créd.', money(fid.vale_credito), 'rel-resumo-card-val--vale') +
            resumoMetricCard('Total compr.', money(m.total_comprado), 'rel-resumo-card-val--dark') +
            resumoMetricCard(
                'Freq. visitas',
                m.frequencia_media_dias != null ? m.frequencia_media_dias + ' dias' : '—',
                'rel-resumo-card-val--dark'
            ) +
            resumoMetricCard(
                'Últ. visita',
                m.ultima_visita_dias != null ? 'há ' + m.ultima_visita_dias + ' dias' : '—',
                'rel-resumo-card-val--dark'
            ) +
            resumoMetricCard('Pets', petsResumoNomes(extra), 'rel-resumo-card-val--pets') +
            resumoMetricCard('WhatsApp', waBtn, 'rel-resumo-card-val--wa') +
            '</div>'
        );
    }

    function renderResumo(d, extra) {
        var html =
            '<div class="space-y-3 text-sm">' +
            '<p class="rounded-xl border-2 border-emerald-200 bg-emerald-50 px-3 py-2 text-[11px] font-bold text-emerald-950">Pets, saúde e anotações salvos no cadastro — qualquer caixa da loja vê. Demais abas em evolução.</p>';
        html += renderResumoCards(d, extra);
        var top = (d.historico_rapido && d.historico_rapido.top_produtos) || [];
        html += topProdutoListHtml(top, false);
        html += '</div>';
        return html;
    }

    function renderHistorico(d) {
        var vendas = (d.historico_rapido && d.historico_rapido.vendas) || [];
        var top = (d.historico_rapido && d.historico_rapido.top_produtos) || [];
        var html = '<div class="pdv-rel-historico space-y-6">';

        html +=
            '<section><h3 class="mb-3 text-sm font-black uppercase tracking-wide text-slate-700">Itens mais comprados</h3>';
        if (!top.length) {
            html += '<p class="rounded-xl border border-dashed border-slate-300 bg-slate-50 px-4 py-6 text-center text-base font-bold text-slate-500">Sem histórico PDV para este cliente.</p>';
        } else {
            html += '<div class="grid gap-3 lg:grid-cols-2">';
            top.forEach(function (p, i) {
                html +=
                    '<article class="flex flex-col gap-2 rounded-2xl border-2 border-slate-200 bg-white p-4 shadow-sm">' +
                    '<div class="flex items-start gap-3">' +
                    '<span class="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl bg-emerald-600 text-sm font-black text-white">' +
                    (i + 1) +
                    '</span>' +
                    '<div class="min-w-0 flex-1">' +
                    '<p class="text-base font-black leading-snug text-slate-900 sm:text-lg">' +
                    esc(p.descricao) +
                    '</p>' +
                    '<p class="mt-1 text-sm font-bold text-slate-600">Cód. <span class="font-mono text-slate-800">' +
                    esc(p.codigo || '—') +
                    '</span></p></div></div>' +
                    '<div class="flex flex-wrap items-center justify-between gap-2 border-t border-slate-100 pt-3">' +
                    '<div class="min-w-0 flex-1 text-sm font-bold text-slate-600">' +
                    histCompraMeta(p) +
                    '</div>' +
                    btnCartCol(p.codigo, true, p.catalogo_disponivel !== false) +
                    '</div></article>';
            });
            html += '</div>';
        }
        html += '</section>';

        html +=
            '<section><h3 class="mb-3 text-sm font-black uppercase tracking-wide text-slate-700">Últimas vendas</h3>';
        if (!vendas.length) {
            html += '<p class="rounded-xl border border-dashed border-slate-300 bg-slate-50 px-4 py-6 text-center text-base font-bold text-slate-500">Nenhuma venda no histórico deste cliente.</p>';
        } else {
            html += '<div id="rel-historico-vendas" class="space-y-3">';
            vendas.forEach(function (v) {
                html += renderVendaHistoricoBlock(v);
            });
            html += '</div>';
            if (historicoHasMore) {
                html +=
                    '<div id="rel-historico-mais-wrap" class="pt-2 text-center">' +
                    '<button type="button" id="rel-historico-mais" class="min-h-[44px] rounded-xl border-2 border-emerald-500 bg-emerald-50 px-6 py-2.5 text-sm font-black uppercase text-emerald-900 hover:bg-emerald-100 disabled:opacity-60"' +
                    (historicoLoadingMore ? ' disabled' : '') +
                    '>' +
                    (historicoLoadingMore ? 'Carregando…' : 'Carregar mais vendas') +
                    '</button></div>';
            }
        }
        html += '</section></div>';
        return html;
    }

    function renderCiclo(d) {
        if (secaoLoading.ciclo_racao) {
            return '<p class="text-sm font-bold text-slate-600">Carregando ciclo de ração…</p>';
        }
        var rows = d.ciclo_racao || [];
        if (!rows.length) {
            return '<p class="text-sm text-slate-600">Nenhum produto tipo ração/sachê no histórico PDV. Estimativa aparece quando houver compras com «ração» no nome.</p>';
        }
        var html = '<div class="space-y-2 text-sm">';
        rows.forEach(function (r) {
            var badge =
                r.status === 'atrasado'
                    ? 'bg-red-100 text-red-900 border-red-300'
                    : r.status === 'recompra'
                      ? 'bg-amber-100 text-amber-950 border-amber-300'
                      : 'bg-emerald-50 text-emerald-900 border-emerald-200';
            html +=
                '<div class="rounded-xl border px-3 py-2 ' +
                badge +
                '"><div class="font-black">' +
                esc(r.descricao) +
                '</div><div class="mt-1 text-xs font-bold">Última: ' +
                (r.dias_desde != null ? 'há ' + r.dias_desde + ' dias' : '—') +
                ' · Qtd ' +
                r.ultima_qtd +
                (r.media_intervalo_dias ? ' · Intervalo médio ~' + r.media_intervalo_dias + 'd' : '') +
                (r.estimativa_dias_pacote ? ' · Pacote ~' + r.estimativa_dias_pacote + 'd' : '') +
                btnCart(r.codigo) +
                '</div></div>';
        });
        html += '</div>';
        return html;
    }

    function renderCross(d) {
        if (secaoLoading.cross_sell) {
            return '<p class="text-sm font-bold text-slate-600">Carregando sugestões…</p>';
        }
        var rows = d.cross_sell || [];
        if (!rows.length) {
            return '<p class="text-sm text-slate-600">Sem sugestões automáticas ainda (depende do histórico de compras na loja).</p>';
        }
        var html = '<ul class="space-y-2 text-sm">';
        rows.forEach(function (r) {
            html +=
                '<li class="rounded-lg border border-sky-200 bg-sky-50 px-3 py-2"><div class="font-black text-slate-900">' +
                esc(r.descricao) +
                '</div><div class="text-[10px] font-bold text-sky-900">' +
                esc(r.motivo) +
                btnCart(r.codigo) +
                '</div></li>';
        });
        html += '</ul>';
        return html;
    }

    function renderFiado(d) {
        var f = d.financeiro_fiado || {};
        var pk = clientePk || (d.cliente && d.cliente.pk);
        var gestaoUrl = pk ? fiadoGestaoClienteUrl(pk) : ((bootstrap.urls && bootstrap.urls.fiadoGestao) || '/fiado/') + '?from=pdv';
        var html =
            '<div class="space-y-3 text-sm">' +
            '<a href="' +
            esc(gestaoUrl) +
            '" class="flex min-h-[48px] w-full items-center justify-center gap-2 rounded-xl border-2 border-orange-500 bg-orange-600 px-4 py-3 text-center text-xs font-black uppercase tracking-wide text-white no-underline shadow-md hover:bg-orange-700">' +
            '<span aria-hidden="true">💳</span> Lançamentos do cliente · baixa</a>' +
            '<div class="rounded-xl border-2 border-orange-200 bg-orange-50 p-3"><p class="text-[10px] font-black uppercase">Total em aberto</p><p class="text-xl font-black text-orange-950">' +
            money(f.total_aberto) +
            '</p><p class="text-xs font-bold text-slate-700">Limite local: ' +
            money(f.limite_local) +
            '</p></div>';
        var tit = f.titulos_abertos || [];
        if (!tit.length) html += '<p class="text-xs text-slate-500">Sem títulos fiado abertos no Agro.</p>';
        tit.forEach(function (t) {
            html +=
                '<div class="rounded-lg border px-2 py-2 ' +
                (t.vencido ? 'border-red-300 bg-red-50' : 'border-slate-200') +
                '"><span class="font-black">' +
                esc(t.documento) +
                '</span> · Venc. ' +
                esc(t.vencimento || '—') +
                ' · <span class="font-black">' +
                money(t.saldo) +
                '</span></div>';
        });
        html += '</div>';
        return html;
    }

    function renderFidelidade(d) {
        var f = d.fidelidade || {};
        return (
            '<div class="grid gap-3 sm:grid-cols-2 text-sm">' +
            '<div class="rounded-xl border-2 border-emerald-300 bg-emerald-50 p-4"><p class="text-[10px] font-black uppercase">Cashback</p><p class="text-2xl font-black text-emerald-800">' +
            money(f.cashback) +
            '</p></div>' +
            '<div class="rounded-xl border-2 border-violet-300 bg-violet-50 p-4"><p class="text-[10px] font-black uppercase">Vale crédito</p><p class="text-2xl font-black text-violet-900">' +
            money(f.vale_credito) +
            '</p></div></div>'
        );
    }

    function renderBonus(d) {
        var rows = (d && d.bonus) || [];
        if (!rows.length) {
            return (
                '<p class="rounded-xl border-2 border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-center text-sm font-bold text-slate-500">' +
                'Nenhum brinde registrado ainda. Saídas «Brinde cliente» no Uso loja aparecem aqui.' +
                '</p>'
            );
        }
        function fmtQtd(n) {
            var x = Number(n);
            if (!isFinite(x)) return '0';
            if (Math.abs(x - Math.round(x)) < 0.0005) return String(Math.round(x));
            return x.toFixed(3).replace(/\.?0+$/, '');
        }
        function fmtData(iso) {
            if (!iso) return '—';
            try {
                var dt = new Date(iso);
                if (isNaN(dt.getTime())) return iso;
                var dd = String(dt.getDate()).padStart(2, '0');
                var mm = String(dt.getMonth() + 1).padStart(2, '0');
                var yy = String(dt.getFullYear()).slice(-2);
                return dd + '/' + mm + '/' + yy;
            } catch (e) {
                return iso;
            }
        }
        var html =
            '<div class="space-y-2">' +
            '<p class="text-xs font-bold text-slate-600">Brindes / uso loja vinculados a este cliente.</p>';
        rows.forEach(function (r) {
            var itens = (r.itens || [])
                .map(function (it) {
                    return esc(it.nome || it.produto_id) + ' × ' + fmtQtd(it.quantidade);
                })
                .join(' · ');
            var totV = 0;
            (r.itens || []).forEach(function (it) {
                var pv = Number(it.preco_venda);
                var q = Number(it.quantidade);
                if (isFinite(pv) && isFinite(q)) totV += pv * q;
            });
            html +=
                '<div class="rounded-xl border-2 ' +
                (r.estornado ? 'border-red-200 bg-red-50 opacity-75' : 'border-violet-200 bg-violet-50') +
                ' px-3 py-2.5">' +
                '<div class="flex flex-wrap items-start justify-between gap-2">' +
                '<div class="min-w-0 text-sm font-bold text-slate-800">' +
                '<span class="font-black">#' +
                esc(String(r.id)) +
                '</span> · ' +
                esc(fmtData(r.criado_em)) +
                ' · ' +
                esc(r.deposito_label || r.deposito || '') +
                (r.estornado
                    ? ' <span class="text-[10px] font-black uppercase text-red-700">Estornado</span>'
                    : '') +
                '<div class="mt-0.5 text-sm font-semibold text-slate-700 leading-snug">' +
                (itens || '—') +
                '</div>' +
                '<div class="mt-0.5 text-[11px] font-semibold text-slate-500">Quem: ' +
                esc(r.quem_levou || '—') +
                ' · PIN: ' +
                esc(r.operador_pin || '—') +
                '</div>' +
                '</div>' +
                '<div class="shrink-0 text-right text-sm font-black text-violet-950">' +
                money(totV) +
                '</div>' +
                '</div></div>';
        });
        html += '</div>';
        return html;
    }

    function renderMetricas(d) {
        var m = d.metricas || {};
        return (
            '<ul class="space-y-2 text-sm font-bold text-slate-800">' +
            '<li>Total de vendas PDV: <span class="font-black">' +
            (m.total_vendas || 0) +
            '</span></li>' +
            '<li>Total comprado: <span class="font-black">' +
            money(m.total_comprado) +
            '</span></li>' +
            '<li>Ticket médio: <span class="font-black">' +
            money(m.ticket_medio) +
            '</span></li>' +
            '<li>Frequência média entre visitas: <span class="font-black">' +
            (m.frequencia_media_dias != null ? m.frequencia_media_dias + ' dias' : '—') +
            '</span></li>' +
            '<li>Última visita: <span class="font-black">' +
            (m.ultima_visita_dias != null ? 'há ' + m.ultima_visita_dias + ' dias' : '—') +
            '</span></li></ul>'
        );
    }

    function renderPets(extra) {
        var pets = extra.pets || [];
        var html =
            '<div class="space-y-3 text-sm"><p class="text-xs font-bold text-slate-600">Salvo no cadastro do cliente — qualquer caixa vê.</p>' +
            '<div id="rel-pets-list" class="space-y-2">';
        pets.forEach(function (p, i) {
            html +=
                '<div class="rounded-lg border border-slate-200 px-2 py-2 text-xs font-bold">' +
                esc(p.nome) +
                ' · ' +
                esc(p.raca || '—') +
                ' · ' +
                esc(p.porte || '—') +
                (p.idade ? ' · ' + esc(p.idade) : '') +
                ' <button type="button" class="rel-pet-del ml-1 text-red-700 underline" data-i="' +
                i +
                '">remover</button></div>';
        });
        html +=
            '</div><div class="grid gap-2 sm:grid-cols-2">' +
            '<input id="rel-pet-nome" class="rounded-lg border border-slate-300 px-2 py-2 text-sm" placeholder="Nome do pet">' +
            '<input id="rel-pet-raca" class="rounded-lg border border-slate-300 px-2 py-2 text-sm" placeholder="Raça">' +
            '<input id="rel-pet-porte" class="rounded-lg border border-slate-300 px-2 py-2 text-sm" placeholder="Porte (P/M/G)">' +
            '<input id="rel-pet-idade" class="rounded-lg border border-slate-300 px-2 py-2 text-sm" placeholder="Idade">' +
            '</div><button type="button" id="rel-pet-add" class="rounded-xl bg-slate-800 px-4 py-2 text-xs font-black uppercase text-white">Adicionar pet</button></div>';
        return html;
    }

    function renderSaude(extra) {
        var rows = extra.lembretes || [];
        var html =
            '<div class="space-y-3 text-sm"><p class="text-xs font-bold text-slate-600">Lembretes salvos no cadastro do cliente.</p><div id="rel-saude-list" class="space-y-2">';
        rows.forEach(function (r, i) {
            html +=
                '<div class="rounded-lg border px-2 py-2 text-xs font-bold ' +
                (r.vencido ? 'border-red-300 bg-red-50' : 'border-slate-200') +
                '">' +
                esc(r.tipo) +
                ' · ' +
                esc(r.produto || '') +
                ' · Vence ' +
                esc(r.data) +
                ' <button type="button" class="rel-saude-del text-red-700 underline" data-i="' +
                i +
                '">remover</button></div>';
        });
        html +=
            '</div><div class="grid gap-2 sm:grid-cols-2">' +
            '<select id="rel-saude-tipo" class="rounded-lg border border-slate-300 px-2 py-2 text-sm"><option>Vacina</option><option>Carrapaticida</option><option>Vermífugo</option><option>Outro</option></select>' +
            '<input id="rel-saude-data" type="date" class="rounded-lg border border-slate-300 px-2 py-2 text-sm">' +
            '<input id="rel-saude-prod" class="sm:col-span-2 rounded-lg border border-slate-300 px-2 py-2 text-sm" placeholder="Produto / observação">' +
            '</div><button type="button" id="rel-saude-add" class="rounded-xl bg-slate-800 px-4 py-2 text-xs font-black uppercase text-white">Adicionar lembrete</button></div>';
        return html;
    }

    function renderAnotacoes(extra) {
        return (
            '<div class="space-y-2 text-sm"><p class="text-xs font-bold text-slate-600">Preferências do balcão — salvas no cadastro do cliente.</p>' +
            '<textarea id="rel-anotacoes-ta" rows="8" class="w-full rounded-xl border-2 border-slate-300 px-3 py-2 text-sm font-semibold">' +
            esc(extra.anotacoes || '') +
            '</textarea>' +
            '<button type="button" id="rel-anotacoes-save" class="rounded-xl bg-emerald-600 px-4 py-2 text-xs font-black uppercase text-white">Salvar anotação</button></div>'
        );
    }

    function renderContato(d) {
        var c = d.cliente || {};
        var html =
            '<div class="space-y-3 text-sm"><p class="font-black text-slate-900">' +
            esc(c.nome) +
            '</p><p class="text-xs font-bold text-slate-600">' +
            esc(c.endereco || 'Sem endereço') +
            '</p>';
        if (c.whatsapp) {
            html +=
                '<p class="font-bold">WhatsApp: ' +
                esc(c.whatsapp) +
                (c.whatsapp_url
                    ? ' <a class="text-emerald-700 underline" href="' +
                      esc(c.whatsapp_url) +
                      '" target="_blank" rel="noopener">Abrir</a>'
                    : '') +
                '</p>';
        }
        html += '</div>';
        return html;
    }

    function renderTabContent() {
        if (!apiData || !dom.panel) return;
        var extra = clienteExtra;
        var map = {
            resumo: function () {
                return renderResumo(apiData, extra);
            },
            historico: renderHistorico,
            ciclo_racao: renderCiclo,
            cross_sell: renderCross,
            fiado: renderFiado,
            fidelidade: renderFidelidade,
            bonus: renderBonus,
            pets: function () {
                return renderPets(extra);
            },
            saude: function () {
                return renderSaude(extra);
            },
            anotacoes: function () {
                return renderAnotacoes(extra);
            },
            contato: renderContato,
        };
        var fn = map[activeTab] || renderResumo;
        if (activeTab === 'ciclo_racao' || activeTab === 'cross_sell') {
            ensureSecaoCarregada(activeTab);
        }
        dom.panel.innerHTML = fn(apiData);
        bindPanelActions(extra);
    }

    function carregarMaisHistorico() {
        if (!clientePk || historicoLoadingMore || !historicoHasMore) return;
        historicoLoadingMore = true;
        var btn = dom.panel && dom.panel.querySelector('#rel-historico-mais');
        if (btn) {
            btn.disabled = true;
            btn.textContent = 'Carregando…';
        }
        var params =
            'secao=historico&historico_offset=' +
            encodeURIComponent(String(historicoOffset)) +
            '&historico_limit=' +
            encodeURIComponent(String(HISTORICO_PAGE));
        fetchRelacionamentoJson(params)
            .then(function (data) {
                if (!data || !data.ok) throw new Error((data && data.erro) || 'Falha ao carregar');
                appendHistoricoVendas(data);
                syncHistoricoMetaFromApi();
                var list = dom.panel && dom.panel.querySelector('#rel-historico-vendas');
                if (list && data.historico_rapido && data.historico_rapido.vendas) {
                    data.historico_rapido.vendas.forEach(function (v) {
                        list.insertAdjacentHTML('beforeend', renderVendaHistoricoBlock(v));
                    });
                    list.querySelectorAll('.rel-add-gm').forEach(function (el) {
                        if (el.dataset && el.dataset.relBound) return;
                        el.dataset.relBound = '1';
                        el.addEventListener('click', function (e) {
                            e.preventDefault();
                            e.stopPropagation();
                            addProductToCartFromRel(el);
                        });
                    });
                }
                var oldWrap = dom.panel && dom.panel.querySelector('#rel-historico-mais-wrap');
                if (oldWrap) oldWrap.remove();
                if (historicoHasMore && dom.panel) {
                    var anchor = dom.panel.querySelector('#rel-historico-vendas');
                    if (anchor) {
                        anchor.insertAdjacentHTML(
                            'afterend',
                            '<div id="rel-historico-mais-wrap" class="pt-2 text-center">' +
                                '<button type="button" id="rel-historico-mais" class="min-h-[44px] rounded-xl border-2 border-emerald-500 bg-emerald-50 px-6 py-2.5 text-sm font-black uppercase text-emerald-900 hover:bg-emerald-100">Carregar mais vendas</button></div>'
                        );
                        var nb = dom.panel.querySelector('#rel-historico-mais');
                        if (nb) nb.addEventListener('click', carregarMaisHistorico);
                    }
                }
            })
            .catch(function (err) {
                alert(err.message || 'Não foi possível carregar mais vendas.');
            })
            .finally(function () {
                historicoLoadingMore = false;
            });
    }

    function bindPanelActions(extra) {
        dom.panel.querySelectorAll('.rel-add-gm').forEach(function (btn) {
            btn.addEventListener('click', function (e) {
                e.preventDefault();
                e.stopPropagation();
                addProductToCartFromRel(btn);
            });
        });
        var histMais = dom.panel.querySelector('#rel-historico-mais');
        if (histMais) {
            histMais.addEventListener('click', carregarMaisHistorico);
        }
        dom.panel.querySelectorAll('.rel-resumo-wa').forEach(function (a) {
            a.addEventListener('click', function (e) {
                if (typeof window.agroAbrirUrlExterna === 'function') {
                    e.preventDefault();
                    window.agroAbrirUrlExterna(a.getAttribute('href') || '');
                }
            });
        });
        var petAdd = dom.panel.querySelector('#rel-pet-add');
        if (petAdd) {
            petAdd.addEventListener('click', function () {
                var nome = (dom.panel.querySelector('#rel-pet-nome') || {}).value || '';
                if (!nome.trim()) return;
                var next = {
                    pets: (extra.pets || []).slice(),
                    lembretes: (extra.lembretes || []).slice(),
                    anotacoes: extra.anotacoes || '',
                };
                next.pets.push({
                    nome: nome.trim(),
                    raca: ((dom.panel.querySelector('#rel-pet-raca') || {}).value || '').trim(),
                    porte: ((dom.panel.querySelector('#rel-pet-porte') || {}).value || '').trim(),
                    idade: ((dom.panel.querySelector('#rel-pet-idade') || {}).value || '').trim(),
                });
                petAdd.disabled = true;
                persistExtra(next, function () {
                    renderTabContent();
                }).finally(function () {
                    petAdd.disabled = false;
                });
            });
        }
        dom.panel.querySelectorAll('.rel-pet-del').forEach(function (btn) {
            btn.addEventListener('click', function () {
                var i = parseInt(btn.getAttribute('data-i'), 10);
                var next = {
                    pets: (extra.pets || []).slice(),
                    lembretes: (extra.lembretes || []).slice(),
                    anotacoes: extra.anotacoes || '',
                };
                next.pets.splice(i, 1);
                btn.disabled = true;
                persistExtra(next, function () {
                    renderTabContent();
                }).finally(function () {
                    btn.disabled = false;
                });
            });
        });
        var saudeAdd = dom.panel.querySelector('#rel-saude-add');
        if (saudeAdd) {
            saudeAdd.addEventListener('click', function () {
                var data = (dom.panel.querySelector('#rel-saude-data') || {}).value || '';
                if (!data) return;
                var next = {
                    pets: (extra.pets || []).slice(),
                    lembretes: (extra.lembretes || []).slice(),
                    anotacoes: extra.anotacoes || '',
                };
                next.lembretes.push({
                    tipo: (dom.panel.querySelector('#rel-saude-tipo') || {}).value || 'Outro',
                    produto: ((dom.panel.querySelector('#rel-saude-prod') || {}).value || '').trim(),
                    data: data,
                });
                saudeAdd.disabled = true;
                persistExtra(next, function () {
                    renderTabContent();
                }).finally(function () {
                    saudeAdd.disabled = false;
                });
            });
        }
        dom.panel.querySelectorAll('.rel-saude-del').forEach(function (btn) {
            btn.addEventListener('click', function () {
                var i = parseInt(btn.getAttribute('data-i'), 10);
                var next = {
                    pets: (extra.pets || []).slice(),
                    lembretes: (extra.lembretes || []).slice(),
                    anotacoes: extra.anotacoes || '',
                };
                next.lembretes.splice(i, 1);
                btn.disabled = true;
                persistExtra(next, function () {
                    renderTabContent();
                }).finally(function () {
                    btn.disabled = false;
                });
            });
        });
        var anSave = dom.panel.querySelector('#rel-anotacoes-save');
        if (anSave) {
            anSave.addEventListener('click', function () {
                var next = {
                    pets: (extra.pets || []).slice(),
                    lembretes: (extra.lembretes || []).slice(),
                    anotacoes: (dom.panel.querySelector('#rel-anotacoes-ta') || {}).value || '',
                };
                anSave.disabled = true;
                persistExtra(next, function () {
                    anSave.textContent = 'Salvo ✓';
                    setTimeout(function () {
                        anSave.textContent = 'Salvar anotação';
                    }, 1200);
                }).finally(function () {
                    anSave.disabled = false;
                });
            });
        }
    }

    function renderTabs() {
        if (!dom.tabs) return;
        dom.tabs.innerHTML = TABS.map(function (t) {
            var on = t.id === activeTab;
            var fmeta = t.id === 'fiado' ? fiadoTabAlertMeta(apiData) : null;
            var rmeta = t.id === 'resumo' ? resumoTabAlertMeta(apiData) : null;
            var cmeta = t.id === 'ciclo_racao' ? cicloTabAlertMeta(apiData) : null;
            var alertFiado = !!fmeta;
            var alertOrange = !!(rmeta || cmeta);
            var cls = 'rel-tab rounded-lg border-2 px-0.5 py-1 font-black uppercase leading-tight ';
            if (on) {
                if (alertFiado) {
                    cls += fmeta.vencido
                        ? 'rel-tab--fiado-alerta rel-tab--fiado-alerta-on rel-tab--fiado-vencido border-red-600 bg-red-600 text-white shadow-md'
                        : 'rel-tab--fiado-alerta rel-tab--fiado-alerta-on border-orange-600 bg-orange-600 text-white shadow-md';
                } else if (alertOrange) {
                    cls += 'rel-tab--fiado-alerta rel-tab--fiado-alerta-on border-orange-600 bg-orange-600 text-white shadow-md';
                } else {
                    cls += 'border-emerald-600 bg-emerald-600 text-white shadow-md';
                }
            } else if (alertFiado) {
                cls += fmeta.vencido
                    ? 'rel-tab--fiado-alerta rel-tab--fiado-vencido border-red-500 bg-red-50 text-red-950 hover:bg-red-100'
                    : 'rel-tab--fiado-alerta border-orange-400 bg-orange-50 text-orange-950 hover:bg-orange-100';
            } else if (alertOrange) {
                cls += 'rel-tab--fiado-alerta border-orange-400 bg-orange-50 text-orange-950 hover:bg-orange-100';
            } else {
                cls += 'border-slate-300 bg-white text-slate-800 hover:bg-slate-50';
            }
            var tabTitle = t.label;
            var extra = '';
            if (alertFiado) {
                tabTitle = fmeta.vencido ? 'Fiado · vencido' : 'Fiado · em aberto';
                extra = '<span class="rel-tab-extra tabular-nums">' + esc(moneyCompact(fmeta.total)) + '</span>';
            } else if (rmeta) {
                tabTitle = rmeta.title || tabTitle;
                extra = '<span class="rel-tab-extra tabular-nums">' + esc(rmeta.extra) + '</span>';
            } else if (cmeta) {
                tabTitle = cmeta.title || tabTitle;
                extra = '<span class="rel-tab-extra tabular-nums">' + esc(cmeta.extra) + '</span>';
            }
            return (
                '<button type="button" class="' +
                cls +
                '" data-tab="' +
                esc(t.id) +
                '" title="' +
                esc(tabTitle) +
                '">' +
                '<span class="rel-tab-label">' +
                esc(tabDisplayLabel(t)) +
                '</span>' +
                extra +
                '</button>'
            );
        }).join('');
        dom.tabs.querySelectorAll('.rel-tab').forEach(function (btn) {
            btn.addEventListener('click', function () {
                activeTab = btn.getAttribute('data-tab') || 'resumo';
                renderTabs();
                renderTabContent();
                if (activeTab === 'ciclo_racao' || activeTab === 'cross_sell') {
                    ensureSecaoCarregada(activeTab);
                }
            });
        });
    }

    function setLoading(on) {
        if (dom.loading) dom.loading.classList.toggle('hidden', !on);
        if (dom.panel) dom.panel.classList.toggle('hidden', on);
    }

    function fetchData(pk) {
        setLoading(true);
        historicoOffset = 0;
        historicoHasMore = false;
        historicoLoadingMore = false;
        secaoCarregada = { ciclo_racao: false, cross_sell: false };
        secaoLoading = { ciclo_racao: false, cross_sell: false };
        var params = 'historico_limit=' + encodeURIComponent(String(HISTORICO_PAGE));
        return fetchRelacionamentoJson(params)
            .then(function (data) {
                if (!data || !data.ok) throw new Error((data && data.erro) || 'Falha ao carregar');
                apiData = data;
                clienteExtra = extraFromApi(data);
                return tryMigrateLocalExtras(pk, clienteExtra);
            })
            .then(function (extras) {
                clienteExtra = extras;
                if (apiData) apiData.extras = extras;
                syncHistoricoMetaFromApi();
                setLoading(false);
                renderTabs();
                renderTabContent();
                prefetchCicloParaResumo();
            })
            .catch(function (err) {
                setLoading(false);
                if (dom.panel) {
                    dom.panel.classList.remove('hidden');
                    dom.panel.innerHTML =
                        '<p class="text-sm font-bold text-red-700">' + esc(err.message || 'Erro') + '</p>';
                }
            });
    }

    function openModal() {
        var cli = getClienteFromPdv();
        if (!cli || !cli.cliente_agro_pk) {
            alert('Selecione um cliente cadastrado (não consumidor final).');
            return;
        }
        clientePk = cli.cliente_agro_pk;
        activeTab = 'resumo';
        relCartAdded = {};
        historicoOffset = 0;
        historicoHasMore = false;
        historicoLoadingMore = false;
        secaoCarregada = { ciclo_racao: false, cross_sell: false };
        secaoLoading = { ciclo_racao: false, cross_sell: false };
        clienteExtra = defaultExtra();
        if (dom.title) dom.title.textContent = cli.nome || 'Cliente';
        if (dom.modal) {
            dom.modal.classList.remove('hidden');
            dom.modal.classList.add('flex');
        }
        renderTabs();
        fetchData(clientePk);
    }

    function closeModal() {
        if (dom.modal) {
            dom.modal.classList.add('hidden');
            dom.modal.classList.remove('flex');
        }
    }

    function init() {
        try {
            var el = document.getElementById('agro-pdv-wizard-bootstrap');
            bootstrap = el ? JSON.parse(el.textContent || '{}') : {};
        } catch (e) {
            bootstrap = {};
        }
        dom.modal = document.getElementById('pdv-relacionamento-modal');
        dom.title = document.getElementById('pdv-relacionamento-titulo');
        dom.tabs = document.getElementById('pdv-relacionamento-tabs');
        dom.panel = document.getElementById('pdv-relacionamento-panel');
        dom.loading = document.getElementById('pdv-relacionamento-loading');
        dom.fechar = document.getElementById('pdv-relacionamento-fechar');
        if (dom.fechar) dom.fechar.addEventListener('click', closeModal);
        if (dom.modal) {
            dom.modal.addEventListener('click', function (e) {
                /* Fundo nao fecha — so X / FECHAR / Esc */
            });
        }
        document.addEventListener('keydown', function (e) {
            if (e.key === 'Escape' && dom.modal && !dom.modal.classList.contains('hidden')) {
                e.preventDefault();
                closeModal();
            }
        });
    }

    window.AgroPdvRelacionamento = {
        open: openModal,
        close: closeModal,
        init: init,
    };

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
