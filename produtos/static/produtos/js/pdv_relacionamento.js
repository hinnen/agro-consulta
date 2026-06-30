/**
 * PDV — Relacionamento com cliente (rascunho · atalho F8).
 * Dados reais via API; pets/lembretes/anotações em localStorage até validar escopo.
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
        { id: 'metricas', label: 'Métricas' },
        { id: 'pets', label: 'Pets' },
        { id: 'saude', label: 'Saúde' },
        { id: 'anotacoes', label: 'Anotações', labelTab: 'Anot.' },
        { id: 'contato', label: 'Contato' },
    ];

    var dom = {};
    var bootstrap = {};
    var apiData = null;
    var clientePk = null;
    var activeTab = 'resumo';
    var relCartAdded = {};

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

    function loadLocalExtra(pk) {
        try {
            var raw = localStorage.getItem(STORAGE_PREFIX + pk);
            return raw ? JSON.parse(raw) : { pets: [], lembretes: [], anotacoes: '' };
        } catch (e) {
            return { pets: [], lembretes: [], anotacoes: '' };
        }
    }

    function saveLocalExtra(pk, data) {
        try {
            localStorage.setItem(STORAGE_PREFIX + pk, JSON.stringify(data));
        } catch (e) {}
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

    function btnCartHtml(codigo, large, added) {
        if (!codigo) return '';
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

    function btnCartCol(codigo, large) {
        return (
            '<div class="rel-add-gm-col flex shrink-0 items-center border-l border-slate-200 pl-2 sm:pl-3">' +
            btnCart(codigo, large) +
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
            btnCart(p.codigo, largeBtn) +
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

    function btnCart(codigo, large) {
        var key = String(codigo || '').trim();
        return btnCartHtml(key, large, !!relCartAdded[key]);
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

    function renderResumoCards(d, extra) {
        var m = d.metricas || {};
        var fid = d.fidelidade || {};
        var c = d.cliente || {};
        var waUrl = (c.whatsapp_url || '').trim();
        var waBtn =
            waUrl
                ? '<a class="rel-resumo-wa inline-flex min-h-[2rem] w-full items-center justify-center rounded-lg border-2 border-emerald-500 bg-emerald-50 px-2 py-1 text-[11px] font-black uppercase text-emerald-900 no-underline hover:bg-emerald-100" href="' +
                  esc(waUrl) +
                  '" target="_blank" rel="noopener noreferrer"><span aria-hidden="true">💬</span> Conversar</a>'
                : '<span class="text-sm font-bold text-slate-400">Sem número</span>';
        return (
            '<div class="rel-resumo-cards">' +
            resumoMetricCard('Visitas', String(m.total_vendas || 0), 'rel-resumo-card-val--dark') +
            resumoMetricCard('Ticket médio', money(m.ticket_medio), 'rel-resumo-card-val--dark') +
            resumoMetricCard('Cashback', money(fid.cashback), 'rel-resumo-card-val--cash') +
            resumoMetricCard('Vale crédito', money(fid.vale_credito), 'rel-resumo-card-val--vale') +
            resumoMetricCard('Total comprado', money(m.total_comprado), 'rel-resumo-card-val--dark') +
            resumoMetricCard(
                'Freq. entre visitas',
                m.frequencia_media_dias != null ? m.frequencia_media_dias + ' dias' : '—',
                'rel-resumo-card-val--dark'
            ) +
            resumoMetricCard(
                'Última visita',
                m.ultima_visita_dias != null ? 'há ' + m.ultima_visita_dias + ' dias' : '—',
                'rel-resumo-card-val--dark'
            ) +
            resumoMetricCard('Pets', petsResumoNomes(extra), 'rel-resumo-card-val--pets') +
            resumoMetricCard('WhatsApp', waBtn, 'rel-resumo-card-val--wa') +
            '</div>'
        );
    }

    function renderResumo(d, extra) {
        var m = d.metricas || {};
        var alertas = [];
        if (m.ultima_visita_dias != null && m.ultima_visita_dias > 60) {
            alertas.push('Cliente sumido há ' + m.ultima_visita_dias + ' dias');
        }
        var ciclo = d.ciclo_racao || [];
        ciclo.forEach(function (c) {
            if (c.status === 'atrasado') alertas.push('Recompra atrasada: ' + esc(c.descricao).slice(0, 40));
        });
        var html =
            '<div class="space-y-3 text-sm">' +
            '<p class="rounded-xl border-2 border-amber-300 bg-amber-50 px-3 py-2 text-[11px] font-bold text-amber-950">Rascunho — teste ferramenta a ferramenta. Pets/saúde/anotações ficam neste navegador (localStorage).</p>';
        if (alertas.length) {
            html += '<div class="rounded-xl border-2 border-red-300 bg-red-50 px-3 py-2"><p class="text-[10px] font-black uppercase text-red-800">Alertas</p><ul class="mt-1 list-disc pl-4 text-xs font-bold text-red-950">';
            alertas.forEach(function (a) {
                html += '<li>' + a + '</li>';
            });
            html += '</ul></div>';
        }
        html += renderResumoCards(d, extra);
        var top = (d.historico_rapido && d.historico_rapido.top_produtos) || [];
        html += topProdutoListHtml(top, false);
        html += '</div>';
        return html;
    }

    function renderHistorico(d) {
        var vendas = (d.historico_rapido && d.historico_rapido.vendas) || [];
        var top = (d.historico_rapido && d.historico_rapido.top_produtos) || [];
        var m = d.metricas || {};
        var html =
            '<div class="pdv-rel-historico space-y-6">' +
            '<div class="grid gap-3 sm:grid-cols-3">' +
            '<div class="rounded-2xl border-2 border-emerald-200 bg-emerald-50 px-4 py-3"><p class="text-[11px] font-black uppercase text-emerald-800">Visitas PDV</p><p class="text-2xl font-black text-emerald-950">' +
            (m.total_vendas || 0) +
            '</p></div>' +
            '<div class="rounded-2xl border-2 border-sky-200 bg-sky-50 px-4 py-3"><p class="text-[11px] font-black uppercase text-sky-800">Total comprado</p><p class="text-2xl font-black text-sky-950">' +
            money(m.total_comprado) +
            '</p></div>' +
            '<div class="rounded-2xl border-2 border-violet-200 bg-violet-50 px-4 py-3"><p class="text-[11px] font-black uppercase text-violet-800">Ticket médio</p><p class="text-2xl font-black text-violet-950">' +
            money(m.ticket_medio) +
            '</p></div></div>';

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
                    btnCartCol(p.codigo, true) +
                    '</div></article>';
            });
            html += '</div>';
        }
        html += '</section>';

        html +=
            '<section><h3 class="mb-3 text-sm font-black uppercase tracking-wide text-slate-700">Últimas vendas</h3>';
        if (!vendas.length) {
            html += '<p class="rounded-xl border border-dashed border-slate-300 bg-slate-50 px-4 py-6 text-center text-base font-bold text-slate-500">Nenhuma venda recente no PDV.</p>';
        } else {
            html += '<div class="space-y-3">';
            vendas.forEach(function (v) {
                html +=
                    '<details class="group rounded-2xl border-2 border-slate-200 bg-white shadow-sm">' +
                    '<summary class="flex cursor-pointer list-none flex-wrap items-center gap-x-3 gap-y-2 px-4 py-3.5 sm:px-5 sm:py-4 [&::-webkit-details-marker]:hidden">' +
                    '<span class="text-lg font-black text-slate-900 sm:text-xl">' +
                    money(v.total) +
                    '</span>' +
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
                        btnCart(it.codigo, false) +
                        '</td></tr>';
                });
                html += '</tbody></table></div></details>';
            });
            html += '</div>';
        }
        html += '</section></div>';
        return html;
    }

    function renderCiclo(d) {
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
            '<div class="space-y-3 text-sm"><p class="text-xs text-slate-600">Cadastro rápido (teste) — salvo neste PC.</p>' +
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
            '<div class="space-y-3 text-sm"><p class="text-xs text-slate-600">Lembretes de vacina, carrapaticida, vermífugo (teste local).</p><div id="rel-saude-list" class="space-y-2">';
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
            '<div class="space-y-2 text-sm"><p class="text-xs text-slate-600">Preferências do balcão (salvo neste PC).</p>' +
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
        var extra = loadLocalExtra(clientePk);
        var map = {
            resumo: function () {
                return renderResumo(apiData, extra);
            },
            historico: renderHistorico,
            ciclo_racao: renderCiclo,
            cross_sell: renderCross,
            fiado: renderFiado,
            fidelidade: renderFidelidade,
            metricas: renderMetricas,
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
        dom.panel.innerHTML = fn(apiData);
        bindPanelActions(extra);
    }

    function bindPanelActions(extra) {
        dom.panel.querySelectorAll('.rel-add-gm').forEach(function (btn) {
            btn.addEventListener('click', function (e) {
                e.preventDefault();
                e.stopPropagation();
                addProductToCartFromRel(btn);
            });
        });
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
                extra.pets = extra.pets || [];
                extra.pets.push({
                    nome: nome.trim(),
                    raca: ((dom.panel.querySelector('#rel-pet-raca') || {}).value || '').trim(),
                    porte: ((dom.panel.querySelector('#rel-pet-porte') || {}).value || '').trim(),
                    idade: ((dom.panel.querySelector('#rel-pet-idade') || {}).value || '').trim(),
                });
                saveLocalExtra(clientePk, extra);
                renderTabContent();
            });
        }
        dom.panel.querySelectorAll('.rel-pet-del').forEach(function (btn) {
            btn.addEventListener('click', function () {
                var i = parseInt(btn.getAttribute('data-i'), 10);
                extra.pets.splice(i, 1);
                saveLocalExtra(clientePk, extra);
                renderTabContent();
            });
        });
        var saudeAdd = dom.panel.querySelector('#rel-saude-add');
        if (saudeAdd) {
            saudeAdd.addEventListener('click', function () {
                var data = (dom.panel.querySelector('#rel-saude-data') || {}).value || '';
                if (!data) return;
                extra.lembretes = extra.lembretes || [];
                var hoje = new Date().toISOString().slice(0, 10);
                extra.lembretes.push({
                    tipo: (dom.panel.querySelector('#rel-saude-tipo') || {}).value || 'Outro',
                    produto: ((dom.panel.querySelector('#rel-saude-prod') || {}).value || '').trim(),
                    data: data,
                    vencido: data < hoje,
                });
                saveLocalExtra(clientePk, extra);
                renderTabContent();
            });
        }
        dom.panel.querySelectorAll('.rel-saude-del').forEach(function (btn) {
            btn.addEventListener('click', function () {
                var i = parseInt(btn.getAttribute('data-i'), 10);
                extra.lembretes.splice(i, 1);
                saveLocalExtra(clientePk, extra);
                renderTabContent();
            });
        });
        var anSave = dom.panel.querySelector('#rel-anotacoes-save');
        if (anSave) {
            anSave.addEventListener('click', function () {
                extra.anotacoes = (dom.panel.querySelector('#rel-anotacoes-ta') || {}).value || '';
                saveLocalExtra(clientePk, extra);
                anSave.textContent = 'Salvo ✓';
                setTimeout(function () {
                    anSave.textContent = 'Salvar anotação';
                }, 1200);
            });
        }
    }

    function renderTabs() {
        if (!dom.tabs) return;
        var fmeta = fiadoTabAlertMeta(apiData);
        dom.tabs.innerHTML = TABS.map(function (t) {
            var on = t.id === activeTab;
            var alertFiado = t.id === 'fiado' && fmeta;
            var cls = 'rel-tab rounded-lg border-2 px-0.5 py-1 font-black uppercase leading-tight ';
            if (on) {
                cls += alertFiado
                    ? 'rel-tab--fiado-alerta rel-tab--fiado-alerta-on border-orange-600 bg-orange-600 text-white shadow-md'
                    : 'border-emerald-600 bg-emerald-600 text-white shadow-md';
            } else if (alertFiado) {
                cls += fmeta.vencido
                    ? 'rel-tab--fiado-alerta rel-tab--fiado-vencido border-red-500 bg-red-50 text-red-950 hover:bg-red-100'
                    : 'rel-tab--fiado-alerta border-orange-400 bg-orange-50 text-orange-950 hover:bg-orange-100';
            } else {
                cls += 'border-slate-300 bg-white text-slate-800 hover:bg-slate-50';
            }
            var extra =
                alertFiado
                    ? '<span class="rel-tab-extra tabular-nums">' + esc(moneyCompact(fmeta.total)) + '</span>'
                    : '';
            return (
                '<button type="button" class="' +
                cls +
                '" data-tab="' +
                esc(t.id) +
                '" title="' +
                esc(t.label) +
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
            });
        });
    }

    function setLoading(on) {
        if (dom.loading) dom.loading.classList.toggle('hidden', !on);
        if (dom.panel) dom.panel.classList.toggle('hidden', on);
    }

    function fetchData(pk) {
        var url = (bootstrap.urls && bootstrap.urls.apiPdvRelacionamentoCliente) || '/api/pdv/relacionamento-cliente/';
        setLoading(true);
        return fetch(url + '?cliente_agro_pk=' + encodeURIComponent(String(pk)), {
            credentials: 'same-origin',
            headers: { Accept: 'application/json' },
        })
            .then(function (r) {
                return r.json();
            })
            .then(function (data) {
                if (!data || !data.ok) throw new Error((data && data.erro) || 'Falha ao carregar');
                apiData = data;
                setLoading(false);
                renderTabs();
                renderTabContent();
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
                if (e.target === dom.modal) closeModal();
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
