/**
 * Home administrativa: lembretes e orçamentos salvos em pop-up (sem navegar para /consulta/).
 * Usa os mesmos localStorage que o PDV (gmLembretesCaixa, historicoOrcamentos).
 */
(function () {
    'use strict';

    var BOOT = {};
    try {
        var el = document.getElementById('home-pdv-bootstrap');
        if (el && el.textContent) BOOT = JSON.parse(el.textContent);
    } catch (e) {}
    var URLS = BOOT.urls || {};
    var CSRF = BOOT.csrfToken || '';

    var CLIENTE_PADRAO_PDV = 'CONSUMIDOR NÃO IDENTIFICADO...';

    function escapeHtml(texto) {
        return String(texto || '')
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#039;');
    }

    function ehClienteGenericoPdv(str) {
        var s = String(str || '').trim();
        if (!s) return true;
        if (s === CLIENTE_PADRAO_PDV) return true;
        if (/^consumidor\s+final$/i.test(s)) return true;
        return false;
    }

    function nomeClienteOrcBody(h) {
        if (!h) return CLIENTE_PADRAO_PDV;
        var c = String(h.cliente || '').trim();
        return ehClienteGenericoPdv(c) ? CLIENTE_PADRAO_PDV : c;
    }

    function obterLembretes() {
        try {
            var dados = JSON.parse(localStorage.getItem('gmLembretesCaixa') || '[]');
            return Array.isArray(dados) ? dados : [];
        } catch (e) {
            return [];
        }
    }

    function salvarListaLembretes(lista) {
        localStorage.setItem('gmLembretesCaixa', JSON.stringify(lista));
    }

    function renderizarLembretes() {
        var listaLembretes = document.getElementById('lista-lembretes');
        if (!listaLembretes) return;
        var lembretes = obterLembretes().sort(function (a, b) {
            return String(a.hora || '').localeCompare(String(b.hora || ''));
        });
        listaLembretes.innerHTML = '';
        if (!lembretes.length) {
            listaLembretes.innerHTML =
                '<div class="px-3 py-4 text-center text-[11px] font-bold text-slate-400">Nenhum lembrete cadastrado.</div>';
            return;
        }
        lembretes.forEach(function (lembrete) {
            var row = document.createElement('div');
            var due = !!lembrete.disparado && !lembrete.concluido;
            row.className =
                'px-3 py-2 ' +
                (due ? 'reminder-due ' : '') +
                (lembrete.concluido ? 'done-reminder' : '');
            row.innerHTML =
                '<div class="flex items-start justify-between gap-2">' +
                '<div class="flex items-start gap-2 min-w-0">' +
                '<label class="mt-0.5 flex items-center gap-2 cursor-pointer">' +
                '<input type="checkbox" ' +
                (lembrete.concluido ? 'checked' : '') +
                ' class="w-4 h-4 accent-sky-600" data-lid="' +
                escapeHtml(lembrete.id) +
                '">' +
                '</label>' +
                '<div class="min-w-0">' +
                '<div class="done-text text-[11px] font-black text-slate-700 uppercase truncate">' +
                escapeHtml(lembrete.texto) +
                '</div>' +
                '<div class="text-[10px] font-bold text-slate-400">' +
                escapeHtml(lembrete.hora || '--:--') +
                (lembrete.concluido ? ' • feito' : '') +
                '</div></div></div>' +
                '<button type="button" class="home-btn-remover-lembrete text-[10px] font-black uppercase text-red-500 hover:text-red-600" data-lid="' +
                escapeHtml(lembrete.id) +
                '">Remover</button></div>';
            var chk = row.querySelector('input[type="checkbox"]');
            if (chk) {
                chk.addEventListener('change', function () {
                    alternarLembreteFeito(lembrete.id, chk.checked);
                });
            }
            var btnR = row.querySelector('.home-btn-remover-lembrete');
            if (btnR) {
                btnR.addEventListener('click', function () {
                    removerLembrete(lembrete.id);
                });
            }
            listaLembretes.appendChild(row);
        });
    }

    function abrirModalLembretes() {
        var mh0 = document.getElementById('modal-historico-vendas');
        if (mh0 && !mh0.classList.contains('hidden')) {
            mh0.classList.add('hidden');
            mh0.classList.remove('flex');
        }
        var m = document.getElementById('modal-lembretes');
        if (!m) return;
        renderizarLembretes();
        m.classList.remove('hidden');
        m.classList.add('flex');
        document.body.classList.add('modal-open');
        var inp = document.getElementById('lembrete-texto');
        if (inp) setTimeout(function () { inp.focus(); }, 50);
    }

    function fecharModalLembretes() {
        var m = document.getElementById('modal-lembretes');
        if (!m) return;
        m.classList.add('hidden');
        m.classList.remove('flex');
        if (!document.getElementById('modal-historico-vendas') || document.getElementById('modal-historico-vendas').classList.contains('hidden')) {
            document.body.classList.remove('modal-open');
        }
    }

    function salvarLembrete() {
        var textoInput = document.getElementById('lembrete-texto');
        var horaInput = document.getElementById('lembrete-hora');
        var texto = (textoInput && textoInput.value ? textoInput.value : '').trim();
        var hora = horaInput && horaInput.value ? horaInput.value : '';
        if (!texto || !hora) {
            alert('Preencha o lembrete e o horário.');
            return;
        }
        var lista = obterLembretes();
        lista.push({
            id: String(Date.now()),
            texto: texto,
            hora: hora,
            disparado: false,
            data: new Date().toISOString().slice(0, 10),
        });
        salvarListaLembretes(lista);
        if (textoInput) textoInput.value = '';
        if (horaInput) horaInput.value = '';
        renderizarLembretes();
    }

    function removerLembrete(id) {
        var lista = obterLembretes().filter(function (item) {
            return item.id !== id;
        });
        salvarListaLembretes(lista);
        renderizarLembretes();
    }

    function alternarLembreteFeito(id, feito) {
        var lista = obterLembretes();
        var item = lista.find(function (x) {
            return x.id === id;
        });
        if (!item) return;
        item.concluido = !!feito;
        if (feito) item.disparado = false;
        salvarListaLembretes(lista);
        renderizarLembretes();
    }

    function abrirHistoricoLocal() {
        var ml0 = document.getElementById('modal-lembretes');
        if (ml0 && !ml0.classList.contains('hidden')) {
            ml0.classList.add('hidden');
            ml0.classList.remove('flex');
        }
        var container = document.getElementById('lista-historico-local');
        if (!container) return;
        var historico = [];
        try {
            historico = JSON.parse(localStorage.getItem('historicoOrcamentos') || '[]');
        } catch (e) {
            historico = [];
        }
        container.innerHTML = '';
        if (!historico.length) {
            container.innerHTML =
                '<div class="text-center text-slate-400 py-10 font-bold text-sm">Nenhum orçamento salvo neste navegador.</div>';
        } else {
            historico.forEach(function (h) {
                var hid = Number(h.id);
                var u = String((h.usuario != null && h.usuario !== '') ? h.usuario : (h.operador || '')).trim();
                var userLinha = u
                    ? '<span class="inline-flex items-center rounded-md bg-slate-200/70 px-1.5 py-0.5 text-[7px] font-black uppercase text-slate-600 ring-1 ring-slate-300/60" title="Usuário que salvou o orçamento">👤 ' +
                      escapeHtml(u) +
                      '</span>'
                    : '<span class="text-[9px] font-bold text-slate-400">— usuário</span>';
                container.innerHTML +=
                    '<div class="bg-slate-50 border border-slate-200 p-3 sm:p-4 rounded-2xl hover:bg-slate-100/90 transition-colors">' +
                    '<div class="flex flex-wrap items-start justify-between gap-2 gap-y-1">' +
                    '<div class="min-w-0 flex-1">' +
                    '<div class="font-black text-slate-800 text-sm uppercase leading-snug">' +
                    escapeHtml(h.cliente) +
                    (h.entrega ? ' <span class="text-sky-600 font-black">· Entrega</span>' : '') +
                    '</div>' +
                    '<div class="mt-1 flex flex-wrap items-center gap-x-2 gap-y-1 text-[10px] text-slate-500 font-bold">' +
                    '<span>' +
                    escapeHtml(h.data) +
                    '</span><span class="text-slate-300">·</span><span>' +
                    (Number(h.itens && h.itens.length) || 0) +
                    ' itens</span>' +
                    (h.forma_pagamento
                        ? '<span class="text-slate-300">·</span><span>' + escapeHtml(h.forma_pagamento) + '</span>'
                        : '') +
                    '</div>' +
                    '<div class="mt-1.5 flex flex-wrap items-center gap-2">' +
                    userLinha +
                    '</div></div>' +
                    '<div class="text-right shrink-0">' +
                    '<div class="font-black text-emerald-600 text-lg tabular-nums">' +
                    escapeHtml(h.total) +
                    '</div>' +
                    (h.orc_barcode
                        ? '<div class="text-[9px] font-mono font-bold text-slate-400 mt-0.5">' +
                          escapeHtml(String(h.orc_barcode)) +
                          '</div>'
                        : '') +
                    '</div></div>' +
                    '<div class="mt-3 flex flex-wrap gap-2">' +
                    '<button type="button" class="home-orc-btn flex-1 min-w-[8rem] py-2.5 rounded-xl border-2 border-sky-400 bg-sky-500 hover:bg-sky-600 text-white text-[10px] font-black uppercase shadow-sm active:scale-[0.98]" data-orc-id="' +
                    hid +
                    '" data-action="consulta">Abrir como orçamento</button>' +
                    '<button type="button" class="home-orc-btn flex-1 min-w-[8rem] py-2.5 rounded-xl border-2 border-emerald-500 bg-emerald-600 hover:bg-emerald-700 text-white text-[10px] font-black uppercase shadow-sm active:scale-[0.98]" data-orc-id="' +
                    hid +
                    '" data-action="pdv">Abrir como venda (PDV)</button>' +
                    '<button type="button" class="home-orc-btn flex-1 min-w-[6rem] py-2.5 rounded-xl border-2 border-red-200 bg-white text-red-700 hover:bg-red-50 text-[10px] font-black uppercase tracking-wide" data-orc-id="' +
                    hid +
                    '" data-action="del">Excluir</button>' +
                    '</div></div>';
            });
            container.querySelectorAll('.home-orc-btn').forEach(function (btn) {
                btn.addEventListener('click', function () {
                    var id = Number(btn.getAttribute('data-orc-id'));
                    var act = btn.getAttribute('data-action');
                    if (act === 'consulta') recuperarOrcamentoIrConsulta(id);
                    else if (act === 'pdv') abrirOrcamentoComoVendaPdv(id);
                    else if (act === 'del') excluirOrcamentoHistorico(id);
                });
            });
        }
        var mh = document.getElementById('modal-historico-vendas');
        if (mh) {
            mh.classList.remove('hidden');
            mh.classList.add('flex');
        }
        document.body.classList.add('modal-open');
    }

    function fecharHistoricoLocal() {
        var mh = document.getElementById('modal-historico-vendas');
        if (mh) {
            mh.classList.add('hidden');
            mh.classList.remove('flex');
        }
        var ml = document.getElementById('modal-lembretes');
        if (!ml || ml.classList.contains('hidden')) {
            document.body.classList.remove('modal-open');
        }
    }

    function excluirOrcamentoHistorico(id) {
        if (!confirm('Excluir este orçamento salvo neste navegador?')) return;
        var historico = [];
        try {
            historico = JSON.parse(localStorage.getItem('historicoOrcamentos') || '[]');
        } catch (e) {
            return;
        }
        historico = historico.filter(function (x) {
            return Number(x.id) !== Number(id);
        });
        try {
            localStorage.setItem('historicoOrcamentos', JSON.stringify(historico));
        } catch (e) {}
        abrirHistoricoLocal();
    }

    function recuperarOrcamentoIrConsulta(id) {
        try {
            sessionStorage.setItem('agro_pdv_aplicar_orcamento_id', String(id));
        } catch (e) {}
        var url = URLS.consultaPdv || '/consulta/';
        window.location.href = url;
    }

    async function abrirOrcamentoComoVendaPdv(id) {
        var historico = [];
        try {
            historico = JSON.parse(localStorage.getItem('historicoOrcamentos') || '[]');
        } catch (e) {
            return;
        }
        var h = historico.find(function (x) {
            return Number(x.id) === Number(id);
        });
        if (!h) {
            alert('Orçamento não encontrado.');
            return;
        }
        var draftUrl = URLS.apiPdvSalvarCheckoutDraft;
        if (!draftUrl) {
            alert('URL do rascunho PDV indisponível.');
            return;
        }
        var body = {
            itens: h.itens,
            cliente: nomeClienteOrcBody(h),
            cliente_extra:
                h.cliente_extra && typeof h.cliente_extra === 'object' ? h.cliente_extra : null,
            forma_pagamento: h.forma_pagamento || '',
        };
        try {
            var res = await fetch(draftUrl, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-CSRFToken': CSRF,
                },
                body: JSON.stringify(body),
                credentials: 'same-origin',
            });
            var j = await res.json();
            if (j.ok) {
                var base = (URLS.pdvWizardHome || '/pdv/').replace(/\/?$/, '/');
                window.location.href = base + '?reabrir=1';
            } else {
                alert(j.erro || 'Não foi possível abrir o fechamento.');
            }
        } catch (err) {
            alert('Erro de rede ao salvar o rascunho da venda.');
        }
    }

    function wireHomeStripButtons() {
        var lb = document.getElementById('home-link-lembretes');
        var ob = document.getElementById('home-link-orcamentos');
        if (lb) {
            lb.addEventListener('click', function (e) {
                e.preventDefault();
                abrirModalLembretes();
            });
        }
        if (ob) {
            ob.addEventListener('click', function (e) {
                e.preventDefault();
                abrirHistoricoLocal();
            });
        }
    }

    document.addEventListener('keydown', function (e) {
        if (e.key !== 'Escape') return;
        var ml = document.getElementById('modal-lembretes');
        var mh = document.getElementById('modal-historico-vendas');
        if (ml && !ml.classList.contains('hidden')) {
            e.preventDefault();
            fecharModalLembretes();
            return;
        }
        if (mh && !mh.classList.contains('hidden')) {
            e.preventDefault();
            fecharHistoricoLocal();
        }
    });

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', wireHomeStripButtons);
    } else {
        wireHomeStripButtons();
    }

    window.fecharModalLembretes = fecharModalLembretes;
    window.salvarLembrete = salvarLembrete;
    window.alternarLembreteFeito = alternarLembreteFeito;
    window.removerLembrete = removerLembrete;
    window.fecharHistoricoLocal = fecharHistoricoLocal;
    window.abrirHistoricoLocal = abrirHistoricoLocal;
    window.excluirOrcamentoHistorico = excluirOrcamentoHistorico;
    window.abrirOrcamentoComoVendaPdv = abrirOrcamentoComoVendaPdv;
})();
