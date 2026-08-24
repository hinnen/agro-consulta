(function (w) {
    'use strict';

    function boot() {
        return w.AGRO_CLI_BOOT || {};
    }

    function urls() {
        return boot().urls || {};
    }

    function csrfToken() {
        var m = document.cookie.match(/csrftoken=([^;]+)/);
        if (m) return decodeURIComponent(m[1]);
        return String(boot().csrf || '');
    }

    function patternUrl(pattern, pk) {
        return String(pattern || '').replace('__pk__', String(pk));
    }

    function overlay() {
        return document.getElementById('agro-cli-acao-overlay');
    }

    function el(id) {
        return document.getElementById(id);
    }

    function money(n) {
        var v = Number(n);
        if (!isFinite(v)) v = 0;
        return v.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
    }

    function jsonPost(url, payload) {
        return fetch(url, {
            method: 'POST',
            credentials: 'same-origin',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': csrfToken(),
                Accept: 'application/json'
            },
            body: JSON.stringify(payload || {})
        }).then(function (r) {
            return r.json().then(function (data) {
                return { ok: r.ok, status: r.status, data: data || {} };
            });
        });
    }

    function jsonGet(url) {
        return fetch(url, {
            method: 'GET',
            credentials: 'same-origin',
            headers: { Accept: 'application/json' }
        }).then(function (r) {
            return r.json().then(function (data) {
                return { ok: r.ok, status: r.status, data: data || {} };
            });
        });
    }

    function closeOverlay() {
        var root = overlay();
        if (!root) return;
        root.classList.add('hidden');
        root.classList.remove('flex');
        var extra = el('agro-cli-acao-extra');
        if (extra) extra.innerHTML = '';
        var err = el('agro-cli-acao-erro');
        if (err) {
            err.textContent = '';
            err.classList.add('hidden');
        }
    }

    function showErro(msg) {
        var err = el('agro-cli-acao-erro');
        if (!err) return;
        err.textContent = String(msg || '');
        err.classList.toggle('hidden', !msg);
    }

    function btnHtml(label, kind, attr) {
        var cls =
            kind === 'danger'
                ? 'agro-cli-acao-btn flex-1 rounded-xl border-2 border-red-600 bg-red-600 px-3 text-white hover:bg-red-700'
                : kind === 'primary'
                  ? 'agro-cli-acao-btn flex-1 rounded-xl border-2 border-emerald-700 bg-emerald-600 px-3 text-white hover:bg-emerald-700'
                  : 'agro-cli-acao-btn flex-1 rounded-xl border-2 border-slate-300 bg-white px-3 text-slate-800 hover:bg-slate-100';
        return (
            '<button type="button" data-cli-act="' +
            attr +
            '" class="' +
            cls +
            '">' +
            label +
            '</button>'
        );
    }

    function openPanel(title, body, extraHtml, buttonsHtml) {
        var root = overlay();
        if (!root) return;
        el('agro-cli-acao-title').textContent = title || 'Atenção';
        el('agro-cli-acao-body').textContent = body || '';
        el('agro-cli-acao-extra').innerHTML = extraHtml || '';
        el('agro-cli-acao-btns').innerHTML = buttonsHtml || '';
        showErro('');
        root.classList.remove('hidden');
        root.classList.add('flex');
        var pin = root.querySelector('#agro-cli-acao-pin');
        if (pin) setTimeout(function () { pin.focus(); }, 40);
    }

    function pinField() {
        return (
            '<label class="block">' +
            '<span class="mb-1 block text-[0.75rem] font-black uppercase tracking-wide text-slate-500">PIN do operador</span>' +
            '<input id="agro-cli-acao-pin" type="password" inputmode="numeric" autocomplete="new-password" class="agro-cli-acao-input w-full rounded-xl border-2 border-slate-300 px-3">' +
            '</label>'
        );
    }

    function pinVal() {
        var inp = el('agro-cli-acao-pin');
        return inp ? String(inp.value || '').trim() : '';
    }

    function origemTela() {
        return String(boot().origemTela || 'pdv');
    }

    var onAbrirCadastro = null;
    var onAposMudanca = null;
    var onIniciarValePago = null;

    function showDuplicado(dup, opts) {
        opts = opts || {};
        dup = dup || {};
        var nome = String(dup.nome || 'outro cliente');
        var pk = dup.pk;
        var msg =
            'Este telefone já está no cadastro de «' +
            nome +
            '».\n\nVocê pode abrir esse cadastro para conferir, ou limpar o número dali (o outro fica sem telefone) para salvar este.';
        openPanel(
            'Telefone já cadastrado',
            msg,
            pinField(),
            btnHtml('Cancelar', 'ghost', 'close') +
                (pk ? btnHtml('Abrir cadastro de ' + nome, 'ghost', 'abrir') : '') +
                btnHtml('Limpar telefone do outro', 'danger', 'limpar')
        );
        var root = overlay();
        root.querySelectorAll('[data-cli-act]').forEach(function (b) {
            b.addEventListener('click', function () {
                var act = b.getAttribute('data-cli-act');
                if (act === 'close') {
                    closeOverlay();
                    return;
                }
                if (act === 'abrir') {
                    closeOverlay();
                    if (typeof onAbrirCadastro === 'function') onAbrirCadastro(dup);
                    return;
                }
                if (act === 'limpar') {
                    var pin = pinVal();
                    if (!pin) {
                        showErro('Digite o PIN.');
                        return;
                    }
                    b.disabled = true;
                    jsonPost(patternUrl(urls().apiClienteLimparWhatsappPattern, pk), {
                        pin: pin,
                        origem_tela: origemTela()
                    })
                        .then(function (res) {
                            if (!res.data || !res.data.ok) {
                                showErro((res.data && res.data.erro) || 'Não foi possível limpar o telefone.');
                                b.disabled = false;
                                return;
                            }
                            closeOverlay();
                            if (typeof opts.onLimpo === 'function') opts.onLimpo(res.data);
                            else if (typeof onAposMudanca === 'function') onAposMudanca({ tipo: 'limpar', data: res.data });
                        })
                        .catch(function () {
                            showErro('Erro de rede.');
                            b.disabled = false;
                        });
                }
            });
        });
    }

    function buscarDestinoBind(pkAtual) {
        var inp = el('agro-cli-acao-destino');
        var box = el('agro-cli-acao-destino-list');
        if (!inp || !box) return;
        var timer = null;
        inp.addEventListener('input', function () {
            clearTimeout(timer);
            timer = setTimeout(function () {
                var q = String(inp.value || '').trim();
                box.innerHTML = '';
                box.dataset.pk = '';
                if (q.length < 2) return;
                var u = urls().apiBuscarClientes || '/api/buscar-clientes/';
                jsonGet(u + (u.indexOf('?') >= 0 ? '&' : '?') + 'q=' + encodeURIComponent(q)).then(function (res) {
                    var lista = (res.data && (res.data.clientes || res.data.results || res.data)) || [];
                    if (!Array.isArray(lista)) lista = [];
                    box.innerHTML = lista
                        .slice(0, 8)
                        .map(function (c) {
                            var pk = c.cliente_agro_pk || c.pk || c.id;
                            if (String(pk) === String(pkAtual)) return '';
                            return (
                                '<button type="button" class="block w-full rounded-lg px-3 py-2 text-left text-sm font-bold hover:bg-emerald-50" data-dest-pk="' +
                                pk +
                                '" data-dest-nome="' +
                                String(c.nome || '').replace(/"/g, '') +
                                '">' +
                                String(c.nome || '') +
                                (c.telefone ? ' · ' + c.telefone : '') +
                                '</button>'
                            );
                        })
                        .join('');
                    box.querySelectorAll('[data-dest-pk]').forEach(function (row) {
                        row.addEventListener('click', function () {
                            box.dataset.pk = row.getAttribute('data-dest-pk') || '';
                            inp.value = row.getAttribute('data-dest-nome') || '';
                            box.innerHTML =
                                '<p class="text-sm font-black text-emerald-800">Destino: ' +
                                inp.value +
                                '</p>';
                        });
                    });
                });
            }, 250);
        });
    }

    function destinoPk() {
        var box = el('agro-cli-acao-destino-list');
        return box && box.dataset.pk ? Number(box.dataset.pk) : null;
    }

    function openExcluir(pk) {
        var u = patternUrl(urls().apiClienteExclusaoPreviewPattern, pk);
        jsonGet(u).then(function (res) {
            if (!res.data || !res.data.ok) {
                showDuplicado({ nome: 'Erro' });
                openPanel('Excluir cadastro', (res.data && res.data.erro) || 'Não foi possível conferir o cadastro.', '', btnHtml('Fechar', 'ghost', 'close'));
                overlay().querySelector('[data-cli-act="close"]').addEventListener('click', closeOverlay);
                return;
            }
            var p = res.data;
            var cli = p.cliente || {};
            var extra = pinField();
            var body =
                'Excluir «' +
                (cli.nome || '') +
                '»?\n\nCashback: ' +
                money(p.saldo_cashback) +
                '\nVale crédito: ' +
                money(p.saldo_vale_credito);
            if (p.fiado_aberto && p.fiado_aberto.bloqueia) {
                openPanel('Não dá para excluir', p.bloqueio || 'Há fiado em aberto.', '', btnHtml('Fechar', 'ghost', 'close'));
                overlay().querySelector('[data-cli-act="close"]').addEventListener('click', closeOverlay);
                return;
            }
            if (p.precisa_transferir) {
                body +=
                    '\n\nTransfira o cashback/vale para o cadastro certo antes de excluir.';
                extra +=
                    '<label class="block mt-2"><span class="mb-1 block text-[0.75rem] font-black uppercase tracking-wide text-slate-500">Passar saldos para</span>' +
                    '<input id="agro-cli-acao-destino" type="text" class="agro-cli-acao-input w-full rounded-xl border-2 border-slate-300 px-3" placeholder="Nome ou telefone">' +
                    '<div id="agro-cli-acao-destino-list" class="mt-1 max-h-40 overflow-y-auto rounded-xl border border-slate-200"></div></label>';
            }
            openPanel(
                'Excluir cadastro',
                body,
                extra,
                btnHtml('Cancelar', 'ghost', 'close') + btnHtml('Excluir', 'danger', 'excluir')
            );
            if (p.precisa_transferir) buscarDestinoBind(pk);
            overlay().querySelectorAll('[data-cli-act]').forEach(function (b) {
                b.addEventListener('click', function () {
                    if (b.getAttribute('data-cli-act') === 'close') {
                        closeOverlay();
                        return;
                    }
                    var pin = pinVal();
                    if (!pin) {
                        showErro('Digite o PIN.');
                        return;
                    }
                    var dest = destinoPk();
                    if (p.precisa_transferir && !dest) {
                        showErro('Escolha o cadastro que vai receber o cashback/vale.');
                        return;
                    }
                    b.disabled = true;
                    jsonPost(patternUrl(urls().apiClienteExcluirPattern, pk), {
                        pin: pin,
                        destino_pk: dest,
                        origem_tela: origemTela()
                    })
                        .then(function (r2) {
                            if (!r2.data || !r2.data.ok) {
                                showErro((r2.data && r2.data.erro) || 'Não foi possível excluir.');
                                b.disabled = false;
                                return;
                            }
                            closeOverlay();
                            if (typeof onAposMudanca === 'function') {
                                onAposMudanca({ tipo: 'excluir', data: r2.data, pk: pk });
                            }
                        })
                        .catch(function () {
                            showErro('Erro de rede.');
                            b.disabled = false;
                        });
                });
            });
        });
    }

    function openVale(pk, nome) {
        var extra =
            pinField() +
            '<label class="block"><span class="mb-1 block text-[0.75rem] font-black uppercase tracking-wide text-slate-500">Valor do vale</span>' +
            '<input id="agro-cli-acao-valor" type="text" inputmode="decimal" class="agro-cli-acao-input w-full rounded-xl border-2 border-slate-300 px-3" placeholder="0,00"></label>' +
            '<label class="block"><span class="mb-1 block text-[0.75rem] font-black uppercase tracking-wide text-slate-500">Motivo (obrigatório no manual)</span>' +
            '<input id="agro-cli-acao-motivo" type="text" class="agro-cli-acao-input w-full rounded-xl border-2 border-slate-300 px-3" placeholder="Ex.: acerto da migração"></label>';
        openPanel(
            'Adicionar vale crédito',
            'Cliente: ' + (nome || '') + '\n\nPagar na hora entra no caixa. Manual só credita a conta, sem caixa.',
            extra,
            btnHtml('Cancelar', 'ghost', 'close') +
                btnHtml('Manual (sem caixa)', 'ghost', 'manual') +
                btnHtml('Pagar na hora', 'primary', 'pago')
        );
        overlay().querySelectorAll('[data-cli-act]').forEach(function (b) {
            b.addEventListener('click', function () {
                var act = b.getAttribute('data-cli-act');
                if (act === 'close') {
                    closeOverlay();
                    return;
                }
                var valorInp = el('agro-cli-acao-valor');
                var valorRaw = valorInp ? String(valorInp.value || '').trim() : '';
                var valor = Number(String(valorRaw).replace(/\./g, '').replace(',', '.'));
                if (!isFinite(valor) || valor <= 0) {
                    showErro('Informe o valor do vale.');
                    return;
                }
                var pin = pinVal();
                if (!pin) {
                    showErro('Digite o PIN.');
                    return;
                }
                if (act === 'manual') {
                    var mot = el('agro-cli-acao-motivo');
                    jsonPost(patternUrl(urls().apiClienteValeManualPattern, pk), {
                        pin: pin,
                        valor: valor,
                        motivo: mot ? mot.value : '',
                        origem_tela: origemTela()
                    }).then(function (res) {
                        if (!res.data || !res.data.ok) {
                            showErro((res.data && res.data.erro) || 'Não foi possível creditar.');
                            return;
                        }
                        closeOverlay();
                        if (typeof onAposMudanca === 'function') onAposMudanca({ tipo: 'vale_manual', data: res.data });
                    });
                    return;
                }
                if (act === 'pago') {
                    closeOverlay();
                    if (typeof onIniciarValePago === 'function') {
                        onIniciarValePago({ pk: pk, valor: valor, pin: pin, nome: nome });
                    }
                }
            });
        });
    }

    function openHistorico(pk) {
        jsonGet(patternUrl(urls().apiClienteEventosPattern, pk)).then(function (res) {
            var ev = (res.data && res.data.eventos) || [];
            var html = ev.length
                ? '<ul class="space-y-2">' +
                  ev
                      .map(function (e) {
                          var when = String(e.criado_em || '').replace('T', ' ').slice(0, 16);
                          return (
                              '<li class="rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-sm font-semibold text-slate-800">' +
                              '<span class="font-black">' +
                              (e.tipo_label || e.tipo) +
                              '</span> · ' +
                              when +
                              '<br>' +
                              (e.usuario || '') +
                              (e.destino_nome ? ' → ' + e.destino_nome : '') +
                              '</li>'
                          );
                      })
                      .join('') +
                  '</ul>'
                : '<p class="font-bold text-slate-500">Nenhum registro ainda.</p>';
            openPanel('Histórico do cadastro', '', html, btnHtml('Fechar', 'ghost', 'close'));
            overlay().querySelector('[data-cli-act="close"]').addEventListener('click', closeOverlay);
        });
    }

    function init(opts) {
        opts = opts || {};
        onAbrirCadastro = opts.onAbrirCadastro || null;
        onAposMudanca = opts.onAposMudanca || null;
        onIniciarValePago = opts.onIniciarValePago || null;
        var root = overlay();
        if (root && !root.dataset.bound) {
            root.dataset.bound = '1';
            root.addEventListener('click', function (e) {
                /* Fundo nao fecha — so X / FECHAR / Esc */
            });
        }
    }

    w.AgroClienteCadastroAcoes = {
        init: init,
        showDuplicado: showDuplicado,
        openExcluir: openExcluir,
        openVale: openVale,
        openHistorico: openHistorico,
        close: closeOverlay
    };
})(window);
