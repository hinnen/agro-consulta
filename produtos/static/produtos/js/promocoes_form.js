(function () {
    'use strict';

    var cfg = window.PROMO_FORM_CONFIG || {};
    cfg.apiSalvar = cfg.apiSalvar || '/api/promocoes/salvar/';
    cfg.apiBuscarProduto = cfg.apiBuscarProduto || '/api/promocoes/buscar-produto/';

    var elData = document.getElementById('promo-initial-data');
    var state = { produtos: [] };
    if (elData && elData.textContent) {
        try {
            state = JSON.parse(elData.textContent);
        } catch (e) {
            console.error('promo-initial-data inválido', e);
        }
    }
    if (!Array.isArray(state.produtos)) state.produtos = [];

    var dom = {
        nome: document.getElementById('promo-nome'),
        tipo: document.getElementById('promo-tipo'),
        qtdX: document.getElementById('promo-qtd-x'),
        precoY: document.getElementById('promo-preco-y'),
        inicio: document.getElementById('promo-inicio'),
        fim: document.getElementById('promo-fim'),
        ativo: document.getElementById('promo-ativo'),
        permanente: document.getElementById('promo-permanente'),
        wrapQtdX: document.getElementById('wrap-qtd-x'),
        wrapPrecoY: document.getElementById('wrap-preco-y'),
        labelQtdX: document.getElementById('label-qtd-x'),
        colPrecoPromo: document.getElementById('col-preco-promo'),
        busca: document.getElementById('promo-busca-produto'),
        resultados: document.getElementById('promo-resultados-busca'),
        lista: document.getElementById('promo-produtos-lista'),
        msg: document.getElementById('promo-msg'),
        contador: document.getElementById('promo-produtos-contador'),
    };

    var sugestoesCache = [];
    var ultimaBusca = '';
    var buscando = false;

    function fmtMoney(n) {
        return Number(n || 0).toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
    }

    function parseMoney(s) {
        if (s == null || s === '') return null;
        var t = String(s).trim().replace(/R\$\s?/g, '');
        if (t.indexOf(',') >= 0) t = t.replace(/\./g, '').replace(',', '.');
        var n = parseFloat(t);
        return isFinite(n) ? n : null;
    }

    function showMsg(text, ok) {
        if (!dom.msg) {
            alert(text);
            return;
        }
        dom.msg.textContent = text;
        dom.msg.style.display = 'block';
        dom.msg.classList.remove('hidden', 'border-emerald-200', 'bg-emerald-50', 'text-emerald-900', 'border-red-200', 'bg-red-50', 'text-red-900');
        if (ok) dom.msg.classList.add('border-emerald-200', 'bg-emerald-50', 'text-emerald-900');
        else dom.msg.classList.add('border-red-200', 'bg-red-50', 'text-red-900');
    }

    function tipoAtual() {
        return dom.tipo ? dom.tipo.value : 'leve_pague';
    }

    function syncTipoUi() {
        var t = tipoAtual();
        var valorDireto = t === 'valor_direto';
        if (dom.wrapQtdX) dom.wrapQtdX.style.display = valorDireto ? 'none' : '';
        if (dom.wrapPrecoY) dom.wrapPrecoY.style.display = valorDireto ? 'none' : '';
        if (dom.labelQtdX) {
            dom.labelQtdX.textContent = t === 'acima_unidades' ? 'Acima de X unidades' : 'Leve X unidades';
        }
        if (dom.colPrecoPromo) {
            dom.colPrecoPromo.textContent = valorDireto ? 'Preço unitário venda' : '—';
        }
        renderListaProdutos();
    }

    function syncPermanenteUi() {
        var perm = dom.permanente && dom.permanente.checked;
        if (dom.fim) {
            dom.fim.disabled = !!perm;
            dom.fim.style.opacity = perm ? '0.45' : '1';
        }
    }

    function hydrateForm() {
        if (dom.nome) dom.nome.value = state.nome || '';
        if (dom.tipo) dom.tipo.value = state.tipo || 'leve_pague';
        if (dom.qtdX && state.qtd_x != null) dom.qtdX.value = String(state.qtd_x).replace('.', ',');
        if (dom.precoY && state.preco_y != null) dom.precoY.value = String(state.preco_y).replace('.', ',');
        if (dom.inicio) dom.inicio.value = state.data_inicio || '';
        if (dom.fim) dom.fim.value = state.data_fim || '';
        if (dom.permanente) dom.permanente.checked = !!state.permanente;
        if (dom.ativo) dom.ativo.checked = state.ativo !== false;
        document.querySelectorAll('.promo-empresa-cb').forEach(function (cb) {
            cb.checked = (state.empresas || ['centro']).indexOf(cb.value) >= 0;
        });
        syncPermanenteUi();
        syncTipoUi();
    }

    function produtoJaNaLista(pid) {
        return state.produtos.some(function (x) {
            return String(x.produto_externo_id) === String(pid);
        });
    }

    function bindClickResultado(btn, p) {
        btn.addEventListener('click', function (ev) {
            ev.preventDefault();
            ev.stopPropagation();
            incluirProduto(p);
        });
    }

    function renderResultadosBusca() {
        if (!dom.resultados) return;
        if (!ultimaBusca) {
            dom.resultados.innerHTML = '';
            return;
        }
        if (!sugestoesCache.length) {
            dom.resultados.innerHTML =
                '<div class="px-4 py-8 text-center border-b border-slate-100">' +
                '<p class="text-sm font-black text-red-700">Nenhum produto para «' +
                ultimaBusca +
                '».</p>' +
                '<p class="text-xs font-semibold text-slate-500 mt-1">Tente código GM ou parte do nome.</p>' +
                '</div>';
            return;
        }

        var html =
            '<div class="px-3 py-2 bg-amber-50 border-b border-amber-200 text-[11px] font-black uppercase text-amber-900 sticky top-0 z-10">' +
            sugestoesCache.length +
            ' resultado(s) para «' +
            ultimaBusca +
            '» — clique na linha</div>';

        sugestoesCache.forEach(function (p) {
            var pid = String(p.produto_externo_id || '').trim();
            var ja = produtoJaNaLista(pid);
            html +=
                '<button type="button" data-pid="' +
                pid +
                '" class="promo-res-row w-full text-left px-3 py-3 grid grid-cols-1 sm:grid-cols-[0.7fr_1.4fr_0.7fr_0.8fr] gap-2 items-center border-b border-slate-100 last:border-0 ' +
                (ja ? 'bg-slate-100 opacity-60 cursor-not-allowed' : 'bg-amber-50/40 hover:bg-emerald-50 cursor-pointer') +
                '">' +
                '<p class="text-xs font-black text-slate-700">' +
                (p.codigo || '—') +
                '</p>' +
                '<p class="text-sm font-bold text-slate-900 leading-snug">' +
                (p.nome_produto || '') +
                '</p>' +
                '<p class="text-sm font-black text-slate-600 tabular-nums">' +
                fmtMoney(p.preco_padrao) +
                '</p>' +
                '<p class="text-[10px] font-black uppercase ' +
                (ja ? 'text-slate-500' : 'text-emerald-700') +
                '">' +
                (ja ? 'Já na promoção' : '+ Adicionar') +
                '</p>' +
                '</button>';
        });
        dom.resultados.innerHTML = html;

        dom.resultados.querySelectorAll('.promo-res-row').forEach(function (btn) {
            var pid = btn.getAttribute('data-pid');
            var p = sugestoesCache.find(function (x) {
                return String(x.produto_externo_id) === String(pid);
            });
            if (p && !produtoJaNaLista(pid)) bindClickResultado(btn, p);
        });
    }

    function renderListaProdutos() {
        if (!dom.lista) return;
        var t = tipoAtual();
        var valorDireto = t === 'valor_direto';
        var n = state.produtos.length;

        if (dom.contador) {
            dom.contador.textContent = n ? n + ' produto(s) na promoção' : '';
        }

        if (!n) {
            dom.lista.innerHTML = ultimaBusca
                ? ''
                : '<p class="px-4 py-10 text-center text-sm font-bold text-slate-400">Busque um produto e clique na linha do resultado.</p>';
            renderResultadosBusca();
            return;
        }

        var html =
            '<div class="px-3 py-2 bg-emerald-50 border-y border-emerald-200 text-[11px] font-black uppercase text-emerald-900">' +
            'Na promoção (' +
            n +
            ')</div>';

        state.produtos.forEach(function (p, idx) {
            var precoPromoCell = valorDireto
                ? '<input type="text" inputmode="decimal" data-idx="' +
                  idx +
                  '" class="promo-preco-prod w-full rounded-lg border-2 border-emerald-200 px-2 py-2 font-black text-sm min-h-[44px]" value="' +
                  (p.preco_promocional != null ? String(p.preco_promocional).replace('.', ',') : '') +
                  '">'
                : '<span class="text-xs text-slate-400 font-bold">Pelo critério</span>';
            html +=
                '<div class="px-3 py-3 grid grid-cols-1 sm:grid-cols-[0.7fr_1.4fr_0.7fr_0.8fr_0.4fr] gap-2 items-center border-b border-slate-100 last:border-0 bg-white">' +
                '<p class="text-xs font-black text-slate-700">' +
                (p.codigo || '—') +
                '</p>' +
                '<p class="text-sm font-bold text-slate-900 leading-snug">' +
                (p.nome_produto || '') +
                '</p>' +
                '<p class="text-sm font-black text-slate-600 tabular-nums">' +
                fmtMoney(p.preco_padrao) +
                '</p>' +
                '<div>' +
                precoPromoCell +
                '</div>' +
                '<button type="button" data-rm="' +
                idx +
                '" class="text-xs font-black uppercase text-red-700 min-h-[44px] px-2">Remover</button>' +
                '</div>';
        });
        dom.lista.innerHTML = html;

        dom.lista.querySelectorAll('[data-rm]').forEach(function (btn) {
            btn.addEventListener('click', function (ev) {
                ev.preventDefault();
                ev.stopPropagation();
                var i = parseInt(btn.getAttribute('data-rm'), 10);
                state.produtos.splice(i, 1);
                renderListaProdutos();
            });
        });
        dom.lista.querySelectorAll('.promo-preco-prod').forEach(function (inp) {
            inp.addEventListener('change', function () {
                var i = parseInt(inp.getAttribute('data-idx'), 10);
                if (state.produtos[i]) state.produtos[i].preco_promocional = parseMoney(inp.value);
            });
        });

        renderResultadosBusca();
    }

    function buscarProdutos() {
        var q = dom.busca ? dom.busca.value.trim() : '';
        if (q.length < 2) {
            showMsg('Digite ao menos 2 caracteres para buscar.', false);
            return Promise.resolve([]);
        }
        if (buscando) {
            return Promise.resolve(sugestoesCache.slice());
        }
        buscando = true;
        ultimaBusca = q;
        if (dom.resultados) {
            dom.resultados.innerHTML =
                '<p class="px-4 py-8 text-center text-sm font-bold text-slate-500">Buscando…</p>';
        }
        if (window.gmLoadingBar) window.gmLoadingBar.show();
        return fetch(cfg.apiBuscarProduto + '?q=' + encodeURIComponent(q), {
            credentials: 'same-origin',
            headers: { Accept: 'application/json' },
        })
            .then(function (r) {
                return r.text().then(function (txt) {
                    var data = {};
                    try {
                        data = JSON.parse(txt);
                    } catch (e) {
                        throw new Error('Resposta inválida do servidor (login expirou?)');
                    }
                    if (!r.ok) throw new Error((data && data.erro) || 'HTTP ' + r.status);
                    return data;
                });
            })
            .then(function (d) {
                sugestoesCache = d.produtos || [];
                renderListaProdutos();
                if (sugestoesCache.length) {
                    showMsg(sugestoesCache.length + ' produto(s) encontrado(s). Clique na linha para adicionar.', true);
                } else {
                    showMsg('Nenhum produto encontrado para «' + q + '».', false);
                }
                return sugestoesCache;
            })
            .catch(function (err) {
                sugestoesCache = [];
                renderListaProdutos();
                showMsg(err && err.message ? err.message : 'Falha ao buscar produtos.', false);
                return [];
            })
            .finally(function () {
                buscando = false;
                if (window.gmLoadingBar) window.gmLoadingBar.hide();
            });
    }

    function incluirProduto(p) {
        if (!p) {
            showMsg('Selecione um produto nos resultados.', false);
            return false;
        }
        var pid = String(p.produto_externo_id || p.id || '').trim();
        if (!pid) {
            showMsg('Produto sem ID no ERP. Escolha outro item.', false);
            return false;
        }
        if (produtoJaNaLista(pid)) {
            showMsg('Este produto já está na lista.', false);
            return false;
        }
        state.produtos.push({
            produto_externo_id: pid,
            codigo: String(p.codigo || ''),
            nome_produto: String(p.nome_produto || p.nome || ''),
            preco_padrao: Number(p.preco_padrao != null ? p.preco_padrao : p.preco_venda || 0),
            preco_promocional:
                tipoAtual() === 'valor_direto'
                    ? Number(p.preco_padrao != null ? p.preco_padrao : p.preco_venda || 0)
                    : null,
        });
        renderListaProdutos();
        showMsg('Adicionado: ' + (p.codigo || pid), true);
        return true;
    }

    function adicionarProduto() {
        if (sugestoesCache.length === 1 && ultimaBusca === (dom.busca ? dom.busca.value.trim() : '')) {
            incluirProduto(sugestoesCache[0]);
            return;
        }
        if (sugestoesCache.length > 1 && ultimaBusca === (dom.busca ? dom.busca.value.trim() : '')) {
            showMsg('Vários resultados — clique na linha do produto desejado.', false);
            return;
        }
        buscarProdutos().then(function (items) {
            if (items.length === 1) incluirProduto(items[0]);
            else if (items.length > 1) showMsg('Clique na linha do produto para adicionar.', false);
        });
    }

    function coletarPayload() {
        var empresas = [];
        document.querySelectorAll('.promo-empresa-cb:checked').forEach(function (cb) {
            empresas.push(cb.value);
        });
        var permanente = dom.permanente ? dom.permanente.checked : false;
        return {
            id: state.id || null,
            nome: dom.nome ? dom.nome.value.trim() : '',
            tipo: tipoAtual(),
            qtd_x: dom.qtdX ? dom.qtdX.value.trim() : null,
            preco_y: dom.precoY ? dom.precoY.value.trim() : null,
            data_inicio: dom.inicio ? dom.inicio.value : '',
            data_fim: permanente ? '' : dom.fim ? dom.fim.value : '',
            permanente: permanente,
            telas: [],
            empresas: empresas,
            ativo: dom.ativo ? dom.ativo.checked : true,
            produtos: state.produtos.map(function (p) {
                return {
                    produto_externo_id: p.produto_externo_id,
                    codigo: p.codigo,
                    nome_produto: p.nome_produto,
                    preco_padrao: p.preco_padrao,
                    preco_promocional: p.preco_promocional,
                };
            }),
        };
    }

    function salvar() {
        if (!state.produtos.length) {
            showMsg('Adicione ao menos um produto antes de salvar.', false);
            return;
        }
        if (window.gmLoadingBar) window.gmLoadingBar.show();
        fetch(cfg.apiSalvar, {
            method: 'POST',
            credentials: 'same-origin',
            headers: {
                'Content-Type': 'application/json',
                Accept: 'application/json',
                'X-CSRFToken': cfg.csrfToken || '',
            },
            body: JSON.stringify(coletarPayload()),
        })
            .then(function (r) {
                return r.text().then(function (txt) {
                    var data = {};
                    try {
                        data = JSON.parse(txt);
                    } catch (e) {
                        throw new Error('Resposta inválida ao salvar.');
                    }
                    return { ok: r.ok, data: data };
                });
            })
            .then(function (res) {
                if (!res.ok || !res.data.ok) {
                    showMsg((res.data && res.data.erro) || 'Não foi possível salvar.', false);
                    return;
                }
                showMsg('Promoção salva.', true);
                if (res.data.redirect) window.location.assign(res.data.redirect);
            })
            .catch(function (err) {
                showMsg(err && err.message ? err.message : 'Erro de rede ao salvar.', false);
            })
            .finally(function () {
                if (window.gmLoadingBar) window.gmLoadingBar.hide();
            });
    }

    function bindClick(id, fn) {
        var el = document.getElementById(id);
        if (!el) return;
        el.addEventListener('click', function (ev) {
            ev.preventDefault();
            ev.stopPropagation();
            fn();
        });
    }

    if (dom.tipo) dom.tipo.addEventListener('change', syncTipoUi);
    if (dom.permanente) dom.permanente.addEventListener('change', syncPermanenteUi);
    bindClick('promo-btn-buscar', function () {
        buscarProdutos();
    });
    bindClick('promo-btn-adicionar', adicionarProduto);
    bindClick('promo-btn-salvar', salvar);

    if (dom.busca) {
        dom.busca.setAttribute('autocomplete', 'off');
        dom.busca.addEventListener('keydown', function (e) {
            if (e.key === 'Enter') {
                e.preventDefault();
                adicionarProduto();
            }
        });
    }

    window.PromocaoForm = {
        state: state,
        buscar: buscarProdutos,
        adicionar: incluirProduto,
        render: renderListaProdutos,
    };

    hydrateForm();
    renderListaProdutos();
})();
