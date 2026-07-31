/**
 * PDV — overlay Uso loja (saída estoque consumo interno).
 * Independente do carrinho de venda.
 */
(function () {
  'use strict';

  function boot() {
    var el =
      document.getElementById('agro-pdv-wizard-bootstrap') ||
      document.getElementById('agro-pdv-bootstrap');
    try {
      return el ? JSON.parse(el.textContent || '{}') : {};
    } catch (e) {
      return {};
    }
  }

  var bootstrap = boot();
  var urls = bootstrap.urls || {};
  var overlay = document.getElementById('pdv-uso-loja-overlay');
  if (!overlay) return;

  var cart = [];
  var deposito = 'centro';
  var depositoTravado = false;
  var searchTimer = null;
  var busy = false;

  var dom = {
    btnOpen: document.getElementById('pdv-topbar-uso-loja-btn'),
    fechar: document.getElementById('pdv-uso-loja-fechar'),
    btnHist: document.getElementById('pdv-uso-loja-btn-historico'),
    btnVoltar: document.getElementById('pdv-uso-loja-btn-voltar-saida'),
    sub: document.getElementById('pdv-uso-loja-sub'),
    depHint: document.getElementById('pdv-uso-loja-dep-hint'),
    depCentro: document.getElementById('pdv-uso-loja-dep-centro'),
    depVila: document.getElementById('pdv-uso-loja-dep-vila'),
    busca: document.getElementById('pdv-uso-loja-busca'),
    hits: document.getElementById('pdv-uso-loja-hits'),
    cart: document.getElementById('pdv-uso-loja-cart'),
    cartEmpty: document.getElementById('pdv-uso-loja-cart-empty'),
    limpar: document.getElementById('pdv-uso-loja-limpar'),
    quem: document.getElementById('pdv-uso-loja-quem'),
    motivo: document.getElementById('pdv-uso-loja-motivo'),
    pin: document.getElementById('pdv-uso-loja-pin'),
    confirmar: document.getElementById('pdv-uso-loja-confirmar'),
    status: document.getElementById('pdv-uso-loja-status'),
    histList: document.getElementById('pdv-uso-loja-hist-list'),
    histStatus: document.getElementById('pdv-uso-loja-hist-status'),
  };

  function csrf() {
    if (bootstrap.csrfToken) return bootstrap.csrfToken;
    var m = document.querySelector('meta[name=csrfmiddlewaretoken]');
    return m ? m.getAttribute('content') : '';
  }

  function setStatus(msg, isErr) {
    if (!dom.status) return;
    dom.status.textContent = msg || '';
    dom.status.className =
      'text-sm font-bold ' + (isErr ? 'text-red-700' : 'text-slate-600');
  }

  function setHistStatus(msg, isErr) {
    if (!dom.histStatus) return;
    dom.histStatus.textContent = msg || '';
    dom.histStatus.className =
      'text-sm font-bold ' + (isErr ? 'text-red-700' : 'text-slate-600');
  }

  function fmtQtd(n) {
    var x = Number(n);
    if (!isFinite(x)) return '0';
    if (Math.abs(x - Math.round(x)) < 0.0005) return String(Math.round(x));
    return x.toFixed(3).replace(/\.?0+$/, '');
  }

  function syncDepBtns() {
    [dom.depCentro, dom.depVila].forEach(function (btn) {
      if (!btn) return;
      var on = btn.getAttribute('data-dep') === deposito;
      btn.classList.toggle('is-on', on);
      btn.disabled = depositoTravado;
    });
    if (dom.depHint) {
      dom.depHint.textContent = depositoTravado
        ? 'travado pelo caixa'
        : 'escolha a loja';
    }
    if (dom.sub) {
      dom.sub.textContent =
        'Saída · estoque ' + (deposito === 'vila' ? 'Vila Elias' : 'Centro');
    }
  }

  function setView(view) {
    overlay.setAttribute('data-ul-view', view);
    var hist = view === 'historico';
    if (dom.btnHist) dom.btnHist.classList.toggle('hidden', hist);
    if (dom.btnVoltar) {
      dom.btnVoltar.classList.toggle('hidden', !hist);
      dom.btnVoltar.classList.toggle('inline-flex', hist);
    }
    var saida = overlay.querySelector('.ul-view-saida');
    var historico = overlay.querySelector('.ul-view-historico');
    if (saida) {
      saida.classList.toggle('hidden', hist);
      saida.classList.toggle('flex', !hist);
    }
    if (historico) {
      historico.classList.toggle('hidden', !hist);
      historico.classList.toggle('flex', hist);
    }
  }

  function renderCart() {
    if (!dom.cart) return;
    var rows = cart.slice();
    if (!rows.length) {
      dom.cart.innerHTML =
        '<p id="pdv-uso-loja-cart-empty" class="py-4 text-center text-sm font-semibold text-slate-500">Nenhum item — busque acima.</p>';
      return;
    }
    dom.cart.innerHTML = rows
      .map(function (it, idx) {
        return (
          '<div class="ul-cart-row" data-idx="' +
          idx +
          '">' +
          '<div class="min-w-0">' +
          '<div class="truncate text-sm font-black text-slate-900">' +
          escapeHtml(it.nome || it.produto_id) +
          '</div>' +
          '<div class="truncate text-[11px] font-bold text-slate-500">' +
          escapeHtml(it.codigo || it.produto_id) +
          '</div>' +
          '</div>' +
          '<input type="number" min="0.001" step="any" class="ul-field w-[5.5rem] text-center tabular-nums" data-ul-qtd="' +
          idx +
          '" value="' +
          fmtQtd(it.quantidade) +
          '" />' +
          '<button type="button" class="inline-flex min-h-[2.5rem] min-w-[2.5rem] items-center justify-center rounded-lg border border-red-200 bg-red-50 text-sm font-black text-red-700" data-ul-rm="' +
          idx +
          '" title="Remover">×</button>' +
          '</div>'
        );
      })
      .join('');
  }

  function escapeHtml(s) {
    return String(s || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function addProduct(p) {
    var pid = String(p.id || p.produto_id || '').trim();
    if (!pid) return;
    var nome = String(p.nome || p.name || pid).trim();
    var codigo = String(p.codigo_gm || p.codigo_nfe || p.codigo || p.gm || '').trim();
    var found = null;
    for (var i = 0; i < cart.length; i++) {
      if (cart[i].produto_id === pid) {
        found = cart[i];
        break;
      }
    }
    if (found) {
      found.quantidade = Number(found.quantidade || 0) + 1;
    } else {
      cart.push({
        produto_id: pid,
        nome: nome,
        codigo: codigo,
        quantidade: 1,
      });
    }
    if (dom.busca) dom.busca.value = '';
    if (dom.hits) dom.hits.innerHTML = '';
    renderCart();
    setStatus(nome + ' adicionado.');
  }

  function renderHits(lista) {
    if (!dom.hits) return;
    if (!lista || !lista.length) {
      dom.hits.innerHTML =
        '<p class="text-sm font-semibold text-slate-500 px-1">Nenhum produto.</p>';
      return;
    }
    dom.hits.innerHTML = lista
      .slice(0, 12)
      .map(function (p) {
        var pid = String(p.id || '').trim();
        var nome = String(p.nome || pid).trim();
        var cod = String(p.codigo_gm || p.codigo_nfe || p.codigo || '').trim();
        return (
          '<button type="button" class="ul-hit" data-ul-add="' +
          escapeHtml(pid) +
          '">' +
          '<span class="min-w-0 flex-1">' +
          '<span class="block truncate text-sm font-black text-slate-900">' +
          escapeHtml(nome) +
          '</span>' +
          '<span class="block truncate text-[11px] font-bold text-slate-500">' +
          escapeHtml(cod || pid) +
          '</span>' +
          '</span>' +
          '<span class="shrink-0 rounded-lg bg-emerald-600 px-2 py-1 text-[10px] font-black uppercase text-white">+</span>' +
          '</button>'
        );
      })
      .join('');
    dom.hits.querySelectorAll('[data-ul-add]').forEach(function (btn) {
      btn.addEventListener('click', function () {
        var id = btn.getAttribute('data-ul-add');
        var match = null;
        for (var i = 0; i < lista.length; i++) {
          if (String(lista[i].id || '') === id) {
            match = lista[i];
            break;
          }
        }
        if (match) addProduct(match);
      });
    });
  }

  function buscar(q) {
    var query = String(q || '').trim();
    if (query.length < 1) {
      if (dom.hits) dom.hits.innerHTML = '';
      return;
    }
    var base = urls.apiBuscarProdutos || '/api/buscar/';
    var url =
      base +
      (base.indexOf('?') >= 0 ? '&' : '?') +
      'wizard=1&q=' +
      encodeURIComponent(query);
    fetch(url, { credentials: 'same-origin' })
      .then(function (r) {
        return r.json();
      })
      .then(function (data) {
        var lista =
          (data && (data.produtos || data.results || data.items)) || [];
        if (!Array.isArray(lista) && data && Array.isArray(data.data)) {
          lista = data.data;
        }
        renderHits(lista);
      })
      .catch(function () {
        if (dom.hits) {
          dom.hits.innerHTML =
            '<p class="text-sm font-semibold text-red-600 px-1">Falha na busca.</p>';
        }
      });
  }

  function loadMeta(cb) {
    var url = urls.apiPdvUsoLojaMeta || '/api/pdv/uso-loja/meta/';
    fetch(url, { credentials: 'same-origin' })
      .then(function (r) {
        return r.json();
      })
      .then(function (data) {
        if (data && data.ok) {
          deposito = data.deposito === 'vila' ? 'vila' : 'centro';
          depositoTravado = !!data.deposito_travado;
          syncDepBtns();
        }
        if (typeof cb === 'function') cb();
      })
      .catch(function () {
        syncDepBtns();
        if (typeof cb === 'function') cb();
      });
  }

  function openOverlay() {
    overlay.classList.remove('hidden');
    overlay.classList.add('flex');
    document.body.classList.add('modal-open');
    setView('saida');
    setStatus('');
    loadMeta(function () {
      if (dom.busca) {
        try {
          dom.busca.focus();
        } catch (e) {}
      }
    });
  }

  function closeOverlay() {
    overlay.classList.add('hidden');
    overlay.classList.remove('flex');
    document.body.classList.remove('modal-open');
    if (dom.hits) dom.hits.innerHTML = '';
    if (dom.pin) dom.pin.value = '';
  }

  function confirmar() {
    if (busy) return;
    if (!cart.length) {
      setStatus('Adicione ao menos um produto.', true);
      return;
    }
    var pin = (dom.pin && dom.pin.value) || '';
    if (!String(pin).trim()) {
      setStatus('Informe o PIN.', true);
      if (dom.pin) dom.pin.focus();
      return;
    }
    busy = true;
    setStatus('Gravando saída…');
    var payload = {
      pin: String(pin).trim(),
      deposito: deposito,
      quem_levou: (dom.quem && dom.quem.value) || '',
      motivo: (dom.motivo && dom.motivo.value) || '',
      itens: cart.map(function (it) {
        return {
          produto_id: it.produto_id,
          nome: it.nome,
          codigo: it.codigo,
          quantidade: it.quantidade,
        };
      }),
    };
    var url = urls.apiPdvUsoLojaConfirmar || '/api/pdv/uso-loja/confirmar/';
    fetch(url, {
      method: 'POST',
      credentials: 'same-origin',
      headers: {
        'Content-Type': 'application/json',
        'X-CSRFToken': csrf(),
      },
      body: JSON.stringify(payload),
    })
      .then(function (r) {
        return r.json().then(function (j) {
          return { status: r.status, j: j };
        });
      })
      .then(function (pack) {
        busy = false;
        var j = pack.j || {};
        if (!j.ok) {
          setStatus(j.erro || 'Não foi possível gravar.', true);
          return;
        }
        cart = [];
        renderCart();
        if (dom.pin) dom.pin.value = '';
        if (dom.quem) dom.quem.value = '';
        if (dom.motivo) dom.motivo.value = '';
        setStatus(j.mensagem || 'Saída registrada.');
      })
      .catch(function () {
        busy = false;
        setStatus('Falha de rede ao gravar.', true);
      });
  }

  function fmtData(iso) {
    if (!iso) return '—';
    try {
      var d = new Date(iso);
      if (isNaN(d.getTime())) return iso;
      var dd = String(d.getDate()).padStart(2, '0');
      var mm = String(d.getMonth() + 1).padStart(2, '0');
      var yy = String(d.getFullYear()).slice(-2);
      var hh = String(d.getHours()).padStart(2, '0');
      var mi = String(d.getMinutes()).padStart(2, '0');
      return dd + '/' + mm + '/' + yy + ' ' + hh + ':' + mi;
    } catch (e) {
      return iso;
    }
  }

  function loadHistorico() {
    setHistStatus('Carregando…');
    var url = urls.apiPdvUsoLojaHistorico || '/api/pdv/uso-loja/historico/';
    fetch(url + (url.indexOf('?') >= 0 ? '&' : '?') + 'limit=50', {
      credentials: 'same-origin',
    })
      .then(function (r) {
        return r.json();
      })
      .then(function (data) {
        var itens = (data && data.itens) || [];
        if (!itens.length) {
          dom.histList.innerHTML =
            '<p class="py-6 text-center text-sm font-semibold text-slate-500">Nenhuma saída ainda.</p>';
          setHistStatus('');
          return;
        }
        dom.histList.innerHTML = itens
          .map(function (r) {
            var itensTxt = (r.itens || [])
              .map(function (it) {
                return (
                  escapeHtml(it.nome || it.produto_id) +
                  ' × ' +
                  fmtQtd(it.quantidade)
                );
              })
              .join('<br>');
            var est = r.estornado
              ? '<span class="text-[10px] font-black uppercase text-red-700">Estornada</span>'
              : '<button type="button" class="inline-flex min-h-[2.25rem] items-center rounded-lg border border-amber-300 bg-amber-50 px-2 text-[10px] font-black uppercase text-amber-950" data-ul-estornar="' +
                r.id +
                '">Estornar</button>';
            return (
              '<div class="ul-hist-row' +
              (r.estornado ? ' is-estornado' : '') +
              '">' +
              '<div class="flex flex-wrap items-start justify-between gap-2">' +
              '<div class="min-w-0">' +
              '<div class="text-xs font-black uppercase text-slate-800">#' +
              r.id +
              ' · ' +
              escapeHtml(r.deposito_label || r.deposito) +
              ' · ' +
              fmtData(r.criado_em) +
              '</div>' +
              '<div class="mt-0.5 text-sm font-bold text-slate-700">Quem: ' +
              escapeHtml(r.quem_levou || '—') +
              (r.motivo_label
                ? ' · ' + escapeHtml(r.motivo_label)
                : '') +
              '</div>' +
              '<div class="mt-1 text-sm font-semibold text-slate-600 leading-snug">' +
              itensTxt +
              '</div>' +
              '<div class="mt-0.5 text-[11px] font-semibold text-slate-400">PIN: ' +
              escapeHtml(r.operador_pin || '—') +
              '</div>' +
              '</div>' +
              '<div class="shrink-0">' +
              est +
              '</div>' +
              '</div>' +
              '</div>'
            );
          })
          .join('');
        setHistStatus(itens.length + ' registro(s)');
        dom.histList.querySelectorAll('[data-ul-estornar]').forEach(function (btn) {
          btn.addEventListener('click', function () {
            var pk = btn.getAttribute('data-ul-estornar');
            estornar(pk);
          });
        });
      })
      .catch(function () {
        setHistStatus('Falha ao carregar histórico.', true);
      });
  }

  function estornar(pk) {
    var pin = window.prompt('PIN para estornar a saída #' + pk + ':');
    if (pin == null) return;
    pin = String(pin).trim();
    if (!pin) {
      setHistStatus('PIN obrigatório para estornar.', true);
      return;
    }
    setHistStatus('Estornando…');
    var pattern =
      urls.apiPdvUsoLojaEstornarPattern ||
      '/api/pdv/uso-loja/estornar/__pk__/';
    var url = pattern.replace('__pk__', String(pk));
    fetch(url, {
      method: 'POST',
      credentials: 'same-origin',
      headers: {
        'Content-Type': 'application/json',
        'X-CSRFToken': csrf(),
      },
      body: JSON.stringify({ pin: pin }),
    })
      .then(function (r) {
        return r.json();
      })
      .then(function (j) {
        if (!j || !j.ok) {
          setHistStatus((j && j.erro) || 'Não foi possível estornar.', true);
          return;
        }
        setHistStatus(j.mensagem || 'Estornada.');
        loadHistorico();
      })
      .catch(function () {
        setHistStatus('Falha de rede no estorno.', true);
      });
  }

  if (dom.btnOpen) {
    dom.btnOpen.addEventListener('click', function (ev) {
      ev.preventDefault();
      openOverlay();
    });
  }
  if (dom.fechar) dom.fechar.addEventListener('click', closeOverlay);
  overlay.addEventListener('click', function (ev) {
    if (ev.target === overlay) closeOverlay();
  });
  if (dom.btnHist) {
    dom.btnHist.addEventListener('click', function () {
      setView('historico');
      loadHistorico();
    });
  }
  if (dom.btnVoltar) {
    dom.btnVoltar.addEventListener('click', function () {
      setView('saida');
      setStatus('');
    });
  }
  [dom.depCentro, dom.depVila].forEach(function (btn) {
    if (!btn) return;
    btn.addEventListener('click', function () {
      if (depositoTravado) return;
      deposito = btn.getAttribute('data-dep') === 'vila' ? 'vila' : 'centro';
      syncDepBtns();
    });
  });
  if (dom.busca) {
    dom.busca.addEventListener('input', function () {
      clearTimeout(searchTimer);
      var q = dom.busca.value;
      searchTimer = setTimeout(function () {
        buscar(q);
      }, 220);
    });
    dom.busca.addEventListener('keydown', function (ev) {
      if (ev.key === 'Enter') {
        ev.preventDefault();
        clearTimeout(searchTimer);
        buscar(dom.busca.value);
      }
    });
  }
  if (dom.cart) {
    dom.cart.addEventListener('click', function (ev) {
      var rm = ev.target.closest('[data-ul-rm]');
      if (!rm) return;
      var idx = parseInt(rm.getAttribute('data-ul-rm'), 10);
      if (isNaN(idx)) return;
      cart.splice(idx, 1);
      renderCart();
    });
    dom.cart.addEventListener('change', function (ev) {
      var inp = ev.target.closest('[data-ul-qtd]');
      if (!inp) return;
      var idx = parseInt(inp.getAttribute('data-ul-qtd'), 10);
      if (isNaN(idx) || !cart[idx]) return;
      var v = parseFloat(String(inp.value || '').replace(',', '.'));
      if (!isFinite(v) || v <= 0) v = 1;
      cart[idx].quantidade = v;
      inp.value = fmtQtd(v);
    });
  }
  if (dom.limpar) {
    dom.limpar.addEventListener('click', function () {
      cart = [];
      renderCart();
      setStatus('Lista limpa.');
    });
  }
  if (dom.confirmar) dom.confirmar.addEventListener('click', confirmar);
  document.addEventListener('keydown', function (ev) {
    if (ev.key !== 'Escape') return;
    if (overlay.classList.contains('hidden')) return;
    closeOverlay();
  });

  window.AgroPdvUsoLoja = {
    open: openOverlay,
    close: closeOverlay,
  };
})();
