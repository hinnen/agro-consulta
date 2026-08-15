/**
 * PDV — overlay Pedir loja (pedido de transferência Centro ↔ Vila).
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
  var overlay = document.getElementById('pdv-pedir-loja-overlay');
  if (!overlay) return;

  var cart = [];
  var deposito = 'centro';
  var outra = 'vila';
  var outraLabel = 'Vila Elias';
  var depLabel = 'Centro';
  var busy = false;
  var searchTimer = null;
  var hitList = [];
  var hitSelectionIndex = -1;
  var pollTimer = null;
  var pendingTransferId = null;

  var dom = {
    btnOpen: document.getElementById('pdv-topbar-pedir-loja-btn'),
    btnCount: document.getElementById('pdv-topbar-pedir-loja-count'),
    fechar: document.getElementById('pdv-pedir-loja-fechar'),
    sub: document.getElementById('pdv-pedir-loja-sub'),
    dir: document.getElementById('pdv-pedir-loja-dir'),
    dirHint: document.getElementById('pdv-pedir-loja-dir-hint'),
    busca: document.getElementById('pdv-pedir-loja-busca'),
    hits: document.getElementById('pdv-pedir-loja-hits'),
    cart: document.getElementById('pdv-pedir-loja-cart'),
    limpar: document.getElementById('pdv-pedir-loja-limpar'),
    enviar: document.getElementById('pdv-pedir-loja-enviar'),
    status: document.getElementById('pdv-pedir-loja-status'),
    lista: document.getElementById('pdv-pedir-loja-lista'),
    listaStatus: document.getElementById('pdv-pedir-loja-lista-status'),
    badgeRec: document.getElementById('pdv-pedir-loja-badge-rec'),
    badgeEnv: document.getElementById('pdv-pedir-loja-badge-env'),
    tabPedir: document.getElementById('pdv-pedir-loja-tab-pedir'),
    tabRec: document.getElementById('pdv-pedir-loja-tab-recebidos'),
    tabEnv: document.getElementById('pdv-pedir-loja-tab-enviados'),
    pinBox: document.getElementById('pdv-pedir-loja-pin'),
    pinInput: document.getElementById('pdv-pedir-loja-pin-input'),
    pinErr: document.getElementById('pdv-pedir-loja-pin-err'),
    pinOk: document.getElementById('pdv-pedir-loja-pin-ok'),
    pinCancel: document.getElementById('pdv-pedir-loja-pin-cancelar'),
    pinHint: document.getElementById('pdv-pedir-loja-pin-hint'),
  };

  function csrf() {
    if (bootstrap.csrfToken) return bootstrap.csrfToken;
    var m = document.querySelector('meta[name=csrfmiddlewaretoken]');
    if (m) return m.getAttribute('content') || '';
    var ck = document.cookie.match(/csrftoken=([^;]+)/);
    return ck ? decodeURIComponent(ck[1]) : '';
  }

  function escapeHtml(s) {
    return String(s ?? '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function fmtQtd(n) {
    var x = Number(n);
    if (!isFinite(x)) return '0';
    if (Math.abs(x - Math.round(x)) < 0.0005) return String(Math.round(x));
    return x.toFixed(3).replace(/\.?0+$/, '');
  }

  function setStatus(msg, isErr) {
    if (!dom.status) return;
    dom.status.textContent = msg || '';
    dom.status.className =
      'text-sm font-bold ' + (isErr ? 'text-red-700' : 'text-slate-600');
  }

  function setListaStatus(msg, isErr) {
    if (!dom.listaStatus) return;
    dom.listaStatus.textContent = msg || '';
    dom.listaStatus.className =
      'text-sm font-bold ' + (isErr ? 'text-red-700' : 'text-slate-600');
  }

  function urlResumo() {
    return urls.apiPdvPedirLojaResumo || '/api/pdv/pedir-loja/resumo/';
  }
  function urlLista() {
    return urls.apiPdvPedirLojaLista || '/api/pdv/pedir-loja/lista/';
  }
  function urlCriar() {
    return urls.apiPdvPedirLojaCriar || '/api/pdv/pedir-loja/criar/';
  }
  function urlAcao(pk) {
    var p =
      urls.apiPdvPedirLojaAcaoPattern || '/api/pdv/pedir-loja/__pk__/acao/';
    return p.replace('__pk__', String(pk));
  }

  function pareceBipCodigo(q) {
    var s = String(q || '').trim();
    if (s.length < 6) return false;
    return /^\d{6,}$/.test(s) || /^[A-Za-z0-9]{8,}$/.test(s);
  }

  function syncDir() {
    if (dom.dir) {
      dom.dir.textContent =
        outraLabel + ' → ' + depLabel + ' (pede para ' + outraLabel + ')';
    }
    if (dom.dirHint) {
      dom.dirHint.textContent = 'este PDV: ' + depLabel;
    }
    if (dom.sub) {
      dom.sub.textContent = 'Pedir produto de ' + outraLabel;
    }
  }

  function applyResumo(data) {
    if (!data || !data.ok) return;
    deposito = data.deposito === 'vila' ? 'vila' : 'centro';
    outra = data.outra_loja === 'vila' ? 'vila' : 'centro';
    depLabel = data.deposito_label || (deposito === 'vila' ? 'Vila Elias' : 'Centro');
    outraLabel = data.outra_loja_label || (outra === 'vila' ? 'Vila Elias' : 'Centro');
    syncDir();
    var rec = Number(data.pendentes_recebidos || 0) + Number(data.aceitos_recebidos || 0);
    var env = Number(data.pendentes_enviados || 0);
    var badge = Number(data.badge || rec);
    if (dom.badgeRec) {
      dom.badgeRec.textContent = String(rec);
      dom.badgeRec.classList.toggle('hidden', rec <= 0);
    }
    if (dom.badgeEnv) {
      dom.badgeEnv.textContent = String(env);
      dom.badgeEnv.classList.toggle('hidden', env <= 0);
    }
    if (dom.btnCount) {
      if (badge > 0) {
        dom.btnCount.textContent = String(badge);
        dom.btnCount.classList.remove('hidden');
      } else {
        dom.btnCount.classList.add('hidden');
      }
    }
    if (dom.btnOpen) {
      dom.btnOpen.classList.toggle('pdv-wiz-topbar-btn--pedir-pendente', badge > 0);
      dom.btnOpen.title =
        badge > 0
          ? 'Pedidos da outra loja aguardando (' + badge + ')'
          : 'Pedir produto da outra loja';
    }
  }

  function refreshResumo() {
    return fetch(urlResumo(), { credentials: 'same-origin' })
      .then(function (r) {
        return r.json();
      })
      .then(applyResumo)
      .catch(function () {});
  }

  function hideHits() {
    if (dom.hits) {
      dom.hits.classList.add('hidden');
      dom.hits.innerHTML = '';
    }
    hitList = [];
    hitSelectionIndex = -1;
  }

  function positionHits() {
    if (!dom.hits || !dom.busca || !overlay) return;
    var panel = document.getElementById('pdv-pedir-loja-panel');
    if (!panel) return;
    var br = dom.busca.getBoundingClientRect();
    var pr = panel.getBoundingClientRect();
    var top = br.bottom - pr.top + 6;
    var bottomGap = 12;
    dom.hits.style.top = top + 'px';
    dom.hits.style.bottom = bottomGap + 'px';
  }

  function saldoDe(p, loja) {
    if (loja === 'vila') return Number(p.saldo_vila != null ? p.saldo_vila : 0);
    return Number(p.saldo_centro != null ? p.saldo_centro : 0);
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
      if (!found.codigo && codigo) found.codigo = codigo;
    } else {
      cart.push({
        produto_id: pid,
        nome: nome,
        codigo: codigo,
        quantidade: 1,
        saldo_origem: saldoDe(p, outra),
      });
    }
    if (dom.busca) dom.busca.value = '';
    hideHits();
    renderCart();
    setStatus(nome + ' adicionado.');
  }

  function renderHits(lista) {
    if (!dom.hits) return;
    hitList = Array.isArray(lista) ? lista.slice(0, 50) : [];
    if (!hitList.length) {
      hitSelectionIndex = -1;
      dom.hits.innerHTML =
        '<div class="pl-hits-scroll"><p class="text-sm font-semibold text-slate-700 px-2 py-2">Nenhum produto.</p></div>';
      dom.hits.classList.remove('hidden');
      positionHits();
      return;
    }
    if (hitSelectionIndex < 0 || hitSelectionIndex >= hitList.length) {
      hitSelectionIndex = 0;
    }
    var rows = hitList
      .map(function (p, i) {
        var pid = String(p.id || '').trim();
        var nome = String(p.nome || pid).trim();
        var cod =
          String(p.codigo_gm || p.codigo_nfe || p.codigo || '').trim() || '—';
        var sc = fmtQtd(p.saldo_centro);
        var sv = fmtQtd(p.saldo_vila);
        var sel = i === hitSelectionIndex ? ' pl-hit-selected' : '';
        return (
          '<tr data-pl-add="' +
          escapeHtml(pid) +
          '" data-pl-idx="' +
          i +
          '" tabindex="-1" role="option" class="' +
          sel.trim() +
          '">' +
          '<td class="pl-td-gm">' +
          escapeHtml(cod) +
          '</td>' +
          '<td class="pl-td-nome" title="' +
          escapeHtml(nome) +
          '">' +
          escapeHtml(nome) +
          '</td>' +
          '<td class="pl-td-saldo">' +
          escapeHtml(sc) +
          '</td>' +
          '<td class="pl-td-saldo">' +
          escapeHtml(sv) +
          '</td>' +
          '<td class="text-center"><span class="inline-flex h-7 w-7 items-center justify-center rounded-full bg-sky-600 font-black text-white">+</span></td>' +
          '</tr>'
        );
      })
      .join('');
    dom.hits.innerHTML =
      '<div class="pl-hits-scroll">' +
      '<table class="pl-hits-table">' +
      '<colgroup><col class="pl-col-gm"/><col class="pl-col-nome"/><col class="pl-col-c"/><col class="pl-col-v"/><col class="pl-col-add"/></colgroup>' +
      '<thead><tr><th>Código GM</th><th>Nome</th><th>C</th><th>V</th><th></th></tr></thead>' +
      '<tbody>' +
      rows +
      '</tbody></table></div>';
    dom.hits.classList.remove('hidden');
    positionHits();
    dom.hits.querySelectorAll('[data-pl-add]').forEach(function (row) {
      row.addEventListener('click', function () {
        var i = parseInt(row.getAttribute('data-pl-idx'), 10);
        if (!isNaN(i) && hitList[i]) addProduct(hitList[i]);
      });
    });
  }

  function buscar(q, opts) {
    opts = opts || {};
    var query = String(q || '').trim();
    if (query.length < 1) {
      hideHits();
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
        var exact = !!(data && data.exact_barcode_match);
        if (exact || pareceBipCodigo(query) || opts.fromEnter) {
          if (lista.length === 1) {
            addProduct(lista[0]);
            return;
          }
        }
        renderHits(lista);
      })
      .catch(function () {
        if (dom.hits) {
          dom.hits.innerHTML =
            '<div class="pl-hits-scroll"><p class="text-sm font-semibold text-red-600 px-2 py-2">Falha na busca.</p></div>';
          dom.hits.classList.remove('hidden');
        }
      });
  }

  function renderCart() {
    if (!dom.cart) return;
    if (!cart.length) {
      dom.cart.innerHTML =
        '<p class="py-4 text-center text-sm font-semibold text-slate-500">Nenhum item — busque acima.</p>';
      return;
    }
    var rows = cart
      .map(function (it, i) {
        return (
          '<tr>' +
          '<td class="pl-td-gm">' +
          escapeHtml(it.codigo || '—') +
          '</td>' +
          '<td class="pl-td-nome" title="' +
          escapeHtml(it.nome) +
          '">' +
          escapeHtml(it.nome) +
          '</td>' +
          '<td><input type="number" min="0.001" step="1" class="pl-field" data-pl-qtd="' +
          i +
          '" value="' +
          escapeHtml(fmtQtd(it.quantidade)) +
          '" /></td>' +
          '<td class="text-center"><button type="button" class="pl-cart-rm" data-pl-rm="' +
          i +
          '" aria-label="Remover">×</button></td>' +
          '</tr>'
        );
      })
      .join('');
    dom.cart.innerHTML =
      '<table class="pl-cart-table">' +
      '<colgroup><col class="pl-col-gm"/><col class="pl-col-nome"/><col class="pl-col-qtd"/><col class="pl-col-rm"/></colgroup>' +
      '<thead><tr><th>GM</th><th>Produto</th><th>Qtd</th><th></th></tr></thead>' +
      '<tbody>' +
      rows +
      '</tbody></table>';
    dom.cart.querySelectorAll('[data-pl-qtd]').forEach(function (inp) {
      inp.addEventListener('change', function () {
        var i = parseInt(inp.getAttribute('data-pl-qtd'), 10);
        var n = Number(String(inp.value || '').replace(',', '.'));
        if (!isFinite(n) || n <= 0) n = 1;
        if (cart[i]) cart[i].quantidade = n;
      });
    });
    dom.cart.querySelectorAll('[data-pl-rm]').forEach(function (btn) {
      btn.addEventListener('click', function () {
        var i = parseInt(btn.getAttribute('data-pl-rm'), 10);
        if (!isNaN(i)) {
          cart.splice(i, 1);
          renderCart();
        }
      });
    });
  }

  function setView(view) {
    overlay.setAttribute('data-pl-view', view);
    [dom.tabPedir, dom.tabRec, dom.tabEnv].forEach(function (btn) {
      if (!btn) return;
      btn.classList.toggle('is-on', btn.getAttribute('data-pl-tab') === view);
    });
    if (view !== 'pedir') loadLista(view);
  }

  function statusClass(st) {
    if (st === 'PENDENTE') return 'is-pendente';
    if (st === 'ACEITO') return 'is-aceito';
    return '';
  }

  function renderLista(itens, papel) {
    if (!dom.lista) return;
    if (!itens || !itens.length) {
      var vazio =
        papel === 'enviados'
          ? 'Nenhum pedido enviado em aberto.'
          : 'Nenhum pedido recebido em aberto.';
      dom.lista.innerHTML =
        '<p class="py-8 text-center text-sm font-semibold text-slate-500">' +
        vazio +
        '</p>';
      return;
    }
    dom.lista.innerHTML = itens
      .map(function (it) {
        var st = String(it.status || '');
        var qtd = fmtQtd(it.quantidade);
        var meta =
          qtd +
          ' · ' +
          escapeHtml(it.loja_origem_label || '') +
          ' → ' +
          escapeHtml(it.loja_destino_label || '') +
          ' · ' +
          escapeHtml(st) +
          ' · ' +
          escapeHtml(it.criado_em || '');
        var acts = '';
        if (papel === 'recebidos') {
          if (st === 'PENDENTE') {
            acts +=
              '<button type="button" class="pl-act pl-act--ok" data-pl-acao="aceitar" data-pl-id="' +
              it.id +
              '">Aceitar</button>';
          }
          if (st === 'PENDENTE' || st === 'ACEITO') {
            acts +=
              '<button type="button" class="pl-act pl-act--tr" data-pl-acao="transferir" data-pl-id="' +
              it.id +
              '">Transferir agora</button>';
            acts +=
              '<button type="button" class="pl-act pl-act--no" data-pl-acao="recusar" data-pl-id="' +
              it.id +
              '">Recusar</button>';
          }
        } else if (st === 'PENDENTE') {
          acts +=
            '<button type="button" class="pl-act pl-act--no" data-pl-acao="cancelar" data-pl-id="' +
            it.id +
            '">Cancelar</button>';
        }
        return (
          '<article class="pl-req ' +
          statusClass(st) +
          '">' +
          '<div class="pl-req-name">' +
          escapeHtml(it.nome || it.produto_id) +
          '</div>' +
          '<div class="pl-req-meta">' +
          escapeHtml(it.codigo || '') +
          (it.codigo ? ' · ' : '') +
          meta +
          '</div>' +
          (acts ? '<div class="pl-req-actions">' + acts + '</div>' : '') +
          '</article>'
        );
      })
      .join('');
    dom.lista.querySelectorAll('[data-pl-acao]').forEach(function (btn) {
      btn.addEventListener('click', function () {
        var acao = btn.getAttribute('data-pl-acao');
        var id = parseInt(btn.getAttribute('data-pl-id'), 10);
        if (!acao || !id) return;
        if (acao === 'transferir') {
          openPin(id);
          return;
        }
        runAcao(id, acao, '');
      });
    });
  }

  function loadLista(papel) {
    setListaStatus('Carregando…');
    var url =
      urlLista() +
      (urlLista().indexOf('?') >= 0 ? '&' : '?') +
      'papel=' +
      encodeURIComponent(papel);
    fetch(url, { credentials: 'same-origin' })
      .then(function (r) {
        return r.json();
      })
      .then(function (data) {
        applyResumo(data);
        renderLista(data.itens || [], papel);
        setListaStatus(
          (data.itens && data.itens.length ? data.itens.length + ' pedido(s).' : '') ||
            ''
        );
      })
      .catch(function () {
        setListaStatus('Falha ao carregar.', true);
      });
  }

  function runAcao(pk, acao, pin) {
    if (busy) return;
    busy = true;
    fetch(urlAcao(pk), {
      method: 'POST',
      credentials: 'same-origin',
      headers: {
        'Content-Type': 'application/json',
        'X-CSRFToken': csrf(),
      },
      body: JSON.stringify({ acao: acao, pin: pin || '', deposito: deposito }),
    })
      .then(function (r) {
        return r.json().then(function (data) {
          return { okHttp: r.ok, data: data };
        });
      })
      .then(function (pack) {
        busy = false;
        var data = pack.data || {};
        if (!data.ok) {
          if (acao === 'transferir' && dom.pinErr) {
            dom.pinErr.textContent = data.erro || 'Falha.';
          }
          setListaStatus(data.erro || 'Falha.', true);
          return;
        }
        closePin();
        applyResumo(data);
        var view = overlay.getAttribute('data-pl-view') || 'recebidos';
        if (view !== 'pedir') loadLista(view);
        setListaStatus(
          acao === 'transferir' ? 'Estoque transferido.' : 'Status atualizado.'
        );
      })
      .catch(function () {
        busy = false;
        setListaStatus('Falha de rede.', true);
      });
  }

  function openPin(pk) {
    pendingTransferId = pk;
    if (dom.pinErr) dom.pinErr.textContent = '';
    if (dom.pinInput) dom.pinInput.value = '';
    if (dom.pinBox) dom.pinBox.classList.remove('hidden');
    try {
      if (dom.pinInput) dom.pinInput.focus();
    } catch (e) {}
  }

  function closePin() {
    pendingTransferId = null;
    if (dom.pinBox) dom.pinBox.classList.add('hidden');
  }

  function enviarPedido() {
    if (busy) return;
    if (!cart.length) {
      setStatus('Inclua ao menos um produto.', true);
      return;
    }
    busy = true;
    setStatus('Enviando…');
    fetch(urlCriar(), {
      method: 'POST',
      credentials: 'same-origin',
      headers: {
        'Content-Type': 'application/json',
        'X-CSRFToken': csrf(),
      },
      body: JSON.stringify({
        itens: cart.map(function (it) {
          return {
            produto_id: it.produto_id,
            nome: it.nome,
            codigo: it.codigo,
            quantidade: it.quantidade,
          };
        }),
        deposito: deposito,
      }),
    })
      .then(function (r) {
        return r.json();
      })
      .then(function (data) {
        busy = false;
        if (!data || !data.ok) {
          setStatus((data && data.erro) || 'Falha ao enviar.', true);
          return;
        }
        cart = [];
        renderCart();
        applyResumo(data);
        setStatus(data.mensagem || 'Pedido enviado.');
        setView('enviados');
      })
      .catch(function () {
        busy = false;
        setStatus('Falha de rede.', true);
      });
  }

  function openOverlay(view) {
    overlay.classList.remove('hidden');
    overlay.classList.add('flex');
    refreshResumo().then(function () {
      setView(view || (Number((dom.btnCount || {}).textContent) > 0 ? 'recebidos' : 'pedir'));
      renderCart();
      try {
        if (dom.busca) dom.busca.focus();
      } catch (e) {}
    });
  }

  function closeOverlay() {
    hideHits();
    closePin();
    overlay.classList.add('hidden');
    overlay.classList.remove('flex');
  }

  function isOpen() {
    return overlay.classList.contains('flex');
  }

  if (dom.btnOpen) {
    dom.btnOpen.addEventListener('click', function () {
      if (isOpen()) closeOverlay();
      else openOverlay();
    });
  }
  if (dom.fechar) dom.fechar.addEventListener('click', closeOverlay);
  overlay.addEventListener('click', function (ev) {
    if (ev.target === overlay) closeOverlay();
  });
  [dom.tabPedir, dom.tabRec, dom.tabEnv].forEach(function (btn) {
    if (!btn) return;
    btn.addEventListener('click', function () {
      setView(btn.getAttribute('data-pl-tab'));
    });
  });
  if (dom.limpar) {
    dom.limpar.addEventListener('click', function () {
      cart = [];
      renderCart();
      setStatus('');
    });
  }
  if (dom.enviar) dom.enviar.addEventListener('click', enviarPedido);
  if (dom.busca) {
    dom.busca.addEventListener('input', function () {
      clearTimeout(searchTimer);
      var q = dom.busca.value;
      searchTimer = setTimeout(function () {
        buscar(q);
      }, 180);
    });
    dom.busca.addEventListener('keydown', function (ev) {
      if (ev.key === 'Enter') {
        ev.preventDefault();
        buscar(dom.busca.value, { fromEnter: true });
      } else if (ev.key === 'Escape') {
        hideHits();
      }
    });
  }
  if (dom.pinOk) {
    dom.pinOk.addEventListener('click', function () {
      var pin = (dom.pinInput && dom.pinInput.value) || '';
      if (!pendingTransferId) return;
      runAcao(pendingTransferId, 'transferir', pin);
    });
  }
  if (dom.pinCancel) dom.pinCancel.addEventListener('click', closePin);
  if (dom.pinInput) {
    dom.pinInput.addEventListener('keydown', function (ev) {
      if (ev.key === 'Enter') {
        ev.preventDefault();
        if (dom.pinOk) dom.pinOk.click();
      }
    });
  }
  document.addEventListener('keydown', function (ev) {
    if (ev.key === 'Escape' && isOpen()) {
      if (dom.pinBox && !dom.pinBox.classList.contains('hidden')) {
        closePin();
        return;
      }
      closeOverlay();
    }
  });

  refreshResumo();
  pollTimer = setInterval(function () {
    if (document.hidden) return;
    refreshResumo();
  }, 25000);

  window.agroPedirLojaAbrir = openOverlay;
})();
