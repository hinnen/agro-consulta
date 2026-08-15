/**
 * PDV — overlay Pedir loja (pedido de transferência Centro ↔ Vila).
 * Pedido não move estoque. Transferência usa PIN e a API já existente.
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
  var painel = {
    deposito: 'centro',
    deposito_label: 'Centro',
    outra: 'vila',
    outra_label: 'Vila Elias',
    badge: 0,
    recebidos: [],
    enviados: [],
    historico: [],
  };
  var searchTimer = null;
  var pollTimer = null;
  var busy = false;
  var hitList = [];
  var hitSelectionIndex = -1;
  var transferIds = [];

  var dom = {
    btnOpen: document.getElementById('pdv-topbar-pedir-loja-btn'),
    btnCount: document.getElementById('pdv-topbar-pedir-loja-count'),
    fechar: document.getElementById('pdv-pedir-loja-fechar'),
    sub: document.getElementById('pdv-pedir-loja-sub'),
    btnPedidos: document.getElementById('pdv-pedir-loja-btn-pedidos'),
    btnPedidosCount: document.getElementById('pdv-pedir-loja-btn-pedidos-count'),
    btnPedir: document.getElementById('pdv-pedir-loja-btn-pedir'),
    busca: document.getElementById('pdv-pedir-loja-busca'),
    hits: document.getElementById('pdv-pedir-loja-hits'),
    cart: document.getElementById('pdv-pedir-loja-cart'),
    limpar: document.getElementById('pdv-pedir-loja-limpar'),
    enviar: document.getElementById('pdv-pedir-loja-enviar'),
    status: document.getElementById('pdv-pedir-loja-status'),
    recebidos: document.getElementById('pdv-pedir-loja-recebidos'),
    enviados: document.getElementById('pdv-pedir-loja-enviados'),
    historico: document.getElementById('pdv-pedir-loja-historico'),
    inHint: document.getElementById('pdv-pedir-loja-in-hint'),
    pedidosStatus: document.getElementById('pdv-pedir-loja-pedidos-status'),
    aceitarTodos: document.getElementById('pdv-pedir-loja-aceitar-todos'),
    transferirTodos: document.getElementById('pdv-pedir-loja-transferir-todos'),
    pinPop: document.getElementById('pdv-pedir-loja-pin-pop'),
    pin: document.getElementById('pdv-pedir-loja-pin'),
    pinErr: document.getElementById('pdv-pedir-loja-pin-err'),
    pinOk: document.getElementById('pdv-pedir-loja-pin-ok'),
    pinCancel: document.getElementById('pdv-pedir-loja-pin-cancelar'),
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

  function setPedidosStatus(msg, isErr) {
    if (!dom.pedidosStatus) return;
    dom.pedidosStatus.textContent = msg || '';
    dom.pedidosStatus.className =
      'text-sm font-bold ' + (isErr ? 'text-red-700' : 'text-slate-600');
  }

  function fmtQtd(n) {
    var x = Number(n);
    if (!isFinite(x)) return '0';
    if (Math.abs(x - Math.round(x)) < 0.0005) return String(Math.round(x));
    return x.toFixed(3).replace(/\.?0+$/, '');
  }

  function escapeHtml(s) {
    return String(s || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function setView(view) {
    overlay.setAttribute('data-pt-view', view);
    var pedidos = view === 'pedidos';
    if (dom.btnPedidos) dom.btnPedidos.classList.toggle('hidden', pedidos);
    if (dom.btnPedir) {
      dom.btnPedir.classList.toggle('hidden', !pedidos);
      dom.btnPedir.classList.toggle('inline-flex', pedidos);
    }
    var pedirEl = overlay.querySelector('.pt-view-pedir');
    var pedEl = overlay.querySelector('.pt-view-pedidos');
    if (pedirEl) {
      pedirEl.classList.toggle('hidden', pedidos);
      pedirEl.classList.toggle('flex', !pedidos);
    }
    if (pedEl) {
      pedEl.classList.toggle('hidden', !pedidos);
      pedEl.classList.toggle('flex', pedidos);
    }
  }

  function applyPainel(data) {
    if (!data || !data.ok) return;
    painel.deposito = data.deposito === 'vila' ? 'vila' : 'centro';
    painel.deposito_label = data.deposito_label || 'Centro';
    painel.outra = data.outra === 'centro' ? 'centro' : 'vila';
    painel.outra_label = data.outra_label || 'Vila Elias';
    painel.badge = Number(data.badge || 0);
    painel.recebidos = data.recebidos || [];
    painel.enviados = data.enviados || [];
    painel.historico = data.historico || [];
    if (dom.sub) {
      dom.sub.textContent =
        'Este PDV: ' +
        painel.deposito_label +
        ' · pede para ' +
        painel.outra_label;
    }
    updateBadge(painel.badge);
    renderPedidos();
  }

  function updateBadge(n) {
    var count = Number(n || 0);
    function paint(el) {
      if (!el) return;
      if (count > 0) {
        el.textContent = count > 99 ? '99+' : String(count);
        el.classList.remove('hidden');
      } else {
        el.classList.add('hidden');
      }
    }
    paint(dom.btnCount);
    paint(dom.btnPedidosCount);
    if (dom.btnOpen) {
      dom.btnOpen.classList.toggle('pdv-wiz-topbar-btn--pedir-pendente', count > 0);
    }
  }

  function saldoProduto(p, key) {
    var v = p && (p[key] != null ? p[key] : p[key.replace('saldo_', 'estoque_')]);
    var n = Number(v);
    return isFinite(n) ? n : 0;
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
        saldo_centro: saldoProduto(p, 'saldo_centro'),
        saldo_vila: saldoProduto(p, 'saldo_vila'),
      });
    }
    if (dom.busca) dom.busca.value = '';
    hideHits();
    renderCart();
    setStatus(nome + ' adicionado.');
  }

  function hideHits() {
    if (!dom.hits) return;
    dom.hits.innerHTML = '';
    dom.hits.classList.add('hidden');
    hitList = [];
    hitSelectionIndex = -1;
  }

  function positionHits() {
    if (!dom.hits || dom.hits.classList.contains('hidden')) return;
    var wrap = overlay && overlay.querySelector('.pt-busca-wrap');
    var panel = document.getElementById('pdv-pedir-loja-panel');
    if (!wrap || !panel) return;
    var wr = wrap.getBoundingClientRect();
    var pr = panel.getBoundingClientRect();
    dom.hits.style.left = wr.left - pr.left + 'px';
    dom.hits.style.width = wr.width + 'px';
    dom.hits.style.top = wr.bottom - pr.top + 4 + 'px';
  }

  function syncHitSelection() {
    if (!dom.hits) return;
    dom.hits.querySelectorAll('[data-pt-idx]').forEach(function (row) {
      var i = parseInt(row.getAttribute('data-pt-idx'), 10);
      row.classList.toggle('pt-hit-selected', i === hitSelectionIndex);
    });
  }

  function pickHitByIndex(idx) {
    if (idx < 0 || idx >= hitList.length) return;
    addProduct(hitList[idx]);
  }

  function renderHits(lista) {
    if (!dom.hits) return;
    hitList = Array.isArray(lista) ? lista.slice(0, 50) : [];
    if (!hitList.length) {
      hitSelectionIndex = -1;
      dom.hits.innerHTML =
        '<div class="pt-hits-scroll"><p class="text-sm font-semibold text-slate-700 px-2 py-2">Nenhum produto.</p></div>';
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
        var sel = i === hitSelectionIndex ? ' pt-hit-selected' : '';
        return (
          '<tr data-pt-idx="' +
          i +
          '" class="' +
          sel.trim() +
          '">' +
          '<td>' +
          escapeHtml(cod) +
          '</td>' +
          '<td title="' +
          escapeHtml(nome) +
          '">' +
          escapeHtml(nome) +
          '</td>' +
          '<td class="pt-td-stk">' +
          escapeHtml(fmtQtd(saldoProduto(p, 'saldo_centro'))) +
          '</td>' +
          '<td class="pt-td-stk">' +
          escapeHtml(fmtQtd(saldoProduto(p, 'saldo_vila'))) +
          '</td>' +
          '<td style="text-align:center;font-weight:900;color:#0284c7">+</td>' +
          '</tr>'
        );
      })
      .join('');
    dom.hits.innerHTML =
      '<div class="pt-hits-scroll"><table class="pt-hits-table"><colgroup>' +
      '<col class="pt-col-gm" /><col class="pt-col-nome" /><col class="pt-col-stk" />' +
      '<col class="pt-col-stk" /><col class="pt-col-add" /></colgroup>' +
      '<thead><tr><th>Código</th><th>Produto</th><th>C</th><th>V</th><th></th></tr></thead>' +
      '<tbody>' +
      rows +
      '</tbody></table></div>';
    dom.hits.classList.remove('hidden');
    positionHits();
    syncHitSelection();
    dom.hits.querySelectorAll('[data-pt-idx]').forEach(function (row) {
      row.addEventListener('click', function () {
        var i = parseInt(row.getAttribute('data-pt-idx'), 10);
        if (!isNaN(i)) pickHitByIndex(i);
      });
      row.addEventListener('mouseenter', function () {
        var i = parseInt(row.getAttribute('data-pt-idx'), 10);
        if (isNaN(i)) return;
        hitSelectionIndex = i;
        syncHitSelection();
      });
    });
  }

  function onlyDigits(s) {
    return String(s || '').replace(/\D/g, '');
  }

  function pareceBipCodigo(q) {
    var s = String(q || '').trim();
    if (!s) return false;
    var compact = s.replace(/\s/g, '');
    var d = onlyDigits(compact);
    return d.length >= 8 && d === compact;
  }

  function produtoBateCodigo(p, q) {
    if (!p) return false;
    var qt = String(q || '').trim().toLowerCase();
    var qd = onlyDigits(q);
    var ean = String(p.codigo_barras || p.ean || '').trim();
    var eanD = onlyDigits(ean);
    var nfe = String(p.codigo_nfe || p.codigo_gm || p.codigo || p.gm || '')
      .trim()
      .toLowerCase();
    var id = String(p.id || p.produto_id || '').trim();
    if (ean && ean.toLowerCase() === qt) return true;
    if (qd.length >= 8 && eanD && eanD === qd) return true;
    if (nfe && nfe === qt) return true;
    if (id && (id === qt || onlyDigits(id) === qd)) return true;
    return false;
  }

  function escolherHitBip(lista, q, exactFlag, allowSingle) {
    if (!lista || !lista.length) return null;
    if (exactFlag) return lista[0];
    var i;
    for (i = 0; i < lista.length; i++) {
      if (produtoBateCodigo(lista[i], q)) return lista[i];
    }
    if (allowSingle && pareceBipCodigo(q) && lista.length === 1) return lista[0];
    return null;
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
        var fromEnter = !!opts.fromEnter || !!opts.forceAutoAdd;
        if (exact || pareceBipCodigo(query) || fromEnter) {
          var hit = escolherHitBip(
            lista,
            query,
            exact,
            fromEnter || !!opts.forceAutoAdd
          );
          if (hit) {
            addProduct(hit);
            return;
          }
        }
        renderHits(lista);
      })
      .catch(function () {
        if (dom.hits) {
          dom.hits.innerHTML =
            '<div class="pt-hits-scroll"><p class="text-sm font-semibold text-red-600 px-2 py-2">Falha na busca.</p></div>';
          dom.hits.classList.remove('hidden');
          positionHits();
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
    var body = cart
      .map(function (it, idx) {
        return (
          '<tr data-idx="' +
          idx +
          '">' +
          '<td>' +
          escapeHtml(it.codigo || '—') +
          '</td>' +
          '<td title="' +
          escapeHtml(it.nome) +
          '">' +
          escapeHtml(it.nome) +
          '</td>' +
          '<td class="pt-td-stk">' +
          escapeHtml(fmtQtd(it.saldo_centro)) +
          '</td>' +
          '<td class="pt-td-stk">' +
          escapeHtml(fmtQtd(it.saldo_vila)) +
          '</td>' +
          '<td class="pt-td-qtd"><input class="pt-field" type="number" min="0.001" step="1" value="' +
          escapeHtml(fmtQtd(it.quantidade)) +
          '" data-pt-qtd="' +
          idx +
          '" /></td>' +
          '<td style="text-align:center"><button type="button" class="pt-cart-rm" data-pt-rm="' +
          idx +
          '" aria-label="Remover">×</button></td>' +
          '</tr>'
        );
      })
      .join('');
    dom.cart.innerHTML =
      '<table class="pt-cart-table"><colgroup>' +
      '<col class="pt-col-gm" /><col class="pt-col-nome" /><col class="pt-col-stk" />' +
      '<col class="pt-col-stk" /><col class="pt-col-qtd" /><col class="pt-col-rm" /></colgroup>' +
      '<thead><tr><th>Código</th><th>Produto</th><th>C</th><th>V</th><th>Qtd</th><th></th></tr></thead>' +
      '<tbody>' +
      body +
      '</tbody></table>';
    dom.cart.querySelectorAll('[data-pt-qtd]').forEach(function (inp) {
      inp.addEventListener('change', function () {
        var i = parseInt(inp.getAttribute('data-pt-qtd'), 10);
        var n = Number(String(inp.value || '').replace(',', '.'));
        if (!isFinite(n) || n <= 0) n = 1;
        if (cart[i]) cart[i].quantidade = n;
      });
    });
    dom.cart.querySelectorAll('[data-pt-rm]').forEach(function (btn) {
      btn.addEventListener('click', function () {
        var i = parseInt(btn.getAttribute('data-pt-rm'), 10);
        if (!isNaN(i)) cart.splice(i, 1);
        renderCart();
      });
    });
  }

  function rowHtml(it, kind) {
    var acts = '';
    if (kind === 'in') {
      acts =
        '<button type="button" class="pt-act pt-act--ok" data-pt-acao="aceitar" data-pt-id="' +
        it.id +
        '">Aceitar</button>' +
        '<button type="button" class="pt-act pt-act--go" data-pt-acao="transferir" data-pt-id="' +
        it.id +
        '">Transferir</button>' +
        '<button type="button" class="pt-act pt-act--no" data-pt-acao="recusar" data-pt-id="' +
        it.id +
        '">Recusar</button>';
    } else if (kind === 'out') {
      acts =
        '<button type="button" class="pt-act pt-act--no" data-pt-acao="cancelar" data-pt-id="' +
        it.id +
        '">Cancelar</button>';
    }
    return (
      '<div class="pt-row' +
      (kind === 'in' ? ' is-in' : '') +
      '">' +
      '<div class="flex flex-wrap items-center gap-2">' +
      '<div class="min-w-0 flex-1">' +
      '<div class="pt-row-title">' +
      escapeHtml(it.nome || '') +
      '</div>' +
      '<div class="pt-row-meta">' +
      escapeHtml(it.codigo || '—') +
      ' · qtd ' +
      escapeHtml(fmtQtd(it.quantidade)) +
      ' · ' +
      escapeHtml(it.origem_label) +
      ' → ' +
      escapeHtml(it.destino_label) +
      ' · ' +
      escapeHtml(it.status_label) +
      ' · ' +
      escapeHtml(it.criado_em || '') +
      '</div></div>' +
      '<div class="flex flex-wrap gap-1.5">' +
      acts +
      '</div></div></div>'
    );
  }

  function emptyBlock(msg) {
    return (
      '<p class="py-2 text-sm font-semibold text-slate-500">' +
      escapeHtml(msg) +
      '</p>'
    );
  }

  function renderPedidos() {
    if (dom.recebidos) {
      if (!painel.recebidos.length) {
        dom.recebidos.innerHTML = emptyBlock('Nenhum pedido para esta loja.');
      } else {
        dom.recebidos.innerHTML = painel.recebidos
          .map(function (it) {
            return rowHtml(it, 'in');
          })
          .join('');
      }
    }
    if (dom.enviados) {
      if (!painel.enviados.length) {
        dom.enviados.innerHTML = emptyBlock('Você ainda não pediu nada.');
      } else {
        dom.enviados.innerHTML = painel.enviados
          .map(function (it) {
            return rowHtml(it, 'out');
          })
          .join('');
      }
    }
    if (dom.historico) {
      if (!painel.historico.length) {
        dom.historico.innerHTML = emptyBlock('Sem histórico recente.');
      } else {
        dom.historico.innerHTML = painel.historico
          .map(function (it) {
            return rowHtml(it, 'hist');
          })
          .join('');
      }
    }
    if (dom.inHint) {
      dom.inHint.textContent = painel.recebidos.length
        ? painel.recebidos.length + ' em aberto'
        : 'nenhum';
    }
    bindPedidoActions(dom.recebidos);
    bindPedidoActions(dom.enviados);
  }

  function bindPedidoActions(root) {
    if (!root) return;
    root.querySelectorAll('[data-pt-acao]').forEach(function (btn) {
      btn.addEventListener('click', function () {
        var acao = btn.getAttribute('data-pt-acao');
        var id = parseInt(btn.getAttribute('data-pt-id'), 10);
        if (!acao || !id) return;
        if (acao === 'transferir') {
          abrirPin([id]);
          return;
        }
        postStatus(id, acao);
      });
    });
  }

  function jsonFetch(url, body) {
    var opts = { credentials: 'same-origin' };
    if (body) {
      opts.method = 'POST';
      opts.headers = {
        'Content-Type': 'application/json',
        'X-CSRFToken': csrf(),
      };
      opts.body = JSON.stringify(body);
    }
    return fetch(url, opts).then(function (r) {
      return r.json().then(function (j) {
        return { status: r.status, j: j || {} };
      });
    });
  }

  function loadLista() {
    var url = urls.apiPdvPedidoTransferenciaLista || '/api/pdv/pedido-transferencia/';
    return jsonFetch(url).then(function (pack) {
      if (pack.j && pack.j.ok) applyPainel(pack.j);
      return pack.j;
    });
  }

  function loadResumo() {
    var url =
      urls.apiPdvPedidoTransferenciaResumo ||
      '/api/pdv/pedido-transferencia/resumo/';
    return jsonFetch(url)
      .then(function (pack) {
        if (pack.j && pack.j.ok) updateBadge(pack.j.badge);
      })
      .catch(function () {});
  }

  function postStatus(id, acao) {
    if (busy) return;
    busy = true;
    var url = (
      urls.apiPdvPedidoTransferenciaStatusPattern ||
      '/api/pdv/pedido-transferencia/__pk__/status/'
    ).replace('__pk__', String(id));
    jsonFetch(url, { acao: acao })
      .then(function (pack) {
        busy = false;
        if (!pack.j.ok) {
          setPedidosStatus(pack.j.erro || 'Não foi possível atualizar.', true);
          return;
        }
        applyPainel(pack.j);
        setPedidosStatus('Pedido atualizado.');
      })
      .catch(function () {
        busy = false;
        setPedidosStatus('Falha de rede.', true);
      });
  }

  function enviarPedido() {
    if (busy) return;
    if (!cart.length) {
      setStatus('Busque e adicione produtos.', true);
      return;
    }
    busy = true;
    var url =
      urls.apiPdvPedidoTransferenciaCriar ||
      '/api/pdv/pedido-transferencia/criar/';
    jsonFetch(url, {
      itens: cart.map(function (it) {
        return {
          produto_id: it.produto_id,
          nome: it.nome,
          codigo: it.codigo,
          quantidade: it.quantidade,
        };
      }),
    })
      .then(function (pack) {
        busy = false;
        if (!pack.j.ok) {
          setStatus(pack.j.erro || 'Não foi possível enviar.', true);
          return;
        }
        cart = [];
        renderCart();
        applyPainel(pack.j);
        setStatus(pack.j.mensagem || 'Pedido enviado.');
        setView('pedidos');
      })
      .catch(function () {
        busy = false;
        setStatus('Falha de rede ao enviar.', true);
      });
  }

  function abrirPin(ids) {
    transferIds = ids || [];
    if (dom.pinErr) {
      dom.pinErr.textContent = '';
      dom.pinErr.classList.add('hidden');
    }
    if (dom.pin) dom.pin.value = '';
    if (dom.pinPop) dom.pinPop.classList.remove('hidden');
    try {
      if (dom.pin) dom.pin.focus();
    } catch (e) {}
  }

  function fecharPin() {
    transferIds = [];
    if (dom.pinPop) dom.pinPop.classList.add('hidden');
  }

  function confirmarPin() {
    if (busy) return;
    var pin = dom.pin ? String(dom.pin.value || '').trim() : '';
    if (!pin) {
      if (dom.pinErr) {
        dom.pinErr.textContent = 'Informe o PIN.';
        dom.pinErr.classList.remove('hidden');
      }
      return;
    }
    busy = true;
    var url =
      urls.apiPdvPedidoTransferenciaTransferir ||
      '/api/pdv/pedido-transferencia/transferir/';
    jsonFetch(url, { pin: pin, ids: transferIds })
      .then(function (pack) {
        busy = false;
        if (!pack.j.ok) {
          if (dom.pinErr) {
            dom.pinErr.textContent = pack.j.erro || 'PIN ou transferência falhou.';
            dom.pinErr.classList.remove('hidden');
          }
          setPedidosStatus(pack.j.erro || 'Não foi possível transferir.', true);
          return;
        }
        applyPainel(pack.j);
        fecharPin();
        setPedidosStatus(pack.j.mensagem || 'Transferido.');
      })
      .catch(function () {
        busy = false;
        if (dom.pinErr) {
          dom.pinErr.textContent = 'Falha de rede.';
          dom.pinErr.classList.remove('hidden');
        }
      });
  }

  function aceitarTodos() {
    var ids = (painel.recebidos || [])
      .filter(function (it) {
        return it.status === 'PENDENTE';
      })
      .map(function (it) {
        return it.id;
      });
    if (!ids.length) {
      setPedidosStatus('Nenhum pendente para aceitar.');
      return;
    }
    var i = 0;
    function next() {
      if (i >= ids.length) {
        loadLista();
        setPedidosStatus('Pedidos aceitos.');
        return;
      }
      var id = ids[i++];
      var url = (
        urls.apiPdvPedidoTransferenciaStatusPattern ||
        '/api/pdv/pedido-transferencia/__pk__/status/'
      ).replace('__pk__', String(id));
      jsonFetch(url, { acao: 'aceitar' }).then(next).catch(next);
    }
    next();
  }

  function openOverlay() {
    overlay.classList.remove('hidden');
    overlay.classList.add('flex');
    loadLista().then(function () {
      if (painel.badge > 0) setView('pedidos');
      else setView('pedir');
      if (painel.badge > 0) setPedidosStatus('Há pedidos para esta loja.');
    });
    renderCart();
    try {
      if (dom.busca) dom.busca.focus();
    } catch (e) {}
  }

  function closeOverlay() {
    hideHits();
    fecharPin();
    overlay.classList.add('hidden');
    overlay.classList.remove('flex');
  }

  if (dom.btnOpen) {
    dom.btnOpen.addEventListener('click', openOverlay);
  }
  if (dom.fechar) dom.fechar.addEventListener('click', closeOverlay);
  overlay.addEventListener('click', function (ev) {
    if (ev.target === overlay) closeOverlay();
  });
  if (dom.btnPedidos) {
    dom.btnPedidos.addEventListener('click', function () {
      setView('pedidos');
      loadLista();
    });
  }
  if (dom.btnPedir) {
    dom.btnPedir.addEventListener('click', function () {
      setView('pedir');
      try {
        if (dom.busca) dom.busca.focus();
      } catch (e) {}
    });
  }
  if (dom.limpar) {
    dom.limpar.addEventListener('click', function () {
      cart = [];
      renderCart();
      setStatus('Lista limpa.');
    });
  }
  if (dom.enviar) dom.enviar.addEventListener('click', enviarPedido);
  if (dom.aceitarTodos) dom.aceitarTodos.addEventListener('click', aceitarTodos);
  if (dom.transferirTodos) {
    dom.transferirTodos.addEventListener('click', function () {
      var ids = (painel.recebidos || []).map(function (it) {
        return it.id;
      });
      if (!ids.length) {
        setPedidosStatus('Nada para transferir.');
        return;
      }
      abrirPin(ids);
    });
  }
  if (dom.pinOk) dom.pinOk.addEventListener('click', confirmarPin);
  if (dom.pinCancel) dom.pinCancel.addEventListener('click', fecharPin);
  if (dom.pin) {
    dom.pin.addEventListener('keydown', function (ev) {
      if (ev.key === 'Enter') {
        ev.preventDefault();
        confirmarPin();
      }
    });
  }
  if (dom.busca) {
    dom.busca.addEventListener('input', function () {
      var q = dom.busca.value;
      clearTimeout(searchTimer);
      searchTimer = setTimeout(function () {
        buscar(q);
      }, 180);
    });
    dom.busca.addEventListener('keydown', function (ev) {
      if (ev.key === 'Enter') {
        ev.preventDefault();
        if (hitList.length && hitSelectionIndex >= 0) {
          pickHitByIndex(hitSelectionIndex);
          return;
        }
        buscar(dom.busca.value, { fromEnter: true });
      } else if (ev.key === 'ArrowDown') {
        ev.preventDefault();
        if (!hitList.length) return;
        hitSelectionIndex = Math.min(hitList.length - 1, hitSelectionIndex + 1);
        syncHitSelection();
      } else if (ev.key === 'ArrowUp') {
        ev.preventDefault();
        if (!hitList.length) return;
        hitSelectionIndex = Math.max(0, hitSelectionIndex - 1);
        syncHitSelection();
      } else if (ev.key === 'Escape') {
        hideHits();
      }
    });
  }
  document.addEventListener('keydown', function (ev) {
    if (ev.key !== 'Escape') return;
    if (overlay.classList.contains('hidden')) return;
    if (dom.pinPop && !dom.pinPop.classList.contains('hidden')) {
      fecharPin();
      ev.preventDefault();
      return;
    }
    closeOverlay();
  });

  renderCart();
  loadResumo();
  pollTimer = setInterval(loadResumo, 25000);
})();
