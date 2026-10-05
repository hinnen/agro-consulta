/**
 * PDV — overlay Pedir loja (solicitação Centro ↔ Vila).
 * Pedir / Aceitar / Pronto = só status. Transferir = move estoque (só depois de Pronto).
 * PIN: usa o operador já logado no PDV (sem redigitar).
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
  var searchTimer = null;
  var buscaSeq = 0;
  var pollTimer = null;
  var beepTimer = null;
  var pendentesBeep = 0;
  var busy = false;
  var aba = 'pedir';
  var confirmCb = null;

  var dom = {
    btnOpen: document.getElementById('pdv-topbar-pedir-loja-btn'),
    btnCount: document.getElementById('pdv-topbar-pedir-loja-count'),
    fechar: document.getElementById('pdv-pedir-loja-fechar'),
    sub: document.getElementById('pdv-pedir-loja-sub'),
    busca: document.getElementById('pdv-pedir-loja-busca'),
    hits: document.getElementById('pdv-pedir-loja-hits'),
    cart: document.getElementById('pdv-pedir-loja-cart'),
    obs: document.getElementById('pdv-pedir-loja-obs'),
    livre: document.getElementById('pdv-pedir-loja-livre'),
    livreAdd: document.getElementById('pdv-pedir-loja-livre-add'),
    limpar: document.getElementById('pdv-pedir-loja-limpar'),
    enviar: document.getElementById('pdv-pedir-loja-enviar'),
    lista: document.getElementById('pdv-pedir-loja-lista'),
    listaToolbar: document.getElementById('pdv-pedir-loja-lista-toolbar'),
    imprimirTodos: document.getElementById('pdv-pedir-loja-imprimir-todos'),
    aceitarTodos: document.getElementById('pdv-pedir-loja-aceitar-todos'),
    transferirSel: document.getElementById('pdv-pedir-loja-transferir-sel'),
    listaCount: document.getElementById('pdv-pedir-loja-lista-count'),
    status: document.getElementById('pdv-pedir-loja-status'),
    pinAviso: document.getElementById('pdv-pedir-loja-pin-aviso'),
    abrirPin: document.getElementById('pdv-pedir-loja-abrir-pin'),
    badgeRec: document.getElementById('pdv-pedir-loja-badge-rec'),
    confirm: document.getElementById('pdv-pedir-loja-confirm'),
    confirmTitle: document.getElementById('pdv-pedir-loja-confirm-title'),
    confirmBody: document.getElementById('pdv-pedir-loja-confirm-body'),
    confirmExtra: document.getElementById('pdv-pedir-loja-confirm-extra'),
    confirmFurado: document.getElementById('pdv-pedir-loja-confirm-furado'),
    confirmAjustar: document.getElementById('pdv-pedir-loja-confirm-ajustar'),
    confirmAjusteWrap: document.getElementById('pdv-pedir-loja-confirm-ajuste-wrap'),
    confirmQtd: document.getElementById('pdv-pedir-loja-confirm-qtd'),
    confirmSim: document.getElementById('pdv-pedir-loja-confirm-sim'),
    confirmNao: document.getElementById('pdv-pedir-loja-confirm-nao'),
    ajuste: document.getElementById('pdv-pedir-loja-ajuste'),
    ajusteNome: document.getElementById('pdv-pedir-loja-ajuste-nome'),
    ajusteCentro: document.getElementById('pdv-pedir-loja-ajuste-centro'),
    ajusteVila: document.getElementById('pdv-pedir-loja-ajuste-vila'),
    ajusteSim: document.getElementById('pdv-pedir-loja-ajuste-sim'),
    ajusteNao: document.getElementById('pdv-pedir-loja-ajuste-nao'),
    temPedido: document.getElementById('pdv-pedir-loja-tem-pedido'),
    temPedidoMsg: document.getElementById('pdv-pedir-loja-tem-pedido-msg'),
    temPedidoOk: document.getElementById('pdv-pedir-loja-tem-pedido-ok'),
  };
  var ajusteProduto = null;

  function csrf() {
    var c = document.cookie.match(/csrftoken=([^;]+)/);
    return (c && c[1]) || (bootstrap.csrfToken || '');
  }

  function escapeHtml(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function depositoAtual() {
    var d = (bootstrap.pdvDeposito && bootstrap.pdvDeposito.deposito) || 'centro';
    return String(d).toLowerCase() === 'vila' ? 'vila' : 'centro';
  }

  function lojaOutraLabel() {
    return depositoAtual() === 'vila' ? 'Centro' : 'Vila Elias';
  }

  function setStatus(msg, isErr) {
    if (!dom.status) return;
    if (!msg) {
      dom.status.classList.add('hidden');
      dom.status.textContent = '';
      return;
    }
    dom.status.textContent = msg;
    dom.status.classList.remove('hidden');
    dom.status.className =
      'mx-3 mt-1 text-base font-bold ' + (isErr ? 'text-red-700' : 'text-emerald-800');
  }

  function setPinAviso(precisa) {
    if (!dom.pinAviso) return;
    if (precisa) dom.pinAviso.classList.remove('hidden');
    else dom.pinAviso.classList.add('hidden');
  }

  function plBeep() {
    try {
      var Ctx = window.AudioContext || window.webkitAudioContext;
      if (!Ctx) return;
      var ctx = new Ctx();
      var o = ctx.createOscillator();
      var g = ctx.createGain();
      o.connect(g);
      g.connect(ctx.destination);
      o.type = 'square';
      o.frequency.value = 880;
      g.gain.value = 0.08;
      o.start();
      o.stop(ctx.currentTime + 0.18);
      setTimeout(function () {
        var o2 = ctx.createOscillator();
        var g2 = ctx.createGain();
        o2.connect(g2);
        g2.connect(ctx.destination);
        o2.type = 'square';
        o2.frequency.value = 660;
        g2.gain.value = 0.08;
        o2.start();
        o2.stop(ctx.currentTime + 0.22);
      }, 200);
    } catch (e) {}
  }

  function uiPedirSemAlerta() {
    return !!(dom.btnOpen && !dom.btnOpen.classList.contains('pdv-wiz-topbar-btn--pedir-loja-alerta'));
  }

  function syncBeepPendentes(n) {
    pendentesBeep = Number(n || 0);
    if (pendentesBeep > 0) {
      if (!beepTimer) {
        plBeep();
        beepTimer = setInterval(function () {
          /* Defesa: badge sumiu e timer ficou preso (ex. enviar pedido sem syncBeep). */
          if (pendentesBeep <= 0 || uiPedirSemAlerta()) {
            syncBeepPendentes(0);
            return;
          }
          plBeep();
        }, 60000);
      }
    } else if (beepTimer) {
      clearInterval(beepTimer);
      beepTimer = null;
    }
  }

  function applyResumoCounts(d) {
    d = d || {};
    var n = Number(d.recebidos_abertos || 0);
    /* Bip: pendente sempre; aceito/pronto só após 30 min sem transferir (servidor / Postgres). */
    var bip = Number(
      d.recebidos_bip != null
        ? d.recebidos_bip
        : d.recebidos_pendentes != null
          ? d.recebidos_pendentes
          : d.recebidos_abertos || 0
    );
    var pend = Number(
      d.recebidos_pendentes != null ? d.recebidos_pendentes : d.recebidos_abertos || 0
    );
    applyBadge(n, bip);
    syncBeepPendentes(bip);
    return { n: n, pend: pend, bip: bip };
  }

  function syncFuradoUi() {
    if (!dom.confirmAjusteWrap || !dom.confirmFurado) return;
    if (dom.confirmFurado.checked) dom.confirmAjusteWrap.classList.add('is-on');
    else dom.confirmAjusteWrap.classList.remove('is-on');
  }

  function fecharConfirm(ok) {
    if (dom.confirm) {
      dom.confirm.classList.remove('is-open');
      dom.confirm.setAttribute('aria-hidden', 'true');
      try {
        if (window.AgroOverlayStack) window.AgroOverlayStack.setOpen(dom.confirm, false);
      } catch (_) {}
    }
    var cb = confirmCb;
    confirmCb = null;
    if (cb) cb(!!ok);
  }

  function abrirConfirm(opts) {
    opts = opts || {};
    return new Promise(function (resolve) {
      if (!dom.confirm) {
        resolve({ ok: false });
        return;
      }
      confirmCb = function (ok) {
        if (!ok) {
          resolve({ ok: false });
          return;
        }
        var furado = !!(dom.confirmFurado && dom.confirmFurado.checked);
        var ajustar = !!(furado && dom.confirmAjustar && dom.confirmAjustar.checked);
        var qtd = dom.confirmQtd ? String(dom.confirmQtd.value || '0') : '0';
        resolve({ ok: true, estoque_furado: furado, ajustar_estoque: ajustar, ajuste_quantidade: qtd });
      };
      if (dom.confirmTitle) dom.confirmTitle.textContent = opts.title || 'Confirmar';
      if (dom.confirmBody) dom.confirmBody.textContent = opts.body || '';
      if (dom.confirmSim) dom.confirmSim.textContent = opts.confirmLabel || 'Confirmar';
      if (dom.confirmExtra) {
        if (opts.furado) {
          dom.confirmExtra.classList.remove('hidden');
          if (dom.confirmFurado) dom.confirmFurado.checked = false;
          if (dom.confirmAjustar) dom.confirmAjustar.checked = true;
          if (dom.confirmQtd) dom.confirmQtd.value = '0';
          syncFuradoUi();
        } else {
          dom.confirmExtra.classList.add('hidden');
        }
      }
      dom.confirm.classList.add('is-open');
      dom.confirm.setAttribute('aria-hidden', 'false');
      try {
        if (window.AgroOverlayStack) window.AgroOverlayStack.setOpen(dom.confirm, true);
      } catch (_) {}
    });
  }

  function abrirPin() {
    if (typeof window.gmSspinGarantirOperador === 'function') {
      window.gmSspinGarantirOperador(function () {
        refreshResumo({ aposPin: true });
      }, { titulo: 'PIN do PDV' });
      return;
    }
    if (typeof window.gmSspinAbrirEntrada === 'function') window.gmSspinAbrirEntrada();
  }

  function abrir() {
    overlay.classList.remove('hidden');
    overlay.classList.add('flex');
    try {
      if (window.AgroOverlayStack) window.AgroOverlayStack.setOpen(overlay, true);
    } catch (_) {}
    if (dom.sub) {
      dom.sub.textContent = 'Pedindo para ' + lojaOutraLabel();
    }
    setAba(aba || 'pedir');
    refreshResumo();
    if (dom.busca && window.matchMedia && window.matchMedia('(hover: hover)').matches) {
      try {
        dom.busca.focus({ preventScroll: true });
      } catch (e) {}
    }
  }

  function fechar() {
    overlay.classList.add('hidden');
    overlay.classList.remove('flex');
    try {
      if (window.AgroOverlayStack) window.AgroOverlayStack.setOpen(overlay, false);
    } catch (_) {}
    fecharAjuste();
    if (dom.confirm && dom.confirm.classList.contains('is-open')) fecharConfirm(false);
  }

  function setAba(nome) {
    aba = nome || 'pedir';
    overlay.setAttribute('data-pl-aba', aba);
    overlay.querySelectorAll('.pl-tab').forEach(function (btn) {
      btn.classList.toggle('is-on', btn.getAttribute('data-pl-aba') === aba);
    });
    var lista = overlay.querySelector('.pl-view-lista');
    if (lista) {
      if (aba === 'pedir') lista.classList.add('hidden');
      else lista.classList.remove('hidden');
    }
    syncListaToolbar();
    if (aba !== 'pedir') carregarLista(aba);
  }

  function syncListaToolbar() {
    if (!dom.listaToolbar) return;
    var show = aba === 'recebidos' || aba === 'enviados' || aba === 'historico';
    dom.listaToolbar.classList.toggle('is-show', show);
    var rows = (dom.lista && dom.lista._rows) || [];
    var n = rows.length;
    if (dom.imprimirTodos) {
      dom.imprimirTodos.disabled = n === 0;
      dom.imprimirTodos.textContent =
        n > 1 ? 'Imprimir todos (' + n + ')' : n === 1 ? 'Imprimir cupom' : 'Imprimir todos';
    }
    if (dom.listaCount) {
      dom.listaCount.textContent = n
        ? n + ' pedido' + (n === 1 ? '' : 's') + ' nesta lista'
        : '';
    }
    var nPend = 0;
    if (aba === 'recebidos') {
      rows.forEach(function (r) {
        if (r && r.status === 'pendente') nPend += 1;
      });
    }
    if (dom.aceitarTodos) {
      if (nPend > 0) {
        dom.aceitarTodos.classList.remove('hidden');
        dom.aceitarTodos.textContent =
          nPend === 1 ? 'Aceitar 1 pendente' : 'Aceitar todos (' + nPend + ')';
      } else {
        dom.aceitarTodos.classList.add('hidden');
      }
    }
    syncTransferirSelBtn();
  }

  function contarSelecaoLista() {
    var nPedidos = 0;
    var nItens = 0;
    if (!dom.lista || aba !== 'recebidos') return { pedidos: 0, itens: 0 };
    dom.lista.querySelectorAll('.pl-card[data-pl-st="pronto"]').forEach(function (card) {
      var sel = lerSelecaoDoCard(card);
      var comQtd = sel.itens.filter(function (it) {
        return Number(it.quantidade) > 0;
      });
      if (comQtd.length) {
        nPedidos += 1;
        nItens += comQtd.length;
      }
    });
    return { pedidos: nPedidos, itens: nItens };
  }

  function syncTransferirSelBtn() {
    if (!dom.transferirSel) return;
    if (aba !== 'recebidos') {
      dom.transferirSel.classList.add('hidden');
      return;
    }
    var c = contarSelecaoLista();
    if (c.itens <= 0) {
      dom.transferirSel.classList.add('hidden');
      return;
    }
    dom.transferirSel.classList.remove('hidden');
    dom.transferirSel.textContent =
      c.pedidos > 1
        ? 'Transferir selecionados (' + c.itens + ' em ' + c.pedidos + ')'
        : 'Transferir selecionados (' + c.itens + ')';
  }

  function atualizarBtnTransferir(card) {
    if (!card) return;
    var btn = card.querySelector('[data-pl-acao="transferir"]');
    if (!btn) return;
    var sel = lerSelecaoDoCard(card);
    var n = sel.itens.length;
    var total = card.querySelectorAll('.pl-item-row[data-pl-item-id]').length;
    if (!total) {
      btn.textContent = 'Transferir estoque';
      syncTransferirSelBtn();
      return;
    }
    if (n === 0) btn.textContent = 'Transferir (marque □)';
    else if (n < total) btn.textContent = 'Transferir ' + n + ' de ' + total;
    else btn.textContent = 'Transferir estoque';
    syncTransferirSelBtn();
  }

  function marcarChecksDoCard(card, ligado) {
    if (!card) return;
    card.querySelectorAll('.pl-item-check').forEach(function (cb) {
      cb.checked = !!ligado;
      var row = cb.closest('.pl-item-row');
      if (row) {
        row.classList.toggle('is-off', !cb.checked);
        var inp = row.querySelector('.pl-item-qtd');
        if (inp) inp.disabled = !cb.checked;
      }
    });
    atualizarBtnTransferir(card);
  }

  function syncQtdDiff(inp) {
    if (!inp) return;
    var ped = Number(inp.getAttribute('data-pl-pedida') || '');
    var val = Number(inp.value);
    var diff = !isNaN(ped) && !isNaN(val) && val !== ped;
    inp.classList.toggle('is-diff', diff);
  }

  function applyBadge(n, bip) {
    n = Number(n || 0);
    bip = Number(bip != null ? bip : n);
    if (dom.btnOpen) {
      var base = 'pdv-action-btn pdv-wiz-topbar-btn pdv-wiz-topbar-btn--slate relative';
      /* Alerta/bip só com recebidos_bip (Postgres) — Aceitar silencia 30 min em todos os PCs. */
      if (bip > 0) base += ' pdv-wiz-topbar-btn--pedir-loja-alerta';
      dom.btnOpen.className = base;
      dom.btnOpen.title =
        n > 0
          ? bip > 0
            ? n + ' pedido(s) da outra loja'
            : n + ' pedido(s) · bip pausado 30 min após Aceitar'
          : 'Pedir produto da outra loja (Centro ↔ Vila)';
    }
    if (dom.btnCount) {
      if (n > 0) {
        dom.btnCount.textContent = String(n);
        dom.btnCount.classList.remove('hidden');
      } else {
        dom.btnCount.classList.add('hidden');
      }
    }
    if (dom.badgeRec) {
      if (n > 0) {
        dom.badgeRec.textContent = String(n);
        dom.badgeRec.classList.add('is-show');
      } else {
        dom.badgeRec.classList.remove('is-show');
      }
    }
  }

  function fecharTemPedido() {
    if (!dom.temPedido) return;
    dom.temPedido.classList.remove('is-open');
    dom.temPedido.setAttribute('aria-hidden', 'true');
    try {
      if (window.AgroOverlayStack) window.AgroOverlayStack.setOpen(dom.temPedido, false);
    } catch (_) {}
  }

  function abrirTemPedido(n) {
    if (!dom.temPedido) return;
    n = Number(n || 0);
    if (n <= 0) return;
    if (dom.temPedidoMsg) {
      dom.temPedidoMsg.textContent =
        n === 1 ? 'Tem pedido da outra loja.' : 'Tem ' + n + ' pedidos da outra loja.';
    }
    dom.temPedido.classList.add('is-open');
    dom.temPedido.setAttribute('aria-hidden', 'false');
    try {
      if (window.AgroOverlayStack) window.AgroOverlayStack.setOpen(dom.temPedido, true);
    } catch (_) {}
    window.setTimeout(function () {
      try {
        if (dom.temPedidoOk) dom.temPedidoOk.focus();
      } catch (e) {}
    }, 40);
  }

  function refreshResumo(opts) {
    opts = opts || {};
    var url = urls.apiPdvTransfLojaResumo;
    if (!url) return;
    url +=
      (url.indexOf('?') >= 0 ? '&' : '?') + 'loja=' + encodeURIComponent(depositoAtual());
    fetch(url, { credentials: 'same-origin', headers: { Accept: 'application/json' } })
      .then(function (r) {
        return r.json();
      })
      .then(function (d) {
        if (!d || !d.ok) return;
        var counts = applyResumoCounts(d);
        setPinAviso(!!d.precisa_pin);
        if (opts.aposPin && !d.precisa_pin && counts.n > 0) abrirTemPedido(counts.n);
      })
      .catch(function () {});
  }

  function produtoId(p) {
    return String((p && (p.id || p.produto_id || p.produto_externo_id)) || '').trim();
  }

  function numSaldo(p, chave) {
    var v = p && (p[chave] != null ? p[chave] : p[chave === 'saldo_centro' ? 'estoque_centro' : 'estoque_vila']);
    var n = Number(v);
    return isFinite(n) ? n : 0;
  }

  function fmtSaldo(n) {
    var x = Number(n);
    if (!isFinite(x)) return '—';
    if (Math.abs(x - Math.round(x)) < 0.001) return String(Math.round(x));
    return String(Math.round(x * 100) / 100).replace('.', ',');
  }

  function hitsHint(msg) {
    if (!dom.hits) return;
    dom.hits.innerHTML =
      '<tr class="pl-hint-row"><td colspan="5" class="pl-hint">' + escapeHtml(msg) + '</td></tr>';
  }

  function fecharAjuste() {
    if (dom.ajuste) {
      dom.ajuste.classList.remove('is-open');
      dom.ajuste.setAttribute('aria-hidden', 'true');
      try {
        if (window.AgroOverlayStack) window.AgroOverlayStack.setOpen(dom.ajuste, false);
      } catch (_) {}
    }
    ajusteProduto = null;
  }

  function abrirAjuste(p) {
    if (!dom.ajuste || !p) return;
    ajusteProduto = p;
    var nome = p.nome || p.nome_produto || 'Produto';
    var gm = codigoGm(p);
    if (dom.ajusteNome) {
      dom.ajusteNome.textContent =
        nome + (gm ? ' · GM ' + gm : '') + ' · digite o saldo real de cada loja';
    }
    if (dom.ajusteCentro) dom.ajusteCentro.value = String(numSaldo(p, 'saldo_centro'));
    if (dom.ajusteVila) dom.ajusteVila.value = String(numSaldo(p, 'saldo_vila'));
    dom.ajuste.classList.add('is-open');
    dom.ajuste.setAttribute('aria-hidden', 'false');
    try {
      if (window.AgroOverlayStack) window.AgroOverlayStack.setOpen(dom.ajuste, true);
    } catch (_) {}
    window.setTimeout(function () {
      try {
        if (dom.ajusteCentro) {
          dom.ajusteCentro.focus();
          dom.ajusteCentro.select();
        }
      } catch (e) {}
    }, 30);
  }

  function salvarAjuste() {
    if (!ajusteProduto || busy) return;
    var url = urls.apiPdvTransfLojaAjustar;
    if (!url) {
      setStatus('URL de ajuste indisponível.', true);
      return;
    }
    var pid = produtoId(ajusteProduto);
    if (!pid) {
      setStatus('Produto inválido.', true);
      return;
    }
    busy = true;
    if (dom.ajusteSim) dom.ajusteSim.disabled = true;
    setStatus('Ajustando estoque…');
    fetch(url, {
      method: 'POST',
      credentials: 'same-origin',
      headers: {
        'Content-Type': 'application/json',
        'X-CSRFToken': csrf(),
        Accept: 'application/json',
      },
      body: JSON.stringify({
        produto_id: pid,
        nome: ajusteProduto.nome || ajusteProduto.nome_produto || '',
        codigo_interno: codigoGm(ajusteProduto),
        saldo_centro: dom.ajusteCentro ? String(dom.ajusteCentro.value || '0') : '0',
        saldo_vila: dom.ajusteVila ? String(dom.ajusteVila.value || '0') : '0',
      }),
    })
      .then(function (r) {
        return r.json().then(function (d) {
          return { ok: r.ok, data: d };
        });
      })
      .then(function (res) {
        busy = false;
        if (dom.ajusteSim) dom.ajusteSim.disabled = false;
        if (res.data && res.data.precisa_pin) {
          setPinAviso(true);
          setStatus(res.data.erro || 'Entre com o PIN.', true);
          return;
        }
        if (!res.ok || !res.data || !res.data.ok) {
          setStatus((res.data && res.data.erro) || 'Não ajustou.', true);
          return;
        }
        if (res.data.saldo_centro != null) ajusteProduto.saldo_centro = res.data.saldo_centro;
        if (res.data.saldo_vila != null) ajusteProduto.saldo_vila = res.data.saldo_vila;
        fecharAjuste();
        setStatus(res.data.mensagem || 'Estoque ajustado.');
        if (dom.busca && String(dom.busca.value || '').trim().length >= 2) {
          buscar(dom.busca.value);
        }
      })
      .catch(function () {
        busy = false;
        if (dom.ajusteSim) dom.ajusteSim.disabled = false;
        setStatus('Erro de rede ao ajustar.', true);
      });
  }

  function codigoGm(p) {
    return String((p && (p.codigo_interno || p.codigo || p.gm || '')) || '').trim();
  }

  function aplicarSaldos(lista, mapa) {
    if (!mapa) return lista;
    (lista || []).forEach(function (p) {
      var s = mapa[produtoId(p)];
      if (!s) return;
      if (s.saldo_centro != null) p.saldo_centro = s.saldo_centro;
      if (s.saldo_vila != null) p.saldo_vila = s.saldo_vila;
    });
    return lista;
  }

  function addCart(p) {
    var id = produtoId(p);
    if (!id) return;
    var achou = cart.filter(function (x) {
      return x.id === id;
    })[0];
    if (achou) {
      achou.qtd += 1;
    } else {
      cart.push({
        id: id,
        nome: p.nome || p.nome_produto || 'Produto',
        codigo: p.codigo_interno || p.codigo || '',
        qtd: 1,
        livre: false,
        saldo_centro: numSaldo(p, 'saldo_centro'),
        saldo_vila: numSaldo(p, 'saldo_vila'),
      });
    }
    renderCart();
    hitsHint('Digite o nome, GM ou código.');
    if (dom.busca) dom.busca.value = '';
  }

  function slugLivre(texto) {
    return String(texto || '')
      .toLowerCase()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '')
      .slice(0, 40) || 'txt';
  }

  function addCartLivre(silent) {
    var texto = dom.livre ? String(dom.livre.value || '').trim() : '';
    if (!texto) {
      if (!silent) {
        setStatus('Escreva o que quer pedir (ex.: sacola, café).', true);
        if (dom.livre) {
          try {
            dom.livre.focus();
          } catch (e) {}
        }
      }
      return false;
    }
    var id = 'livre:' + slugLivre(texto);
    var achou = cart.filter(function (x) {
      return x.id === id;
    })[0];
    if (achou) {
      achou.qtd += 1;
    } else {
      cart.push({
        id: id,
        nome: texto,
        codigo: 'ESCRITO',
        qtd: 1,
        livre: true,
        saldo_centro: null,
        saldo_vila: null,
      });
    }
    if (dom.livre) dom.livre.value = '';
    if (!silent) setStatus('');
    renderCart();
    if (!silent && dom.livre) {
      try {
        dom.livre.focus();
      } catch (e) {}
    }
    return true;
  }

  function garantirItensAntesDeEnviar() {
    var texto = dom.livre ? String(dom.livre.value || '').trim() : '';
    if (texto) addCartLivre(true);
    if (cart.length) return true;
    var obs = dom.obs ? String(dom.obs.value || '').trim() : '';
    if (obs) {
      if (dom.livre) dom.livre.value = obs;
      addCartLivre(true);
      return cart.length > 0;
    }
    return false;
  }

  function renderCart() {
    if (!dom.cart) return;
    if (!cart.length) {
      dom.cart.innerHTML =
        '<p class="px-1 py-2 text-sm font-bold text-slate-500">Busque à esquerda ou escreva embaixo e Enviar.</p>';
      return;
    }
    var outra = depositoAtual() === 'vila' ? 'saldo_centro' : 'saldo_vila';
    var outraLbl = depositoAtual() === 'vila' ? 'Saldo Centro' : 'Saldo Vila';
    dom.cart.innerHTML = cart
      .map(function (it, idx) {
        var saldos =
          it.livre
            ? '<span class="pl-livre-badge">Pedido escrito</span>'
            : '<div class="pl-saldos mt-2">' +
              '<div class="pl-saldo-pill"><small>' +
              escapeHtml(outraLbl) +
              '</small><b>' +
              escapeHtml(fmtSaldo(it[outra])) +
              '</b></div></div>';
        return (
          '<div class="pl-card">' +
          '<div class="pl-card-top">' +
          '<p class="pl-name">' +
          escapeHtml(it.nome) +
          '</p>' +
          '<button type="button" class="pl-rm" data-pl-rm="' +
          idx +
          '" aria-label="Tirar da lista">×</button>' +
          '</div>' +
          saldos +
          '<div class="pl-qty-row">' +
          '<button type="button" class="pl-qty-btn" data-pl-q="-1" data-i="' +
          idx +
          '" aria-label="Menos">−</button>' +
          '<span class="pl-qty">' +
          escapeHtml(String(it.qtd)) +
          '</span>' +
          '<button type="button" class="pl-qty-btn" data-pl-q="1" data-i="' +
          idx +
          '" aria-label="Mais">+</button>' +
          '</div></div>'
        );
      })
      .join('');
  }

  function buscar(q) {
    q = String(q || '').trim();
    if (q.length < 2) {
      buscaSeq += 1;
      hitsHint('Digite o nome, GM ou código.');
      return;
    }
    var seq = ++buscaSeq;
    var base = urls.apiBuscarProdutos || '/api/buscar/';
    fetch(base + '?wizard=1&q=' + encodeURIComponent(q), {
      credentials: 'same-origin',
      headers: { Accept: 'application/json' },
    })
      .then(function (r) {
        return r.json();
      })
      .then(function (d) {
        var lista = (d && (d.produtos || d.itens || d.results)) || [];
        if (!Array.isArray(lista)) lista = [];
        lista = lista.slice(0, 12);
        var ids = lista.map(produtoId).filter(Boolean);
        var saldosUrl = urls.apiPdvTransfLojaSaldos;
        if (!saldosUrl || !ids.length) return { lista: lista };
        return fetch(saldosUrl + '?ids=' + encodeURIComponent(ids.join(',')), {
          credentials: 'same-origin',
          headers: { Accept: 'application/json' },
        })
          .then(function (r) {
            return r.json();
          })
          .then(function (s) {
            return { lista: aplicarSaldos(lista, s && s.saldos) };
          })
          .catch(function () {
            return { lista: lista };
          });
      })
      .then(function (pack) {
        if (seq !== buscaSeq) return;
        if (!dom.hits || !pack) return;
        var lista = pack.lista || [];
        if (!lista.length) {
          hitsHint('Nenhum produto.');
          return;
        }
        dom.hits.innerHTML = lista
          .map(function (p) {
            var id = produtoId(p);
            return (
              '<tr class="pl-hit" data-pl-add="' +
              escapeHtml(id) +
              '">' +
              '<td class="pl-td-nome">' +
              escapeHtml(p.nome || '') +
              '</td>' +
              '<td class="pl-td-gm">' +
              escapeHtml(codigoGm(p) || '—') +
              '</td>' +
              '<td class="pl-td-n">' +
              escapeHtml(fmtSaldo(numSaldo(p, 'saldo_centro'))) +
              '</td>' +
              '<td class="pl-td-n">' +
              escapeHtml(fmtSaldo(numSaldo(p, 'saldo_vila'))) +
              '</td>' +
              '<td class="pl-td-aj">' +
              '<button type="button" class="pl-btn-aj" data-pl-aj="' +
              escapeHtml(id) +
              '" title="Ajustar estoque">Ajustar</button>' +
              '</td></tr>'
            );
          })
          .join('');
        dom.hits._hits = lista;
      })
      .catch(function () {
        hitsHint('Erro na busca.');
      });
  }

  function enviarPedido() {
    if (busy) return;
    if (!garantirItensAntesDeEnviar()) {
      setStatus('Escreva o pedido embaixo ou inclua um produto.', true);
      if (dom.livre) {
        try {
          dom.livre.focus();
        } catch (e) {}
      }
      return;
    }
    var doEnviar = function () {
      enviarPedidoExec();
    };
    if (typeof window.gmSspinGarantirOperador === 'function') {
      window.gmSspinGarantirOperador(doEnviar, { titulo: 'PIN para Pedir loja' });
    } else {
      doEnviar();
    }
  }

  function enviarPedidoExec() {
    if (busy) return;
    var url = urls.apiPdvTransfLojaCriar;
    if (!url) return;
    busy = true;
    setStatus('Enviando…');
    fetch(url, {
      method: 'POST',
      credentials: 'same-origin',
      headers: {
        'Content-Type': 'application/json',
        'X-CSRFToken': csrf(),
        Accept: 'application/json',
      },
      body: JSON.stringify({
        loja: depositoAtual(),
        observacao: (dom.obs && dom.obs.value) || '',
        itens: cart.map(function (it) {
          return {
            produto_id: it.id,
            nome: it.nome,
            codigo_interno: it.codigo,
            quantidade: it.qtd,
            livre: !!it.livre,
          };
        }),
      }),
    })
      .then(function (r) {
        return r.json().then(function (d) {
          return { ok: r.ok, data: d };
        }).catch(function () {
          return {
            ok: false,
            data: {
              erro:
                r.status === 500
                  ? 'Erro no servidor (rode migrate no PC se acabou de atualizar).'
                  : 'Resposta inválida do servidor.',
            },
          };
        });
      })
      .then(function (res) {
        busy = false;
        if (res.data && res.data.precisa_pin) {
          setPinAviso(true);
          setStatus(res.data.erro || 'Entre com o PIN.', true);
          if (typeof window.gmSspinGarantirOperador === 'function') {
            window.gmSspinGarantirOperador(function () {
              enviarPedidoExec();
            }, { titulo: 'PIN para Pedir loja' });
          }
          return;
        }
        if (!res.ok || !res.data || !res.data.ok) {
          setStatus((res.data && res.data.erro) || 'Não enviou.', true);
          return;
        }
        cart = [];
        if (dom.obs) dom.obs.value = '';
        renderCart();
        applyResumoCounts(res.data);
        setStatus(res.data.mensagem || 'Pedido enviado.');
        setAba('enviados');
      })
      .catch(function () {
        busy = false;
        setStatus('Erro de rede.', true);
      });
  }

  function acoesHtml(row) {
    var st = row.status;
    var btns = [];
    if (aba === 'recebidos' && (st === 'pendente' || st === 'aceito' || st === 'pronto')) {
      btns.push('<button type="button" class="pl-btn pl-btn--print" data-pl-acao="imprimir">Imprimir cupom</button>');
      btns.push(
        '<button type="button" class="pl-btn pl-btn--etq" data-pl-acao="etiquetas" title="Lista compacta na térmica 53×30 (separar estoque)">Etiquetas 53</button>'
      );
    }
    if (aba === 'enviados' && (st === 'pendente' || st === 'aceito' || st === 'pronto')) {
      btns.push('<button type="button" class="pl-btn pl-btn--print" data-pl-acao="imprimir">Imprimir cupom</button>');
      btns.push(
        '<button type="button" class="pl-btn pl-btn--etq" data-pl-acao="etiquetas" title="Lista compacta na térmica 53×30 (separar estoque)">Etiquetas 53</button>'
      );
    }
    if (aba === 'recebidos' && st === 'pendente') {
      btns.push('<button type="button" class="pl-btn pl-btn--ok" data-pl-acao="aceitar">Aceitar</button>');
    }
    if (aba === 'recebidos' && st === 'aceito') {
      btns.push('<button type="button" class="pl-btn pl-btn--ok" data-pl-acao="pronto">Pronto</button>');
    }
    /* Transferir só depois de Pronto (não pula o passo). */
    if (aba === 'recebidos' && st === 'pronto') {
      btns.push('<button type="button" class="pl-btn pl-btn--transf" data-pl-acao="transferir">Transferir estoque</button>');
    }
    if (st === 'pendente' || st === 'aceito' || st === 'pronto') {
      btns.push('<button type="button" class="pl-btn pl-btn--danger" data-pl-acao="cancelar">Cancelar</button>');
    }
    return btns.join('');
  }

  function podeEditarQtd(row) {
    return (
      aba === 'recebidos' &&
      (row.status === 'aceito' || row.status === 'pronto')
    );
  }

  function itensHtml(row) {
    var itens = row.itens || [];
    if (!itens.length) return '';
    var edit = podeEditarQtd(row);
    var lines = itens
      .map(function (it) {
        var pedida =
          it.quantidade_pedida != null ? it.quantidade_pedida : it.quantidade;
        var qtdAtual = edit ? pedida : it.quantidade;
        var gm = it.codigo_interno || '';
        var livre = !!it.livre || String(it.produto_id || '').indexOf('livre:') === 0;
        var pedHint = '';
        if (livre) {
          pedHint = '<p class="pl-item-ped">Pedido escrito (sem estoque)</p>';
        } else if (edit) {
          pedHint =
            '<p class="pl-item-ped">Pedido: ' + escapeHtml(fmtSaldo(pedida)) + '</p>';
        } else if (
          Number(it.quantidade_pedida) > 0 &&
          Number(it.quantidade) === 0
        ) {
          pedHint =
            '<p class="pl-item-ped pl-item-ped--zero">Pedido ' +
            escapeHtml(fmtSaldo(it.quantidade_pedida)) +
            ' · <strong>NÃO ENVIADO (0)</strong></p>';
        } else if (
          Number(it.quantidade_pedida) > 0 &&
          Number(it.quantidade_pedida) !== Number(it.quantidade)
        ) {
          pedHint =
            '<p class="pl-item-ped">Pedido ' +
            escapeHtml(fmtSaldo(it.quantidade_pedida)) +
            ' · enviado ' +
            escapeHtml(fmtSaldo(it.quantidade)) +
            '</p>';
        } else if (!edit) {
          pedHint =
            '<p class="pl-item-ped">Qtd ' + escapeHtml(fmtSaldo(qtdAtual)) + '</p>';
        }
        var qtdCell = edit
          ? '<input type="number" class="pl-item-qtd" min="0" step="0.001" data-pl-item-id="' +
            escapeHtml(String(it.id)) +
            '" data-pl-pedida="' +
            escapeHtml(String(pedida)) +
            '" value="' +
            escapeHtml(String(qtdAtual)) +
            '" aria-label="Quantidade a enviar" />'
          : '<span class="pl-item-qtd-ro">' + escapeHtml(fmtSaldo(qtdAtual)) + '</span>';
        var checkCell = edit
          ? '<input type="checkbox" class="pl-item-check" checked data-pl-item-id="' +
            escapeHtml(String(it.id)) +
            '" title="Marcado = envia agora · desmarcado = fica pra depois" aria-label="Enviar este produto agora" />'
          : '';
        return (
          '<div class="pl-item-row' +
          (edit ? '' : ' pl-item-row--ro') +
          '" data-pl-item-id="' +
          escapeHtml(String(it.id)) +
          '">' +
          checkCell +
          '<div class="pl-item-meta" data-pl-toggle-check="1">' +
          '<p class="pl-item-nome">' +
          escapeHtml(it.nome || '') +
          (livre ? ' <span class="pl-livre-badge">Escrito</span>' : '') +
          '</p>' +
          (!livre && gm ? '<p class="pl-item-gm">GM ' + escapeHtml(gm) + '</p>' : '') +
          pedHint +
          '</div>' +
          qtdCell +
          '</div>'
        );
      })
      .join('');
    var selBar = '';
    if (edit && itens.length > 1) {
      selBar =
        '<div class="pl-sel-bar">' +
        '<button type="button" class="pl-btn pl-btn--ghost" data-pl-sel="todos">Marcar todos</button>' +
        '<button type="button" class="pl-btn pl-btn--ghost" data-pl-sel="nenhum">Só depois</button>' +
        '</div>';
    }
    return (
      '<div class="pl-itens">' +
      (edit
        ? '<p class="m-0 text-[10px] font-black uppercase tracking-wide text-orange-800">□ = envia agora · qtd 0 = não enviou · desmarque = fica pra depois</p>'
        : '') +
      selBar +
      lines +
      '</div>'
    );
  }

  function lerSelecaoDoCard(card) {
    if (!card) return { itens: [], adiar_itens: [] };
    var itens = [];
    var adiar = [];
    var rows = card.querySelectorAll('.pl-item-row[data-pl-item-id]');
    if (!rows.length) {
      /* legado: só inputs de qtd */
      card.querySelectorAll('.pl-item-qtd[data-pl-item-id]').forEach(function (inp) {
        itens.push({
          id: Number(inp.getAttribute('data-pl-item-id')),
          quantidade: String(inp.value || '0'),
        });
      });
      return { itens: itens, adiar_itens: adiar };
    }
    rows.forEach(function (row) {
      var id = Number(row.getAttribute('data-pl-item-id'));
      var cb = row.querySelector('.pl-item-check');
      var inp = row.querySelector('.pl-item-qtd');
      if (cb && !cb.checked) {
        adiar.push(id);
        return;
      }
      itens.push({
        id: id,
        quantidade: String(inp ? inp.value || '0' : '0'),
      });
    });
    return { itens: itens, adiar_itens: adiar };
  }

  function lerQtdsDoCard(card) {
    return lerSelecaoDoCard(card).itens;
  }

  function abrirPrintIframe(html, titulo, erroMsg) {
    var iframe = document.getElementById('pdv-pedir-loja-print-iframe');
    if (!iframe) {
      iframe = document.createElement('iframe');
      iframe.id = 'pdv-pedir-loja-print-iframe';
      iframe.setAttribute('title', titulo || 'Impressão Pedir loja');
      iframe.style.cssText =
        'position:fixed;right:0;bottom:0;width:0;height:0;border:0;opacity:0;pointer-events:none';
      document.body.appendChild(iframe);
    }
    var idoc = iframe.contentDocument || (iframe.contentWindow && iframe.contentWindow.document);
    if (!idoc) {
      setStatus('Não abriu a impressão.', true);
      return;
    }
    idoc.open();
    idoc.write(html);
    idoc.close();
    window.setTimeout(function () {
      try {
        iframe.contentWindow.focus();
        iframe.contentWindow.print();
      } catch (e) {
        setStatus(erroMsg || 'Não imprimiu.', true);
      }
    }, 120);
  }

  function imprimirCupomSeparacao(row) {
    if (!row) return;
    abrirPrintIframe(
      montarHtmlCupomPedidos([row]),
      'Cupom separação',
      'Não imprimiu. Confira a térmica 80mm.'
    );
  }

  function montarHtmlCupomPedidos(rows) {
    var dh = new Date().toLocaleString('pt-BR');
    var blocos = '';
    (rows || []).forEach(function (row, idx) {
      if (!row) return;
      var itens = row.itens || [];
      var bodyItens = '';
      itens.forEach(function (it) {
        var livre = !!it.livre || String(it.produto_id || '').indexOf('livre:') === 0;
        var ped =
          it.quantidade_pedida != null && Number(it.quantidade_pedida) > 0
            ? it.quantidade_pedida
            : it.quantidade;
        var env = it.quantidade;
        var qLine =
          row.status === 'concluido' && Number(ped) !== Number(env)
            ? 'PEDIDO ' +
              escapeHtml(fmtSaldo(ped)) +
              ' · ENVIADO ' +
              escapeHtml(fmtSaldo(env)) +
              (Number(env) === 0 ? ' (NÃO ENVIADO)' : '')
            : 'QTD ' + escapeHtml(fmtSaldo(ped));
        bodyItens +=
          '<div style="border-top:1px dashed #000;margin-top:6px;padding-top:4px;">' +
          (livre
            ? '<div style="font-weight:900;font-size:11px;">PEDIDO ESCRITO</div>'
            : it.codigo_interno
              ? '<div><b>GM</b> ' + escapeHtml(it.codigo_interno) + '</div>'
              : '') +
          '<div style="font-weight:bold;font-size:13px;">' +
          escapeHtml(it.nome || '') +
          '</div>' +
          '<div style="font-size:18px;font-weight:900;margin:4px 0;">' +
          qLine +
          '</div>' +
          '</div>';
      });
      if (idx > 0) {
        blocos += '<div style="border-top:3px double #000;margin:12px 0 8px;"></div>';
      }
      blocos +=
        '<div style="text-align:center;font-weight:900;font-size:14px;">SEPARAÇÃO</div>' +
        '<div style="text-align:center;font-weight:900;font-size:12px;margin-top:2px;">PEDIR LOJA #' +
        escapeHtml(String(row.id || '')) +
        '</div>' +
        '<div style="margin-top:6px;">' +
        escapeHtml(dh) +
        '</div>' +
        '<div style="margin-top:4px;font-weight:900;font-size:13px;">' +
        escapeHtml(row.loja_origem_label || '') +
        ' → ' +
        escapeHtml(row.loja_destino_label || '') +
        '</div>' +
        (row.criado_por
          ? '<div style="margin-top:2px;"><b>Pediu</b> ' + escapeHtml(row.criado_por) + '</div>'
          : '') +
        (row.observacao
          ? '<div style="margin-top:4px;"><b>Obs</b> ' + escapeHtml(row.observacao) + '</div>'
          : '') +
        '<div style="border-top:2px solid #000;margin:8px 0 4px;"></div>' +
        bodyItens;
    });
    return (
      '<!DOCTYPE html><html><head><meta charset="utf-8"><title>Separação Pedir loja</title>' +
      '<style>@page{margin:0;size:80mm auto}html,body{margin:0;padding:0;width:80mm}' +
      'body{font-family:"Courier New",Courier,monospace;color:#000;background:#fff}' +
      '.pg{width:80mm;box-sizing:border-box;padding:4mm 3mm;font-size:11px;line-height:1.35}</style></head><body>' +
      '<div class="pg">' +
      (rows && rows.length > 1
        ? '<div style="text-align:center;font-weight:900;font-size:13px;margin-bottom:8px;">TODOS · ' +
          escapeHtml(String(rows.length)) +
          ' PEDIDO(S)</div>'
        : '') +
      blocos +
      '<div style="margin-top:10px;text-align:center;font-size:10px;font-weight:900;">Conferir e transferir no PDV</div>' +
      '</div></body></html>'
    );
  }

  function imprimirTodosCupons() {
    var rows = (dom.lista && dom.lista._rows) || [];
    if (!rows.length) {
      setStatus('Nada pra imprimir nesta lista.', true);
      return;
    }
    abrirPrintIframe(
      montarHtmlCupomPedidos(rows),
      'Cupom todos',
      'Não imprimiu. Confira a térmica 80mm.'
    );
    setStatus('Imprimindo ' + rows.length + ' pedido(s) em um cupom.');
  }

  /**
   * Lista compacta na bobina 53×30 — gambiarra sem cupom 80mm.
   * Várias linhas por etiqueta pra gastar o mínimo de papel.
   */
  function imprimirEtiquetasSeparacao53(row) {
    if (!row) return;
    var itens = row.itens || [];
    if (!itens.length) {
      setStatus('Pedido sem itens pra etiqueta.', true);
      return;
    }
    var LINHAS_POR_ETQ = 6;
    var rota =
      String(row.loja_origem_label || '').replace(/\s+/g, ' ').trim() +
      '→' +
      String(row.loja_destino_label || '').replace(/\s+/g, ' ').trim();
    var linhas = [];
    itens.forEach(function (it) {
      var livre = !!it.livre || String(it.produto_id || '').indexOf('livre:') === 0;
      var q =
        it.quantidade_pedida != null && Number(it.quantidade_pedida) > 0
          ? it.quantidade_pedida
          : it.quantidade;
      var gm = String(it.codigo_interno || '').trim();
      var nome = String(it.nome || '')
        .replace(/\s+/g, ' ')
        .trim();
      if (nome.length > 34) nome = nome.slice(0, 33) + '…';
      var left = livre ? 'ESC' : gm ? gm : '—';
      linhas.push({
        q: fmtSaldo(q),
        left: left,
        nome: nome || (livre ? 'pedido escrito' : ''),
      });
    });
    var pages = [];
    for (var i = 0; i < linhas.length; i += LINHAS_POR_ETQ) {
      pages.push(linhas.slice(i, i + LINHAS_POR_ETQ));
    }
    var totalPg = pages.length;
    var body = pages
      .map(function (chunk, idx) {
        var head =
          '#' +
          String(row.id || '') +
          ' ' +
          rota +
          (totalPg > 1 ? ' ·' + (idx + 1) + '/' + totalPg : '');
        var rowsHtml = chunk
          .map(function (ln) {
            return (
              '<div class="ln"><b class="q">x' +
              escapeHtml(ln.q) +
              '</b> <span class="gm">' +
              escapeHtml(ln.left) +
              '</span> ' +
              escapeHtml(ln.nome) +
              '</div>'
            );
          })
          .join('');
        return (
          '<div class="pg"><div class="hd">' +
          escapeHtml(head) +
          '</div>' +
          rowsHtml +
          '</div>'
        );
      })
      .join('');
    var html =
      '<!DOCTYPE html><html><head><meta charset="utf-8"><title>Etiquetas Pedir loja</title>' +
      '<style>@page{margin:0;size:53mm 30mm}' +
      'html,body{margin:0;padding:0;width:53mm;background:#fff}' +
      'body{font-family:Arial,Helvetica,sans-serif;color:#000;-webkit-print-color-adjust:exact;print-color-adjust:exact}' +
      '.pg{display:block;width:53mm;height:30mm;box-sizing:border-box;padding:1.2mm 1.4mm;overflow:hidden;' +
      'page-break-inside:avoid;break-inside:avoid-page}' +
      '.pg + .pg{page-break-before:always;break-before:page}' +
      '.hd{font-size:7.5pt;font-weight:900;line-height:1.1;margin:0 0 0.6mm;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;' +
      'border-bottom:0.3mm solid #000;padding-bottom:0.4mm}' +
      '.ln{font-size:7pt;line-height:1.15;margin:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}' +
      '.q{font-weight:900}.gm{font-weight:800}</style></head><body>' +
      body +
      '</body></html>';
    abrirPrintIframe(html, 'Etiquetas 53×30', 'Não imprimiu. Escolha a térmica 53×30.');
    setStatus(
      totalPg === 1
        ? '1 etiqueta 53×30 · ' + linhas.length + ' produto(s).'
        : totalPg + ' etiquetas 53×30 · ' + linhas.length + ' produto(s).'
    );
  }

  function renderLista(itens) {
    if (!dom.lista) return;
    if (!itens || !itens.length) {
      dom.lista.innerHTML =
        '<p class="py-10 text-center text-base font-bold text-slate-500">Nada aqui agora.</p>';
      dom.lista._rows = [];
      syncListaToolbar();
      return;
    }
    dom.lista._rows = itens;
    var labels = {
      pendente: 'Pendente',
      aceito: 'Aceito',
      pronto: 'Pronto',
      concluido: 'Concluído',
      cancelado: 'Cancelado',
    };
    var html = '';
    var lastSt = null;
    var agrupa = aba === 'recebidos' || aba === 'enviados';
    itens.forEach(function (row) {
      var st = String(row.status || '');
      if (agrupa && st !== lastSt) {
        html +=
          '<p class="pl-sec">' +
          escapeHtml(labels[st] || row.status_label || st) +
          '</p>';
        lastSt = st;
      }
      var badges = '';
      if (row.eh_resto) {
        badges += '<span class="pl-badge-resto">RESTANTE</span>';
      }
      if (row.parcialmente_enviado) {
        badges += '<span class="pl-badge-parcial">ENVIO PARCIAL</span>';
      }
      html +=
        '<article class="pl-card" data-pl-id="' +
        escapeHtml(String(row.id)) +
        '" data-pl-st="' +
        escapeHtml(st) +
        '">' +
        '<p class="pl-st pl-st--' +
        escapeHtml(st) +
        '">' +
        escapeHtml(row.status_label || row.status) +
        ' · #' +
        escapeHtml(String(row.id)) +
        badges +
        '</p>' +
        '<p class="mt-1 text-sm font-bold text-slate-500">' +
        escapeHtml(row.loja_origem_label) +
        ' → ' +
        escapeHtml(row.loja_destino_label) +
        (row.criado_por ? ' · ' + escapeHtml(row.criado_por) : '') +
        '</p>' +
        (row.observacao
          ? '<p class="mt-1 rounded-lg border border-orange-200 bg-orange-50 px-2 py-1.5 text-sm font-bold text-orange-950">💬 ' +
            escapeHtml(row.observacao) +
            '</p>'
          : '') +
        itensHtml(row) +
        '<div class="pl-actions">' +
        acoesHtml(row) +
        '</div></article>';
    });
    dom.lista.innerHTML = html;
    syncListaToolbar();
  }

  function carregarLista(qual) {
    var url = urls.apiPdvTransfLojaLista;
    if (!url || !dom.lista) return;
    dom.lista.innerHTML = '<p class="py-6 text-center text-sm font-bold text-slate-500">Carregando…</p>';
    var qs =
      '?aba=' +
      encodeURIComponent(qual) +
      '&loja=' +
      encodeURIComponent(depositoAtual());
    fetch(url + qs, {
      credentials: 'same-origin',
      headers: { Accept: 'application/json' },
    })
      .then(function (r) {
        return r.json();
      })
      .then(function (d) {
        if (!d || !d.ok) {
          dom.lista.innerHTML = '<p class="text-sm font-bold text-red-700">Não carregou a lista.</p>';
          dom.lista._rows = [];
          syncListaToolbar();
          return;
        }
        applyResumoCounts(d);
        renderLista(d.itens || []);
      })
      .catch(function () {
        dom.lista.innerHTML = '<p class="text-sm font-bold text-red-700">Erro de rede.</p>';
        if (dom.lista) dom.lista._rows = [];
        syncListaToolbar();
      });
  }

  function postAcao(id, acao, extra) {
    if (busy) return;
    var run = function () {
      postAcaoExec(id, acao, extra);
    };
    if (typeof window.gmSspinGarantirOperador === 'function') {
      window.gmSspinGarantirOperador(run, { titulo: 'PIN para Pedir loja' });
    } else {
      run();
    }
  }

  function postAcaoExec(id, acao, extra) {
    if (busy) return;
    var pattern = urls.apiPdvTransfLojaAcaoPattern || '';
    var url = pattern.replace('__pk__', String(id));
    if (!url) return;
    var body = Object.assign({ acao: acao, loja: depositoAtual() }, extra || {});
    busy = true;
    setStatus('Salvando…');
    fetch(url, {
      method: 'POST',
      credentials: 'same-origin',
      headers: {
        'Content-Type': 'application/json',
        'X-CSRFToken': csrf(),
        Accept: 'application/json',
      },
      body: JSON.stringify(body),
    })
      .then(function (r) {
        return r.json().then(function (d) {
          return { ok: r.ok, data: d };
        });
      })
      .then(function (res) {
        busy = false;
        if (res.data && res.data.precisa_pin) {
          setPinAviso(true);
          setStatus(res.data.erro || 'Entre com o PIN.', true);
          if (typeof window.gmSspinGarantirOperador === 'function') {
            window.gmSspinGarantirOperador(function () {
              postAcaoExec(id, acao, extra);
            }, { titulo: 'PIN para Pedir loja' });
          }
          return;
        }
        if (!res.ok || !res.data || !res.data.ok) {
          setStatus((res.data && res.data.erro) || 'Não salvou.', true);
          return;
        }
        applyResumoCounts(res.data);
        setStatus(res.data.mensagem || 'Ok.');
        carregarLista(aba);
        refreshResumo();
      })
      .catch(function () {
        busy = false;
        setStatus('Erro de rede.', true);
      });
  }

  function pedirAcao(id, acao, card) {
    if (acao === 'imprimir' || acao === 'etiquetas') {
      var rows = (dom.lista && dom.lista._rows) || [];
      var row = rows.filter(function (r) {
        return String(r.id) === String(id);
      })[0];
      if (acao === 'etiquetas') {
        if (row) imprimirEtiquetasSeparacao53(row);
      } else if (row) {
        imprimirCupomSeparacao(row);
      }
      return;
    }
    if (acao === 'cancelar') {
      abrirConfirm({
        title: 'Cancelar pedido?',
        body: 'O pedido some da fila. Se o estoque estiver errado, marque furado e ajuste o saldo.',
        confirmLabel: 'Cancelar pedido',
        furado: true,
      }).then(function (r) {
        if (!r.ok) return;
        postAcao(id, 'cancelar', {
          estoque_furado: !!r.estoque_furado,
          ajustar_estoque: !!r.ajustar_estoque,
          ajuste_quantidade: r.ajuste_quantidade,
          motivo: r.estoque_furado ? 'Estoque furado' : '',
        });
      });
      return;
    }
    if (acao === 'transferir') {
      var sel = lerSelecaoDoCard(card);
      if (!sel.itens.length) {
        setStatus('Marque ao menos um produto (□) para enviar agora.', true);
        return;
      }
      var temQtdPos = sel.itens.some(function (it) {
        return Number(it.quantidade) > 0;
      });
      if (!temQtdPos) {
        setStatus(
          'Coloque quantidade maior que zero em pelo menos um marcado. Qtd 0 sozinho não fecha o envio.',
          true
        );
        return;
      }
      var rows = (dom.lista && dom.lista._rows) || [];
      var rowData = rows.filter(function (r) {
        return String(r.id) === String(id);
      })[0];
      var nomesPorId = {};
      if (rowData && rowData.itens) {
        rowData.itens.forEach(function (it) {
          nomesPorId[String(it.id)] = it.nome || '#' + it.id;
        });
      }
      var enviandoTxt = sel.itens
        .map(function (it) {
          var nome = nomesPorId[String(it.id)] || '#' + it.id;
          var q = Number(it.quantidade);
          if (q === 0) return '· ' + nome + ' — NÃO ENVIAR (0)';
          return '· ' + nome + ' × ' + fmtSaldo(it.quantidade);
        })
        .join('\n');
      var bodyTxt = 'Vai agora:\n' + enviandoTxt;
      if (sel.adiar_itens.length) {
        var depoisTxt = sel.adiar_itens
          .map(function (iid) {
            return '· ' + (nomesPorId[String(iid)] || '#' + iid);
          })
          .join('\n');
        bodyTxt +=
          '\n\nFicam na fila (' + sel.adiar_itens.length + '):\n' + depoisTxt;
      }
      abrirConfirm({
        title: 'Transferir estoque?',
        body: bodyTxt,
        confirmLabel: sel.adiar_itens.length
          ? 'Transferir e deixar resto'
          : 'Transferir',
        furado: true,
      }).then(function (r) {
        if (!r.ok) return;
        var extra = {
          estoque_furado: !!r.estoque_furado,
          ajustar_estoque: !!r.ajustar_estoque,
          ajuste_quantidade: r.ajuste_quantidade,
          itens: sel.itens,
        };
        if (sel.adiar_itens.length) extra.adiar_itens = sel.adiar_itens;
        postAcao(id, 'transferir', extra);
      });
      return;
    }
    postAcao(id, acao);
  }

  function coletarLotesTransferirSel() {
    var lotes = [];
    if (!dom.lista || aba !== 'recebidos') return lotes;
    var rows = (dom.lista._rows) || [];
    dom.lista.querySelectorAll('.pl-card[data-pl-st="pronto"]').forEach(function (card) {
      var id = card.getAttribute('data-pl-id');
      var sel = lerSelecaoDoCard(card);
      var comQtd = sel.itens.filter(function (it) {
        return Number(it.quantidade) > 0;
      });
      if (!comQtd.length) return;
      var rowData = rows.filter(function (r) {
        return String(r.id) === String(id);
      })[0];
      var nomesPorId = {};
      if (rowData && rowData.itens) {
        rowData.itens.forEach(function (it) {
          nomesPorId[String(it.id)] = it.nome || '#' + it.id;
        });
      }
      lotes.push({
        id: id,
        sel: { itens: sel.itens, adiar_itens: sel.adiar_itens },
        nomesPorId: nomesPorId,
      });
    });
    return lotes;
  }

  function transferirSelecionadosTodos() {
    var lotes = coletarLotesTransferirSel();
    if (!lotes.length) {
      setStatus('Marque □ em pedidos Pronto com quantidade maior que zero.', true);
      return;
    }
    var linhas = [];
    var totalItens = 0;
    lotes.forEach(function (lote) {
      linhas.push('Pedido #' + lote.id + ':');
      lote.sel.itens.forEach(function (it) {
        var nome = lote.nomesPorId[String(it.id)] || '#' + it.id;
        var q = Number(it.quantidade);
        if (q === 0) linhas.push('  · ' + nome + ' — NÃO ENVIAR (0)');
        else {
          linhas.push('  · ' + nome + ' × ' + fmtSaldo(it.quantidade));
          totalItens += 1;
        }
      });
      if (lote.sel.adiar_itens.length) {
        linhas.push('  (ficam na fila: ' + lote.sel.adiar_itens.length + ')');
      }
    });
    abrirConfirm({
      title: 'Transferir selecionados?',
      body:
        lotes.length +
        ' pedido(s) · ' +
        totalItens +
        ' produto(s)\n\n' +
        linhas.join('\n'),
      confirmLabel: 'Transferir tudo',
      furado: true,
    }).then(function (r) {
      if (!r.ok) return;
      var idx = 0;
      var okN = 0;
      function runOne() {
        if (idx >= lotes.length) {
          busy = false;
          setStatus('Transferidos: ' + okN + '/' + lotes.length + ' pedido(s).');
          carregarLista(aba);
          refreshResumo();
          return;
        }
        var lote = lotes[idx];
        idx += 1;
        var pattern = urls.apiPdvTransfLojaAcaoPattern || '';
        var url = pattern.replace('__pk__', String(lote.id));
        if (!url) {
          runOne();
          return;
        }
        busy = true;
        setStatus('Transferindo ' + idx + '/' + lotes.length + '…');
        var body = {
          acao: 'transferir',
          loja: depositoAtual(),
          estoque_furado: !!r.estoque_furado,
          ajustar_estoque: !!r.ajustar_estoque,
          ajuste_quantidade: r.ajuste_quantidade,
          itens: lote.sel.itens,
        };
        if (lote.sel.adiar_itens.length) body.adiar_itens = lote.sel.adiar_itens;
        fetch(url, {
          method: 'POST',
          credentials: 'same-origin',
          headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': csrf(),
            Accept: 'application/json',
          },
          body: JSON.stringify(body),
        })
          .then(function (resp) {
            return resp.json().then(function (d) {
              return { ok: resp.ok, data: d };
            });
          })
          .then(function (res) {
            if (res.data && res.data.precisa_pin) {
              busy = false;
              setPinAviso(true);
              setStatus(res.data.erro || 'Entre com o PIN.', true);
              if (typeof window.gmSspinGarantirOperador === 'function') {
                window.gmSspinGarantirOperador(function () {
                  transferirSelecionadosTodos();
                }, { titulo: 'PIN para Pedir loja' });
              }
              return;
            }
            if (!res.ok || !res.data || !res.data.ok) {
              busy = false;
              setStatus(
                (res.data && res.data.erro) ||
                  'Falhou no pedido #' + lote.id + '.',
                true
              );
              carregarLista(aba);
              refreshResumo();
              return;
            }
            okN += 1;
            applyResumoCounts(res.data);
            runOne();
          })
          .catch(function () {
            busy = false;
            setStatus('Erro de rede no pedido #' + lote.id + '.', true);
            carregarLista(aba);
            refreshResumo();
          });
      }
      runOne();
    });
  }

  /* Escolha: Pedir × Transferência forçada (badge continua só nos pedidos) */
  var escolha = document.getElementById('pdv-pedir-loja-escolha');
  var escolhaFechar = document.getElementById('pdv-pedir-loja-escolha-fechar');
  var escolhaCancelar = document.getElementById('pdv-pedir-loja-escolha-cancelar');
  var escolhaPedir = document.getElementById('pdv-pedir-loja-escolha-pedir');
  var escolhaForcada = document.getElementById('pdv-pedir-loja-escolha-forcada');

  function escolhaAberta() {
    return !!(escolha && !escolha.classList.contains('hidden'));
  }

  function fecharEscolha() {
    if (!escolha) return;
    escolha.classList.add('hidden');
    escolha.classList.remove('flex');
    try {
      if (window.AgroOverlayStack) window.AgroOverlayStack.setOpen(escolha, false);
    } catch (_) {}
  }

  function abrirEscolha() {
    if (!escolha) {
      abrir();
      return;
    }
    escolha.classList.remove('hidden');
    escolha.classList.add('flex');
    try {
      if (window.AgroOverlayStack) window.AgroOverlayStack.setOpen(escolha, true);
    } catch (_) {}
  }

  if (dom.btnOpen) dom.btnOpen.addEventListener('click', abrirEscolha);
  if (escolhaFechar) escolhaFechar.addEventListener('click', fecharEscolha);
  if (escolhaCancelar) escolhaCancelar.addEventListener('click', fecharEscolha);
  if (escolhaPedir) {
    escolhaPedir.addEventListener('click', function () {
      fecharEscolha();
      abrir();
    });
  }
  if (escolhaForcada) {
    escolhaForcada.addEventListener('click', function () {
      fecharEscolha();
      if (window.PdvTransfForcada && typeof window.PdvTransfForcada.abrirDirecao === 'function') {
        window.PdvTransfForcada.abrirDirecao();
      } else {
        alert('Transferência forçada indisponível neste PDV.');
      }
    });
  }
  if (dom.fechar) dom.fechar.addEventListener('click', fechar);
  overlay.addEventListener('click', function (e) {
    /* Fundo nao fecha — so X / FECHAR / Esc */
  });
  overlay.querySelectorAll('.pl-tab').forEach(function (btn) {
    btn.addEventListener('click', function () {
      setAba(btn.getAttribute('data-pl-aba'));
    });
  });
  if (dom.abrirPin) dom.abrirPin.addEventListener('click', abrirPin);
  if (dom.limpar) {
    dom.limpar.addEventListener('click', function () {
      cart = [];
      renderCart();
    });
  }
  if (dom.enviar) dom.enviar.addEventListener('click', enviarPedido);
  if (dom.livreAdd) dom.livreAdd.addEventListener('click', addCartLivre);
  if (dom.livre) {
    dom.livre.addEventListener('keydown', function (e) {
      if (e.key === 'Enter') {
        e.preventDefault();
        addCartLivre();
      }
    });
  }
  if (dom.busca) {
    dom.busca.addEventListener('input', function () {
      clearTimeout(searchTimer);
      searchTimer = setTimeout(function () {
        buscar(dom.busca.value);
      }, 220);
    });
  }
  if (dom.hits) {
    dom.hits.addEventListener('click', function (e) {
      var lista = dom.hits._hits || [];
      var aj = e.target.closest('[data-pl-aj]');
      if (aj) {
        e.preventDefault();
        e.stopPropagation();
        var idAj = aj.getAttribute('data-pl-aj');
        var pAj = lista.filter(function (x) {
          return produtoId(x) === idAj;
        })[0];
        if (pAj) abrirAjuste(pAj);
        return;
      }
      var btn = e.target.closest('[data-pl-add]');
      if (!btn) return;
      var id = btn.getAttribute('data-pl-add');
      var p = lista.filter(function (x) {
        return produtoId(x) === id;
      })[0];
      if (p) addCart(p);
    });
  }
  if (dom.cart) {
    dom.cart.addEventListener('click', function (e) {
      var q = e.target.closest('[data-pl-q]');
      var rm = e.target.closest('[data-pl-rm]');
      if (q) {
        var i = Number(q.getAttribute('data-i'));
        var delta = Number(q.getAttribute('data-pl-q'));
        if (cart[i]) {
          cart[i].qtd = Math.max(1, Number(cart[i].qtd) + delta);
          renderCart();
        }
      }
      if (rm) {
        cart.splice(Number(rm.getAttribute('data-pl-rm')), 1);
        renderCart();
      }
    });
  }
  if (dom.lista) {
    dom.lista.addEventListener('click', function (e) {
      var selBtn = e.target.closest('[data-pl-sel]');
      if (selBtn) {
        e.preventDefault();
        var cardSel = selBtn.closest('[data-pl-id]');
        marcarChecksDoCard(cardSel, selBtn.getAttribute('data-pl-sel') === 'todos');
        return;
      }
      var toggleMeta = e.target.closest('[data-pl-toggle-check]');
      if (toggleMeta) {
        var rowT = toggleMeta.closest('.pl-item-row');
        var cbT = rowT && rowT.querySelector('.pl-item-check');
        if (cbT) {
          cbT.checked = !cbT.checked;
          rowT.classList.toggle('is-off', !cbT.checked);
          var inpT = rowT.querySelector('.pl-item-qtd');
          if (inpT) inpT.disabled = !cbT.checked;
          atualizarBtnTransferir(rowT.closest('[data-pl-id]'));
        }
        return;
      }
      var btn = e.target.closest('[data-pl-acao]');
      if (!btn) return;
      var card = btn.closest('[data-pl-id]');
      if (!card) return;
      pedirAcao(card.getAttribute('data-pl-id'), btn.getAttribute('data-pl-acao'), card);
    });
    dom.lista.addEventListener('change', function (e) {
      var cb = e.target.closest('.pl-item-check');
      if (cb) {
        var row = cb.closest('.pl-item-row');
        if (row) {
          row.classList.toggle('is-off', !cb.checked);
          var inp = row.querySelector('.pl-item-qtd');
          if (inp) inp.disabled = !cb.checked;
        }
        atualizarBtnTransferir(cb.closest('[data-pl-id]'));
        return;
      }
      var qInp = e.target.closest('.pl-item-qtd');
      if (qInp) {
        syncQtdDiff(qInp);
        atualizarBtnTransferir(qInp.closest('[data-pl-id]'));
      }
    });
    dom.lista.addEventListener('input', function (e) {
      var qInp = e.target.closest('.pl-item-qtd');
      if (qInp) syncQtdDiff(qInp);
    });
  }
  if (dom.imprimirTodos) {
    dom.imprimirTodos.addEventListener('click', function () {
      imprimirTodosCupons();
    });
  }
  if (dom.aceitarTodos) {
    dom.aceitarTodos.addEventListener('click', function () {
      var rows = (dom.lista && dom.lista._rows) || [];
      var pend = rows.filter(function (r) {
        return r && r.status === 'pendente';
      });
      if (!pend.length) {
        setStatus('Nenhum pendente pra aceitar.', true);
        return;
      }
      abrirConfirm({
        title: 'Aceitar todos os pendentes?',
        body:
          'Vai aceitar ' +
          pend.length +
          ' pedido(s). Depois: Pronto → Transferir.',
        confirmLabel: 'Aceitar todos',
      }).then(function (r) {
        if (!r.ok) return;
        var idx = 0;
        function runOne() {
          if (idx >= pend.length) {
            setStatus('Aceitos: ' + pend.length + ' pedido(s).');
            carregarLista(aba);
            refreshResumo();
            return;
          }
          var pk = pend[idx].id;
          idx += 1;
          var pattern = urls.apiPdvTransfLojaAcaoPattern || '';
          var url = pattern.replace('__pk__', String(pk));
          if (!url) {
            runOne();
            return;
          }
          busy = true;
          setStatus('Aceitando ' + idx + '/' + pend.length + '…');
          fetch(url, {
            method: 'POST',
            credentials: 'same-origin',
            headers: {
              'Content-Type': 'application/json',
              'X-CSRFToken': csrf(),
              Accept: 'application/json',
            },
            body: JSON.stringify({ acao: 'aceitar', loja: depositoAtual() }),
          })
            .then(function (resp) {
              return resp.json().then(function (d) {
                return { ok: resp.ok, data: d };
              });
            })
            .then(function (res) {
              if (res.data && res.data.precisa_pin) {
                busy = false;
                setPinAviso(true);
                setStatus(res.data.erro || 'Entre com o PIN.', true);
                if (typeof window.gmSspinGarantirOperador === 'function') {
                  window.gmSspinGarantirOperador(function () {
                    if (dom.aceitarTodos) dom.aceitarTodos.click();
                  }, { titulo: 'PIN para Pedir loja' });
                }
                return;
              }
              if (!res.ok || !res.data || !res.data.ok) {
                busy = false;
                setStatus((res.data && res.data.erro) || 'Falhou ao aceitar.', true);
                carregarLista(aba);
                refreshResumo();
                return;
              }
              applyResumoCounts(res.data);
              runOne();
            })
            .catch(function () {
              busy = false;
              setStatus('Erro de rede ao aceitar.', true);
              carregarLista(aba);
              refreshResumo();
            });
        }
        runOne();
      });
    });
  }
  if (dom.transferirSel) {
    dom.transferirSel.addEventListener('click', function () {
      transferirSelecionadosTodos();
    });
  }

  if (dom.confirmSim) dom.confirmSim.addEventListener('click', function () { fecharConfirm(true); });
  if (dom.confirmNao) dom.confirmNao.addEventListener('click', function () { fecharConfirm(false); });
  if (dom.confirm) {
    dom.confirm.addEventListener('click', function (e) {
      /* Fundo nao fecha — so X / FECHAR / Esc */
    });
  }
  if (dom.confirmFurado) dom.confirmFurado.addEventListener('change', syncFuradoUi);
  if (dom.ajusteSim) dom.ajusteSim.addEventListener('click', salvarAjuste);
  if (dom.ajusteNao) dom.ajusteNao.addEventListener('click', fecharAjuste);
  if (dom.ajuste) {
    dom.ajuste.addEventListener('click', function (e) {
      /* Fundo nao fecha — so X / FECHAR / Esc */
    });
  }

  document.addEventListener('keydown', function (e) {
    if (dom.temPedido && dom.temPedido.classList.contains('is-open')) {
      if (e.key === 'Enter' || e.key === 'Escape') {
        e.preventDefault();
        e.stopPropagation();
        fecharTemPedido();
      }
      return;
    }
    if (e.key === 'Escape' && escolhaAberta()) {
      e.preventDefault();
      fecharEscolha();
      return;
    }
    if (e.key === 'Escape' && dom.ajuste && dom.ajuste.classList.contains('is-open')) {
      e.preventDefault();
      fecharAjuste();
      return;
    }
    if (e.key === 'Escape' && dom.confirm && dom.confirm.classList.contains('is-open')) {
      e.preventDefault();
      fecharConfirm(false);
      return;
    }
    if (e.key === 'Escape' && overlay.classList.contains('flex')) {
      e.preventDefault();
      fechar();
    }
  });

  window.PdvPedirLoja = { abrir: abrir, fechar: fechar };
  window.PdvPedirLojaEscolha = { abrir: abrirEscolha, fechar: fecharEscolha };

  if (dom.temPedidoOk) dom.temPedidoOk.addEventListener('click', fecharTemPedido);
  if (dom.temPedido) {
    dom.temPedido.addEventListener('click', function (e) {
      /* Fundo nao fecha — so X / FECHAR / Esc */
    });
  }

  renderCart();
  refreshResumo();
  pollTimer = setInterval(function () {
    refreshResumo();
  }, 12000);
  /* Só badge/beep — NÃO abrir «tem pedido» aqui.
     Chat/venda também disparam gm-sspin-operador ao renovar PIN (~45s).
     O popup «tem pedido» fica só em abrirPin() (botão PIN do Pedir loja). */
  window.addEventListener('gm-sspin-operador', function () {
    refreshResumo();
  });
  document.addEventListener('visibilitychange', function () {
    if (!document.hidden) refreshResumo();
  });
})();
