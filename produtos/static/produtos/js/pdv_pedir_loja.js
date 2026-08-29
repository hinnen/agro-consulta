/**
 * PDV — overlay Pedir loja (solicitação Centro ↔ Vila).
 * Pedir / Aceitar / Pronto = só status. Transferir = move estoque.
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
    limpar: document.getElementById('pdv-pedir-loja-limpar'),
    enviar: document.getElementById('pdv-pedir-loja-enviar'),
    lista: document.getElementById('pdv-pedir-loja-lista'),
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

  function syncBeepPendentes(n) {
    pendentesBeep = Number(n || 0);
    if (pendentesBeep > 0) {
      if (!beepTimer) {
        plBeep();
        beepTimer = setInterval(function () {
          if (pendentesBeep > 0) plBeep();
        }, 60000);
      }
    } else if (beepTimer) {
      clearInterval(beepTimer);
      beepTimer = null;
    }
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
    });
  }

  function abrirPin() {
    if (typeof window.gmSspinAbrirEntrada === 'function') window.gmSspinAbrirEntrada();
  }

  function abrir() {
    overlay.classList.remove('hidden');
    overlay.classList.add('flex');
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
    if (aba !== 'pedir') carregarLista(aba);
  }

  function applyBadge(n) {
    n = Number(n || 0);
    if (dom.btnOpen) {
      var base = 'pdv-action-btn pdv-wiz-topbar-btn pdv-wiz-topbar-btn--rose relative';
      if (n > 0) base += ' pdv-wiz-topbar-btn--pedir-loja-alerta';
      dom.btnOpen.className = base;
      dom.btnOpen.title =
        n > 0
          ? n + ' pedido(s) da outra loja'
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
    fetch(url, { credentials: 'same-origin', headers: { Accept: 'application/json' } })
      .then(function (r) {
        return r.json();
      })
      .then(function (d) {
        if (!d || !d.ok) return;
        var n = Number(d.recebidos_abertos || 0);
        var pend = Number(
          d.recebidos_pendentes != null ? d.recebidos_pendentes : d.recebidos_abertos || 0
        );
        applyBadge(n);
        syncBeepPendentes(pend);
        setPinAviso(!!d.precisa_pin);
        if (opts.aposPin && !d.precisa_pin && n > 0) abrirTemPedido(n);
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
        saldo_centro: numSaldo(p, 'saldo_centro'),
        saldo_vila: numSaldo(p, 'saldo_vila'),
      });
    }
    renderCart();
    hitsHint('Digite o nome, GM ou código.');
    if (dom.busca) dom.busca.value = '';
  }

  function renderCart() {
    if (!dom.cart) return;
    if (!cart.length) {
      dom.cart.innerHTML =
        '<p class="px-1 py-2 text-sm font-bold text-slate-500">Busque à esquerda e toque no produto.</p>';
      return;
    }
    var outra = depositoAtual() === 'vila' ? 'saldo_centro' : 'saldo_vila';
    var outraLbl = depositoAtual() === 'vila' ? 'Saldo Centro' : 'Saldo Vila';
    dom.cart.innerHTML = cart
      .map(function (it, idx) {
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
          '<div class="pl-saldos mt-2">' +
          '<div class="pl-saldo-pill"><small>' +
          escapeHtml(outraLbl) +
          '</small><b>' +
          escapeHtml(fmtSaldo(it[outra])) +
          '</b></div>' +
          '</div>' +
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
    if (!cart.length) {
      setStatus('Inclua ao menos um produto.', true);
      return;
    }
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
          return;
        }
        if (!res.ok || !res.data || !res.data.ok) {
          setStatus((res.data && res.data.erro) || 'Não enviou.', true);
          return;
        }
        cart = [];
        if (dom.obs) dom.obs.value = '';
        renderCart();
        applyBadge(res.data.recebidos_abertos || 0);
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
    }
    if (aba === 'enviados' && (st === 'pendente' || st === 'aceito' || st === 'pronto')) {
      btns.push('<button type="button" class="pl-btn pl-btn--print" data-pl-acao="imprimir">Imprimir cupom</button>');
    }
    if (aba === 'recebidos' && st === 'pendente') {
      btns.push('<button type="button" class="pl-btn pl-btn--ok" data-pl-acao="aceitar">Aceitar</button>');
    }
    if (aba === 'recebidos' && st === 'aceito') {
      btns.push('<button type="button" class="pl-btn pl-btn--ok" data-pl-acao="pronto">Pronto</button>');
    }
    if ((st === 'aceito' || st === 'pronto') && (aba === 'recebidos' || aba === 'enviados')) {
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
        var pedHint = '';
        if (edit) {
          pedHint =
            '<p class="pl-item-ped">Pedido: ' + escapeHtml(fmtSaldo(pedida)) + '</p>';
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
        }
        var qtdCell = edit
          ? '<input type="number" class="pl-item-qtd" min="0" step="0.001" data-pl-item-id="' +
            escapeHtml(String(it.id)) +
            '" value="' +
            escapeHtml(String(qtdAtual)) +
            '" aria-label="Quantidade a enviar" />'
          : '<span class="pl-item-qtd-ro">' + escapeHtml(fmtSaldo(qtdAtual)) + '</span>';
        return (
          '<div class="pl-item-row">' +
          '<div class="pl-item-meta">' +
          '<p class="pl-item-nome">' +
          escapeHtml(it.nome || '') +
          '</p>' +
          (gm ? '<p class="pl-item-gm">GM ' + escapeHtml(gm) + '</p>' : '') +
          pedHint +
          '</div>' +
          qtdCell +
          '</div>'
        );
      })
      .join('');
    return (
      '<div class="pl-itens">' +
      (edit
        ? '<p class="m-0 text-[10px] font-black uppercase tracking-wide text-orange-800">Qtd a enviar (já vem com o pedido · mude se faltar)</p>'
        : '') +
      lines +
      '</div>'
    );
  }

  function lerQtdsDoCard(card) {
    if (!card) return [];
    var out = [];
    card.querySelectorAll('.pl-item-qtd[data-pl-item-id]').forEach(function (inp) {
      out.push({
        id: Number(inp.getAttribute('data-pl-item-id')),
        quantidade: String(inp.value || '0'),
      });
    });
    return out;
  }

  function imprimirCupomSeparacao(row) {
    if (!row) return;
    var dh = new Date().toLocaleString('pt-BR');
    var itens = row.itens || [];
    var bodyItens = '';
    itens.forEach(function (it) {
      var q =
        it.quantidade_pedida != null && Number(it.quantidade_pedida) > 0
          ? it.quantidade_pedida
          : it.quantidade;
      bodyItens +=
        '<div style="border-top:1px dashed #000;margin-top:6px;padding-top:4px;">' +
        (it.codigo_interno
          ? '<div><b>GM</b> ' + escapeHtml(it.codigo_interno) + '</div>'
          : '') +
        '<div style="font-weight:bold;font-size:13px;">' +
        escapeHtml(it.nome || '') +
        '</div>' +
        '<div style="font-size:22px;font-weight:900;margin:4px 0;">QTD ' +
        escapeHtml(fmtSaldo(q)) +
        '</div>' +
        '</div>';
    });
    var html =
      '<!DOCTYPE html><html><head><meta charset="utf-8"><title>Separação Pedir loja</title>' +
      '<style>@page{margin:0;size:80mm auto}html,body{margin:0;padding:0;width:80mm}' +
      'body{font-family:"Courier New",Courier,monospace;color:#000;background:#fff}' +
      '.pg{width:80mm;box-sizing:border-box;padding:4mm 3mm;font-size:11px;line-height:1.35}</style></head><body>' +
      '<div class="pg">' +
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
      bodyItens +
      '<div style="margin-top:10px;text-align:center;font-size:10px;font-weight:900;">Conferir e transferir no PDV</div>' +
      '</div></body></html>';

    var iframe = document.getElementById('pdv-pedir-loja-print-iframe');
    if (!iframe) {
      iframe = document.createElement('iframe');
      iframe.id = 'pdv-pedir-loja-print-iframe';
      iframe.setAttribute('title', 'Cupom separação');
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
        setStatus('Não imprimiu. Confira a térmica 80mm.', true);
      }
    }, 120);
  }

  function renderLista(itens) {
    if (!dom.lista) return;
    if (!itens || !itens.length) {
      dom.lista.innerHTML =
        '<p class="py-10 text-center text-base font-bold text-slate-500">Nada aqui agora.</p>';
      dom.lista._rows = [];
      return;
    }
    dom.lista._rows = itens;
    dom.lista.innerHTML = itens
      .map(function (row) {
        return (
          '<article class="pl-card" data-pl-id="' +
          escapeHtml(String(row.id)) +
          '">' +
          '<p class="pl-st">' +
          escapeHtml(row.status_label || row.status) +
          ' · #' +
          escapeHtml(String(row.id)) +
          '</p>' +
          '<p class="pl-name">' +
          escapeHtml(row.resumo || '') +
          '</p>' +
          '<p class="mt-1 text-sm font-bold text-slate-500">' +
          escapeHtml(row.loja_origem_label) +
          ' → ' +
          escapeHtml(row.loja_destino_label) +
          (row.criado_por ? ' · ' + escapeHtml(row.criado_por) : '') +
          '</p>' +
          itensHtml(row) +
          '<div class="pl-actions">' +
          acoesHtml(row) +
          '</div></article>'
        );
      })
      .join('');
  }

  function carregarLista(qual) {
    var url = urls.apiPdvTransfLojaLista;
    if (!url || !dom.lista) return;
    dom.lista.innerHTML = '<p class="py-6 text-center text-sm font-bold text-slate-500">Carregando…</p>';
    fetch(url + '?aba=' + encodeURIComponent(qual), {
      credentials: 'same-origin',
      headers: { Accept: 'application/json' },
    })
      .then(function (r) {
        return r.json();
      })
      .then(function (d) {
        if (!d || !d.ok) {
          dom.lista.innerHTML = '<p class="text-sm font-bold text-red-700">Não carregou a lista.</p>';
          return;
        }
        applyBadge(d.recebidos_abertos || 0);
        syncBeepPendentes(
          d.recebidos_pendentes != null ? d.recebidos_pendentes : d.recebidos_abertos || 0
        );
        renderLista(d.itens || []);
      })
      .catch(function () {
        dom.lista.innerHTML = '<p class="text-sm font-bold text-red-700">Erro de rede.</p>';
      });
  }

  function postAcao(id, acao, extra) {
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
          return;
        }
        if (!res.ok || !res.data || !res.data.ok) {
          setStatus((res.data && res.data.erro) || 'Não salvou.', true);
          return;
        }
        applyBadge(res.data.recebidos_abertos || 0);
        syncBeepPendentes(
          res.data.recebidos_pendentes != null ? res.data.recebidos_pendentes : res.data.recebidos_abertos || 0
        );
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
    if (acao === 'imprimir') {
      var rows = (dom.lista && dom.lista._rows) || [];
      var row = rows.filter(function (r) {
        return String(r.id) === String(id);
      })[0];
      if (row) imprimirCupomSeparacao(row);
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
      var qtds = lerQtdsDoCard(card);
      abrirConfirm({
        title: 'Transferir estoque?',
        body: 'Some na origem e entra na loja que pediu. Confira as quantidades nos campos (já vêm com o pedido). Se o saldo estiver errado, marque estoque furado.',
        confirmLabel: 'Transferir',
        furado: true,
      }).then(function (r) {
        if (!r.ok) return;
        var extra = {
          estoque_furado: !!r.estoque_furado,
          ajustar_estoque: !!r.ajustar_estoque,
          ajuste_quantidade: r.ajuste_quantidade,
        };
        if (qtds.length) extra.itens = qtds;
        postAcao(id, 'transferir', extra);
      });
      return;
    }
    postAcao(id, acao);
  }

  if (dom.btnOpen) dom.btnOpen.addEventListener('click', abrir);
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
      var btn = e.target.closest('[data-pl-acao]');
      if (!btn) return;
      var card = btn.closest('[data-pl-id]');
      if (!card) return;
      pedirAcao(card.getAttribute('data-pl-id'), btn.getAttribute('data-pl-acao'), card);
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
  }, 25000);
  window.addEventListener('gm-sspin-operador', function (ev) {
    var nome = ev && ev.detail && ev.detail.nome;
    if (!nome) {
      refreshResumo();
      return;
    }
    refreshResumo({ aposPin: true });
  });
})();
