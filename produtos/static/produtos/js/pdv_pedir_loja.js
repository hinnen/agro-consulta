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
  var busy = false;
  var aba = 'pedir';

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
  };

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

  function refreshResumo() {
    var url = urls.apiPdvTransfLojaResumo;
    if (!url) return;
    fetch(url, { credentials: 'same-origin', headers: { Accept: 'application/json' } })
      .then(function (r) {
        return r.json();
      })
      .then(function (d) {
        if (!d || !d.ok) return;
        applyBadge(d.recebidos_abertos || 0);
        setPinAviso(!!d.precisa_pin);
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
    if (dom.hits) {
      dom.hits.innerHTML = '<p class="pl-hint">Digite o nome, GM ou código.</p>';
    }
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
      if (dom.hits) {
        dom.hits.innerHTML = '<p class="pl-hint">Digite o nome, GM ou código.</p>';
      }
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
          dom.hits.innerHTML =
            '<p class="text-base font-bold text-slate-500">Nenhum produto.</p>';
          return;
        }
        dom.hits.innerHTML = lista
          .map(function (p) {
            var id = produtoId(p);
            return (
              '<button type="button" class="pl-hit" data-pl-add="' +
              escapeHtml(id) +
              '">' +
              '<span class="pl-name">' +
              escapeHtml(p.nome || '') +
              '</span>' +
              '<span class="pl-saldos">' +
              '<span class="pl-saldo-pill"><b>' +
              escapeHtml(fmtSaldo(numSaldo(p, 'saldo_centro'))) +
              '</b></span>' +
              '<span class="pl-saldo-pill"><b>' +
              escapeHtml(fmtSaldo(numSaldo(p, 'saldo_vila'))) +
              '</b></span>' +
              '</span></button>'
            );
          })
          .join('');
        dom.hits._hits = lista;
      })
      .catch(function () {
        if (dom.hits) {
          dom.hits.innerHTML = '<p class="text-sm font-bold text-red-700">Erro na busca.</p>';
        }
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

  function renderLista(itens) {
    if (!dom.lista) return;
    if (!itens || !itens.length) {
      dom.lista.innerHTML =
        '<p class="py-10 text-center text-base font-bold text-slate-500">Nada aqui agora.</p>';
      return;
    }
    dom.lista.innerHTML = itens
      .map(function (row) {
        var qtdHero = '';
        var linhas = row.itens || [];
        if (linhas.length === 1) {
          qtdHero =
            '<p class="pl-qty" style="text-align:left;margin-top:0.35rem">' +
            escapeHtml(String(linhas[0].quantidade_texto || linhas[0].quantidade || '')) +
            '</p>';
        }
        return (
          '<article class="pl-card" data-pl-id="' +
          escapeHtml(String(row.id)) +
          '">' +
          '<p class="pl-st">' +
          escapeHtml(row.status_label || row.status) +
          '</p>' +
          '<p class="pl-name">' +
          escapeHtml(row.resumo || '') +
          '</p>' +
          qtdHero +
          '<p class="mt-1 text-base font-bold text-slate-500">' +
          escapeHtml(row.loja_origem_label) +
          ' → ' +
          escapeHtml(row.loja_destino_label) +
          (row.criado_por ? ' · ' + escapeHtml(row.criado_por) : '') +
          '</p>' +
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
        renderLista(d.itens || []);
      })
      .catch(function () {
        dom.lista.innerHTML = '<p class="text-sm font-bold text-red-700">Erro de rede.</p>';
      });
  }

  function postAcao(id, acao) {
    if (busy) return;
    var pattern = urls.apiPdvTransfLojaAcaoPattern || '';
    var url = pattern.replace('__pk__', String(id));
    if (!url) return;
    if (acao === 'cancelar' && !window.confirm('Cancelar este pedido?')) return;
    if (acao === 'transferir' && !window.confirm('Transferir o estoque agora? Some na origem e entra na loja que pediu.'))
      return;
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
      body: JSON.stringify({ acao: acao, loja: depositoAtual() }),
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
        setStatus(res.data.mensagem || 'Ok.');
        carregarLista(aba);
      })
      .catch(function () {
        busy = false;
        setStatus('Erro de rede.', true);
      });
  }

  if (dom.btnOpen) dom.btnOpen.addEventListener('click', abrir);
  if (dom.fechar) dom.fechar.addEventListener('click', fechar);
  overlay.addEventListener('click', function (e) {
    if (e.target === overlay) fechar();
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
      var btn = e.target.closest('[data-pl-add]');
      if (!btn) return;
      var id = btn.getAttribute('data-pl-add');
      var lista = dom.hits._hits || [];
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
      postAcao(card.getAttribute('data-pl-id'), btn.getAttribute('data-pl-acao'));
    });
  }

  document.addEventListener('keydown', function (e) {
    if (e.key === 'Escape' && overlay.classList.contains('flex')) {
      e.preventDefault();
      fechar();
    }
  });

  renderCart();
  refreshResumo();
  pollTimer = setInterval(refreshResumo, 25000);
  window.addEventListener('gm-sspin-operador', function () {
    refreshResumo();
  });
})();
