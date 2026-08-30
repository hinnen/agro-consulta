/**
 * PDV — Transferência forçada (espelho Logística).
 * APIs: /api/buscar/, /estoque/api_resolver_codigos_transferencia_forcada/,
 *       /estoque/api_transferir_forcado_vila_para_centro/
 * PIN sempre na confirmação. Esc: PIN → overlay → direção → escolha.
 */
(function () {
  'use strict';

  var backdrop = document.getElementById('pdv-tf-backdrop');
  var elDir = document.getElementById('pdv-tf-direcao');
  var overlay = document.getElementById('pdv-tf-overlay');
  var elPin = document.getElementById('pdv-tf-pin');
  if (!overlay || !elDir) return;

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
  var carrinho = [];
  var buscaTimer = null;
  var buscaSeq = 0;
  var resultados = [];
  var highlightIdx = -1;
  var direcao = 'vila_centro';
  var pinResolve = null;
  var returnToEscolha = true;

  var dom = {
    titulo: document.getElementById('pdv-tf-titulo'),
    header: document.getElementById('pdv-tf-header'),
    btnOk: document.getElementById('pdv-tf-btn-transferir'),
    busca: document.getElementById('pdv-tf-busca'),
    status: document.getElementById('pdv-tf-busca-status'),
    res: document.getElementById('pdv-tf-resultados'),
    cart: document.getElementById('pdv-tf-carrinho'),
    cartCnt: document.getElementById('pdv-tf-carrinho-count'),
    colar: document.getElementById('pdv-tf-colar'),
    colarSt: document.getElementById('pdv-tf-colar-status'),
    colarAplicar: document.getElementById('pdv-tf-colar-aplicar'),
    limpar: document.getElementById('pdv-tf-limpar'),
    fechar: document.getElementById('pdv-tf-fechar'),
    cancelar: document.getElementById('pdv-tf-cancelar'),
    dirVc: document.getElementById('pdv-tf-dir-vc'),
    dirCv: document.getElementById('pdv-tf-dir-cv'),
    dirCancel: document.getElementById('pdv-tf-dir-cancelar'),
    pinDesc: document.getElementById('pdv-tf-pin-desc'),
    pinInput: document.getElementById('pdv-tf-pin-input'),
    pinOk: document.getElementById('pdv-tf-pin-ok'),
    pinCancel: document.getElementById('pdv-tf-pin-cancelar'),
  };

  function csrf() {
    var c = document.cookie.match(/csrftoken=([^;]+)/);
    return (c && c[1]) || (bootstrap.csrfToken || '');
  }

  function esc(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function fmtSaldo(n) {
    var v = Number(n || 0);
    if (Number.isNaN(v)) return '—';
    return v.toFixed(1);
  }

  function rotuloDirecao() {
    return direcao === 'centro_vila' ? 'Centro→Vila' : 'Vila→Centro';
  }

  function depositoAtual() {
    try {
      bootstrap = boot();
    } catch (e) {}
    var d = (bootstrap.pdvDeposito && bootstrap.pdvDeposito.deposito) || 'centro';
    return String(d).toLowerCase() === 'vila' ? 'vila' : 'centro';
  }

  /** Destaque: botão que sai da loja do PDV fica maior; os outros = Voltar. */
  function aplicarDestaqueDirecao() {
    var dep = depositoAtual();
    var hero = dep === 'vila' ? dom.dirVc : dom.dirCv;
    var sec = dep === 'vila' ? dom.dirCv : dom.dirVc;
    [dom.dirVc, dom.dirCv, dom.dirCancel].forEach(function (btn) {
      if (!btn) return;
      btn.classList.remove('pdv-tf-dir-btn--hero');
      btn.classList.add('pdv-tf-dir-btn--sec');
    });
    if (hero) {
      hero.classList.remove('pdv-tf-dir-btn--sec');
      hero.classList.add('pdv-tf-dir-btn--hero');
    }
    if (sec) {
      sec.classList.add('pdv-tf-dir-btn--sec');
      sec.classList.remove('pdv-tf-dir-btn--hero');
    }
    if (dom.dirCancel) {
      dom.dirCancel.classList.add('pdv-tf-dir-btn--sec');
      dom.dirCancel.classList.remove('pdv-tf-dir-btn--hero');
    }
    var wrap = elDir.querySelector('.pdv-tf-dir-btns');
    if (wrap) wrap.setAttribute('data-pdv-tf-deposito', dep);
  }

  function showBackdrop(on) {
    if (!backdrop) return;
    if (on) backdrop.classList.remove('hidden');
    else backdrop.classList.add('hidden');
  }

  function isDirOpen() {
    return elDir && !elDir.classList.contains('hidden');
  }

  function isOverlayOpen() {
    return overlay && !overlay.classList.contains('hidden');
  }

  function isPinOpen() {
    return elPin && !elPin.classList.contains('hidden');
  }

  function setDirecao(dir) {
    direcao = dir === 'centro_vila' ? 'centro_vila' : 'vila_centro';
    var isCv = direcao === 'centro_vila';
    var rotulo = isCv ? 'Centro → Vila' : 'Vila → Centro';
    if (dom.titulo) dom.titulo.textContent = rotulo;
    if (dom.header) {
      dom.header.classList.toggle('pdv-tf-header--vc', !isCv);
      dom.header.classList.toggle('pdv-tf-header--cv', isCv);
    }
    overlay.classList.toggle('tf-dir-vc', !isCv);
    overlay.classList.toggle('tf-dir-cv', isCv);
    if (dom.btnOk) {
      dom.btnOk.textContent =
        'Transferir ' + (isCv ? 'C→Vila' : 'Vila→C');
    }
    overlay.classList.toggle('tf-layout-invertido', isCv);
  }

  function reopenEscolha() {
    if (
      returnToEscolha &&
      window.PdvPedirLojaEscolha &&
      typeof window.PdvPedirLojaEscolha.abrir === 'function'
    ) {
      window.PdvPedirLojaEscolha.abrir();
    }
  }

  function fecharDirecao(opts) {
    opts = opts || {};
    if (elDir) elDir.classList.add('hidden');
    if (!isOverlayOpen() && !isPinOpen()) showBackdrop(false);
    if (opts.voltarEscolha) reopenEscolha();
  }

  function fecharOverlay(opts) {
    opts = opts || {};
    if (overlay) {
      overlay.classList.add('hidden');
      overlay.classList.remove('flex');
    }
    if (!isDirOpen() && !isPinOpen()) showBackdrop(false);
    if (opts.voltarEscolha) reopenEscolha();
  }

  function fecharPin(ok) {
    if (elPin) {
      elPin.classList.add('hidden');
      elPin.classList.remove('flex');
    }
    var r = pinResolve;
    pinResolve = null;
    if (r) r(ok ? String((dom.pinInput && dom.pinInput.value) || '').trim() : null);
  }

  function pedirPin(mensagem) {
    return new Promise(function (resolve) {
      pinResolve = resolve;
      if (dom.pinDesc) dom.pinDesc.textContent = mensagem || 'Confirme com seu PIN';
      if (dom.pinInput) dom.pinInput.value = '';
      if (elPin) {
        elPin.classList.remove('hidden');
        elPin.classList.add('flex');
      }
      setTimeout(function () {
        if (dom.pinInput) {
          try {
            dom.pinInput.focus();
            dom.pinInput.select();
          } catch (e) {}
        }
      }, 40);
    });
  }

  function abrirDirecao() {
    returnToEscolha = true;
    aplicarDestaqueDirecao();
    showBackdrop(true);
    if (elDir) elDir.classList.remove('hidden');
    setTimeout(function () {
      var hero = elDir && elDir.querySelector('.pdv-tf-dir-btn--hero');
      if (hero) {
        try {
          hero.focus();
        } catch (e) {}
      }
    }, 40);
  }

  function iniciar(dir) {
    fecharDirecao({ voltarEscolha: false });
    carrinho = [];
    resultados = [];
    highlightIdx = -1;
    if (dom.res) {
      dom.res.innerHTML =
        '<div class="p-6 text-center text-sm font-bold text-slate-400">—</div>';
    }
    if (dom.status) dom.status.textContent = '';
    if (dom.colar) dom.colar.value = '';
    if (dom.colarSt) dom.colarSt.textContent = '';
    setDirecao(dir);
    renderCarrinho();
    showBackdrop(true);
    overlay.classList.remove('hidden');
    overlay.classList.add('flex');
    setTimeout(function () {
      if (dom.busca) {
        dom.busca.value = '';
        try {
          dom.busca.focus();
        } catch (e) {}
      }
    }, 40);
  }

  function renderCarrinho() {
    if (dom.cartCnt) dom.cartCnt.textContent = '(' + carrinho.length + ')';
    if (dom.btnOk) dom.btnOk.disabled = carrinho.length === 0;
    if (!dom.cart) return;
    if (!carrinho.length) {
      dom.cart.innerHTML =
        '<div class="p-10 text-center text-sm font-bold text-slate-400">—</div>';
      return;
    }
    var html =
      '<div class="tf-grid-head sticky top-0 z-[1] bg-slate-100 border-b border-slate-200 px-2.5 py-2 text-[10px] sm:text-[11px] font-black uppercase tracking-wide text-slate-500">' +
      '<div>Produto</div>' +
      '<div class="tf-col-codigo text-right">Cód.</div>' +
      '<div class="text-right">Vila</div>' +
      '<div class="tf-col-centro text-right">Centro</div>' +
      '<div class="text-center">Qtd</div>' +
      '<div></div>' +
      '</div>';
    carrinho.forEach(function (it, idx) {
      var vilaNeg = Number(it.saldo_vila || 0) <= 0;
      var vilaCls = vilaNeg ? 'text-amber-700' : 'text-slate-800';
      var cod = it.codigo_interno || it.codigo_barras || it.produto_id || '—';
      html +=
        '<div class="tf-grid-row px-2.5 py-2 border-b border-slate-100 hover:bg-emerald-50/50" data-tf-idx="' +
        idx +
        '">' +
        '<div class="min-w-0"><div class="text-sm sm:text-[15px] font-black text-slate-900 leading-snug uppercase truncate" title="' +
        esc(it.nome) +
        '">' +
        esc(it.nome) +
        '</div></div>' +
        '<div class="tf-col-codigo text-right text-xs font-bold text-slate-600 tabular-nums truncate" title="' +
        esc(cod) +
        '">' +
        esc(cod) +
        '</div>' +
        '<div class="text-right text-sm font-black tabular-nums ' +
        vilaCls +
        '">' +
        fmtSaldo(it.saldo_vila) +
        '</div>' +
        '<div class="tf-col-centro text-right text-sm font-black tabular-nums text-slate-800">' +
        fmtSaldo(it.saldo_centro) +
        '</div>' +
        '<div class="flex justify-center">' +
        '<input type="number" min="0.001" step="any" inputmode="decimal" data-tf-qtd="' +
        idx +
        '" value="' +
        (Number(it.quantidade) || 1) +
        '" class="w-full max-w-[5rem] rounded-lg border-2 border-emerald-400 bg-emerald-50 text-center text-base sm:text-lg font-black text-emerald-950 py-1.5 outline-none focus:ring-2 focus:ring-emerald-400/50" />' +
        '</div>' +
        '<div class="flex justify-end">' +
        '<button type="button" data-tf-rm="' +
        idx +
        '" class="shrink-0 w-8 h-8 inline-flex items-center justify-center text-sm font-black text-red-600 border border-red-200 rounded-lg bg-red-50 hover:bg-red-100" title="Remover">✕</button>' +
        '</div></div>';
    });
    dom.cart.innerHTML = html;
  }

  function focarQtd(idx) {
    requestAnimationFrame(function () {
      var inp = document.querySelector('#pdv-tf-carrinho input[data-tf-qtd="' + idx + '"]');
      if (!inp) return;
      inp.focus();
      try {
        inp.select();
      } catch (e) {}
      try {
        inp.scrollIntoView({ block: 'nearest' });
      } catch (e2) {}
    });
  }

  function voltarBusca() {
    if (dom.status) dom.status.textContent = '';
    if (dom.res) {
      dom.res.innerHTML =
        '<div class="p-6 text-center text-sm font-bold text-slate-400">—</div>';
    }
    if (dom.busca) {
      dom.busca.value = '';
      dom.busca.focus();
    }
  }

  function alterarQtd(idx, val) {
    if (idx < 0 || idx >= carrinho.length) return;
    var q = parseFloat(String(val).replace(',', '.'));
    if (Number.isNaN(q) || q <= 0) {
      alert('Quantidade inválida.');
      renderCarrinho();
      focarQtd(idx);
      return;
    }
    carrinho[idx].quantidade = q;
  }

  function addOuSomar(produto, opts) {
    if (!produto || !produto.produto_id) return -1;
    var pid = String(produto.produto_id);
    var forcarQtd = opts && opts.quantidade != null;
    var qtdForcada = 1;
    if (forcarQtd) {
      var q = Number(opts.quantidade);
      qtdForcada = !Number.isNaN(q) && q > 0 ? q : 1;
    }
    var idx = carrinho.findIndex(function (x) {
      return String(x.produto_id) === pid;
    });
    if (idx >= 0) {
      if (forcarQtd) {
        carrinho[idx].quantidade = Number(carrinho[idx].quantidade || 0) + qtdForcada;
      }
      if (produto.saldo_vila != null) carrinho[idx].saldo_vila = produto.saldo_vila;
      if (produto.saldo_centro != null) carrinho[idx].saldo_centro = produto.saldo_centro;
    } else {
      carrinho.push({
        produto_id: pid,
        nome: produto.nome || 'Produto ' + pid,
        codigo_interno: produto.codigo_interno || '',
        codigo_barras: produto.codigo_barras || '',
        quantidade: forcarQtd ? qtdForcada : 1,
        saldo_vila: Number(produto.saldo_vila || 0),
        saldo_centro: Number(produto.saldo_centro || 0),
      });
      idx = carrinho.length - 1;
    }
    renderCarrinho();
    return idx;
  }

  function mapBusca(p) {
    return {
      produto_id: String(p.id || p.produto_id || ''),
      nome: p.nome || '',
      codigo_interno: p.codigo || p.codigo_interno || p.codigo_nfe || '',
      codigo_barras: p.codigo_barras || p.ean || '',
      quantidade: 1,
      saldo_vila: Number(p.saldo_vila || 0),
      saldo_centro: Number(p.saldo_centro || 0),
    };
  }

  function termoPareceBip(termo) {
    var q = String(termo || '')
      .trim()
      .replace(/\s+/g, '');
    return /^\d{8,}$/.test(q);
  }

  function renderResultados() {
    if (!dom.res) return;
    if (!resultados.length) {
      dom.res.innerHTML =
        '<div class="p-6 text-center text-sm font-bold text-slate-400">0</div>';
      return;
    }
    var html =
      '<table class="tf-busca-table">' +
      '<colgroup><col class="tf-c-prod"><col class="tf-c-cod"><col class="tf-c-v"><col class="tf-c-c"></colgroup>' +
      '<thead><tr><th>Produto</th><th>Cód.</th><th>Vila</th><th>Centro</th></tr></thead><tbody>';
    resultados.forEach(function (p, i) {
      var hi = i === highlightIdx ? ' tf-hi' : '';
      var vilaNeg = Number(p.saldo_vila || 0) <= 0;
      var vilaCls = vilaNeg ? 'tf-saldo tf-neg' : 'tf-saldo';
      var cod = p.codigo_interno || p.codigo_barras || p.produto_id || '—';
      html +=
        '<tr role="option" data-tf-res="' +
        i +
        '" class="' +
        hi.trim() +
        '">' +
        '<td class="tf-nome" title="' +
        esc(p.nome) +
        '">' +
        esc(p.nome) +
        '</td>' +
        '<td class="tf-cod" title="' +
        esc(cod) +
        '">' +
        esc(cod) +
        '</td>' +
        '<td class="' +
        vilaCls +
        '">' +
        fmtSaldo(p.saldo_vila) +
        '</td>' +
        '<td class="tf-saldo">' +
        fmtSaldo(p.saldo_centro) +
        '</td></tr>';
    });
    html += '</tbody></table>';
    dom.res.innerHTML = html;
    var hiEl = dom.res.querySelector('[data-tf-res="' + highlightIdx + '"]');
    if (hiEl && hiEl.scrollIntoView) hiEl.scrollIntoView({ block: 'nearest' });
  }

  function escolherResultado(idx, opts) {
    if (idx < 0 || idx >= resultados.length) return;
    var fromBip = !!(opts && opts.fromBip);
    var cartIdx = fromBip
      ? addOuSomar(resultados[idx], { quantidade: 1 })
      : addOuSomar(resultados[idx]);
    if (dom.status) dom.status.textContent = '';
    if (dom.busca) dom.busca.value = '';
    resultados = [];
    highlightIdx = -1;
    if (dom.res) {
      dom.res.innerHTML =
        '<div class="p-6 text-center text-sm font-bold text-slate-400">—</div>';
    }
    if (cartIdx < 0) return;
    if (fromBip) voltarBusca();
    else focarQtd(cartIdx);
  }

  async function executarBusca(termo) {
    var q = String(termo || '').trim();
    if (q.length < 1) {
      resultados = [];
      highlightIdx = -1;
      if (dom.res) {
        dom.res.innerHTML =
          '<div class="p-6 text-center text-sm font-bold text-slate-400">—</div>';
      }
      if (dom.status) dom.status.textContent = '';
      return;
    }
    var seq = ++buscaSeq;
    if (dom.status) dom.status.textContent = '…';
    try {
      var response = await fetch('/api/buscar/?q=' + encodeURIComponent(q), {
        credentials: 'same-origin',
      });
      var data = await response.json();
      if (seq !== buscaSeq) return;
      var lista = (data.produtos || [])
        .slice(0, 40)
        .map(mapBusca)
        .filter(function (p) {
          return p.produto_id;
        });
      resultados = lista;
      highlightIdx = lista.length ? 0 : -1;
      if (dom.status) dom.status.textContent = lista.length ? lista.length + '' : '0';
      var qDigits = q.replace(/\s+/g, '');
      if (lista.length === 1) {
        var only = lista[0];
        var codes = [only.codigo_interno, only.codigo_barras, only.produto_id]
          .map(function (x) {
            return String(x || '')
              .trim()
              .toLowerCase();
          })
          .filter(Boolean);
        var isBarcodeLike = termoPareceBip(qDigits);
        if (codes.indexOf(qDigits.toLowerCase()) >= 0 || isBarcodeLike) {
          escolherResultado(0, { fromBip: isBarcodeLike });
          return;
        }
      }
      renderResultados();
    } catch (e) {
      if (seq !== buscaSeq) return;
      if (dom.status) dom.status.textContent = 'erro';
      if (dom.res) {
        dom.res.innerHTML =
          '<div class="p-6 text-center text-sm font-bold text-red-500">erro</div>';
      }
    }
  }

  function agendarBusca() {
    if (!dom.busca) return;
    clearTimeout(buscaTimer);
    buscaTimer = setTimeout(function () {
      executarBusca(dom.busca.value);
    }, 220);
  }

  function parseLinhasColar(texto) {
    var linhas = [];
    String(texto || '')
      .split(/\r?\n/)
      .forEach(function (raw) {
        var line = String(raw || '').trim();
        if (!line) return;
        var parts = line.split(/[\s\t;,|]+/).filter(Boolean);
        if (!parts.length) return;
        var codigo = parts[0];
        var qtd = 1;
        if (parts.length >= 2) {
          var maybeQ = parseFloat(String(parts[parts.length - 1]).replace(',', '.'));
          if (!Number.isNaN(maybeQ) && maybeQ > 0) {
            qtd = maybeQ;
            codigo = parts.slice(0, -1).join(' ');
          }
        }
        if (codigo) linhas.push({ codigo: codigo.trim(), quantidade: qtd });
      });
    return linhas;
  }

  async function aplicarColarLista() {
    var linhas = parseLinhasColar(dom.colar ? dom.colar.value : '');
    if (!linhas.length) {
      if (dom.colarSt) dom.colarSt.textContent = 'Cole ao menos uma linha com código.';
      return;
    }
    if (dom.colarSt) dom.colarSt.textContent = 'Resolvendo ' + linhas.length + ' linha(s)…';
    try {
      if (window.gmLoadingBar) window.gmLoadingBar.show();
      var res = await fetch('/estoque/api_resolver_codigos_transferencia_forcada/', {
        method: 'POST',
        credentials: 'same-origin',
        headers: {
          'Content-Type': 'application/json',
          'X-CSRFToken': csrf(),
          Accept: 'application/json',
        },
        body: JSON.stringify({ linhas: linhas }),
      });
      var data = await res.json();
      if (!data.ok) {
        if (dom.colarSt) dom.colarSt.textContent = data.erro || 'Não foi possível resolver.';
        alert(data.erro || 'Não foi possível resolver a lista.');
        return;
      }
      var lastIdx = -1;
      (data.itens || []).forEach(function (it) {
        lastIdx = addOuSomar(it, { quantidade: it.quantidade });
      });
      var nOk = (data.itens || []).length;
      var nFail = (data.nao_encontrados || []).length;
      var msg = nOk + ' no carrinho';
      if (nFail) msg += ' · ' + nFail + ' não achado(s)';
      if (dom.colarSt) dom.colarSt.textContent = msg;
      if (dom.colar && nOk) dom.colar.value = '';
      if (lastIdx >= 0) focarQtd(lastIdx);
    } catch (e) {
      if (dom.colarSt) dom.colarSt.textContent = 'Erro de rede.';
      alert('Erro ao resolver lista.');
    } finally {
      if (window.gmLoadingBar) window.gmLoadingBar.hide();
    }
  }

  async function confirmarTransferencia() {
    if (!carrinho.length) {
      alert('Carrinho vazio.');
      return;
    }
    var i;
    for (i = 0; i < carrinho.length; i++) {
      var q = Number(carrinho[i].quantidade);
      if (Number.isNaN(q) || q <= 0) {
        alert('Quantidade inválida em: ' + (carrinho[i].nome || ''));
        return;
      }
    }
    var origemKey = direcao === 'centro_vila' ? 'saldo_centro' : 'saldo_vila';
    var origemNome = direcao === 'centro_vila' ? 'Centro' : 'Vila';
    var comAviso = carrinho.filter(function (x) {
      return Number(x[origemKey] || 0) <= 0;
    });
    var msgConfirm =
      'Transferir ' + carrinho.length + ' item(ns) ' + rotuloDirecao() + ' no Agro?';
    if (comAviso.length) {
      msgConfirm +=
        '\n\nAtenção: ' +
        comAviso.length +
        ' com saldo ' +
        origemNome +
        ' zerado/negativo (pode ficar mais negativo).';
    }
    if (!confirm(msgConfirm)) return;
    try {
      var pin = await pedirPin('PIN para transferência forçada ' + rotuloDirecao());
      if (!pin) return;
      if (window.gmLoadingBar) window.gmLoadingBar.show();
      var res = await fetch('/estoque/api_transferir_forcado_vila_para_centro/', {
        method: 'POST',
        credentials: 'same-origin',
        headers: {
          'Content-Type': 'application/json',
          'X-CSRFToken': csrf(),
          Accept: 'application/json',
        },
        body: JSON.stringify({
          pin: pin,
          direcao: direcao,
          itens: carrinho.map(function (it) {
            return {
              produto_id: it.produto_id,
              quantidade: it.quantidade,
              nome_produto: it.nome,
              codigo_interno: it.codigo_interno || '',
            };
          }),
        }),
      });
      var raw = await res.text();
      var data = {};
      try {
        data = raw ? JSON.parse(raw) : {};
      } catch (_) {
        data = {};
      }
      if (!res.ok && !data.mensagem) {
        alert(data.erro || 'Erro ' + res.status);
        return;
      }
      var msg = data.mensagem || '';
      if (data.falhas && data.falhas.length) {
        msg +=
          '\n\nFalhas:\n' +
          data.falhas
            .slice(0, 15)
            .map(function (f) {
              return (f.nome || f.produto_id || '?') + ': ' + (f.erro || '');
            })
            .join('\n');
      }
      alert((data.ok ? 'Concluído.\n' : 'Concluído com avisos.\n') + msg);
      if (data.transferidos && data.transferidos.length) {
        var okIds = {};
        data.transferidos.forEach(function (t) {
          okIds[String(t.produto_id)] = true;
        });
        carrinho = carrinho.filter(function (c) {
          return !okIds[String(c.produto_id)];
        });
        renderCarrinho();
      }
      if (!carrinho.length) {
        returnToEscolha = false;
        fecharOverlay({ voltarEscolha: false });
      }
    } catch (e) {
      alert('Erro de comunicação: ' + (e.message || String(e)));
    } finally {
      if (window.gmLoadingBar) window.gmLoadingBar.hide();
    }
  }

  /* —— binds —— */
  if (dom.dirVc) {
    dom.dirVc.addEventListener('click', function () {
      iniciar('vila_centro');
    });
  }
  if (dom.dirCv) {
    dom.dirCv.addEventListener('click', function () {
      iniciar('centro_vila');
    });
  }
  if (dom.dirCancel) {
    dom.dirCancel.addEventListener('click', function () {
      fecharDirecao({ voltarEscolha: true });
    });
  }
  if (dom.fechar) {
    dom.fechar.addEventListener('click', function () {
      fecharOverlay({ voltarEscolha: true });
    });
  }
  if (dom.cancelar) {
    dom.cancelar.addEventListener('click', function () {
      fecharOverlay({ voltarEscolha: true });
    });
  }
  if (dom.limpar) {
    dom.limpar.addEventListener('click', function () {
      if (!carrinho.length) return;
      if (!confirm('Limpar o carrinho?')) return;
      carrinho = [];
      renderCarrinho();
    });
  }
  if (dom.btnOk) dom.btnOk.addEventListener('click', confirmarTransferencia);
  if (dom.colarAplicar) dom.colarAplicar.addEventListener('click', aplicarColarLista);

  if (dom.cart) {
    dom.cart.addEventListener('change', function (e) {
      var inp = e.target.closest('[data-tf-qtd]');
      if (!inp) return;
      alterarQtd(Number(inp.getAttribute('data-tf-qtd')), inp.value);
    });
    dom.cart.addEventListener('keydown', function (e) {
      var inp = e.target.closest('[data-tf-qtd]');
      if (!inp || e.key !== 'Enter') return;
      e.preventDefault();
      e.stopPropagation();
      alterarQtd(Number(inp.getAttribute('data-tf-qtd')), inp.value);
      voltarBusca();
    });
    dom.cart.addEventListener('click', function (e) {
      var rm = e.target.closest('[data-tf-rm]');
      if (!rm) return;
      var idx = Number(rm.getAttribute('data-tf-rm'));
      if (idx < 0 || idx >= carrinho.length) return;
      carrinho.splice(idx, 1);
      renderCarrinho();
    });
    dom.cart.addEventListener('focusin', function (e) {
      var inp = e.target.closest('[data-tf-qtd]');
      if (inp) {
        try {
          inp.select();
        } catch (err) {}
      }
    });
  }

  if (dom.res) {
    dom.res.addEventListener('click', function (e) {
      var tr = e.target.closest('[data-tf-res]');
      if (!tr) return;
      escolherResultado(Number(tr.getAttribute('data-tf-res')));
    });
  }

  if (dom.busca) {
    dom.busca.addEventListener('input', agendarBusca);
    dom.busca.addEventListener('keydown', function (e) {
      if (e.key === 'ArrowDown') {
        e.preventDefault();
        if (!resultados.length) return;
        highlightIdx = Math.min(resultados.length - 1, highlightIdx + 1);
        renderResultados();
      } else if (e.key === 'ArrowUp') {
        e.preventDefault();
        if (!resultados.length) return;
        highlightIdx = Math.max(0, highlightIdx - 1);
        renderResultados();
      } else if (e.key === 'Enter') {
        e.preventDefault();
        var fromBip = termoPareceBip(dom.busca.value);
        if (highlightIdx >= 0 && resultados[highlightIdx]) {
          escolherResultado(highlightIdx, { fromBip: fromBip });
        } else {
          clearTimeout(buscaTimer);
          executarBusca(dom.busca.value);
        }
      }
    });
  }

  if (dom.pinOk) {
    dom.pinOk.addEventListener('click', function () {
      fecharPin(true);
    });
  }
  if (dom.pinCancel) {
    dom.pinCancel.addEventListener('click', function () {
      fecharPin(false);
    });
  }
  if (dom.pinInput) {
    dom.pinInput.addEventListener('keydown', function (e) {
      if (e.key === 'Enter') {
        e.preventDefault();
        fecharPin(true);
      }
    });
  }

  document.addEventListener(
    'keydown',
    function (e) {
      if (e.key === 'Enter' && isDirOpen() && !isOverlayOpen() && !isPinOpen()) {
        e.preventDefault();
        e.stopPropagation();
        var hero = elDir.querySelector('.pdv-tf-dir-btn--hero');
        if (hero) hero.click();
        return;
      }
      if (e.key !== 'Escape') return;
      if (isPinOpen()) {
        e.preventDefault();
        e.stopPropagation();
        fecharPin(false);
        return;
      }
      if (isOverlayOpen()) {
        e.preventDefault();
        e.stopPropagation();
        fecharOverlay({ voltarEscolha: true });
        return;
      }
      if (isDirOpen()) {
        e.preventDefault();
        e.stopPropagation();
        fecharDirecao({ voltarEscolha: true });
      }
    },
    true
  );

  window.PdvTransfForcada = {
    abrirDirecao: abrirDirecao,
    fechar: function () {
      fecharPin(false);
      fecharOverlay({ voltarEscolha: false });
      fecharDirecao({ voltarEscolha: false });
      showBackdrop(false);
    },
  };
})();
