/**
 * PDV — overlay Uso loja (saída estoque consumo interno).
 * Independente do carrinho de venda.
 * Fluxo: lista → Confirmar → pop quem → pop motivo → pop PIN.
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
  var draft = { quem: '', motivo: '', pin: '' };
  var stepName = null; // quem | motivo | pin
  var hitList = [];
  var hitSelectionIndex = -1;
  var funcionarios = [];
  var quemOutrosMode = false;

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
    limpar: document.getElementById('pdv-uso-loja-limpar'),
    confirmar: document.getElementById('pdv-uso-loja-confirmar'),
    status: document.getElementById('pdv-uso-loja-status'),
    histList: document.getElementById('pdv-uso-loja-hist-list'),
    histStatus: document.getElementById('pdv-uso-loja-hist-status'),
    stepPop: document.getElementById('pdv-uso-loja-step-pop'),
    stepEyebrow: document.getElementById('pdv-uso-loja-step-eyebrow'),
    stepTitle: document.getElementById('pdv-uso-loja-step-title'),
    stepHint: document.getElementById('pdv-uso-loja-step-hint'),
    bodyQuem: document.getElementById('pdv-uso-loja-step-body-quem'),
    bodyMotivo: document.getElementById('pdv-uso-loja-step-body-motivo'),
    bodyPin: document.getElementById('pdv-uso-loja-step-body-pin'),
    stepQuem: document.getElementById('pdv-uso-loja-step-quem'),
    stepQuemOutrosWrap: document.getElementById('pdv-uso-loja-step-quem-outros-wrap'),
    quemGrid: document.getElementById('pdv-uso-loja-step-quem-grid'),
    stepPin: document.getElementById('pdv-uso-loja-step-pin'),
    stepPinErr: document.getElementById('pdv-uso-loja-step-pin-err'),
    stepPular: document.getElementById('pdv-uso-loja-step-pular'),
    stepOk: document.getElementById('pdv-uso-loja-step-ok'),
    motivoGrid: document.getElementById('pdv-uso-loja-step-motivo-grid'),
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

  function escapeHtml(s) {
    return String(s || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function renderCart() {
    if (!dom.cart) return;
    var rows = cart.slice();
    if (!rows.length) {
      dom.cart.innerHTML =
        '<p id="pdv-uso-loja-cart-empty" class="py-4 text-center text-sm font-semibold text-slate-500">Nenhum item — busque acima.</p>';
      return;
    }
    var body = rows
      .map(function (it, idx) {
        return (
          '<tr data-idx="' +
          idx +
          '">' +
          '<td class="ul-td-gm">' +
          escapeHtml(it.codigo || '—') +
          '</td>' +
          '<td class="ul-td-nome" title="' +
          escapeHtml(it.nome || it.produto_id) +
          '">' +
          escapeHtml(it.nome || it.produto_id) +
          '</td>' +
          '<td class="ul-td-preco">' +
          escapeHtml(fmtMoney(it.preco_venda)) +
          '</td>' +
          '<td class="ul-td-qtd">' +
          '<input type="number" min="0.001" step="any" class="ul-field tabular-nums" data-ul-qtd="' +
          idx +
          '" value="' +
          fmtQtd(it.quantidade) +
          '" />' +
          '</td>' +
          '<td class="ul-td-rm">' +
          '<button type="button" class="ul-cart-rm" data-ul-rm="' +
          idx +
          '" title="Remover">×</button>' +
          '</td>' +
          '</tr>'
        );
      })
      .join('');
    dom.cart.innerHTML =
      '<table class="ul-cart-table">' +
      '<colgroup>' +
      '<col class="ul-col-gm" />' +
      '<col class="ul-col-nome" />' +
      '<col class="ul-col-preco" />' +
      '<col class="ul-col-qtd" />' +
      '<col class="ul-col-rm" />' +
      '</colgroup>' +
      '<thead><tr>' +
      '<th scope="col">Código GM</th>' +
      '<th scope="col">Nome produto</th>' +
      '<th scope="col" class="ul-th-preco">Preço</th>' +
      '<th scope="col" class="ul-th-qtd">Qtd</th>' +
      '<th scope="col"></th>' +
      '</tr></thead>' +
      '<tbody>' +
      body +
      '</tbody></table>';
  }

  function addProduct(p) {
    var pid = String(p.id || p.produto_id || '').trim();
    if (!pid) return;
    var nome = String(p.nome || p.name || pid).trim();
    var codigo = String(p.codigo_gm || p.codigo_nfe || p.codigo || p.gm || '').trim();
    var preco = precoProduto(p);
    var found = null;
    for (var i = 0; i < cart.length; i++) {
      if (cart[i].produto_id === pid) {
        found = cart[i];
        break;
      }
    }
    if (found) {
      found.quantidade = Number(found.quantidade || 0) + 1;
      if (found.preco_venda == null && preco != null) found.preco_venda = preco;
      if (!found.codigo && codigo) found.codigo = codigo;
    } else {
      cart.push({
        produto_id: pid,
        nome: nome,
        codigo: codigo,
        preco_venda: preco,
        quantidade: 1,
      });
    }
    if (dom.busca) dom.busca.value = '';
    hideHits();
    renderCart();
    setStatus(nome + ' adicionado.');
  }

  function fmtMoney(v) {
    var n = Number(v);
    if (!isFinite(n)) return '—';
    try {
      return n.toLocaleString('pt-BR', {
        style: 'currency',
        currency: 'BRL',
      });
    } catch (e) {
      return 'R$ ' + n.toFixed(2).replace('.', ',');
    }
  }

  function precoProduto(p) {
    if (!p) return null;
    if (p.preco_venda != null && p.preco_venda !== '') return p.preco_venda;
    if (p.preco != null && p.preco !== '') return p.preco;
    if (p.PrecoVenda != null && p.PrecoVenda !== '') return p.PrecoVenda;
    return null;
  }

  function hideHits() {
    if (!dom.hits) return;
    dom.hits.innerHTML = '';
    dom.hits.classList.add('hidden');
    dom.hits.style.top = '';
    dom.hits.style.bottom = '';
    hitList = [];
    hitSelectionIndex = -1;
  }

  function positionHits() {
    if (!dom.hits || dom.hits.classList.contains('hidden')) return;
    var panel =
      document.getElementById('pdv-uso-loja-panel') ||
      (overlay && overlay.querySelector('[role="document"]'));
    var wrap = overlay && overlay.querySelector('.ul-busca-wrap');
    if (!panel || !wrap) return;
    var pr = panel.getBoundingClientRect();
    var wr = wrap.getBoundingClientRect();
    var topPx = Math.max(0, wr.bottom - pr.top + 6);
    dom.hits.style.top = topPx + 'px';
    dom.hits.style.bottom = '0.45rem';
    dom.hits.style.height = 'auto';
    dom.hits.style.maxHeight = 'none';
  }

  function syncHitSelection() {
    if (!dom.hits) return;
    var rows = dom.hits.querySelectorAll('tbody tr[data-ul-add]');
    rows.forEach(function (row, i) {
      row.classList.toggle('ul-hit-selected', i === hitSelectionIndex);
    });
    if (hitSelectionIndex >= 0 && rows[hitSelectionIndex]) {
      try {
        rows[hitSelectionIndex].scrollIntoView({ block: 'nearest' });
      } catch (e) {}
    }
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
        '<div class="ul-hits-scroll"><p class="text-sm font-semibold text-slate-700 px-2 py-2">Nenhum produto.</p></div>';
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
        var preco = fmtMoney(precoProduto(p));
        var sel = i === hitSelectionIndex ? ' ul-hit-selected' : '';
        return (
          '<tr data-ul-add="' +
          escapeHtml(pid) +
          '" data-ul-idx="' +
          i +
          '" tabindex="-1" role="option" class="' +
          sel.trim() +
          '">' +
          '<td class="ul-td-gm">' +
          escapeHtml(cod) +
          '</td>' +
          '<td class="ul-td-nome" title="' +
          escapeHtml(nome) +
          '">' +
          escapeHtml(nome) +
          '</td>' +
          '<td class="ul-td-preco">' +
          escapeHtml(preco) +
          '</td>' +
          '<td class="ul-td-add"><span class="ul-hit-plus" aria-hidden="true">+</span></td>' +
          '</tr>'
        );
      })
      .join('');
    dom.hits.innerHTML =
      '<div class="ul-hits-scroll">' +
      '<table class="ul-hits-table">' +
      '<colgroup>' +
      '<col class="ul-col-gm" />' +
      '<col class="ul-col-nome" />' +
      '<col class="ul-col-preco" />' +
      '<col class="ul-col-add" />' +
      '</colgroup>' +
      '<thead><tr>' +
      '<th scope="col">Código GM</th>' +
      '<th scope="col">Nome produto</th>' +
      '<th scope="col" class="ul-th-preco">Preço</th>' +
      '<th scope="col"></th>' +
      '</tr></thead>' +
      '<tbody>' +
      rows +
      '</tbody></table></div>';
    dom.hits.classList.remove('hidden');
    positionHits();
    syncHitSelection();
    dom.hits.querySelectorAll('[data-ul-add]').forEach(function (row) {
      row.addEventListener('click', function () {
        var i = parseInt(row.getAttribute('data-ul-idx'), 10);
        if (!isNaN(i)) pickHitByIndex(i);
      });
      row.addEventListener('mouseenter', function () {
        var i = parseInt(row.getAttribute('data-ul-idx'), 10);
        if (isNaN(i)) return;
        hitSelectionIndex = i;
        syncHitSelection();
      });
    });
  }

  function buscar(q) {
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
        renderHits(lista);
      })
      .catch(function () {
        if (dom.hits) {
          dom.hits.innerHTML =
            '<div class="ul-hits-scroll"><p class="text-sm font-semibold text-red-600 px-2 py-2">Falha na busca.</p></div>';
          dom.hits.classList.remove('hidden');
          positionHits();
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
          funcionarios = Array.isArray(data.funcionarios) ? data.funcionarios : [];
          renderQuemGrid();
          syncDepBtns();
        }
        if (typeof cb === 'function') cb();
      })
      .catch(function () {
        syncDepBtns();
        if (typeof cb === 'function') cb();
      });
  }

  function hideStepPop() {
    stepName = null;
    quemOutrosMode = false;
    if (dom.stepPop) {
      dom.stepPop.classList.add('hidden');
      dom.stepPop.removeAttribute('data-quem-outros');
    }
  }

  function syncMotivoBtns() {
    if (!dom.motivoGrid) return;
    dom.motivoGrid.querySelectorAll('[data-motivo]').forEach(function (btn) {
      btn.classList.toggle(
        'is-on',
        btn.getAttribute('data-motivo') === draft.motivo
      );
    });
  }

  function renderQuemGrid() {
    if (!dom.quemGrid) return;
    var html = funcionarios
      .map(function (f) {
        var nome = String((f && f.nome) || '').trim();
        if (!nome) return '';
        return (
          '<button type="button" class="ul-step-quem-btn" data-quem-nome="' +
          escapeHtml(nome) +
          '">' +
          escapeHtml(nome) +
          '</button>'
        );
      })
      .join('');
    html +=
      '<button type="button" class="ul-step-quem-btn" data-quem-outros="1">Outros</button>';
    if (!funcionarios.length) {
      html =
        '<p class="col-span-2 px-1 py-2 text-sm font-semibold text-slate-500">Nenhum funcionário ativo no RH — use Outros ou Pular.</p>' +
        html;
    }
    dom.quemGrid.innerHTML = html;
    syncQuemBtns();
  }

  function syncQuemBtns() {
    if (!dom.quemGrid) return;
    dom.quemGrid.querySelectorAll('.ul-step-quem-btn').forEach(function (btn) {
      var isOutros = btn.getAttribute('data-quem-outros') === '1';
      var on = isOutros
        ? quemOutrosMode
        : !quemOutrosMode &&
          btn.getAttribute('data-quem-nome') === draft.quem;
      btn.classList.toggle('is-on', on);
    });
  }

  function setQuemOutrosMode(on) {
    quemOutrosMode = !!on;
    if (dom.stepPop) {
      if (quemOutrosMode) dom.stepPop.setAttribute('data-quem-outros', '1');
      else dom.stepPop.removeAttribute('data-quem-outros');
    }
    if (dom.stepQuemOutrosWrap) {
      dom.stepQuemOutrosWrap.classList.toggle('hidden', !quemOutrosMode);
    }
    syncQuemBtns();
    if (quemOutrosMode && dom.stepQuem) {
      try {
        dom.stepQuem.focus();
        dom.stepQuem.select();
      } catch (e) {}
    }
  }

  function pickQuemFuncionario(nome) {
    draft.quem = String(nome || '').trim();
    setQuemOutrosMode(false);
    if (dom.stepQuem) dom.stepQuem.value = '';
    showStep('motivo');
  }

  function showStep(name) {
    stepName = name;
    if (!dom.stepPop) return;
    dom.stepPop.classList.remove('hidden');
    dom.stepPop.setAttribute('data-step', name);
    if (name !== 'quem') {
      quemOutrosMode = false;
      dom.stepPop.removeAttribute('data-quem-outros');
    }
    if (dom.bodyQuem) dom.bodyQuem.classList.toggle('hidden', name !== 'quem');
    if (dom.bodyMotivo) dom.bodyMotivo.classList.toggle('hidden', name !== 'motivo');
    if (dom.bodyPin) dom.bodyPin.classList.toggle('hidden', name !== 'pin');

    var isPin = name === 'pin';
    if (dom.stepPular) {
      dom.stepPular.classList.toggle('hidden', isPin);
      dom.stepPular.style.display = isPin ? 'none' : '';
      dom.stepPular.setAttribute('aria-hidden', isPin ? 'true' : 'false');
      if (!isPin) {
        dom.stepPular.innerHTML =
          'Pular <kbd class="ml-1 rounded border border-slate-300 bg-slate-50 px-1 font-mono text-[10px] normal-case tracking-normal">Enter</kbd>';
      }
    }
    if (dom.stepOk) {
      dom.stepOk.textContent = isPin ? 'Confirmar PIN' : 'Confirmar';
      dom.stepOk.style.flex = isPin ? '1 1 100%' : '';
      dom.stepOk.classList.toggle(
        'hidden',
        name === 'quem' && !quemOutrosMode
      );
    }
    if (dom.stepPinErr) {
      dom.stepPinErr.classList.add('hidden');
      dom.stepPinErr.textContent = '';
    }

    if (name === 'quem') {
      if (dom.stepEyebrow) dom.stepEyebrow.textContent = '1 de 3 · opcional';
      if (dom.stepTitle) dom.stepTitle.textContent = 'Quem levou?';
      if (dom.stepHint)
        dom.stepHint.textContent =
          'Toque o nome (avança) · Outros digita · Enter pula';
      setQuemOutrosMode(false);
      if (dom.stepQuem) dom.stepQuem.value = '';
      if (dom.stepOk) dom.stepOk.classList.add('hidden');
      renderQuemGrid();
      try {
        if (dom.stepPular) dom.stepPular.focus();
      } catch (e) {}
    } else if (name === 'motivo') {
      if (dom.stepEyebrow) dom.stepEyebrow.textContent = '2 de 3 · opcional';
      if (dom.stepTitle) dom.stepTitle.textContent = 'Motivo?';
      if (dom.stepHint) dom.stepHint.textContent = 'Toque um motivo ou Pular · Enter pula';
      if (dom.stepOk) dom.stepOk.classList.remove('hidden');
      syncMotivoBtns();
      try {
        if (dom.stepPular) dom.stepPular.focus();
      } catch (e2) {}
    } else if (name === 'pin') {
      if (dom.stepEyebrow) dom.stepEyebrow.textContent = '3 de 3 · obrigatório';
      if (dom.stepTitle) dom.stepTitle.textContent = 'PIN para confirmar';
      if (dom.stepHint) dom.stepHint.textContent = 'Digite o PIN e confirme a saída — obrigatório';
      if (dom.stepOk) dom.stepOk.classList.remove('hidden');
      if (dom.stepPin) {
        dom.stepPin.value = '';
        try {
          dom.stepPin.focus();
        } catch (e3) {}
      }
    }
  }

  function openOverlay() {
    overlay.classList.remove('hidden');
    overlay.classList.add('flex');
    document.body.classList.add('modal-open');
    hideStepPop();
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
    hideStepPop();
    hideHits();
    overlay.classList.add('hidden');
    overlay.classList.remove('flex');
    document.body.classList.remove('modal-open');
    draft = { quem: '', motivo: '', pin: '' };
  }

  function startWizard() {
    if (busy) return;
    if (!cart.length) {
      setStatus('Adicione ao menos um produto.', true);
      return;
    }
    hideHits();
    draft = { quem: '', motivo: '', pin: '' };
    setStatus('');
    showStep('quem');
  }

  function advanceFromQuem(skip) {
    if (skip) {
      draft.quem = '';
    } else if (quemOutrosMode) {
      draft.quem = String((dom.stepQuem && dom.stepQuem.value) || '').trim();
    }
    setQuemOutrosMode(false);
    showStep('motivo');
  }

  function advanceFromMotivo(skip) {
    if (skip) draft.motivo = '';
    showStep('pin');
  }

  function gravarSaida() {
    if (busy) return;
    var pin = String((dom.stepPin && dom.stepPin.value) || draft.pin || '').trim();
    if (!pin) {
      if (dom.stepPinErr) {
        dom.stepPinErr.textContent = 'Informe o PIN.';
        dom.stepPinErr.classList.remove('hidden');
      }
      if (dom.stepPin) {
        try {
          dom.stepPin.focus();
        } catch (e) {}
      }
      return;
    }
    draft.pin = pin;
    busy = true;
    if (dom.stepOk) dom.stepOk.disabled = true;
    if (dom.stepPinErr) {
      dom.stepPinErr.classList.add('hidden');
      dom.stepPinErr.textContent = '';
    }
    setStatus('Gravando saída…');
    var payload = {
      pin: pin,
      deposito: deposito,
      quem_levou: draft.quem || '',
      motivo: draft.motivo || '',
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
        if (dom.stepOk) dom.stepOk.disabled = false;
        var j = pack.j || {};
        if (!j.ok) {
          if (dom.stepPinErr) {
            dom.stepPinErr.textContent = j.erro || 'Não foi possível gravar.';
            dom.stepPinErr.classList.remove('hidden');
          }
          setStatus(j.erro || 'Não foi possível gravar.', true);
          return;
        }
        cart = [];
        renderCart();
        draft = { quem: '', motivo: '', pin: '' };
        hideStepPop();
        setStatus(j.mensagem || 'Saída registrada.');
      })
      .catch(function () {
        busy = false;
        if (dom.stepOk) dom.stepOk.disabled = false;
        if (dom.stepPinErr) {
          dom.stepPinErr.textContent = 'Falha de rede ao gravar.';
          dom.stepPinErr.classList.remove('hidden');
        }
        setStatus('Falha de rede ao gravar.', true);
      });
  }

  function onStepOk() {
    if (stepName === 'quem') advanceFromQuem(false);
    else if (stepName === 'motivo') advanceFromMotivo(false);
    else if (stepName === 'pin') gravarSaida();
  }

  function onStepPular() {
    if (stepName === 'pin') return;
    if (stepName === 'quem') advanceFromQuem(true);
    else if (stepName === 'motivo') advanceFromMotivo(true);
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
      hideStepPop();
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
      hitSelectionIndex = -1;
      var q = dom.busca.value;
      searchTimer = setTimeout(function () {
        buscar(q);
      }, 220);
    });
    dom.busca.addEventListener('keydown', function (ev) {
      var hitsOpen = dom.hits && !dom.hits.classList.contains('hidden') && hitList.length;
      if (ev.key === 'ArrowDown') {
        if (!hitsOpen) return;
        ev.preventDefault();
        hitSelectionIndex = Math.min(
          (hitSelectionIndex < 0 ? -1 : hitSelectionIndex) + 1,
          hitList.length - 1
        );
        syncHitSelection();
        return;
      }
      if (ev.key === 'ArrowUp') {
        if (!hitsOpen) return;
        ev.preventDefault();
        hitSelectionIndex = Math.max(hitSelectionIndex - 1, 0);
        syncHitSelection();
        return;
      }
      if (ev.key === 'Enter') {
        ev.preventDefault();
        clearTimeout(searchTimer);
        if (hitsOpen && hitSelectionIndex >= 0) {
          pickHitByIndex(hitSelectionIndex);
          return;
        }
        buscar(dom.busca.value);
        return;
      }
      if (ev.key === 'Escape') {
        if (hitsOpen) {
          ev.preventDefault();
          hideHits();
        }
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
    dom.cart.addEventListener('focusin', function (ev) {
      var inp = ev.target.closest('[data-ul-qtd]');
      if (!inp) return;
      setTimeout(function () {
        try {
          inp.select();
        } catch (e) {}
      }, 0);
    });
    dom.cart.addEventListener('click', function (ev) {
      var inp = ev.target.closest('[data-ul-qtd]');
      if (!inp) return;
      setTimeout(function () {
        try {
          inp.select();
        } catch (e) {}
      }, 0);
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
  if (dom.confirmar) dom.confirmar.addEventListener('click', startWizard);
  if (dom.stepPular) dom.stepPular.addEventListener('click', onStepPular);
  if (dom.stepOk) dom.stepOk.addEventListener('click', onStepOk);
  if (dom.motivoGrid) {
    dom.motivoGrid.addEventListener('click', function (ev) {
      var btn = ev.target.closest('[data-motivo]');
      if (!btn) return;
      draft.motivo = btn.getAttribute('data-motivo') || '';
      syncMotivoBtns();
    });
  }
  if (dom.quemGrid) {
    dom.quemGrid.addEventListener('click', function (ev) {
      var btn = ev.target.closest('.ul-step-quem-btn');
      if (!btn) return;
      if (btn.getAttribute('data-quem-outros') === '1') {
        draft.quem = '';
        setQuemOutrosMode(true);
        if (dom.stepOk) dom.stepOk.classList.remove('hidden');
        if (dom.stepHint)
          dom.stepHint.textContent = 'Digite o nome e Confirmar · Enter pula';
        if (dom.stepQuem) dom.stepQuem.value = '';
        return;
      }
      pickQuemFuncionario(btn.getAttribute('data-quem-nome') || '');
    });
  }
  if (dom.stepQuem) {
    dom.stepQuem.addEventListener('keydown', function (ev) {
      if (ev.key === 'Enter') {
        ev.preventDefault();
        if (quemOutrosMode && String(dom.stepQuem.value || '').trim()) {
          advanceFromQuem(false);
        } else {
          onStepPular();
        }
      }
    });
  }
  if (dom.stepPin) {
    dom.stepPin.addEventListener('keydown', function (ev) {
      if (ev.key === 'Enter') {
        ev.preventDefault();
        gravarSaida();
      }
    });
  }
  document.addEventListener('keydown', function (ev) {
    if (overlay.classList.contains('hidden')) return;
    if (stepName && (stepName === 'quem' || stepName === 'motivo') && ev.key === 'Enter') {
      var t = ev.target;
      if (t && (t.id === 'pdv-uso-loja-step-quem' || t.id === 'pdv-uso-loja-step-pin')) {
        return;
      }
      ev.preventDefault();
      onStepPular();
      return;
    }
    if (ev.key === 'Escape') {
      if (stepName) {
        ev.preventDefault();
        hideStepPop();
        setStatus('Confirmação cancelada.');
        return;
      }
      closeOverlay();
    }
  });

  window.addEventListener('resize', function () {
    positionHits();
  });

  window.AgroPdvUsoLoja = {
    open: openOverlay,
    close: closeOverlay,
  };
})();
