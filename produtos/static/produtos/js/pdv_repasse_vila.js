/**
 * PDV — overlay Repasse Vila → Centro.
 * Abre com ?repasse=1 ou botão topbar / Retiradas.
 */
(function () {
  'use strict';

  var overlay = document.getElementById('pdv-repasse-overlay');
  if (!overlay) return;

  var calc = null;
  var funcionarios = [];
  var formasPagamento = ['Dinheiro', 'PIX', 'Cartão de débito', 'Cartão de crédito', 'Outro'];
  var quem = '';
  var formaPag = 'Dinheiro';
  var busy = false;

  var dom = {
    fechar: document.getElementById('pdv-repasse-fechar'),
    cancelar: document.getElementById('pdv-repasse-cancelar'),
    confirmar: document.getElementById('pdv-repasse-confirmar'),
    sub: document.getElementById('pdv-repasse-sub'),
    pct: document.getElementById('pdv-rp-pct'),
    cmv: document.getElementById('pdv-rp-cmv'),
    lucro: document.getElementById('pdv-rp-lucro'),
    fiado: document.getElementById('pdv-rp-fiado'),
    todos: document.getElementById('pdv-rp-todos'),
    cheio: document.getElementById('pdv-rp-cheio'),
    manual: document.getElementById('pdv-rp-manual'),
    pin: document.getElementById('pdv-rp-pin'),
    status: document.getElementById('pdv-rp-status'),
    quemGrid: document.getElementById('pdv-rp-quem-grid'),
    quemOutros: document.getElementById('pdv-rp-quem-outros'),
    formaGrid: document.getElementById('pdv-rp-forma-grid'),
  };

  function money(n) {
    return 'R$ ' + Number(n || 0).toLocaleString('pt-BR', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
  }

  function csrf() {
    var m = document.cookie.match(/csrftoken=([^;]+)/);
    return m ? m[1] : '';
  }

  function qs() {
    try {
      return new URLSearchParams(window.location.search || '');
    } catch (e) {
      return new URLSearchParams();
    }
  }

  /** Só dígitos / vírgula / ponto — limpa autofill tipo "renan". */
  function sanitizeManualField() {
    if (!dom.manual) return;
    var raw = String(dom.manual.value || '');
    if (!raw) return;
    if (/[a-zA-Z]/.test(raw)) {
      dom.manual.value = '';
      return;
    }
    var cleaned = raw.replace(/[^\d.,]/g, '');
    if (cleaned !== raw) dom.manual.value = cleaned;
  }

  function parseManualValor() {
    sanitizeManualField();
    var man = String(dom.manual.value || '').trim();
    if (!man) return null;
    var n = Number(man.replace(/\./g, '').replace(',', '.'));
    if (isNaN(n) || n <= 0) return null;
    return n;
  }

  function applyQueryPrefs() {
    var q = qs();
    if (q.get('pct')) dom.pct.value = q.get('pct');
    if (q.get('cmv') === '0') dom.cmv.checked = false;
    if (q.get('lucro') === '0') dom.lucro.checked = false;
    if (q.get('fiado') === '0') dom.fiado.checked = false;
    if (q.get('cheio') === '1') dom.cheio.checked = true;
    if (q.get('forma')) formaPag = q.get('forma');
    dom.todos.checked = dom.cmv.checked && dom.lucro.checked && dom.fiado.checked;
  }

  function renderCalc() {
    var c = calc || {};
    var d = c.disponivel || {};
    document.getElementById('pdv-rp-receita').textContent = money(c.receita_dia);
    document.getElementById('pdv-rp-cmv-dia').textContent = money(c.cmv_dia);
    document.getElementById('pdv-rp-lucro-dia').textContent = money(c.lucro_bruto_dia);
    document.getElementById('pdv-rp-fiado-dia').textContent = money(c.fiado_pago_dia);
    document.getElementById('pdv-rp-disp-cmv').textContent = money(d.cmv);
    document.getElementById('pdv-rp-disp-lucro').textContent = money(d.lucro);
    document.getElementById('pdv-rp-disp-fiado').textContent = money(d.fiado);
    var tot = 0;
    if (dom.cmv.checked) tot += Number(d.cmv || 0);
    if (dom.lucro.checked) tot += Number(d.lucro || 0);
    if (dom.fiado.checked) tot += Number(d.fiado || 0);
    var mv = parseManualValor();
    if (mv != null) tot = mv;
    document.getElementById('pdv-rp-total').textContent = money(tot);
  }

  function renderQuem() {
    dom.quemGrid.innerHTML = '';
    funcionarios.forEach(function (f) {
      var b = document.createElement('button');
      b.type = 'button';
      b.className = 'rp-quem-btn' + (quem === f.nome ? ' is-on' : '');
      b.textContent = f.nome;
      b.addEventListener('click', function () {
        quem = f.nome;
        dom.quemOutros.classList.add('hidden');
        dom.quemOutros.value = '';
        renderQuem();
      });
      dom.quemGrid.appendChild(b);
    });
    var outros = document.createElement('button');
    outros.type = 'button';
    outros.className = 'rp-quem-btn' + (dom.quemOutros && !dom.quemOutros.classList.contains('hidden') ? ' is-on' : '');
    outros.textContent = 'Outros';
    outros.addEventListener('click', function () {
      quem = '';
      dom.quemOutros.classList.remove('hidden');
      dom.quemOutros.focus();
      renderQuem();
    });
    dom.quemGrid.appendChild(outros);
  }

  function renderForma() {
    if (!dom.formaGrid) return;
    dom.formaGrid.innerHTML = '';
    if (!formasPagamento.length) formasPagamento = ['Dinheiro'];
    if (formasPagamento.indexOf(formaPag) < 0) formaPag = formasPagamento[0];
    formasPagamento.forEach(function (fn) {
      var b = document.createElement('button');
      b.type = 'button';
      b.className = 'rp-forma-btn' + (formaPag === fn ? ' is-on' : '');
      b.textContent = fn;
      b.addEventListener('click', function () {
        formaPag = fn;
        renderForma();
      });
      dom.formaGrid.appendChild(b);
    });
  }

  function fetchCalc() {
    var pct = dom.pct.value || '50';
    var cheio = dom.cheio.checked ? '1' : '0';
    return fetch('/api/repasse-vila/calc/?pct=' + encodeURIComponent(pct) + '&dia_cheio=' + cheio, {
      credentials: 'same-origin',
    })
      .then(function (r) {
        return r.json();
      })
      .then(function (j) {
        if (j && j.ok) {
          calc = j;
          renderCalc();
        }
      });
  }

  function openOverlay() {
    overlay.classList.remove('hidden');
    overlay.classList.add('flex');
    document.body.classList.add('modal-open');
    applyQueryPrefs();
    if (dom.manual) dom.manual.value = '';
    if (dom.pin) dom.pin.value = '';
    sanitizeManualField();
    dom.status.textContent = 'Carregando…';
    fetch('/api/repasse-vila/meta/', { credentials: 'same-origin' })
      .then(function (r) {
        return r.json();
      })
      .then(function (j) {
        if (!j || !j.ok) {
          dom.status.textContent = 'Falha ao carregar';
          return;
        }
        funcionarios = j.funcionarios || [];
        if (Array.isArray(j.formas_pagamento) && j.formas_pagamento.length) {
          formasPagamento = j.formas_pagamento;
        }
        if (!dom.pct.value || dom.pct.value === '50') {
          dom.pct.value = String(Math.round(j.percentual_padrao || 50));
        }
        calc = j.calc || null;
        if (!j.caixa_vila_aberto) {
          dom.sub.textContent = 'Caixa da Vila FECHADO — abra antes de transferir';
          dom.sub.classList.add('text-red-700');
        } else {
          dom.sub.textContent = 'Caixa Vila aberto · sai da Vila · entra no Centro';
          dom.sub.classList.remove('text-red-700');
        }
        renderQuem();
        renderForma();
        return fetchCalc();
      })
      .then(function () {
        sanitizeManualField();
        renderCalc();
        dom.status.textContent = '';
      })
      .catch(function () {
        dom.status.textContent = 'Falha de rede';
      });
  }

  function closeOverlay() {
    overlay.classList.add('hidden');
    overlay.classList.remove('flex');
    document.body.classList.remove('modal-open');
  }

  function confirmar() {
    if (busy) return;
    sanitizeManualField();
    var q = quem || String(dom.quemOutros.value || '').trim();
    if (q.length < 2) {
      dom.status.textContent = 'Informe quem levou';
      return;
    }
    if (!formaPag) {
      dom.status.textContent = 'Escolha a forma de pagamento';
      return;
    }
    var pin = String(dom.pin.value || '').trim();
    if (!pin) {
      dom.status.textContent = 'Digite o PIN';
      return;
    }
    var manRaw = String(dom.manual.value || '').trim();
    if (manRaw && /[a-zA-Z]/.test(manRaw)) {
      dom.manual.value = '';
      dom.status.textContent = 'Valor inválido — deixe vazio para automático ou digite o R$';
      return;
    }
    var body = {
      quem_levou: q,
      pin: pin,
      percentual_lucro: dom.pct.value || '50',
      incluir_cmv: dom.cmv.checked,
      incluir_lucro: dom.lucro.checked,
      incluir_fiado: dom.fiado.checked,
      modo_dia_cheio: dom.cheio.checked,
      forma_pagamento: formaPag || 'Dinheiro',
    };
    var mv = parseManualValor();
    if (mv != null) body.valor_manual = String(mv);

    busy = true;
    dom.status.textContent = 'Transferindo…';
    fetch('/api/repasse-vila/confirmar/', {
      method: 'POST',
      credentials: 'same-origin',
      headers: {
        'Content-Type': 'application/json',
        'X-CSRFToken': csrf(),
      },
      body: JSON.stringify(body),
    })
      .then(function (r) {
        return r.json().then(function (j) {
          return { okHttp: r.ok, j: j };
        });
      })
      .then(function (pack) {
        busy = false;
        var j = pack.j || {};
        if (!j.ok) {
          dom.status.textContent = j.erro || 'Não foi possível transferir';
          return;
        }
        var tot = (j.repasse && j.repasse.valor_total) || 0;
        dom.status.textContent = 'OK — enviado ' + money(tot) + ' · ' + (formaPag || '');
        dom.pin.value = '';
        if (dom.manual) dom.manual.value = '';
        fetchCalc();
        setTimeout(closeOverlay, 900);
      })
      .catch(function () {
        busy = false;
        dom.status.textContent = 'Falha de rede';
      });
  }

  var btnOpen =
    document.getElementById('pdv-topbar-repasse-btn') ||
    document.getElementById('crh-btn-repasse');
  if (btnOpen) btnOpen.addEventListener('click', openOverlay);
  if (dom.fechar) dom.fechar.addEventListener('click', closeOverlay);
  if (dom.cancelar) dom.cancelar.addEventListener('click', closeOverlay);
  if (dom.confirmar) dom.confirmar.addEventListener('click', confirmar);

  dom.todos.addEventListener('change', function () {
    var on = dom.todos.checked;
    dom.cmv.checked = on;
    dom.lucro.checked = on;
    dom.fiado.checked = on;
    renderCalc();
  });
  [dom.cmv, dom.lucro, dom.fiado, dom.manual].forEach(function (el) {
    el.addEventListener('change', function () {
      dom.todos.checked = dom.cmv.checked && dom.lucro.checked && dom.fiado.checked;
      renderCalc();
    });
    el.addEventListener('input', renderCalc);
  });
  var t = null;
  dom.pct.addEventListener('input', function () {
    clearTimeout(t);
    t = setTimeout(fetchCalc, 300);
  });
  dom.cheio.addEventListener('change', fetchCalc);
  if (dom.quemOutros) {
    dom.quemOutros.addEventListener('input', function () {
      quem = String(dom.quemOutros.value || '').trim();
    });
  }

  overlay.addEventListener('click', function (ev) {
    if (ev.target === overlay) closeOverlay();
  });

  if (qs().get('repasse') === '1') {
    setTimeout(openOverlay, 200);
  }
})();
