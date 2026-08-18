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
    data: document.getElementById('pdv-rp-data'),
    dataHint: document.getElementById('pdv-rp-data-hint'),
    pct: document.getElementById('pdv-rp-pct'),
    cmv: document.getElementById('pdv-rp-cmv'),
    lucro: document.getElementById('pdv-rp-lucro'),
    fiado: document.getElementById('pdv-rp-fiado'),
    todos: document.getElementById('pdv-rp-todos'),
    cheio: document.getElementById('pdv-rp-cheio'),
    acumulado: document.getElementById('pdv-rp-acumulado'),
    manual: document.getElementById('pdv-rp-manual'),
    pin: document.getElementById('pdv-rp-pin'),
    status: document.getElementById('pdv-rp-status'),
    quemGrid: document.getElementById('pdv-rp-quem-grid'),
    quemOutros: document.getElementById('pdv-rp-quem-outros'),
    formaGrid: document.getElementById('pdv-rp-forma-grid'),
  };

  function todayIso() {
    var d = new Date();
    return d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0');
  }

  function minDataIso() {
    var d = new Date();
    d.setDate(d.getDate() - 180);
    return d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0');
  }

  function setupDataField() {
    if (!dom.data) return;
    var hoje = todayIso();
    dom.data.max = hoje;
    dom.data.min = minDataIso();
    if (!dom.data.value) dom.data.value = hoje;
    updateDataHint();
  }

  function dataRef() {
    if (!dom.data || !dom.data.value) return todayIso();
    return String(dom.data.value).slice(0, 10);
  }

  function updateDataHint() {
    if (!dom.dataHint) return;
    var d = dataRef();
    var hoje = todayIso();
    if (d === hoje) {
      dom.dataHint.textContent = 'Repasse de hoje';
      dom.dataHint.className = 'text-xs font-bold text-slate-600 pb-1';
    } else {
      var parts = d.split('-');
      var br = (parts[2] || '') + '/' + (parts[1] || '') + '/' + (parts[0] || '');
      dom.dataHint.textContent = 'Dia passado: ' + br + ' · dinheiro sai do caixa de agora';
      dom.dataHint.className = 'text-xs font-bold text-amber-800 pb-1';
    }
  }

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
    if (q.get('data') && dom.data) dom.data.value = String(q.get('data')).slice(0, 10);
    dom.todos.checked = dom.cmv.checked && dom.lucro.checked && dom.fiado.checked;
    updateDataHint();
  }

  function renderCalc() {
    var c = calc || {};
    var d = c.disponivel || {};
    document.getElementById('pdv-rp-receita').textContent = money(c.receita_dia);
    document.getElementById('pdv-rp-cmv-dia').textContent = money(c.cmv_dia);
    document.getElementById('pdv-rp-lucro-dia').textContent = money(c.lucro_bruto_dia);
    document.getElementById('pdv-rp-fiado-dia').textContent = money(c.fiado_pago_dia);
    var eletEl = document.getElementById('pdv-rp-elet');
    if (eletEl) eletEl.textContent = money(c.ja_eletronico);
    var jaDin = document.getElementById('pdv-rp-ja-din');
    if (jaDin) jaDin.textContent = money((c.ja_enviado || {}).total);
    var faltaEl = document.getElementById('pdv-rp-falta');
    if (faltaEl) faltaEl.textContent = money(c.falta_dinheiro != null ? c.falta_dinheiro : d.total);
    var acumEl = document.getElementById('pdv-rp-acum');
    var acum = Number(c.acumulado_anterior || 0);
    if (acumEl) {
      acumEl.textContent = money(acum);
      acumEl.className = 'text-lg font-black tabular-nums ' + (acum > 0 ? 'text-amber-950' : acum < 0 ? 'text-sky-900' : 'text-slate-600');
    }
    var sugEl = document.getElementById('pdv-rp-total-sug');
    var sug = Number(c.total_sugerido != null ? c.total_sugerido : (Number(c.falta_dinheiro || d.total || 0) + acum));
    if (sugEl) sugEl.textContent = money(Math.max(0, sug));
    document.getElementById('pdv-rp-disp-cmv').textContent = money(d.cmv);
    document.getElementById('pdv-rp-disp-lucro').textContent = money(d.lucro);
    var despHint = document.getElementById('pdv-rp-desp-hint');
    if (despHint) {
      var dc = Number(c.despesas_centro_dia || 0);
      if (dc > 0) {
        despHint.classList.remove('hidden');
        despHint.textContent = '− planos no envio ao Centro: ' + money(dc);
      } else {
        despHint.classList.add('hidden');
        despHint.textContent = '';
      }
    }
    document.getElementById('pdv-rp-disp-fiado').textContent = money(d.fiado);
    var tot = 0;
    if (dom.cmv.checked) tot += Number(d.cmv || 0);
    if (dom.lucro.checked) tot += Number(d.lucro || 0);
    if (dom.fiado.checked) tot += Number(d.fiado || 0);
    var mv = parseManualValor();
    if (mv != null) {
      tot = mv;
    } else if (dom.acumulado && dom.acumulado.checked && acum !== 0) {
      tot = Math.max(0, tot + acum);
    }
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
    var data = dataRef();
    return fetch(
      '/api/repasse-vila/calc/?pct=' +
        encodeURIComponent(pct) +
        '&dia_cheio=' +
        cheio +
        '&data=' +
        encodeURIComponent(data),
      { credentials: 'same-origin' }
    )
      .then(function (r) {
        return r.json();
      })
      .then(function (j) {
        if (j && j.ok) {
          calc = j;
          renderCalc();
          if (dom.status) dom.status.textContent = '';
        } else if (dom.status) {
          dom.status.textContent = (j && j.erro) || 'Falha ao calcular';
        }
      });
  }

  function openOverlay() {
    overlay.classList.remove('hidden');
    overlay.classList.add('flex');
    document.body.classList.add('modal-open');
    setupDataField();
    applyQueryPrefs();
    if (dom.manual) dom.manual.value = '';
    if (dom.pin) dom.pin.value = '';
    sanitizeManualField();
    updateDataHint();
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
        if (!qs().get('data')) {
          calc = j.calc || null;
        }
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
        if (dom.status && dom.status.textContent === 'Carregando…') dom.status.textContent = '';
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
      data_ref: dataRef(),
      incluir_acumulado: !!(dom.acumulado && dom.acumulado.checked),
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
  [dom.cmv, dom.lucro, dom.fiado, dom.manual, dom.acumulado].forEach(function (el) {
    if (!el) return;
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
  if (dom.data) {
    dom.data.addEventListener('change', function () {
      updateDataHint();
      fetchCalc();
    });
  }
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

  var acumModal = document.getElementById('pdv-rp-acum-modal');
  var acumBtn = document.getElementById('pdv-rp-btn-acum');
  var acumFechar = document.getElementById('pdv-rp-acum-fechar');
  var acumLista = document.getElementById('pdv-rp-acum-lista');
  var acumStatus = document.getElementById('pdv-rp-acum-status');

  function fmtDataIso(iso) {
    if (!iso) return '—';
    var p = String(iso).slice(0, 10).split('-');
    return (p[2] || '') + '/' + (p[1] || '') + '/' + (p[0] || '');
  }

  function renderAcumModal(j) {
    if (!j || !acumLista) return;
    document.getElementById('pdv-rp-acum-modal-saldo').textContent = money(j.acumulado_anterior);
    document.getElementById('pdv-rp-acum-modal-falta').textContent = money(j.falta_dia);
    document.getElementById('pdv-rp-acum-modal-sug').textContent = money(Math.max(0, j.total_sugerido));
    acumLista.innerHTML = '';
    var rows = (j.ajustes || []).concat(j.linhas_dias || []);
    if (!rows.length) {
      acumLista.innerHTML = '<p class="text-slate-500 py-4 text-center">Nenhum dia anterior com diferença.</p>';
      return;
    }
    rows.forEach(function (row) {
      var div = document.createElement('div');
      div.className = 'py-1.5 border-b border-slate-100 flex flex-wrap justify-between gap-1';
      var delta = Number(row.delta || 0);
      var lbl = row.tipo === 'ajuste'
        ? 'Ajuste · ' + (row.observacao || '')
        : fmtDataIso(row.data) + ' · alvo ' + money(row.alvo_fisico) + ' · enviado ' + money(row.enviado);
      div.innerHTML =
        '<span class="min-w-0 flex-1">' + lbl + '</span>' +
        '<span class="tabular-nums font-black ' + (delta > 0 ? 'text-amber-800' : delta < 0 ? 'text-sky-800' : 'text-slate-600') + '">' +
        (delta > 0 ? '+' : '') + money(delta) +
        '</span>';
      acumLista.appendChild(div);
    });
  }

  function fetchAcumModal() {
    if (acumStatus) acumStatus.textContent = 'Carregando…';
    return fetch('/api/repasse-vila/acumulado/?data=' + encodeURIComponent(dataRef()), { credentials: 'same-origin' })
      .then(function (r) { return r.json(); })
      .then(function (j) {
        if (j && j.ok) {
          renderAcumModal(j);
          if (acumStatus) acumStatus.textContent = '';
        } else if (acumStatus) {
          acumStatus.textContent = (j && j.erro) || 'Falha ao carregar';
        }
      });
  }

  function openAcumModal() {
    if (!acumModal) return;
    acumModal.classList.remove('hidden');
    acumModal.classList.add('flex');
    fetchAcumModal();
  }

  function closeAcumModal() {
    if (!acumModal) return;
    acumModal.classList.add('hidden');
    acumModal.classList.remove('flex');
  }

  if (acumBtn) acumBtn.addEventListener('click', openAcumModal);
  if (acumFechar) acumFechar.addEventListener('click', closeAcumModal);
  if (acumModal) {
    acumModal.addEventListener('click', function (ev) {
      if (ev.target === acumModal) closeAcumModal();
    });
  }

  var acumSalvar = document.getElementById('pdv-rp-acum-salvar');
  if (acumSalvar) {
    acumSalvar.addEventListener('click', function () {
      var pin = String((document.getElementById('pdv-rp-acum-pin') || {}).value || '').trim();
      var val = String((document.getElementById('pdv-rp-acum-valor') || {}).value || '').trim();
      var obs = String((document.getElementById('pdv-rp-acum-obs') || {}).value || '').trim();
      if (!pin) { if (acumStatus) acumStatus.textContent = 'Digite o PIN'; return; }
      if (!val) { if (acumStatus) acumStatus.textContent = 'Informe o valor'; return; }
      if (acumStatus) acumStatus.textContent = 'Salvando…';
      fetch('/api/repasse-vila/acumulado/ajuste/', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf() },
        body: JSON.stringify({
          pin: pin,
          valor: val,
          observacao: obs,
          data_calc: dataRef(),
        }),
      })
        .then(function (r) { return r.json(); })
        .then(function (j) {
          if (!j || !j.ok) {
            if (acumStatus) acumStatus.textContent = (j && j.erro) || 'Erro';
            return;
          }
          if (acumStatus) acumStatus.textContent = 'Ajuste registrado';
          var pinEl = document.getElementById('pdv-rp-acum-pin');
          if (pinEl) pinEl.value = '';
          renderAcumModal(j.acumulado);
          fetchCalc();
        })
        .catch(function () {
          if (acumStatus) acumStatus.textContent = 'Falha de rede';
        });
    });
  }
  var acumZerar = document.getElementById('pdv-rp-acum-zerar');
  if (acumZerar) {
    acumZerar.addEventListener('click', function () {
      var pin = String((document.getElementById('pdv-rp-acum-pin') || {}).value || '').trim();
      if (!pin) { if (acumStatus) acumStatus.textContent = 'Digite o PIN'; return; }
      if (!window.confirm('Zerar o acumulado? Use se o dinheiro já foi transferido antes da ferramenta.')) return;
      if (acumStatus) acumStatus.textContent = 'Zerando…';
      fetch('/api/repasse-vila/acumulado/zerar/', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf() },
        body: JSON.stringify({ pin: pin, data_calc: dataRef() }),
      })
        .then(function (r) { return r.json(); })
        .then(function (j) {
          if (!j || !j.ok) {
            if (acumStatus) acumStatus.textContent = (j && j.erro) || 'Erro';
            return;
          }
          if (acumStatus) acumStatus.textContent = 'Acumulado zerado';
          renderAcumModal(j.acumulado);
          fetchCalc();
        })
        .catch(function () { if (acumStatus) acumStatus.textContent = 'Falha de rede'; });
    });
  }
})();
