/**
 * PDV — overlay Repasse Vila → Centro.
 * Hero limpo · quem e PIN só no popup (Confirmar) · forma oculta (Dinheiro).
 */
(function () {
  'use strict';

  var overlay = document.getElementById('pdv-repasse-overlay');
  if (!overlay) return;

  var calc = null;
  var histMes = null;
  var funcionarios = [];
  var formasPagamento = ['Dinheiro', 'PIX', 'Cartão de débito', 'Cartão de crédito', 'Outro'];
  var quem = '';
  var formaPag = 'Dinheiro';
  var busy = false;
  var pendingConfirmar = false;

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
    reserva: document.getElementById('pdv-rp-reserva'),
    salvarReserva: document.getElementById('pdv-rp-salvar-reserva'),
    separarReserva: document.getElementById('pdv-rp-separar-reserva'),
    manual: document.getElementById('pdv-rp-manual'),
    pin: document.getElementById('pdv-rp-pin'),
    status: document.getElementById('pdv-rp-status'),
    quemGrid: document.getElementById('pdv-rp-quem-grid'),
    quemOutros: document.getElementById('pdv-rp-quem-outros'),
    formaGrid: document.getElementById('pdv-rp-forma-grid'),
  };

  var quemModal = document.getElementById('pdv-rp-quem-modal');
  var formaModal = document.getElementById('pdv-rp-forma-modal');
  var pinModal = document.getElementById('pdv-rp-pin-modal');
  var cofreConfirmModal = document.getElementById('pdv-rp-cofre-confirm-modal');
  var cofreConfirmPending = null;
  var forcarManualModal = document.getElementById('pdv-rp-forcar-manual-modal');
  var manualDirty = false;
  var manualAutoFmt = '';
  var selectManualPending = false;
  var forcarManualPendingBody = null;

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
    } else {
      var parts = d.split('-');
      var br = (parts[2] || '') + '/' + (parts[1] || '') + '/' + (parts[0] || '');
      dom.dataHint.textContent = 'Dia passado: ' + br + ' · dinheiro sai do caixa de agora';
    }
  }

  function parseMoneyBR(raw) {
    var s = String(raw || '').trim();
    if (!s) return 0;
    if (s.indexOf(',') >= 0) s = s.replace(/\./g, '').replace(',', '.');
    var n = Number(s);
    return isNaN(n) || n < 0 ? 0 : n;
  }

  function reservaAtual() {
    if (dom.reserva) return parseMoneyBR(dom.reserva.value);
    return Number((calc || {}).reserva_vila || 0);
  }

  function money(n) {
    return 'R$ ' + Number(n || 0).toLocaleString('pt-BR', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
  }

  function fmtManualNum(n) {
    return Number(n || 0).toLocaleString('pt-BR', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
  }

  function syncManualFromAuto(totAuto, opts) {
    if (!dom.manual || manualDirty) return;
    var fmt = fmtManualNum(totAuto);
    manualAutoFmt = fmt;
    if (dom.manual.value !== fmt) dom.manual.value = fmt;
    if (opts && opts.select) {
      selectManualPending = false;
      focusSoon(dom.manual);
    } else if (selectManualPending) {
      selectManualPending = false;
      focusSoon(dom.manual);
    }
  }

  function markManualDirtyFromInput() {
    if (!dom.manual) return;
    sanitizeManualField();
    var cur = String(dom.manual.value || '').trim();
    if (!cur || cur === manualAutoFmt) {
      manualDirty = false;
    } else {
      manualDirty = true;
    }
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

  function setText(id, txt) {
    var el = document.getElementById(id);
    if (el) el.textContent = txt;
  }

  function showModal(el) {
    if (!el) return;
    el.classList.remove('hidden');
    el.classList.add('flex');
  }

  function hideModal(el) {
    if (!el) return;
    el.classList.add('hidden');
    el.classList.remove('flex');
  }

  function focusSoon(el) {
    if (!el) return;
    setTimeout(function () {
      try {
        el.focus();
        if (typeof el.select === 'function' && el.tagName === 'INPUT') el.select();
      } catch (_) {}
    }, 40);
  }

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

  function quemAtual() {
    return quem || String((dom.quemOutros && dom.quemOutros.value) || '').trim();
  }

  function pinAtual() {
    return String((dom.pin && dom.pin.value) || '').trim();
  }

  function applyQueryPrefs() {
    var q = qs();
    if (q.get('pct') && dom.pct) dom.pct.value = q.get('pct');
    if (q.get('cmv') === '0' && dom.cmv) dom.cmv.checked = false;
    if (q.get('lucro') === '0' && dom.lucro) dom.lucro.checked = false;
    if (q.get('fiado') === '0' && dom.fiado) dom.fiado.checked = false;
    if (q.get('cheio') === '1' && dom.cheio) dom.cheio.checked = true;
    if (q.get('forma')) formaPag = q.get('forma');
    if (q.get('data') && dom.data) dom.data.value = String(q.get('data')).slice(0, 10);
    if (dom.todos && dom.cmv && dom.lucro && dom.fiado) {
      dom.todos.checked = dom.cmv.checked && dom.lucro.checked && dom.fiado.checked;
    }
    updateDataHint();
  }

  function renderMesCards() {
    var h = histMes || {};
    setText('pdv-rp-mes-dinheiro', money(h.total_mes));
    setText('pdv-rp-hero-mes', money(h.total_mes));
    setText('pdv-rp-hero-geral', money(h.total_geral));
    setText('pdv-rp-mes-lucro-ficou', money(h.lucro_ficou_vila));
    var c = calc || {};
    var cofre = c.cofrinho || {};
    var cofreVe = c.cofre_vila_elias || {};
    setText('pdv-rp-card-cofre', money(cofre.saldo));
    setText('pdv-rp-card-cofre-ve', money(cofreVe.saldo));
    var elet = Number(c.ja_eletronico || 0);
    var jaDin = Number((c.ja_enviado || {}).total || 0);
    setText('pdv-rp-dia-todas', money(elet + jaDin));
  }

  function renderCalc() {
    var c = calc || {};
    var cofre = c.cofrinho || {};
    var cofreVe = c.cofre_vila_elias || {};
    var d = c.disponivel || {};
    setText('pdv-rp-receita', money(c.receita_dia));
    setText('pdv-rp-cmv-dia', money(c.cmv_dia));
    setText('pdv-rp-lucro-dia', money(c.lucro_bruto_dia));
    setText('pdv-rp-fiado-dia', money(c.fiado_pago_dia));
    setText('pdv-rp-elet', money(c.ja_eletronico));
    setText('pdv-rp-ja-din', money((c.ja_enviado || {}).total));
    setText('pdv-rp-falta', money(c.falta_dinheiro != null ? c.falta_dinheiro : d.total));

    var acum = Number(c.acumulado_anterior || 0);
    var acumEl = document.getElementById('pdv-rp-acum');
    if (acumEl) {
      acumEl.textContent = money(acum);
      acumEl.className =
        'text-lg font-black tabular-nums ' +
        (acum > 0 ? 'text-amber-950' : acum < 0 ? 'text-sky-900' : 'text-slate-600');
    }
    var dispCmv = document.getElementById('pdv-rp-disp-cmv');
    var dispLucro = document.getElementById('pdv-rp-disp-lucro');
    var dispFiado = document.getElementById('pdv-rp-disp-fiado');
    if (dispCmv) dispCmv.textContent = money(d.cmv);
    if (dispLucro) dispLucro.textContent = money(d.lucro);
    if (dispFiado) dispFiado.textContent = money(d.fiado);

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

    var diaAuto = 0;
    if (dom.cmv && dom.cmv.checked) diaAuto += Number(d.cmv || 0);
    if (dom.lucro && dom.lucro.checked) diaAuto += Number(d.lucro || 0);
    if (dom.fiado && dom.fiado.checked) diaAuto += Number(d.fiado || 0);
    var inclAcum = !!(dom.acumulado && dom.acumulado.checked && acum !== 0);
    var totAuto = diaAuto;
    if (inclAcum) {
      totAuto = Math.max(0, diaAuto + acum);
    }
    syncManualFromAuto(totAuto);
    var mv = manualDirty ? parseManualValor() : null;
    var tot = mv != null ? mv : totAuto;
    // Grande = só o dia · menor = acumulado (na mesma tag)
    setText('pdv-rp-total', money(diaAuto));
    var acumHero = document.getElementById('pdv-rp-total-acum');
    if (acumHero) {
      acumHero.textContent = money(acum);
      acumHero.className =
        'text-xl sm:text-2xl font-black tabular-nums leading-none ' +
        (acum > 0.009 ? 'text-amber-950' : acum < -0.009 ? 'text-sky-900' : 'text-slate-600');
    }
    var levadoHoje = Number((c.ja_enviado || {}).total || 0);
    var levadoWrap = document.getElementById('pdv-rp-levado-hoje-wrap');
    var levadoEl = document.getElementById('pdv-rp-levado-hoje');
    if (levadoWrap && levadoEl) {
      if (levadoHoje > 0.009) {
        levadoWrap.classList.remove('hidden');
        levadoEl.textContent = money(levadoHoje);
      } else {
        levadoWrap.classList.add('hidden');
        levadoEl.textContent = money(0);
      }
    }

    var cx = c.caixa_vila || {};
    var cxEl = document.getElementById('pdv-rp-caixa-din');
    var cxHint = document.getElementById('pdv-rp-caixa-din-hint');
    if (cxEl) {
      if (cx.aberto) {
        cxEl.textContent = money(cx.saldo_dinheiro);
        cxEl.className = 'text-3xl sm:text-4xl font-black tabular-nums text-slate-950 leading-none';
        if (cxHint) cxHint.textContent = 'Esperado na gaveta (antes de levar)';
      } else {
        cxEl.textContent = 'Fechado';
        cxEl.className = 'text-3xl sm:text-4xl font-black tabular-nums text-red-800 leading-none';
        if (cxHint) cxHint.textContent = 'Abra o caixa da Vila para ver o saldo';
      }
    }

    var enviarHint = document.getElementById('pdv-rp-enviar-hint');
    if (enviarHint) {
      if (mv != null) {
        enviarHint.textContent = 'Valor digitado manualmente · total a levar ' + money(tot);
      } else if (inclAcum && Math.abs(acum) > 0.009) {
        enviarHint.textContent = 'Total a levar ' + money(tot) + ' (dia + acumulado)';
      } else {
        enviarHint.textContent = '';
      }
    }

    var hintOp = document.getElementById('pdv-rp-opcoes-hint');
    if (hintOp && dom.pct) hintOp.textContent = (dom.pct.value || '50') + '%';

    function renderCofreHero(resumo, ids, avisoTxt) {
      var pendente = Number(resumo.pendente_dia || 0);
      var saldo = Number(resumo.saldo || 0);
      var realizado = Number(resumo.realizada_dia || 0);
      setText(ids.aSeparar, money(pendente));
      setText(ids.saldo, money(saldo));
      var wrapHoje = document.getElementById(ids.hojeWrap);
      var hojeEl = document.getElementById(ids.hoje);
      if (wrapHoje && hojeEl) {
        if (realizado > 0.009) {
          wrapHoje.classList.remove('hidden');
          hojeEl.textContent = money(realizado);
        } else {
          wrapHoje.classList.add('hidden');
          hojeEl.textContent = money(0);
        }
      }
      var aviso = document.getElementById(ids.aviso);
      if (aviso) {
        if (pendente > 0.009) {
          aviso.classList.remove('hidden');
          aviso.textContent = avisoTxt;
        } else {
          aviso.classList.add('hidden');
          aviso.textContent = '';
        }
      }
    }

    renderCofreHero(cofre, {
      aSeparar: 'pdv-rp-hero-cofre',
      saldo: 'pdv-rp-hero-cofre-saldo',
      hojeWrap: 'pdv-rp-hero-cofre-hoje-wrap',
      hoje: 'pdv-rp-hero-cofre-hoje',
      aviso: 'pdv-rp-cofre-aviso',
    }, 'NÃO levar no envelope · Separar junto puxa o acumulado.');

    renderCofreHero(cofreVe, {
      aSeparar: 'pdv-rp-hero-cofre-ve',
      saldo: 'pdv-rp-hero-cofre-ve-saldo',
      hojeWrap: 'pdv-rp-hero-cofre-ve-hoje-wrap',
      hoje: 'pdv-rp-hero-cofre-ve-hoje',
      aviso: 'pdv-rp-cofre-ve-aviso',
    }, 'Lucro que fica na Vila · Separar junto puxa o acumulado.');

    renderMesCards();
  }

  function renderQuem() {
    if (!dom.quemGrid) return;
    dom.quemGrid.innerHTML = '';
    funcionarios.forEach(function (f) {
      var b = document.createElement('button');
      b.type = 'button';
      b.className = 'rp-quem-btn' + (quem === f.nome ? ' is-on' : '');
      b.textContent = f.nome;
      b.addEventListener('click', function () {
        pickQuem(f.nome);
      });
      dom.quemGrid.appendChild(b);
    });
    var outros = document.createElement('button');
    outros.type = 'button';
    outros.className =
      'rp-quem-btn' +
      (dom.quemOutros && !dom.quemOutros.classList.contains('hidden') ? ' is-on' : '');
    outros.textContent = 'Outros';
    outros.addEventListener('click', function () {
      quem = '';
      if (dom.quemOutros) {
        dom.quemOutros.classList.remove('hidden');
        focusSoon(dom.quemOutros);
      }
      renderQuem();
    });
    dom.quemGrid.appendChild(outros);
  }

  function renderForma() {
    if (!dom.formaGrid) return;
    dom.formaGrid.innerHTML = '';
    if (!formasPagamento.length) formasPagamento = ['Dinheiro'];
    if (formasPagamento.indexOf(formaPag) < 0) formaPag = 'Dinheiro';
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

  function fetchHistoricoMes() {
    var d = dataRef() || '';
    var ano = d.slice(0, 4);
    var mes = d.slice(5, 7);
    var q =
      ano && mes
        ? '?ano=' + encodeURIComponent(ano) + '&mes=' + encodeURIComponent(mes)
        : '';
    return fetch('/api/repasse-vila/historico/' + q, { credentials: 'same-origin' })
      .then(function (r) {
        return r.json();
      })
      .then(function (j) {
        if (j && j.ok) histMes = j;
        renderMesCards();
      })
      .catch(function () {});
  }

  function fetchCalc() {
    var pct = (dom.pct && dom.pct.value) || '50';
    var cheio = dom.cheio && dom.cheio.checked ? '1' : '0';
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

  function pickQuem(nome) {
    quem = String(nome || '').trim();
    if (dom.quemOutros) {
      dom.quemOutros.classList.add('hidden');
      dom.quemOutros.value = '';
    }
    hideModal(quemModal);
    if (dom.status) dom.status.textContent = '';
    if (pendingConfirmar) tryConfirmarFlow();
  }

  function openQuemModal() {
    if (dom.status) dom.status.textContent = '';
    showModal(quemModal);
    renderQuem();
    if (dom.quemOutros && !dom.quemOutros.classList.contains('hidden')) {
      focusSoon(dom.quemOutros);
    } else {
      var first = dom.quemGrid && dom.quemGrid.querySelector('.rp-quem-btn');
      focusSoon(first);
    }
  }

  function closeQuemModal(cancel) {
    if (cancel) {
      pendingConfirmar = false;
      hideModal(quemModal);
      return;
    }
    if (quemAtual().length < 2) return;
    hideModal(quemModal);
    if (pendingConfirmar) tryConfirmarFlow();
  }

  function openPinModal() {
    if (dom.status) dom.status.textContent = '';
    showModal(pinModal);
    focusSoon(dom.pin);
  }

  function closePinModal(cancel) {
    if (cancel) {
      pendingConfirmar = false;
      hideModal(pinModal);
      return;
    }
    hideModal(pinModal);
    if (pendingConfirmar) tryConfirmarFlow();
  }

  function tryConfirmarFlow() {
    formaPag = 'Dinheiro';
    var q = quemAtual();
    if (q.length < 2) {
      pendingConfirmar = true;
      openQuemModal();
      return;
    }
    if (!pinAtual()) {
      pendingConfirmar = true;
      openPinModal();
      return;
    }
    pendingConfirmar = false;
    confirmar();
  }

  function openOverlay() {
    overlay.classList.remove('hidden');
    overlay.classList.add('flex');
    document.body.classList.add('modal-open');
    pendingConfirmar = false;
    setupDataField();
    applyQueryPrefs();
    manualDirty = false;
    manualAutoFmt = '';
    selectManualPending = true;
    if (dom.manual) dom.manual.value = '';
    if (dom.pin) dom.pin.value = '';
    quem = '';
    formaPag = 'Dinheiro';
    if (dom.quemOutros) {
      dom.quemOutros.value = '';
      dom.quemOutros.classList.add('hidden');
    }
    sanitizeManualField();
    updateDataHint();
    if (dom.status) dom.status.textContent = 'Carregando…';
    fetch('/api/repasse-vila/meta/', { credentials: 'same-origin' })
      .then(function (r) {
        return r.json();
      })
      .then(function (j) {
        if (!j || !j.ok) {
          if (dom.status) dom.status.textContent = 'Falha ao carregar';
          return;
        }
        funcionarios = j.funcionarios || [];
        if (Array.isArray(j.formas_pagamento) && j.formas_pagamento.length) {
          formasPagamento = j.formas_pagamento;
        }
        if (dom.pct && (!dom.pct.value || dom.pct.value === '50')) {
          dom.pct.value = String(Math.round(j.percentual_padrao || 50));
        }
        if (dom.reserva && (j.reserva_vila != null || (j.calc && j.calc.reserva_vila != null))) {
          var rv = j.reserva_vila != null ? j.reserva_vila : j.calc.reserva_vila;
          dom.reserva.value = Number(rv || 0).toLocaleString('pt-BR', {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
          });
        }
        if (!qs().get('data')) {
          calc = j.calc || null;
          if (calc && j.cofrinho) calc.cofrinho = j.cofrinho;
          if (calc && j.cofre_vila_elias) calc.cofre_vila_elias = j.cofre_vila_elias;
          if (calc && j.caixa_vila) calc.caixa_vila = j.caixa_vila;
        } else if (j.caixa_vila && calc) {
          calc.caixa_vila = j.caixa_vila;
        }
        if (j.caixa_vila && !calc) {
          calc = { caixa_vila: j.caixa_vila };
        }
        if (dom.sub) {
          if (!j.caixa_vila_aberto) {
            dom.sub.textContent = 'Caixa da Vila FECHADO — abra antes de transferir';
            dom.sub.classList.add('text-red-700');
          } else {
            dom.sub.textContent = 'Caixa Vila aberto · sai da Vila · entra no Centro';
            dom.sub.classList.remove('text-red-700');
          }
        }
        renderQuem();
        renderForma();
        fetchHistoricoMes();
        return fetchCalc();
      })
      .then(function () {
        sanitizeManualField();
        renderCalc();
        if (dom.status && dom.status.textContent === 'Carregando…') dom.status.textContent = '';
        focusSoon(dom.manual);
      })
      .catch(function () {
        if (dom.status) dom.status.textContent = 'Falha de rede';
      });
  }

  function closeOverlay() {
    pendingConfirmar = false;
    closeForcarManualModal();
    hideModal(quemModal);
    hideModal(formaModal);
    hideModal(pinModal);
    hideModal(cofreConfirmModal);
    hideModal(acumModal);
    overlay.classList.add('hidden');
    overlay.classList.remove('flex');
    document.body.classList.remove('modal-open');
  }

  function notifyParentFecharAtualizar() {
    try {
      if (window.parent && window.parent !== window) {
        window.parent.postMessage({ type: 'agro-caixa-fechar-atualizar' }, window.location.origin);
      }
    } catch (_) {}
  }

  function parseMoneyInput(el) {
    if (!el) return 0;
    var s = String(el.value || '')
      .replace(/\s/g, '')
      .replace(/R\$/gi, '')
      .replace(/\./g, '')
      .replace(',', '.');
    var n = Number(s);
    return isFinite(n) ? n : NaN;
  }

  function closeCofreConfirmModal(ok) {
    hideModal(cofreConfirmModal);
    var cb = cofreConfirmPending;
    cofreConfirmPending = null;
    if (ok && typeof cb === 'function') cb();
  }

  function openCofreConfirmModal(opts, onConfirm) {
    opts = opts || {};
    var inpSal = document.getElementById('pdv-rp-cofre-confirm-input-sal');
    var inpVe = document.getElementById('pdv-rp-cofre-confirm-input-ve');
    var inpLev = document.getElementById('pdv-rp-cofre-confirm-input-levar');
    if (inpSal) inpSal.value = fmtManualNum(opts.salario || 0);
    if (inpVe) inpVe.value = fmtManualNum(opts.vilaElias || 0);
    if (inpLev) inpLev.value = fmtManualNum(opts.levar || 0);
    cofreConfirmPending = onConfirm;
    showModal(cofreConfirmModal);
    focusSoon(inpLev || document.getElementById('pdv-rp-cofre-confirm-ok'));
  }

  function autoLinhasZeradas() {
    if (!calc) return false;
    var d = calc.disponivel || {};
    var t = 0;
    if (dom.cmv && dom.cmv.checked) t += Number(d.cmv || 0);
    if (dom.lucro && dom.lucro.checked) t += Number(d.lucro || 0);
    if (dom.fiado && dom.fiado.checked) t += Number(d.fiado || 0);
    return t < 0.009;
  }

  function closeForcarManualModal() {
    forcarManualPendingBody = null;
    hideModal(forcarManualModal);
    var pinF = document.getElementById('pdv-rp-forcar-manual-pin');
    if (pinF) pinF.value = '';
    var st = document.getElementById('pdv-rp-forcar-manual-status');
    if (st) st.textContent = '';
  }

  function openForcarManualModal(msg, body) {
    forcarManualPendingBody = body;
    var elMsg = document.getElementById('pdv-rp-forcar-manual-msg');
    if (elMsg) {
      elMsg.textContent =
        msg ||
        'O cálculo automático deste dia já está zerado (já enviado ou cartão/PIX cobriu). Confirme com o PIN de novo.';
    }
    var pinF = document.getElementById('pdv-rp-forcar-manual-pin');
    if (pinF) pinF.value = '';
    var st = document.getElementById('pdv-rp-forcar-manual-status');
    if (st) st.textContent = '';
    // PIN do fluxo normal não conta — exige digitar de novo neste modal
    if (dom.pin) dom.pin.value = '';
    showModal(forcarManualModal);
    focusSoon(pinF);
  }

  function submitForcarManual() {
    var pinF = document.getElementById('pdv-rp-forcar-manual-pin');
    var pin = String((pinF && pinF.value) || '').trim();
    var st = document.getElementById('pdv-rp-forcar-manual-status');
    if (!pin) {
      if (st) st.textContent = 'Digite o PIN de novo para confirmar.';
      focusSoon(pinF);
      return;
    }
    if (!forcarManualPendingBody) return;
    var body = Object.assign({}, forcarManualPendingBody, {
      pin: pin,
      forcar_manual_zerado: true,
    });
    closeForcarManualModal();
    enviarConfirmacao(body);
  }

  function confirmar() {
    if (busy) return;
    sanitizeManualField();
    var q = quemAtual();
    if (q.length < 2) {
      pendingConfirmar = true;
      openQuemModal();
      return;
    }
    formaPag = 'Dinheiro';
    var pin = pinAtual();
    if (!pin) {
      pendingConfirmar = true;
      openPinModal();
      return;
    }
    var manRaw = String((dom.manual && dom.manual.value) || '').trim();
    if (manRaw && /[a-zA-Z]/.test(manRaw)) {
      dom.manual.value = '';
      manualDirty = false;
      if (dom.status) dom.status.textContent = 'Valor inválido — digite só o número (ex. 1500,00)';
      return;
    }
    var body = {
      quem_levou: q,
      pin: pin,
      percentual_lucro: (dom.pct && dom.pct.value) || '50',
      incluir_cmv: !!(dom.cmv && dom.cmv.checked),
      incluir_lucro: !!(dom.lucro && dom.lucro.checked),
      incluir_fiado: !!(dom.fiado && dom.fiado.checked),
      modo_dia_cheio: !!(dom.cheio && dom.cheio.checked),
      forma_pagamento: formaPag || 'Dinheiro',
      data_ref: dataRef(),
      incluir_acumulado: !!(dom.acumulado && dom.acumulado.checked),
      separar_reserva: !!(dom.separarReserva && dom.separarReserva.checked),
    };
    // Só manda valor_manual se o operador alterou o campo (senão segue o automático)
    var mv = manualDirty ? parseManualValor() : null;
    if (mv != null) body.valor_manual = String(mv);

    var cofre = (calc && calc.cofrinho) || {};
    var cofreVe = (calc && calc.cofre_vila_elias) || {};
    var pendenteCofre = Number(cofre.pendente_dia || 0);
    var pendenteVe = Number(cofreVe.pendente_dia || 0);
    var levarAuto = 0;
    if (mv != null && mv > 0) {
      levarAuto = mv;
    } else {
      var d = (calc && calc.disponivel) || {};
      if (dom.cmv && dom.cmv.checked) levarAuto += Number(d.cmv || 0);
      if (dom.lucro && dom.lucro.checked) levarAuto += Number(d.lucro || 0);
      if (dom.fiado && dom.fiado.checked) levarAuto += Number(d.fiado || 0);
      var acumN = Number((calc && calc.acumulado_anterior) || 0);
      if (dom.acumulado && dom.acumulado.checked && acumN) levarAuto += acumN;
    }
    if (!(dom.separarReserva && dom.separarReserva.checked)) {
      pendenteCofre = 0;
      pendenteVe = 0;
    }

    openCofreConfirmModal(
      { salario: pendenteCofre, vilaElias: pendenteVe, levar: Math.max(0, levarAuto) },
      function () {
        var vSal = parseMoneyInput(document.getElementById('pdv-rp-cofre-confirm-input-sal'));
        var vVe = parseMoneyInput(document.getElementById('pdv-rp-cofre-confirm-input-ve'));
        var vLev = parseMoneyInput(document.getElementById('pdv-rp-cofre-confirm-input-levar'));
        if (!isFinite(vSal) || !isFinite(vVe) || !isFinite(vLev) || vSal < 0 || vVe < 0 || vLev < 0) {
          if (dom.status) dom.status.textContent = 'Confira os 3 valores (número ≥ 0).';
          return;
        }
        body.valor_cofre_salario = String(vSal);
        body.valor_cofre_vila_elias = String(vVe);
        body.valor_manual = String(vLev);
        body.separar_reserva = vSal > 0.009 || vVe > 0.009;
        body.incluir_acumulado = false;
        if (vLev < 0.009 && vSal < 0.009 && vVe < 0.009) {
          if (dom.status) dom.status.textContent = 'Informe ao menos um valor para confirmar.';
          return;
        }
        if (vLev > 0.009 && autoLinhasZeradas() && !body.forcar_manual_zerado) {
          openForcarManualModal(
            'O cálculo automático deste dia já está zerado (já enviado ou cartão/PIX cobriu). Você está forçando ' +
              money(vLev) +
              '. Confirme com o PIN de novo.',
            body
          );
          return;
        }
        enviarConfirmacao(body);
      }
    );
  }

  function enviarConfirmacao(body) {
    if (busy) return;
    busy = true;
    if (dom.status) dom.status.textContent = 'Transferindo…';
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
          if (j.precisa_forcar_manual && !body.forcar_manual_zerado) {
            openForcarManualModal(j.erro, body);
            return;
          }
          if (dom.status) dom.status.textContent = j.erro || 'Não foi possível transferir';
          return;
        }
        var tot = (j.repasse && j.repasse.valor_total) || 0;
        var saldoCofre = j.cofrinho ? money(j.cofrinho.saldo) : '—';
        var saldoVe = j.cofre_vila_elias ? money(j.cofre_vila_elias.saldo) : '—';
        if (dom.status) {
          if (j.somente_cofres) {
            dom.status.textContent =
              'OK — só cofres · Salário ' + saldoCofre + ' · Vila Elias ' + saldoVe;
          } else {
            dom.status.textContent =
              'OK — enviado ' + money(tot) + ' · Salário ' + saldoCofre + ' · Vila Elias ' + saldoVe;
          }
        }
        if (dom.pin) dom.pin.value = '';
        if (dom.manual) dom.manual.value = '';
        manualDirty = false;
        manualAutoFmt = '';
        notifyParentFecharAtualizar();
        fetchHistoricoMes();
        fetchCalc();
        setTimeout(closeOverlay, 900);
      })
      .catch(function () {
        busy = false;
        if (dom.status) dom.status.textContent = 'Falha de rede';
      });
  }

  var btnOpen =
    document.getElementById('pdv-topbar-repasse-btn') ||
    document.getElementById('crh-btn-repasse');
  if (btnOpen) btnOpen.addEventListener('click', openOverlay);
  if (dom.fechar) dom.fechar.addEventListener('click', closeOverlay);
  if (dom.cancelar) dom.cancelar.addEventListener('click', closeOverlay);
  if (dom.confirmar) {
    dom.confirmar.addEventListener('click', function () {
      pendingConfirmar = true;
      tryConfirmarFlow();
    });
  }

  var quemFechar = document.getElementById('pdv-rp-quem-fechar');
  var quemOk = document.getElementById('pdv-rp-quem-ok');
  if (quemFechar) quemFechar.addEventListener('click', function () { closeQuemModal(true); });
  if (quemOk) quemOk.addEventListener('click', function () { closeQuemModal(); });

  var pinFechar = document.getElementById('pdv-rp-pin-fechar');
  var pinOk = document.getElementById('pdv-rp-pin-ok');
  if (pinFechar) pinFechar.addEventListener('click', function () { closePinModal(true); });
  if (pinOk) pinOk.addEventListener('click', function () { closePinModal(); });

  var cofreCancelar = document.getElementById('pdv-rp-cofre-confirm-cancelar');
  var cofreOk = document.getElementById('pdv-rp-cofre-confirm-ok');
  if (cofreCancelar) cofreCancelar.addEventListener('click', function () { closeCofreConfirmModal(false); });
  if (cofreOk) cofreOk.addEventListener('click', function () { closeCofreConfirmModal(true); });

  var forcarCancelar = document.getElementById('pdv-rp-forcar-manual-cancelar');
  var forcarOk = document.getElementById('pdv-rp-forcar-manual-ok');
  var forcarPin = document.getElementById('pdv-rp-forcar-manual-pin');
  if (forcarCancelar) forcarCancelar.addEventListener('click', closeForcarManualModal);
  if (forcarOk) forcarOk.addEventListener('click', submitForcarManual);
  if (forcarPin) {
    forcarPin.addEventListener('keydown', function (ev) {
      if (ev.key === 'Enter') {
        ev.preventDefault();
        submitForcarManual();
      }
    });
  }

  if (dom.todos) {
    dom.todos.addEventListener('change', function () {
      var on = dom.todos.checked;
      if (dom.cmv) dom.cmv.checked = on;
      if (dom.lucro) dom.lucro.checked = on;
      if (dom.fiado) dom.fiado.checked = on;
      renderCalc();
    });
  }
  [dom.cmv, dom.lucro, dom.fiado, dom.acumulado].forEach(function (el) {
    if (!el) return;
    el.addEventListener('change', function () {
      if (dom.todos && dom.cmv && dom.lucro && dom.fiado) {
        dom.todos.checked = dom.cmv.checked && dom.lucro.checked && dom.fiado.checked;
      }
      renderCalc();
    });
    el.addEventListener('input', renderCalc);
  });
  if (dom.manual) {
    dom.manual.addEventListener('input', function () {
      markManualDirtyFromInput();
      renderCalc();
    });
    dom.manual.addEventListener('focus', function () {
      try {
        dom.manual.select();
      } catch (_) {}
    });
  }
  if (dom.reserva) dom.reserva.addEventListener('input', renderCalc);
  if (dom.salvarReserva) {
    dom.salvarReserva.addEventListener('click', function () {
      if (dom.status) dom.status.textContent = 'Salvando…';
      fetch('/api/repasse-vila/config/', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf() },
        body: JSON.stringify({ reserva_vila: reservaAtual() }),
      })
        .then(function (r) {
          return r.json();
        })
        .then(function (j) {
          if (dom.status) {
            dom.status.textContent = j.ok
              ? 'Reserva diária salva: ' + money(j.reserva_vila)
              : j.erro || 'Erro';
          }
          if (j && j.ok) fetchCalc();
        })
        .catch(function () {
          if (dom.status) dom.status.textContent = 'Falha ao salvar';
        });
    });
  }
  var t = null;
  if (dom.pct) {
    dom.pct.addEventListener('input', function () {
      clearTimeout(t);
      t = setTimeout(fetchCalc, 300);
    });
  }
  if (dom.cheio) dom.cheio.addEventListener('change', fetchCalc);
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

  function onEnterConfirm(ev, fn) {
    if (ev.key === 'Enter') {
      ev.preventDefault();
      fn();
    }
  }
  if (dom.quemOutros) {
    dom.quemOutros.addEventListener('keydown', function (ev) {
      onEnterConfirm(ev, function () { closeQuemModal(); });
    });
  }
  if (dom.pin) {
    dom.pin.addEventListener('keydown', function (ev) {
      onEnterConfirm(ev, function () { closePinModal(); });
    });
  }
  if (dom.manual) {
    dom.manual.addEventListener('keydown', function (ev) {
      if (ev.key === 'Enter') {
        ev.preventDefault();
        pendingConfirmar = true;
        tryConfirmarFlow();
      }
    });
  }

  document.addEventListener('keydown', function (ev) {
    if (ev.key !== 'Escape') return;
    if (forcarManualModal && !forcarManualModal.classList.contains('hidden')) {
      closeForcarManualModal();
      return;
    }
    if (cofreConfirmModal && !cofreConfirmModal.classList.contains('hidden')) {
      closeCofreConfirmModal(false);
      return;
    }
    if (quemModal && !quemModal.classList.contains('hidden')) {
      closeQuemModal(true);
      return;
    }
    if (pinModal && !pinModal.classList.contains('hidden')) {
      closePinModal(true);
      return;
    }
    if (acumModal && !acumModal.classList.contains('hidden')) {
      closeAcumModal();
      return;
    }
    if (overlay && !overlay.classList.contains('hidden')) closeOverlay();
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
    setText('pdv-rp-acum-modal-saldo', money(j.acumulado_anterior));
    setText('pdv-rp-acum-modal-falta', money(j.falta_dia));
    setText('pdv-rp-acum-modal-sug', money(Math.max(0, j.total_sugerido)));
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
      var lbl =
        row.tipo === 'ajuste'
          ? 'Ajuste · ' + (row.observacao || '')
          : fmtDataIso(row.data) +
            ' · alvo ' +
            money(row.alvo_fisico) +
            ' · enviado ' +
            money(row.enviado);
      div.innerHTML =
        '<span class="min-w-0 flex-1">' +
        lbl +
        '</span>' +
        '<span class="tabular-nums font-black ' +
        (delta > 0 ? 'text-amber-800' : delta < 0 ? 'text-sky-800' : 'text-slate-600') +
        '">' +
        (delta > 0 ? '+' : '') +
        money(delta) +
        '</span>';
      acumLista.appendChild(div);
    });
  }

  function fetchAcumModal() {
    if (acumStatus) acumStatus.textContent = 'Carregando…';
    return fetch('/api/repasse-vila/acumulado/?data=' + encodeURIComponent(dataRef()), {
      credentials: 'same-origin',
    })
      .then(function (r) {
        return r.json();
      })
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
    showModal(acumModal);
    fetchAcumModal();
    focusSoon(document.getElementById('pdv-rp-acum-valor'));
  }

  function closeAcumModal() {
    hideModal(acumModal);
  }

  if (acumBtn) acumBtn.addEventListener('click', openAcumModal);
  if (acumFechar) acumFechar.addEventListener('click', closeAcumModal);

  var acumSalvar = document.getElementById('pdv-rp-acum-salvar');
  if (acumSalvar) {
    acumSalvar.addEventListener('click', function () {
      var pin = String((document.getElementById('pdv-rp-acum-pin') || {}).value || '').trim();
      var val = String((document.getElementById('pdv-rp-acum-valor') || {}).value || '').trim();
      var obs = String((document.getElementById('pdv-rp-acum-obs') || {}).value || '').trim();
      if (!pin) {
        if (acumStatus) acumStatus.textContent = 'Digite o PIN';
        return;
      }
      if (!val) {
        if (acumStatus) acumStatus.textContent = 'Informe o valor';
        return;
      }
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
        .then(function (r) {
          return r.json();
        })
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
      if (!pin) {
        if (acumStatus) acumStatus.textContent = 'Digite o PIN';
        return;
      }
      if (!window.confirm('Zerar o acumulado? Use se o dinheiro já foi transferido antes da ferramenta.')) return;
      if (acumStatus) acumStatus.textContent = 'Zerando…';
      fetch('/api/repasse-vila/acumulado/zerar/', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf() },
        body: JSON.stringify({ pin: pin, data_calc: dataRef() }),
      })
        .then(function (r) {
          return r.json();
        })
        .then(function (j) {
          if (!j || !j.ok) {
            if (acumStatus) acumStatus.textContent = (j && j.erro) || 'Erro';
            return;
          }
          if (acumStatus) acumStatus.textContent = 'Acumulado zerado';
          renderAcumModal(j.acumulado);
          fetchCalc();
        })
        .catch(function () {
          if (acumStatus) acumStatus.textContent = 'Falha de rede';
        });
    });
  }

  var acumValor = document.getElementById('pdv-rp-acum-valor');
  var acumPin = document.getElementById('pdv-rp-acum-pin');
  [acumValor, acumPin, document.getElementById('pdv-rp-acum-obs')].forEach(function (el) {
    if (!el) return;
    el.addEventListener('keydown', function (ev) {
      if (ev.key === 'Enter' && acumSalvar) {
        ev.preventDefault();
        acumSalvar.click();
      }
    });
  });
})();
