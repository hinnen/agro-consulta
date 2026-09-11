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
  var pctFromPadraoApplied = false;

  function pctAtual() {
    if (!dom.pct) return '0';
    var v = String(dom.pct.value == null ? '' : dom.pct.value).trim();
    if (v === '') return '0';
    return v;
  }

  function pctPadraoDeMeta(j) {
    var pad = j && j.percentual_padrao;
    if (pad === null || pad === undefined || pad === '') return 50;
    var n = Number(pad);
    if (!isFinite(n)) return 50;
    if (n < 0) n = 0;
    if (n > 100) n = 100;
    return Math.round(n);
  }

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
    fundoTroco: document.getElementById('pdv-rp-fundo-troco'),
    fundoAviso: document.getElementById('pdv-rp-fundo-aviso'),
    salvarReserva: document.getElementById('pdv-rp-salvar-reserva'),
    separarReserva: document.getElementById('pdv-rp-separar-reserva'),
    manual: document.getElementById('pdv-rp-manual'),
    inputCofreSal: document.getElementById('pdv-rp-input-cofre-sal'),
    inputCofreVe: document.getElementById('pdv-rp-input-cofre-ve'),
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
  var cofreCheckModal = document.getElementById('pdv-rp-cofre-check-modal');
  var cofreCheckStep = 0;
  var cofreCheckVals = { salario: 0, vilaElias: 0, levar: 0 };
  var cofreCheckPassosAtivos = [];
  var cofreCheckPending = null;
  var COFRE_CHECK_PASSOS = [
    {
      pergunta: 'Você já separou o dinheiro do cofrinho do funcionário?',
      key: 'salario',
    },
    {
      pergunta: 'Você já separou o dinheiro do cofre Vila Elias?',
      key: 'vilaElias',
    },
    {
      pergunta: 'Você já pegou o dinheiro que vai ser levado para o Centro?',
      key: 'levar',
    },
  ];
  var forcarManualModal = document.getElementById('pdv-rp-forcar-manual-modal');
  var avisoModal = document.getElementById('pdv-rp-aviso-modal');
  var manualDirty = false;
  var manualAutoFmt = '';
  var selectManualPending = false;
  var forcarManualPendingBody = null;
  var cofreSalDirty = false;
  var cofreVeDirty = false;
  var cofreSalAutoFmt = '';
  var cofreVeAutoFmt = '';

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

  function fundoTrocoAtual() {
    if (dom.fundoTroco && String(dom.fundoTroco.value || '').trim() !== '') {
      return parseMoneyBR(dom.fundoTroco.value);
    }
    var c = calc || {};
    if (c.fundo_troco_vila != null) return Number(c.fundo_troco_vila || 0);
    return 500;
  }

  function sugerirFundoTroco(gaveta, alvo, sepSal, sepVe, levar) {
    gaveta = Math.max(0, Number(gaveta || 0));
    alvo = Math.max(0, Number(alvo || 0));
    var baseSal = Math.max(0, Number(sepSal || 0));
    var baseVe = Math.max(0, Number(sepVe || 0));
    var baseCen = Math.max(0, Number(levar || 0));
    var pool = Math.max(0, Math.round((gaveta - alvo) * 100) / 100);
    var sal = Math.min(baseSal, pool);
    pool = Math.round((pool - sal) * 100) / 100;
    var ve = Math.min(baseVe, pool);
    pool = Math.round((pool - ve) * 100) / 100;
    var cen = Math.min(baseCen, pool);
    var sobra = Math.round((gaveta - sal - ve - cen) * 100) / 100;
    var cortouCen = Math.round((baseCen - cen) * 100) / 100;
    var cortouVe = Math.round((baseVe - ve) * 100) / 100;
    var cortouSal = Math.round((baseSal - sal) * 100) / 100;
    var avisos = [];
    if (alvo > 0.009) {
      if (sobra + 0.009 < alvo) {
        avisos.push(
          'Troco ficaria em ' + money(sobra) + ' (alvo ' + money(alvo) + '). Cortamos Centro → Vila Elias → Salário.'
        );
      } else if (sobra > alvo + 0.99) {
        avisos.push(
          'Gaveta ficaria com ' + money(sobra) + ' (alvo ' + money(alvo) + '). Cofres no teto do pendente — sobra fica de troco.'
        );
      }
      if (cortouCen > 0.009 || cortouVe > 0.009 || cortouSal > 0.009) {
        var partes = [];
        if (cortouCen > 0.009) partes.push('Centro −' + money(cortouCen));
        if (cortouVe > 0.009) partes.push('Vila Elias −' + money(cortouVe));
        if (cortouSal > 0.009) partes.push('Salário −' + money(cortouSal));
        avisos.push('Ajuste fundo troco: ' + partes.join(' · '));
      }
    }
    return {
      sep_salario: sal,
      sep_vila_elias: ve,
      levar_centro: cen,
      sobra_gaveta: sobra,
      aviso: avisos.join(' · '),
    };
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

  function syncCofreInput(el, dirty, valor) {
    if (!el) return fmtManualNum(valor);
    if (dirty) return String(el.value || '').trim();
    var fmt = fmtManualNum(valor);
    if (el.value !== fmt) el.value = fmt;
    return fmt;
  }

  function sanitizeMoneyField(el) {
    if (!el) return;
    var s = String(el.value || '');
    if (/[a-zA-Z]/.test(s)) {
      el.value = '';
      return;
    }
    var cleaned = s.replace(/[^\d.,]/g, '');
    if (cleaned !== s) el.value = cleaned;
  }

  function markCofreDirty(which) {
    var el = which === 've' ? dom.inputCofreVe : dom.inputCofreSal;
    var auto = which === 've' ? cofreVeAutoFmt : cofreSalAutoFmt;
    if (!el) return;
    sanitizeMoneyField(el);
    var cur = String(el.value || '').trim();
    var dirty = !(!cur || cur === auto);
    if (which === 've') cofreVeDirty = dirty;
    else cofreSalDirty = dirty;
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
    try {
      if (window.AgroOverlayStack) window.AgroOverlayStack.setOpen(el, true);
    } catch (_) {}
  }

  /** Popup filho interno — NÃO empilhar no AgroOverlayStack (senão o pai congela o clique). */
  function showNestedPopup(el) {
    if (!el) return;
    el.classList.remove('hidden');
    el.classList.add('flex');
  }

  function hideModal(el) {
    if (!el) return;
    el.classList.add('hidden');
    el.classList.remove('flex');
    try {
      if (window.AgroOverlayStack) window.AgroOverlayStack.setOpen(el, false);
    } catch (_) {}
  }

  function hideNestedPopup(el) {
    if (!el) return;
    el.classList.add('hidden');
    el.classList.remove('flex');
  }

  var statusFlash = document.getElementById('pdv-rp-status-flash');
  var statusFlashPanel = document.getElementById('pdv-rp-status-flash-panel');
  var statusFlashTitle = document.getElementById('pdv-rp-status-flash-title');
  var statusFlashMsg = document.getElementById('pdv-rp-status-flash-msg');
  var statusFlashOk = document.getElementById('pdv-rp-status-flash-ok');
  var statusFlashKind = '';

  function paintStatusLine(msg) {
    if (!dom.status) return;
    var t = String(msg || '').trim();
    dom.status.textContent = t;
    if (t) {
      dom.status.classList.remove('hidden');
      dom.status.classList.add('has-msg');
    } else {
      dom.status.classList.add('hidden');
      dom.status.classList.remove('has-msg');
    }
  }

  function hideStatusFlash() {
    statusFlashKind = '';
    hideNestedPopup(statusFlash);
    if (statusFlashOk) statusFlashOk.classList.add('hidden');
  }

  /**
   * Status visível no meio da tela.
   * kind: busy | ok | error | line (só faixa) | '' (limpa)
   */
  function setStatus(msg, kind) {
    var t = String(msg || '').trim();
    var k = kind || (t ? 'line' : '');
    if (!t || k === '') {
      paintStatusLine('');
      hideStatusFlash();
      return;
    }
    paintStatusLine(t);
    if (k === 'line') {
      hideStatusFlash();
      return;
    }
    statusFlashKind = k;
    if (statusFlashTitle) {
      if (k === 'busy') statusFlashTitle.textContent = t.indexOf('Salv') === 0 ? 'Salvando…' : 'Transferindo…';
      else if (k === 'ok') statusFlashTitle.textContent = 'Pronto';
      else statusFlashTitle.textContent = 'Atenção';
    }
    if (statusFlashMsg) {
      statusFlashMsg.textContent = k === 'busy' ? 'Aguarde — não feche esta tela.' : t;
    }
    if (statusFlashPanel) {
      statusFlashPanel.classList.remove('is-busy', 'is-ok', 'is-err');
      statusFlashPanel.classList.add(
        k === 'busy' ? 'is-busy' : k === 'ok' ? 'is-ok' : 'is-err'
      );
    }
    if (statusFlashOk) {
      if (k === 'error') {
        statusFlashOk.classList.remove('hidden');
      } else {
        statusFlashOk.classList.add('hidden');
      }
    }
    showNestedPopup(statusFlash);
    if (k === 'error' && statusFlashOk) focusSoon(statusFlashOk);
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

    var sepJunto = !!(dom.separarReserva && dom.separarReserva.checked);
    var pendSal = sepJunto ? Number(cofre.pendente_dia || 0) : 0;
    var pendVe = sepJunto ? Number(cofreVe.pendente_dia || 0) : 0;
    var cx = c.caixa_vila || {};
    var gaveta = Number(cx.saldo_dinheiro || 0);
    if (!isFinite(gaveta) || gaveta < 0) gaveta = 0;
    var alvoTroco = fundoTrocoAtual();
    // Mesma regra aberto ou fechado: fundo troco + prioridade Salário → VE → Centro.
    // Fechado usa saldo do último fechamento (API) — não mostra «levar 400» no papel.
    var aloc = sugerirFundoTroco(gaveta, alvoTroco, pendSal, pendVe, totAuto);
    totAuto = aloc.levar_centro;
    pendSal = aloc.sep_salario;
    pendVe = aloc.sep_vila_elias;

    syncManualFromAuto(totAuto);
    var mv = manualDirty ? parseManualValor() : null;
    var tot = mv != null ? mv : totAuto;
    // Grande = dia (já com fundo troco) · menor = acumulado
    var diaHero = inclAcum ? Math.max(0, Math.round((totAuto - acum) * 100) / 100) : totAuto;
    setText('pdv-rp-total', money(diaHero));
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

    var cxEl = document.getElementById('pdv-rp-caixa-din');
    var cxHint = document.getElementById('pdv-rp-caixa-din-hint');
    if (cxEl) {
      if (cx.aberto) {
        cxEl.textContent = money(cx.saldo_dinheiro);
        cxEl.className = 'text-3xl sm:text-4xl font-black tabular-nums text-slate-950 leading-none';
        if (cxHint) {
          cxHint.textContent =
            'Esperado na gaveta (antes) · após: ' + money(aloc.sobra_gaveta) + ' (alvo ' + money(alvoTroco) + ')';
        }
      } else {
        cxEl.textContent = 'Fechado';
        cxEl.className = 'text-3xl sm:text-4xl font-black tabular-nums text-red-800 leading-none';
        if (cxHint) {
          var fonte = String(cx.fonte || '');
          if (fonte === 'ultimo_fechamento' && gaveta > 0.009) {
            cxHint.textContent =
              'Último fechamento ' +
              money(gaveta) +
              ' · sugestão com troco ' +
              money(alvoTroco) +
              ' · abra o caixa para transferir';
          } else {
            cxHint.textContent =
              'Sem saldo do último fechamento · sugestão com gaveta 0 · abra o caixa da Vila';
          }
        }
      }
    }

    if (dom.fundoAviso) {
      if (aloc.aviso) {
        dom.fundoAviso.classList.remove('hidden');
        dom.fundoAviso.textContent = aloc.aviso;
      } else {
        dom.fundoAviso.classList.add('hidden');
        dom.fundoAviso.textContent = '';
      }
    }

    var enviarHint = document.getElementById('pdv-rp-enviar-hint');
    if (enviarHint) {
      var difLev = Math.round((tot - totAuto) * 100) / 100;
      if (mv != null && difLev > 0.009) {
        enviarHint.textContent = 'Digitou a mais · excedente ' + money(difLev) + ' desconta amanhã';
      } else if (mv != null && difLev < -0.009) {
        enviarHint.textContent = 'Digitou a menos · falta ' + money(-difLev) + ' soma amanhã';
      } else if (mv != null) {
        enviarHint.textContent = 'Valor digitado · total a levar ' + money(tot);
      } else if (inclAcum && Math.abs(acum) > 0.009) {
        enviarHint.textContent = 'Total a levar ' + money(tot) + ' (dia + acumulado)';
      } else {
        enviarHint.textContent = '';
      }
    }

    var hintOp = document.getElementById('pdv-rp-opcoes-hint');
    if (hintOp && dom.pct) hintOp.textContent = pctAtual() + '%';

    function renderCofreHero(resumo, ids, avisoTxt, valorSep) {
      var pendente = Number(resumo.pendente_dia || 0);
      var adiantado = Number(resumo.adiantado || 0);
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
      var acumEl = document.getElementById(ids.acum);
      if (acumEl) {
        var liquido = pendente - adiantado;
        var apos = Math.round((liquido - Number(valorSep || 0)) * 100) / 100;
        acumEl.textContent = money(apos);
        acumEl.className =
          'text-lg font-black tabular-nums leading-none ' +
          (apos > 0.009 ? 'text-amber-950' : apos < -0.009 ? 'text-sky-900' : 'text-slate-600');
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

    cofreSalAutoFmt = syncCofreInput(dom.inputCofreSal, cofreSalDirty, pendSal);
    cofreVeAutoFmt = syncCofreInput(dom.inputCofreVe, cofreVeDirty, pendVe);
    var vSalNow = parseMoneyInput(dom.inputCofreSal);
    var vVeNow = parseMoneyInput(dom.inputCofreVe);
    if (!isFinite(vSalNow)) vSalNow = pendSal;
    if (!isFinite(vVeNow)) vVeNow = pendVe;

    renderCofreHero(cofre, {
      aSeparar: 'pdv-rp-hero-cofre',
      saldo: 'pdv-rp-hero-cofre-saldo',
      hojeWrap: 'pdv-rp-hero-cofre-hoje-wrap',
      hoje: 'pdv-rp-hero-cofre-hoje',
      aviso: 'pdv-rp-cofre-aviso',
      acum: 'pdv-rp-hero-cofre-acum',
    }, 'NÃO levar no envelope · arredondar puxa o acumulado.', sepJunto ? vSalNow : 0);

    renderCofreHero(cofreVe, {
      aSeparar: 'pdv-rp-hero-cofre-ve',
      saldo: 'pdv-rp-hero-cofre-ve-saldo',
      hojeWrap: 'pdv-rp-hero-cofre-ve-hoje-wrap',
      hoje: 'pdv-rp-hero-cofre-ve-hoje',
      aviso: 'pdv-rp-cofre-ve-aviso',
      acum: 'pdv-rp-hero-cofre-ve-acum',
    }, 'Lucro que fica na Vila · arredondar puxa o acumulado.', sepJunto ? vVeNow : 0);

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
    var pct = pctAtual();
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
          setStatus('');
        } else {
          setStatus((j && j.erro) || 'Falha ao calcular', 'error');
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
    setStatus('');
    if (pendingConfirmar) tryConfirmarFlow();
  }

  function openQuemModal() {
    setStatus('');
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
    setStatus('');
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
    try {
      if (window.AgroOverlayStack) window.AgroOverlayStack.setOpen(overlay, true);
    } catch (_) {}
    pendingConfirmar = false;
    setupDataField();
    applyQueryPrefs();
    manualDirty = false;
    manualAutoFmt = '';
    selectManualPending = true;
    cofreSalDirty = false;
    cofreVeDirty = false;
    cofreSalAutoFmt = '';
    cofreVeAutoFmt = '';
    if (dom.manual) dom.manual.value = '';
    if (dom.inputCofreSal) dom.inputCofreSal.value = '';
    if (dom.inputCofreVe) dom.inputCofreVe.value = '';
    if (dom.pin) dom.pin.value = '';
    quem = '';
    formaPag = 'Dinheiro';
    if (dom.quemOutros) {
      dom.quemOutros.value = '';
      dom.quemOutros.classList.add('hidden');
    }
    sanitizeManualField();
    updateDataHint();
    setStatus('Carregando…', 'line');
    fetch('/api/repasse-vila/meta/', { credentials: 'same-origin' })
      .then(function (r) {
        return r.json();
      })
      .then(function (j) {
        if (!j || !j.ok) {
          setStatus('Falha ao carregar', 'error');
          return;
        }
        funcionarios = j.funcionarios || [];
        if (Array.isArray(j.formas_pagamento) && j.formas_pagamento.length) {
          formasPagamento = j.formas_pagamento;
        }
        if (dom.pct && !pctFromPadraoApplied && !qs().get('pct')) {
          // 0% é válido — não usar `|| 50` (zero falsy voltava sempre pra 50).
          dom.pct.value = String(pctPadraoDeMeta(j));
          pctFromPadraoApplied = true;
        }
        if (dom.reserva && (j.reserva_vila != null || (j.calc && j.calc.reserva_vila != null))) {
          var rv = j.reserva_vila != null ? j.reserva_vila : j.calc.reserva_vila;
          dom.reserva.value = Number(rv || 0).toLocaleString('pt-BR', {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
          });
        }
        if (dom.fundoTroco) {
          var ft =
            j.fundo_troco_vila != null
              ? j.fundo_troco_vila
              : j.calc && j.calc.fundo_troco_vila != null
                ? j.calc.fundo_troco_vila
                : 500;
          dom.fundoTroco.value = Number(ft || 0).toLocaleString('pt-BR', {
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
        if (statusFlashKind !== 'error') setStatus('');
        focusSoon(dom.manual);
      })
      .catch(function () {
        setStatus('Falha de rede', 'error');
      });
  }

  function requestCloseOverlay() {
    if (busy || statusFlashKind === 'busy') {
      setStatus('Aguarde — transferência em andamento. Não feche agora.', 'busy');
      return;
    }
    closeOverlay();
  }

  function closeOverlay() {
    pendingConfirmar = false;
    pctFromPadraoApplied = false;
    busy = false;
    hideStatusFlash();
    paintStatusLine('');
    closeForcarManualModal();
    closeAvisoModal();
    hideModal(quemModal);
    hideModal(formaModal);
    hideModal(pinModal);
    hideModal(cofreConfirmModal);
    hideModal(acumModal);
    closeHistModal();
    overlay.classList.add('hidden');
    overlay.classList.remove('flex');
    document.body.classList.remove('modal-open');
    try {
      if (window.AgroOverlayStack) window.AgroOverlayStack.setOpen(overlay, false);
    } catch (_) {}
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

  function closeCofreCheckModal(okFinal) {
    var cb = cofreCheckPending;
    cofreCheckPending = null;
    cofreCheckStep = 0;
    // Evita o clique do OK “cair” no Cancelar/botão de baixo (ghost click)
    if (cofreCheckModal) {
      try {
        cofreCheckModal.style.pointerEvents = 'none';
      } catch (_) {}
    }
    setTimeout(function () {
      hideModal(cofreCheckModal);
      if (cofreCheckModal) {
        try {
          cofreCheckModal.style.pointerEvents = '';
        } catch (_) {}
      }
      if (okFinal && typeof cb === 'function') cb();
    }, 160);
  }

  function renderCofreCheckPasso() {
    var passo = cofreCheckPassosAtivos[cofreCheckStep];
    if (!passo) {
      closeCofreCheckModal(true);
      return;
    }
    var elP = document.getElementById('pdv-rp-cofre-check-pergunta');
    var elV = document.getElementById('pdv-rp-cofre-check-valor');
    var elS = document.getElementById('pdv-rp-cofre-check-passo');
    if (elP) elP.textContent = passo.pergunta;
    if (elV) elV.textContent = money(cofreCheckVals[passo.key] || 0);
    if (elS) elS.textContent = (cofreCheckStep + 1) + ' de ' + cofreCheckPassosAtivos.length;
    focusSoon(document.getElementById('pdv-rp-cofre-check-ok'));
  }

  function openCofreCheckSequence(opts, onDone) {
    cofreCheckVals = {
      salario: Number(opts.salario || 0),
      vilaElias: Number(opts.vilaElias || 0),
      levar: Number(opts.levar || 0),
    };
    cofreCheckPassosAtivos = COFRE_CHECK_PASSOS.filter(function (p) {
      return Number(cofreCheckVals[p.key] || 0) >= 0.009;
    });
    cofreCheckPending = onDone;
    cofreCheckStep = 0;
    if (!cofreCheckPassosAtivos.length) {
      cofreCheckPending = null;
      if (typeof onDone === 'function') onDone();
      return;
    }
    if (cofreCheckModal) {
      try {
        cofreCheckModal.style.pointerEvents = '';
      } catch (_) {}
    }
    showModal(cofreCheckModal);
    renderCofreCheckPasso();
  }

  function advanceCofreCheck() {
    if (cofreCheckPending == null) return;
    cofreCheckStep += 1;
    if (cofreCheckStep >= cofreCheckPassosAtivos.length) {
      closeCofreCheckModal(true);
      return;
    }
    renderCofreCheckPasso();
  }

  function closeCofreConfirmModal(ok) {
    hideModal(cofreConfirmModal);
    var cb = cofreConfirmPending;
    var vals = cofreConfirmValsSnapshot;
    cofreConfirmPending = null;
    cofreConfirmValsSnapshot = null;
    if (!ok || typeof cb !== 'function') return;
    // Mesma proteção: não abrir o próximo no mesmo tick do clique
    setTimeout(function () {
      openCofreCheckSequence(vals || {}, cb);
    }, 80);
  }

  var cofreConfirmValsSnapshot = null;

  function openCofreConfirmModal(opts, onConfirm) {
    opts = opts || {};
    var elVal = document.getElementById('pdv-rp-cofre-confirm-valor');
    var elVe = document.getElementById('pdv-rp-cofre-confirm-valor-ve');
    var elLev = document.getElementById('pdv-rp-cofre-confirm-valor-levar');
    if (elVal) elVal.textContent = money(opts.salario || 0);
    if (elVe) elVe.textContent = money(opts.vilaElias || 0);
    if (elLev) elLev.textContent = money(opts.levar || 0);
    cofreConfirmValsSnapshot = {
      salario: Number(opts.salario || 0),
      vilaElias: Number(opts.vilaElias || 0),
      levar: Number(opts.levar || 0),
    };
    cofreConfirmPending = onConfirm;
    showModal(cofreConfirmModal);
    focusSoon(document.getElementById('pdv-rp-cofre-confirm-ok'));
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

  function closeAvisoModal() {
    hideNestedPopup(avisoModal);
  }

  function openAvisoModal(msg) {
    var el = document.getElementById('pdv-rp-aviso-msg');
    var texto = msg || 'Não foi possível transferir';
    if (el) el.textContent = texto;
    paintStatusLine(texto);
    hideStatusFlash();
    // Atrasa um pouco p/ não fechar no mesmo clique do OK anterior
    setTimeout(function () {
      showNestedPopup(avisoModal);
      focusSoon(document.getElementById('pdv-rp-aviso-ok'));
    }, 120);
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
    sanitizeMoneyField(dom.inputCofreSal);
    sanitizeMoneyField(dom.inputCofreVe);
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
    function zeroSeVazio(el) {
      if (!el) return;
      if (!String(el.value || '').trim()) el.value = '0,00'; // pode ser 0,00
    }
    zeroSeVazio(dom.inputCofreSal);
    zeroSeVazio(dom.inputCofreVe);
    zeroSeVazio(dom.manual);
    var vSal = parseMoneyInput(dom.inputCofreSal);
    var vVe = parseMoneyInput(dom.inputCofreVe);
    var vLev = parseMoneyInput(dom.manual);
    if (!isFinite(vSal) || !isFinite(vVe) || !isFinite(vLev) || vSal < 0 || vVe < 0 || vLev < 0) {
      openAvisoModal('Confira os 3 valores (número ≥ 0).');
      return;
    }
    if (vLev < 0.009 && vSal < 0.009 && vVe < 0.009) {
      openAvisoModal('Informe ao menos um valor maior que zero.');
      return;
    }
    var body = {
      quem_levou: q,
      pin: pin,
      percentual_lucro: pctAtual(),
      incluir_cmv: !!(dom.cmv && dom.cmv.checked),
      incluir_lucro: !!(dom.lucro && dom.lucro.checked),
      incluir_fiado: !!(dom.fiado && dom.fiado.checked),
      modo_dia_cheio: !!(dom.cheio && dom.cheio.checked),
      forma_pagamento: formaPag || 'Dinheiro',
      data_ref: dataRef(),
      incluir_acumulado: false,
      separar_reserva: vSal > 0.009 || vVe > 0.009,
      valor_cofre_salario: String(vSal),
      valor_cofre_vila_elias: String(vVe),
      valor_manual: String(vLev),
    };
    openCofreConfirmModal({ salario: vSal, vilaElias: vVe, levar: vLev }, function () {
      // Já veio dos 3 campos + 3 OKs — manda direto (sem 4º modal de “forçar”)
      body.forcar_manual_zerado = true;
      enviarConfirmacao(body);
    });
  }

  function enviarConfirmacao(body) {
    if (busy) return;
    busy = true;
    setStatus('Transferindo…', 'busy');
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
          hideStatusFlash();
          if (j.precisa_forcar_manual && !body.forcar_manual_zerado) {
            openForcarManualModal(j.erro, body);
            return;
          }
          openAvisoModal(j.erro || 'Não foi possível transferir');
          return;
        }
        var tot = (j.repasse && j.repasse.valor_total) || 0;
        var saldoCofre = j.cofrinho ? money(j.cofrinho.saldo) : '—';
        var saldoVe = j.cofre_vila_elias ? money(j.cofre_vila_elias.saldo) : '—';
        var okMsg = j.somente_cofres
          ? 'OK — só cofres · Salário ' + saldoCofre + ' · Vila Elias ' + saldoVe
          : 'OK — enviado ' + money(tot) + ' · Salário ' + saldoCofre + ' · Vila Elias ' + saldoVe;
        setStatus(okMsg, 'ok');
        if (dom.pin) dom.pin.value = '';
        if (dom.manual) dom.manual.value = '';
        manualDirty = false;
        manualAutoFmt = '';
        notifyParentFecharAtualizar();
        fetchHistoricoMes();
        fetchCalc();
        setTimeout(closeOverlay, 1100);
      })
      .catch(function () {
        busy = false;
        hideStatusFlash();
        openAvisoModal('Falha de rede');
      });
  }

  var btnOpen =
    document.getElementById('pdv-topbar-repasse-btn') ||
    document.getElementById('crh-btn-repasse');
  if (btnOpen) btnOpen.addEventListener('click', openOverlay);
  if (dom.fechar) dom.fechar.addEventListener('click', requestCloseOverlay);
  if (dom.cancelar) dom.cancelar.addEventListener('click', requestCloseOverlay);
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

  var cofreCheckCancelar = document.getElementById('pdv-rp-cofre-check-cancelar');
  var cofreCheckOk = document.getElementById('pdv-rp-cofre-check-ok');
  if (cofreCheckCancelar) {
    cofreCheckCancelar.addEventListener('click', function () {
      closeCofreCheckModal(false);
    });
  }
  if (cofreCheckOk) {
    cofreCheckOk.addEventListener('click', function () {
      advanceCofreCheck();
    });
  }

  var avisoOk = document.getElementById('pdv-rp-aviso-ok');
  if (avisoOk) avisoOk.addEventListener('click', closeAvisoModal);

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
  if (dom.inputCofreSal) {
    dom.inputCofreSal.addEventListener('input', function () {
      markCofreDirty('sal');
      renderCalc();
    });
    dom.inputCofreSal.addEventListener('focus', function () {
      try {
        dom.inputCofreSal.select();
      } catch (_) {}
    });
  }
  if (dom.inputCofreVe) {
    dom.inputCofreVe.addEventListener('input', function () {
      markCofreDirty('ve');
      renderCalc();
    });
    dom.inputCofreVe.addEventListener('focus', function () {
      try {
        dom.inputCofreVe.select();
      } catch (_) {}
    });
  }
  if (dom.separarReserva) {
    dom.separarReserva.addEventListener('change', function () {
      cofreSalDirty = false;
      cofreVeDirty = false;
      renderCalc();
    });
  }
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
  if (dom.fundoTroco) {
    dom.fundoTroco.addEventListener('input', function () {
      sanitizeMoneyField(dom.fundoTroco);
      renderCalc();
    });
  }
  if (dom.salvarReserva) {
    dom.salvarReserva.addEventListener('click', function () {
      setStatus('Salvando…', 'busy');
      fetch('/api/repasse-vila/config/', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf() },
        body: JSON.stringify({
          reserva_vila: reservaAtual(),
          fundo_troco_vila: fundoTrocoAtual(),
        }),
      })
        .then(function (r) {
          return r.json();
        })
        .then(function (j) {
          if (j.ok) {
            setStatus(
              'Salvo · reserva ' +
                money(j.reserva_vila) +
                ' · fundo troco ' +
                money(j.fundo_troco_vila),
              'ok'
            );
            setTimeout(function () {
              if (statusFlashKind === 'ok') setStatus('');
            }, 1200);
          } else {
            openAvisoModal(j.erro || 'Erro ao salvar');
          }
          if (j && j.ok) {
            if (dom.fundoTroco && j.fundo_troco_vila != null) {
              dom.fundoTroco.value = Number(j.fundo_troco_vila || 0).toLocaleString('pt-BR', {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2,
              });
            }
            fetchCalc();
          }
        })
        .catch(function () {
          hideStatusFlash();
          openAvisoModal('Falha ao salvar');
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
    if (busy || statusFlashKind === 'busy') {
      setStatus('Aguarde — transferência em andamento. Não feche agora.', 'busy');
      return;
    }
    if (statusFlash && !statusFlash.classList.contains('hidden') && statusFlashKind === 'error') {
      hideStatusFlash();
      return;
    }
    if (forcarManualModal && !forcarManualModal.classList.contains('hidden')) {
      closeForcarManualModal();
      return;
    }
    if (avisoModal && !avisoModal.classList.contains('hidden')) {
      closeAvisoModal();
      return;
    }
    if (cofreCheckModal && !cofreCheckModal.classList.contains('hidden')) {
      closeCofreCheckModal(false);
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
    if (overlay && !overlay.classList.contains('hidden')) requestCloseOverlay();
  });

  if (statusFlashOk) {
    statusFlashOk.addEventListener('click', function () {
      hideStatusFlash();
    });
  }

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

  /* —— Histórico Salário / Vila Elias / Centro —— */
  var histModal = document.getElementById('pdv-rp-hist-modal');
  var histLista = document.getElementById('pdv-rp-hist-lista');
  var histStatus = document.getElementById('pdv-rp-hist-status');
  var histTitle = document.getElementById('pdv-rp-hist-title');
  var histPrintPanel = document.getElementById('pdv-rp-hist-print-panel');
  var histKind = 'salario';
  var histItems = [];
  var histPickerBound = false;

  var HIST_TITLES = {
    salario: 'Histórico · Cofrinho Salário',
    vila_elias: 'Histórico · Cofre Vila Elias',
    centro: 'Histórico · Levar ao Centro',
  };

  function isoToday() {
    var d = new Date();
    return d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0');
  }

  function isoDaysAgo(n) {
    var d = new Date();
    d.setDate(d.getDate() - n);
    return d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0');
  }

  function escHtml(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function bindHistPicker() {
    if (histPickerBound) return;
    histPickerBound = true;
    try {
      if (window.AgroDatePicker) {
        window.AgroDatePicker.bind(document.getElementById('pdv-rp-hist-print-dates'), {
          accent: '#ea580c',
          accentSoft: '#fff7ed',
        });
      }
    } catch (_) {}
  }

  function closeHistModal() {
    if (histPrintPanel) histPrintPanel.classList.add('hidden');
    try {
      var cal = document.getElementById('agro-cal-pop-shared');
      if (cal) cal.classList.add('hidden');
    } catch (_) {}
    hideNestedPopup(histModal);
  }

  function openHistModal(kind) {
    histKind = kind || 'salario';
    if (histTitle) histTitle.textContent = HIST_TITLES[histKind] || 'Histórico';
    if (histPrintPanel) histPrintPanel.classList.add('hidden');
    if (histStatus) histStatus.textContent = 'Carregando…';
    if (histLista) histLista.innerHTML = '';
    showNestedPopup(histModal);
    loadHistList();
  }

  function histUrl(de, ate) {
    var q = new URLSearchParams();
    q.set('limit', '200');
    if (de) q.set('de', de);
    if (ate) q.set('ate', ate);
    if (histKind === 'centro') {
      return '/api/repasse-vila/envios/?' + q.toString();
    }
    q.set('cofre', histKind === 'vila_elias' ? 'vila_elias' : 'salario');
    q.set('data', dataRef());
    return '/api/repasse-vila/cofrinho/?' + q.toString();
  }

  function loadHistList(de, ate) {
    if (!de) de = isoDaysAgo(90);
    if (!ate) ate = isoToday();
    if (histStatus) histStatus.textContent = 'Carregando…';
    fetch(histUrl(de, ate), { credentials: 'same-origin' })
      .then(function (r) { return r.json(); })
      .then(function (j) {
        if (!j || !j.ok) {
          histItems = [];
          if (histStatus) histStatus.textContent = (j && j.erro) || 'Falha ao carregar';
          if (histLista) histLista.innerHTML = '';
          return;
        }
        histItems = j.movimentos || j.envios || [];
        renderHistList(histItems);
        if (histStatus) {
          histStatus.textContent = histItems.length
            ? histItems.length + ' movimento(s) · últimos 90 dias'
            : 'Nenhum movimento neste período';
        }
      })
      .catch(function () {
        histItems = [];
        if (histStatus) histStatus.textContent = 'Falha de rede';
      });
  }

  function fmtHistQuando(it) {
    if (it && it.criado_em_label) return String(it.criado_em_label);
    var raw = it && (it.criado_em || it.data_ref);
    if (!raw) return '—';
    var s = String(raw).trim();
    // já dd/mm/yyyy …
    if (/^\d{2}\/\d{2}\/\d{4}/.test(s)) return s.length > 16 ? s.slice(0, 16) : s;
    // ISO 2026-08-29T09:30:31…
    var m = s.match(/^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2}))/);
    if (m) return m[3] + '/' + m[2] + '/' + m[1].slice(2) + ' ' + m[4] + ':' + m[5];
    var d = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (d) return d[3] + '/' + d[2] + '/' + d[1].slice(2);
    return s;
  }

  function moneyParts(n) {
    var t = Number(Math.abs(n || 0)).toLocaleString('pt-BR', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
    return { sym: 'R$', val: t };
  }

  /** Entrada = ↑ verde · Saída = ↓ vermelha (cofres: sinal do valor; Centro: sempre saída). */
  function histDirecao(it) {
    if (histKind === 'centro') return 'out';
    var v = Number(it && it.valor != null ? it.valor : 0);
    if (!isFinite(v) || v === 0) {
      var t = String((it && it.tipo) || '').toLowerCase();
      if (t === 'retirada') return 'out';
      if (t === 'separacao' || t === 'saldo_inicial') return 'in';
      return 'in';
    }
    return v < 0 ? 'out' : 'in';
  }

  function renderHistList(items) {
    if (!histLista) return;
    histLista.innerHTML = '';
    if (!items || !items.length) {
      histLista.innerHTML = '<p class="text-sm font-bold text-slate-500 py-6 text-center">Sem movimentações.</p>';
      return;
    }
    var table = document.createElement('table');
    table.className = 'rp-hist-table';
    table.innerHTML =
      '<thead><tr>' +
      '<th class="rp-hist-col-data">Data</th>' +
      '<th class="rp-hist-col-tipo">Tipo</th>' +
      '<th class="rp-hist-col-quem">Quem</th>' +
      '<th class="rp-hist-col-valor">Valor</th>' +
      '</tr></thead>';
    var tbody = document.createElement('tbody');
    items.forEach(function (it) {
      var quando = fmtHistQuando(it);
      var tipo = it.tipo_label || it.tipo || 'Movimento';
      var quem = it.operador || it.quem_levou || '—';
      var rawVal = it.valor != null ? it.valor : it.valor_total;
      var mp = moneyParts(rawVal);
      var dir = histDirecao(it);
      var arrow = dir === 'out' ? '↓' : '↑';
      var dirTitle = dir === 'out' ? 'Saída' : 'Entrada';
      var detParts = [];
      if (it.origem_label) detParts.push('<div><b>Origem:</b> ' + escHtml(it.origem_label) + '</div>');
      if (it.saldo_anterior != null && it.saldo_posterior != null) {
        detParts.push(
          '<div><b>Saldo:</b> ' + money(it.saldo_anterior) + ' → ' + money(it.saldo_posterior) + '</div>'
        );
      }
      if (it.valor_cmv != null) detParts.push('<div><b>CMV:</b> ' + money(it.valor_cmv) + '</div>');
      if (it.valor_lucro != null) detParts.push('<div><b>Lucro enviado:</b> ' + money(it.valor_lucro) + '</div>');
      if (it.valor_fiado != null) detParts.push('<div><b>Fiado:</b> ' + money(it.valor_fiado) + '</div>');
      if (it.percentual_lucro != null) detParts.push('<div><b>% lucro:</b> ' + escHtml(it.percentual_lucro) + '%</div>');
      if (it.status_centro) detParts.push('<div><b>Status:</b> ' + escHtml(it.status_centro) + '</div>');
      if (it.plano_nome) detParts.push('<div><b>Plano:</b> ' + escHtml(it.plano_nome) + '</div>');
      if (it.observacao) detParts.push('<div><b>Obs:</b> ' + escHtml(it.observacao) + '</div>');
      if (it.estornado) detParts.push('<div class="font-black text-rose-700">Já estornado</div>');
      if (it.repasse_id) detParts.push('<div><b>Repasse #</b>' + escHtml(it.repasse_id) + '</div>');
      if (it.id && histKind === 'centro') detParts.push('<div><b>Envio #</b>' + escHtml(it.id) + '</div>');
      detParts.unshift(
        '<div><b>Sentido:</b> ' + (dir === 'out' ? 'Saída ↓' : 'Entrada ↑') + '</div>'
      );
      if (!detParts.length) detParts.push('<div class="text-slate-500">Sem detalhes extras.</div>');

      var tr = document.createElement('tr');
      tr.className = 'rp-hist-tr';
      tr.innerHTML =
        '<td class="rp-hist-col-data">' + escHtml(quando) + '</td>' +
        '<td class="rp-hist-col-tipo truncate" title="' + escHtml(tipo) + '">' + escHtml(tipo) + '</td>' +
        '<td class="rp-hist-col-quem truncate" title="' + escHtml(quem) + '">' + escHtml(quem) + '</td>' +
        '<td class="rp-hist-col-valor"><span class="rp-hist-moeda rp-hist-moeda--' +
        dir +
        '" title="' +
        dirTitle +
        '"><span class="rp-hist-dir rp-hist-dir--' +
        dir +
        '" aria-hidden="true">' +
        arrow +
        '</span><span class="rp-hist-moeda-sym">' +
        mp.sym +
        '</span><span class="rp-hist-moeda-val">' +
        escHtml(mp.val) +
        '</span></span></td>';

      var trDet = document.createElement('tr');
      trDet.className = 'rp-hist-det-tr';
      trDet.innerHTML =
        '<td colspan="4"><div class="rp-hist-det space-y-0.5">' + detParts.join('') + '</div></td>';

      tr.addEventListener('click', function () {
        var open = !tr.classList.contains('is-open');
        tr.classList.toggle('is-open', open);
        trDet.classList.toggle('is-open', open);
      });

      tbody.appendChild(tr);
      tbody.appendChild(trDet);
    });
    table.appendChild(tbody);
    histLista.appendChild(table);
  }

  function openHistPrintPanel() {
    var deEl = document.getElementById('pdv-rp-hist-de');
    var ateEl = document.getElementById('pdv-rp-hist-ate');
    if (deEl && !deEl.value) deEl.value = isoDaysAgo(30);
    if (ateEl && !ateEl.value) ateEl.value = isoToday();
    if (histPrintPanel) histPrintPanel.classList.remove('hidden');
    bindHistPicker();
  }

  function ensureHistPrintIframe() {
    var id = 'pdv-rp-hist-print-iframe';
    var iframe = document.getElementById(id);
    if (!iframe) {
      iframe = document.createElement('iframe');
      iframe.id = id;
      iframe.title = 'Impressão histórico repasse';
      iframe.setAttribute('aria-hidden', 'true');
      iframe.style.cssText =
        'position:fixed;right:0;bottom:0;width:0;height:0;border:0;opacity:0;pointer-events:none;';
      document.body.appendChild(iframe);
    }
    return iframe;
  }

  function histStyles80mm() {
    return (
      '@page{margin:0;size:80mm auto}' +
      'html,body{margin:0;padding:0}' +
      'body{font-family:system-ui,Segoe UI,sans-serif;width:80mm;max-width:80mm;margin:0 auto;' +
      '-webkit-print-color-adjust:exact;print-color-adjust:exact;color:#000;background:#fff}' +
      '.pg{width:80mm;max-width:80mm;margin:0 auto;padding:2mm 3mm 0;box-sizing:border-box;' +
      'font-size:12px;line-height:1.25;overflow:visible}' +
      '.tit{text-align:center;font-size:13px;font-weight:900;text-transform:uppercase;' +
      'letter-spacing:.04em;border:2px solid #000;padding:5px 4px;margin:0 0 6px}' +
      '.meta{text-align:center;font-size:11px;font-weight:700;margin:0 0 8px}' +
      '.item{border-top:1px dashed #000;padding:5px 0 4px;page-break-inside:avoid;break-inside:avoid}' +
      '.item:first-of-type{border-top:2px solid #000}' +
      '.linha{display:flex;justify-content:space-between;align-items:baseline;gap:4px}' +
      '.data{font-size:11px;font-weight:800}' +
      '.valor{font-size:16px;font-weight:900;white-space:nowrap;font-variant-numeric:tabular-nums}' +
      '.tipo{font-size:12px;font-weight:900;margin-top:1px}' +
      '.quem{font-size:11px;font-weight:700}' +
      '.sub{font-size:10px;font-weight:600;margin-top:1px}' +
      '.tot{border-top:2px solid #000;margin-top:6px;padding-top:5px;font-weight:900;' +
      'display:flex;justify-content:space-between;font-size:13px}' +
      '.corte{display:block;height:14mm;min-height:14mm;line-height:14mm;font-size:1px;' +
      'color:transparent;overflow:hidden;margin:0;padding:0}' +
      '@media print{.item{page-break-inside:avoid;break-inside:avoid}.corte{display:block;height:14mm;min-height:14mm}}'
    );
  }

  function histStylesA4() {
    return (
      '@page{margin:12mm}' +
      'body{font:12px/1.35 system-ui,sans-serif;margin:0;padding:8mm;box-sizing:border-box;color:#000}' +
      'h1{font-size:14px;margin:0 0 6px;text-transform:uppercase}' +
      '.meta{font-size:11px;margin-bottom:10px}' +
      'table{width:100%;border-collapse:collapse;table-layout:fixed}' +
      'th{text-align:left;font-size:10px;text-transform:uppercase;border-bottom:2px solid #000;padding:3px 2px}' +
      'td{padding:4px 2px;border-bottom:1px solid #ccc;vertical-align:top}' +
      'td.v{text-align:right;font-weight:900;white-space:nowrap;font-size:13px;font-variant-numeric:tabular-nums}' +
      '.sub{font-size:10px;font-weight:600}' +
      '@media print{body{-webkit-print-color-adjust:exact;print-color-adjust:exact}}'
    );
  }

  function printHist(formato) {
    var deEl = document.getElementById('pdv-rp-hist-de');
    var ateEl = document.getElementById('pdv-rp-hist-ate');
    var de = (deEl && deEl.value) || isoDaysAgo(30);
    var ate = (ateEl && ateEl.value) || isoToday();
    if (histStatus) histStatus.textContent = 'Montando impressão…';
    fetch(histUrl(de, ate), { credentials: 'same-origin' })
      .then(function (r) { return r.json(); })
      .then(function (j) {
        var itens = (j && j.ok) ? (j.movimentos || j.envios || []) : [];
        if (!itens.length) {
          if (histStatus) histStatus.textContent = 'Nada para imprimir neste período';
          return;
        }
        var titulo = HIST_TITLES[histKind] || 'Histórico';
        var deLbl = de.split('-').reverse().join('/');
        var ateLbl = ate.split('-').reverse().join('/');
        var styles;
        var bodyHtml;
        if (formato === '80mm') {
          styles = histStyles80mm();
          var soma = 0;
          var blocos = itens.map(function (it) {
            var quando = fmtHistQuando(it);
            var tipo = it.tipo_label || it.tipo || '';
            var quem = it.operador || it.quem_levou || '';
            var rawVal = Number(it.valor != null ? it.valor : it.valor_total) || 0;
            var dir = histDirecao(it);
            var arrow = dir === 'out' ? '-' : '+';
            var abs = Math.abs(rawVal);
            soma += dir === 'out' ? -abs : abs;
            var extras = [];
            if (it.origem_label) extras.push(it.origem_label);
            if (it.observacao) extras.push(it.observacao);
            return (
              '<div class="item">' +
              '<div class="linha"><span class="data">' + escHtml(quando) +
              '</span><span class="valor">' + arrow + ' ' + money(abs) + '</span></div>' +
              '<div class="tipo">' + escHtml(tipo) + '</div>' +
              '<div class="quem">' + escHtml(quem) + '</div>' +
              (extras.length ? '<div class="sub">' + escHtml(extras.join(' · ')) + '</div>' : '') +
              '</div>'
            );
          }).join('');
          bodyHtml =
            '<div class="pg">' +
            '<div class="tit">' + escHtml(titulo) + '</div>' +
            '<div class="meta">De ' + escHtml(deLbl) + ' ate ' + escHtml(ateLbl) +
            '<br>' + itens.length + ' movimento(s)</div>' +
            blocos +
            '<div class="tot"><span>Liquido</span><span>' + money(soma) + '</span></div>' +
            '<div class="corte" aria-hidden="true">&nbsp;</div>' +
            '</div>';
        } else {
          styles = histStylesA4();
          var rows = itens.map(function (it) {
            var quando = fmtHistQuando(it);
            var tipo = it.tipo_label || it.tipo || '';
            var quem = it.operador || it.quem_levou || '';
            var rawVal = it.valor != null ? it.valor : it.valor_total;
            var dir = histDirecao(it);
            var arrow = dir === 'out' ? '-' : '+';
            var valor = arrow + ' ' + money(Math.abs(Number(rawVal || 0)));
            var extra = [];
            if (it.origem_label) extra.push(it.origem_label);
            if (it.observacao) extra.push(it.observacao);
            return (
              '<tr><td>' + escHtml(quando) + '</td><td>' + escHtml(tipo) +
              '</td><td>' + escHtml(quem) +
              (extra.length ? '<div class="sub">' + escHtml(extra.join(' · ')) + '</div>' : '') +
              '</td><td class="v">' + valor + '</td></tr>'
            );
          }).join('');
          bodyHtml =
            '<h1>' + escHtml(titulo) + '</h1>' +
            '<div class="meta">De ' + escHtml(deLbl) + ' até ' + escHtml(ateLbl) +
            ' · ' + itens.length + ' item(ns)</div>' +
            '<table><thead><tr><th>Data</th><th>Tipo</th><th>Quem</th><th>Valor</th></tr></thead><tbody>' +
            rows +
            '</tbody></table>';
        }
        var iframe = ensureHistPrintIframe();
        var idoc = iframe.contentDocument || (iframe.contentWindow && iframe.contentWindow.document);
        if (!idoc) {
          if (histStatus) histStatus.textContent = 'Não foi possível preparar a impressão';
          return;
        }
        idoc.open();
        idoc.write(
          '<!DOCTYPE html><html><head><meta charset="utf-8"><title>' +
            escHtml(titulo) +
            '</title><style>' +
            styles +
            '</style></head><body>' +
            bodyHtml +
            '</body></html>'
        );
        idoc.close();
        setTimeout(function () {
          try {
            iframe.contentWindow.focus();
            iframe.contentWindow.print();
            if (histStatus) histStatus.textContent = 'Impressão · ' + itens.length + ' item(ns)';
            if (histPrintPanel) histPrintPanel.classList.add('hidden');
          } catch (ePr) {
            if (histStatus) histStatus.textContent = 'Falha ao abrir a impressora';
          }
        }, 120);
      })
      .catch(function () {
        if (histStatus) histStatus.textContent = 'Falha de rede na impressão';
      });
  }

  var btnHistSal = document.getElementById('pdv-rp-hist-sal');
  var btnHistVe = document.getElementById('pdv-rp-hist-ve');
  var btnHistCentro = document.getElementById('pdv-rp-hist-centro');
  if (btnHistSal) btnHistSal.addEventListener('click', function () { openHistModal('salario'); });
  if (btnHistVe) btnHistVe.addEventListener('click', function () { openHistModal('vila_elias'); });
  if (btnHistCentro) btnHistCentro.addEventListener('click', function () { openHistModal('centro'); });

  var histVoltar = document.getElementById('pdv-rp-hist-voltar');
  if (histVoltar) histVoltar.addEventListener('click', closeHistModal);
  var histImprimir = document.getElementById('pdv-rp-hist-imprimir');
  if (histImprimir) histImprimir.addEventListener('click', openHistPrintPanel);
  var histPrintCancel = document.getElementById('pdv-rp-hist-print-cancel');
  if (histPrintCancel) {
    histPrintCancel.addEventListener('click', function () {
      if (histPrintPanel) histPrintPanel.classList.add('hidden');
    });
  }
  var histPrint80 = document.getElementById('pdv-rp-hist-print-80');
  var histPrintA4 = document.getElementById('pdv-rp-hist-print-a4');
  if (histPrint80) histPrint80.addEventListener('click', function () { printHist('80mm'); });
  if (histPrintA4) histPrintA4.addEventListener('click', function () { printHist('a4'); });
})();
