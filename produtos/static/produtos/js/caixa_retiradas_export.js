/**
 * Exportação Excel — histórico retiradas / saídas (/caixa/retiradas/).
 */
(function () {
  'use strict';

  var CFG = window.CRH_EXPORT || {};
  var btnExp = document.getElementById('crh-btn-export-xlsx');
  if (!btnExp) return;
  if (!CFG.urlExport) {
    console.warn('CRH_EXPORT.urlExport ausente — export Excel desativado.');
    return;
  }

  var modal = document.getElementById('crh-export-modal');
  var back = document.getElementById('crh-export-back');
  var elCols = document.getElementById('crh-export-colunas');
  var btnBaixar = document.getElementById('crh-export-baixar');
  var btnFec = document.getElementById('crh-export-fechar');
  var inpDe = document.getElementById('crh-export-de');
  var inpAte = document.getElementById('crh-export-ate');
  var selPlano = document.getElementById('crh-export-plano');
  var selQuem = document.getElementById('crh-export-quem');
  var radTela = document.getElementById('crh-export-fonte-tela');
  var radCustom = document.getElementById('crh-export-fonte-custom');
  var painelCustom = document.getElementById('crh-export-painel-custom');
  var elStatus = document.getElementById('crh-export-status');

  var formFiltros = document.getElementById('crh-filtros');
  var telaDe = document.getElementById('id-de');
  var telaAte = document.getElementById('id-ate');
  var telaPlano = document.getElementById('id-plano');
  var telaQuem = document.getElementById('id-quem');

  var exportColsDef = [
    { key: 'data', label: 'Data', fixa: true },
    { key: 'hora', label: 'Hora', fixa: true },
    { key: 'operador_pin', label: 'Operador (PIN)', fixa: true },
    { key: 'forma', label: 'Forma de pagamento', fixa: true },
    { key: 'plano', label: 'Plano de contas' },
    { key: 'quem', label: 'Quem levou' },
    { key: 'valor', label: 'Valor (R$)' },
    { key: 'banco', label: 'Conta / banco' },
    { key: 'descricao', label: 'Descrição' },
    { key: 'observacoes', label: 'Observações' },
    { key: 'sessao_id', label: 'Sessão caixa' },
    { key: 'fonte', label: 'Fonte (financeiro / caixa)' },
    { key: 'registro_id', label: 'ID registro' }
  ];

  function escapeHtml(s) {
    return String(s || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function montarColunasExport() {
    if (!elCols) return;
    elCols.innerHTML = exportColsDef.map(function (c) {
      var chk = c.fixa ? ' checked disabled' : ' checked';
      var hint = c.fixa
        ? ' <span class="text-[10px] font-black uppercase text-orange-600">(sempre)</span>'
        : '';
      return (
        '<label class="flex items-center gap-2 min-h-[40px] px-2 rounded-lg hover:bg-orange-50/80">' +
        '<input type="checkbox" class="crh-export-col-cb w-5 h-5" data-key="' +
        escapeHtml(c.key) +
        '"' +
        chk +
        ' />' +
        '<span>' +
        escapeHtml(c.label) +
        hint +
        '</span></label>'
      );
    }).join('');
  }

  function syncPainelCustom() {
    var custom = radCustom && radCustom.checked;
    if (painelCustom) {
      painelCustom.classList.toggle('hidden', !custom);
      painelCustom.setAttribute('aria-hidden', custom ? 'false' : 'true');
    }
    if (custom && inpDe && telaDe && telaDe.value) inpDe.value = telaDe.value;
    if (custom && inpAte && telaAte && telaAte.value) inpAte.value = telaAte.value;
    if (custom && selPlano && telaPlano) selPlano.value = telaPlano.value || '';
    if (custom && selQuem && telaQuem) selQuem.value = telaQuem.value || '';
  }

  function lerFiltrosExport() {
    var usarTela = !radCustom || !radCustom.checked;
    var de = '';
    var ate = '';
    var plano = '';
    var quem = '';
    var completo = false;
    if (usarTela) {
      de = (telaDe && telaDe.value) || '';
      ate = (telaAte && telaAte.value) || '';
      plano = (telaPlano && telaPlano.value) || '';
      quem = (telaQuem && telaQuem.value) || '';
    } else {
      de = (inpDe && inpDe.value) || '';
      ate = (inpAte && inpAte.value) || '';
      plano = (selPlano && selPlano.value) || '';
      quem = (selQuem && selQuem.value) || '';
      var chkCompleto = document.getElementById('crh-export-completo');
      completo = !!(chkCompleto && chkCompleto.checked);
      if (completo) {
        plano = '';
        quem = '';
      }
    }
    return { de: de, ate: ate, plano: plano, quem: quem, completo: completo };
  }

  function atualizarStatus() {
    if (!elStatus) return;
    var f = lerFiltrosExport();
    var partes = [];
    if (f.de && f.ate) partes.push(f.de === f.ate ? f.de : f.de + ' → ' + f.ate);
    if (f.completo) {
      partes.push('plano e quem: todos');
    } else {
      if (f.plano) partes.push('plano filtrado');
      else partes.push('todos os planos');
      if (f.quem) partes.push('quem: ' + f.quem);
    }
    elStatus.textContent = 'Será exportado: ' + partes.join(' · ') + '.';
  }

  function abrirExport() {
    montarColunasExport();
    if (radTela) radTela.checked = true;
    syncPainelCustom();
    atualizarStatus();
    if (modal) modal.classList.remove('hidden');
    if (back) back.classList.remove('hidden');
    if (window.AgroDatePicker && painelCustom) {
      window.AgroDatePicker.bind(painelCustom, { accent: '#ea580c', accentSoft: '#fff7ed' });
    }
  }

  function fecharExport() {
    if (modal) modal.classList.add('hidden');
    if (back) back.classList.add('hidden');
  }

  function presetColunasMinimas() {
    if (!elCols) return;
    elCols.querySelectorAll('.crh-export-col-cb').forEach(function (cb) {
      if (cb.disabled) return;
      var k = cb.getAttribute('data-key');
      cb.checked = k === 'plano' || k === 'quem' || k === 'valor';
    });
  }

  function presetColunasTodas() {
    if (!elCols) return;
    elCols.querySelectorAll('.crh-export-col-cb').forEach(function (cb) {
      if (!cb.disabled) cb.checked = true;
    });
  }

  function aplicarPresetFiltro(tipo) {
    if (radCustom) radCustom.checked = true;
    syncPainelCustom();
    var chkCompleto = document.getElementById('crh-export-completo');
    if (chkCompleto) chkCompleto.checked = false;
    if (tipo === 'completo') {
      if (chkCompleto) chkCompleto.checked = true;
      if (selPlano) selPlano.value = '';
      if (selQuem) selQuem.value = '';
    } else if (tipo === 'plano' && selPlano && telaPlano) {
      selPlano.value = telaPlano.value || '';
      if (selQuem) selQuem.value = '';
    } else if (tipo === 'quem' && selQuem && telaQuem) {
      selQuem.value = telaQuem.value || '';
      if (selPlano) selPlano.value = '';
    } else if (tipo === 'periodo') {
      if (selPlano) selPlano.value = '';
      if (selQuem) selQuem.value = '';
    }
    atualizarStatus();
  }

  btnExp.addEventListener('click', abrirExport);
  if (btnFec) btnFec.addEventListener('click', fecharExport);
  if (back) back.addEventListener('click', fecharExport);

  if (radTela) radTela.addEventListener('change', function () {
    syncPainelCustom();
    atualizarStatus();
  });
  if (radCustom) radCustom.addEventListener('change', function () {
    syncPainelCustom();
    atualizarStatus();
  });

  ['crh-export-completo', 'crh-export-de', 'crh-export-ate', 'crh-export-plano', 'crh-export-quem'].forEach(function (id) {
    var el = document.getElementById(id);
    if (el) el.addEventListener('change', atualizarStatus);
    if (el && el.type === 'date') el.addEventListener('input', atualizarStatus);
  });

  var btnPresetPeriodo = document.getElementById('crh-export-preset-periodo');
  var btnPresetPlano = document.getElementById('crh-export-preset-plano');
  var btnPresetQuem = document.getElementById('crh-export-preset-quem');
  var btnPresetCompleto = document.getElementById('crh-export-preset-completo');
  var btnColsMin = document.getElementById('crh-export-cols-min');
  var btnColsAll = document.getElementById('crh-export-cols-all');

  if (btnPresetPeriodo) btnPresetPeriodo.addEventListener('click', function () { aplicarPresetFiltro('periodo'); });
  if (btnPresetPlano) btnPresetPlano.addEventListener('click', function () { aplicarPresetFiltro('plano'); });
  if (btnPresetQuem) btnPresetQuem.addEventListener('click', function () { aplicarPresetFiltro('quem'); });
  if (btnPresetCompleto) btnPresetCompleto.addEventListener('click', function () { aplicarPresetFiltro('completo'); });
  if (btnColsMin) btnColsMin.addEventListener('click', presetColunasMinimas);
  if (btnColsAll) btnColsAll.addEventListener('click', presetColunasTodas);

  if (btnBaixar) {
    btnBaixar.addEventListener('click', function () {
      var cols = [];
      if (elCols) {
        elCols.querySelectorAll('.crh-export-col-cb').forEach(function (cb) {
          if (cb.checked || cb.disabled) cols.push(cb.getAttribute('data-key'));
        });
      }
      if (!cols.length) cols = ['data', 'hora', 'operador_pin', 'forma'];
      var f = lerFiltrosExport();
      var params = new URLSearchParams();
      if (f.de) params.set('de', f.de);
      if (f.ate) params.set('ate', f.ate);
      if (f.plano) params.set('plano', f.plano);
      if (f.quem) params.set('quem', f.quem);
      if (f.completo) params.set('completo', '1');
      params.set('cols', cols.join(','));
      fecharExport();
      window.location.href = CFG.urlExport + '?' + params.toString();
    });
  }

  document.addEventListener('keydown', function (ev) {
    if (ev.key === 'Escape' && modal && !modal.classList.contains('hidden')) {
      fecharExport();
    }
  });
})();
