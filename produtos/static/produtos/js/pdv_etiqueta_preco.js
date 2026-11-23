/**
 * PDV — aviso para trocar etiqueta quando o preço da prateleira muda.
 */
(function () {
  var btn = document.getElementById('pdv-topbar-etiqueta-preco-btn');
  var countEl = document.getElementById('pdv-topbar-etiqueta-preco-count');
  if (!btn || !countEl) return;

  var itens = [];
  var loja = '';
  var painel = null;

  function csrf() {
    var m = document.querySelector('meta[name=csrfmiddlewaretoken]');
    return m ? m.getAttribute('content') : '';
  }

  function esc(s) {
    return String(s || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function pintar() {
    var n = itens.length;
    countEl.textContent = String(n);
    btn.classList.toggle('hidden', n === 0);
    if (painel && !vendoHistorico) desenhar();
  }

  function carregar() {
    fetch('/api/pdv/etiqueta-preco/', { credentials: 'same-origin' })
      .then(function (r) { return r.json(); })
      .then(function (j) {
        if (!j || !j.ok) return;
        itens = j.itens || [];
        loja = j.loja || '';
        pintar();
      })
      .catch(function () {});
  }

  var esperaUso = null;
  function aoUsarPdv() {
    if (esperaUso) return;
    esperaUso = setTimeout(function () {
      esperaUso = null;
      carregar();
    }, 1200);
  }

  function post(acao, extra) {
    var body = Object.assign({ acao: acao }, extra || {});
    return fetch('/api/pdv/etiqueta-preco/', {
      method: 'POST',
      credentials: 'same-origin',
      headers: {
        'Content-Type': 'application/json',
        'X-CSRFToken': csrf(),
      },
      body: JSON.stringify(body),
    }).then(function (r) { return r.json(); });
  }

  function cupom() {
    var linhas = itens.map(function (it) {
      return (
        '<div style="margin:0 0 8px;border-bottom:1px dashed #000;padding-bottom:6px;">' +
        '<div style="font-weight:900;font-size:13px;">' + esc(it.nome) + '</div>' +
        (it.codigo ? '<div style="font-size:11px;">' + esc(it.codigo) + '</div>' : '') +
        '<div style="font-size:12px;">De R$ ' + esc(it.preco_de) + '</div>' +
        '<div style="font-weight:900;font-size:16px;">Para R$ ' + esc(it.preco_para) + '</div>' +
        '</div>'
      );
    }).join('');
    var html =
      '<!DOCTYPE html><html><head><meta charset="utf-8"><title>Etiquetas para trocar</title>' +
      '<style>@page{margin:0;size:80mm auto}html,body{margin:0;padding:0;width:80mm}' +
      'body{font-family:system-ui,sans-serif;color:#000;background:#fff}' +
      '.pg{width:80mm;box-sizing:border-box;padding:4mm 3mm}</style></head><body><div class="pg">' +
      '<div style="text-align:center;font-weight:900;font-size:16px;">TROCAR ETIQUETA</div>' +
      '<div style="text-align:center;font-weight:800;margin-bottom:8px;">' + esc(loja) + ' · ' + itens.length + '</div>' +
      linhas +
      '<div style="text-align:center;font-size:11px;font-weight:800;">Marque Já troquei no PDV</div>' +
      '</div></body></html>';
    var w = window.open('', 'etq-preco-80', 'width=420,height=700');
    if (!w) return;
    w.document.open();
    w.document.write(html);
    w.document.close();
    w.focus();
    setTimeout(function () { try { w.print(); } catch (e) {} }, 250);
  }

  var vendoHistorico = false;

  function cabecalho(modo) {
    var cab = painel && painel.querySelector('[data-cabeca]');
    if (!cab) return;
    cab.innerHTML = modo === 'hist'
      ? '<span>Produto</span><span>Código</span><span>Preço</span><span>Hora</span><span>Usuário</span>'
      : '<span>Produto</span><span>Código</span><span>Era</span><span>Agora</span><span></span>';
  }

  function desenharHistorico(lista) {
    if (!painel) return;
    vendoHistorico = true;
    cabecalho('hist');
    var corpo = (lista || []).map(function (it) {
      return (
        '<div style="display:grid;grid-template-columns:minmax(16rem,1fr) 7rem 6.5rem 8.5rem 8rem;align-items:center;gap:0.75rem;border-bottom:1px solid #e2e8f0;padding:0.45rem 0;">' +
        '<div style="min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:0.95rem;font-weight:900;color:#0f172a;" title="' + esc(it.nome) + '">' + esc(it.nome) + '</div>' +
        '<div style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:0.8rem;font-weight:700;color:#64748b;">' + esc(it.codigo) + '</div>' +
        '<div style="font-size:0.95rem;font-weight:900;color:#0f172a;">R$ ' + esc(it.preco) + '</div>' +
        '<div style="font-size:0.85rem;font-weight:800;color:#0f172a;">' + esc(it.quando) + '</div>' +
        '<div style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:0.85rem;font-weight:800;color:#0f172a;">' + esc(it.usuario || '—') + '</div>' +
        '</div>'
      );
    }).join('');
    painel.querySelector('[data-lista]').innerHTML = corpo || '<p class="py-4 text-center font-bold text-slate-500">Nenhuma troca registrada.</p>';
    painel.querySelector('[data-loja]').textContent = 'Já trocadas · ' + (lista || []).length;
    var btn = painel.querySelector('[data-hist]');
    if (btn) btn.textContent = 'Voltar';
  }

  function alternarHistorico() {
    if (vendoHistorico) {
      vendoHistorico = false;
      cabecalho('pend');
      var btn = painel.querySelector('[data-hist]');
      if (btn) btn.textContent = 'Já trocadas';
      pintar();
      return;
    }
    fetch('/api/pdv/etiqueta-preco/?historico=1', { credentials: 'same-origin' })
      .then(function (r) { return r.json(); })
      .then(function (j) {
        if (!j || !j.ok) return;
        desenharHistorico(j.historico || []);
      })
      .catch(function () {});
  }

  function desenhar() {
    cabecalho('pend');
    var btn = painel && painel.querySelector('[data-hist]');
    if (btn) btn.textContent = 'Já trocadas';
    if (!painel) return;
    var corpo = itens.map(function (it) {
      return (
        '<div style="display:grid;grid-template-columns:minmax(16rem,1fr) 7rem 6.5rem 8.5rem 8rem;align-items:center;gap:0.75rem;border-bottom:1px solid #e2e8f0;padding:0.45rem 0;">' +
        '<div style="min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:0.95rem;font-weight:900;color:#0f172a;" title="' + esc(it.nome) + '">' + esc(it.nome) + '</div>' +
        '<div style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:0.8rem;font-weight:700;color:#64748b;">' + esc(it.codigo) + '</div>' +
        '<div style="font-size:0.85rem;font-weight:700;color:#475569;">R$ ' + esc(it.preco_de) + '</div>' +
        '<div style="font-size:1rem;font-weight:900;color:#c2410c;">R$ ' + esc(it.preco_para) + '</div>' +
        '<button type="button" data-ok="' + it.id + '" class="rounded-xl border-2 border-emerald-700 bg-emerald-600 px-2 py-2 text-sm font-black text-white">Já troquei</button>' +
        '</div>'
      );
    }).join('');
    painel.querySelector('[data-lista]').innerHTML = corpo || '<p class="py-4 text-center font-bold text-slate-500">Nada pendente.</p>';
    painel.querySelector('[data-loja]').textContent = loja + ' · ' + itens.length;
  }

  function preencherPresets(selectEl) {
    var Core = window.AgroEtiquetasCore;
    if (!Core || !selectEl) return;
    Core.fillPresetSelect(selectEl, null);
    if (typeof Core.refreshPresetsFromServer === 'function') {
      Core.refreshPresetsFromServer().then(function (list) {
        Core.fillPresetSelect(selectEl, null, list);
      }).catch(function () {});
    }
  }

  function abrirImpressaoPdv() {
    var Core = window.AgroEtiquetasCore;
    if (!Core || !itens.length) return;
    var modal = document.getElementById('pdv-pe-etq-modal');
    var back = document.getElementById('pdv-pe-etq-back');
    var nomeEl = document.getElementById('pdv-pe-etq-nome');
    var gmEl = document.getElementById('pdv-pe-etq-gm');
    var qtdEl = document.getElementById('pdv-pe-etq-qtd');
    var presetEl = document.getElementById('pdv-pe-etq-preset');
    var statusEl = document.getElementById('pdv-pe-etq-status');
    var btnImp = document.getElementById('pdv-pe-etq-imprimir');
    if (!modal || !presetEl || !btnImp) return;
    if (itens.length === 1) {
      if (nomeEl) nomeEl.textContent = itens[0].nome || '—';
      if (gmEl) gmEl.textContent = (itens[0].codigo || '') + ' · R$ ' + (itens[0].preco_para || '');
    } else {
      if (nomeEl) nomeEl.textContent = itens.length + ' etiquetas para trocar';
      if (gmEl) gmEl.textContent = loja || '';
    }
    if (qtdEl) qtdEl.value = '1';
    if (statusEl) statusEl.textContent = '';
    preencherPresets(presetEl);
    modal.dataset.etqPreco = '1';
    if (back) {
      back.classList.remove('hidden');
      back.setAttribute('aria-hidden', 'false');
    }
    modal.classList.remove('hidden');
    modal.setAttribute('aria-hidden', 'false');
    if (btnImp.dataset.etqPrecoLigado !== '1') {
      btnImp.dataset.etqPrecoLigado = '1';
      btnImp.addEventListener('click', function (ev) {
        if (modal.dataset.etqPreco !== '1') return;
        ev.stopImmediatePropagation();
        imprimirPendentes();
      }, true);
    }
  }

  function fecharImpressaoPdv() {
    var modal = document.getElementById('pdv-pe-etq-modal');
    var back = document.getElementById('pdv-pe-etq-back');
    if (modal) {
      modal.dataset.etqPreco = '';
      modal.classList.add('hidden');
      modal.setAttribute('aria-hidden', 'true');
    }
    if (back) {
      back.classList.add('hidden');
      back.setAttribute('aria-hidden', 'true');
    }
  }

  function imprimirPendentes() {
    var Core = window.AgroEtiquetasCore;
    var presetEl = document.getElementById('pdv-pe-etq-preset');
    var qtdEl = document.getElementById('pdv-pe-etq-qtd');
    var statusEl = document.getElementById('pdv-pe-etq-status');
    var btnImp = document.getElementById('pdv-pe-etq-imprimir');
    if (!Core || !itens.length) return;
    var qtd = parseInt(qtdEl && qtdEl.value, 10) || 1;
    if (qtd < 1) qtd = 1;
    if (qtd > 999) qtd = 999;
    var presetId = presetEl && presetEl.value;
    var st = Core.loadStorage();
    if (presetId) {
      st.preset_ativo = presetId;
      Core.saveStorage(st);
    }
    var lista = itens.map(function (it) {
      return Core.produtoParaItem({
        id: it.produto_id,
        nome: it.nome,
        codigo_gm: it.codigo,
        codigo_barras: /^\d{8,}$/.test(String(it.codigo || '')) ? it.codigo : '',
        preco_venda: it.preco_para_num,
      }, qtd);
    });
    if (statusEl) statusEl.textContent = 'Enviando…';
    if (btnImp) btnImp.disabled = true;
    Core.imprimirItens(lista, {
      presetId: presetId || st.preset_ativo,
      textoRodape: (st.texto_rodape_global || (Core.getPresetAtivo(st).texto_rodape) || ''),
      origem: 'pdv_preco',
    }).then(function (res) {
      if (res && res.ok) {
        fecharImpressaoPdv();
        return;
      }
      if (statusEl) statusEl.textContent = 'Falha: ' + ((res && res.reason) || 'erro');
    }).catch(function (err) {
      if (statusEl) statusEl.textContent = 'Falha: ' + ((err && err.message) || 'erro de impressão');
    }).then(function () {
      if (btnImp) btnImp.disabled = false;
    });
  }

  function abrir() {
    if (!painel) {
      painel = document.createElement('div');
      painel.className = 'fixed inset-0 z-[80] flex items-center justify-center bg-slate-900/50 p-3';
      painel.innerHTML =
        '<div class="flex max-h-[90dvh] w-[min(96vw,72rem)] flex-col rounded-2xl border-2 border-orange-600 bg-white shadow-2xl">' +
        '<div class="flex items-center justify-between gap-2 border-b border-orange-200 bg-orange-50 px-3 py-2">' +
        '<div><div class="text-base font-black uppercase text-orange-900">Trocar etiqueta</div>' +
        '<div data-loja class="text-xs font-bold text-slate-600"></div></div>' +
        '<button type="button" data-fechar class="rounded-xl border-2 border-slate-400 bg-white px-3 py-2 text-sm font-black">Fechar</button></div>' +
        '<div data-cabeca style="display:grid;grid-template-columns:minmax(16rem,1fr) 7rem 6.5rem 8.5rem 8rem;gap:0.75rem;padding:0.35rem 0.75rem 0;font-size:10px;font-weight:900;letter-spacing:0.04em;text-transform:uppercase;color:#94a3b8;">' +
        '<span>Produto</span><span>Código</span><span>Era</span><span>Agora</span><span></span></div>' +
        '<div data-lista class="min-h-0 flex-1 overflow-y-auto px-3"></div>' +
        '<div class="flex flex-wrap gap-2 border-t border-slate-200 p-3">' +
        '<button type="button" data-cupom class="rounded-xl border-2 border-slate-800 bg-slate-900 px-3 py-2 text-sm font-black text-white">Lista 80mm</button>' +
        '<button type="button" data-etq class="rounded-xl border-2 border-orange-800 bg-orange-500 px-3 py-2 text-sm font-black text-white">Imprimir etiquetas</button>' +
        '<button type="button" data-hist class="rounded-xl border-2 border-slate-400 bg-white px-3 py-2 text-sm font-black text-slate-800">Já trocadas</button>' +
        '</div></div>';
      document.body.appendChild(painel);
      painel.addEventListener('click', function (ev) {
        var t = ev.target;
        if (t === painel || t.closest('[data-fechar]')) {
          painel.classList.add('hidden');
          return;
        }
        if (t.closest('[data-cupom]')) {
          cupom();
          return;
        }
        if (t.closest('[data-etq]')) {
          abrirImpressaoPdv();
          return;
        }
        if (t.closest('[data-hist]')) {
          alternarHistorico();
          return;
        }
        var ok = t.closest('[data-ok]');
        if (!ok) return;
        var idOk = Number(ok.getAttribute('data-ok'));
        var marcar = function () {
          post('ok', { id: idOk }).then(function (j) {
            if (!j || !j.ok) return;
            itens = j.itens || [];
            loja = j.loja || loja;
            pintar();
          });
        };
        if (typeof window.gmSspinGarantirOperador === 'function') {
          window.gmSspinGarantirOperador(marcar, { titulo: 'PIN de quem trocou a etiqueta' });
          return;
        }
        marcar();
      });
    }
    painel.classList.remove('hidden');
    desenhar();
  }

  btn.addEventListener('click', abrir);
  document.addEventListener('click', aoUsarPdv, true);
  document.addEventListener('keydown', aoUsarPdv, true);
  window.addEventListener('focus', carregar);
  carregar();
  setInterval(carregar, 10000);
  window.AgroEtiquetaPreco = { carregar: carregar };
})();
