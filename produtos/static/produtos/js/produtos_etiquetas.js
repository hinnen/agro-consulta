(function () {
  'use strict';

  var Core = window.AgroEtiquetasCore;
  if (!Core) return;

  var URL_BUSCAR = '/api/produtos/cadastro/';

  var state = {
    fila: [],
    storage: null,
    buscaTimer: null,
  };

  function $(id) {
    return document.getElementById(id);
  }

  function uid() {
    return 'etq-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 8);
  }

  function produtoParaFilaItem(prod) {
    var it = Core.produtoParaItem(prod, 1);
    it.fila_id = uid();
    return it;
  }

  function reloadStorage() {
    state.storage = Core.loadStorage();
  }

  function persistStorage() {
    Core.saveStorage(state.storage);
  }

  function getPresetAtivo() {
    return Core.getPresetAtivo(state.storage);
  }

  function renderPresetOptions(selectEl, activeId) {
    if (!selectEl || !state.storage) return;
    selectEl.innerHTML = state.storage.presets
      .map(function (p) {
        return (
          '<option value="' +
          Core.esc(p.id) +
          '"' +
          (p.id === activeId ? ' selected' : '') +
          '>' +
          Core.esc(p.nome) +
          '</option>'
        );
      })
      .join('');
  }

  function renderPresetSelect() {
    renderPresetOptions($('etq-preset-select'), state.storage.preset_ativo);
    renderPresetOptions($('etq-fila-preset'), state.storage.preset_ativo);
  }

  function renderPresetForm() {
    var p = getPresetAtivo();
    var map = {
      'etq-preset-nome': p.nome,
      'etq-preset-largura': p.largura_mm,
      'etq-preset-altura': p.altura_mm,
      'etq-preset-nome-pt': p.nome_pt,
      'etq-preset-preco-pt': p.preco_pt,
      'etq-preset-codigo-pt': p.codigo_pt,
      'etq-preset-rodape-pt': p.rodape_pt,
      'etq-preset-bar-h': p.barcode_height,
      'etq-preset-bar-w': p.barcode_width,
      'etq-preset-texto-rodape': p.texto_rodape,
    };
    Object.keys(map).forEach(function (k) {
      var el = $(k);
      if (el) el.value = map[k];
    });
    var tr = $('etq-texto-rodape-global');
    if (tr && !tr.dataset.touched) {
      tr.value = state.storage.texto_rodape_global || p.texto_rodape || '';
    }
    carregarImpressoras(p.impressora || '');
  }

  function renderFila() {
    var tbody = $('etq-fila-body');
    var badge = $('etq-fila-total');
    if (!tbody) return;
    var totalEtq = state.fila.reduce(function (acc, it) {
      return acc + Math.max(1, parseInt(it.qtd, 10) || 1);
    }, 0);
    if (badge) badge.textContent = String(totalEtq);

    if (!state.fila.length) {
      tbody.innerHTML =
        '<tr><td colspan="5" class="px-3 py-4 text-center text-sm text-slate-500">Nenhum produto na fila.</td></tr>';
      return;
    }

    tbody.innerHTML = state.fila
      .map(function (it) {
        return (
          '<tr class="border-t border-slate-700/80">' +
          '<td class="px-3 py-1.5 text-sm font-semibold text-white truncate max-w-[14rem]">' +
          Core.esc(it.nome || '—') +
          '</td>' +
          '<td class="px-3 py-1.5 text-xs font-mono text-slate-300">' +
          Core.esc(it.codigo_gm || '—') +
          '</td>' +
          '<td class="px-3 py-1.5 text-sm font-bold text-emerald-400">' +
          Core.esc(Core.fmtPreco(it.preco_venda)) +
          '</td>' +
          '<td class="px-3 py-1.5">' +
          '<input type="number" min="1" max="999" value="' +
          Core.esc(it.qtd) +
          '" data-fila-qtd="' +
          Core.esc(it.fila_id) +
          '" class="w-14 min-h-[36px] rounded-lg border border-slate-600 bg-slate-900 px-1 text-center text-sm font-bold text-white" />' +
          '</td>' +
          '<td class="px-3 py-1.5 text-right">' +
          '<button type="button" data-fila-del="' +
          Core.esc(it.fila_id) +
          '" class="rounded-lg border border-red-700/60 px-2 py-1 text-[10px] font-bold uppercase text-red-300">×</button>' +
          '</td>' +
          '</tr>'
        );
      })
      .join('');

    tbody.querySelectorAll('[data-fila-qtd]').forEach(function (inp) {
      inp.addEventListener('change', function () {
        var id = inp.getAttribute('data-fila-qtd');
        var it = state.fila.find(function (x) {
          return x.fila_id === id;
        });
        if (!it) return;
        var q = parseInt(inp.value, 10);
        it.qtd = q > 0 ? q : 1;
        inp.value = String(it.qtd);
        renderFila();
      });
    });
    tbody.querySelectorAll('[data-fila-del]').forEach(function (btn) {
      btn.addEventListener('click', function () {
        var id = btn.getAttribute('data-fila-del');
        state.fila = state.fila.filter(function (x) {
          return x.fila_id !== id;
        });
        renderFila();
      });
    });
  }

  function renderBusca(produtos) {
    var box = $('etq-busca-resultados');
    if (!box) return;
    if (!produtos || !produtos.length) {
      box.innerHTML = '<p class="px-3 py-3 text-sm text-slate-500">Nenhum produto.</p>';
      return;
    }
    box.innerHTML = produtos
      .slice(0, 24)
      .map(function (p) {
        var gm = String(p.codigo_nfe || p.codigo_gm || p.codigo || '').trim();
        return (
          '<button type="button" class="etq-busca-item flex w-full items-center justify-between gap-2 border-b border-slate-700/70 px-3 py-2 text-left hover:bg-slate-700/40" data-prod-id="' +
          Core.esc(p.id) +
          '">' +
          '<span class="min-w-0 flex-1 truncate text-sm font-bold text-white">' +
          Core.esc(p.nome || '—') +
          '</span>' +
          '<span class="shrink-0 text-xs text-slate-400">' +
          Core.esc(gm) +
          ' · ' +
          Core.esc(Core.fmtPreco(p.preco_venda)) +
          '</span>' +
          '<span class="shrink-0 rounded bg-emerald-600 px-1.5 py-0.5 text-[10px] font-black text-white">+</span>' +
          '</button>'
        );
      })
      .join('');

    box.querySelectorAll('.etq-busca-item').forEach(function (btn) {
      btn.addEventListener('click', function () {
        var id = btn.getAttribute('data-prod-id');
        var prod = produtos.find(function (x) {
          return String(x.id) === String(id);
        });
        if (!prod) return;
        var qtdInp = $('etq-add-qtd');
        var qtd = parseInt(qtdInp && qtdInp.value, 10) || 1;
        var it = produtoParaFilaItem(prod);
        it.qtd = qtd > 0 ? qtd : 1;
        state.fila.push(it);
        renderFila();
      });
    });
  }

  function buscarProdutos(q) {
    var box = $('etq-busca-resultados');
    if (!q || q.length < 2) {
      if (box) box.innerHTML = '<p class="px-3 py-3 text-sm text-slate-500">Digite ao menos 2 caracteres.</p>';
      return;
    }
    if (box) box.innerHTML = '<p class="px-3 py-3 text-sm text-slate-400">…</p>';
    fetch(URL_BUSCAR + '?q=' + encodeURIComponent(q) + '&limit=24', { credentials: 'same-origin' })
      .then(function (r) {
        return r.json();
      })
      .then(function (data) {
        if (!data || data.ok === false) {
          if (box) box.innerHTML = '<p class="px-3 py-3 text-sm text-red-400">Erro na busca.</p>';
          return;
        }
        renderBusca((data && data.produtos) || []);
      })
      .catch(function () {
        if (box) box.innerHTML = '<p class="px-3 py-3 text-sm text-red-400">Erro na busca.</p>';
      });
  }

  function setStatus(msg, isErr) {
    var el = $('etq-status');
    if (!el || !msg) {
      if (el) el.textContent = '';
      return;
    }
    el.textContent = msg;
    el.className = 'text-xs font-semibold ' + (isErr ? 'text-red-400' : 'text-emerald-400');
    if (!isErr) setTimeout(function () { if (el.textContent === msg) el.textContent = ''; }, 2500);
  }

  function presetIdFila() {
    var sel = $('etq-fila-preset');
    return (sel && sel.value) || state.storage.preset_ativo;
  }

  function imprimirFila() {
    if (!state.fila.length) return;
    var presetId = presetIdFila();
    state.storage.preset_ativo = presetId;
    persistStorage();
    var textoRodape =
      ($('etq-texto-rodape-global') && $('etq-texto-rodape-global').value.trim()) ||
      state.storage.texto_rodape_global ||
      getPresetAtivo().texto_rodape ||
      '';
    Core.imprimirItens(state.fila, { presetId: presetId, textoRodape: textoRodape }).then(function (res) {
      if (res && res.ok) setStatus('Enviado.');
      else if (res && res.reason) setStatus('Falha: ' + res.reason, true);
    });
  }

  function lerPresetForm() {
    var p = getPresetAtivo();
    p.nome = ($('etq-preset-nome') && $('etq-preset-nome').value.trim()) || p.nome;
    p.largura_mm = Number($('etq-preset-largura') && $('etq-preset-largura').value) || 40;
    p.altura_mm = Number($('etq-preset-altura') && $('etq-preset-altura').value) || 40;
    p.nome_pt = Number($('etq-preset-nome-pt') && $('etq-preset-nome-pt').value) || 8;
    p.preco_pt = Number($('etq-preset-preco-pt') && $('etq-preset-preco-pt').value) || 28;
    p.codigo_pt = Number($('etq-preset-codigo-pt') && $('etq-preset-codigo-pt').value) || 7;
    p.rodape_pt = Number($('etq-preset-rodape-pt') && $('etq-preset-rodape-pt').value) || 8;
    p.barcode_height = Number($('etq-preset-bar-h') && $('etq-preset-bar-h').value) || 26;
    p.barcode_width = Number($('etq-preset-bar-w') && $('etq-preset-bar-w').value) || 1.05;
    p.texto_rodape = ($('etq-preset-texto-rodape') && $('etq-preset-texto-rodape').value) || '';
    p.impressora = ($('etq-preset-impressora') && $('etq-preset-impressora').value.trim()) || '';
    return p;
  }

  function salvarPresetAtual() {
    var p = lerPresetForm();
    var nome = prompt('Nome do preset (ex.: OPE 7):', p.nome);
    if (!nome) return;
    p.nome = nome.trim() || p.nome;
    var idx = state.storage.presets.findIndex(function (x) {
      return x.id === p.id;
    });
    if (idx >= 0) state.storage.presets[idx] = p;
    state.storage.preset_ativo = p.id;
    persistStorage();
    renderPresetSelect();
    setStatus('Preset salvo.');
  }

  function criarNovoPreset() {
    var nome = prompt('Nome do novo preset:', 'OPE 7');
    if (!nome) return;
    var np = Core.clonePreset(getPresetAtivo());
    np.id = 'preset-' + Date.now().toString(36);
    np.nome = nome.trim() || 'Novo';
    state.storage.presets.push(np);
    state.storage.preset_ativo = np.id;
    persistStorage();
    renderPresetSelect();
    renderPresetForm();
    setStatus('Preset criado.');
  }

  function excluirPresetAtual() {
    if (state.storage.presets.length <= 1) {
      alert('Mantenha ao menos um preset.');
      return;
    }
    var p = getPresetAtivo();
    if (!confirm('Excluir «' + p.nome + '»?')) return;
    state.storage.presets = state.storage.presets.filter(function (x) {
      return x.id !== p.id;
    });
    state.storage.preset_ativo = state.storage.presets[0].id;
    persistStorage();
    renderPresetSelect();
    renderPresetForm();
  }

  function carregarImpressoras(atual) {
    var sel = $('etq-preset-impressora');
    if (!sel) return;
    if (!(window.agroShell && typeof window.agroShell.listPrinters === 'function')) {
      sel.innerHTML = '<option value="">(Padrão do Windows)</option>';
      if (atual) sel.value = atual;
      return;
    }
    window.agroShell.listPrinters().then(function (res) {
      if (!res || !res.ok) return;
      var opts = ['<option value="">(Padrão do Windows)</option>'];
      (res.printers || []).forEach(function (p) {
        opts.push(
          '<option value="' +
            Core.esc(p.name) +
            '"' +
            (atual === p.name || (!atual && p.isDefault) ? ' selected' : '') +
            '>' +
            Core.esc(p.name) +
            '</option>'
        );
      });
      sel.innerHTML = opts.join('');
    });
  }

  function ensureModalOnBody() {
    var m = $('etq-modal-back');
    if (m && m.parentElement !== document.body) document.body.appendChild(m);
  }

  function abrirModalPreset() {
    ensureModalOnBody();
    var m = $('etq-modal-back');
    if (!m) return;
    m.classList.remove('hidden');
    m.setAttribute('aria-hidden', 'false');
    document.body.classList.add('modal-open');
    renderPresetForm();
  }

  function fecharModalPreset() {
    var m = $('etq-modal-back');
    if (!m) return;
    m.classList.add('hidden');
    m.setAttribute('aria-hidden', 'true');
    document.body.classList.remove('modal-open');
  }

  function bindEvents() {
    var inpBusca = $('etq-busca-input');
    if (inpBusca) {
      inpBusca.addEventListener('input', function () {
        clearTimeout(state.buscaTimer);
        state.buscaTimer = setTimeout(function () {
          buscarProdutos(inpBusca.value.trim());
        }, 280);
      });
      inpBusca.addEventListener('keydown', function (ev) {
        if (ev.key === 'Enter') {
          ev.preventDefault();
          buscarProdutos(inpBusca.value.trim());
        }
      });
    }

    $('etq-btn-imprimir') && $('etq-btn-imprimir').addEventListener('click', imprimirFila);
    $('etq-btn-limpar') &&
      $('etq-btn-limpar').addEventListener('click', function () {
        if (state.fila.length && !confirm('Limpar fila?')) return;
        state.fila = [];
        renderFila();
      });
    $('etq-btn-salvar-preset') && $('etq-btn-salvar-preset').addEventListener('click', salvarPresetAtual);
    $('etq-btn-novo-preset') && $('etq-btn-novo-preset').addEventListener('click', criarNovoPreset);
    $('etq-btn-excluir-preset') && $('etq-btn-excluir-preset').addEventListener('click', excluirPresetAtual);
    $('etq-btn-preset') && $('etq-btn-preset').addEventListener('click', abrirModalPreset);
    $('etq-modal-fechar') && $('etq-modal-fechar').addEventListener('click', fecharModalPreset);
    $('etq-modal-back') &&
      $('etq-modal-back').addEventListener('click', function (ev) {
        if (ev.target === $('etq-modal-back')) fecharModalPreset();
      });

    ['etq-preset-select', 'etq-fila-preset'].forEach(function (id) {
      var sel = $(id);
      if (!sel) return;
      sel.addEventListener('change', function () {
        state.storage.preset_ativo = sel.value;
        persistStorage();
        renderPresetSelect();
        if (id === 'etq-preset-select') renderPresetForm();
      });
    });

    var tr = $('etq-texto-rodape-global');
    if (tr) {
      tr.addEventListener('input', function () {
        tr.dataset.touched = '1';
        state.storage.texto_rodape_global = tr.value;
        persistStorage();
      });
    }

    document.addEventListener('keydown', function (ev) {
      if (ev.key === 'Escape') fecharModalPreset();
    });
  }

  function init() {
    ensureModalOnBody();
    reloadStorage();
    renderPresetSelect();
    renderPresetForm();
    renderFila();
    bindEvents();
    var inp = $('etq-busca-input');
    if (inp) inp.focus();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
