(function () {
  'use strict';

  var Core = window.AgroEtiquetasCore;
  if (!Core) return;

  var URL_BUSCAR = '/api/produtos/cadastro/';
  var URL_HISTORICO = '/api/produtos/etiquetas/historico/';
  var HISTORICO_DIAS = 30;

  var state = {
    fila: [],
    storage: null,
    buscaTimer: null,
    buscaProdutos: [],
    buscaSelIdx: -1,
    buscaQuery: '',
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
    return Core.normalizarPreset(Core.getPresetAtivo(state.storage));
  }

  var layoutDrag = null;

  function bindLayoutEditor() {
    var stage = $('etq-layout-stage');
    if (!stage || stage.dataset.layBound) return;
    stage.dataset.layBound = '1';

    function pctFromEvent(ev, rect) {
      return {
        x: ((ev.clientX - rect.left) / rect.width) * 100,
        y: ((ev.clientY - rect.top) / rect.height) * 100,
      };
    }

    stage.addEventListener('pointerdown', function (ev) {
      var handle = ev.target.closest('.etq-lay-handle');
      var item = ev.target.closest('.etq-lay-item');
      if (!item || !stage.contains(item)) return;
      ev.preventDefault();
      var rect = stage.getBoundingClientRect();
      var box = {
        x: parseFloat(item.style.left) || 0,
        y: parseFloat(item.style.top) || 0,
        w: parseFloat(item.style.width) || 10,
        h: parseFloat(item.style.height) || 10,
      };
      var pt = pctFromEvent(ev, rect);
      layoutDrag = {
        el: item,
        mode: handle ? 'resize' : 'move',
        start: pt,
        box: box,
        pointerId: ev.pointerId,
      };
      try {
        stage.setPointerCapture(ev.pointerId);
      } catch (e) {}
    });

    stage.addEventListener('pointermove', function (ev) {
      if (!layoutDrag || layoutDrag.pointerId !== ev.pointerId) return;
      var rect = stage.getBoundingClientRect();
      var pt = pctFromEvent(ev, rect);
      var dx = pt.x - layoutDrag.start.x;
      var dy = pt.y - layoutDrag.start.y;
      var b = layoutDrag.box;
      var nx = b.x;
      var ny = b.y;
      var nw = b.w;
      var nh = b.h;
      if (layoutDrag.mode === 'move') {
        nx = Math.max(0, Math.min(100 - b.w, b.x + dx));
        ny = Math.max(0, Math.min(100 - b.h, b.y + dy));
      } else {
        nw = Math.max(8, Math.min(100 - b.x, b.w + dx));
        nh = Math.max(8, Math.min(100 - b.y, b.h + dy));
      }
      layoutDrag.el.style.left = Math.round(nx * 10) / 10 + '%';
      layoutDrag.el.style.top = Math.round(ny * 10) / 10 + '%';
      layoutDrag.el.style.width = Math.round(nw * 10) / 10 + '%';
      layoutDrag.el.style.height = Math.round(nh * 10) / 10 + '%';
    });

    function endDrag(ev) {
      if (!layoutDrag || (ev && layoutDrag.pointerId !== ev.pointerId)) return;
      layoutDrag = null;
      var p = lerPresetForm();
      var idx = state.storage.presets.findIndex(function (x) {
        return x.id === p.id;
      });
      if (idx >= 0) state.storage.presets[idx] = p;
      persistStorage();
    }
    stage.addEventListener('pointerup', endDrag);
    stage.addEventListener('pointercancel', endDrag);
  }

  function commitPresetFormLive() {
    var p = lerPresetForm();
    var idx = state.storage.presets.findIndex(function (x) {
      return x.id === p.id;
    });
    if (idx >= 0) state.storage.presets[idx] = p;
    persistStorage();
    togglePresetFields(p.estilo || 'termica');
    syncLayoutStageSize(p);
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

  function togglePresetFields(estilo) {
    var gondola = estilo === 'gondola';
    document.querySelectorAll('.etq-field-gondola').forEach(function (el) {
      el.classList.toggle('hidden', !gondola);
    });
    document.querySelectorAll('.etq-field-termica').forEach(function (el) {
      el.classList.toggle('hidden', gondola);
    });
  }

  function applyLayoutBoxes(layout) {
    var stage = $('etq-layout-stage');
    if (!stage || !layout) return;
    stage.querySelectorAll('.etq-lay-item').forEach(function (el) {
      var id = el.getAttribute('data-lay');
      var box = layout[id];
      if (!box) return;
      el.style.left = Number(box.x) + '%';
      el.style.top = Number(box.y) + '%';
      el.style.width = Number(box.w) + '%';
      el.style.height = Number(box.h) + '%';
    });
  }

  function readLayoutBoxes() {
    var stage = $('etq-layout-stage');
    var out = {};
    if (!stage) return out;
    stage.querySelectorAll('.etq-lay-item').forEach(function (el) {
      var id = el.getAttribute('data-lay');
      out[id] = {
        x: Math.round(parseFloat(el.style.left) * 10) / 10 || 0,
        y: Math.round(parseFloat(el.style.top) * 10) / 10 || 0,
        w: Math.round(parseFloat(el.style.width) * 10) / 10 || 10,
        h: Math.round(parseFloat(el.style.height) * 10) / 10 || 10,
      };
    });
    return out;
  }

  function syncLayoutStageSize(p) {
    var stage = $('etq-layout-stage');
    if (!stage) return;
    var wMm = Number(p.largura_mm) || 90;
    var hMm = Number(p.altura_mm) || 35;
    var maxW = 420;
    var scale = maxW / wMm;
    stage.style.width = Math.round(wMm * scale) + 'px';
    stage.style.height = Math.round(hMm * scale) + 'px';
    var cores = (p.cores && typeof p.cores === 'object') ? p.cores : {};
    stage.style.background = cores.fundo || '#ffffff';
    stage.style.borderColor = cores.borda || cores.faixa_bg || '#1a4d2e';
  }

  function renderPresetForm() {
    var p = Core.normalizarPreset(getPresetAtivo());
    var idx = state.storage.presets.findIndex(function (x) {
      return x.id === p.id;
    });
    if (idx >= 0) state.storage.presets[idx] = p;

    var map = {
      'etq-preset-nome': p.nome,
      'etq-preset-estilo': p.estilo || 'termica',
      'etq-preset-largura': p.largura_mm,
      'etq-preset-altura': p.altura_mm,
      'etq-preset-nome-pt': p.nome_pt,
      'etq-preset-nome-pt-1': p.nome_pt_1 != null ? p.nome_pt_1 : p.nome_pt || 11,
      'etq-preset-nome-pt-2': p.nome_pt_2 != null ? p.nome_pt_2 : 9,
      'etq-preset-nome-pt-3': p.nome_pt_3 != null ? p.nome_pt_3 : 7.5,
      'etq-preset-preco-pt': p.preco_pt,
      'etq-preset-rs-pt': p.rs_pt != null ? p.rs_pt : 11,
      'etq-preset-peso-pt': p.peso_pt != null ? p.peso_pt : 7,
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
    var cores = p.cores || {};
    var corMap = {
      'etq-cor-faixa-bg': cores.faixa_bg || '#1a4d2e',
      'etq-cor-faixa-fg': cores.faixa_fg || '#ffffff',
      'etq-cor-fundo': cores.fundo || '#ffffff',
      'etq-cor-preco': cores.preco_fg || '#1a4d2e',
      'etq-cor-rs': cores.rs_fg || cores.preco_fg || '#1a4d2e',
      'etq-cor-peso': cores.peso_fg || '#1a4d2e',
      'etq-cor-borda': cores.borda || '#1a4d2e',
      'etq-cor-corte': cores.marca_corte || '#94a3b8',
    };
    Object.keys(corMap).forEach(function (k) {
      var el = $(k);
      if (el) el.value = corMap[k];
    });
    var showLogo = $('etq-preset-show-logo');
    if (showLogo) showLogo.checked = p.show_logo !== false;

    togglePresetFields(p.estilo || 'termica');
    syncLayoutStageSize(p);
    applyLayoutBoxes(p.layout || Core.DEFAULT_GONDOLA_LAYOUT);

    var tr = $('etq-texto-rodape-global');
    if (tr && !tr.dataset.touched) {
      tr.value = state.storage.texto_rodape_global || p.texto_rodape || '';
    }
    carregarImpressoras(p.impressora || '');
  }

  function lerPresetForm() {
    var p = Core.normalizarPreset(getPresetAtivo());
    p.nome = ($('etq-preset-nome') && $('etq-preset-nome').value.trim()) || p.nome;
    p.estilo = ($('etq-preset-estilo') && $('etq-preset-estilo').value) || p.estilo || 'termica';
    p.largura_mm = Number($('etq-preset-largura') && $('etq-preset-largura').value) || 40;
    p.altura_mm = Number($('etq-preset-altura') && $('etq-preset-altura').value) || 40;
    p.nome_pt = Number($('etq-preset-nome-pt') && $('etq-preset-nome-pt').value) || 8;
    p.nome_pt_1 = Number($('etq-preset-nome-pt-1') && $('etq-preset-nome-pt-1').value) || 11;
    p.nome_pt_2 = Number($('etq-preset-nome-pt-2') && $('etq-preset-nome-pt-2').value) || 9;
    p.nome_pt_3 = Number($('etq-preset-nome-pt-3') && $('etq-preset-nome-pt-3').value) || 7.5;
    if (($('etq-preset-estilo') && $('etq-preset-estilo').value) === 'gondola') {
      p.nome_pt = p.nome_pt_1;
    }
    p.preco_pt = Number($('etq-preset-preco-pt') && $('etq-preset-preco-pt').value) || 28;
    p.rs_pt = Number($('etq-preset-rs-pt') && $('etq-preset-rs-pt').value) || 11;
    p.peso_pt = Number($('etq-preset-peso-pt') && $('etq-preset-peso-pt').value) || 7;
    p.codigo_pt = Number($('etq-preset-codigo-pt') && $('etq-preset-codigo-pt').value) || 7;
    p.rodape_pt = Number($('etq-preset-rodape-pt') && $('etq-preset-rodape-pt').value) || 8;
    p.barcode_height = Number($('etq-preset-bar-h') && $('etq-preset-bar-h').value) || 26;
    p.barcode_width = Number($('etq-preset-bar-w') && $('etq-preset-bar-w').value) || 1.05;
    p.texto_rodape = ($('etq-preset-texto-rodape') && $('etq-preset-texto-rodape').value) || '';
    p.impressora = ($('etq-preset-impressora') && $('etq-preset-impressora').value.trim()) || '';
    p.show_logo = !($('etq-preset-show-logo') && !$('etq-preset-show-logo').checked);
    if (Core.ehGondola(p)) {
      p.cores = {
        faixa_bg: ($('etq-cor-faixa-bg') && $('etq-cor-faixa-bg').value) || '#1a4d2e',
        faixa_fg: ($('etq-cor-faixa-fg') && $('etq-cor-faixa-fg').value) || '#ffffff',
        fundo: ($('etq-cor-fundo') && $('etq-cor-fundo').value) || '#ffffff',
        preco_fg: ($('etq-cor-preco') && $('etq-cor-preco').value) || '#1a4d2e',
        rs_fg: ($('etq-cor-rs') && $('etq-cor-rs').value) || '#1a4d2e',
        peso_fg: ($('etq-cor-peso') && $('etq-cor-peso').value) || '#1a4d2e',
        borda: ($('etq-cor-borda') && $('etq-cor-borda').value) || '#1a4d2e',
        marca_corte: ($('etq-cor-corte') && $('etq-cor-corte').value) || '#94a3b8',
      };
      p.layout = readLayoutBoxes();
      p.folha = 'a4';
      p.cols_folha = 2;
      p.rows_folha = 9;
    }
    return p;
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
        '<tr><td colspan="6" class="px-3 py-4 text-center text-sm text-slate-500">Nenhum produto na fila.</td></tr>';
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
          '<td class="px-3 py-1.5 text-xs font-semibold text-slate-300">' +
          Core.esc(it.peso_etiqueta || '—') +
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

  function limparBuscaVisual() {
    state.buscaProdutos = [];
    state.buscaSelIdx = -1;
    state.buscaQuery = '';
    var box = $('etq-busca-resultados');
    if (box) {
      box.innerHTML = '<p class="px-3 py-3 text-sm text-slate-500">Digite ao menos 2 caracteres.</p>';
    }
  }

  function adicionarProdutoFila(prod, opts) {
    opts = opts || {};
    if (!prod) return false;
    var qtdInp = $('etq-add-qtd');
    var qtd = parseInt(qtdInp && qtdInp.value, 10) || 1;
    var it = produtoParaFilaItem(prod);
    it.qtd = qtd > 0 ? qtd : 1;
    state.fila.push(it);
    renderFila();
    if (opts.limparBusca !== false) {
      limparBuscaVisual();
      var inp = $('etq-busca-input');
      if (inp) {
        inp.value = '';
        inp.focus();
      }
    }
    return true;
  }

  function highlightBuscaSelecao() {
    var box = $('etq-busca-resultados');
    if (!box) return;
    var items = box.querySelectorAll('.etq-busca-item');
    items.forEach(function (btn, i) {
      var sel = i === state.buscaSelIdx;
      btn.classList.toggle('etq-busca-item--sel', sel);
      btn.setAttribute('aria-selected', sel ? 'true' : 'false');
    });
    if (state.buscaSelIdx >= 0 && items[state.buscaSelIdx]) {
      items[state.buscaSelIdx].scrollIntoView({ block: 'nearest', inline: 'nearest' });
    }
  }

  function moverSelecaoBusca(delta) {
    var n = state.buscaProdutos.length;
    if (!n) return;
    if (state.buscaSelIdx < 0) {
      state.buscaSelIdx = delta > 0 ? 0 : n - 1;
    } else {
      state.buscaSelIdx = Math.max(0, Math.min(n - 1, state.buscaSelIdx + delta));
    }
    highlightBuscaSelecao();
  }

  function tentarAdicionarBuscaEnter() {
    var prods = state.buscaProdutos;
    if (prods.length === 1) {
      adicionarProdutoFila(prods[0]);
      return true;
    }
    if (prods.length > 1) {
      if (state.buscaSelIdx >= 0 && prods[state.buscaSelIdx]) {
        adicionarProdutoFila(prods[state.buscaSelIdx]);
        return true;
      }
      setStatus('Vários resultados — use ↑ ↓ ou clique no produto.', true);
      return false;
    }
    return false;
  }

  function renderBusca(produtos) {
    var box = $('etq-busca-resultados');
    if (!box) return;
    state.buscaProdutos = (produtos || []).slice(0, 24);
    state.buscaSelIdx = -1;
    if (!state.buscaProdutos.length) {
      box.innerHTML = '<p class="px-3 py-3 text-sm text-slate-500">Nenhum produto.</p>';
      return;
    }
    box.innerHTML = state.buscaProdutos
      .map(function (p, idx) {
        var gm = String(p.codigo_nfe || p.codigo_gm || p.codigo || '').trim();
        return (
          '<button type="button" role="option" aria-selected="false" class="etq-busca-item flex w-full items-center justify-between gap-2 border-b border-slate-700/70 px-3 py-2 text-left hover:bg-slate-700/40" data-prod-id="' +
          Core.esc(p.id) +
          '" data-busca-idx="' +
          idx +
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
        var prod = state.buscaProdutos.find(function (x) {
          return String(x.id) === String(id);
        });
        if (!prod) return;
        adicionarProdutoFila(prod);
      });
    });
  }

  function buscarProdutos(q, opts) {
    opts = opts || {};
    var box = $('etq-busca-resultados');
    q = String(q || '').trim();
    if (!q || q.length < 2) {
      limparBuscaVisual();
      return Promise.resolve([]);
    }
    if (box) box.innerHTML = '<p class="px-3 py-3 text-sm text-slate-400">…</p>';
    return fetch(URL_BUSCAR + '?q=' + encodeURIComponent(q) + '&limit=24', { credentials: 'same-origin' })
      .then(function (r) {
        return r.json();
      })
      .then(function (data) {
        if (!data || data.ok === false) {
          state.buscaProdutos = [];
          state.buscaSelIdx = -1;
          if (box) box.innerHTML = '<p class="px-3 py-3 text-sm text-red-400">Erro na busca.</p>';
          if (opts.onDone) opts.onDone([]);
          return [];
        }
        var prods = (data && data.produtos) || [];
        state.buscaQuery = q;
        renderBusca(prods);
        if (opts.onDone) opts.onDone(state.buscaProdutos);
        return state.buscaProdutos;
      })
      .catch(function () {
        state.buscaProdutos = [];
        state.buscaSelIdx = -1;
        if (box) box.innerHTML = '<p class="px-3 py-3 text-sm text-red-400">Erro na busca.</p>';
        if (opts.onDone) opts.onDone([]);
        return [];
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
    Core.imprimirItens(state.fila, {
      presetId: presetId,
      textoRodape: textoRodape,
      origem: 'fila',
    }).then(function (res) {
      if (res && res.ok) {
        setStatus('Enviado.');
        if ($('etq-hist-back') && !$('etq-hist-back').classList.contains('hidden')) {
          carregarHistorico();
        }
      } else if (res && res.reason) setStatus('Falha: ' + res.reason, true);
    });
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

  function ensureHistModalOnBody() {
    var m = $('etq-hist-back');
    if (m && m.parentElement !== document.body) document.body.appendChild(m);
  }

  function fecharModalHistorico() {
    var m = $('etq-hist-back');
    if (!m) return;
    m.classList.add('hidden');
    m.setAttribute('aria-hidden', 'true');
    document.body.classList.remove('modal-open');
  }

  function renderHistoricoLista(rows, meta) {
    var box = $('etq-hist-lista');
    var metaEl = $('etq-hist-meta');
    if (metaEl && meta) {
      metaEl.textContent =
        'Últimos ' +
        (meta.dias || HISTORICO_DIAS) +
        ' dias · ' +
        (meta.total != null ? meta.total : rows.length) +
        ' impressão(ões)';
    }
    if (!box) return;
    if (!rows || !rows.length) {
      box.innerHTML = '<p class="px-4 py-6 text-center text-sm text-slate-400">Nenhuma impressão no período.</p>';
      return;
    }
    box.innerHTML = rows
      .map(function (h) {
        return (
          '<div class="flex flex-wrap items-center gap-2 border-b border-slate-700/80 px-3 py-2.5 hover:bg-slate-800/50">' +
          '<div class="min-w-0 flex-1">' +
          '<div class="truncate text-sm font-bold text-white">' +
          Core.esc(h.resumo_nomes || '—') +
          '</div>' +
          '<div class="mt-0.5 text-[11px] text-slate-400">' +
          Core.esc(h.criado_em_br || '') +
          (h.usuario ? ' · ' + Core.esc(h.usuario) : '') +
          ' · ' +
          Core.esc(h.preset_nome || 'Preset') +
          ' · ' +
          Core.esc(String(h.total_etiquetas || 0)) +
          ' etiqueta(s)' +
          '</div>' +
          '</div>' +
          '<button type="button" class="etq-hist-reimp shrink-0 inline-flex min-h-[40px] items-center justify-center gap-1.5 rounded-xl border-2 border-emerald-600 bg-emerald-700/40 px-3 text-xs font-black uppercase text-emerald-200 hover:bg-emerald-600/30" data-hist-id="' +
          Core.esc(h.id) +
          '">' +
          '<i class="fas fa-print" aria-hidden="true"></i>Reimprimir</button>' +
          '</div>'
        );
      })
      .join('');

    box.querySelectorAll('.etq-hist-reimp').forEach(function (btn) {
      btn.addEventListener('click', function () {
        var id = btn.getAttribute('data-hist-id');
        if (id) reimprimirHistorico(id, btn);
      });
    });
  }

  function carregarHistorico() {
    var box = $('etq-hist-lista');
    if (box) box.innerHTML = '<p class="px-4 py-6 text-center text-sm text-slate-400">Carregando…</p>';
    fetch(URL_HISTORICO + '?dias=' + HISTORICO_DIAS + '&limit=300', { credentials: 'same-origin' })
      .then(function (r) {
        return r.json();
      })
      .then(function (data) {
        if (!data || !data.ok) {
          if (box) box.innerHTML = '<p class="px-4 py-6 text-center text-sm text-red-400">Erro ao carregar histórico.</p>';
          return;
        }
        renderHistoricoLista(data.historico || [], data);
      })
      .catch(function () {
        if (box) box.innerHTML = '<p class="px-4 py-6 text-center text-sm text-red-400">Erro ao carregar histórico.</p>';
      });
  }

  function reimprimirHistorico(id, btn) {
    if (btn) {
      btn.disabled = true;
      btn.textContent = '…';
    }
    fetch(URL_HISTORICO + encodeURIComponent(id) + '/', { credentials: 'same-origin' })
      .then(function (r) {
        return r.json();
      })
      .then(function (data) {
        if (!data || !data.ok || !data.job) {
          setStatus('Não foi possível reimprimir.', true);
          return;
        }
        var job = data.job;
        var itens = (job.itens || []).map(function (it) {
          return {
            id: it.id,
            nome: it.nome,
            codigo_gm: it.codigo_gm,
            codigo_barras: it.codigo_barras,
            preco_venda: it.preco_venda,
            qtd: it.qtd,
          };
        });
        if (!itens.length) {
          setStatus('Job sem itens.', true);
          return;
        }
        return Core.imprimirItens(itens, {
          presetId: job.preset_id || state.storage.preset_ativo,
          textoRodape: job.texto_rodape || '',
          origem: 'historico',
        }).then(function (res) {
          if (res && res.ok) {
            setStatus('Reimpresso.');
            carregarHistorico();
          } else {
            setStatus('Falha na reimpressão.', true);
          }
        });
      })
      .catch(function () {
        setStatus('Erro na reimpressão.', true);
      })
      .finally(function () {
        if (btn) {
          btn.disabled = false;
          btn.innerHTML = '<i class="fas fa-print" aria-hidden="true"></i>Reimprimir';
        }
      });
  }

  function abrirModalHistorico() {
    ensureHistModalOnBody();
    var m = $('etq-hist-back');
    if (!m) return;
    m.classList.remove('hidden');
    m.setAttribute('aria-hidden', 'false');
    document.body.classList.add('modal-open');
    carregarHistorico();
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
        var q = inpBusca.value.trim();
        if (ev.key === 'ArrowDown') {
          ev.preventDefault();
          if (state.buscaProdutos.length) moverSelecaoBusca(1);
          return;
        }
        if (ev.key === 'ArrowUp') {
          ev.preventDefault();
          if (state.buscaProdutos.length) moverSelecaoBusca(-1);
          return;
        }
        if (ev.key === 'Enter') {
          ev.preventDefault();
          clearTimeout(state.buscaTimer);
          if (q.length < 2) return;
          if (state.buscaQuery === q && state.buscaProdutos.length) {
            tentarAdicionarBuscaEnter();
            return;
          }
          buscarProdutos(q, {
            onDone: function (prods) {
              if (prods.length === 1) {
                adicionarProdutoFila(prods[0]);
              } else if (prods.length > 1) {
                setStatus('Vários resultados — use ↑ ↓ ou clique no produto.', true);
              }
            },
          });
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
    $('etq-btn-historico') && $('etq-btn-historico').addEventListener('click', abrirModalHistorico);
    $('etq-modal-fechar') && $('etq-modal-fechar').addEventListener('click', fecharModalPreset);
    $('etq-hist-fechar') && $('etq-hist-fechar').addEventListener('click', fecharModalHistorico);
    $('etq-btn-reset-layout') &&
      $('etq-btn-reset-layout').addEventListener('click', function () {
        var p = getPresetAtivo();
        if (Core.ehGondola(p)) {
          p.layout = Core.clonePreset(Core.DEFAULT_GONDOLA_LAYOUT);
          p.largura_mm = 90;
          p.altura_mm = 30;
          p.folha = 'a4';
          p.cols_folha = 2;
          p.rows_folha = 9;
          var idx = state.storage.presets.findIndex(function (x) {
            return x.id === p.id;
          });
          if (idx >= 0) state.storage.presets[idx] = p;
          persistStorage();
          renderPresetForm();
        } else {
          applyLayoutBoxes(Core.clonePreset(Core.DEFAULT_GONDOLA_LAYOUT));
          commitPresetFormLive();
        }
        setStatus('Layout A4 9×3 resetado.');
      });

    bindLayoutEditor();

    var estiloSel = $('etq-preset-estilo');
    if (estiloSel) {
      estiloSel.addEventListener('change', function () {
        commitPresetFormLive();
        renderPresetForm();
      });
    }
    [
      'etq-preset-largura',
      'etq-preset-altura',
      'etq-preset-nome-pt',
      'etq-preset-nome-pt-1',
      'etq-preset-nome-pt-2',
      'etq-preset-nome-pt-3',
      'etq-preset-preco-pt',
      'etq-preset-rs-pt',
      'etq-preset-peso-pt',
      'etq-cor-faixa-bg',
      'etq-cor-faixa-fg',
      'etq-cor-fundo',
      'etq-cor-preco',
      'etq-cor-rs',
      'etq-cor-peso',
      'etq-cor-borda',
      'etq-cor-corte',
      'etq-preset-show-logo',
    ].forEach(function (id) {
      var el = $(id);
      if (!el) return;
      el.addEventListener('change', commitPresetFormLive);
      el.addEventListener('input', function () {
        if (id.indexOf('etq-cor-') === 0 || id === 'etq-preset-largura' || id === 'etq-preset-altura') {
          commitPresetFormLive();
        }
      });
    });

    $('etq-modal-back') &&
      $('etq-modal-back').addEventListener('click', function (ev) {
        if (ev.target === $('etq-modal-back')) fecharModalPreset();
      });
    $('etq-hist-back') &&
      $('etq-hist-back').addEventListener('click', function (ev) {
        if (ev.target === $('etq-hist-back')) fecharModalHistorico();
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
      if (ev.key === 'Escape') {
        fecharModalHistorico();
        fecharModalPreset();
      }
    });
  }

  function init() {
    ensureModalOnBody();
    ensureHistModalOnBody();
    reloadStorage();
    /* Garante seed Gôndola no PC (quem já tinha só 4×4). */
    persistStorage();
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
