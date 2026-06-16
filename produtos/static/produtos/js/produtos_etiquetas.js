(function () {
  'use strict';

  var LS_KEY = 'agro_etiquetas_presets_v1';
  var URL_BUSCAR = '/api/buscar/';

  var DEFAULT_PRESET = {
    id: 'padrao-4x4',
    nome: '4×4 padrão',
    largura_mm: 40,
    altura_mm: 40,
    nome_pt: 8,
    preco_pt: 28,
    codigo_pt: 7,
    rodape_pt: 8,
    barcode_height: 26,
    barcode_width: 1.05,
    texto_rodape: 'Gm Agro Mais',
    impressora: '',
  };

  var state = {
    fila: [],
    presets: [],
    presetAtivoId: DEFAULT_PRESET.id,
    buscaTimer: null,
    textoRodapeGlobal: DEFAULT_PRESET.texto_rodape,
  };

  function $(id) {
    return document.getElementById(id);
  }

  function esc(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function fmtPreco(v) {
    var n = Number(v);
    if (!isFinite(n)) return '0,00';
    return n.toFixed(2).replace('.', ',');
  }

  function ean13Digito(d12) {
    var d = String(d12 || '').replace(/\D/g, '');
    if (d.length !== 12) return null;
    var soma = 0;
    for (var i = 0; i < 12; i++) {
      var dig = parseInt(d.charAt(i), 10);
      soma += i % 2 === 0 ? dig : dig * 3;
    }
    return (10 - (soma % 10)) % 10;
  }

  function valorBarcodeProduto(p) {
    var cb = String(p.codigo_barras || '').replace(/\D/g, '');
    if (cb.length === 13) return { valor: cb, formato: 'EAN13' };
    if (cb.length === 12) {
      var dv = ean13Digito(cb);
      if (dv != null) return { valor: cb + String(dv), formato: 'EAN13' };
    }
    var gm = String(p.codigo_gm || p.codigo_nfe || '').trim();
    if (gm) return { valor: gm, formato: 'CODE128' };
    if (cb) return { valor: cb, formato: 'CODE128' };
    return { valor: String(p.id || '0'), formato: 'CODE128' };
  }

  function loadPresets() {
    try {
      var raw = localStorage.getItem(LS_KEY);
      if (!raw) {
        state.presets = [clonePreset(DEFAULT_PRESET)];
        state.presetAtivoId = DEFAULT_PRESET.id;
        return;
      }
      var data = JSON.parse(raw);
      state.presets = Array.isArray(data.presets) && data.presets.length ? data.presets : [clonePreset(DEFAULT_PRESET)];
      state.presetAtivoId = data.preset_ativo || state.presets[0].id;
      state.textoRodapeGlobal = data.texto_rodape_global || getPresetAtivo().texto_rodape || '';
    } catch (e) {
      state.presets = [clonePreset(DEFAULT_PRESET)];
      state.presetAtivoId = DEFAULT_PRESET.id;
    }
  }

  function savePresets() {
    localStorage.setItem(
      LS_KEY,
      JSON.stringify({
        presets: state.presets,
        preset_ativo: state.presetAtivoId,
        texto_rodape_global: state.textoRodapeGlobal,
      })
    );
  }

  function clonePreset(p) {
    return JSON.parse(JSON.stringify(p));
  }

  function getPresetAtivo() {
    var p = state.presets.find(function (x) {
      return x.id === state.presetAtivoId;
    });
    return p || state.presets[0] || clonePreset(DEFAULT_PRESET);
  }

  function uid() {
    return 'etq-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 8);
  }

  function produtoParaFilaItem(prod) {
    return {
      fila_id: uid(),
      id: String(prod.id || ''),
      nome: String(prod.nome || '').trim(),
      codigo_gm: String(prod.codigo_nfe || prod.codigo_gm || prod.codigo || '').trim(),
      codigo_barras: String(prod.codigo_barras || '').trim(),
      preco_venda: Number(prod.preco_venda) || 0,
      qtd: 1,
    };
  }

  function renderPresetSelect() {
    var sel = $('etq-preset-select');
    if (!sel) return;
    sel.innerHTML = state.presets
      .map(function (p) {
        return (
          '<option value="' +
          esc(p.id) +
          '"' +
          (p.id === state.presetAtivoId ? ' selected' : '') +
          '>' +
          esc(p.nome) +
          '</option>'
        );
      })
      .join('');
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
      'etq-preset-impressora': p.impressora || '',
    };
    Object.keys(map).forEach(function (k) {
      var el = $(k);
      if (el) el.value = map[k];
    });
    var tr = $('etq-texto-rodape-global');
    if (tr && !tr.dataset.touched) tr.value = state.textoRodapeGlobal || p.texto_rodape || '';
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
        '<tr><td colspan="5" class="px-3 py-6 text-center text-sm text-slate-500">Nenhum produto na fila.</td></tr>';
      return;
    }

    tbody.innerHTML = state.fila
      .map(function (it, idx) {
        return (
          '<tr class="border-t border-slate-700/80">' +
          '<td class="px-3 py-2 text-sm font-semibold text-white">' +
          esc(it.nome || '—') +
          '</td>' +
          '<td class="px-3 py-2 text-xs font-mono text-slate-300">' +
          esc(it.codigo_gm || '—') +
          '</td>' +
          '<td class="px-3 py-2 text-sm font-bold text-emerald-400">' +
          esc(fmtPreco(it.preco_venda)) +
          '</td>' +
          '<td class="px-3 py-2">' +
          '<input type="number" min="1" max="999" value="' +
          esc(it.qtd) +
          '" data-fila-qtd="' +
          esc(it.fila_id) +
          '" class="etq-qtd-input w-16 min-h-[40px] rounded-lg border border-slate-600 bg-slate-900 px-2 text-center text-sm font-bold text-white" />' +
          '</td>' +
          '<td class="px-3 py-2 text-right">' +
          '<button type="button" data-fila-del="' +
          esc(it.fila_id) +
          '" class="rounded-lg border border-red-700/60 bg-red-950/40 px-2 py-1 text-xs font-bold uppercase text-red-300 hover:bg-red-900/50">Remover</button>' +
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
      box.innerHTML = '<p class="px-3 py-4 text-sm text-slate-500">Nenhum produto encontrado.</p>';
      return;
    }
    box.innerHTML = produtos
      .slice(0, 24)
      .map(function (p) {
        var gm = String(p.codigo_nfe || p.codigo || '').trim();
        return (
          '<button type="button" class="etq-busca-item flex w-full items-center justify-between gap-3 border-b border-slate-700/70 px-3 py-3 text-left hover:bg-slate-700/40" data-prod-id="' +
          esc(p.id) +
          '">' +
          '<span class="min-w-0 flex-1">' +
          '<span class="block truncate text-sm font-bold text-white">' +
          esc(p.nome || '—') +
          '</span>' +
          '<span class="block truncate text-xs text-slate-400">' +
          esc(gm) +
          ' · R$ ' +
          esc(fmtPreco(p.preco_venda)) +
          '</span>' +
          '</span>' +
          '<span class="shrink-0 rounded-lg bg-emerald-600 px-2 py-1 text-[10px] font-black uppercase text-white">+ Fila</span>' +
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
        state.fila.push(produtoParaFilaItem(prod));
        renderFila();
      });
    });
  }

  function buscarProdutos(q) {
    var box = $('etq-busca-resultados');
    if (!q || q.length < 2) {
      if (box) box.innerHTML = '<p class="px-3 py-4 text-sm text-slate-500">Digite ao menos 2 caracteres.</p>';
      return;
    }
    if (box) box.innerHTML = '<p class="px-3 py-4 text-sm text-slate-400">Buscando…</p>';
    fetch(URL_BUSCAR + '?q=' + encodeURIComponent(q), { credentials: 'same-origin' })
      .then(function (r) {
        return r.json();
      })
      .then(function (data) {
        renderBusca(data.produtos || []);
      })
      .catch(function () {
        if (box) box.innerHTML = '<p class="px-3 py-4 text-sm text-red-400">Erro na busca.</p>';
      });
  }

  function montarHtmlImpressao(preset, itens, textoRodape) {
    var w = Number(preset.largura_mm) || 40;
    var h = Number(preset.altura_mm) || 40;
    var labels = [];
    itens.forEach(function (it, idxIt) {
      var qtd = Math.max(1, parseInt(it.qtd, 10) || 1);
      var bc = valorBarcodeProduto(it);
      var gm = String(it.codigo_gm || '').trim();
      for (var i = 0; i < qtd; i++) {
        labels.push({
          nome: String(it.nome || ''),
          preco: fmtPreco(it.preco_venda),
          gm: gm,
          bcValor: bc.valor,
          bcFormato: bc.formato,
          bcId: 'bc-' + idxIt + '-' + i,
        });
      }
    });

    var css =
      '@page{size:' +
      w +
      'mm ' +
      h +
      'mm;margin:0}' +
      'html,body{margin:0;padding:0}' +
      'body{font-family:Arial,Helvetica,sans-serif;-webkit-print-color-adjust:exact;print-color-adjust:exact}' +
      '.etq{width:' +
      w +
      'mm;height:' +
      h +
      'mm;box-sizing:border-box;display:flex;flex-direction:column;align-items:center;justify-content:center;overflow:hidden;page-break-after:always;break-after:page;padding:1.2mm 1mm;text-align:center}' +
      '.etq:last-child{page-break-after:auto;break-after:auto}' +
      '.nome{width:100%;font-size:' +
      preset.nome_pt +
      'pt;line-height:1.1;font-weight:600;margin-bottom:0.8mm;overflow:hidden;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical}' +
      '.preco{font-size:' +
      preset.preco_pt +
      'pt;font-weight:900;line-height:1;margin:0.6mm 0}' +
      '.barcode-wrap{width:92%;display:flex;justify-content:center;margin:0.4mm 0}' +
      '.barcode-wrap svg{max-width:100%;height:auto}' +
      '.codigo-gm{font-size:' +
      preset.codigo_pt +
      'pt;font-weight:700;line-height:1;margin-top:0.3mm}' +
      '.rodape{font-size:' +
      preset.rodape_pt +
      'pt;font-weight:800;line-height:1.1;margin-top:0.8mm;max-width:100%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}';

    var body = labels
      .map(function (lb) {
        return (
          '<div class="etq">' +
          '<div class="nome">' +
          esc(lb.nome) +
          '</div>' +
          '<div class="preco">' +
          esc(lb.preco) +
          '</div>' +
          '<div class="barcode-wrap"><svg id="' +
          esc(lb.bcId) +
          '"></svg></div>' +
          '<div class="codigo-gm">' +
          esc(lb.gm) +
          '</div>' +
          '<div class="rodape">' +
          esc(textoRodape) +
          '</div>' +
          '</div>'
        );
      })
      .join('');

    var jsData = JSON.stringify(
      labels.map(function (lb) {
        return { id: lb.bcId, valor: lb.bcValor, formato: lb.bcFormato };
      })
    );

    return (
      '<!DOCTYPE html><html><head><meta charset="utf-8"><style>' +
      css +
      '</style></head><body>' +
      body +
      '<script src="https://cdn.jsdelivr.net/npm/jsbarcode@3.11.5/dist/JsBarcode.all.min.js"><\/script>' +
      '<script>var _bars=' +
      jsData +
      ';function _draw(){try{_bars.forEach(function(b){var el=document.getElementById(b.id);if(!el)return;JsBarcode(el,b.valor,{format:b.formato,width:' +
      preset.barcode_width +
      ',height:' +
      preset.barcode_height +
      ',displayValue:false,margin:0});});}catch(e){}}if(document.readyState==="loading"){document.addEventListener("DOMContentLoaded",function(){setTimeout(_draw,30);});}else{setTimeout(_draw,30);}<\/script>' +
      '</body></html>'
    );
  }

  function setStatus(msg, isErr) {
    var el = $('etq-status');
    if (!el) return;
    el.textContent = msg || '';
    el.className =
      'text-sm font-semibold ' + (isErr ? 'text-red-400' : msg ? 'text-emerald-400' : 'text-slate-500');
  }

  function imprimirFila() {
    if (!state.fila.length) {
      setStatus('Adicione produtos à fila antes de imprimir.', true);
      return;
    }
    var preset = getPresetAtivo();
    var textoRodape =
      ($('etq-texto-rodape-global') && $('etq-texto-rodape-global').value.trim()) ||
      state.textoRodapeGlobal ||
      preset.texto_rodape ||
      '';
    var html = montarHtmlImpressao(preset, state.fila, textoRodape);
    setStatus('Enviando para impressão…');

    if (window.agroShell && typeof window.agroShell.silentPrint === 'function') {
      window.agroShell
        .silentPrint({
          html: html,
          deviceName: preset.impressora || '',
          waitMs: 500,
        })
        .then(function (res) {
          if (res && res.ok) {
            setStatus('Impressão enviada (silenciosa).');
          } else {
            setStatus('Falha na impressão silenciosa: ' + (res && res.reason ? res.reason : 'erro'), true);
          }
        })
        .catch(function (e) {
          setStatus('Erro Electron: ' + String(e && e.message), true);
        });
      return;
    }

    var iframe = document.getElementById('etq-print-iframe');
    if (!iframe) {
      iframe = document.createElement('iframe');
      iframe.id = 'etq-print-iframe';
      iframe.title = 'Impressão etiquetas';
      iframe.setAttribute('aria-hidden', 'true');
      iframe.style.cssText =
        'position:fixed;right:0;bottom:0;width:0;height:0;border:0;opacity:0;pointer-events:none;';
      document.body.appendChild(iframe);
    }
    var idoc = iframe.contentDocument || iframe.contentWindow.document;
    idoc.open();
    idoc.write(html);
    idoc.close();
    setTimeout(function () {
      try {
        iframe.contentWindow.focus();
        iframe.contentWindow.print();
        setStatus('Diálogo de impressão aberto (use Electron para impressão direta).');
      } catch (e) {
        setStatus('Erro ao imprimir: ' + String(e && e.message), true);
      }
    }, 450);
  }

  function salvarPresetAtual() {
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
    savePresets();
    renderPresetSelect();
    setStatus('Preset salvo.');
  }

  function criarNovoPreset() {
    var nome = prompt('Nome do novo preset:', 'Novo modelo');
    if (!nome) return;
    var np = clonePreset(getPresetAtivo());
    np.id = 'preset-' + Date.now().toString(36);
    np.nome = nome.trim() || 'Novo modelo';
    state.presets.push(np);
    state.presetAtivoId = np.id;
    savePresets();
    renderPresetSelect();
    renderPresetForm();
    setStatus('Preset criado.');
  }

  function excluirPresetAtual() {
    if (state.presets.length <= 1) {
      alert('É necessário manter ao menos um preset.');
      return;
    }
    var p = getPresetAtivo();
    if (!confirm('Excluir preset «' + p.nome + '»?')) return;
    state.presets = state.presets.filter(function (x) {
      return x.id !== p.id;
    });
    state.presetAtivoId = state.presets[0].id;
    savePresets();
    renderPresetSelect();
    renderPresetForm();
    setStatus('Preset excluído.');
  }

  function carregarImpressoras() {
    if (!(window.agroShell && typeof window.agroShell.listPrinters === 'function')) return;
    window.agroShell.listPrinters().then(function (res) {
      var sel = $('etq-preset-impressora');
      if (!sel || !res || !res.ok) return;
      var atual = sel.value;
      var opts = ['<option value="">(Padrão do Windows)</option>'];
      (res.printers || []).forEach(function (p) {
        opts.push(
          '<option value="' +
            esc(p.name) +
            '"' +
            (p.isDefault && !atual ? ' selected' : atual === p.name ? ' selected' : '') +
            '>' +
            esc(p.name) +
            (p.isDefault ? ' (padrão)' : '') +
            '</option>'
        );
      });
      sel.innerHTML = opts.join('');
      if (atual) sel.value = atual;
    });
  }

  function bindEvents() {
    var inpBusca = $('etq-busca-input');
    if (inpBusca) {
      inpBusca.addEventListener('input', function () {
        clearTimeout(state.buscaTimer);
        var q = inpBusca.value.trim();
        state.buscaTimer = setTimeout(function () {
          buscarProdutos(q);
        }, 280);
      });
      inpBusca.addEventListener('keydown', function (ev) {
        if (ev.key === 'Enter') {
          ev.preventDefault();
          buscarProdutos(inpBusca.value.trim());
        }
      });
    }

    var btnAddQtd = $('etq-btn-add-qtd');
    if (btnAddQtd) {
      btnAddQtd.addEventListener('click', function () {
        var qtdInp = $('etq-add-qtd');
        var qtd = parseInt(qtdInp && qtdInp.value, 10) || 1;
        var box = $('etq-busca-resultados');
        var first = box && box.querySelector('.etq-busca-item');
        if (first) {
          first.click();
          var last = state.fila[state.fila.length - 1];
          if (last) last.qtd = qtd > 0 ? qtd : 1;
          renderFila();
        }
      });
    }

    $('etq-btn-imprimir') &&
      $('etq-btn-imprimir').addEventListener('click', function () {
        imprimirFila();
      });
    $('etq-btn-limpar') &&
      $('etq-btn-limpar').addEventListener('click', function () {
        if (state.fila.length && !confirm('Limpar toda a fila?')) return;
        state.fila = [];
        renderFila();
        setStatus('');
      });
    $('etq-btn-salvar-preset') &&
      $('etq-btn-salvar-preset').addEventListener('click', salvarPresetAtual);
    $('etq-btn-novo-preset') &&
      $('etq-btn-novo-preset').addEventListener('click', criarNovoPreset);
    $('etq-btn-excluir-preset') &&
      $('etq-btn-excluir-preset').addEventListener('click', excluirPresetAtual);

    var selPreset = $('etq-preset-select');
    if (selPreset) {
      selPreset.addEventListener('change', function () {
        state.presetAtivoId = selPreset.value;
        savePresets();
        renderPresetForm();
        setStatus('');
      });
    }

    var tr = $('etq-texto-rodape-global');
    if (tr) {
      tr.addEventListener('input', function () {
        tr.dataset.touched = '1';
        state.textoRodapeGlobal = tr.value;
        savePresets();
      });
    }
  }

  function init() {
    loadPresets();
    renderPresetSelect();
    renderPresetForm();
    renderFila();
    bindEvents();
    carregarImpressoras();
    var shellHint = $('etq-shell-hint');
    if (shellHint) {
      shellHint.textContent =
        window.agroShell && typeof window.agroShell.silentPrint === 'function'
          ? 'Electron detectado — impressão direta na impressora configurada no preset.'
          : 'No navegador abre o diálogo do Windows. Use o app Electron para impressão direta.';
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
