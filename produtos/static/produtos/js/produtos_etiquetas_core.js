(function (global) {
  'use strict';

  var LS_KEY = 'agro_etiquetas_presets_v1';

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

  function clonePreset(p) {
    return JSON.parse(JSON.stringify(p));
  }

  function loadStorage() {
    try {
      var raw = localStorage.getItem(LS_KEY);
      if (!raw) {
        return {
          presets: [clonePreset(DEFAULT_PRESET)],
          preset_ativo: DEFAULT_PRESET.id,
          texto_rodape_global: DEFAULT_PRESET.texto_rodape,
        };
      }
      var data = JSON.parse(raw);
      var presets =
        Array.isArray(data.presets) && data.presets.length ? data.presets : [clonePreset(DEFAULT_PRESET)];
      return {
        presets: presets,
        preset_ativo: data.preset_ativo || presets[0].id,
        texto_rodape_global: data.texto_rodape_global || presets[0].texto_rodape || '',
      };
    } catch (e) {
      return {
        presets: [clonePreset(DEFAULT_PRESET)],
        preset_ativo: DEFAULT_PRESET.id,
        texto_rodape_global: DEFAULT_PRESET.texto_rodape,
      };
    }
  }

  function saveStorage(data) {
    localStorage.setItem(
      LS_KEY,
      JSON.stringify({
        presets: data.presets,
        preset_ativo: data.preset_ativo,
        texto_rodape_global: data.texto_rodape_global || '',
      })
    );
  }

  function getPresetById(presets, id) {
    return presets.find(function (x) {
      return x.id === id;
    });
  }

  function getPresetAtivo(data) {
    return (
      getPresetById(data.presets, data.preset_ativo) ||
      data.presets[0] ||
      clonePreset(DEFAULT_PRESET)
    );
  }

  function produtoParaItem(prod, qtd) {
    return {
      id: String(prod.id || ''),
      nome: String(prod.nome || '').trim(),
      codigo_gm: String(prod.codigo_gm || prod.codigo_nfe || prod.codigo || '').trim(),
      codigo_barras: String(prod.codigo_barras || '').trim(),
      preco_venda: Number(prod.preco_venda) || 0,
      qtd: qtd > 0 ? qtd : 1,
    };
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
      ',displayValue:false,margin:0});});}catch(e){}}function _go(){setTimeout(_draw,40);setTimeout(function(){document.body.dataset.agroReady="1";},520);}if(typeof JsBarcode!=="undefined"){_go();}else{window.addEventListener("load",_go);}<\/script>' +
      '</body></html>'
    );
  }

  function podeSilentPrint() {
    return !!(global.agroShell && typeof global.agroShell.silentPrint === 'function');
  }

  function imprimirItens(itens, opts) {
    opts = opts || {};
    if (!itens || !itens.length) {
      return Promise.resolve({ ok: false, reason: 'empty' });
    }
    var data = loadStorage();
    var preset = opts.preset
      ? opts.preset
      : opts.presetId
        ? getPresetById(data.presets, opts.presetId) || getPresetAtivo(data)
        : getPresetAtivo(data);
    var textoRodape =
      opts.textoRodape != null
        ? String(opts.textoRodape)
        : data.texto_rodape_global || preset.texto_rodape || '';
    var html = montarHtmlImpressao(preset, itens, textoRodape);
    var w = Number(preset.largura_mm) || 40;
    var h = Number(preset.altura_mm) || 40;

    if (podeSilentPrint()) {
      return global.agroShell.silentPrint({
        html: html,
        deviceName: preset.impressora || '',
        waitMs: 900,
        pageWidthMicrons: Math.round(w * 1000),
        pageHeightMicrons: Math.round(h * 1000),
      });
    }

    return new Promise(function (resolve) {
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
          resolve({ ok: true, silent: false });
        } catch (e) {
          resolve({ ok: false, reason: String(e && e.message) });
        }
      }, 650);
    });
  }

  global.AgroEtiquetasCore = {
    LS_KEY: LS_KEY,
    DEFAULT_PRESET: DEFAULT_PRESET,
    esc: esc,
    fmtPreco: fmtPreco,
    clonePreset: clonePreset,
    loadStorage: loadStorage,
    saveStorage: saveStorage,
    getPresetAtivo: getPresetAtivo,
    getPresetById: getPresetById,
    produtoParaItem: produtoParaItem,
    montarHtmlImpressao: montarHtmlImpressao,
    imprimirItens: imprimirItens,
    podeSilentPrint: podeSilentPrint,
  };
})(typeof window !== 'undefined' ? window : this);
