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

  function ean13ChecksumOk(d13) {
    var d = String(d13 || '').replace(/\D/g, '');
    if (d.length !== 13) return false;
    var dv = ean13Digito(d.slice(0, 12));
    return dv !== null && String(dv) === d.charAt(12);
  }

  /** Corrige DV errado ou completa 12 dígitos — retorna null se inválido. */
  function normalizarEan13(cb) {
    var d = String(cb || '').replace(/\D/g, '');
    if (d.length === 12) {
      var dv = ean13Digito(d);
      if (dv == null) return null;
      return { valor: d + String(dv), formato: 'EAN13', ean_corrigido: true };
    }
    if (d.length === 13) {
      if (ean13ChecksumOk(d)) return { valor: d, formato: 'EAN13' };
      var fix = ean13Digito(d.slice(0, 12));
      if (fix == null) return null;
      return {
        valor: d.slice(0, 12) + String(fix),
        formato: 'EAN13',
        ean_corrigido: true,
        valor_original: d,
      };
    }
    return null;
  }

  function barcodeWidthParaFormato(formato, larguraMm, presetWidth) {
    var bw = Number(presetWidth) || 1.05;
    if (formato === 'EAN13') {
      var modulos = 95;
      var availPx = (Number(larguraMm) || 40) * 3.7795275591 * 0.88;
      bw = Math.min(bw, Math.max(0.55, availPx / modulos));
    } else if (formato === 'EAN8') {
      bw = Math.min(bw, 1.1);
    }
    return bw;
  }

  function barcodeHeightParaFormato(formato, presetHeight, alturaMm) {
    var bh = Number(presetHeight) || 26;
    var hMm = Number(alturaMm) || 40;
    if (formato === 'EAN13' || formato === 'EAN8') {
      return Math.min(bh, Math.max(14, Math.round(hMm * 3.7795275591 * 0.18)));
    }
    return Math.min(bh, Math.max(12, Math.round(hMm * 3.7795275591 * 0.2)));
  }

  function candidatosCodigoBarrasNumerico(p) {
    return [
      p && p.codigo_barras,
      p && p.ean,
      p && p.EAN,
      p && p.gtin,
      p && p.GTIN,
      p && p.codigo_barras_loja,
    ];
  }

  function extrairEanNumerico(p) {
    var cands = candidatosCodigoBarrasNumerico(p);
    for (var i = 0; i < cands.length; i++) {
      var cb = String(cands[i] || '').replace(/\D/g, '');
      if (cb.length === 13 || cb.length === 12) {
        var norm = normalizarEan13(cb);
        if (norm) return norm;
      }
      if (cb.length === 8) return { valor: cb, formato: 'EAN8' };
    }
    return null;
  }

  /** Código numérico curto (4–14 dígitos, ex. 1813647) — CODE128, não EAN. */
  function extrairCodigoBarrasCurto(p) {
    var cands = candidatosCodigoBarrasNumerico(p);
    for (var i = 0; i < cands.length; i++) {
      var cb = String(cands[i] || '').replace(/\D/g, '');
      if (cb.length >= 4 && cb.length <= 14 && cb.length !== 8 && cb.length !== 12 && cb.length !== 13) {
        return { valor: cb, formato: 'CODE128' };
      }
    }
    var loose = String((p && p.codigo_barras) || '').replace(/\s/g, '');
    if (/^\d{4,14}$/.test(loose) && loose.length !== 8 && loose.length !== 12 && loose.length !== 13) {
      return { valor: loose, formato: 'CODE128' };
    }
    return null;
  }

  function valorBarcodeProduto(p) {
    var ean = extrairEanNumerico(p);
    if (ean) return ean;
    var cbCurto = extrairCodigoBarrasCurto(p);
    if (cbCurto) return cbCurto;
    /* Fallback GM só sem EAN/código numérico válido — leitor bipa texto (CODE128). */
    var gm = String(p.codigo_gm || p.codigo_nfe || p.codigo_interno || p.codigo || '').trim();
    if (gm) return { valor: gm, formato: 'CODE128', fallback_gm: true };
    var cbLoose = String(p.codigo_barras || '').replace(/\s/g, '');
    if (cbLoose) return { valor: cbLoose, formato: 'CODE128' };
    return { valor: String(p.id || '0'), formato: 'CODE128', fallback_gm: true };
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
      codigo_gm: String(
        prod.codigo_gm || prod.codigo_nfe || prod.codigo_interno || prod.codigo || ''
      ).trim(),
      codigo_barras: String(prod.codigo_barras || prod.ean || prod.gtin || '').trim(),
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
      'html,body{margin:0;padding:0;width:' +
      w +
      'mm;height:' +
      h +
      'mm;zoom:1!important;transform:none!important;overflow:hidden}' +
      'body{font-family:Arial,Helvetica,sans-serif;-webkit-print-color-adjust:exact;print-color-adjust:exact}' +
      '.etq{width:' +
      w +
      'mm;height:' +
      h +
      'mm;box-sizing:border-box;display:flex;flex-direction:column;align-items:center;justify-content:flex-start;overflow:hidden;page-break-after:always;break-after:page;padding:1mm 0.8mm;text-align:center;gap:0.25mm}' +
      '.etq:last-child{page-break-after:auto;break-after:auto}' +
      '.nome{width:100%;flex:0 0 auto;font-size:' +
      preset.nome_pt +
      'pt;line-height:1.05;font-weight:600;overflow:hidden;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical}' +
      '.preco{flex:0 0 auto;font-size:' +
      Math.min(Number(preset.preco_pt) || 28, Math.round(h * 2.2)) +
      'pt;font-weight:900;line-height:1;margin:0}' +
      '.barcode-wrap{flex:0 1 auto;width:92%;max-height:' +
      Math.max(8, Math.round(h * 0.28)) +
      'mm;display:flex;align-items:center;justify-content:center;overflow:hidden;margin:0.2mm 0}' +
      '.barcode-wrap svg{display:block;max-width:100%!important;max-height:' +
      Math.max(8, Math.round(h * 0.28)) +
      'mm!important;width:auto!important;height:auto!important}' +
      '.codigo-gm{flex:0 0 auto;font-size:' +
      preset.codigo_pt +
      'pt;font-weight:700;line-height:1}' +
      '.rodape{flex:0 0 auto;font-size:' +
      preset.rodape_pt +
      'pt;font-weight:800;line-height:1.05;margin-top:0.2mm;max-width:100%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}';

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
        return {
          id: lb.bcId,
          valor: lb.bcValor,
          formato: lb.bcFormato,
          bw: barcodeWidthParaFormato(lb.bcFormato, w, preset.barcode_width),
          bh: barcodeHeightParaFormato(lb.bcFormato, preset.barcode_height, h),
        };
      })
    );

    return (
      '<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=' +
      w +
      'mm"><style>' +
      css +
      '</style></head><body>' +
      body +
      '<script src="https://cdn.jsdelivr.net/npm/jsbarcode@3.11.5/dist/JsBarcode.all.min.js"><\/script>' +
      '<script>var _bars=' +
      jsData +
      ';function _drawOne(b){var el=document.getElementById(b.id);if(!el)return;var opts={format:b.formato,width:b.bw,height:b.bh,displayValue:false,margin:0};try{JsBarcode(el,b.valor,opts);return;}catch(e1){try{JsBarcode(el,b.valor,{format:"CODE128",width:b.bw,height:b.bh,displayValue:false,margin:0});}catch(e2){el.setAttribute("data-bc-erro","1");}}}function _draw(){try{_bars.forEach(_drawOne);}catch(e){}}function _go(){setTimeout(_draw,40);setTimeout(function(){document.body.dataset.agroReady="1";},520);}if(typeof JsBarcode!=="undefined"){_go();}else{window.addEventListener("load",_go);}<\/script>' +
      '</body></html>'
    );
  }

  function podeSilentPrint() {
    return !!(global.agroShell && typeof global.agroShell.silentPrint === 'function');
  }

  function registrarHistoricoBackend(opts, itens) {
    opts = opts || {};
    if (!itens || !itens.length) return;
    try {
      var data = loadStorage();
      var preset = opts.preset
        ? opts.preset
        : opts.presetId
          ? getPresetById(data.presets, opts.presetId) || getPresetAtivo(data)
          : getPresetAtivo(data);
      var csrf = (document.cookie.match(/csrftoken=([^;]+)/) || [])[1] || '';
      var payload = {
        origem: opts.origem || 'fila',
        preset_id: preset.id || '',
        preset_nome: preset.nome || '',
        texto_rodape:
          opts.textoRodape != null
            ? String(opts.textoRodape)
            : data.texto_rodape_global || preset.texto_rodape || '',
        itens: itens.map(function (it) {
          return {
            id: it.id,
            nome: it.nome,
            codigo_gm: it.codigo_gm,
            codigo_barras: it.codigo_barras,
            preco_venda: it.preco_venda,
            qtd: it.qtd,
          };
        }),
      };
      fetch('/api/produtos/etiquetas/historico/salvar/', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf },
        body: JSON.stringify(payload),
      }).catch(function () {});
    } catch (e) {}
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
      return global.agroShell
        .silentPrint({
          html: html,
          deviceName: preset.impressora || '',
          waitMs: 900,
          pageWidthMicrons: Math.round(w * 1000),
          pageHeightMicrons: Math.round(h * 1000),
        })
        .then(function (res) {
          if (res && res.ok) registrarHistoricoBackend(opts, itens);
          return res;
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
          registrarHistoricoBackend(opts, itens);
          resolve({ ok: true, silent: false });
        } catch (e) {
          resolve({ ok: false, reason: String(e && e.message) });
        }
      }, 650);
    });
  }

  function fillPresetSelect(selectEl, activeId) {
    if (!selectEl) return activeId || '';
    var st = loadStorage();
    var aid = activeId || st.preset_ativo || (st.presets[0] && st.presets[0].id) || '';
    selectEl.innerHTML = st.presets
      .map(function (p) {
        return (
          '<option value="' +
          esc(p.id) +
          '"' +
          (p.id === aid ? ' selected' : '') +
          '>' +
          esc(p.nome) +
          '</option>'
        );
      })
      .join('');
    selectEl.value = aid;
    return aid;
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
    fillPresetSelect: fillPresetSelect,
    valorBarcodeProduto: valorBarcodeProduto,
    extrairEanNumerico: extrairEanNumerico,
    ean13ChecksumOk: ean13ChecksumOk,
    normalizarEan13: normalizarEan13,
  };
})(typeof window !== 'undefined' ? window : this);
