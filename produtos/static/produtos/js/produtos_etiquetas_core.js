(function (global) {
  'use strict';

  var LS_KEY = 'agro_etiquetas_presets_v1';
  var LS_MIGRATE_FLAG = 'agro_etiquetas_presets_pg_v1';
  var BUILTIN_IDS = { 'padrao-4x4': 1, gondola: 1 };

  var DEFAULT_PRESET = {
    id: 'padrao-4x4',
    nome: '4×4 padrão',
    estilo: 'termica',
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

  var DEFAULT_GONDOLA_LAYOUT = {
    nome: { x: 0, y: 0, w: 100, h: 32 },
    rs: { x: 10, y: 40, w: 16, h: 36 },
    preco: { x: 26, y: 36, w: 58, h: 42 },
    logo: { x: 2, y: 74, w: 14, h: 22 },
    peso: { x: 20, y: 78, w: 78, h: 18 },
    /* Novo: GM curto — fora do caminho do preset clássico; só imprime se show_gm */
    gm: { x: 2, y: 55, w: 18, h: 16 },
  };

  var DEFAULT_GONDOLA_PRESET = {
    id: 'gondola',
    nome: 'Gôndola A4',
    estilo: 'gondola',
    folha: 'a4',
    largura_mm: 90,
    altura_mm: 30,
    nome_pt: 10,
    nome_pt_1: 11,
    nome_pt_2: 9,
    nome_pt_3: 7.5,
    preco_pt: 20,
    rs_pt: 11,
    peso_pt: 7,
    gm_pt: 8,
    codigo_pt: 7,
    rodape_pt: 8,
    barcode_height: 26,
    barcode_width: 1.05,
    texto_rodape: '',
    impressora: '',
    show_logo: true,
    show_nome: true,
    show_rs: true,
    show_preco: true,
    show_peso: true,
    show_gm: false,
    cols_folha: 2,
    rows_folha: 9,
    borda_mm: 0.5,
    cores: {
      faixa_bg: '#1a4d2e',
      faixa_fg: '#ffffff',
      fundo: '#ffffff',
      preco_fg: '#1a4d2e',
      rs_fg: '#1a4d2e',
      peso_fg: '#1a4d2e',
      gm_fg: '#1a4d2e',
      borda: '#1a4d2e',
      marca_corte: '#94a3b8',
    },
    layout: JSON.parse(JSON.stringify(DEFAULT_GONDOLA_LAYOUT)),
  };

  /** Grade suportada na A4 (90 mm = 2 colunas; 60 mm = 3; sempre 9 linhas). */
  function calcularGradeA4(larguraMm, alturaMm, bordaMm, colsSalvas, rowsSalvas) {
    var pageW = 210;
    var pageH = 297;
    var b = Number(bordaMm);
    if (!(b > 0)) b = 0.5;
    var w = Number(larguraMm);
    var h = Number(alturaMm);
    if (!(w > 0)) w = 90;
    if (!(h > 0)) h = 30;
    var outerW = w + 2 * b;
    var outerH = h + 2 * b;
    var largura60 = Math.abs(w - 60) < 0.01;
    var cols = largura60 ? 3 : Math.max(1, parseInt(colsSalvas, 10) || 2);
    var rows = Math.max(1, parseInt(rowsSalvas, 10) || 9);
    return {
      cols: cols,
      rows: rows,
      outer_w: Math.round(outerW * 10) / 10,
      outer_h: Math.round(outerH * 10) / 10,
      per_page: cols * rows,
      cabe_a4: cols * outerW <= pageW && rows * outerH <= pageH,
    };
  }

  var LOGO_AGRO_URL = '/static/produtos/img/logo_agro_mais.png';

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

  var CB_LOJA_PREFIX_RE = /^230\d{10}$/;

  /** Faixa interna loja (230 + 10 dígitos) — 13 chars mas NÃO é EAN-13 (sem DV EAN). */
  function ehCodigoBarrasLojaInterno(cb) {
    var d = String(cb || '').replace(/\D/g, '');
    return CB_LOJA_PREFIX_RE.test(d);
  }

  function extrairCodigoBarrasLojaInterno(p) {
    var cands = candidatosCodigoBarrasNumerico(p);
    for (var i = 0; i < cands.length; i++) {
      var cb = String(cands[i] || '').replace(/\D/g, '');
      if (ehCodigoBarrasLojaInterno(cb)) {
        return { valor: cb, formato: 'CODE128', codigo_loja: true };
      }
    }
    return null;
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
      if (ehCodigoBarrasLojaInterno(cb)) continue;
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
    var loja = extrairCodigoBarrasLojaInterno(p);
    if (loja) return loja;
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

  function ehGondola(preset) {
    return String((preset && preset.estilo) || '') === 'gondola';
  }

  function normalizarPreset(p) {
    if (!p || typeof p !== 'object') return clonePreset(DEFAULT_PRESET);
    var out = clonePreset(p);
    if (!out.estilo) out.estilo = out.id === 'gondola' ? 'gondola' : 'termica';
    if (ehGondola(out)) {
      out.folha = 'a4';
      /* Força padrão 9×3 cm se ainda no seed antigo 90×35. */
      if (Number(out.largura_mm) === 90 && Number(out.altura_mm) === 35) {
        out.altura_mm = 30;
      }
      if (!out.largura_mm) out.largura_mm = 90;
      if (!out.altura_mm) out.altura_mm = 30;
      if (out.borda_mm == null || !(Number(out.borda_mm) > 0)) out.borda_mm = 0.5;
      var grade = calcularGradeA4(
        out.largura_mm,
        out.altura_mm,
        out.borda_mm,
        out.cols_folha,
        out.rows_folha
      );
      out.cols_folha = grade.cols;
      out.rows_folha = grade.rows;
      if (!out.cores || typeof out.cores !== 'object') {
        out.cores = clonePreset(DEFAULT_GONDOLA_PRESET.cores);
      } else {
        Object.keys(DEFAULT_GONDOLA_PRESET.cores).forEach(function (k) {
          if (!out.cores[k]) out.cores[k] = DEFAULT_GONDOLA_PRESET.cores[k];
        });
      }
      if (!out.layout || typeof out.layout !== 'object') {
        out.layout = clonePreset(DEFAULT_GONDOLA_LAYOUT);
      } else {
        Object.keys(DEFAULT_GONDOLA_LAYOUT).forEach(function (k) {
          if (!out.layout[k]) out.layout[k] = clonePreset(DEFAULT_GONDOLA_LAYOUT[k]);
        });
      }
      if (out.peso_pt == null) out.peso_pt = 7;
      if (out.rs_pt == null) out.rs_pt = 11;
      if (out.gm_pt == null) out.gm_pt = 8;
      if (out.show_logo == null) out.show_logo = true;
      /* Presets antigos: campos clássicos ligados; GM desligado (não muda o visual salvo) */
      if (out.show_nome == null) out.show_nome = true;
      if (out.show_rs == null) out.show_rs = true;
      if (out.show_preco == null) out.show_preco = true;
      if (out.show_peso == null) out.show_peso = true;
      if (out.show_gm == null) out.show_gm = false;
      if (out.nome_pt_1 == null) out.nome_pt_1 = Number(out.nome_pt) || 11;
      if (out.nome_pt_2 == null) out.nome_pt_2 = Math.max(4, Math.round((Number(out.nome_pt_1) || 11) * 0.82 * 10) / 10);
      if (out.nome_pt_3 == null) out.nome_pt_3 = Math.max(4, Math.round((Number(out.nome_pt_1) || 11) * 0.68 * 10) / 10);
      if (!out.nome || out.nome === 'Gôndola') out.nome = 'Gôndola A4';
    }
    return out;
  }

  function csrfToken() {
    var m = document.cookie.match(/(?:^|;\s*)csrftoken=([^;]+)/);
    return m ? decodeURIComponent(m[1]) : '';
  }

  function presetsApiUrl() {
    var cfg = global.AGRO_ETQ_CFG || {};
    return cfg.etqPresetsUrl || '/api/produtos/etiquetas/presets/';
  }

  function presetDetailUrl(clientKey) {
    var base = presetsApiUrl().replace(/\/?$/, '/');
    return base + encodeURIComponent(String(clientKey || '')) + '/';
  }

  function loadPrefs() {
    try {
      var raw = localStorage.getItem(LS_KEY);
      if (!raw) {
        return {
          preset_ativo: DEFAULT_PRESET.id,
          texto_rodape_global: DEFAULT_PRESET.texto_rodape,
        };
      }
      var data = JSON.parse(raw);
      return {
        preset_ativo: data.preset_ativo || DEFAULT_PRESET.id,
        texto_rodape_global: data.texto_rodape_global || DEFAULT_PRESET.texto_rodape || '',
      };
    } catch (e) {
      return {
        preset_ativo: DEFAULT_PRESET.id,
        texto_rodape_global: DEFAULT_PRESET.texto_rodape,
      };
    }
  }

  function savePrefs(data) {
    var prev = {};
    try {
      prev = JSON.parse(localStorage.getItem(LS_KEY) || '{}') || {};
    } catch (e) {
      prev = {};
    }
    localStorage.setItem(
      LS_KEY,
      JSON.stringify({
        /* presets só como cache; fonte da verdade = Postgres */
        presets: Array.isArray(data.presets) ? data.presets : prev.presets || [],
        preset_ativo: data.preset_ativo || prev.preset_ativo || DEFAULT_PRESET.id,
        texto_rodape_global: data.texto_rodape_global != null ? data.texto_rodape_global : prev.texto_rodape_global || '',
      })
    );
  }

  function loadStorage() {
    var prefs = loadPrefs();
    var cached = [];
    try {
      var raw = localStorage.getItem(LS_KEY);
      if (raw) {
        var data = JSON.parse(raw);
        cached = Array.isArray(data.presets) ? data.presets : [];
      }
    } catch (e) {
      cached = [];
    }
    return {
      presets: ensureSeedPresets(cached),
      preset_ativo: prefs.preset_ativo,
      texto_rodape_global: prefs.texto_rodape_global,
    };
  }

  function saveStorage(data) {
    savePrefs(data || {});
  }

  function mergeServerPresets(localList, serverList) {
    var byId = {};
    ensureSeedPresets(localList || []).forEach(function (p) {
      byId[p.id] = normalizarPreset(p);
    });
    (serverList || []).forEach(function (p) {
      if (!p || !p.id) return;
      byId[p.id] = normalizarPreset(p);
    });
    var out = Object.keys(byId).map(function (k) {
      return byId[k];
    });
    return ensureSeedPresets(out);
  }

  function fetchPresetsFromServer() {
    return fetch(presetsApiUrl(), {
      credentials: 'same-origin',
      headers: { Accept: 'application/json' },
      cache: 'no-store',
    })
      .then(function (r) {
        if (r.status === 401 || r.status === 403) {
          var err = new Error('login');
          err.code = 'auth';
          throw err;
        }
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json();
      })
      .then(function (j) {
        if (!j || !j.ok) throw new Error((j && j.erro) || 'Falha ao listar presets');
        return (j.presets || []).map(normalizarPreset);
      });
  }

  function upsertPresetToServer(preset) {
    var p = normalizarPreset(preset);
    if (!p.id) return Promise.reject(new Error('preset sem id'));
    return fetch(presetsApiUrl(), {
      method: 'POST',
      credentials: 'same-origin',
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
        'X-CSRFToken': csrfToken(),
      },
      body: JSON.stringify({ client_key: p.id, nome: p.nome, payload: p }),
    }).then(function (r) {
      if (r.status === 401 || r.status === 403) {
        var err = new Error('Faça login para gravar preset na loja.');
        err.code = 'auth';
        throw err;
      }
      if (!r.ok) throw new Error('HTTP ' + r.status);
      return r.json();
    });
  }

  function deletePresetFromServer(clientKey) {
    return fetch(presetDetailUrl(clientKey), {
      method: 'DELETE',
      credentials: 'same-origin',
      headers: { Accept: 'application/json', 'X-CSRFToken': csrfToken() },
    }).then(function (r) {
      if (r.status === 404) return { ok: true };
      if (r.status === 401 || r.status === 403) {
        var err = new Error('Faça login para excluir preset na loja.');
        err.code = 'auth';
        throw err;
      }
      if (!r.ok) throw new Error('HTTP ' + r.status);
      return r.json();
    });
  }

  function migrateLocalPresetsToServerOnce(presets) {
    if (typeof localStorage === 'undefined') return Promise.resolve({ migrated: 0, skipped: true });
    if (localStorage.getItem(LS_MIGRATE_FLAG) === '1') {
      return Promise.resolve({ migrated: 0, skipped: true });
    }
    var list = (presets || []).filter(function (p) {
      return p && p.id;
    });
    if (!list.length) {
      localStorage.setItem(LS_MIGRATE_FLAG, '1');
      return Promise.resolve({ migrated: 0 });
    }
    var i = 0;
    var ok = 0;
    var fail = 0;
    function next() {
      if (i >= list.length) {
        /* Só marca migrado se tudo subiu — senão tenta de novo no próximo login/PC. */
        if (fail === 0) localStorage.setItem(LS_MIGRATE_FLAG, '1');
        return Promise.resolve({ migrated: ok, failed: fail });
      }
      var p = list[i++];
      return upsertPresetToServer(p)
        .then(function () {
          ok += 1;
        })
        .catch(function () {
          fail += 1;
        })
        .then(next);
    }
    return next();
  }

  function ensureSeedPresets(presets) {
    var list = Array.isArray(presets) ? presets.map(normalizarPreset) : [];
    if (!list.length) list = [clonePreset(DEFAULT_PRESET)];
    var hasGondola = list.some(function (p) {
      return p.id === 'gondola' || ehGondola(p);
    });
    if (!hasGondola) list.push(clonePreset(DEFAULT_GONDOLA_PRESET));
    var hasPadrao = list.some(function (p) {
      return p.id === 'padrao-4x4';
    });
    if (!hasPadrao) list.unshift(clonePreset(DEFAULT_PRESET));
    return list;
  }

  /* loadStorage/saveStorage definidos acima (prefs + cache) — stubs antigos removidos */

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
      peso_etiqueta: String(prod.peso_etiqueta || '').trim(),
      qtd: qtd > 0 ? qtd : 1,
    };
  }

  function fmtPesoEtiqueta(peso) {
    var t = String(peso || '').trim();
    if (!t) return '';
    t = t.replace(/^peso\s*:?\s*/i, '').trim();
    return t.toUpperCase();
  }

  /**
   * GM curto na etiqueta gôndola:
   * GM0050-1 → GM50 · GM0123-10 → GM123 · GM0090-55 → GM90
   * (tira zeros à esquerda do número e o sufixo após o hífen)
   */
  function fmtGmCurto(code) {
    var s = String(code || '').trim().toUpperCase();
    if (!s) return '';
    var m = s.match(/^GM0*(\d+)(?:[-:].*)?$/i);
    if (m && m[1]) return 'GM' + String(parseInt(m[1], 10));
    m = s.match(/^GM(\d+)$/i);
    if (m && m[1]) return 'GM' + String(parseInt(m[1], 10));
    return s;
  }

  function campoVisivel(preset, key, defaultOn) {
    var k = 'show_' + key;
    if (preset && preset[k] == null) return defaultOn !== false;
    return !!(preset && preset[k]);
  }

  function splitPrecoParts(v) {
    var s = fmtPreco(v);
    var i = s.indexOf(',');
    if (i < 0) return { inteiro: s || '0', centavos: ',00' };
    return { inteiro: s.slice(0, i) || '0', centavos: s.slice(i) };
  }

  function logoImgMarkup() {
    return (
      '<img src="' +
      esc(LOGO_AGRO_URL) +
      '" alt="Agro Mais" style="width:100%;height:100%;object-fit:contain;display:block" />'
    );
  }

  function boxCss(box) {
    var b = box || { x: 0, y: 0, w: 100, h: 20 };
    return (
      'left:' +
      Number(b.x) +
      '%;top:' +
      Number(b.y) +
      '%;width:' +
      Number(b.w) +
      '%;height:' +
      Number(b.h) +
      '%'
    );
  }

  function marcasCorteHtml(xMm, yMm, wMm, hMm, cor) {
    var c = cor || '#94a3b8';
    var L = 2.2;
    var o = 0.15;
    function line(x1, y1, x2, y2) {
      return (
        '<line x1="' +
        x1 +
        '" y1="' +
        y1 +
        '" x2="' +
        x2 +
        '" y2="' +
        y2 +
        '" stroke="' +
        esc(c) +
        '" stroke-width="0.25" />'
      );
    }
    /* Cruzetas nos 4 cantos, para fora da etiqueta. */
    var parts = [];
    /* TL */
    parts.push(line(xMm - L, yMm - o, xMm + L * 0.35, yMm - o));
    parts.push(line(xMm - o, yMm - L, xMm - o, yMm + L * 0.35));
    /* TR */
    parts.push(line(xMm + wMm - L * 0.35, yMm - o, xMm + wMm + L, yMm - o));
    parts.push(line(xMm + wMm + o, yMm - L, xMm + wMm + o, yMm + L * 0.35));
    /* BL */
    parts.push(line(xMm - L, yMm + hMm + o, xMm + L * 0.35, yMm + hMm + o));
    parts.push(line(xMm - o, yMm + hMm - L * 0.35, xMm - o, yMm + hMm + L));
    /* BR */
    parts.push(line(xMm + wMm - L * 0.35, yMm + hMm + o, xMm + wMm + L, yMm + hMm + o));
    parts.push(line(xMm + wMm + o, yMm + hMm - L * 0.35, xMm + wMm + o, yMm + hMm + L));
    return parts.join('');
  }

  function montarConteudoEtiquetaGondola(lb, layout, preset) {
    var showNome = campoVisivel(preset, 'nome', true);
    var showRs = campoVisivel(preset, 'rs', true);
    var showPreco = campoVisivel(preset, 'preco', true);
    var showPeso = campoVisivel(preset, 'peso', true);
    var showLogo = campoVisivel(preset, 'logo', true);
    var showGm = campoVisivel(preset, 'gm', false);
    var precoHtml =
      '<span class="preco-int">' +
      esc(lb.inteiro) +
      '</span><span class="preco-cent">' +
      esc(lb.centavos) +
      '</span>';
    var html = '';
    if (showNome) {
      html +=
        '<div class="slot slot-nome" style="' +
        boxCss(layout.nome) +
        '">' +
        esc(lb.nome) +
        '</div>';
    }
    if (showRs) {
      html +=
        '<div class="slot slot-rs" style="' +
        boxCss(layout.rs) +
        '">R$</div>';
    }
    if (showPreco) {
      html +=
        '<div class="slot slot-preco" style="' +
        boxCss(layout.preco) +
        '">' +
        precoHtml +
        '</div>';
    }
    if (showLogo) {
      html +=
        '<div class="slot slot-logo" style="' +
        boxCss(layout.logo) +
        '">' +
        logoImgMarkup() +
        '</div>';
    }
    if (showPeso && lb.peso) {
      html +=
        '<div class="slot slot-peso" style="' +
        boxCss(layout.peso) +
        '">' +
        esc(lb.peso) +
        '</div>';
    }
    if (showGm && lb.gm) {
      html +=
        '<div class="slot slot-gm" style="' +
        boxCss(layout.gm) +
        '">' +
        esc(lb.gm) +
        '</div>';
    }
    return html;
  }

  function cssGondolaSlots(preset, cores) {
    return (
      '.slot{position:absolute;box-sizing:border-box;overflow:hidden}' +
      '.slot-nome{display:-webkit-box;-webkit-box-orient:vertical;-webkit-line-clamp:3;align-content:center;justify-content:center;padding:0.4mm 1.2mm;background:' +
      esc(cores.faixa_bg || '#1a4d2e') +
      ';color:' +
      esc(cores.faixa_fg || '#fff') +
      ';font-size:' +
      (Number(preset.nome_pt_1) || Number(preset.nome_pt) || 11) +
      'pt;font-weight:800;line-height:1.08;text-align:center;text-transform:uppercase;letter-spacing:0.02em;overflow:hidden;word-break:break-word;overflow-wrap:anywhere}' +
      '.slot-rs{display:flex;align-items:flex-end;justify-content:flex-end;padding:0 0.4mm 1.2mm 0;color:' +
      esc(cores.rs_fg || cores.preco_fg || '#1a4d2e') +
      ';font-size:' +
      (Number(preset.rs_pt) || 11) +
      'pt;font-weight:800;line-height:1}' +
      '.slot-preco{display:flex;align-items:center;justify-content:flex-start;padding:0.3mm;color:' +
      esc(cores.preco_fg || '#1a4d2e') +
      ';font-size:' +
      (Number(preset.preco_pt) || 20) +
      'pt;font-weight:900;line-height:1;white-space:nowrap}' +
      '.preco-int{font-size:1em;font-weight:900}' +
      '.preco-cent{font-size:0.58em;font-weight:800;vertical-align:super;margin-left:0.05em;position:relative;top:-0.15em}' +
      '.slot-peso{display:flex;align-items:center;justify-content:flex-end;padding:0 1.5mm;color:' +
      esc(cores.peso_fg || '#1a4d2e') +
      ';font-size:' +
      (Number(preset.peso_pt) || 7) +
      'pt;font-weight:800;line-height:1.1;text-transform:uppercase}' +
      '.slot-gm{display:flex;align-items:center;justify-content:center;padding:0 0.8mm;color:' +
      esc(cores.gm_fg || cores.peso_fg || '#1a4d2e') +
      ';font-size:' +
      (Number(preset.gm_pt) || 8) +
      'pt;font-weight:900;line-height:1.1;letter-spacing:0.02em;white-space:nowrap}' +
      '.slot-logo{display:flex;align-items:center;justify-content:center;padding:0.2mm}'
    );
  }

  function scriptFitNomeGondola(preset) {
    var pt1 = Number(preset.nome_pt_1) || Number(preset.nome_pt) || 11;
    var pt2 = Number(preset.nome_pt_2) || Math.max(4, pt1 * 0.82);
    var pt3 = Number(preset.nome_pt_3) || Math.max(4, pt1 * 0.68);
    return (
      '<script>(function(){var P1=' +
      pt1 +
      ',P2=' +
      pt2 +
      ',P3=' +
      pt3 +
      ';function cabe(el,linhas,pt){el.style.fontSize=pt+"pt";el.style.webkitLineClamp=String(linhas);el.style.lineClamp=String(linhas);void el.offsetHeight;return el.scrollHeight<=el.clientHeight+1.5;}function fit(el){if(cabe(el,1,P1))return;if(cabe(el,2,P2))return;el.style.fontSize=P3+"pt";el.style.webkitLineClamp="3";el.style.lineClamp="3";}function go(){document.querySelectorAll(".slot-nome").forEach(fit);}if(document.readyState==="loading")document.addEventListener("DOMContentLoaded",go);else go();setTimeout(go,30);})();<\/script>'
    );
  }

  function montarHtmlGondola(preset, itens) {
    var w = Number(preset.largura_mm) || 90;
    var h = Number(preset.altura_mm) || 30;
    var bordaMm = Number(preset.borda_mm);
    if (!(bordaMm > 0)) bordaMm = 0.5;
    /* Borda pra fora: total = útil + 2×borda (ex. 90×30 + 0,5 cada lado → 91×31). */
    var outerW = w + 2 * bordaMm;
    var outerH = h + 2 * bordaMm;
    var cols = Math.max(1, parseInt(preset.cols_folha, 10) || 2);
    var rows = Math.max(1, parseInt(preset.rows_folha, 10) || 9);
    var perPage = cols * rows;
    var pageW = 210;
    var pageH = 297;
    var marginX = Math.max(0, (pageW - cols * outerW) / 2);
    var marginY = Math.max(0, (pageH - rows * outerH) / 2);
    var cores = preset.cores || DEFAULT_GONDOLA_PRESET.cores;
    var layout = preset.layout || DEFAULT_GONDOLA_LAYOUT;
    var labels = [];
    itens.forEach(function (it) {
      var qtd = Math.max(1, parseInt(it.qtd, 10) || 1);
      var parts = splitPrecoParts(it.preco_venda);
      for (var i = 0; i < qtd; i++) {
        labels.push({
          nome: String(it.nome || ''),
          inteiro: parts.inteiro,
          centavos: parts.centavos,
          peso: fmtPesoEtiqueta(it.peso_etiqueta),
          gm: fmtGmCurto(it.codigo_gm || it.codigo_nfe || ''),
        });
      }
    });

    var css =
      '@page{size:A4;margin:0}' +
      'html,body{margin:0;padding:0;width:' +
      pageW +
      'mm;background:#fff;zoom:1!important;transform:none!important}' +
      'body{font-family:Arial,Helvetica,sans-serif;-webkit-print-color-adjust:exact;print-color-adjust:exact}' +
      '.sheet{position:relative;width:' +
      pageW +
      'mm;height:' +
      pageH +
      'mm;box-sizing:border-box;page-break-after:always;break-after:page;overflow:hidden}' +
      '.sheet:last-child{page-break-after:auto;break-after:auto}' +
      '.etq{position:absolute;width:' +
      outerW +
      'mm;height:' +
      outerH +
      'mm;box-sizing:border-box;overflow:hidden;background:' +
      esc(cores.fundo || '#fff') +
      ';border:' +
      bordaMm +
      'mm solid ' +
      esc(cores.borda || cores.faixa_bg || '#1a4d2e') +
      '}' +
      '.crop-layer{position:absolute;left:0;top:0;width:' +
      pageW +
      'mm;height:' +
      pageH +
      'mm;pointer-events:none;z-index:5}' +
      cssGondolaSlots(preset, cores);

    var pages = [];
    var total = Math.max(labels.length, 1);
    for (var start = 0; start < total; start += perPage) {
      var chunk = labels.slice(start, start + perPage);
      var marks = [];
      var cells = chunk
        .map(function (lb, idx) {
          var col = idx % cols;
          var row = Math.floor(idx / cols);
          var x = marginX + col * outerW;
          var y = marginY + row * outerH;
          marks.push(marcasCorteHtml(x, y, outerW, outerH, cores.marca_corte));
          return (
            '<div class="etq" style="left:' +
            x +
            'mm;top:' +
            y +
            'mm">' +
            montarConteudoEtiquetaGondola(lb, layout, preset) +
            '</div>'
          );
        })
        .join('');
      for (var gi = chunk.length; gi < perPage; gi++) {
        var gc = gi % cols;
        var gr = Math.floor(gi / cols);
        marks.push(
          marcasCorteHtml(
            marginX + gc * outerW,
            marginY + gr * outerH,
            outerW,
            outerH,
            cores.marca_corte
          )
        );
      }
      pages.push(
        '<div class="sheet">' +
          cells +
          '<svg class="crop-layer" viewBox="0 0 ' +
          pageW +
          ' ' +
          pageH +
          '" xmlns="http://www.w3.org/2000/svg">' +
          marks.join('') +
          '</svg></div>'
      );
      if (!labels.length) break;
    }

    return (
      '<!DOCTYPE html><html><head><meta charset="utf-8"><title>Etiquetas Gôndola A4</title><style>' +
      css +
      '</style></head><body>' +
      pages.join('') +
      scriptFitNomeGondola(preset) +
      '</body></html>'
    );
  }

  function montarHtmlTermica(preset, itens, textoRodape) {
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

  function montarHtmlImpressao(preset, itens, textoRodape) {
    preset = normalizarPreset(preset);
    if (ehGondola(preset)) return montarHtmlGondola(preset, itens);
    return montarHtmlTermica(preset, itens, textoRodape);
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
            peso_etiqueta: it.peso_etiqueta || '',
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
    preset = normalizarPreset(preset);
    var textoRodape =
      opts.textoRodape != null
        ? String(opts.textoRodape)
        : data.texto_rodape_global || preset.texto_rodape || '';
    var html = montarHtmlImpressao(preset, itens, textoRodape);
    var w = Number(preset.largura_mm) || 40;
    var h = Number(preset.altura_mm) || 40;
    var pageWMicrons = Math.round(w * 1000);
    var pageHMicrons = Math.round(h * 1000);
    if (ehGondola(preset)) {
      pageWMicrons = 210000;
      pageHMicrons = 297000;
    }

    if (podeSilentPrint()) {
      return global.agroShell
        .silentPrint({
          html: html,
          deviceName: preset.impressora || '',
          waitMs: ehGondola(preset) ? 1100 : 900,
          pageWidthMicrons: pageWMicrons,
          pageHeightMicrons: pageHMicrons,
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
      var waitPrint = ehGondola(preset) ? 850 : 650;
      setTimeout(function () {
        try {
          iframe.contentWindow.focus();
          iframe.contentWindow.print();
          registrarHistoricoBackend(opts, itens);
          resolve({ ok: true, silent: false });
        } catch (e) {
          resolve({ ok: false, reason: String(e && e.message) });
        }
      }, waitPrint);
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
    DEFAULT_GONDOLA_PRESET: DEFAULT_GONDOLA_PRESET,
    DEFAULT_GONDOLA_LAYOUT: DEFAULT_GONDOLA_LAYOUT,
    calcularGradeA4: calcularGradeA4,
    esc: esc,
    fmtPreco: fmtPreco,
    fmtPesoEtiqueta: fmtPesoEtiqueta,
    fmtGmCurto: fmtGmCurto,
    logoImgMarkup: logoImgMarkup,
    LOGO_AGRO_URL: LOGO_AGRO_URL,
    clonePreset: clonePreset,
    normalizarPreset: normalizarPreset,
    ehGondola: ehGondola,
    loadStorage: loadStorage,
    saveStorage: saveStorage,
    fetchPresetsFromServer: fetchPresetsFromServer,
    upsertPresetToServer: upsertPresetToServer,
    deletePresetFromServer: deletePresetFromServer,
    migrateLocalPresetsToServerOnce: migrateLocalPresetsToServerOnce,
    mergeServerPresets: mergeServerPresets,
    getPresetAtivo: getPresetAtivo,
    getPresetById: getPresetById,
    produtoParaItem: produtoParaItem,
    montarHtmlImpressao: montarHtmlImpressao,
    imprimirItens: imprimirItens,
    podeSilentPrint: podeSilentPrint,
    fillPresetSelect: fillPresetSelect,
    valorBarcodeProduto: valorBarcodeProduto,
    extrairEanNumerico: extrairEanNumerico,
    ehCodigoBarrasLojaInterno: ehCodigoBarrasLojaInterno,
    ean13ChecksumOk: ean13ChecksumOk,
    normalizarEan13: normalizarEan13,
  };
})(typeof window !== 'undefined' ? window : this);
