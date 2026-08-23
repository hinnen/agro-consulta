/**
 * Parser Urano USE-P2 (USE-PII) — dump real da COM4:
 *   30 30 20 1b 4e 31 20 20 20 20 30 2c 30 30 20
 *   "00 " + ESC N 1 + espaços + "0,00 "
 *
 * Último ESC N 1 vence. ESC N 0 é preço/etiqueta e não sobrescreve o peso.
 * 0,00 é prato vazio válido. “Sem bytes” só com buffer vazio.
 */
(function (root) {
  'use strict';

  var ESC = 0x1b;
  var ENQ = 0x05;
  var USER_DUMP_HEX = '30 30 20 1b 4e 31 20 20 20 20 30 2c 30 30 20';
  var MIN_WEIGHT_KG = 0.02;
  var MAX_WEIGHT_KG = 30;
  var STABLE_MS = 380;
  var STABLE_EPSILON_KG = 0.002;

  var SERIAL_DEFAULTS = {
    baudRate: 9600,
    dataBits: 8,
    stopBits: 2,
    parity: 'none',
    flowControl: 'none',
    bufferSize: 255,
  };

  function hexToBytes(hex) {
    var parts = String(hex || '')
      .trim()
      .split(/\s+/)
      .filter(Boolean)
      .map(function (h) {
        return parseInt(h, 16);
      });
    for (var i = 0; i < parts.length; i++) {
      if (!Number.isFinite(parts[i])) throw new Error('hex inválido: ' + hex);
    }
    return Uint8Array.from(parts);
  }

  function bytesToHex(bytes) {
    var out = [];
    for (var i = 0; i < (bytes || []).length; i++) {
      out.push(('0' + bytes[i].toString(16)).slice(-2));
    }
    return out.join(' ');
  }

  function bytesToGlyphs(bytes) {
    var out = [];
    for (var i = 0; i < (bytes || []).length; i++) {
      var b = bytes[i];
      if (b === ESC) out.push('ESC');
      else if (b === 0x0d) out.push('CR');
      else if (b === 0x0a) out.push('LF');
      else if (b === 0x02) out.push('STX');
      else if (b === 0x03) out.push('ETX');
      else if (b === 0x05) out.push('ENQ');
      else if (b === 0x20) out.push('·');
      else if (b >= 32 && b < 127) out.push(String.fromCharCode(b));
      else out.push('x' + ('0' + b.toString(16)).slice(-2));
    }
    return out.join(' ');
  }

  function parseDecimalToken(raw) {
    var cleaned = String(raw || '').replace(/\s/g, '').replace(',', '.');
    if (!/^-?\d+\.\d{2,3}$/.test(cleaned)) return null;
    var n = Number(cleaned);
    return Number.isFinite(n) ? n : null;
  }

  function decodeLatin1(bytes) {
    var s = '';
    for (var i = 0; i < (bytes || []).length; i++) s += String.fromCharCode(bytes[i]);
    return s;
  }

  function collectAsciiAfter(bytes, start) {
    var chars = [];
    for (var i = start; i < bytes.length; i++) {
      var b = bytes[i];
      if (b === ESC) break;
      if (b === 0x0d || b === 0x0a) break;
      if (b >= 32 && b < 127) chars.push(String.fromCharCode(b));
    }
    return chars.join('');
  }

  function firstDecimal(text) {
    var match = String(text || '').match(/-?\d{1,3}[.,]\d{2,3}/);
    return match ? parseDecimalToken(match[0]) : null;
  }

  function allDecimals(text) {
    var matches = String(text || '').match(/-?\d{1,3}[.,]\d{2,3}/g) || [];
    var out = [];
    for (var i = 0; i < matches.length; i++) {
      var n = parseDecimalToken(matches[i]);
      if (n !== null) out.push(n);
    }
    return out;
  }

  function stripEscapes(bytes) {
    var chars = [];
    for (var i = 0; i < bytes.length; i++) {
      var b = bytes[i];
      if (b === ESC) {
        var cmd = bytes[i + 1];
        if (cmd === undefined) break;
        if (
          cmd === 0x4e ||
          cmd === 0x54 ||
          cmd === 0x50 ||
          cmd === 0x41 ||
          cmd === 0x53 ||
          cmd === 0x44 ||
          cmd === 0x51
        ) {
          i += 2;
          continue;
        }
        i += 1;
        continue;
      }
      if (b >= 32 && b < 127) chars.push(String.fromCharCode(b));
      else if (b === 0x0d || b === 0x0a) chars.push(' ');
    }
    return chars.join('');
  }

  function weightFromKgField(text) {
    var lower = String(text || '').toLowerCase();
    var idx = lower.lastIndexOf('kg');
    if (idx < 1) return null;
    return firstDecimal(text.slice(Math.max(0, idx - 8), idx));
  }

  function weightFromPesoL(text) {
    var idx = String(text || '').toUpperCase().indexOf('PESO L:');
    if (idx < 0) return null;
    return firstDecimal(text.slice(idx, idx + 20));
  }

  function isPlausibleWeight(n) {
    return n >= 0 && n <= MAX_WEIGHT_KG;
  }

  function weightFromStx(bytes) {
    var text = decodeLatin1(bytes);
    if (/instav|instável|unstable|------/i.test(text) || /sobrecarga|overload/i.test(text)) {
      return null;
    }
    var mEt = text.match(/\x02\s*([+-]?\d{1,2}[.,]\d{1,4})\s*\x03/);
    if (mEt) {
      var nEt = parseFloat(mEt[1].replace(',', '.'));
      if (Number.isFinite(nEt)) {
        nEt = Math.abs(nEt);
        if (nEt <= MAX_WEIGHT_KG) return nEt;
      }
    }
    var mStx = text.match(/\x02\s*(\d{4,7})\s*[\x03\r\n]?/);
    if (mStx) {
      var grams = parseInt(mStx[1], 10);
      if (Number.isFinite(grams) && grams >= 0) {
        var kgG = grams / 1000;
        if (kgG === 0 || (kgG <= MAX_WEIGHT_KG && kgG >= 0)) return kgG;
      }
    }
    return null;
  }

  function emptyReading(hadBytes, text) {
    return {
      weightKg: null,
      price: null,
      total: null,
      source: null,
      text: text || '',
      hadBytes: !!hadBytes,
    };
  }

  function parseUseP2(bytes) {
    bytes = bytes || new Uint8Array();
    if (!bytes.length) return emptyReading(false, '');

    var rawText = decodeLatin1(bytes);
    var stripped = stripEscapes(bytes);
    if (/TARA\s*:/i.test(rawText) && !/PESO/i.test(rawText)) {
      return emptyReading(true, stripped);
    }

    var weight = null;
    var source = null;
    var fallbackN0 = null;
    var i;

    for (i = 0; i < bytes.length - 2; i++) {
      if (bytes[i] === ESC && bytes[i + 1] === 0x4e) {
        var param = bytes[i + 2];
        var chunk = collectAsciiAfter(bytes, i + 3);
        var value = firstDecimal(chunk);
        if (value === null || !isPlausibleWeight(value)) continue;
        if (param === 0x31) {
          weight = value;
          source = 'esc-n1';
        } else if (weight === null) {
          fallbackN0 = value;
        }
      }
    }

    if (weight === null && fallbackN0 !== null) {
      weight = fallbackN0;
      source = 'esc-n1';
    }

    if (weight === null) {
      var fromPeso = weightFromPesoL(rawText);
      if (fromPeso === null) fromPeso = weightFromPesoL(stripped);
      if (fromPeso !== null && isPlausibleWeight(fromPeso)) {
        weight = fromPeso;
        source = 'peso-l';
      }
    }

    if (weight === null) {
      var fromKg = weightFromKgField(rawText);
      if (fromKg === null) fromKg = weightFromKgField(stripped);
      if (fromKg !== null && isPlausibleWeight(fromKg)) {
        weight = fromKg;
        source = 'kg-field';
      }
    }

    if (weight === null) {
      var decimals = allDecimals(stripped).filter(isPlausibleWeight);
      if (decimals.length > 0) {
        weight = decimals[0];
        source = 'decimal';
      }
    }

    if (weight === null) {
      var stx = weightFromStx(bytes);
      if (stx !== null && isPlausibleWeight(stx)) {
        weight = stx;
        source = 'stx';
      }
    }

    var extras = allDecimals(stripped).filter(function (n) {
      return n !== weight;
    });

    return {
      weightKg: weight,
      price: extras[0] != null ? extras[0] : null,
      total: extras[1] != null ? extras[1] : null,
      source: source,
      text: stripped.trim(),
      hadBytes: true,
    };
  }

  function mergeSerialBuffer(previous, incoming, maxBytes) {
    previous = previous || new Uint8Array();
    incoming = incoming || new Uint8Array();
    maxBytes = maxBytes || 512;
    var merged = new Uint8Array(previous.length + incoming.length);
    merged.set(previous, 0);
    merged.set(incoming, previous.length);
    if (merged.length <= maxBytes) return merged;
    return merged.slice(merged.length - maxBytes);
  }

  function padWeight(weightKg) {
    var formatted = Number(weightKg).toFixed(2).replace('.', ',');
    while (formatted.length < 6) formatted = ' ' + formatted;
    return formatted;
  }

  function encodeUseP2Frame(input) {
    input = input || {};
    var weight = padWeight(input.weightKg || 0);
    var price =
      input.price !== undefined
        ? Number(input.price).toFixed(2).replace('.', ',').padStart(7, ' ')
        : '';
    var total =
      input.total !== undefined
        ? Number(input.total).toFixed(2).replace('.', ',').padStart(7, ' ')
        : '';
    var parts = [ESC, 0x54, 0x31, ESC, 0x42, ESC, 0x4e, 0x31];
    var i;
    for (i = 0; i < weight.length; i++) parts.push(weight.charCodeAt(i));
    parts.push(0x20, ESC, 0x4e, 0x30, 0x6b, 0x67, 0x20);
    for (i = 0; i < price.length; i++) parts.push(price.charCodeAt(i));
    for (i = 0; i < total.length; i++) parts.push(total.charCodeAt(i));
    parts.push(ESC, 0x45, ESC, 0x50, 0x31);
    return Uint8Array.from(parts);
  }

  function formatKg(weightKg) {
    if (weightKg === null || weightKg === undefined || !Number.isFinite(weightKg)) return '--,--';
    return Number(weightKg).toLocaleString('pt-BR', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 3,
    });
  }

  function isWeightStable(history, now, windowMs, epsilon) {
    history = history || [];
    windowMs = windowMs == null ? STABLE_MS : windowMs;
    epsilon = epsilon == null ? STABLE_EPSILON_KG : epsilon;
    if (history.length < 3) return false;
    var recent = history.filter(function (h) {
      return now - h.at <= windowMs + 80;
    });
    if (recent.length < 3) return false;
    var first = recent[0].kg;
    return recent.every(function (h) {
      return Math.abs(h.kg - first) <= epsilon;
    });
  }

  function autoAddKey(code, weightKg) {
    return String(code) + ':' + Number(weightKg).toFixed(3);
  }

  function platterIsEmpty(weightKg, minKg) {
    minKg = minKg == null ? MIN_WEIGHT_KG : minKg;
    return weightKg !== null && Number.isFinite(weightKg) && weightKg < minKg;
  }

  function canAddToCart(input) {
    input = input || {};
    var minKg = input.minKg == null ? MIN_WEIGHT_KG : input.minKg;
    return (
      !!input.hasProduct &&
      input.weightKg !== null &&
      Number.isFinite(input.weightKg) &&
      input.weightKg >= minKg &&
      (input.mode === 'sim' || input.mode === 'serial')
    );
  }

  function decideAutoAdd(input) {
    input = input || {};
    var minKg = input.minKg == null ? MIN_WEIGHT_KG : input.minKg;
    if (platterIsEmpty(input.weightKg, minKg)) {
      return { add: false, nextKey: '' };
    }
    if (!input.open || input.code === null || input.weightKg === null || !input.stable) {
      return { add: false, nextKey: input.lastKey || '' };
    }
    var key = autoAddKey(input.code, input.weightKg);
    if (key === input.lastKey) return { add: false, nextKey: key };
    return { add: true, nextKey: key };
  }

  var AgroUseP2 = {
    ESC: ESC,
    ENQ: ENQ,
    USER_DUMP_HEX: USER_DUMP_HEX,
    MIN_WEIGHT_KG: MIN_WEIGHT_KG,
    MAX_WEIGHT_KG: MAX_WEIGHT_KG,
    STABLE_MS: STABLE_MS,
    STABLE_EPSILON_KG: STABLE_EPSILON_KG,
    SERIAL_DEFAULTS: SERIAL_DEFAULTS,
    hexToBytes: hexToBytes,
    bytesToHex: bytesToHex,
    bytesToGlyphs: bytesToGlyphs,
    parseDecimalToken: parseDecimalToken,
    parseUseP2: parseUseP2,
    mergeSerialBuffer: mergeSerialBuffer,
    encodeUseP2Frame: encodeUseP2Frame,
    formatKg: formatKg,
    isWeightStable: isWeightStable,
    autoAddKey: autoAddKey,
    platterIsEmpty: platterIsEmpty,
    canAddToCart: canAddToCart,
    decideAutoAdd: decideAutoAdd,
  };

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = AgroUseP2;
  }
  root.AgroUseP2 = AgroUseP2;
})(typeof window !== 'undefined' ? window : typeof globalThis !== 'undefined' ? globalThis : this);
