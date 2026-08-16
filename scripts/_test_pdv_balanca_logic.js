/**
 * Provas unitárias do path balança (sem Web Serial / browser).
 * node scripts/_test_pdv_balanca_logic.js
 */
'use strict';

var CODE_MIN = 1;
var CODE_MAX = 199;
var MIN_KG = 0.001;
var MAX_KG = 99.999;
var failed = 0;

function assert(cond, msg) {
  if (!cond) {
    failed++;
    console.error('FAIL:', msg);
  } else {
    console.log('OK:', msg);
  }
}

function barcodeOf(p) {
  return String(p.codigo_barras || p.CodigoBarras || '').trim();
}

function gmGranelNum(c) {
  var m = String(c || '')
    .trim()
    .match(/^GM0*(\d{1,3})(?:-.*)?$/i);
  if (!m) return null;
  var n = parseInt(m[1], 10);
  return n >= CODE_MIN && n <= CODE_MAX ? n : null;
}

function productCodes(p) {
  var list = [p.codigo, p.codigo_nfe, p.codigo_barras, p.codigo_gm];
  if (Array.isArray(p.index_codigos)) list = list.concat(p.index_codigos);
  return list;
}

function productMatchesGranelNum(p, num) {
  var codes = productCodes(p);
  for (var i = 0; i < codes.length; i++) {
    var c = String(codes[i] == null ? '' : codes[i]).trim();
    if (!c) continue;
    if (/^\d{1,4}$/.test(c) && parseInt(c, 10) === num) return true;
    if (gmGranelNum(c) === num) return true;
  }
  return false;
}

function preferHits(hits, num) {
  hits = hits || [];
  var byBar = hits.filter(function (p) {
    var b = barcodeOf(p);
    return b && /^\d{1,4}$/.test(b) && parseInt(b, 10) === num;
  });
  if (byBar.length >= 1) return byBar;
  return hits;
}

function parseWeightFromChunk(text) {
  if (!text) return null;
  var raw = String(text);
  if (/instav|instável|unstable|------/i.test(raw) || /sobrecarga|overload/i.test(raw)) {
    return null;
  }
  var mEt = raw.match(/\x02\s*([+-]?\d{1,2}[.,]\d{1,4})\s*\x03/);
  if (mEt) {
    var nEt = parseFloat(mEt[1].replace(',', '.'));
    if (Number.isFinite(nEt)) {
      nEt = Math.abs(nEt);
      if (nEt <= MAX_KG) return { kg: nEt };
    }
  }
  var mStx = raw.match(/\x02\s*(\d{4,7})\s*[\x03\r\n]?/);
  if (mStx) {
    var grams = parseInt(mStx[1], 10);
    if (Number.isFinite(grams) && grams >= 0) {
      var kgG = grams / 1000;
      if (kgG >= MIN_KG && kgG <= MAX_KG) return { kg: kgG };
      if (grams === 0) return { kg: 0 };
    }
  }
  return null;
}

function looksLikeEscPos(arr) {
  for (var i = 0; i < (arr || []).length; i++) {
    if (arr[i] === 0x1b) return true;
  }
  return false;
}

function feedFrames(byteArrays) {
  var byteBuf = [];
  var samples = [];
  function onWeight(kg) {
    samples.push(kg);
  }
  function feed(u8) {
    for (var i = 0; i < u8.length; i++) byteBuf.push(u8[i]);
    while (true) {
      var end = -1;
      for (var j = 0; j < byteBuf.length; j++) {
        if (byteBuf[j] === 0x0d || byteBuf[j] === 0x0a || byteBuf[j] === 0x03) {
          end = j;
          break;
        }
      }
      if (end < 0) break;
      var frame = byteBuf.splice(0, end + 1);
      var chars = '';
      for (var k = 0; k < frame.length; k++) chars += String.fromCharCode(frame[k]);
      var parsed = parseWeightFromChunk(chars);
      if (parsed) onWeight(parsed.kg);
    }
  }
  byteArrays.forEach(feed);
  return { samples: samples, leftover: byteBuf.length };
}

/* --- testes --- */
var pBar = { id: '1', nome: 'Racao mix', codigo_barras: '0010', codigo_nfe: 'GM0010-1' };
var pGmOnly = { id: '2', nome: 'Outro GM', codigo_nfe: 'GM0010-2' };
var pBar010 = { id: '3', nome: 'Barras 010', codigo_barras: '010' };
var pBar10 = { id: '4', nome: 'Barras 10', codigo_barras: '10' };
var pWrong = { id: '5', nome: 'Errado', codigo_barras: '0011', codigo_nfe: 'GM0011-1' };

assert(productMatchesGranelNum(pBar, 10), '0010 + GM0010-1 casa 10');
assert(productMatchesGranelNum(pGmOnly, 10), 'GM0010-2 casa 10');
assert(!productMatchesGranelNum(pWrong, 10), '0011 não casa 10');
assert(productMatchesGranelNum(pBar010, 10), '010 casa 10');

var pref1 = preferHits([pGmOnly, pBar], 10);
assert(pref1.length === 1 && pref1[0].id === '1', 'preferHits escolhe barras 0010 sobre GM');

var pref2 = preferHits([pBar010, pBar], 10);
assert(pref2.length === 2, 'dois barcodes 10/0010/010 = ambíguo (2)');

var pref3 = preferHits([pGmOnly], 10);
assert(pref3.length === 1 && pref3[0].id === '2', 'só GM ok se único');

assert(parseWeightFromChunk('\x02001000\r').kg === 1, 'STX+CR 1kg');
assert(parseWeightFromChunk('\x02001000\x03').kg === 1, 'STX+ETX gramas 1kg');
assert(parseWeightFromChunk('\x021.000\x03').kg === 1, 'STX+ETX decimal 1kg');
assert(parseWeightFromChunk('\x02000500\r').kg === 0.5, 'STX+CR 0.5kg');
assert(parseWeightFromChunk('\x02012500\r').kg === 12.5, 'STX+CR 12.5kg');

var f1 = feedFrames([Buffer.from([0x02, 0x30, 0x30, 0x31, 0x30, 0x30, 0x30, 0x0d])]);
assert(f1.samples[0] === 1 && f1.leftover === 0, 'feed CR frame 1kg limpo');

var f2 = feedFrames([Buffer.from([0x02, 0x31, 0x2e, 0x30, 0x30, 0x30, 0x03])]);
assert(f2.samples[0] === 1 && f2.leftover === 0, 'feed ETX decimal 1kg limpo');

var f3 = feedFrames([
  Buffer.from([0x02, 0x30, 0x30, 0x31, 0x30, 0x30, 0x30]),
  Buffer.from([0x0d]),
]);
assert(f3.samples[0] === 1, 'frame partido STX… + CR depois');

var f4 = feedFrames([Buffer.from([0x02, 0x30, 0x30, 0x31, 0x30, 0x30, 0x30, 0x30])]);
assert(f4.samples.length === 0 && f4.leftover === 8, '7 dígitos sem delimitador NÃO consome');

assert(preferHits([pBar10], 10)[0].id === '4', 'barras "10" casa');

assert(parseWeightFromChunk('001000\r') == null, 'sem STX não vira peso');
assert(parseWeightFromChunk('1.000') == null, 'decimal solto não vira peso');
assert(looksLikeEscPos([0x1b, 0x45, 0x1b, 0x50, 0x31]), 'detecta ESC impressora');
assert(!looksLikeEscPos([0x02, 0x30, 0x30, 0x31, 0x30, 0x30, 0x30, 0x0d]), 'STX+peso não é ESC');

var fEsc = feedFrames([Buffer.from([0x1b, 0x45, 0x1b, 0x50, 0x31, 0x0d])]);
assert(fEsc.samples.length === 0, 'frame ESC não vira peso');

if (failed) {
  console.error('\n' + failed + ' falha(s)');
  process.exit(1);
}
console.log('\nTodas as provas OK');
