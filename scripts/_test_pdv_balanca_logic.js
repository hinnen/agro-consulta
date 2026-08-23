/**
 * Provas unitárias do path balança (sem Web Serial / browser).
 * node scripts/_test_pdv_balanca_logic.js
 */
'use strict';

var path = require('path');
var P2 = require(path.join(__dirname, '..', 'produtos', 'static', 'produtos', 'js', 'use_p2.js'));

var CODE_MIN = 1;
var CODE_MAX = 199;
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
  var list = [p.codigo, p.codigo_nfe, p.codigo_interno, p.codigo_barras, p.codigo_gm];
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

/* --- cadastro 1–199 --- */
var pBar = { id: '1', nome: 'Racao mix', codigo_barras: '0010', codigo_nfe: 'GM0010-1' };
var pGmOnly = { id: '2', nome: 'Outro GM', codigo_nfe: 'GM0010-2' };
var pBar010 = { id: '3', nome: 'Barras 010', codigo_barras: '010' };
var pBar10 = { id: '4', nome: 'Barras 10', codigo_barras: '10' };
var pWrong = { id: '5', nome: 'Errado', codigo_barras: '0011', codigo_nfe: 'GM0011-1' };
var pInterno = { id: '6', nome: 'Banana-prata', codigo_interno: '1' };

assert(productMatchesGranelNum(pBar, 10), '0010 + GM0010-1 casa 10');
assert(productMatchesGranelNum(pGmOnly, 10), 'GM0010-2 casa 10');
assert(!productMatchesGranelNum(pWrong, 10), '0011 não casa 10');
assert(productMatchesGranelNum(pBar010, 10), '010 casa 10');
assert(productMatchesGranelNum(pInterno, 1), 'codigo_interno 1 casa PLU 1');

var pref1 = preferHits([pGmOnly, pBar], 10);
assert(pref1.length === 1 && pref1[0].id === '1', 'preferHits escolhe barras 0010 sobre GM');

var pref2 = preferHits([pBar010, pBar], 10);
assert(pref2.length === 2, 'dois barcodes 10/0010/010 = ambíguo (2)');

var pref3 = preferHits([pGmOnly], 10);
assert(pref3.length === 1 && pref3[0].id === '2', 'só GM ok se único');
assert(preferHits([pBar10], 10)[0].id === '4', 'barras "10" casa');

/* --- serial POP-Z --- */
assert(P2.SERIAL_DEFAULTS.baudRate === 9600, '9600');
assert(P2.SERIAL_DEFAULTS.dataBits === 8, '8 data bits');
assert(P2.SERIAL_DEFAULTS.stopBits === 2, '2 stop bits (manual POP-Z)');
assert(P2.SERIAL_DEFAULTS.parity === 'none', 'sem paridade');
assert(P2.SERIAL_DEFAULTS.flowControl === 'none', 'sem fluxo');
assert(P2.ENQ === 0x05, 'ENQ 0x05');
assert(P2.MIN_WEIGHT_KG === 0.02, 'mínimo 20 g');
assert(P2.STABLE_MS === 380, 'estável ~380 ms');

/* --- dump real COM4 --- */
var dump = P2.hexToBytes(P2.USER_DUMP_HEX);
assert(dump.length === 15, 'dump 15 bytes');
assert(dump[3] === 0x1b && dump[4] === 0x4e && dump[5] === 0x31, 'ESC N 1 no dump');
assert(dump[11] === 0x2c, 'vírgula brasileira');
var glyphs = P2.bytesToGlyphs(dump);
assert(glyphs.indexOf('ESC') >= 0 && glyphs.indexOf('N') >= 0, 'glifos ESC N');

var rDump = P2.parseUseP2(dump);
assert(rDump.hadBytes === true, 'dump tinha bytes');
assert(rDump.weightKg === 0, 'dump = 0,00 kg');
assert(rDump.source === 'esc-n1', 'fonte esc-n1');
assert(P2.formatKg(rDump.weightKg) === '0,00', 'formatKg 0,00');

/* ESC não é “impressora errada” — é o protocolo */
assert(dump[3] === 0x1b, 'ESC no frame válido da balança');

assert(P2.parseUseP2(new Uint8Array()).hadBytes === false, 'buffer vazio = sem bytes');
assert(P2.parseUseP2(P2.hexToBytes('30 30 20')).weightKg === null, '00 sem vírgula não é peso');
assert(P2.parseUseP2(P2.hexToBytes('30 30 20')).hadBytes === true, '00 ainda tinha bytes');

/* frame partido */
var first = P2.mergeSerialBuffer(new Uint8Array(), dump.slice(0, 5));
assert(P2.parseUseP2(first).weightKg === null, 'metade do dump ainda sem peso');
var merged = P2.mergeSerialBuffer(first, dump.slice(5));
assert(P2.parseUseP2(merged).weightKg === 0, 'merge fecha 0,00');

/* último ESC N 1 vence no buffer rolante */
var buf = new Uint8Array();
var i;
for (i = 0; i < 8; i++) buf = P2.mergeSerialBuffer(buf, dump);
assert(P2.parseUseP2(buf).weightKg === 0, 'vários 0,00 ainda 0');
buf = P2.mergeSerialBuffer(buf, P2.encodeUseP2Frame({ weightKg: 1.25 }));
assert(P2.parseUseP2(buf).weightKg === 1.25, '1,250 vence o 0,00 antigo');

/* ESC N 0 (preço) não sobrescreve peso */
var framed = P2.encodeUseP2Frame({ weightKg: 1.25, price: 8.9, total: 11.13 });
var rFr = P2.parseUseP2(framed);
assert(rFr.weightKg === 1.25, 'encode 1,25 kg');
assert(rFr.source === 'esc-n1', 'encode fonte esc-n1');

/* fallbacks */
assert(P2.parseUseP2(Uint8Array.from('PESO L: 0,450kg'.split('').map(function (c) { return c.charCodeAt(0); }))).weightKg === 0.45, 'PESO L:');
assert(P2.parseUseP2(Uint8Array.from('PESO L: 0,450kg'.split('').map(function (c) { return c.charCodeAt(0); }))).source === 'peso-l', 'fonte peso-l');
assert(P2.parseUseP2(Uint8Array.from('   5,10kg'.split('').map(function (c) { return c.charCodeAt(0); }))).source === 'kg-field', 'fonte kg');
assert(P2.parseUseP2(P2.hexToBytes('1b 4e 31 20 20 31 2e 35 30 20')).weightKg === 1.5, 'ponto no lugar da vírgula');

var tara = Uint8Array.from('TARA: 0,120kg'.split('').map(function (c) { return c.charCodeAt(0); }));
assert(P2.parseUseP2(tara).weightKg === null, 'TARA sozinha ignorada');

/* STX legado (outros firmwares) */
assert(P2.parseUseP2(Uint8Array.from('\x02001000\r'.split('').map(function (c) { return c.charCodeAt(0); }))).weightKg === 1, 'STX+CR 1kg');
assert(P2.parseUseP2(Uint8Array.from('\x021.000\x03'.split('').map(function (c) { return c.charCodeAt(0); }))).weightKg === 1, 'STX+ETX decimal 1kg');

/* min / canAdd */
assert(P2.canAddToCart({ hasProduct: true, weightKg: 0.019, mode: 'sim' }) === false, '19 g não entra');
assert(P2.canAddToCart({ hasProduct: true, weightKg: 0.02, mode: 'sim' }) === true, '20 g entra');
assert(P2.canAddToCart({ hasProduct: true, weightKg: 1.25, mode: 'idle' }) === false, 'idle não adiciona');
assert(P2.canAddToCart({ hasProduct: true, weightKg: 0, mode: 'serial' }) === false, '0,00 não entra no carrinho');

/* estável */
var t = 10000;
assert(
  P2.isWeightStable(
    [
      { kg: 1.25, at: t - 200 },
      { kg: 1.25, at: t - 100 },
    ],
    t
  ) === false,
  '2 leituras ainda instável'
);
assert(
  P2.isWeightStable(
    [
      { kg: 1.25, at: t - 300 },
      { kg: 1.25, at: t - 200 },
      { kg: 1.25, at: t - 100 },
    ],
    t
  ) === true,
  '3 iguais = estável'
);
assert(
  P2.isWeightStable(
    [
      { kg: 1.2, at: t - 300 },
      { kg: 1.25, at: t - 200 },
      { kg: 1.25, at: t - 100 },
    ],
    t
  ) === false,
  'leituras diferentes = instável'
);

/* auto-add + prato vazio */
var firstAdd = P2.decideAutoAdd({
  open: true,
  code: 1,
  weightKg: 1.25,
  stable: true,
  lastKey: '',
});
assert(firstAdd.add === true && firstAdd.nextKey === '1:1.250', 'estável + código 1 entra uma vez');

var again = P2.decideAutoAdd({
  open: true,
  code: 1,
  weightKg: 1.25,
  stable: true,
  lastKey: firstAdd.nextKey,
});
assert(again.add === false, 'não duplica o auto-add');

var empty = P2.decideAutoAdd({
  open: true,
  code: null,
  weightKg: 0,
  stable: true,
  lastKey: firstAdd.nextKey,
});
assert(empty.add === false && empty.nextKey === '', 'prato vazio zera o ciclo');

var second = P2.decideAutoAdd({
  open: true,
  code: 1,
  weightKg: 1.25,
  stable: true,
  lastKey: empty.nextKey,
});
assert(second.add === true, 'depois do prato vazio entra de novo');

var manualKey = P2.autoAddKey(1, 1.25);
var afterManual = P2.decideAutoAdd({
  open: true,
  code: 1,
  weightKg: 1.25,
  stable: true,
  lastKey: manualKey,
});
assert(afterManual.add === false, 'ADICIONAR AGORA trava o auto-add da mesma pesagem');

var lineTotal = Math.round(1.25 * 6.9 * 100) / 100;
assert(lineTotal === 8.63, '1,250 kg × R$ 6,90 = R$ 8,63');

if (failed) {
  console.error('\n' + failed + ' falha(s)');
  process.exit(1);
}
console.log('\nTodas as provas OK');
