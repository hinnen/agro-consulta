/**
 * Path ETQ-PONTE-TOPBAR-BIP — ponte na topbar · sem rodapé na fila · bip limpa · auto-add.
 * Uso: node scripts/verify_etq_ponte_topbar_bip_path.js
 */
'use strict';
const fs = require('fs');
const path = require('path');
const vm = require('vm');

const root = path.join(__dirname, '..');
function read(rel) {
  return fs.readFileSync(path.join(root, rel), 'utf8');
}

let n = 0;
let fails = 0;
function ok(cond, msg) {
  n += 1;
  if (cond) console.log('OK:', msg);
  else {
    fails += 1;
    console.error('FAIL:', msg);
  }
}

const html = read('produtos/templates/produtos/produtos_etiquetas.html');
const js = read('produtos/static/produtos/js/produtos_etiquetas.js');
const buscaUtil = read('produtos/cadastro_busca_codigo_util.py');

ok(html.includes('etq-btn-ponte'), 'topbar botão Ponte');
ok(html.includes('etq-bridge-back'), 'modal Ponte');
ok(html.includes('etq-bridge-card'), 'card/modal bridge');
ok(html.includes('etq-btn-bridge-download'), 'Baixar ponte');
ok(html.includes('etq-size-map-box'), 'mapa tamanho');
ok(html.includes("produtos_etiquetas.js' %}?v=33"), 'cache ui v=33');
ok(!html.includes('etq-texto-rodape-global'), 'sem rodapé duplicado na fila');
ok(html.includes('Rodapé vem do preset') || html.includes('vem do preset'), 'hint rodapé no preset');

ok(js.includes('abrirModalPonte'), 'JS abrirModalPonte');
ok(js.includes('fecharModalPonte'), 'JS fecharModalPonte');
ok(js.includes('limparBusca !== false'), 'limpa busca após add');
ok(js.includes('produtoCasaCodigoBip'), 'casa código bip');
ok(js.includes('autoAddBip'), 'autoAddBip');
ok(js.includes("autoAddBip: true"), 'scheduleBusca liga autoAddBip');
ok(js.includes('pFila.texto_rodape') || js.includes('pFila && pFila.texto_rodape'), 'imprime rodapé do preset');
ok(!js.includes("$('etq-texto-rodape-global')"), 'JS sem campo rodapé global');

ok(buscaUtil.includes('_json_contains_suportado'), 'SQLite: gate JSON contains');
ok(buscaUtil.includes("connection.vendor == \"postgresql\""), 'JSON contains só Postgres');

/* Runtime: produtoCasaCodigoBip via extrato mínimo */
const s = { console };
vm.createContext(s);
const fnSrc =
  js.match(/function produtoCasaCodigoBip\([\s\S]*?\n  \}/) ||
  js.match(/function produtoCasaCodigoBip\([\s\S]*?\n  function /);
ok(!!fnSrc, 'extrai produtoCasaCodigoBip');
if (fnSrc) {
  const body = fnSrc[0].replace(/\n  function $/, '\n');
  vm.runInContext(body + '\nthis.produtoCasaCodigoBip = produtoCasaCodigoBip;', s);
  const f = s.produtoCasaCodigoBip;
  ok(f({ codigo_barras: '3000000001509', codigo: 'GM0050-15' }, '3000000001509') === true, 'casa EAN exato');
  ok(f({ codigo_barras: '3000000001509', codigo: 'GM0050-15' }, 'GM0050-15') === true, 'casa GM');
  ok(f({ codigo_barras: '3000000001509', codigo: 'GM0050-15' }, 'ração') === false, 'nome não auto-add');
  ok(f({ codigo_barras: '789', codigo: 'X' }, '3000000001509') === false, 'EAN diferente não casa');
}

if (fails) {
  console.error('VERIFY_FAIL', fails + '/' + n);
  process.exit(1);
}
console.log('VERIFY_OK', n + '/' + n);
console.log('OK: path ETQ-PONTE-TOPBAR-BIP verificado.');
