/**
 * Path: Pedir loja → Etiquetas 53×30 (lista compacta).
 * node scripts/verify_pdv_pedir_etq53_path.js
 */
const fs = require('fs');
const path = require('path');

const root = path.join(__dirname, '..');
const js = fs.readFileSync(path.join(root, 'produtos/static/produtos/js/pdv_pedir_loja.js'), 'utf8');
const html = fs.readFileSync(
  path.join(root, 'produtos/templates/produtos/partials/pdv/pedir_loja_overlay.html'),
  'utf8'
);

let n = 0;
let fail = 0;
function ok(cond, msg) {
  n += 1;
  if (!cond) {
    fail += 1;
    console.log('FAIL', msg);
  }
}

ok(js.includes('function imprimirEtiquetasSeparacao53'), 'fn etiquetas 53');
ok(js.includes('size:53mm 30mm'), 'página 53×30');
ok(js.includes('LINHAS_POR_ETQ = 6'), '6 linhas por etiqueta');
ok(js.includes('data-pl-acao="etiquetas"'), 'botão acao etiquetas');
ok(js.includes('Etiquetas 53'), 'rótulo botão');
ok(js.includes("acao === 'etiquetas'"), 'handler etiquetas');
ok(js.includes('function abrirPrintIframe'), 'iframe compartilhado');
ok(html.includes('pl-btn--etq'), 'CSS botão roxo');
ok(js.includes('imprimirCupomSeparacao'), 'cupom 80mm intacto');

/* Packing: 6 → 1 página; 7 → 2; 12 → 2; 13 → 3 */
function pagesFor(count, per) {
  return Math.max(1, Math.ceil(count / per));
}
ok(pagesFor(1, 6) === 1, '1 item = 1 etq');
ok(pagesFor(6, 6) === 1, '6 itens = 1 etq');
ok(pagesFor(7, 6) === 2, '7 itens = 2 etq');
ok(pagesFor(12, 6) === 2, '12 itens = 2 etq');
ok(pagesFor(13, 6) === 3, '13 itens = 3 etq');

console.log(fail ? 'FAIL ' + fail + '/' + n : 'OK ' + n + '/' + n);
process.exit(fail ? 1 : 0);
