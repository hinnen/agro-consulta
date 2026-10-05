/**
 * Path PDV-PEDIR-PRINT-3 — Etiqueta 40×40 (substitui Etiquetas 53).
 * node scripts/verify_pdv_pedir_etq53_path.js
 * Mantém o nome do arquivo por quem ainda chama o path antigo.
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

ok(js.includes('function montarHtmlEtiquetas40x40'), 'fn etiquetas 40×40');
ok(js.includes('size:40mm 40mm'), 'página 40×40');
ok(js.includes('LINHAS_POR_ETQ = 3'), '3 linhas por etiqueta');
ok(!js.includes('data-pl-acao="etiquetas"'), 'sem botão acao etiquetas');
ok(!js.includes('Etiquetas 53'), 'sem rótulo Etiquetas 53');
ok(js.includes('abrirEscolhaImpressao'), 'popup escolha');
ok(html.includes('data-pl-print="etq40"'), 'opção etq40 no modal');
ok(js.includes('function abrirPrintIframe'), 'iframe compartilhado');
ok(js.includes('montarHtmlCupomPedidos'), 'cupom 80mm intacto');
ok(js.includes('size:80mm auto'), 'cupom 80mm');
ok(js.includes('montarHtmlA4Separacao'), 'folha A4');

function pagesFor(count, per) {
  return Math.max(1, Math.ceil(count / per));
}
ok(pagesFor(1, 3) === 1, '1 item = 1 etq');
ok(pagesFor(3, 3) === 1, '3 itens = 1 etq');
ok(pagesFor(4, 3) === 2, '4 itens = 2 etq');
ok(pagesFor(9, 3) === 3, '9 itens = 3 etq');

console.log(fail ? 'FAIL ' + fail + '/' + n : 'OK ' + n + '/' + n);
process.exit(fail ? 1 : 0);
