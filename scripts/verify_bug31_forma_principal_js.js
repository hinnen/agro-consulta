/**
 * Executa formaPrincipalParaPreco real do precos_forma_pagamento.js (bug #31).
 * node scripts/verify_bug31_forma_principal_js.js
 */
'use strict';

const fs = require('fs');
const path = require('path');
const vm = require('vm');

const ROOT = path.resolve(__dirname, '..');
const src = fs.readFileSync(
  path.join(ROOT, 'produtos/static/produtos/js/precos_forma_pagamento.js'),
  'utf8'
);

const sandbox = { window: {}, console };
vm.createContext(sandbox);
vm.runInContext(src, sandbox);

const API = sandbox.window.AgroPrecosFormaPagamento;
if (!API || typeof API.formaPrincipalParaPreco !== 'function') {
  console.error('FAIL API formaPrincipalParaPreco ausente');
  process.exit(1);
}

let ok = 0;
let fail = 0;
function check(name, cond) {
  if (cond) {
    ok += 1;
    console.log('  OK ', name);
  } else {
    fail += 1;
    console.log(' FAIL', name);
  }
}

console.log('=== BUG31 JS formaPrincipalParaPreco (VM) ===');

const mix = {
  pagamento: {
    forma: 'Cashback',
    valorDestaForma: 10,
    lancamentos: [{ forma: 'Dinheiro', valor: 90 }],
  },
};
check('mix hint Cashback = Dinheiro', API.formaPrincipalParaPreco(mix, 'Cashback') === 'Dinheiro');
check('mix state = Dinheiro', API.formaPrincipalParaPreco(mix) === 'Dinheiro');
check('obterFormaDoState mix = Dinheiro', API.obterFormaDoState(mix) === 'Dinheiro');

const so = {
  pagamento: { forma: 'Cashback', valorDestaForma: 50, lancamentos: [] },
};
check('so Cashback', API.formaPrincipalParaPreco(so) === 'Cashback');

const vale = {
  pagamento: {
    forma: 'Vale crédito',
    valorDestaForma: 5,
    lancamentos: [{ forma: 'PIX', valor: 200 }],
  },
};
check('PIX + Vale hint = PIX', API.formaPrincipalParaPreco(vale, 'Vale crédito') === 'PIX');

const maior = {
  pagamento: {
    forma: 'Cashback',
    valorDestaForma: 0,
    lancamentos: [
      { forma: 'Dinheiro', valor: 40 },
      { forma: 'Cartão de crédito', valor: 60 },
    ],
  },
};
check(
  'maior mercadoria = credito',
  API.formaPrincipalParaPreco(maior) === 'Cartão de crédito'
);

console.log(`\nResultado: ${ok} ok, ${fail} fail`);
process.exit(fail ? 1 : 0);
