/**
 * node scripts/verify_vendas_lojas_pulso_aviso.js
 * O botão Pulso da loja continua no painel. O protótipo do dashboard saiu.
 */
'use strict';

const fs = require('fs');
const path = require('path');
const painel = require('../produtos/static/produtos/js/vendas_lojas_pulso_painel.js');

let fails = 0;
function ok(cond, msg) {
  if (!cond) {
    fails += 1;
    console.error('FAIL', msg);
  }
}

const amostra = [
  '<button data-titulo="Centro" data-vendido="10,00"></button>',
  '<button data-titulo="Vila Elias" data-vendido="5,50"></button>',
  '<button data-titulo="Total das duas lojas" data-vendido="15,50"></button>'
].join('');
const totais = painel.lerTotais(amostra);
ok(totais && totais.total === '15,50' && totais.centro === '10,00' && totais.vila === '5,50', 'painel lê as três lojas');
ok(painel.lerTotais('<html>login</html>') === null, 'página sem venda não vira número');
const aviso = painel.textoAviso(totais, '09:10', false);
ok(aviso.indexOf('Hoje') === 0 && aviso.includes('Centro') && aviso.includes('Vila Elias') && aviso.endsWith('09:10'), 'texto do pulso');
ok(painel.textoAviso(totais, '09:10', true).startsWith('Último valor'), 'sem rede marca o último');
ok(painel.INTERVALO_MS === 300000, 'painel a cada 5 minutos');
ok(painel.urlHoje('https://loja').includes('/vendas/lojas/painel/?periodo=hoje'), 'busca o dia de hoje no painel');

const tela = fs.readFileSync(
  path.join(__dirname, '../produtos/templates/produtos/vendas_lojas_resumo.html'),
  'utf8'
);
ok(tela.includes('id="vl-pulso"'), 'botão no painel de vendas');
ok(tela.includes('vendas_lojas_pulso_painel.js'), 'painel carrega o aviso');

const dash = fs.readFileSync(
  path.join(__dirname, '../produtos/templates/produtos/vendas_lojas_dashboard.html'),
  'utf8'
);
ok(!dash.includes('vendas_lojas_pulso_aviso.js'), 'dashboard não carrega o protótipo');
ok(!fs.existsSync(path.join(__dirname, '../produtos/static/produtos/js/vendas_lojas_pulso_aviso.js')), 'protótipo removido');

if (fails) {
  console.error('VERIFY_FAIL', fails);
  process.exit(1);
}
console.log('VERIFY_OK');
