/**
 * node scripts/verify_vendas_lojas_pulso_aviso.js
 * Texto do aviso e a regra de só atualizar com a tela ativa.
 */
'use strict';

const fs = require('fs');
const path = require('path');
const pulso = require('../produtos/static/produtos/js/vendas_lojas_pulso_aviso.js');

let fails = 0;
function ok(cond, msg) {
  if (!cond) {
    fails += 1;
    console.error('FAIL', msg);
  }
}

const payload = {
  ok: true,
  atualizado_hora: '14:05',
  por_loja: { total: '1500.5', centro: '1000', vila: '500.5' }
};

const texto = pulso.textoAviso(payload, false);
ok(texto.indexOf('Hoje') === 0, 'total em destaque');
ok(texto.includes('R$'), 'valor em reais');
ok(texto.includes('1.500,50') || texto.includes('1500,50'), 'total formatado');
ok(texto.includes('Centro'), 'centro');
ok(texto.includes('Vila Elias'), 'vila');
ok(texto.trim().endsWith('14:05'), 'horário no fim');

const velho = pulso.textoAviso(payload, true);
ok(velho.startsWith('Último valor · sem conexão'), 'sem rede marca o último valor');
ok(velho.includes('14:05'), 'último valor guarda a hora');

ok(pulso.INTERVALO_MS === 5 * 60 * 1000, 'intervalo de 5 minutos');

const url = pulso.urlHoje('/api/vendas/lojas/dashboard/resumo/', '2026-11-16');
ok(url.includes('loja=todas'), 'as duas lojas');
ok(url.includes('inicio=2026-11-16'), 'começa hoje');
ok(url.includes('fim=2026-11-16'), 'termina hoje');
ok(!url.includes('token') && !url.includes('pin'), 'url sem segredo');

const src = fs.readFileSync(
  path.join(__dirname, '../produtos/static/produtos/js/vendas_lojas_pulso_aviso.js'),
  'utf8'
);
ok(!/serviceWorker|periodicSync|setTimeout\(\s*function/.test(src), 'sem atualização escondida');
ok(src.includes("credentials: 'same-origin'"), 'usa a sessão do SistApp');
ok(!/99738595|senha\s*=/.test(src), 'sem senha no arquivo');

let buscas = 0;
const ctrl = pulso.criarControlador({
  ativo: function () { return false; },
  buscar: function () { buscas += 1; return Promise.resolve(payload); },
  avisar: function () {}
});
ctrl.ligar();
Promise.resolve(ctrl.tick()).then(function () {
  ok(buscas === 0, 'tela fechada não busca venda');
  ctrl.desligar();
  const html = fs.readFileSync(
    path.join(__dirname, '../produtos/templates/produtos/vendas_lojas_dashboard.html'),
    'utf8'
  );
  ok(html.includes('vendas_lojas_pulso_aviso.js'), 'dashboard carrega o aviso');
  const dash = fs.readFileSync(
    path.join(__dirname, '../produtos/static/produtos/js/vendas_lojas_dashboard_mobile.js'),
    'utf8'
  );
  ok(dash.includes('data-action="pulso"'), 'botão Avisar no pulso');
  if (fails) {
    console.error('VERIFY_FAIL', fails);
    process.exit(1);
  }
  console.log('VERIFY_OK');
}).catch(function (err) {
  console.error(err);
  process.exit(1);
});
