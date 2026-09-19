'use strict';
const fs = require('fs');
const vm = require('vm');
const html = fs.readFileSync('produtos/templates/produtos/entrada_nota.html', 'utf8');
const css = html.slice(html.indexOf('/* Nota finalizada'), html.indexOf('body.entrada-nfe--editor'));
function expect(ok, msg) { if (!ok) throw new Error(msg); }
expect((css.match(/input:not\(#nfe-wiz-pin-estoque\)/g) || []).length === 2, 'PIN especial ainda bloqueado pelo CSS');
expect((css.match(/:not\(#btn-estoque-reabrir-nota\)/g) || []).length === 2, 'botao especial ainda bloqueado pelo CSS');
expect((css.match(/pointer-events: none !important/g) || []).length === 2, 'travas gerais foram removidas');
expect(css.includes('[data-nfe-wiz-lock="1"] select'), 'restante da nota deixou de ser bloqueado');
expect(!css.includes(':not(#btn-estoque-agro)'), 'botao azul foi excluido indevidamente da trava');

function block(start, end) {
  const a = html.indexOf(start); const b = html.indexOf(end, a);
  expect(a >= 0 && b > a, `bloco ausente: ${start}`);
  return html.slice(a, b);
}
const fn = block('async function entradaNfeReabrirNota(escopoOpt)', "  document.getElementById('entrada-nfe-wizard-nav')");
const listenerSpecial = html.split(/\r?\n/).find(line => line.includes("btn-estoque-reabrir-nota')?.addEventListener('click'"));
const listenerFull = html.split(/\r?\n/).find(line => line.includes("nfe-wiz-btn-reabrir')?.addEventListener('click'"));
const enterSpecial = html.match(/document\.getElementById\('nfe-wiz-pin-estoque'\)\?\.addEventListener\('keydown',[\s\S]*?\n  \}\);/)?.[0];
expect(listenerSpecial && listenerFull && enterSpecial, 'listeners de recuperacao ausentes');

class El {
  constructor(value = '') { this.value = value; this.disabled = false; this.listeners = {}; }
  addEventListener(type, cb) { (this.listeners[type] ||= []).push(cb); }
  setAttribute() {} removeAttribute() {}
  focus() { this.focused = true; }
  fire(type, props = {}) { for (const cb of this.listeners[type] || []) cb({preventDefault() {}, ...props}); }
}
const els = {
  'nfe-wiz-pin': new El(''), 'nfe-wiz-pin-estoque': new El('2468'),
  'nfe-wiz-btn-reabrir': new El(), 'btn-estoque-reabrir-nota': new El(),
  'btn-estoque-agro': new El(),
};
const calls = [];
const context = {
  console, setTimeout, clearTimeout, encodeURIComponent,
  document: {getElementById: id => els[id] || null},
  entradaNfeRascunhoEditId: 'r1', entradaNfeWizardConferenciaRegistrada: true,
  entradaNfeExtraUltimo: {aprovacao_wizard_em: 'ok'}, URL_REABRIR_NOTA: '/reabrir', URL_RASC_OBTER: '/obter',
  entradaNfeEditorCongeladoAtivo: () => true, entradaNfeWizardAlgumaEtapaIntermediariaTravada: () => true,
  getCookie: () => 'csrf', showMsg() {}, aplicarRascunhoNaGrade() {}, entradaNfeEstoqueJaRegistrado: () => false,
  entradaNfeWizardSetStep() {}, entradaNfeAtualizarUiBotaoWizardFinalizar() {}, entradaNfeAtualizarUiBotaoConfirmarFornecedor() {},
  entradaNfeAtualizarUiBotaoConfirmarProdutos() {}, entradaNfeAtualizarUiBotaoConfirmarCodigos() {}, entradaNfeWizardAgendarAtualizarCores() {},
  entradaNfeAplicarCongelamentoEditorUi() {},
  entradaNfeFetch: async (url, options = {}) => { calls.push({url, options}); return {ok: true, json: async () => url === '/reabrir' ? {ok:true} : {ok:true, rascunho:{extra:{estoque_pendente_liberado_em:'ok'}}}}; },
};
vm.createContext(context);
vm.runInContext(`${fn}\n${listenerFull}\n${listenerSpecial}\n${enterSpecial}`, context);
const tick = () => new Promise(resolve => setTimeout(resolve, 0));
(async () => {
  els['nfe-wiz-pin-estoque'].focus();
  els['btn-estoque-reabrir-nota'].fire('click'); await tick();
  let posts = calls.filter(c => c.options.method === 'POST');
  expect(els['nfe-wiz-pin-estoque'].focused && els['nfe-wiz-pin-estoque'].value === '2468', 'PIN nao aceita foco/digitacao');
  expect(posts.length === 1, `clique especial gerou ${posts.length} POSTs`);
  expect(JSON.parse(posts[0].options.body).escopo === 'estoque_pendente', 'payload especial incorreto');
  calls.length = 0; els['nfe-wiz-pin-estoque'].fire('keydown', {key:'Enter'}); await tick();
  expect(calls.filter(c => c.options.method === 'POST').length === 1, 'Enter nao dispara exatamente um POST');
  calls.length = 0; els['nfe-wiz-btn-reabrir'].fire('click'); await tick(); posts = calls.filter(c => c.options.method === 'POST');
  expect(posts.length === 1 && JSON.parse(posts[0].options.body).escopo === 'completo', 'reabertura da etapa 8 nao envia completo');
  expect(!els['btn-estoque-agro'].listeners.click, 'hotfix tornou botao azul clicavel por listener novo');
  console.log('OK DOM recuperacao estoque Entrada NF');
})().catch(err => { console.error(err.stack || err); process.exitCode = 1; });
