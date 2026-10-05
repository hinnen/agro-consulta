/**
 * Path ETQ-53-QUOTA — hotfix loja: quota localStorage não mata presets/busca.
 * node scripts/verify_etq_53_quota_path.js
 */
const fs = require('fs');
const path = require('path');
const vm = require('vm');

const root = path.join(__dirname, '..');
const coreCode = fs.readFileSync(
  path.join(root, 'produtos/static/produtos/js/produtos_etiquetas_core.js'),
  'utf8'
);
const ui = fs.readFileSync(
  path.join(root, 'produtos/static/produtos/js/produtos_etiquetas.js'),
  'utf8'
);
const page = fs.readFileSync(
  path.join(root, 'produtos/templates/produtos/produtos_etiquetas.html'),
  'utf8'
);
const lote = fs.readFileSync(
  path.join(root, 'produtos/templates/produtos/produtos_etiquetas_lote.html'),
  'utf8'
);
const cad = fs.readFileSync(
  path.join(root, 'produtos/templates/produtos/produtos_cadastro_erp.html'),
  'utf8'
);
const nfe = fs.readFileSync(
  path.join(root, 'produtos/templates/produtos/entrada_nota.html'),
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

/* --- Cache-bust alinhado em todas as telas --- */
ok(page.includes("produtos_etiquetas_core.js' %}?v=31"), 'etiquetas core v=31');
ok(page.includes("produtos_etiquetas.js' %}?v=27"), 'etiquetas ui v=27');
ok(lote.includes("produtos_etiquetas_core.js' %}?v=31"), 'lote core v=31');
ok(cad.includes("produtos_etiquetas_core.js' %}?v=31"), 'cadastro core v=31');
ok(nfe.includes("produtos_etiquetas_core.js' %}?v=31"), 'entrada NF core v=31');
ok(!page.includes('defer></script>'), 'sem defer (ordem Core→UI)');

/* --- Código: ordem paint → persist + tolerância quota --- */
ok(coreCode.includes('Quota') || /catch\s*\(\s*e1\s*\)/.test(coreCode), 'savePrefs catch quota');
ok(coreCode.includes('presets: []'), 'fallback prefs leves (presets vazios)');
ok(ui.includes('pinta a tela ANTES') || ui.includes('ANTES de gravar'), 'comentário ordem paint');
ok(ui.includes('garantirPresetsNaTela'), 'garantirPresetsNaTela existe');
ok(ui.includes('bindEvents'), 'bindEvents existe');

const gIdx = ui.indexOf('function garantirPresetsNaTela');
const gBlock = ui.slice(gIdx, gIdx + 2200);
ok(gBlock.includes('renderPresetSelect()'), 'garantir chama renderPresetSelect');
const renderPos = gBlock.indexOf('renderPresetSelect()');
const persistPos = gBlock.indexOf('persistStorage()');
ok(renderPos >= 0 && persistPos > renderPos, 'garantir: render ANTES de persist');

const initIdx = ui.indexOf('function init()');
const initBlock = ui.slice(initIdx, initIdx + 1800);
ok(initBlock.includes('garantirPresetsNaTela()'), 'init pinta presets');
ok(initBlock.includes('bindEvents()'), 'init liga busca/botões');
const bindPos = initBlock.indexOf('bindEvents()');
const carregarPos = initBlock.indexOf('carregarPresetsDaLoja()');
ok(bindPos >= 0 && (carregarPos < 0 || bindPos < carregarPos), 'init: bindEvents antes/independente da API');

const loadIdx = ui.indexOf('function carregarPresetsDaLoja');
const loadBlock = ui.slice(loadIdx, loadIdx + 1600);
const mergePos = loadBlock.indexOf('mergeServerPresets(state.storage.presets, serverList)');
const paintAfterMerge = loadBlock.indexOf('garantirPresetsNaTela()', mergePos);
const migratePos = loadBlock.indexOf('migrateLocalPresetsToServerOnce', mergePos);
ok(
  mergePos >= 0 && paintAfterMerge > mergePos && migratePos > paintAfterMerge,
  'carregarPresetsDaLoja: paint entre merge e migrate'
);

/* --- VM: Core + quota cheia --- */
const store = {};
const s = {
  document: { cookie: '' },
  localStorage: {
    getItem(k) {
      return Object.prototype.hasOwnProperty.call(store, k) ? store[k] : null;
    },
    setItem(k, v) {
      store[k] = String(v);
    },
    removeItem(k) {
      delete store[k];
    },
  },
};
s.window = s;
vm.createContext(s);
vm.runInContext(coreCode, s);
const Core = s.AgroEtiquetasCore;
ok(!!Core, 'Core carrega no VM');

const seeded = Core.mergeServerPresets([], []);
ok(seeded.some((p) => p.id === 'padrao-53x30'), 'seed 53×30');
ok(seeded.some((p) => p.id === 'padrao-4x4'), 'seed 4×4');
ok(seeded.some((p) => p.id === 'gondola'), 'seed gondola');

/* Quota: setItem sempre falha — saveStorage não pode lançar */
s.localStorage.setItem = function () {
  const err = new Error('QuotaExceededError');
  err.name = 'QuotaExceededError';
  throw err;
};
let threwFull = false;
try {
  Core.saveStorage({
    presets: seeded,
    preset_ativo: 'padrao-53x30',
    texto_rodape_global: 'Gm Agro',
  });
} catch (e) {
  threwFull = true;
}
ok(!threwFull, 'saveStorage quota total não lança');

/* Quota: 1º setItem (payload grande) falha; 2º (leve) ok */
let calls = 0;
const lightStore = {};
s.localStorage.setItem = function (k, v) {
  calls += 1;
  if (calls === 1) {
    const err = new Error('QuotaExceededError');
    err.name = 'QuotaExceededError';
    throw err;
  }
  lightStore[k] = String(v);
};
s.localStorage.getItem = function (k) {
  return Object.prototype.hasOwnProperty.call(lightStore, k) ? lightStore[k] : null;
};
Core.saveStorage({
  presets: seeded,
  preset_ativo: 'padrao-53x30',
  texto_rodape_global: 'Rodape',
});
ok(calls >= 2, 'quota: tenta prefs leves após falha (' + calls + ')');
const saved = lightStore[Object.keys(lightStore)[0]];
ok(saved && saved.includes('"presets":[]'), 'fallback grava presets=[]');
ok(saved.includes('padrao-53x30'), 'fallback mantém preset_ativo');
ok(saved.includes('Rodape'), 'fallback mantém rodapé');

/* loadStorage com cache vazio ainda entrega seeds */
s.localStorage.getItem = function () {
  return null;
};
s.localStorage.setItem = function () {};
const loaded = Core.loadStorage();
ok(Array.isArray(loaded.presets) && loaded.presets.length >= 4, 'loadStorage seeds n=' + (loaded.presets || []).length);
ok(loaded.presets.some((p) => p.id === 'padrao-53x30'), 'loadStorage tem 53×30');

/* merge servidor vazio não apaga seeds */
const again = Core.mergeServerPresets(loaded.presets, []);
ok(again.some((p) => p.id === 'padrao-53x30'), 'merge [] mantém 53×30');

/* boot erro se Core ausente */
ok(ui.includes('Motor de etiquetas não carregou'), 'UI avisa Core ausente');
ok(ui.includes('(Recarregue Ctrl+F5)'), 'select fallback Ctrl+F5');

/* regressão UX 53×30 ainda no HTML */
ok(page.includes('id="etq-btn-size-53"'), 'botão 53×30');
ok(page.includes('53×30 térmica'), 'dica térmica');
ok(ui.includes('aplicarTamanhoTermicaRapido'), 'atalho tamanho');

console.log(fail ? 'FAIL ' + fail + '/' + n : 'ETQ-53-QUOTA OK ' + n + '/' + n);
process.exit(fail ? 1 : 0);
