/**
 * Path ETQ-PRESET-SYNC — alteração de preset aparece nos outros PCs.
 * node scripts/verify_etq_preset_sync_path.js
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

let n = 0;
let fail = 0;
function ok(cond, msg) {
  n += 1;
  if (!cond) {
    fail += 1;
    console.log('FAIL', msg);
  }
}

ok(page.includes("?v=34"), 'core cache v=34');
ok(coreCode.includes('function refreshPresetsFromServer'), 'core tem refreshPresetsFromServer');
ok(page.includes("produtos_etiquetas.js' %}?v=33"), 'ui cache v=33');
ok(coreCode.includes('Postgres manda') || coreCode.includes('servidor vence'), 'merge servidor manda');
ok(coreCode.includes('!onServer[p.id]'), 'migrate não sobrescreve PG');
ok(coreCode.includes("redirect: 'manual'"), 'fetch detecta login redirect');
ok(ui.includes('Arrastar posição tem que ir pro Postgres'), 'drag synca loja');
ok(ui.includes('syncPresetToServer(p, { silent: true })'), 'reset/drag sync');
ok(ui.includes('Presets da loja atualizados'), 'status ao puxar loja');
ok(ui.includes('migrateLocalPresetsToServerOnce(state.storage.presets, { onServer: onServer })'), 'migrate com onServer');
ok(ui.includes('function endDrag'), 'endDrag existe');
ok(ui.includes('function resetLayoutAtivo'), 'resetLayoutAtivo existe');
ok(ui.includes('function syncPresetToServer'), 'syncPresetToServer existe');
ok(ui.includes('function salvarPresetAtual'), 'salvarPresetAtual existe');
ok(coreCode.includes('upsertPresetToServer'), 'upsert no core');
ok((coreCode.match(/redirect:\s*'manual'/g) || []).length >= 2, 'fetch+upsert redirect manual');

const endIdx = ui.indexOf('function endDrag');
const endBlock = ui.slice(endIdx, endIdx + 900);
ok(endBlock.includes('syncPresetToServer'), 'endDrag chama sync');
const resetIdx = ui.indexOf('function resetLayoutAtivo');
const resetBlock = ui.slice(resetIdx, resetIdx + 900);
ok(resetBlock.includes('syncPresetToServer'), 'resetLayout chama sync');
const loadIdx = ui.indexOf('function carregarPresetsDaLoja');
const loadBlock = ui.slice(loadIdx, loadIdx + 1800);
ok(loadBlock.includes('onServer'), 'carregarPresets passa onServer');
ok(loadBlock.includes('mergeServerPresets'), 'carregarPresets faz merge');

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
  },
};
s.window = s;
vm.createContext(s);
vm.runInContext(coreCode, s);
const Core = s.AgroEtiquetasCore;
ok(!!Core, 'Core carrega');

const localOld = Core.mergeServerPresets([], []);
const padrao = localOld.find((p) => p.id === 'padrao-53x30');
ok(!!padrao, 'seed 53×30 local');
padrao.nome = 'LOCAL VELHO';
padrao.preco_pt = 11;

const serverNew = [
  Object.assign({}, padrao, { nome: 'LOJA NOVO', preco_pt: 22, _atualizado_em: '2026-10-03' }),
];
const merged = Core.mergeServerPresets(
  localOld.map((p) => (p.id === 'padrao-53x30' ? padrao : p)),
  serverNew
);
const got = merged.find((p) => p.id === 'padrao-53x30');
ok(got && got.nome === 'LOJA NOVO', 'merge: loja vence nome');
ok(got && Number(got.preco_pt) === 22, 'merge: loja vence preco_pt');

const onlyLocal = Core.mergeServerPresets(
  [{ id: 'preset-xyz', nome: 'Só neste PC', estilo: 'termica', largura_mm: 40, altura_mm: 40 }],
  serverNew
);
ok(onlyLocal.some((p) => p.id === 'preset-xyz'), 'merge mantém preset só local');
ok(onlyLocal.some((p) => p.id === 'padrao-53x30' && p.nome === 'LOJA NOVO'), 'e ainda aplica loja');

console.log(fail ? 'FAIL ' + fail + '/' + n : 'ETQ-PRESET-SYNC OK ' + n + '/' + n);
process.exit(fail ? 1 : 0);
