/**
 * Path ETQ-PRESET-ESPELHO — gestão/NF/PDV puxam a mesma lista do Postgres.
 * node scripts/verify_etq_preset_espelho_path.js
 */
const fs = require('fs');
const path = require('path');
const vm = require('vm');

const root = path.join(__dirname, '..');
const coreCode = fs.readFileSync(
  path.join(root, 'produtos/static/produtos/js/produtos_etiquetas_core.js'),
  'utf8'
);
const cad = fs.readFileSync(
  path.join(root, 'produtos/static/produtos/js/cadastro_erp_panel.js'),
  'utf8'
);
const pdv = fs.readFileSync(
  path.join(root, 'produtos/static/produtos/js/pdv_wizard.js'),
  'utf8'
);
const nfe = fs.readFileSync(
  path.join(root, 'produtos/templates/produtos/entrada_nota.html'),
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
const cadHtml = fs.readFileSync(
  path.join(root, 'produtos/templates/produtos/produtos_cadastro_erp.html'),
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

ok(coreCode.includes('function refreshPresetsFromServer'), 'core tem refreshPresetsFromServer');
ok(coreCode.includes('refreshPresetsFromServer: refreshPresetsFromServer'), 'core exporta refresh');
ok(coreCode.includes('mergeServerPresets(st.presets, serverList'), 'refresh faz merge');
ok(coreCode.includes('saveStorage(st)'), 'refresh grava cache');

ok(cad.includes('refreshPresetsFromServer'), 'cadastro chama refresh');
ok(cad.includes('Core.fillPresetSelect(cadEtqPreset)'), 'cadastro preenche select');
const cadOpen = cad.indexOf('function abrirModalEtiquetaCadastro');
const cadBlock = cad.slice(cadOpen, cadOpen + 4500);
ok(cadBlock.includes('refreshPresetsFromServer'), 'abrir modal cadastro faz refresh');
ok(cadBlock.includes('fillPresetSelect'), 'abrir modal cadastro preenche');

ok(pdv.includes('refreshPresetsFromServer'), 'PDV chama refresh');
const pdvOpen = pdv.indexOf('function openPdvPeEtqModal');
const pdvBlock = pdv.slice(pdvOpen > 0 ? pdvOpen : pdv.indexOf('peEtqPreset'), pdv.indexOf('peEtqPreset') + 2500);
ok(
  pdv.includes('Core.refreshPresetsFromServer().then') ||
    pdvBlock.includes('refreshPresetsFromServer'),
  'PDV modal etiqueta usa refresh'
);

ok(nfe.includes('refreshPresetsFromServer'), 'entrada NF chama refresh');
ok(nfe.includes('nfe-etq-preset'), 'entrada NF tem select preset');

ok(page.includes("produtos_etiquetas_core.js' %}?v=31"), 'etiquetas core v=31');
ok(lote.includes("produtos_etiquetas_core.js' %}?v=31"), 'lote core v=31');
ok(cadHtml.includes("produtos_etiquetas_core.js' %}?v=31"), 'cadastro core v=31');
ok(nfe.includes("produtos_etiquetas_core.js' %}?v=31"), 'entrada NF core v=31');
ok(cadHtml.includes("cadastro_erp_panel.js' %}?v=30"), 'cadastro panel v=30');

const store = {};
const calls = [];
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
  fetch: function () {
    calls.push('fetch');
    return Promise.resolve({
      status: 200,
      ok: true,
      type: 'basic',
      headers: { get: function () { return 'application/json'; } },
      json: function () {
        return Promise.resolve({
          ok: true,
          presets: [
            {
              id: 'custom-loja',
              nome: 'REMEDIOS',
              estilo: 'termica',
              largura_mm: 53,
              altura_mm: 30,
            },
          ],
        });
      },
    });
  },
};
s.window = s;
vm.createContext(s);
vm.runInContext(coreCode, s);
const Core = s.AgroEtiquetasCore;
ok(!!Core && typeof Core.refreshPresetsFromServer === 'function', 'Core.refreshPresetsFromServer runtime');

Core.refreshPresetsFromServer().then(function (list) {
  ok(calls.length >= 1, 'refresh dispara fetch');
  ok(Array.isArray(list) && list.some(function (p) { return p.id === 'custom-loja'; }), 'refresh traz custom da loja');
  const st = Core.loadStorage();
  ok(st.presets.some(function (p) { return p.id === 'custom-loja'; }), 'cache local guarda custom');
  console.log(fail ? 'FAIL ' + fail + '/' + n : 'ETQ-PRESET-ESPELHO OK ' + n + '/' + n);
  process.exit(fail ? 1 : 0);
}).catch(function (e) {
  console.log('FAIL runtime', e && e.message);
  process.exit(1);
});
