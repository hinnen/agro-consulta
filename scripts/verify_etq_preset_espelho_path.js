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
const pdvHtml = fs.readFileSync(
  path.join(root, 'produtos/templates/produtos/pdv_wizard.html'),
  'utf8'
);
const ui = fs.readFileSync(
  path.join(root, 'produtos/static/produtos/js/produtos_etiquetas.js'),
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

/* --- Core helper --- */
ok(coreCode.includes('function refreshPresetsFromServer'), 'core tem refreshPresetsFromServer');
ok(coreCode.includes('refreshPresetsFromServer: refreshPresetsFromServer'), 'core exporta refresh');
ok(coreCode.includes('mergeServerPresets(st.presets, serverList'), 'refresh faz merge');
ok(coreCode.includes('saveStorage(st)'), 'refresh grava cache');
ok(coreCode.includes('Postgres manda') || coreCode.includes('servidor vence'), 'merge: loja manda');
ok(coreCode.includes('!onServer[p.id]'), 'migrate não sobrescreve PG');
ok(coreCode.includes("redirect: 'manual'"), 'fetch detecta login redirect');

const refreshIdx = coreCode.indexOf('function refreshPresetsFromServer');
const refreshBlock = coreCode.slice(refreshIdx, refreshIdx + 900);
ok(refreshBlock.includes('fetchPresetsFromServer()'), 'refresh chama fetch');
ok(refreshBlock.includes('.catch('), 'refresh tolerante a falha');
ok(refreshBlock.includes('loadStorage().presets'), 'refresh fallback = cache local');

/* --- Cadastro (gestão) --- */
ok(cad.includes('refreshPresetsFromServer'), 'cadastro chama refresh');
ok(cad.includes('Core.fillPresetSelect(cadEtqPreset)'), 'cadastro preenche select');
const cadOpen = cad.indexOf('function abrirModalEtiquetaCadastro');
ok(cadOpen >= 0, 'abrirModalEtiquetaCadastro existe');
const cadBlock = cad.slice(cadOpen, cadOpen + 4500);
ok(cadBlock.includes('refreshPresetsFromServer'), 'abrir modal cadastro faz refresh');
ok(cadBlock.includes('fillPresetSelect(cadEtqPreset, null, list)'), 'cadastro preenche com lista da API');
const fill1 = cadBlock.indexOf('fillPresetSelect');
const refreshCad = cadBlock.indexOf('refreshPresetsFromServer');
const fill2 = cadBlock.indexOf('fillPresetSelect', refreshCad);
ok(fill1 >= 0 && refreshCad > fill1, 'cadastro: fill cache ANTES do refresh');
ok(fill2 > refreshCad, 'cadastro: fill de novo DEPOIS do refresh');

/* --- PDV --- */
ok(pdv.includes('refreshPresetsFromServer'), 'PDV chama refresh');
const pdvOpen = pdv.indexOf('function openPdvPeEtqModal');
ok(pdvOpen >= 0, 'openPdvPeEtqModal existe');
const pdvBlock = pdv.slice(pdvOpen, pdvOpen + 2800);
ok(pdvBlock.includes('refreshPresetsFromServer'), 'PDV modal etiqueta usa refresh');
ok(pdvBlock.includes('fillSelect(list)'), 'PDV preenche com lista da API');
ok(pdvHtml.includes('pdv-pe-etq-preset'), 'PDV HTML tem select preset');
ok(pdvHtml.includes('produtos_etiquetas_core.js'), 'PDV HTML puxa core');

/* --- Entrada NF --- */
ok(nfe.includes('refreshPresetsFromServer'), 'entrada NF chama refresh');
ok(nfe.includes('nfe-etq-preset'), 'entrada NF tem select preset');
ok(nfe.includes('function entradaNfeEtiquetasPreencherPresets'), 'NF tem helper presets');
ok(nfe.includes('migrateLocalPresetsToServerOnce'), 'NF sobe preset só do PC');
ok(nfe.includes("fillPresetSelect(selPreset, null, list)"), 'NF preenche com lista da API');
ok(nfe.includes("nfe-etq-preset')?.addEventListener('focus'"), 'NF refresh no foco do preset');
const nfeRender = nfe.indexOf('function entradaNfeEtiquetasRenderLista');
const nfeBlock = nfe.slice(nfeRender, nfeRender + 800);
ok(nfeBlock.includes('entradaNfeEtiquetasPreencherPresets'), 'render lista NF chama helper');

/* --- Fila ainda sincroniza loja --- */
ok(ui.includes('function carregarPresetsDaLoja'), 'fila carrega presets da loja');
ok(ui.includes('mergeServerPresets'), 'fila faz merge');

/* --- Core: memória + não zerar na quota --- */
ok(coreCode.includes('var _presetsMem'), 'core tem memória de presets');
ok(coreCode.includes('NUNCA zerar presets') || coreCode.includes('keep'), 'quota não zera presets');
ok(!/presets:\s*\[\s*\]/.test(coreCode.replace(/\/\*[\s\S]*?\*\//g, '')), 'core sem presets:[] na quota');
ok(coreCode.includes('function fillPresetSelect(selectEl, activeId, presetsOpt)'), 'fill aceita lista');

/* --- Cache-bust --- */
ok(page.includes("produtos_etiquetas_core.js' %}?v=34"), 'etiquetas core v=34');
ok(lote.includes("produtos_etiquetas_core.js' %}?v=34"), 'lote core v=34');
ok(cadHtml.includes("produtos_etiquetas_core.js' %}?v=34"), 'cadastro core v=34');
ok(nfe.includes("produtos_etiquetas_core.js' %}?v=34"), 'entrada NF core v=34');
ok(cadHtml.includes("cadastro_erp_panel.js' %}?v=31"), 'cadastro panel v=31');

/* --- Runtime: merge + refresh --- */
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
              preco_pt: 40,
            },
            {
              id: 'padrao-53x30',
              nome: '53 LOJA',
              estilo: 'termica',
              largura_mm: 53,
              altura_mm: 30,
              preco_pt: 55,
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

/* local velho no mesmo id — loja deve vencer */
const local = Core.mergeServerPresets([], []);
const local53 = local.find((p) => p.id === 'padrao-53x30');
if (local53) {
  local53.nome = 'LOCAL VELHO';
  local53.preco_pt = 11;
  Core.saveStorage({ presets: local, preset_ativo: 'padrao-53x30' });
}

Core.refreshPresetsFromServer()
  .then(function (list) {
    ok(calls.length >= 1, 'refresh dispara fetch');
    ok(
      Array.isArray(list) && list.some(function (p) { return p.id === 'custom-loja'; }),
      'refresh traz custom da loja'
    );
    const st = Core.loadStorage();
    ok(st.presets.some(function (p) { return p.id === 'custom-loja'; }), 'cache local guarda custom');
    const got53 = st.presets.find(function (p) { return p.id === 'padrao-53x30'; });
    ok(got53 && got53.nome === '53 LOJA', 'refresh: loja vence nome do 53×30');
    ok(got53 && Number(got53.preco_pt) === 55, 'refresh: loja vence preco_pt');

    /* falha de rede: não apaga cache */
    s.fetch = function () {
      return Promise.reject(new Error('offline'));
    };
    return Core.refreshPresetsFromServer().then(function (again) {
      ok(
        again.some(function (p) { return p.id === 'custom-loja'; }),
        'offline: mantém custom no cache'
      );

      /* Chrome “vazio” (quota): fill ainda usa lista/memória da API */
      Object.keys(store).forEach(function (k) {
        delete store[k];
      });
      store[Core.LS_KEY] = JSON.stringify({
        presets: [],
        preset_ativo: 'padrao-4x4',
        texto_rodape_global: '',
      });
      const sel = {
        innerHTML: '',
        value: '',
      };
      Core.fillPresetSelect(sel, null, again);
      ok(sel.innerHTML.indexOf('custom-loja') >= 0, 'fill com lista da API mostra custom');
      ok(sel.innerHTML.indexOf('REMEDIOS') >= 0, 'fill mostra nome REMEDIOS');
      const fromMem = Core.loadStorage();
      ok(
        fromMem.presets.some(function (p) { return p.id === 'custom-loja'; }),
        'memória cobre Chrome vazio'
      );

      console.log(fail ? 'FAIL ' + fail + '/' + n : 'ETQ-PRESET-ESPELHO OK ' + n + '/' + n);
      process.exit(fail ? 1 : 0);
    });
  })
  .catch(function (e) {
    console.log('FAIL runtime', e && e.message);
    process.exit(1);
  });
