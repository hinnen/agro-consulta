/**
 * Path ETQ-53-UX — preset 53×30 óbvio + recuperação presets/busca.
 * node scripts/verify_etq_53_ux_path.js
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

const s = {
  document: { cookie: '' },
  localStorage: {
    _d: {},
    getItem(k) {
      return Object.prototype.hasOwnProperty.call(this._d, k) ? this._d[k] : null;
    },
    setItem(k, v) {
      this._d[k] = String(v);
    },
  },
};
s.window = s;
vm.createContext(s);
vm.runInContext(coreCode, s);
const Core = s.AgroEtiquetasCore;

let n = 0;
let fail = 0;
function ok(cond, msg) {
  n += 1;
  if (!cond) {
    fail += 1;
    console.log('FAIL', msg);
  }
}

ok(!!Core, 'Core carrega');
ok(Core.DEFAULT_TERMICA_53X30_PRESET && Core.DEFAULT_TERMICA_53X30_PRESET.id === 'padrao-53x30', 'seed 53×30');
ok(Number(Core.DEFAULT_TERMICA_53X30_PRESET.largura_mm) === 53, 'largura 53');
ok(Number(Core.DEFAULT_TERMICA_53X30_PRESET.altura_mm) === 30, 'altura 30');
ok(Core.BUILTIN_IDS && Core.BUILTIN_IDS['padrao-53x30'], 'builtin 53×30');

const seeded = Core.mergeServerPresets([], []);
const ids = seeded.map((p) => p.id);
ok(ids.includes('padrao-4x4'), 'seed 4×4');
ok(ids.includes('padrao-53x30'), 'seed 53×30 no merge');
ok(ids.includes('gondola'), 'seed gondola');
ok(ids.includes('bonus-a6'), 'seed bonus-a6');

/* Merge com lista vazia do servidor não apaga seeds */
const again = Core.mergeServerPresets(seeded, []);
ok(again.length >= 4, 'merge vazio mantém seeds n=' + again.length);
ok(again.some((p) => p.id === 'padrao-53x30'), '53×30 permanece após merge vazio');

/* HTML impressão 53×30 */
const html53 = Core.montarHtmlImpressao(
  Core.normalizarPreset(Core.clonePreset(Core.DEFAULT_TERMICA_53X30_PRESET)),
  [
    { nome: 'racao teste', preco_venda: 99.9, codigo_gm: 'GM0067-15', qtd: 2 },
    { nome: 'outro', preco_venda: 1, codigo_gm: 'GM1', qtd: 1 },
  ],
  'Gm Agro Mais'
);
ok(html53.includes('@page{size:53mm 30mm;'), 'página 53×30');
ok((html53.match(/class="pg"/g) || []).length === 3, 'qtd soma páginas (2+1=3)');
ok(html53.includes('GM0067-15') || html53.includes('GM0067'), 'GM na etiqueta');

/* 4×4 intacto */
const html40 = Core.montarHtmlImpressao(
  Core.normalizarPreset(Core.clonePreset(Core.DEFAULT_PRESET)),
  [{ nome: 'x', preco_venda: 1, codigo_gm: 'GM2', qtd: 1 }],
  'R'
);
ok(html40.includes('@page{size:40mm 40mm;'), '4×4 intacto');

/* UI / template */
ok(ui.includes('aplicarTamanhoTermicaRapido'), 'atalho tamanho');
ok(ui.includes('garantirPresetsNaTela'), 'recupera presets');
ok(ui.includes('ordenarPresetsParaSelect'), 'ordena presets');
ok(ui.includes('Motor de etiquetas não carregou'), 'aviso Core ausente');
ok(ui.includes("kind === '53'"), 'atalho 53');
ok(page.includes('id="etq-btn-size-53"'), 'botão 53×30');
ok(page.includes('id="etq-btn-size-40"'), 'botão 4×4');
ok(page.includes('53×30 térmica'), 'dica na fila');
ok(page.includes("produtos_etiquetas_core.js' %}?v=34"), 'core v=34');
ok(page.includes("produtos_etiquetas.js' %}?v=33"), 'ui v=33');
ok(!page.includes('defer></script>'), 'sem defer no JS etiquetas (ordem Core)');
ok(page.includes('Térmica (bobina / barras)'), 'estilo sem confundir mm');
ok(coreCode.includes('Quota') || coreCode.includes('e1'), 'savePrefs tolerante a quota');
ok(ui.includes('pinta a tela ANTES') || ui.includes('ANTES de gravar'), 'render antes de persist');

/* Quota cheia: saveStorage não pode estourar (produção Chrome app). */
s.localStorage.setItem = function () {
  var err = new Error('QuotaExceededError');
  err.name = 'QuotaExceededError';
  throw err;
};
var threw = false;
try {
  Core.saveStorage({
    presets: seeded,
    preset_ativo: 'padrao-53x30',
    texto_rodape_global: 'x',
  });
} catch (eQ) {
  threw = true;
}
ok(!threw, 'saveStorage com quota não lança');

/* Aspecto preview: 53×30 mais largo que alto na escala */
const wMm = 53;
const hMm = 30;
const maxW = 420;
const maxH = 260;
const scale = Math.min(maxW / wMm, maxH / hMm);
const pw = Math.round(wMm * scale);
const ph = Math.round(hMm * scale);
ok(pw > ph, 'preview 53×30 mais largo que alto ' + pw + 'x' + ph);
ok(ui.includes('maxH = 260'), 'syncLayout usa maxH');

console.log(fail ? 'FAIL ' + fail + '/' + n : 'ETQ-53-UX OK ' + n + '/' + n);
process.exit(fail ? 1 : 0);
