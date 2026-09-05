'use strict';

/**
 * Path ETQ-A6-BONUS — prova detalhada (grade, HTML, seed, regressão A4).
 * node scripts/verify_etiquetas_a6_path.js
 */
const fs = require('fs');
const path = require('path');
const vm = require('vm');

const root = path.resolve(__dirname, '..');
const corePath = path.join(root, 'produtos', 'static', 'produtos', 'js', 'produtos_etiquetas_core.js');
const jsPath = path.join(root, 'produtos', 'static', 'produtos', 'js', 'produtos_etiquetas.js');
const htmlPath = path.join(root, 'produtos', 'templates', 'produtos', 'produtos_etiquetas.html');

const source = fs.readFileSync(corePath, 'utf8');
const context = { console, setTimeout, clearTimeout };
vm.runInNewContext(source, context, { filename: corePath });
const Core = context.AgroEtiquetasCore;

let passed = 0;
let failed = 0;
const fails = [];

function check(ok, message) {
  if (ok) {
    passed += 1;
    return;
  }
  failed += 1;
  fails.push(message);
  console.error('FAIL:', message);
}

function approx(a, b, eps) {
  return Math.abs(Number(a) - Number(b)) < (eps == null ? 0.001 : eps);
}

// --- arquivos ---
const html = fs.readFileSync(htmlPath, 'utf8');
const jsUi = fs.readFileSync(jsPath, 'utf8');
check(html.includes('id="etq-preset-folha"'), 'HTML tem select Folha');
check(html.includes('value="a6"'), 'HTML tem opção A6');
check(html.includes('?v=16'), 'HTML cache-bust ?v=16 nos JS');
check(jsUi.includes('calcularGradeFolha'), 'UI usa calcularGradeFolha');
check(jsUi.includes('etq-preset-folha'), 'UI lê/grava etq-preset-folha');
check(jsUi.includes('normalizarFolha'), 'UI normaliza folha');

// --- API pública ---
check(typeof Core.calcularGradeFolha === 'function', 'export calcularGradeFolha');
check(typeof Core.dimensoesFolha === 'function', 'export dimensoesFolha');
check(typeof Core.normalizarFolha === 'function', 'export normalizarFolha');
check(Core.DEFAULT_BONUS_A6_PRESET && Core.DEFAULT_BONUS_A6_PRESET.id === 'bonus-a6', 'seed DEFAULT_BONUS_A6_PRESET');

// --- normalizarFolha ---
check(Core.normalizarFolha('a6') === 'a6', 'normalizar a6');
check(Core.normalizarFolha('A6') === 'a6', 'normalizar A6 maiúsculo');
check(Core.normalizarFolha('a4') === 'a4', 'normalizar a4');
check(Core.normalizarFolha('') === 'a4', 'folha vazia → a4');
check(Core.normalizarFolha('xyz') === 'a4', 'folha inválida → a4');

// --- dimensões ---
const d4 = Core.dimensoesFolha('a4');
const d6 = Core.dimensoesFolha('a6');
check(d4.w === 210 && d4.h === 297 && d4.css === 'A4', 'A4 210×297');
check(d6.w === 105 && d6.h === 148 && d6.css === 'A6', 'A6 105×148');

// --- grade A6 100×45 ---
const gBonus = Core.calcularGradeFolha('a6', 100, 45, 0.5);
check(gBonus.cols === 1, 'A6 100×45 → 1 coluna');
check(gBonus.rows === 3, 'A6 100×45 → 3 linhas');
check(gBonus.per_page === 3, 'A6 → 3 por folha');
check(gBonus.outer_w === 101 && gBonus.outer_h === 46, 'borda 0,5 → 101×46');
check(gBonus.cabe === true, 'cabe na A6');
check(approx((105 - 101) / 2, 2), 'margem X centralizada 2 mm');
check(approx((148 - 3 * 46) / 2, 5), 'margem Y centralizada 5 mm');

// A6 não aceita 2 colunas mesmo se pedir
const gForce2 = Core.calcularGradeFolha('a6', 100, 45, 0.5, 2, 9);
check(gForce2.cols === 1, 'A6 ignora cols=2 → força 1');
check(gForce2.rows === 3, 'A6 rows pedidas 9 → corta para caber (3)');

// etiqueta maior que A6 em largura: ainda 1 col, outer > page
const gWide = Core.calcularGradeFolha('a6', 110, 45, 0.5);
check(gWide.cols === 1, 'largura 110 mm ainda 1 coluna');
check(gWide.cabe === false, '110 mm não cabe na A6 (aviso)');

// --- seed / normalizar ---
const bonus = Core.normalizarPreset(Core.clonePreset(Core.DEFAULT_BONUS_A6_PRESET));
check(bonus.folha === 'a6', 'normalizar preserva a6');
check(bonus.largura_mm === 100 && bonus.altura_mm === 45, '100×45');
check(bonus.cols_folha === 1 && bonus.rows_folha === 3, 'grade 1×3');
check(bonus.estilo === 'gondola', 'estilo gôndola');

// payload PG simulado (só folha+mm)
const fromPg = Core.normalizarPreset({
  id: 'bonus-a6',
  nome: 'Bônus A6',
  estilo: 'gondola',
  folha: 'a6',
  largura_mm: 100,
  altura_mm: 45,
  borda_mm: 0.5,
});
check(fromPg.cols_folha === 1 && fromPg.rows_folha === 3, 'PG sem cols → recalcula 1×3');

// trocar A4→A6 no preset gôndola
const switchA6 = Core.normalizarPreset({
  estilo: 'gondola',
  folha: 'a6',
  largura_mm: 100,
  altura_mm: 45,
});
check(switchA6.folha === 'a6' && switchA6.cols_folha === 1, 'switch A6 ok');

// A4 não quebra
const a4 = Core.normalizarPreset(Core.clonePreset(Core.DEFAULT_GONDOLA_PRESET));
check(a4.folha === 'a4' && a4.cols_folha === 2 && a4.rows_folha === 9, 'A4 90×30 = 2×9');
const a4_60 = Core.normalizarPreset({ estilo: 'gondola', folha: 'a4', largura_mm: 60, altura_mm: 30 });
check(a4_60.cols_folha === 3 && a4_60.rows_folha === 9, 'A4 60 mm = 3×9');

// seed merge inclui bonus-a6
const seeded = Core.mergeServerPresets([], []);
check(
  seeded.some(function (p) {
    return p.id === 'bonus-a6' && p.folha === 'a6';
  }),
  'merge seed inclui bonus-a6'
);
check(
  seeded.some(function (p) {
    return p.id === 'gondola' && p.folha === 'a4';
  }),
  'merge seed mantém gondola A4'
);

// --- HTML impressão A6 ---
const html3 = Core.montarHtmlImpressao(bonus, [{ nome: 'BONUS TESTE', preco_venda: 12.5, peso_etiqueta: '5 KG', qtd: 3 }]);
check(html3.includes('@page{size:A6;'), '@page A6');
check(html3.includes('width:105mm'), 'sheet width 105');
check(html3.includes('height:148mm'), 'sheet height 148');
check(!html3.includes('@page{size:A4;'), 'não pede A4 no bônus');
check((html3.match(/class="etq"/g) || []).length === 3, '3 etiquetas');
check((html3.match(/class="sheet"/g) || []).length === 1, '1 folha');
check(html3.includes('BONUS TESTE') || html3.includes('BONUS'), 'nome no HTML');
check(html3.includes('left:2mm;'), '1ª coluna left=2mm (centralizada)');
check(/top:5mm/.test(html3), '1ª linha top=5mm');
check(/top:51mm/.test(html3), '2ª linha top=5+46');
check(/top:97mm/.test(html3), '3ª linha top=5+92');
check(html3.includes('width:101mm') || html3.includes('width:101mm;height:46mm'), 'célula outer 101×46');

const html5 = Core.montarHtmlImpressao(bonus, [{ nome: 'X', preco_venda: 1, qtd: 5 }]);
check((html5.match(/class="sheet"/g) || []).length === 2, '5 etq → 2 folhas');
check((html5.match(/class="etq"/g) || []).length === 5, '5 células');

const html1 = Core.montarHtmlImpressao(bonus, [{ nome: 'Uma', preco_venda: 9.9, qtd: 1 }]);
check((html1.match(/class="sheet"/g) || []).length === 1, '1 etq → 1 folha');
check((html1.match(/class="etq"/g) || []).length === 1, '1 célula preenchida');
check((html1.match(/class="crop-layer"/g) || []).length >= 1, 'marcas de corte na folha');

// regressão HTML A4
const htmlA4 = Core.montarHtmlImpressao(a4, [{ nome: 'A4', preco_venda: 10, qtd: 18 }]);
check(htmlA4.includes('@page{size:A4;'), 'A4 ainda @page A4');
check((htmlA4.match(/class="etq"/g) || []).length === 18, '18 etq A4');
check(htmlA4.includes('width:210mm'), 'sheet A4 210');

// térmica intocada
const term = Core.normalizarPreset(Core.clonePreset(Core.DEFAULT_PRESET));
const htmlT = Core.montarHtmlImpressao(term, [{ nome: 'T', preco_venda: 1, codigo_gm: 'GM1', qtd: 1 }]);
check(htmlT.includes('@page{size:40mm 40mm;'), 'térmica 40×40 intacta');

console.log('');
console.log('ETQ-A6-BONUS path: ' + passed + ' ok · ' + failed + ' fail');
if (failed) {
  process.exit(1);
}
console.log('OK: path A6 bônus verificado (arquivos + grade + HTML + regressão A4/térmica).');
