/**
 * Quebra do nome na térmica: palavra inteira desce, sem reticências.
 * Centavos com tamanho próprio. Chrome abre o HTML da impressão.
 * node scripts/verify_etiquetas_termica_quebra.js
 */
'use strict';

const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawnSync } = require('child_process');
const vm = require('vm');

const root = path.resolve(__dirname, '..');
const source = fs.readFileSync(
  path.join(root, 'produtos/static/produtos/js/produtos_etiquetas_core.js'),
  'utf8'
);
const context = { console, setTimeout, clearTimeout };
vm.runInNewContext(source, context, { filename: 'core.js' });
const Core = context.AgroEtiquetasCore;

let n = 0;
let fail = 0;
function ok(cond, msg) {
  n += 1;
  if (!cond) {
    fail += 1;
    console.log('FAIL', msg);
  }
}

function chromeBin() {
  const cands = [
    process.env.CHROME_PATH,
    'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
    'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe',
  ];
  for (let i = 0; i < cands.length; i++) {
    if (cands[i] && fs.existsSync(cands[i])) return cands[i];
  }
  return '';
}

const nome = 'sache special cat adulto cordeiro 85g';
const preset = Core.normalizarPreset({
  estilo: 'termica',
  largura_mm: 40,
  altura_mm: 40,
  nome_linhas: 4,
  nome_pt_1: 18,
  nome_pt_2: 14,
  nome_pt_3: 12,
  nome_pt_4: 10,
  preco_pt: 40,
  centavos_pt: 14,
  layout: {
    nome: { x: 2, y: 2, w: 96, h: 28 },
    preco: { x: 2, y: 30, w: 96, h: 28 },
    barcode: { x: 6, y: 58, w: 88, h: 22 },
    gm: { x: 2, y: 80, w: 48, h: 10 },
    rodape: { x: 50, y: 80, w: 48, h: 10 },
  },
});
const html = Core.montarHtmlImpressao(
  preset,
  [
    {
      nome: nome,
      preco_venda: 3.5,
      codigo_gm: 'GM1793',
      codigo_barras: '7891234567895',
      qtd: 1,
    },
  ],
  'Gm Agro Mais'
);

ok(/\.preco-int\{[^}]*font-size:40pt/.test(html), 'reais em 40pt');
ok(/\.preco-cent\{[^}]*font-size:14pt/.test(html), 'centavos em 14pt');
ok(html.includes('class="preco-int">3<'), 'real 3');
ok(html.includes('class="preco-cent">,50<'), 'centavos ,50');
ok(!html.includes('...'), 'html nao traz tres pontinhos');
ok(!html.includes('webkitLineClamp'), 'sem corte por linha com reticencias');

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'etq-quebra-'));
const htmlFile = path.join(dir, 'etiqueta.html');
fs.writeFileSync(htmlFile, html, 'utf8');
const bin = chromeBin();
ok(!!bin, 'chrome encontrado');
if (!bin) {
  console.log('FAIL ' + fail + '/' + n);
  process.exit(1);
}

const url = 'file:///' + htmlFile.replace(/\\/g, '/');
const run = spawnSync(
  bin,
  ['--headless=new', '--disable-gpu', '--virtual-time-budget=2500', '--dump-dom', url],
  { timeout: 30000, windowsHide: true, encoding: 'utf8', maxBuffer: 8 * 1024 * 1024 }
);
const dom = String(run.stdout || '');
ok(dom.includes('data-nome="' + nome + '"'), 'nome original guardado');

const bloco = (dom.match(/class="slot slot-nome"[\s\S]*?<\/div>\s*<\/div>/) || [''])[0];
const linhas = [];
const reLinha = /<div[^>]*>([^<]*)<\/div>/g;
let m;
while ((m = reLinha.exec(bloco))) {
  const t = m[1].replace(/\s+/g, ' ').trim();
  if (t) linhas.push(t);
}
ok(linhas.length >= 2, 'nome quebrou em mais de uma linha · ' + linhas.join(' | '));
ok(linhas.join(' ') === nome, 'as linhas juntas sao o nome inteiro');
ok(!linhas.some(function (l) { return l.indexOf('...') >= 0 || l.indexOf('…') >= 0; }), 'nenhuma linha com reticencias');

const palavras = nome.split(' ');
const vistas = linhas.join(' ').split(' ');
ok(vistas.join(' ') === palavras.join(' '), 'nenhuma palavra foi cortada no meio');
palavras.forEach(function (p) {
  ok(linhas.some(function (l) { return (' ' + l + ' ').indexOf(' ' + p + ' ') >= 0; }), 'palavra inteira: ' + p);
});

const svg = (dom.match(/<svg[\s\S]*?<\/svg>/i) || [''])[0];
ok(svg.length > 40, 'codigo de barras desenhado');
ok(/marginLeft|margin-left|rect /.test(svg) || svg.indexOf('<rect') >= 0, 'barras viraram tracos');

console.log(linhas.length ? 'linhas: ' + linhas.join(' | ') : 'sem linhas');
console.log(fail ? 'FAIL ' + fail + '/' + n : 'OK ' + n + '/' + n);
process.exit(fail ? 1 : 0);
