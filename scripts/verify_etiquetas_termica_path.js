/**
 * Etiqueta térmica: várias por fila + layout (posição, linhas, moldura, liga/desliga).
 * node scripts/verify_etiquetas_termica_path.js
 */
const fs = require('fs');
const path = require('path');
const vm = require('vm');

const root = path.join(__dirname, '..');
const code = fs.readFileSync(
  path.join(root, 'produtos/static/produtos/js/produtos_etiquetas_core.js'),
  'utf8'
);
const s = { document: { cookie: '' } };
s.window = s;
vm.createContext(s);
vm.runInContext(code, s);
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

const itens = [
  { nome: 'higiene alfafa feno 1 kg roedores jaal', preco_venda: 37.9, codigo_gm: 'GM0836', qtd: 1 },
  { nome: 'higiene b', preco_venda: 5, codigo_gm: 'GM0837', qtd: 1 },
  { nome: 'higiene c nome bem comprido para cair em varias linhas', preco_venda: 5.5, codigo_gm: 'GM0838', qtd: 1 },
];

const base = Core.normalizarPreset({
  estilo: 'termica',
  largura_mm: 100,
  altura_mm: 70,
  nome_pt: 14,
  preco_pt: 36,
  codigo_pt: 11,
  rodape_pt: 10,
  texto_rodape: 'Gm Agro Mais',
});
ok(base.nome_linhas === 2, 'linhas padrao 2');
ok(base.layout && base.layout.barcode, 'layout padrao tem barras');
ok(base.borda_mm === 0, 'sem moldura se nao pediu');
ok(base.centavos_pt === 36, 'centavos comeca igual ao real');
ok(base.show_gm === true, 'GM ligado por padrao');

const html3 = Core.montarHtmlImpressao(base, itens, 'Gm Agro Mais');
ok((html3.match(/class="pg"/g) || []).length === 3, '3 produtos = 3 paginas');
ok(!/html,body\{[^}]*overflow:hidden/.test(html3), 'body nao corta o resto');
ok(html3.includes('page-break-before:always'), 'quebra entre etiquetas');
ok(html3.includes('class="slot slot-nome"'), 'nome posicionado');
ok(html3.includes('class="slot slot-barcode"'), 'barras posicionadas');
ok((html3.match(/GM0836/g) || []).length >= 1, 'codigo da 1');
ok((html3.match(/GM0838/g) || []).length >= 1, 'codigo da 3');
ok(html3.includes('MAX=2'), 'ajuste de nome usa no maximo 2 linhas');
ok(html3.includes('overflow-wrap:normal'), 'palavra inteira desce');
ok(!html3.includes('webkitLineClamp'), 'sem tres pontinhos');
ok(html3.includes('class="preco-cent"'), 'centavos separado do real');
ok(html3.includes('marginLeft'), 'barras tem zona quieta pro leitor');
ok(html3.includes('shape-rendering:crispEdges'), 'barras sem borrar');
ok(!html3.includes('max-width:100%'), 'svg barras sem encolher no flex');
ok(html3.includes('flat:true'), 'jsbarcode flat');
ok(html3.includes('_fixSvg'), 'fixa px do svg pos render');
const barsJson = html3.match(/var _bars=(\[[\s\S]*?\]);function _eanBits/);
if (barsJson) {
  try {
    const bars = JSON.parse(barsJson[1]);
    ok(bars.length >= 1, 'dados das barras');
    ok(Number(bars[0].bw) >= 1.35, 'modulo largo o bastante bw=' + bars[0].bw);
    ok(Number(bars[0].bh) >= 32, 'altura barras bh=' + bars[0].bh);
    ok(Number(bars[0].mq) >= 14, 'quiet zone mq=' + bars[0].mq);
  } catch (e) {
    ok(false, 'parse _bars: ' + e.message);
  }
} else {
  ok(false, 'bloco _bars no html');
}

const custom = Core.normalizarPreset({
  estilo: 'termica',
  largura_mm: 80,
  altura_mm: 50,
  nome_linhas: 4,
  nome_pt_1: 18,
  nome_pt_2: 14,
  nome_pt_3: 11,
  nome_pt_4: 9,
  borda_mm: 1.5,
  show_barcode: false,
  show_rodape: true,
  show_gm: true,
  show_nome: true,
  show_preco: true,
  cores: { borda: '#ff0000', nome_fg: '#123456', fundo: '#ffffff' },
  layout: {
    nome: { x: 10, y: 5, w: 80, h: 40 },
    preco: { x: 10, y: 46, w: 80, h: 20 },
    gm: { x: 10, y: 70, w: 80, h: 12 },
    rodape: { x: 10, y: 84, w: 80, h: 12 },
  },
  preco_pt: 40,
  codigo_pt: 12,
  rodape_pt: 9,
});
ok(custom.layout.barcode, 'completa caixa de barras que faltou');
ok(custom.nome_linhas === 4, 'aceita 4 linhas');
const htmlC = Core.montarHtmlImpressao(custom, [itens[0]], 'Loja');
ok(htmlC.includes('left:10%'), 'posicao do nome arrastada');
ok(htmlC.includes('border:1.5mm solid'), 'moldura');
ok(htmlC.includes('#ff0000') || htmlC.includes('#FF0000'), 'cor da moldura');
ok(!htmlC.includes('class="slot slot-barcode"'), 'barras desligadas nao saem');
ok(htmlC.includes('MAX=4'), 'script respeita 4 linhas');
ok(htmlC.includes('PTS=[18,14,11,9]'), 'fonte por quantidade de linhas');
ok((htmlC.match(/class="pg"/g) || []).length === 1, 'um item = uma pagina');

const qtd = Core.montarHtmlImpressao(base, [{ nome: 'x', preco_venda: 1, codigo_gm: 'GM1', qtd: 3 }], 'R');
ok((qtd.match(/class="pg"/g) || []).length === 3, 'qtd 3 = 3 etiquetas');

const g = Core.montarHtmlImpressao(
  { id: 'gondola', estilo: 'gondola', folha: 'a6', largura_mm: 100, altura_mm: 45 },
  itens,
  ''
);
ok((g.match(/class="sheet"/g) || []).length === 1, 'gondola A6 ainda junta 3 na folha');
ok(!g.includes('class="pg"'), 'gondola nao usa pagina de termica');

const page = fs.readFileSync(
  path.join(root, 'produtos/templates/produtos/produtos_etiquetas.html'),
  'utf8'
);
const ui = fs.readFileSync(
  path.join(root, 'produtos/static/produtos/js/produtos_etiquetas.js'),
  'utf8'
);
ok(page.includes('id="etq-layout-stage-termica"'), 'tela tem palco para arrastar');
ok(page.includes('id="etq-preset-nome-linhas"'), 'tela tem maximo de linhas');
ok(page.includes('id="etq-preset-centavos-pt"'), 'tela tem tamanho dos centavos');
ok(page.includes('id="etq-preset-borda-mm"'), 'tela tem moldura');
ok(page.includes('id="etq-term-show-barcode"'), 'tela liga/desliga barras');
ok(ui.includes('DEFAULT_TERMICA_LAYOUT'), 'reset usa layout termico');
ok(ui.includes('etq-layout-stage-termica'), 'form grava o palco termico');
ok(ui.includes('enviarBuiltinsFaltantes'), 'sobe seed novo pro Postgres');
ok(ui.includes('aplicarTamanhoTermicaRapido'), 'atalho tamanho 4x4 / 53x30');
ok(ui.includes('garantirPresetsNaTela'), 'recupera presets vazios');
ok(page.includes('id="etq-btn-size-53"'), 'botao rapido 53x30');
ok(page.includes('53×30 térmica'), 'dica preset 53×30 na fila');

const seed53 = Core.DEFAULT_TERMICA_53X30_PRESET;
ok(seed53 && seed53.id === 'padrao-53x30', 'seed 53×30 existe');
ok(Number(seed53.largura_mm) === 53 && Number(seed53.altura_mm) === 30, 'seed 53×30 mm');
ok(seed53.estilo === 'termica', 'seed 53×30 termica');
const seeded = Core.mergeServerPresets([], []);
ok(
  seeded.some(function (p) {
    return p.id === 'padrao-53x30';
  }),
  'merge seed inclui 53×30'
);
const html53 = Core.montarHtmlImpressao(Core.normalizarPreset(seed53), [
  { nome: 'teste 53x30', preco_venda: 12.9, codigo_gm: 'GM100', qtd: 1 },
], 'Gm Agro Mais');
ok(html53.includes('@page{size:53mm 30mm;'), 'página térmica 53×30');
ok((html53.match(/class="pg"/g) || []).length === 1, '53×30 = 1 página por etiqueta');

/* 230 legado: imprime EAN-13 com DV GS1 (cadastro mantém número legado). */
const legado = Core.valorBarcodeProduto({
  nome: 'teste',
  preco_venda: 1,
  codigo_barras: '2300000001571',
  codigo_gm: 'GM1',
});
ok(legado.formato === 'EAN13', 'legado 230 formato EAN13');
ok(legado.valor === '2300000001570', 'legado 230 DV corrigido na etiqueta');
ok(legado.ean_corrigido === true, 'legado 230 ean_corrigido');
ok(legado.valor_original === '2300000001571', 'legado 230 valor_original cadastro');
ok(legado.ean_force !== true, 'legado 230 sem ean_force');
ok(legado.codigo_loja === true, 'legado 230 codigo_loja');
ok(Core.ean13ChecksumOk('2300000001571') === false, 'legado 230 DV invalido no cadastro');
ok(Core.ean13ChecksumOk(legado.valor), 'legado 230 DV ok na etiqueta');

/* 230 novo (DV ok): EAN13 sem force. */
function eanDv(d12) {
  let s = 0;
  for (let i = 0; i < 12; i++) s += parseInt(d12[i], 10) * (i % 2 === 0 ? 1 : 3);
  return String((10 - (s % 10)) % 10);
}
const novo12 = '230000001572';
const novo13 = novo12 + eanDv(novo12);
const novo = Core.valorBarcodeProduto({
  nome: 'teste',
  preco_venda: 1,
  codigo_barras: novo13,
  codigo_gm: 'GM1',
});
ok(Core.ean13ChecksumOk(novo13), 'novo 230 DV ok ' + novo13);
ok(novo.formato === 'EAN13', 'novo 230 formato EAN13');
ok(novo.ean_force !== true, 'novo 230 sem ean_force');

const htmlLoja = Core.montarHtmlImpressao(
  base,
  [{ nome: 'loja', preco_venda: 2, codigo_barras: '2300000001571', codigo_gm: 'GM9', qtd: 1 }],
  'R'
);
ok(htmlLoja.includes('"valor":"2300000001570"') || htmlLoja.includes('"valor": "2300000001570"'), 'html imprint EAN corrigido');
ok(!htmlLoja.includes('"ean_force":true'), 'html legado sem ean_force');
ok(htmlLoja.includes('2300000001570'), 'html traz EAN bipavel');

const htmlPath = path.join(root, 'tmp-etq-termica.html');
fs.writeFileSync(htmlPath, html3);
console.log(fail ? 'FAIL ' + fail + '/' + n : 'OK ' + n + '/' + n);
process.exit(fail ? 1 : 0);
