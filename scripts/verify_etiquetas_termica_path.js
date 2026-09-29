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

const htmlPath = path.join(root, 'tmp-etq-termica.html');
fs.writeFileSync(htmlPath, html3);
console.log(fail ? 'FAIL ' + fail + '/' + n : 'OK ' + n + '/' + n);
process.exit(fail ? 1 : 0);
