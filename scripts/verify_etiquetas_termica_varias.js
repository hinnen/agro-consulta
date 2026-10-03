'use strict';

/**
 * Path ETQ-TERMICA-VARIAS — várias etiquetas térmicas = várias páginas.
 * node scripts/verify_etiquetas_termica_varias.js
 */
const fs = require('fs');
const os = require('os');
const path = require('path');
const zlib = require('zlib');
const { spawnSync } = require('child_process');
const vm = require('vm');

const root = path.resolve(__dirname, '..');
const corePath = path.join(root, 'produtos', 'static', 'produtos', 'js', 'produtos_etiquetas_core.js');
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

function item(nome, gm, preco, qtd, extra) {
  return Object.assign(
    {
      nome: nome,
      codigo_gm: gm,
      codigo_barras: '7891234567895',
      preco_venda: preco,
      qtd: qtd,
    },
    extra || {}
  );
}

const TERMICA = {
  id: 'a6-gondola-termica',
  nome: 'A6 GONDOLA',
  estilo: 'termica',
  largura_mm: 100,
  altura_mm: 80,
  nome_pt: 16,
  preco_pt: 36,
  codigo_pt: 12,
  rodape_pt: 11,
  barcode_height: 36,
  barcode_width: 1.1,
  texto_rodape: 'Gm Agro Mais',
};

function htmlTermica(itens) {
  return Core.montarHtmlImpressao(TERMICA, itens, 'Gm Agro Mais');
}

function count(html, re) {
  return (html.match(re) || []).length;
}

// --- cache e os dois caminhos da loja ---
const etqHtml = fs.readFileSync(
  path.join(root, 'produtos', 'templates', 'produtos', 'produtos_etiquetas.html'),
  'utf8'
);
const nfeHtml = fs.readFileSync(
  path.join(root, 'produtos', 'templates', 'produtos', 'entrada_nota.html'),
  'utf8'
);
const loteHtml = fs.readFileSync(
  path.join(root, 'produtos', 'templates', 'produtos', 'produtos_etiquetas_lote.html'),
  'utf8'
);
const cadHtml = fs.readFileSync(
  path.join(root, 'produtos', 'templates', 'produtos', 'produtos_cadastro_erp.html'),
  'utf8'
);
const etqJs = fs.readFileSync(
  path.join(root, 'produtos', 'static', 'produtos', 'js', 'produtos_etiquetas.js'),
  'utf8'
);

check(etqHtml.includes("produtos_etiquetas_core.js' %}?v=28"), 'tela etiquetas puxa core v=28');
check(nfeHtml.includes("produtos_etiquetas_core.js' %}?v=28"), 'entrada de nota puxa core v=28');
check(loteHtml.includes("produtos_etiquetas_core.js' %}?v=28"), 'lote puxa core v=28');
check(cadHtml.includes("produtos_etiquetas_core.js' %}?v=28"), 'cadastro puxa core v=28');
check(etqJs.includes('Core.imprimirItens(state.fila'), 'fila da tela usa o mesmo imprimir');
check(etqJs.includes("origem: 'historico'"), 'reimpressão do histórico usa o mesmo imprimir');
check(nfeHtml.includes('Core.imprimirItens(itens'), 'etapa 6 da nota usa o mesmo imprimir');
check(nfeHtml.includes("origem: 'entrada_nfe'"), 'etapa 6 marca origem entrada_nfe');
check(!source.includes('width:0;height:0'), 'iframe de impressão não é mais 0×0');
check(source.includes('.pg + .pg{page-break-before:always;break-before:page}'), 'quebra de página entre etiquetas');

// --- HTML térmico: uma caixa .pg por etiqueta, body sem cortar ---
const um = htmlTermica([item('alfafa 1kg', 'GM0836', 37.9, 1)]);
check(count(um, /class="pg"/g) === 1, '1 item → 1 página no HTML');
check(!/html,body\{[^}]*overflow:hidden/.test(um), 'body térmico não esconde o resto');
check(!/html,body\{[^}]*height:\d/.test(um), 'body térmico não trava na altura de 1 etiqueta');
check(um.includes('@page{size:100mm 80mm;margin:0}'), 'página = tamanho da etiqueta');
check(um.includes('alfafa 1kg') && um.includes('>37<') && um.includes('>,90<') && um.includes('GM0836'), '1 etiqueta traz nome, preço e GM');
check(um.includes('Gm Agro Mais'), 'rodapé da loja');
check(um.includes('id="bc-0-0"'), 'código de barras da primeira');

const tres = htmlTermica([
  item('alfafa 1kg', 'GM0836', 37.9, 1),
  item('pact jaal', 'GM0837', 5, 1),
  item('pact g', 'GM0838', 5.5, 1),
]);
check(count(tres, /class="pg"/g) === 3, '3 itens → 3 páginas no HTML');
check(tres.includes('GM0836') && tres.includes('GM0837') && tres.includes('GM0838'), 'os 3 códigos entram');
check(tres.includes('id="bc-0-0"') && tres.includes('id="bc-1-0"') && tres.includes('id="bc-2-0"'), 'um código de barras por item');

const qtd = htmlTermica([item('pilha aa', 'GM1428', 4, 3)]);
check(count(qtd, /class="pg"/g) === 3, 'quantidade 3 → 3 etiquetas');
check(qtd.includes('id="bc-0-0"') && qtd.includes('id="bc-0-1"') && qtd.includes('id="bc-0-2"'), 'códigos distintos na mesma quantidade');

const qtdRuim = htmlTermica([item('sem qtd', 'GM1', 1, 0), item('qtd texto', 'GM2', 2, '2')]);
check(count(qtdRuim, /class="pg"/g) === 3, 'qtd 0 vira 1 e qtd "2" vira 2');

const misto = htmlTermica([
  item('a', 'GM1', 1, 2),
  item('b', 'GM2', 2, 1),
]);
check(count(misto, /class="pg"/g) === 3, '2+1 quantidade → 3 etiquetas');

// --- gôndola não virou 1 página por etiqueta pequena ---
const gondola = Core.montarHtmlImpressao(
  Core.DEFAULT_BONUS_A6_PRESET,
  [
    item('g1', 'GM1', 1, 1),
    item('g2', 'GM2', 2, 1),
    item('g3', 'GM3', 3, 1),
  ],
  ''
);
check(count(gondola, /class="sheet"/g) === 1, 'A6 bônus 3 etiquetas cabem em 1 folha');
check(count(gondola, /class="etq"/g) === 3, 'A6 bônus desenha as 3 na mesma folha');
check(!gondola.includes('class="pg"'), 'gôndola não usa a caixa da térmica');

const gondola4 = Core.montarHtmlImpressao(
  Core.DEFAULT_BONUS_A6_PRESET,
  [1, 2, 3, 4].map(function (n) {
    return item('g' + n, 'GM' + n, n, 1);
  }),
  ''
);
check(count(gondola4, /class="sheet"/g) === 2, '4ª etiqueta A6 vai para a 2ª folha');

const a4 = Core.montarHtmlImpressao(
  Core.DEFAULT_GONDOLA_PRESET,
  [item('gondola a4', 'GM9', 9, 2)],
  ''
);
check(count(a4, /class="sheet"/g) === 1, '2 gôndolas A4 ficam na mesma folha');
check(a4.includes('@page{size:A4;margin:0}'), 'gôndola A4 continua folha A4');

// --- Chrome: o PDF tem uma página por etiqueta, sem folha em branco ---
function chromeBin() {
  const candidatos = [
    path.join(process.env.PROGRAMFILES || '', 'Google', 'Chrome', 'Application', 'chrome.exe'),
    path.join(process.env['PROGRAMFILES(X86)'] || '', 'Google', 'Chrome', 'Application', 'chrome.exe'),
  ];
  for (let i = 0; i < candidatos.length; i++) {
    if (candidatos[i] && fs.existsSync(candidatos[i])) return candidatos[i];
  }
  return '';
}

function pdfPages(buf) {
  const text = buf.toString('latin1');
  return (text.match(/\/Type\s*\/Page(?![a-zA-Z])/g) || []).length;
}

function pdfPlain(buf) {
  const raw = buf.toString('latin1');
  const out = [];
  const re = /stream\r?\n([\s\S]*?)\r?\nendstream/g;
  let m;
  while ((m = re.exec(raw))) {
    const chunk = Buffer.from(m[1], 'latin1');
    try {
      out.push(zlib.inflateSync(chunk).toString('latin1'));
    } catch (e) {
      out.push(chunk.toString('latin1'));
    }
  }
  return out.join('\n');
}

function printPdf(nome, html) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'etq-'));
  const htmlFile = path.join(dir, nome + '.html');
  const pdfFile = path.join(dir, nome + '.pdf');
  fs.writeFileSync(htmlFile, html, 'utf8');
  const bin = chromeBin();
  if (!bin) return { ok: false, reason: 'sem chrome' };
  const url = 'file:///' + htmlFile.replace(/\\/g, '/');
  const run = spawnSync(
    bin,
    ['--headless=new', '--disable-gpu', '--no-pdf-header-footer', '--print-to-pdf=' + pdfFile, url],
    { timeout: 30000, windowsHide: true }
  );
  if (!fs.existsSync(pdfFile)) {
    return { ok: false, reason: 'pdf nao saiu ' + (run.stderr || '') };
  }
  const buf = fs.readFileSync(pdfFile);
  const text = pdfPlain(buf);
  return {
    ok: true,
    pages: pdfPages(buf),
    text: text,
    desenhos: (text.match(/\sTj\b/g) || []).length,
    file: pdfFile,
  };
}

const bin = chromeBin();
check(!!bin, 'Chrome encontrado para provar o PDF');
if (bin) {
  const p1 = printPdf('um', um);
  check(p1.ok && p1.pages === 1, 'PDF 1 etiqueta = 1 página (sem folha extra) · ' + (p1.pages || p1.reason));
  check(p1.ok && p1.desenhos >= 4, 'PDF da única etiqueta não sai em branco · desenhos ' + (p1.desenhos || 0));

  const p3 = printPdf('tres', tres);
  check(p3.ok && p3.pages === 3, 'PDF 3 etiquetas = 3 páginas · ' + (p3.pages || p3.reason));
  check(
    p3.ok && p3.desenhos > p1.desenhos * 2,
    'PDF das 3 desenha o texto das 3 · ' + (p3.desenhos || 0) + ' vs 1 etiqueta ' + (p1.desenhos || 0)
  );

  const pq = printPdf('qtd', qtd);
  check(pq.ok && pq.pages === 3, 'PDF quantidade 3 = 3 páginas · ' + (pq.pages || pq.reason));
  check(
    pq.ok && pq.desenhos > p1.desenhos * 2,
    'as 3 cópias estão desenhadas no PDF · ' + (pq.desenhos || 0)
  );

  const pg = printPdf('gondola3', gondola);
  check(pg.ok && pg.pages === 1, 'PDF gôndola A6 com 3 continua 1 folha · ' + (pg.pages || pg.reason));

  const pg4 = printPdf('gondola4', gondola4);
  check(pg4.ok && pg4.pages === 2, 'PDF gôndola A6 com 4 = 2 folhas · ' + (pg4.pages || pg4.reason));
}

console.log('ETQ-TERMICA-VARIAS ' + passed + '/' + (passed + failed));
if (failed) {
  fails.forEach(function (m) {
    console.error(' - ' + m);
  });
  process.exit(1);
}
