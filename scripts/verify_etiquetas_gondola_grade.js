'use strict';

const fs = require('fs');
const path = require('path');
const vm = require('vm');

const root = path.resolve(__dirname, '..');
const corePath = path.join(root, 'produtos', 'static', 'produtos', 'js', 'produtos_etiquetas_core.js');
const source = fs.readFileSync(corePath, 'utf8');
const context = { console, setTimeout, clearTimeout };
vm.runInNewContext(source, context, { filename: corePath });
const Core = context.AgroEtiquetasCore;

function check(ok, message) {
  if (!ok) throw new Error(message);
}

const remedios = Core.clonePreset(Core.DEFAULT_GONDOLA_PRESET);
remedios.id = 'remedios';
remedios.nome = 'remedios';
remedios.cores.faixa_bg = '#123456';
remedios.show_gm = true;
remedios.layout.preco = { x: 27, y: 35, w: 57, h: 43 };

const antigo = Core.normalizarPreset(remedios);
check(antigo.cols_folha === 2 && antigo.rows_folha === 9, '90 × 30 deve ser 2 × 9');

const seisCm = Core.clonePreset(remedios);
seisCm.id = 'remedios-6cm';
seisCm.nome = 'remedios 6cm';
seisCm.largura_mm = 60;
const novo = Core.normalizarPreset(seisCm);
check(novo.cols_folha === 3 && novo.rows_folha === 9, '60 × 30 deve ser 3 × 9');
check(JSON.stringify(novo.layout) === JSON.stringify(remedios.layout), 'duplicação deve preservar layout percentual');
check(JSON.stringify(novo.cores) === JSON.stringify(remedios.cores), 'duplicação deve preservar cores');
check(novo.show_gm === remedios.show_gm, 'duplicação deve preservar campos ligados/desligados');

const legado60 = Core.normalizarPreset({ estilo: 'gondola', largura_mm: 60, altura_mm: 30 });
const legado90 = Core.normalizarPreset({ estilo: 'gondola', largura_mm: 90, altura_mm: 30 });
check(legado60.cols_folha === 3, 'preset legado 60 mm sem cols deve assumir 3');
check(legado90.cols_folha === 2, 'preset legado 90 mm sem cols deve assumir 2');

const grade = Core.calcularGradeA4(60, 30, 0.5, 3, 9);
check(grade.outer_w === 61 && grade.outer_h === 31, 'borda externa deve resultar em 61 × 31 mm');
check(grade.cabe_a4, 'grade 3 × 9 deve caber na A4');
check((210 - 3 * grade.outer_w) / 2 === 13.5, 'três colunas devem ficar centralizadas');
check((297 - 9 * grade.outer_h) / 2 === 9, 'nove linhas devem ficar centralizadas sem corte');

const html18 = Core.montarHtmlImpressao(antigo, [{ nome: 'Teste', preco_venda: 10, qtd: 18 }]);
check((html18.match(/class="etq"/g) || []).length === 18, '90 mm deve renderizar 18 etiquetas');
check((html18.match(/class="sheet"/g) || []).length === 1, '18 etiquetas devem ocupar uma folha');

const html28 = Core.montarHtmlImpressao(novo, [{ nome: 'Teste', preco_venda: 10, qtd: 28 }]);
check((html28.match(/class="etq"/g) || []).length === 28, '60 mm deve renderizar todas as 28 etiquetas');
check((html28.match(/class="sheet"/g) || []).length === 2, '28 etiquetas devem quebrar em duas folhas');
check(html28.includes('left:135.5mm;top:257mm'), 'última célula da terceira coluna deve permanecer dentro da A4');

console.log('OK: etiquetas gôndola 90 × 30 (2 × 9) e 60 × 30 (3 × 9), centralização e quebra de página.');
