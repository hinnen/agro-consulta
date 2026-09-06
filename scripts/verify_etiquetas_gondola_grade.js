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
check(antigo.folha === 'a4', 'gôndola padrão continua A4');

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
check(html18.includes('@page{size:A4;'), 'HTML A4 deve pedir papel A4');

const html28 = Core.montarHtmlImpressao(novo, [{ nome: 'Teste', preco_venda: 10, qtd: 28 }]);
check((html28.match(/class="etq"/g) || []).length === 28, '60 mm deve renderizar todas as 28 etiquetas');
check((html28.match(/class="sheet"/g) || []).length === 2, '28 etiquetas devem quebrar em duas folhas');
check(html28.includes('left:135.5mm;top:257mm'), 'última célula da terceira coluna deve permanecer dentro da A4');

const bonus = Core.normalizarPreset(Core.clonePreset(Core.DEFAULT_BONUS_A6_PRESET));
check(bonus.folha === 'a6', 'seed Bônus A6 deve usar folha a6');
check(bonus.largura_mm === 100 && bonus.altura_mm === 45, 'Bônus A6 = 100 × 45 mm');
check(bonus.cols_folha === 1 && bonus.rows_folha === 3, 'A6 100×45 deve ser 1 × 3');

const gradeA6 = Core.calcularGradeFolha('a6', 100, 45, 0.5);
check(gradeA6.cols === 1 && gradeA6.rows === 3, 'calcularGradeFolha A6 → 1×3');
check(gradeA6.outer_w === 101 && gradeA6.outer_h === 46, 'borda A6 → 101 × 46');
check(gradeA6.cabe, '1×3 deve caber na A6');
check((105 - gradeA6.outer_w) / 2 === 2, 'coluna única centralizada na A6');
check((148 - 3 * gradeA6.outer_h) / 2 === 5, 'três linhas centralizadas na A6');

const htmlBonus = Core.montarHtmlImpressao(bonus, [{ nome: 'Bônus', preco_venda: 5, qtd: 3 }]);
check(htmlBonus.includes('@page{size:A6;'), 'HTML A6 deve pedir papel A6');
check((htmlBonus.match(/class="etq"/g) || []).length === 3, '3 bônus em uma A6');
check((htmlBonus.match(/class="sheet"/g) || []).length === 1, '3 etiquetas = 1 folha A6');
check(htmlBonus.includes('width:105mm'), 'folha A6 105 mm de largura');
check(htmlBonus.includes('height:148mm'), 'folha A6 148 mm de altura');

const htmlBonus5 = Core.montarHtmlImpressao(bonus, [{ nome: 'Bônus', preco_venda: 5, qtd: 5 }]);
check((htmlBonus5.match(/class="sheet"/g) || []).length === 2, '5 etiquetas A6 quebram em 2 folhas');

const seeded = Core.mergeServerPresets([], []);
check(
  seeded.some(function (p) {
    return p.id === 'bonus-a6';
  }),
  'seed deve incluir Bônus A6'
);

console.log('OK: etiquetas gôndola A4 90/60 mm + A6 bônus 100×45 (1×3), centralização e quebra de página.');
