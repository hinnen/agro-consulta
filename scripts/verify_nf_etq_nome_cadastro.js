'use strict';

/**
 * Path NF-ETQ-NOME-CADASTRO — etiqueta da entrada de nota usa o nome do cadastro,
 * sem o código interno do vínculo (vinculo_c_prod, ean_pg, …).
 * node scripts/verify_nf_etq_nome_cadastro.js
 */
const fs = require('fs');
const path = require('path');
const vm = require('vm');

const root = path.resolve(__dirname, '..');
const corePath = path.join(root, 'produtos', 'static', 'produtos', 'js', 'produtos_etiquetas_core.js');
const nfePath = path.join(root, 'produtos', 'templates', 'produtos', 'entrada_nota.html');
const source = fs.readFileSync(corePath, 'utf8');
const nfe = fs.readFileSync(nfePath, 'utf8');
const context = { console, setTimeout, clearTimeout };
vm.runInNewContext(source, context, { filename: corePath });
const limpa = context.AgroEtiquetasCore.nomeCadastroSemTipoMatch;

let passed = 0;
let failed = 0;

function check(ok, message) {
  if (ok) {
    passed += 1;
    return;
  }
  failed += 1;
  console.error('FAIL:', message);
}

check(typeof limpa === 'function', 'função exportada');
check(limpa('Ração 25 kg', 'vinculo_c_prod') === 'Ração 25 kg', 'nome limpo fica');
check(limpa('Ração 25 kg (vinculo_c_prod)', 'vinculo_c_prod') === 'Ração 25 kg', 'tira um código');
check(
  limpa('Ração 25 kg (vinculo_c_prod) (vinculo_c_prod)', 'vinculo_c_prod') === 'Ração 25 kg',
  'tira código repetido do rascunho'
);
check(limpa('Dipirona (500 ml) (ean_pg)', 'ean_pg') === 'Dipirona (500 ml)', 'não come o parêntese do cadastro');
check(limpa('Dipirona (500 ml)', '') === 'Dipirona (500 ml)', 'sem vínculo o parêntese fica');
check(limpa('Vitaminas (pg)', 'ean_overlay') === 'Vitaminas', 'tira (pg) mesmo sem ser o tipo da linha');
check(limpa('Atropina (codigo_overlay)', '') === 'Atropina', 'tira código já gravado sem o tipo da linha');
check(limpa('  Nome   (xml_vinculo_pre)  ', 'xml_vinculo_pre') === 'Nome', 'espaços');
check(!nfe.includes('${d.nome_catalogo}${mt}'), 'a grade não cola mais o código no nome');
check(nfe.includes('nomeCadastroSemTipoMatch(d.nome_catalogo, d.match_tipo)'), 'ao montar a linha usa o nome limpo');
check(nfe.includes('function entradaNfeEtiquetaNomeDoLinha'), 'a etapa 6 tem nome próprio da etiqueta');
check(nfe.includes('entradaNfeEtiquetaNomeDoLinha(l)'), 'a lista da etapa 6 usa o nome limpo');
check(nfe.includes('row.dataset.etqNome = entradaNfeEtiquetaNomeDoLinha'), 'a impressão usa o nome limpo');
check(nfe.includes('function entradaNfeEtiquetaCodigoGm'), 'a etapa 6 tem código GM próprio da etiqueta');
check(nfe.includes('entradaNfeEtiquetaCodigoGm(l)'), 'a lista da etapa 6 usa código GM do catálogo');
check(nfe.includes('row.dataset.etqGm = entradaNfeEtiquetaCodigoGm'), 'a impressão usa código GM do catálogo');
check(!/entradaNfeEtiquetasRenderLista[\s\S]{0,1200}l\.c_prod/.test(nfe), 'etapa 6 não usa c_prod da nota como GM');

console.log(passed + '/' + (passed + failed));
process.exit(failed ? 1 : 0);
