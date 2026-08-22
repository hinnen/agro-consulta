/**
 * Smoke test: folha da árvore sem filhos reais deve ir direto a pesos (sem card «Geral»).
 * Run: node scripts/verify_catalogo_skip_geral.js
 */
"use strict";

function simulateIrParaPesosOuFilhos(noAtual, pathExactRef) {
  function filhosReaisNo() {
    var no = noAtual();
    return no && Array.isArray(no.filhos) ? no.filhos : [];
  }
  function temProdutosNoNivelAtual() {
    var no = noAtual();
    return !!(no && (no.qtd_exata || 0) > 0);
  }
  function opcoesFilhosNo() {
    var no = noAtual();
    if (!no) return [];
    var optsN = [];
    (no.filhos || []).forEach(function (f) {
      optsN.push({ slug: f.slug, nome: f.nome, qtd: f.qtd || 0 });
    });
    if ((no.qtd_exata || 0) > 0) {
      optsN.push({ slug: "_geral", nome: "Geral", qtd: no.qtd_exata || 0 });
    }
    return optsN;
  }

  if (!filhosReaisNo().length) {
    pathExactRef.value = temProdutosNoNivelAtual();
    return { view: "pesos", pathExact: pathExactRef.value };
  }
  var filhos = opcoesFilhosNo();
  if (filhos.length > 0) {
    return { view: "nivel", filhos: filhos.map(function (f) { return f.slug; }) };
  }
  pathExactRef.value = false;
  return { view: "pesos", pathExact: false };
}

var fails = 0;
function ok(m) { console.log("  OK  " + m); }
function fail(m) { fails++; console.log(" FAIL " + m); }

// Folha com produtos: antes mostrava só «Geral»; agora deve ir a pesos.
var folha = {
  slug: "racas-medias-grandes",
  nome: "Raças Médias e Grandes",
  qtd_exata: 14,
  filhos: [],
};
var pathExactRef = { value: false };
var r = simulateIrParaPesosOuFilhos(function () { return folha; }, pathExactRef);
if (r.view !== "pesos") fail("folha deve ir a pesos, got " + r.view);
else ok("folha → pesos");
if (!r.pathExact) fail("folha pathExact deve ser true");
else ok("folha pathExact true");

// Nó com filhos reais + produtos no nó: ainda mostra grade (inclui Geral).
var pai = {
  slug: "adulto",
  qtd_exata: 2,
  filhos: [{ slug: "premium", nome: "Premium", qtd: 5, filhos: [] }],
};
pathExactRef.value = false;
var r2 = simulateIrParaPesosOuFilhos(function () { return pai; }, pathExactRef);
if (r2.view !== "nivel") fail("pai com filhos deve mostrar nivel");
else ok("pai com filhos → nivel");
if (!r2.filhos || r2.filhos.indexOf("_geral") < 0) fail("pai deve listar _geral entre opções");
else ok("pai mantém _geral quando há subcategorias");

console.log("---");
if (fails) {
  console.log("VERIFY_FAIL (" + fails + ")");
  process.exit(1);
}
console.log("VERIFY_OK");
process.exit(0);
