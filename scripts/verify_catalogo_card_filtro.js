/**
 * VERIFY CATALOGO-CARD-FILTRO — quebra o caminho em chips (sem truncar).
 * Run: node scripts/verify_catalogo_card_filtro.js
 */
"use strict";

var fails = 0;
function ok(m) { console.log("  OK  " + m); }
function fail(m) { fails++; console.log(" FAIL " + m); }

function partsFrom(titulo) {
  return String(titulo || "")
    .split(/\s*[·•]\s*/)
    .map(function (s) { return s.trim(); })
    .filter(Boolean);
}

var got = partsFrom("Cão · Adulto · Raças Médias e Grandes · 1 kg");
var want = ["Cão", "Adulto", "Raças Médias e Grandes", "1 kg"];
if (JSON.stringify(got) !== JSON.stringify(want)) {
  fail("split middle-dot got=" + JSON.stringify(got));
} else {
  ok("split middle-dot");
}

got = partsFrom("Cão • Adulto • Raças Médias e Grandes");
want = ["Cão", "Adulto", "Raças Médias e Grandes"];
if (JSON.stringify(got) !== JSON.stringify(want)) {
  fail("split bullet got=" + JSON.stringify(got));
} else {
  ok("split bullet");
}

got = partsFrom("Busca");
if (JSON.stringify(got) !== JSON.stringify(["Busca"])) {
  fail("single token");
} else {
  ok("single token");
}

if (partsFrom("").length !== 0) fail("empty");
else ok("empty");

if (fails) {
  console.log("VERIFY_FAIL " + fails);
  process.exit(1);
}
console.log("VERIFY_OK");
