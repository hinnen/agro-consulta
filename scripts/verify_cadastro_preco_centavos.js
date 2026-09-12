/**
 * Prova JS real do path CAD-PRECO-CENTAVOS.
 * Extrai parseMoeda/fmtMoeda2 do modal e simula: digita -> blur -> troca aba -> payload -> reabrir.
 */
"use strict";

const fs = require("fs");
const path = require("path");
const vm = require("vm");

const ROOT = path.resolve(__dirname, "..");
const MODAL = path.join(
  ROOT,
  "produtos",
  "templates",
  "produtos",
  "_modal_editar_produto_cadastro_erp.inc.html"
);

let PASS = 0;
let FAIL = 0;

function check(ok, msg) {
  if (ok) {
    PASS += 1;
    console.log("  OK  " + msg);
  } else {
    FAIL += 1;
    console.log(" FAIL " + msg);
  }
}

function sliceFn(html, startMark, endMark) {
  const i = html.indexOf(startMark);
  const j = html.indexOf(endMark, i + 1);
  if (i < 0 || j < 0 || j <= i) return "";
  return html.slice(i, j);
}

const html = fs.readFileSync(MODAL, "utf8");
const parseBlock = sliceFn(html, "function _parseMoedaTexto", "function fmtMoneyPt");
const fmtBlock = sliceFn(html, "function fmtMoeda2", "function setPct2");
if (!parseBlock || !fmtBlock) {
  console.error("FAIL nao extraí parseMoeda/fmtMoeda2 do modal");
  process.exit(1);
}

const ctx = {};
vm.runInNewContext(parseBlock + "\n" + fmtBlock, ctx);
const parseMoeda = ctx.parseMoeda;
const parseMoedaStrict = ctx.parseMoedaStrict;
const fmtMoeda2 = ctx.fmtMoeda2;

function telaCentavos(txt, esperado) {
  const n = parseMoeda(txt);
  const s = String(txt);
  return Math.abs(n - esperado) < 1e-9 && s.indexOf("829") < 0;
}
check(typeof parseMoeda === "function", "parseMoeda extraida");
check(typeof fmtMoeda2 === "function", "fmtMoeda2 extraida");
check(typeof parseMoedaStrict === "function", "parseMoedaStrict extraida");

check(Math.abs(parseMoeda(82.9) - 82.9) < 1e-9, "numero 82.9 continua 82.9");
check(Math.abs(parseMoeda("82,90") - 82.9) < 1e-9, "texto 82,90 = 82.9");
check(Math.abs(parseMoeda("82.90") - 82.9) < 1e-9, "texto 82.90 = 82.9");
check(Math.abs(parseMoeda("82,10") - 82.1) < 1e-9, "texto 82,10 = 82.1 (nao 821)");
check(Math.abs(parseMoeda(82.1) - 82.1) < 1e-9, "numero 82.1 continua 82.1");
check(Math.abs(parseMoeda("1.234,56") - 1234.56) < 1e-9, "1.234,56 = 1234.56");
check(parseMoeda("") === 0, "vazio = 0");
check(parseMoeda(null) === 0, "null = 0");
check(Math.abs(parseMoeda(92) - 92) < 1e-9, "inteiro 92");
check(parseMoedaStrict("82,90") === 82.9, "strict 82,90");
check(parseMoedaStrict("") === null, "strict vazio = null");
check(parseMoedaStrict(-1) === null, "strict negativo = null");

const shown = fmtMoeda2(parseMoeda("82,90"));
check(telaCentavos(shown, 82.9), "fmtMoeda2(82,90) reparseia 82.9 (nao 829)");
check(telaCentavos(fmtMoeda2(parseMoeda(82.9)), 82.9), "fmt apos numero 82.9 nao vira 829");
check(Math.abs(parseMoeda(fmtMoeda2(829)) - 829) < 1e-9, "829 de verdade continua 829");

console.log("== Path Por forma: digita -> blur -> troca aba -> salvar -> reabrir ==");
const map = {};
function blurForma(forma, raw) {
  const t = String(raw || "").trim();
  if (!t) {
    delete map[forma];
    return "";
  }
  const n = parseMoeda(t);
  if (n > 0) {
    map[forma] = n;
    return fmtMoeda2(n);
  }
  delete map[forma];
  return "";
}
const aposBlur = blurForma("PIX", "82,90");
check(telaCentavos(aposBlur, 82.9), "blur PIX: tela 82,90");
check(Math.abs(map.PIX - 82.9) < 1e-9, "blur PIX: estado 82.9");

const aposTrocaAba = fmtMoeda2(parseMoeda(map.PIX));
check(telaCentavos(aposTrocaAba, 82.9), "voltar aba: tela continua 82,90");

function payloadPorForma(m) {
  const out = {};
  Object.keys(m).forEach(function (k) {
    const n = parseMoeda(m[k]);
    if (n > 0) out[k] = n;
  });
  return out;
}
const payload = payloadPorForma(map);
check(Math.abs(payload.PIX - 82.9) < 1e-9, "payload salvar PIX = 82.9");
check(Math.abs(payload.PIX - 829) > 1, "payload nao e 829");

const reabrir = fmtMoeda2(parseMoeda(payload.PIX));
check(telaCentavos(reabrir, 82.9), "reabrir modal: PIX 82,90");

console.log("== Path 2 grupos: digita A -> blur -> salvar -> reabrir ==");
const grupos = { preco_a: null, preco_b: null, formas_a: ["PIX"], formas_b: ["Fiado"] };
function blurGrupo(raw) {
  const t = String(raw || "").trim();
  if (!t) return { n: null, shown: "" };
  const n = parseMoeda(t);
  if (n > 0) return { n: n, shown: fmtMoeda2(n) };
  return { n: null, shown: "" };
}
const gA = blurGrupo("82,90");
grupos.preco_a = gA.n;
check(telaCentavos(gA.shown, 82.9), "blur grupo A: tela 82,90");
const payloadG = {
  preco_a: grupos.preco_a != null && parseMoeda(grupos.preco_a) > 0 ? parseMoeda(grupos.preco_a) : null,
  preco_b: grupos.preco_b != null && parseMoeda(grupos.preco_b) > 0 ? parseMoeda(grupos.preco_b) : null,
  formas_a: grupos.formas_a.slice(),
  formas_b: grupos.formas_b.slice(),
};
check(Math.abs(payloadG.preco_a - 82.9) < 1e-9, "payload grupo A = 82.9");
check(telaCentavos(fmtMoeda2(parseMoeda(payloadG.preco_a)), 82.9), "reabrir grupo A: 82,90");

const gB = blurGrupo("92,00");
check(Math.abs(parseMoeda(gB.shown) - 92) < 1e-9, "grupo B 92,00 intacto");

console.log("== Contraste bug antigo ==");
function parseMoedaAntigo(s) {
  const t = String(s).replace(/\s/g, "").replace(/\./g, "").replace(",", ".");
  const n = parseFloat(t);
  return isFinite(n) ? n : 0;
}
check(Math.abs(parseMoedaAntigo(82.9) - 829) < 1e-9, "bug antigo: 82.9 numero virava 829");
check(Math.abs(parseMoeda(fmtMoeda2(parseMoedaAntigo(82.9))) - 829) < 1e-9, "bug antigo: tela virava 829");
check(Math.abs(parseMoeda(82.9) - 82.9) < 1e-9, "fix: mesmo 82.9 numero fica 82.9");

console.log("\nRESULTADO " + PASS + " ok / " + FAIL + " falha");
process.exit(FAIL ? 1 : 0);
