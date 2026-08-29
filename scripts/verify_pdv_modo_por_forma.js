/**
 * PDV-MODO-POR-FORMA — prova JS real (modoItem + copiar + preço).
 */
"use strict";

const fs = require("fs");
const path = require("path");
const vm = require("vm");

const ROOT = path.resolve(__dirname, "..");
const JS = path.join(ROOT, "produtos", "static", "produtos", "js", "precos_forma_pagamento.js");

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

const code = fs.readFileSync(JS, "utf8");
const sandbox = { window: {}, console };
vm.runInNewContext(code, sandbox);
const API = sandbox.window.AgroPrecosFormaPagamento || sandbox.AgroPrecosFormaPagamento;
check(!!API, "API AgroPrecosFormaPagamento");
check(typeof API.modoItem === "function", "modoItem");
check(typeof API.copiarPrecosPorFormaDoProduto === "function", "copiar");
check(typeof API.precoBaseForma === "function", "precoBaseForma");

const lixoAB = {
  preco_a: 50,
  preco_b: 60,
  formas_a: ["PIX"],
  formas_b: ["Dinheiro"],
};
const ppf = { PIX: 87, Dinheiro: 90 };

check(API.modoItem({ precos_modo: "por_forma", precos_grupos: lixoAB }) === "por_forma", "modoItem: por_forma + lixo");
check(API.modoItem({ precos_modo: "grupos", precos_grupos: lixoAB }) === "grupos", "modoItem: grupos");
check(API.modoItem({ precos_grupos: lixoAB }) === "grupos", "modoItem: sem modo + A/B → grupos");
check(API.modoItem({ precos_modo: "por_forma", precos_por_forma: ppf }) === "por_forma", "modoItem: por_forma limpo");

const item = { preco_padrao: 99 };
API.copiarPrecosPorFormaDoProduto(item, {
  precos_modo: "por_forma",
  precos_por_forma: ppf,
  precos_grupos: lixoAB,
  preco_venda: 99,
});
check(item.precos_modo === "por_forma", "copiar: modo por_forma");
check(!item.precos_grupos, "copiar: removeu A/B");
check(Math.abs(API.precoBaseForma(item, "PIX") - 87) < 1e-9, "preço PIX 87 (não 50)");
check(Math.abs(API.precoBaseForma(item, "Dinheiro") - 90) < 1e-9, "preço Dinheiro 90");

const itemG = { preco_padrao: 99 };
API.copiarPrecosPorFormaDoProduto(itemG, {
  precos_modo: "grupos",
  precos_grupos: lixoAB,
  preco_venda: 99,
});
check(itemG.precos_modo === "grupos", "copiar grupos: modo");
check(!!itemG.precos_grupos, "copiar grupos: manteve A/B");
check(Math.abs(API.precoBaseForma(itemG, "PIX") - 50) < 1e-9, "grupos PIX 50");

const vis = API.precosGruposVisiveis({
  precos_modo: "por_forma",
  precos_grupos: lixoAB,
});
check(vis == null, "chips A/B ocultos em por_forma");

console.log(`\n${PASS} ok · ${FAIL} fail`);
process.exit(FAIL ? 1 : 0);
