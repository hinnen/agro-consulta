#!/usr/bin/env node
/**
 * Garante que a via do cliente do comprovante fiado traz o Nº da venda em destaque.
 */
"use strict";

const fs = require("fs");
const path = require("path");
const vm = require("vm");

const root = path.resolve(__dirname, "..");
const src = fs.readFileSync(path.join(root, "produtos/static/produtos/js/venda_cupom_80mm.js"), "utf8");

const fakeDoc = {
    createElement: function () {
        return { src: "", async: true, onload: null, onerror: null };
    },
    head: { appendChild: function () {} },
    documentElement: { appendChild: function () {} },
};

const ctx = {
    window: {},
    document: fakeDoc,
    location: { origin: "http://localhost" },
    Image: function () {},
    setTimeout: function () {},
    requestIdleCallback: undefined,
    console,
};
ctx.window = ctx;
ctx.globalThis = ctx;
ctx.global = ctx;

vm.runInNewContext(src, ctx, { filename: "venda_cupom_80mm.js" });

if (typeof ctx.agroCupomPagesInnerHtml !== "function") {
    console.error("FAIL agroCupomPagesInnerHtml ausente");
    process.exit(1);
}

const html = ctx.agroCupomPagesInnerHtml({
    eh_fiado: true,
    venda_id: 1403,
    total: 409.5,
    total_texto: "R$ 409,50",
    itens: [{ nome: "Racao", qtd: 1, preco: 409.5, subtotal: 409.5 }],
    cliente_nome: "Joelma ( Esposa Wagner )",
    criado_em: "16/07/2026 20:03:53",
    forma_pagamento: "Fiado",
});

const checks = [
    ["via_cliente", html.indexOf("VIA DO CLIENTE") >= 0],
    ["via_loja", html.indexOf("VIA DA LOJA") >= 0],
    ["numero_1403", html.indexOf("Nº 1403") >= 0],
    ["data_segundos", html.indexOf("16/07/2026 20:03:53") >= 0],
    ["duas_vias", (html.match(/VIA DO CLIENTE/g) || []).length >= 1 && (html.match(/Nº 1403/g) || []).length >= 2],
];

let failed = false;
for (const [name, ok] of checks) {
    console.log((ok ? "  OK  " : "  FAIL ") + name);
    if (!ok) failed = true;
}

const outDir = "/opt/cursor/artifacts";
try {
    fs.mkdirSync(outDir, { recursive: true });
    const preview =
        "<!DOCTYPE html><html><head><meta charset='utf-8'><title>Prévia cupom fiado</title>" +
        "<style>body{font-family:system-ui,sans-serif;background:#e2e8f0;padding:16px}" +
        ctx.agroCupomStyles() +
        ".pg{background:#fff;margin:12px auto;box-shadow:0 1px 6px rgba(0,0,0,.2)}</style></head><body>" +
        html +
        "</body></html>";
    fs.writeFileSync(path.join(outDir, "cupom_fiado_via_cliente_numero.html"), preview);
} catch (e) {
    console.warn("aviso: não gravou prévia HTML", e.message);
}

if (failed) process.exit(1);
console.log("OK cupom numero venda");
