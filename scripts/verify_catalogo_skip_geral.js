/**
 * VERIFY CATALOGO-SKIP-GERAL — FSM 1:1 do JS de navegação.
 *
 * Path da loja: Cão → Adulto → Raças Médias e Grandes → pesos (sem card «Geral»).
 * Run: node scripts/verify_catalogo_skip_geral.js
 */
"use strict";

var fs = require("fs");
var path = require("path");

var fails = 0;
function ok(m) { console.log("  OK  " + m); }
function fail(m) { fails++; console.log(" FAIL " + m); }
function eq(got, want, label) {
  var gs = JSON.stringify(got);
  var ws = JSON.stringify(want);
  if (gs !== ws) fail(label + " got=" + gs + " want=" + ws);
  else ok(label);
}

function makeNav(arvore, catalogo, pesosGrade) {
  var pathStack = [];
  var pathExact = false;
  var pesoAtual = "";
  var viewMode = "home";
  var lastGradeSlugs = [];
  var lastTitulo = "";

  function pathPrefix() {
    return pathStack.map(function (x) { return x.slug; }).join("/");
  }
  function pathTitulo() {
    return pathStack.map(function (x) { return x.nome; }).filter(Boolean).join(" · ");
  }
  function noAtual() {
    if (!pathStack.length) return null;
    var cur = null;
    var list = arvore;
    for (var i = 0; i < pathStack.length; i++) {
      var slug = pathStack[i].slug;
      cur = null;
      for (var j = 0; j < list.length; j++) {
        if (list[j].slug === slug) {
          cur = list[j];
          break;
        }
      }
      if (!cur) return null;
      list = cur.filhos || [];
    }
    return cur;
  }
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
      optsN.push({
        slug: f.slug,
        nome: f.nome,
        qtd: f.qtd || 0,
        filhos: f.filhos || [],
        qtd_exata: f.qtd_exata || 0,
      });
    });
    if ((no.qtd_exata || 0) > 0) {
      optsN.push({
        slug: "_geral",
        nome: "Geral",
        qtd: no.qtd_exata || 0,
        filhos: [],
        qtd_exata: no.qtd_exata || 0,
      });
    }
    return optsN;
  }
  function produtosNoPath() {
    var pref = pathPrefix();
    var out = [];
    catalogo.forEach(function (p) {
      var pth = String(p.path || (p.path_slugs || []).join("/") || "");
      if (!pref) {
        out.push(p);
        return;
      }
      if (pathExact) {
        if (pth === pref) out.push(p);
      } else if (pth === pref || pth.indexOf(pref + "/") === 0) {
        out.push(p);
      }
    });
    return out;
  }
  function pesosDisponiveis() {
    var set = {};
    produtosNoPath().forEach(function (p) {
      (p.peso_keys || []).forEach(function (k) { set[k] = true; });
    });
    return set;
  }
  function renderCardsNivel(lista) {
    lastGradeSlugs = lista.map(function (s) { return s.slug; });
  }
  function mostrarHome() {
    pathStack = [];
    pathExact = false;
    pesoAtual = "";
    viewMode = "home";
    lastGradeSlugs = [];
    lastTitulo = "";
  }
  function mostrarGradeNivel(titulo) {
    viewMode = "nivel";
    lastTitulo = titulo || "";
  }
  function mostrarPesos(titulo) {
    viewMode = "pesos";
    pesoAtual = "";
    lastTitulo = titulo || "Peso";
    lastGradeSlugs = [];
  }
  function mostrarProdutos(titulo) {
    viewMode = "produtos";
    lastTitulo = titulo || "Produtos";
  }
  function irParaPesosOuFilhos() {
    if (!filhosReaisNo().length) {
      pathExact = temProdutosNoNivelAtual();
      mostrarPesos(pathTitulo() || "Peso");
      return;
    }
    var filhos = opcoesFilhosNo();
    if (filhos.length > 0) {
      renderCardsNivel(filhos);
      mostrarGradeNivel(pathTitulo() || "Categoria");
      return;
    }
    pathExact = false;
    mostrarPesos(pathTitulo() || "Peso");
  }
  function abrirNivel(slug, nome) {
    if (slug === "_geral") {
      pathExact = true;
      mostrarPesos(pathTitulo() + (nome ? " · " + nome : ""));
      return;
    }
    pathExact = false;
    pathStack.push({ slug: slug, nome: nome || slug });
    irParaPesosOuFilhos();
  }
  function abrirCategoria(slug, nome) {
    pathStack = [{ slug: slug || "", nome: nome || "" }];
    pathExact = false;
    pesoAtual = "";
    irParaPesosOuFilhos();
  }
  function abrirListaComPeso(pesoKey) {
    pesoAtual = pesoKey || "";
    var label = pesoKey;
    pesosGrade.forEach(function (g) {
      if (g.key === pesoKey) label = g.label;
    });
    mostrarProdutos((pathTitulo() || "Produtos") + (label ? " · " + label : ""));
  }
  function voltarDoProdutos() {
    mostrarPesos(pathTitulo() || "Peso");
  }
  function voltarPesos() {
    pesoAtual = "";
    if (!pathStack.length) {
      mostrarHome();
      return;
    }
    if (pathExact) {
      pathExact = false;
      if (!filhosReaisNo().length) {
        pathStack.pop();
        if (!pathStack.length) {
          mostrarHome();
          return;
        }
        irParaPesosOuFilhos();
        return;
      }
      var filhosG = opcoesFilhosNo();
      if (filhosG.length > 0) {
        renderCardsNivel(filhosG);
        mostrarGradeNivel(pathTitulo() || "Categoria");
        return;
      }
    }
    pathStack.pop();
    if (!pathStack.length) {
      mostrarHome();
      return;
    }
    irParaPesosOuFilhos();
  }
  function voltarGrade() {
    if (!pathStack.length) {
      mostrarHome();
      return;
    }
    pathStack.pop();
    if (!pathStack.length) {
      mostrarHome();
      return;
    }
    irParaPesosOuFilhos();
  }

  return {
    abrirCategoria: abrirCategoria,
    abrirNivel: abrirNivel,
    abrirListaComPeso: abrirListaComPeso,
    voltarPesos: voltarPesos,
    voltarGrade: voltarGrade,
    voltarDoProdutos: voltarDoProdutos,
    snapshot: function () {
      return {
        view: viewMode,
        path: pathPrefix(),
        titulo: lastTitulo,
        slugs: lastGradeSlugs.slice(),
        pathExact: pathExact,
        pesoAtual: pesoAtual,
        produtos: produtosNoPath().map(function (p) { return p.id; }),
        pesos: Object.keys(pesosDisponiveis()).sort(),
      };
    },
  };
}

function arvoreLoja() {
  return [
    {
      slug: "cao",
      nome: "Cão",
      qtd: 14,
      qtd_exata: 0,
      filhos: [
        {
          slug: "adulto",
          nome: "Adulto",
          qtd: 14,
          qtd_exata: 0,
          filhos: [
            {
              slug: "racas-medias-e-grandes",
              nome: "Raças Médias e Grandes",
              qtd: 14,
              qtd_exata: 14,
              filhos: [],
            },
          ],
        },
      ],
    },
  ];
}

function catalogo14() {
  var out = [];
  for (var i = 1; i <= 14; i++) {
    out.push({
      id: String(i),
      path: "cao/adulto/racas-medias-e-grandes",
      path_slugs: ["cao", "adulto", "racas-medias-e-grandes"],
      peso_keys: i <= 8 ? ["kg:15"] : ["kg:10"],
    });
  }
  return out;
}

var PESOS = [
  { key: "kg:1", label: "Granel" },
  { key: "kg:10", label: "Saco 10 kg" },
  { key: "kg:15", label: "Saco 15 kg" },
];

function checkFonte() {
  var js = fs.readFileSync(
    path.join(__dirname, "..", "produtos/static/produtos/js/catalogo_delivery.js"),
    "utf8"
  );
  [
    "function filhosReaisNo()",
    "function temProdutosNoNivelAtual()",
    "if (!filhosReaisNo().length)",
    "pathExact = temProdutosNoNivelAtual()",
    'slug === "_geral"',
    'nome: "Geral"',
  ].forEach(function (n) {
    if (js.indexOf(n) < 0) fail("JS falta `" + n + "`");
    else ok("fonte `" + n + "`");
  });
  var ir = js.split("function irParaPesosOuFilhos()")[1] || "";
  ir = ir.split("function abrirNivel")[0];
  if (ir.indexOf("mostrarGradeNivel") >= 0 && ir.indexOf("filhosReaisNo().length") >= 0) {
    ok("irParaPesosOuFilhos consulta filhos reais antes da grade");
  } else {
    fail("irParaPesosOuFilhos sem gate filhosReaisNo");
  }
  if (ir.indexOf('nome: "Geral"') >= 0) fail("irParaPesosOuFilhos ainda monta Geral");
  else ok("irParaPesosOuFilhos não monta Geral");
}

function pathLojaSemGeral() {
  var nav = makeNav(arvoreLoja(), catalogo14(), PESOS);
  nav.abrirCategoria("cao", "Cão");
  var s1 = nav.snapshot();
  eq(s1.view, "nivel", "Cão → grade");
  eq(s1.slugs, ["adulto"], "Cão lista só Adulto (sem Geral)");
  if (s1.slugs.indexOf("_geral") >= 0) fail("Cão não pode listar Geral");
  else ok("Cão sem Geral");

  nav.abrirNivel("adulto", "Adulto");
  var s2 = nav.snapshot();
  eq(s2.view, "nivel", "Adulto → grade");
  eq(s2.slugs, ["racas-medias-e-grandes"], "Adulto lista Raças");
  if (s2.slugs.indexOf("_geral") >= 0) fail("Adulto não pode listar Geral");
  else ok("Adulto sem Geral");

  nav.abrirNivel("racas-medias-e-grandes", "Raças Médias e Grandes");
  var s3 = nav.snapshot();
  eq(s3.view, "pesos", "folha → pesos (não grade Geral)");
  eq(s3.pathExact, true, "folha pathExact");
  eq(s3.path, "cao/adulto/racas-medias-e-grandes", "path folha");
  eq(s3.titulo, "Cão · Adulto · Raças Médias e Grandes", "título sem «Geral»");
  if (String(s3.titulo).indexOf("Geral") >= 0) fail("título pesos contém Geral");
  else ok("título pesos sem Geral");
  eq(s3.produtos.length, 14, "14 produtos no path exato");
  eq(s3.pesos, ["kg:10", "kg:15"], "pesos 10+15 na folha");

  nav.abrirListaComPeso("kg:15");
  var s4 = nav.snapshot();
  eq(s4.view, "produtos", "peso → lista");
  eq(s4.pesoAtual, "kg:15", "pesoAtual kg:15");

  nav.voltarDoProdutos();
  eq(nav.snapshot().view, "pesos", "voltar lista → pesos");

  nav.voltarPesos();
  var s5 = nav.snapshot();
  eq(s5.view, "nivel", "voltar pesos → grade Adulto");
  eq(s5.path, "cao/adulto", "stack após voltar da folha");
  eq(s5.slugs, ["racas-medias-e-grandes"], "voltar não reabre Geral");
  if (s5.slugs.indexOf("_geral") >= 0) fail("voltar reabriu Geral");
  else ok("voltar da folha sem Geral");

  nav.voltarGrade();
  eq(nav.snapshot().path, "cao", "voltar grade → Cão");
  nav.voltarGrade();
  eq(nav.snapshot().view, "home", "voltar Cão → home");
}

function pathMistoMantemGeral() {
  var arvore = [
    {
      slug: "cao",
      nome: "Cão",
      qtd: 16,
      qtd_exata: 0,
      filhos: [
        {
          slug: "adulto",
          nome: "Adulto",
          qtd: 16,
          qtd_exata: 2,
          filhos: [
            {
              slug: "premium",
              nome: "Premium",
              qtd: 14,
              qtd_exata: 14,
              filhos: [],
            },
          ],
        },
      ],
    },
  ];
  var cat = catalogo14();
  cat.push({ id: "a", path: "cao/adulto", path_slugs: ["cao", "adulto"], peso_keys: ["kg:1"] });
  cat.push({ id: "b", path: "cao/adulto", path_slugs: ["cao", "adulto"], peso_keys: ["kg:1"] });
  var nav = makeNav(arvore, cat, PESOS);

  nav.abrirCategoria("cao", "Cão");
  nav.abrirNivel("adulto", "Adulto");
  var s = nav.snapshot();
  eq(s.view, "nivel", "misto Adulto → grade");
  eq(s.slugs, ["premium", "_geral"], "misto lista Premium + Geral");

  nav.abrirNivel("_geral", "Geral");
  var g = nav.snapshot();
  eq(g.view, "pesos", "Geral → pesos");
  eq(g.pathExact, true, "Geral pathExact");
  eq(g.path, "cao/adulto", "Geral não empurra _geral no stack");
  eq(g.produtos.sort(), ["a", "b"], "Geral só produtos do nó Adulto");
  eq(g.pesos, ["kg:1"], "Geral só granel dos 2 do nó");
  if (String(g.titulo).indexOf("Geral") < 0) fail("título Geral deveria incluir Geral");
  else ok("título misto inclui Geral");

  nav.voltarPesos();
  var v = nav.snapshot();
  eq(v.view, "nivel", "voltar de Geral → grade Adulto");
  eq(v.slugs, ["premium", "_geral"], "voltar de Geral mantém as duas opções");
  eq(v.path, "cao/adulto", "voltar de Geral não popa o Adulto");
}

function pathRaizSemFilhos() {
  var arvore = [{ slug: "outros", nome: "Outros", qtd: 3, qtd_exata: 3, filhos: [] }];
  var cat = [
    { id: "1", path: "outros", peso_keys: ["kg:5"] },
    { id: "2", path: "outros", peso_keys: ["kg:5"] },
    { id: "3", path: "outros", peso_keys: ["kg:10"] },
  ];
  var nav = makeNav(arvore, cat, PESOS.concat([{ key: "kg:5", label: "Saco 5 kg" }]));
  nav.abrirCategoria("outros", "Outros");
  var s = nav.snapshot();
  eq(s.view, "pesos", "raiz folha → pesos direto");
  eq(s.pathExact, true, "raiz folha pathExact");
  if (s.slugs.indexOf("_geral") >= 0) fail("raiz folha listou Geral");
  else ok("raiz folha sem Geral");
  nav.voltarPesos();
  eq(nav.snapshot().view, "home", "voltar da raiz folha → home");
}

function pathCincoNiveis() {
  function no(slug, filhos) {
    return {
      slug: slug,
      nome: slug,
      qtd: 1,
      qtd_exata: filhos.length ? 0 : 1,
      filhos: filhos,
    };
  }
  var arvore = [no("n1", [no("n2", [no("n3", [no("n4", [no("n5", [])])])])])];
  var cat = [{ id: "x", path: "n1/n2/n3/n4/n5", peso_keys: ["kg:20"] }];
  var nav = makeNav(arvore, cat, [{ key: "kg:20", label: "Saco 20 kg" }]);
  nav.abrirCategoria("n1", "n1");
  nav.abrirNivel("n2", "n2");
  nav.abrirNivel("n3", "n3");
  nav.abrirNivel("n4", "n4");
  eq(nav.snapshot().view, "nivel", "N4 ainda tem filho");
  nav.abrirNivel("n5", "n5");
  var s = nav.snapshot();
  eq(s.view, "pesos", "N5 folha → pesos");
  eq(s.path, "n1/n2/n3/n4/n5", "path 5 níveis");
  eq(s.pesos, ["kg:20"], "peso 20 no N5");
}

function pathFilhoVazioNaoPula() {
  var arvore = [
    {
      slug: "cao",
      nome: "Cão",
      qtd: 1,
      qtd_exata: 1,
      filhos: [{ slug: "filhote", nome: "Filhote", qtd: 0, qtd_exata: 0, filhos: [] }],
    },
  ];
  var cat = [{ id: "1", path: "cao", peso_keys: ["kg:1"] }];
  var nav = makeNav(arvore, cat, PESOS);
  nav.abrirCategoria("cao", "Cão");
  var s = nav.snapshot();
  eq(s.view, "nivel", "filho cadastrado (mesmo vazio) ainda mostra grade");
  eq(s.slugs, ["filhote", "_geral"], "filho vazio + Geral dos produtos do nó");
}

function pathExtraSlugEhFilhoReal() {
  var arvore = [
    {
      slug: "cao",
      nome: "Cão",
      qtd: 2,
      qtd_exata: 0,
      filhos: [
        {
          slug: "extra-produto",
          nome: "extra-produto",
          qtd: 2,
          qtd_exata: 2,
          filhos: [],
        },
      ],
    },
  ];
  var cat = [
    { id: "1", path: "cao/extra-produto", peso_keys: ["kg:10"] },
    { id: "2", path: "cao/extra-produto", peso_keys: ["kg:10"] },
  ];
  var nav = makeNav(arvore, cat, PESOS);
  nav.abrirCategoria("cao", "Cão");
  var s = nav.snapshot();
  eq(s.view, "nivel", "slug extra da árvore é filho real");
  eq(s.slugs, ["extra-produto"], "extra não vira Geral no pai");
  nav.abrirNivel("extra-produto", "extra-produto");
  eq(nav.snapshot().view, "pesos", "extra folha → pesos");
}

function main() {
  console.log("=== VERIFY CATALOGO-SKIP-GERAL ===");
  checkFonte();
  pathLojaSemGeral();
  pathMistoMantemGeral();
  pathRaizSemFilhos();
  pathCincoNiveis();
  pathFilhoVazioNaoPula();
  pathExtraSlugEhFilhoReal();
  console.log("---");
  if (fails) {
    console.log("VERIFY_FAIL (" + fails + ")");
    process.exit(1);
  }
  console.log("VERIFY_OK");
  process.exit(0);
}

main();
