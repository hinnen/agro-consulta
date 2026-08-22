/**
 * Catálogo delivery GM Agro — carrinho + checkout (celular/PC).
 */
(function (global) {
  "use strict";

  function getCookie(n) {
    var v = document.cookie.match("(^|;)\\s*" + n + "\\s*=\\s*([^;]+)");
    return v ? v.pop() : "";
  }

  function fmt(v) {
    return Number(v).toLocaleString("pt-BR", { style: "currency", currency: "BRL" });
  }

  function digitsTel(s) {
    return String(s || "").replace(/\D/g, "");
  }

  function initCatalogoDelivery(opts) {
    opts = opts || {};
    var catalogo = Array.isArray(opts.catalogo) ? opts.catalogo : [];
    var byId = {};
    catalogo.forEach(function (p) {
      byId[String(p.id)] = p;
      var embs = Array.isArray(p.embalagens) ? p.embalagens : [];
      embs.forEach(function (e) {
        var eid = String(e.id || e.produto_id || "");
        if (!eid) return;
        if (!byId[eid]) {
          byId[eid] = {
            id: eid,
            nome: e.nome || p.nome || eid,
            preco: e.preco,
            marca: p.marca || "",
            peso_texto: e.peso_texto || e.rotulo || "",
            imagem: p.imagem || "",
          };
        }
      });
    });
    var carrinho = {};
    var pathStack = []; // [{slug, nome}]
    var pathExact = false;
    var pesoAtual = "";
    var modoBusca = false;
    var viewMode = "home"; // home | nivel | pesos | produtos

    function totalQtd() {
      var n = 0;
      Object.keys(carrinho).forEach(function (k) {
        n += carrinho[k];
      });
      return n;
    }

    function totalValor() {
      var t = 0;
      Object.keys(carrinho).forEach(function (k) {
        var p = byId[k];
        if (p) t += Number(p.preco || 0) * carrinho[k];
      });
      return t;
    }

    function renderBarra() {
      var bar = document.getElementById("barra-carrinho");
      var q = totalQtd();
      if (!bar) return;
      if (q <= 0) {
        bar.classList.add("hidden");
        return;
      }
      bar.classList.remove("hidden");
      var qEl = document.getElementById("carrinho-qtd");
      var tEl = document.getElementById("carrinho-total");
      if (qEl) qEl.textContent = q + (q === 1 ? " item" : " itens");
      if (tEl) tEl.textContent = fmt(totalValor());
    }

    function renderCheckoutItens() {
      var box = document.getElementById("checkout-itens");
      if (!box) return;
      var html = "";
      Object.keys(carrinho).forEach(function (k) {
        var p = byId[k];
        if (!p) return;
        var q = carrinho[k];
        html +=
          '<div class="flex justify-between gap-2"><span class="font-semibold">' +
          q +
          "× " +
          (p.nome || "") +
          '</span><span class="font-black text-emerald-700">' +
          fmt(Number(p.preco || 0) * q) +
          "</span></div>";
      });
      box.innerHTML = html || '<p class="text-slate-500">Carrinho vazio</p>';
    }

    function abrirCheckout() {
      var m = document.getElementById("modal-checkout");
      if (!m) return;
      renderCheckoutItens();
      m.classList.remove("hidden");
      m.classList.add("flex");
      setTimeout(function () {
        var tel = document.getElementById("checkout-telefone");
        if (!tel) return;
        try {
          var salvo = localStorage.getItem("catalogo_checkout_wa_v1") || "";
          if (!digitsTel(tel.value) && salvo.length >= 10) {
            tel.value = salvo;
            tel.dispatchEvent(new Event("input", { bubbles: true }));
          }
        } catch (e) {}
        tel.focus();
      }, 80);
    }

    function fecharCheckout() {
      var m = document.getElementById("modal-checkout");
      if (!m) return;
      m.classList.add("hidden");
      m.classList.remove("flex");
    }

    function addToCart(id) {
      id = String(id || "");
      if (!byId[id]) return;
      carrinho[id] = (carrinho[id] || 0) + 1;
      renderBarra();
    }

    function fecharModalEmbalagem() {
      var m = document.getElementById("modal-embalagem");
      if (!m) return;
      m.classList.add("hidden");
      m.classList.remove("flex");
      m.removeAttribute("data-card-id");
    }

    function abrirModalEmbalagem(cardId) {
      var p = byId[String(cardId)];
      if (!p) return;
      var embs = Array.isArray(p.embalagens) ? p.embalagens.slice() : [];
      if (pesoAtual) {
        embs = embs.filter(function (e) {
          var keys = [];
          var raws = [e.peso_texto || "", e.rotulo || ""];
          raws.forEach(function (raw) {
            var t = String(raw || "")
              .toLowerCase()
              .replace(",", ".")
              .replace(/\s*k\s*g\s*$/, "")
              .trim();
            var map = {
              "1": "kg:1",
              "2.5": "kg:2.5",
              "5": "kg:5",
              "10": "kg:10",
              "15": "kg:15",
              "20": "kg:20",
              "25": "kg:25",
              granel: "kg:1",
            };
            if (t.indexOf("granel") >= 0) keys.push("kg:1");
            else if (map[t]) keys.push(map[t]);
            else {
              var n = parseFloat(t);
              if (!isNaN(n)) {
                [1, 2.5, 5, 10, 15, 20, 25].forEach(function (pKg) {
                  if (Math.abs(n - pKg) <= 0.05) {
                    keys.push(pKg === Math.round(pKg) ? "kg:" + Math.round(pKg) : "kg:" + pKg);
                  }
                });
              }
            }
          });
          return keys.indexOf(pesoAtual) >= 0;
        });
      }
      if (embs.length <= 1) {
        addToCart(embs.length === 1 ? embs[0].id || embs[0].produto_id || p.id : p.id);
        return;
      }
      var m = document.getElementById("modal-embalagem");
      var list = document.getElementById("modal-embalagem-opcoes");
      var tit = document.getElementById("modal-embalagem-titulo");
      if (!m || !list) {
        addToCart(p.id);
        return;
      }
      if (tit) tit.textContent = p.nome || "Escolha a embalagem";
      list.innerHTML = embs
        .map(function (e) {
          var eid = String(e.id || e.produto_id || "");
          return (
            '<button type="button" class="emb-opt w-full flex items-center justify-between gap-3 px-4 py-3.5 rounded-xl border-2 border-emerald-200 bg-white hover:bg-emerald-50 text-left" data-id="' +
            eid +
            '">' +
            '<span class="font-black text-slate-900">' +
            (e.rotulo || e.peso_texto || "Opção") +
            "</span>" +
            '<span class="font-black text-emerald-700 tabular-nums">' +
            fmt(Number(e.preco || 0)) +
            "</span>" +
            "</button>"
          );
        })
        .join("");
      list.querySelectorAll(".emb-opt").forEach(function (btn) {
        btn.addEventListener("click", function () {
          addToCart(btn.getAttribute("data-id"));
          fecharModalEmbalagem();
        });
      });
      m.setAttribute("data-card-id", String(cardId));
      m.classList.remove("hidden");
      m.classList.add("flex");
    }

    document.querySelectorAll(".btn-add").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var id = String(btn.getAttribute("data-id") || "");
        if (!byId[id]) return;
        abrirModalEmbalagem(id);
      });
    });

    var embClose = document.getElementById("modal-embalagem-fechar");
    if (embClose) embClose.addEventListener("click", fecharModalEmbalagem);
    var embRoot = document.getElementById("modal-embalagem");
    if (embRoot) {
      embRoot.addEventListener("click", function (ev) {
        if (ev.target === embRoot) fecharModalEmbalagem();
      });
    }

    var busca = document.getElementById("busca-catalogo");
    var homeEl = document.getElementById("home-categorias");
    var viewSubs = document.getElementById("view-subcategorias");
    var viewPesos = document.getElementById("view-pesos");
    var viewProd = document.getElementById("view-produtos");
    var gradeSubs = document.getElementById("grade-subcategorias");
    var gradePesos = document.getElementById("grade-pesos");
    var pesosVazio = document.getElementById("pesos-vazio");
    var tituloSubPasso = document.getElementById("titulo-sub-passo");
    var tituloSubAjuda = document.getElementById("titulo-sub-ajuda");
    var tituloPesoPasso = document.getElementById("titulo-peso-passo");
    var tituloCat = document.getElementById("titulo-cat-atual");
    var listaVazia = document.getElementById("lista-vazia-cat");
    var arvore = Array.isArray(opts.arvore) ? opts.arvore : [];
    var pesosGrade = Array.isArray(opts.pesosGrade) ? opts.pesosGrade : [];
    var arvoreBySlug = {};
    function indexArvore(nodes, map) {
      (nodes || []).forEach(function (n) {
        map[n.slug] = n;
        indexArvore(n.filhos || [], map);
      });
    }
    indexArvore(arvore, arvoreBySlug);

    function pathPrefix() {
      return pathStack.map(function (x) { return x.slug; }).join("/");
    }
    function pathTitulo() {
      return pathStack.map(function (x) { return x.nome; }).filter(Boolean).join(" · ");
    }
    function escChip(s) {
      return String(s || "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;");
    }
    function renderPathChips(el, titulo) {
      if (!el) return;
      var parts = String(titulo || "")
        .split(/\s*[·•]\s*/)
        .map(function (s) { return s.trim(); })
        .filter(Boolean);
      if (!parts.length) {
        el.innerHTML = '<span class="card-filtro-chip is-atual">Catálogo</span>';
        return;
      }
      el.innerHTML = parts
        .map(function (nome, i) {
          var last = i === parts.length - 1;
          var chip =
            '<span class="card-filtro-chip' +
            (last ? " is-atual" : "") +
            '">' +
            escChip(nome) +
            "</span>";
          if (!last) chip += '<span class="card-filtro-sep" aria-hidden="true">›</span>';
          return chip;
        })
        .join("");
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

    function esconderTodasViews() {
      if (homeEl) homeEl.classList.add("hidden");
      if (viewSubs) viewSubs.classList.add("hidden");
      if (viewPesos) viewPesos.classList.add("hidden");
      if (viewProd) viewProd.classList.add("hidden");
    }

    function mostrarHome() {
      pathStack = [];
      pathExact = false;
      pesoAtual = "";
      modoBusca = false;
      viewMode = "home";
      esconderTodasViews();
      if (homeEl) homeEl.classList.remove("hidden");
      if (busca) busca.value = "";
      window.scrollTo({ top: 0, behavior: "smooth" });
    }

    function mostrarGradeNivel(titulo, ajuda) {
      viewMode = "nivel";
      esconderTodasViews();
      if (viewSubs) viewSubs.classList.remove("hidden");
      if (tituloSubPasso) renderPathChips(tituloSubPasso, titulo || "Categoria");
      if (tituloSubAjuda) tituloSubAjuda.textContent = ajuda || "Escolha";
      window.scrollTo({ top: 0, behavior: "smooth" });
    }

    function mostrarPesos(titulo) {
      viewMode = "pesos";
      pesoAtual = "";
      esconderTodasViews();
      if (viewPesos) viewPesos.classList.remove("hidden");
      if (tituloPesoPasso) renderPathChips(tituloPesoPasso, titulo || "Peso");
      renderPesos();
      window.scrollTo({ top: 0, behavior: "smooth" });
    }

    function mostrarProdutos(titulo) {
      viewMode = "produtos";
      esconderTodasViews();
      if (viewProd) viewProd.classList.remove("hidden");
      if (tituloCat) renderPathChips(tituloCat, titulo || "Produtos");
      window.scrollTo({ top: 0, behavior: "smooth" });
    }

    function produtosNoPath() {
      var pref = pathPrefix();
      var out = [];
      catalogo.forEach(function (p) {
        var path = String(p.path || (p.path_slugs || []).join("/") || "");
        if (!pref) {
          out.push(p);
          return;
        }
        if (pathExact) {
          if (path === pref) out.push(p);
        } else if (path === pref || path.indexOf(pref + "/") === 0) {
          out.push(p);
        }
      });
      return out;
    }

    function pesosDisponiveis() {
      var set = {};
      produtosNoPath().forEach(function (p) {
        (p.peso_keys || []).forEach(function (k) {
          set[k] = true;
        });
      });
      // Também pelos data-pesos do DOM se JSON antigo
      document.querySelectorAll(".produto-linha").forEach(function (el) {
        var path = el.getAttribute("data-path") || "";
        var pref = pathPrefix();
        var okPath = !pref
          ? true
          : pathExact
            ? path === pref
            : path === pref || path.indexOf(pref + "/") === 0;
        if (!okPath) return;
        String(el.getAttribute("data-pesos") || "")
          .split(",")
          .forEach(function (k) {
            k = String(k || "").trim();
            if (k) set[k] = true;
          });
      });
      return set;
    }

    function renderPesos() {
      if (!gradePesos) return;
      var avail = pesosDisponiveis();
      var html = "";
      pesosGrade.forEach(function (g) {
        if (!avail[g.key]) return;
        html +=
          '<button type="button" class="card-cat peso-home-card" data-peso="' +
          g.key +
          '">' +
          '<div class="card-cat-ph">' +
          (g.label || "?").charAt(0) +
          "</div>" +
          '<div class="px-2.5 py-2.5">' +
          '<p class="font-black text-slate-900 text-[0.95rem] leading-tight">' +
          (g.label || g.key) +
          "</p></div></button>";
      });
      gradePesos.innerHTML = html;
      if (pesosVazio) pesosVazio.classList.toggle("hidden", !!html);
      gradePesos.querySelectorAll(".peso-home-card").forEach(function (btn) {
        btn.addEventListener("click", function () {
          abrirListaComPeso(String(btn.getAttribute("data-peso") || ""));
        });
      });
    }

    function renderCardsNivel(lista, onPick) {
      if (!gradeSubs) return;
      var html = "";
      lista.forEach(function (s) {
        var letra = (s.nome || "?").charAt(0).toUpperCase();
        html +=
          '<button type="button" class="card-cat sub-home-card" data-slug="' +
          s.slug +
          '" data-nome="' +
          String(s.nome || "").replace(/"/g, "&quot;") +
          '">' +
          '<div class="card-cat-ph">' +
          letra +
          "</div>" +
          '<div class="px-2.5 py-2.5">' +
          '<p class="font-black text-slate-900 text-[0.95rem] leading-tight">' +
          (s.nome || "") +
          "</p>" +
          '<p class="text-[0.7rem] font-semibold text-slate-500 mt-0.5">' +
          (s.qtd
            ? s.qtd + " produto" + (s.qtd !== 1 ? "s" : "")
            : "Em breve") +
          "</p></div></button>";
      });
      gradeSubs.innerHTML =
        html || '<p class="col-span-2 text-sm text-slate-500 py-8 text-center">Nenhuma opção.</p>';
      gradeSubs.querySelectorAll(".sub-home-card").forEach(function (card) {
        card.addEventListener("click", function () {
          onPick(
            String(card.getAttribute("data-slug") || ""),
            String(card.getAttribute("data-nome") || "")
          );
        });
      });
    }

    function aplicarFiltros() {
      var q = String((busca && busca.value) || "")
        .toLowerCase()
        .trim();
      var pref = pathPrefix();
      var algum = false;
      document.querySelectorAll(".produto-linha").forEach(function (el) {
        var nome = el.getAttribute("data-nome") || "";
        var path = el.getAttribute("data-path") || "";
        var pesos = String(el.getAttribute("data-pesos") || "");
        var matchQ = !q || nome.indexOf(q) >= 0;
        var matchPath = true;
        if (!modoBusca && pref) {
          if (pathExact) matchPath = path === pref;
          else matchPath = path === pref || path.indexOf(pref + "/") === 0;
        }
        var matchPeso = true;
        if (!modoBusca && pesoAtual) {
          matchPeso = ("," + pesos + ",").indexOf("," + pesoAtual + ",") >= 0;
        }
        var ok = matchQ && matchPath && matchPeso;
        el.classList.toggle("hidden", !ok);
        if (ok) algum = true;
      });
      document.querySelectorAll(".secao-cat").forEach(function (sec) {
        var visible = sec.querySelectorAll(".produto-linha:not(.hidden)").length > 0;
        sec.classList.toggle("hidden", !visible);
      });
      if (listaVazia) listaVazia.classList.toggle("hidden", algum);
    }

    function irParaPesosOuFilhos() {
      // Sem subcategorias reais: último nível preenchido → peso da embalagem (sem passo «Geral»).
      if (!filhosReaisNo().length) {
        pathExact = temProdutosNoNivelAtual();
        mostrarPesos(pathTitulo() || "Peso");
        return;
      }
      var filhos = opcoesFilhosNo();
      if (filhos.length > 0) {
        renderCardsNivel(filhos, abrirNivel);
        mostrarGradeNivel(pathTitulo() || "Categoria", "Escolha a próxima categoria");
        return;
      }
      pathExact = false;
      mostrarPesos(pathTitulo() || "Peso");
    }

    function abrirNivel(slug, nome) {
      modoBusca = false;
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
      modoBusca = false;
      pathStack = [{ slug: slug || "", nome: nome || "" }];
      pathExact = false;
      pesoAtual = "";
      if (busca) busca.value = "";
      irParaPesosOuFilhos();
    }

    function abrirListaComPeso(pesoKey) {
      pesoAtual = pesoKey || "";
      var label = pesoKey;
      pesosGrade.forEach(function (g) {
        if (g.key === pesoKey) label = g.label;
      });
      mostrarProdutos(
        (pathTitulo() || "Produtos") + (label ? " · " + label : "")
      );
      aplicarFiltros();
    }

    function voltarDoProdutos() {
      if (modoBusca) {
        mostrarHome();
        return;
      }
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
          renderCardsNivel(filhosG, abrirNivel);
          mostrarGradeNivel(pathTitulo() || "Categoria", "Escolha a próxima categoria");
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

    document.querySelectorAll(".cat-home-card").forEach(function (card) {
      card.addEventListener("click", function () {
        abrirCategoria(
          String(card.getAttribute("data-cat") || ""),
          String(card.getAttribute("data-nome") || "")
        );
      });
    });

    var btnVoltarHome = document.getElementById("btn-voltar-home");
    if (btnVoltarHome) btnVoltarHome.addEventListener("click", voltarGrade);

    var btnVoltarPesos = document.getElementById("btn-voltar-pesos");
    if (btnVoltarPesos) btnVoltarPesos.addEventListener("click", voltarPesos);

    var btnVoltar = document.getElementById("btn-voltar-cats");
    if (btnVoltar) btnVoltar.addEventListener("click", voltarDoProdutos);

    if (busca) {
      busca.addEventListener("input", function () {
        var q = String(busca.value || "")
          .toLowerCase()
          .trim();
        if (q) {
          modoBusca = true;
          pathStack = [];
          pathExact = false;
          pesoAtual = "";
          mostrarProdutos("Busca");
          aplicarFiltros();
        } else if (modoBusca) {
          mostrarHome();
        } else if (viewMode === "produtos") {
          aplicarFiltros();
        }
      });
    }

    var btnOpen = document.getElementById("btn-abrir-checkout");
    if (btnOpen) btnOpen.addEventListener("click", abrirCheckout);
    var btnClose = document.getElementById("btn-fechar-checkout");
    if (btnClose) btnClose.addEventListener("click", fecharCheckout);

    var forma = document.getElementById("checkout-forma");
    var trocoWrap = document.getElementById("checkout-troco-wrap");
    if (forma && trocoWrap) {
      forma.addEventListener("change", function () {
        trocoWrap.classList.toggle("hidden", forma.value !== "Dinheiro");
      });
    }

    function val(id) {
      var el = document.getElementById(id);
      return el ? String(el.value || "").trim() : "";
    }

    function setVal(id, v) {
      var el = document.getElementById(id);
      if (el) el.value = v != null ? String(v) : "";
    }

    function setGeoStatus(msg, ok) {
      var el = document.getElementById("checkout-geo-status");
      if (!el) return;
      if (!msg) {
        el.classList.add("hidden");
        el.textContent = "";
        return;
      }
      el.textContent = msg;
      el.classList.remove("hidden");
      el.className =
        "text-xs font-medium " + (ok ? "text-emerald-700" : "text-rose-700");
    }

    function setCheckoutErro(msg) {
      var el = document.getElementById("checkout-erro");
      if (!el) return;
      if (!msg) {
        el.classList.add("hidden");
        el.textContent = "";
        return;
      }
      el.textContent = msg;
      el.classList.remove("hidden");
    }

    function syncEnderecoHidden() {
      var parts = [val("checkout-logradouro"), val("checkout-numero"), val("checkout-bairro"), val("checkout-cidade"), val("checkout-uf")];
      var linha = parts.filter(Boolean).join(", ");
      setVal("checkout-endereco", linha);
    }

    function preencherCamposEndereco(data) {
      if (!data) return;
      var map = {
        "checkout-cidade": data.cidade,
        "checkout-logradouro": data.logradouro,
        "checkout-numero": data.numero,
        "checkout-bairro": data.bairro,
        "checkout-uf": data.uf,
        "checkout-cep": data.cep,
        "checkout-endereco": data.endereco_linha,
      };
      Object.keys(map).forEach(function (id) {
        if (map[id] != null && String(map[id]).trim()) setVal(id, map[id]);
      });
      syncEnderecoHidden();
    }

    function setEnderecoModo(modo) {
      var manual = document.getElementById("checkout-endereco-manual");
      var trocar = document.getElementById("checkout-trocar-manual");
      var btnGeo = document.getElementById("btn-usar-localizacao");
      var geo = modo === "geo";
      if (manual) manual.classList.toggle("hidden", geo);
      if (trocar) trocar.classList.toggle("hidden", !geo);
      if (btnGeo && geo) {
        btnGeo.textContent = "✓ Localização definida";
        btnGeo.classList.add("opacity-80");
      } else if (btnGeo) {
        btnGeo.textContent = "📍 Usar minha localização (Plus Code automático)";
        btnGeo.classList.remove("opacity-80");
      }
    }

    function limparLocalizacao() {
      ["checkout-plus-code", "checkout-lat", "checkout-lng", "checkout-maps-url"].forEach(function (id) {
        setVal(id, "");
      });
      setGeoStatus("", true);
    }

    function preencherLocalizacao(data) {
      setVal("checkout-plus-code", data.plus_code);
      setVal("checkout-lat", data.lat);
      setVal("checkout-lng", data.lng);
      setVal("checkout-maps-url", data.maps_url);
      preencherCamposEndereco(data);
      if (data.plus_code) setEnderecoModo("geo");
    }

    var btnTrocarManual = document.getElementById("checkout-trocar-manual");
    if (btnTrocarManual) {
      btnTrocarManual.addEventListener("click", function () {
        limparLocalizacao();
        setEnderecoModo("manual");
      });
    }

    if (val("checkout-plus-code")) setEnderecoModo("geo");

    var btnGeo = document.getElementById("btn-usar-localizacao");
    if (btnGeo && opts.apiLocalizacao) {
      btnGeo.addEventListener("click", function () {
        if (!navigator.geolocation) {
          setGeoStatus("Seu aparelho não suporta GPS.", false);
          return;
        }
        btnGeo.disabled = true;
        btnGeo.textContent = "Obtendo localização…";
        setGeoStatus("Aguarde — pedindo permissão do GPS.", true);
        navigator.geolocation.getCurrentPosition(
          function (pos) {
            fetch(opts.apiLocalizacao, {
              method: "POST",
              credentials: "same-origin",
              headers: {
                "Content-Type": "application/json",
                "X-CSRFToken": getCookie("csrftoken"),
              },
              body: JSON.stringify({
                lat: pos.coords.latitude,
                lng: pos.coords.longitude,
              }),
            })
              .then(function (r) {
                return r.json();
              })
              .then(function (data) {
                if (!data || !data.ok) throw new Error((data && data.erro) || "Erro ao localizar");
                preencherLocalizacao(data);
                setGeoStatus("Localização OK para a entrega.", true);
              })
              .catch(function (ex) {
                setGeoStatus((ex && ex.message) || "Falha ao obter endereço.", false);
              })
              .finally(function () {
                btnGeo.disabled = false;
                if (!val("checkout-plus-code")) {
                  btnGeo.textContent = "📍 Usar minha localização (Plus Code automático)";
                } else {
                  btnGeo.textContent = "✓ Localização definida";
                }
              });
          },
          function (err) {
            btnGeo.disabled = false;
            btnGeo.textContent = "📍 Usar minha localização (Plus Code automático)";
            var msg = "Não foi possível usar o GPS.";
            if (err && err.code === 1) msg = "Permita o acesso à localização no navegador.";
            setGeoStatus(msg, false);
          },
          { enableHighAccuracy: true, timeout: 20000, maximumAge: 60000 }
        );
      });
    }

    var tel = document.getElementById("checkout-telefone");
    var telTimer = null;

    function setClienteHint(msg, ok) {
      var hint = document.getElementById("checkout-cliente-hint");
      if (!hint) return;
      if (!msg) {
        hint.classList.add("hidden");
        hint.textContent = "";
        return;
      }
      hint.textContent = msg;
      hint.classList.remove("hidden");
      hint.className =
        "text-xs font-medium mt-1 block " +
        (ok === true ? "text-emerald-700" : ok === false ? "text-slate-500" : "text-slate-600");
    }

    if (tel && opts.apiCliente) {
      tel.addEventListener("input", function () {
        clearTimeout(telTimer);
        telTimer = setTimeout(function () {
          var d = digitsTel(tel.value);
          if (d.length < 10) {
            setClienteHint("", true);
            return;
          }
          setClienteHint("Buscando cadastro…", null);
          fetch(opts.apiCliente + "?telefone=" + encodeURIComponent(d), {
            credentials: "same-origin",
          })
            .then(function (r) {
              return r.json();
            })
            .then(function (j) {
              if (!j || !j.encontrado || !j.cliente) {
                setClienteHint("WhatsApp novo — preencha nome e endereço abaixo.", false);
                return;
              }
              var c = j.cliente;
              if (c.nome) setVal("checkout-nome", c.nome);
              if (c.cidade) setVal("checkout-cidade", c.cidade);
              if (c.logradouro) setVal("checkout-logradouro", c.logradouro);
              if (c.numero) setVal("checkout-numero", c.numero);
              if (c.bairro) setVal("checkout-bairro", c.bairro);
              if (c.uf) setVal("checkout-uf", c.uf);
              if (c.cep) setVal("checkout-cep", c.cep);
              if (c.plus_code) {
                setVal("checkout-plus-code", c.plus_code);
                setEnderecoModo("geo");
              }
              if (c.maps_url) setVal("checkout-maps-url", c.maps_url);
              syncEnderecoHidden();
              try {
                localStorage.setItem("catalogo_checkout_wa_v1", d);
              } catch (e) {}
              setClienteHint("Cadastro encontrado — nome e endereço preenchidos. Confira se está certo.", true);
            })
            .catch(function () {
              setClienteHint("", true);
            });
        }, 400);
      });
    }

    var form = document.getElementById("form-pedido");
    if (form) {
      form.addEventListener("submit", function (ev) {
        ev.preventDefault();
        setCheckoutErro("");
        if (totalQtd() <= 0) {
          setCheckoutErro("Carrinho vazio.");
          return;
        }
        syncEnderecoHidden();
        var plus = val("checkout-plus-code");
        if (!plus && !(val("checkout-cidade") && val("checkout-logradouro") && val("checkout-numero"))) {
          setCheckoutErro("Informe cidade, logradouro e número — ou use a localização.");
          return;
        }
        var btn = document.getElementById("btn-enviar-pedido");
        if (btn) {
          btn.disabled = true;
          btn.textContent = "Enviando…";
        }
        var itens = Object.keys(carrinho).map(function (k) {
          return { produto_id: k, qtd: carrinho[k] };
        });
        var trocoEl = document.getElementById("checkout-troco");
        var payload = {
          cliente_nome: val("checkout-nome"),
          telefone: val("checkout-telefone"),
          cidade: val("checkout-cidade"),
          logradouro: val("checkout-logradouro"),
          numero: val("checkout-numero"),
          bairro: val("checkout-bairro"),
          uf: val("checkout-uf") || "SP",
          cep: val("checkout-cep"),
          plus_code: plus,
          maps_url: val("checkout-maps-url"),
          endereco_linha: val("checkout-endereco"),
          forma_pagamento: val("checkout-forma"),
          troco_precisa: !!(trocoEl && trocoEl.checked),
          observacoes: val("checkout-obs"),
          itens: itens,
        };
        fetch(opts.apiPedido, {
          method: "POST",
          credentials: "same-origin",
          headers: {
            "Content-Type": "application/json",
            "X-CSRFToken": getCookie("csrftoken"),
          },
          body: JSON.stringify(payload),
        })
          .then(function (r) {
            return r.json().then(function (j) {
              return { okHttp: r.ok, j: j };
            });
          })
          .then(function (res) {
            if (!res.j || !res.j.ok) {
              throw new Error((res.j && res.j.erro) || "Falha ao enviar pedido");
            }
            window.location.href = res.j.redirect || "/catalogo/pedido-ok/?id=" + res.j.id;
          })
          .catch(function (err) {
            setCheckoutErro(err.message || "Erro ao enviar");
            if (btn) {
              btn.disabled = false;
              btn.textContent = "Enviar pedido";
            }
          });
      });
    }
  }

  global.initCatalogoDelivery = initCatalogoDelivery;
})(typeof window !== "undefined" ? window : this);
