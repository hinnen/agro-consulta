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
    });
    var carrinho = {};

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
    }

    function fecharCheckout() {
      var m = document.getElementById("modal-checkout");
      if (!m) return;
      m.classList.add("hidden");
      m.classList.remove("flex");
    }

    document.querySelectorAll(".btn-add").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var id = String(btn.getAttribute("data-id") || "");
        if (!byId[id]) return;
        carrinho[id] = (carrinho[id] || 0) + 1;
        renderBarra();
      });
    });

    var busca = document.getElementById("busca-catalogo");
    var homeEl = document.getElementById("home-categorias");
    var viewSubs = document.getElementById("view-subcategorias");
    var viewProd = document.getElementById("view-produtos");
    var gradeSubs = document.getElementById("grade-subcategorias");
    var tituloSubPasso = document.getElementById("titulo-sub-passo");
    var tituloCat = document.getElementById("titulo-cat-atual");
    var listaVazia = document.getElementById("lista-vazia-cat");
    var arvore = Array.isArray(opts.arvore) ? opts.arvore : [];
    var arvoreBySlug = {};
    arvore.forEach(function (c) {
      arvoreBySlug[c.slug] = c;
    });
    var catAtual = "";
    var catNomeAtual = "";
    var subAtual = "";
    var veioDeSubs = false;
    var modoBusca = false;

    function esconderTodasViews() {
      if (homeEl) homeEl.classList.add("hidden");
      if (viewSubs) viewSubs.classList.add("hidden");
      if (viewProd) viewProd.classList.add("hidden");
    }

    function mostrarHome() {
      catAtual = "";
      catNomeAtual = "";
      subAtual = "";
      veioDeSubs = false;
      modoBusca = false;
      esconderTodasViews();
      if (homeEl) homeEl.classList.remove("hidden");
      if (busca) busca.value = "";
      window.scrollTo({ top: 0, behavior: "smooth" });
    }

    function mostrarSubs() {
      esconderTodasViews();
      if (viewSubs) viewSubs.classList.remove("hidden");
      if (tituloSubPasso) tituloSubPasso.textContent = catNomeAtual || "Subcategoria";
      window.scrollTo({ top: 0, behavior: "smooth" });
    }

    function mostrarProdutos(titulo) {
      esconderTodasViews();
      if (viewProd) viewProd.classList.remove("hidden");
      if (tituloCat) tituloCat.textContent = titulo || "Produtos";
      window.scrollTo({ top: 0, behavior: "smooth" });
    }

    function opcoesSubDaCategoria(slug) {
      var info = arvoreBySlug[slug];
      if (!info) return [];
      var optsSub = [];
      (info.filhos || []).forEach(function (f) {
        optsSub.push({
          slug: f.slug,
          nome: f.nome,
          qtd: f.qtd || 0,
        });
      });
      if (info.qtd_sem_sub > 0) {
        optsSub.push({
          slug: "_geral",
          nome: "Geral",
          qtd: info.qtd_sem_sub,
        });
      }
      return optsSub;
    }

    function renderCardsSub(lista) {
      if (!gradeSubs) return;
      var html = "";
      lista.forEach(function (s) {
        var letra = (s.nome || "?").charAt(0).toUpperCase();
        html +=
          '<button type="button" class="card-cat sub-home-card" data-sub="' +
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
      gradeSubs.innerHTML = html || '<p class="col-span-2 text-sm text-slate-500 py-8 text-center">Sem subcategorias.</p>';
      gradeSubs.querySelectorAll(".sub-home-card").forEach(function (card) {
        card.addEventListener("click", function () {
          abrirSubcategoria(
            String(card.getAttribute("data-sub") || ""),
            String(card.getAttribute("data-nome") || "")
          );
        });
      });
    }

    function aplicarFiltros() {
      var q = String((busca && busca.value) || "")
        .toLowerCase()
        .trim();
      var algum = false;
      document.querySelectorAll(".produto-linha").forEach(function (el) {
        var nome = el.getAttribute("data-nome") || "";
        var pc = el.getAttribute("data-cat") || "";
        var matchQ = !q || nome.indexOf(q) >= 0;
        var matchCat = !catAtual || pc === catAtual;
        var matchSub = true;
        if (subAtual) {
          var bloco = el.closest(".bloco-sub");
          var bs = bloco ? String(bloco.getAttribute("data-sub") || "") : "";
          matchSub = bs === subAtual;
        }
        var ok = matchQ && matchCat && matchSub;
        el.classList.toggle("hidden", !ok);
        if (ok) algum = true;
      });
      document.querySelectorAll(".secao-cat").forEach(function (sec) {
        var sc = String(sec.getAttribute("data-cat") || "");
        var matchCat = !catAtual || sc === catAtual;
        var visible = sec.querySelectorAll(".produto-linha:not(.hidden)").length > 0;
        sec.classList.toggle("hidden", !(matchCat && visible));
      });
      document.querySelectorAll(".bloco-sub").forEach(function (b) {
        var visible = b.querySelectorAll(".produto-linha:not(.hidden)").length > 0;
        b.classList.toggle("hidden", !visible);
      });
      if (listaVazia) listaVazia.classList.toggle("hidden", algum);
    }

    function abrirProdutosFiltrados(titulo) {
      modoBusca = false;
      mostrarProdutos(titulo);
      aplicarFiltros();
    }

    function abrirSubcategoria(slugSub, nomeSub) {
      subAtual = slugSub || "";
      veioDeSubs = true;
      var titulo =
        (catNomeAtual || "") +
        (nomeSub ? " · " + nomeSub : "");
      abrirProdutosFiltrados(titulo || "Produtos");
    }

    function abrirCategoria(slug, nome) {
      modoBusca = false;
      catAtual = slug || "";
      catNomeAtual = nome || "";
      subAtual = "";
      veioDeSubs = false;
      if (busca) busca.value = "";
      var subs = opcoesSubDaCategoria(catAtual);
      if (subs.length > 0) {
        renderCardsSub(subs);
        mostrarSubs();
        return;
      }
      // Sem subcategoria cadastrada: vai direto aos produtos da categoria
      abrirProdutosFiltrados(catNomeAtual || "Produtos");
    }

    function voltarDoProdutos() {
      if (modoBusca) {
        mostrarHome();
        return;
      }
      if (veioDeSubs && catAtual) {
        subAtual = "";
        var subs = opcoesSubDaCategoria(catAtual);
        renderCardsSub(subs);
        mostrarSubs();
        return;
      }
      mostrarHome();
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
    if (btnVoltarHome) btnVoltarHome.addEventListener("click", mostrarHome);

    var btnVoltar = document.getElementById("btn-voltar-cats");
    if (btnVoltar) btnVoltar.addEventListener("click", voltarDoProdutos);

    if (busca) {
      busca.addEventListener("input", function () {
        var q = String(busca.value || "")
          .toLowerCase()
          .trim();
        if (q) {
          modoBusca = true;
          catAtual = "";
          catNomeAtual = "";
          subAtual = "";
          veioDeSubs = false;
          mostrarProdutos("Busca");
          aplicarFiltros();
        } else if (modoBusca) {
          mostrarHome();
        } else if (catAtual && (subAtual || !opcoesSubDaCategoria(catAtual).length)) {
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

    var tel = document.getElementById("checkout-telefone");
    var hint = document.getElementById("checkout-cliente-hint");
    var telTimer = null;
    if (tel && opts.apiCliente) {
      tel.addEventListener("input", function () {
        clearTimeout(telTimer);
        telTimer = setTimeout(function () {
          var d = digitsTel(tel.value);
          if (d.length < 10) {
            if (hint) hint.classList.add("hidden");
            return;
          }
          fetch(opts.apiCliente + "?telefone=" + encodeURIComponent(d), {
            credentials: "same-origin",
          })
            .then(function (r) {
              return r.json();
            })
            .then(function (j) {
              if (!j || !j.encontrado || !j.cliente) {
                if (hint) hint.classList.add("hidden");
                return;
              }
              var c = j.cliente;
              var nome = document.getElementById("checkout-nome");
              if (nome && c.nome) nome.value = c.nome;
              var cid = document.getElementById("checkout-cidade");
              if (cid && c.cidade) cid.value = c.cidade;
              var log = document.getElementById("checkout-logradouro");
              if (log && c.logradouro) log.value = c.logradouro;
              var num = document.getElementById("checkout-numero");
              if (num && c.numero) num.value = c.numero;
              var bai = document.getElementById("checkout-bairro");
              if (bai && c.bairro) bai.value = c.bairro;
              if (hint) hint.classList.remove("hidden");
            })
            .catch(function () {});
        }, 400);
      });
    }

    var form = document.getElementById("form-pedido");
    if (form) {
      form.addEventListener("submit", function (ev) {
        ev.preventDefault();
        if (totalQtd() <= 0) {
          alert("Carrinho vazio.");
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
          cliente_nome: (document.getElementById("checkout-nome") || {}).value || "",
          telefone: (document.getElementById("checkout-telefone") || {}).value || "",
          cidade: (document.getElementById("checkout-cidade") || {}).value || "",
          logradouro: (document.getElementById("checkout-logradouro") || {}).value || "",
          numero: (document.getElementById("checkout-numero") || {}).value || "",
          bairro: (document.getElementById("checkout-bairro") || {}).value || "",
          uf: "SP",
          forma_pagamento: (document.getElementById("checkout-forma") || {}).value || "",
          troco_precisa: !!(trocoEl && trocoEl.checked),
          observacoes: (document.getElementById("checkout-obs") || {}).value || "",
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
            alert(err.message || "Erro ao enviar");
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
