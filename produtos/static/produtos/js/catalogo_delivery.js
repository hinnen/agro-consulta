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
    var tituloSubAjuda = document.getElementById("titulo-sub-ajuda");
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
    var subNomeAtual = "";
    var sub2Atual = "";
    var nivelPasso = 0; // 0 home · 1 cat·subs · 2 sub·subs2 · 3 produtos
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
      subNomeAtual = "";
      sub2Atual = "";
      nivelPasso = 0;
      modoBusca = false;
      esconderTodasViews();
      if (homeEl) homeEl.classList.remove("hidden");
      if (busca) busca.value = "";
      window.scrollTo({ top: 0, behavior: "smooth" });
    }

    function mostrarGradeNivel(titulo, ajuda) {
      esconderTodasViews();
      if (viewSubs) viewSubs.classList.remove("hidden");
      if (tituloSubPasso) tituloSubPasso.textContent = titulo || "";
      if (tituloSubAjuda) tituloSubAjuda.textContent = ajuda || "Escolha";
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
          filhos: f.filhos || [],
          qtd_sem_sub2: f.qtd_sem_sub2 || 0,
        });
      });
      if (info.qtd_sem_sub > 0) {
        optsSub.push({
          slug: "_geral",
          nome: "Geral",
          qtd: info.qtd_sem_sub,
          filhos: [],
          qtd_sem_sub2: 0,
        });
      }
      return optsSub;
    }

    function opcoesSub2(slugCat, slugSub) {
      var info = arvoreBySlug[slugCat];
      if (!info) return [];
      var sub = null;
      (info.filhos || []).forEach(function (f) {
        if (f.slug === slugSub) sub = f;
      });
      if (!sub) return [];
      var opts2 = [];
      (sub.filhos || []).forEach(function (n) {
        opts2.push({ slug: n.slug, nome: n.nome, qtd: n.qtd || 0 });
      });
      if (sub.qtd_sem_sub2 > 0) {
        opts2.push({
          slug: "_geral",
          nome: "Geral",
          qtd: sub.qtd_sem_sub2,
        });
      }
      return opts2;
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
      var algum = false;
      document.querySelectorAll(".produto-linha").forEach(function (el) {
        var nome = el.getAttribute("data-nome") || "";
        var pc = el.getAttribute("data-cat") || "";
        var matchQ = !q || nome.indexOf(q) >= 0;
        var matchCat = !catAtual || pc === catAtual;
        var matchSub = true;
        var matchSub2 = true;
        if (subAtual) {
          var bloco = el.closest(".bloco-sub");
          var bs = bloco ? String(bloco.getAttribute("data-sub") || "") : "";
          matchSub = bs === subAtual;
        }
        if (sub2Atual) {
          var b2 = el.closest(".bloco-sub2");
          var bs2 = b2 ? String(b2.getAttribute("data-sub2") || "") : "";
          matchSub2 = bs2 === sub2Atual;
        }
        var ok = matchQ && matchCat && matchSub && matchSub2;
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
      document.querySelectorAll(".bloco-sub2").forEach(function (b) {
        var visible = b.querySelectorAll(".produto-linha:not(.hidden)").length > 0;
        b.classList.toggle("hidden", !visible);
      });
      if (listaVazia) listaVazia.classList.toggle("hidden", algum);
    }

    function abrirProdutosFiltrados(titulo) {
      modoBusca = false;
      nivelPasso = 3;
      mostrarProdutos(titulo);
      aplicarFiltros();
    }

    function abrirSub2(slug2, nome2) {
      sub2Atual = slug2 || "";
      var titulo =
        (catNomeAtual || "") +
        (subNomeAtual ? " · " + subNomeAtual : "") +
        (nome2 ? " · " + nome2 : "");
      abrirProdutosFiltrados(titulo || "Produtos");
    }

    function abrirSubcategoria(slugSub, nomeSub) {
      subAtual = slugSub || "";
      subNomeAtual = nomeSub || "";
      sub2Atual = "";
      if (slugSub === "_geral") {
        abrirProdutosFiltrados(
          (catNomeAtual || "") + (nomeSub ? " · " + nomeSub : "")
        );
        return;
      }
      var netos = opcoesSub2(catAtual, subAtual);
      if (netos.length > 0) {
        nivelPasso = 2;
        renderCardsNivel(netos, abrirSub2);
        mostrarGradeNivel(
          (catNomeAtual || "") + (nomeSub ? " · " + nomeSub : ""),
          "Escolha a sub-subcategoria"
        );
        return;
      }
      abrirProdutosFiltrados(
        (catNomeAtual || "") + (nomeSub ? " · " + nomeSub : "")
      );
    }

    function abrirCategoria(slug, nome) {
      modoBusca = false;
      catAtual = slug || "";
      catNomeAtual = nome || "";
      subAtual = "";
      subNomeAtual = "";
      sub2Atual = "";
      if (busca) busca.value = "";
      var subs = opcoesSubDaCategoria(catAtual);
      if (subs.length > 0) {
        nivelPasso = 1;
        renderCardsNivel(subs, abrirSubcategoria);
        mostrarGradeNivel(catNomeAtual || "Subcategoria", "Escolha a subcategoria");
        return;
      }
      abrirProdutosFiltrados(catNomeAtual || "Produtos");
    }

    function voltarDoProdutos() {
      if (modoBusca) {
        mostrarHome();
        return;
      }
      if (sub2Atual && subAtual && catAtual) {
        sub2Atual = "";
        var netos = opcoesSub2(catAtual, subAtual);
        if (netos.length > 0) {
          nivelPasso = 2;
          renderCardsNivel(netos, abrirSub2);
          mostrarGradeNivel(
            (catNomeAtual || "") + (subNomeAtual ? " · " + subNomeAtual : ""),
            "Escolha a sub-subcategoria"
          );
          return;
        }
      }
      if (subAtual && catAtual) {
        subAtual = "";
        subNomeAtual = "";
        sub2Atual = "";
        var subs = opcoesSubDaCategoria(catAtual);
        if (subs.length > 0) {
          nivelPasso = 1;
          renderCardsNivel(subs, abrirSubcategoria);
          mostrarGradeNivel(catNomeAtual || "Subcategoria", "Escolha a subcategoria");
          return;
        }
      }
      mostrarHome();
    }

    function voltarGrade() {
      if (nivelPasso === 2 && catAtual) {
        subAtual = "";
        subNomeAtual = "";
        sub2Atual = "";
        nivelPasso = 1;
        renderCardsNivel(opcoesSubDaCategoria(catAtual), abrirSubcategoria);
        mostrarGradeNivel(catNomeAtual || "Subcategoria", "Escolha a subcategoria");
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
    if (btnVoltarHome) btnVoltarHome.addEventListener("click", voltarGrade);

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
          subNomeAtual = "";
          sub2Atual = "";
          nivelPasso = 0;
          mostrarProdutos("Busca");
          aplicarFiltros();
        } else if (modoBusca) {
          mostrarHome();
        } else if (nivelPasso === 3) {
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
      var vis = document.getElementById("checkout-plus-visivel");
      if (vis) {
        vis.textContent = "";
        vis.classList.add("hidden");
      }
      setGeoStatus("", true);
    }

    function preencherLocalizacao(data) {
      setVal("checkout-plus-code", data.plus_code);
      setVal("checkout-lat", data.lat);
      setVal("checkout-lng", data.lng);
      setVal("checkout-maps-url", data.maps_url);
      preencherCamposEndereco(data);
      var vis = document.getElementById("checkout-plus-visivel");
      if (vis && data.plus_code) {
        vis.textContent = "Plus Code: " + data.plus_code;
        vis.classList.remove("hidden");
      }
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
                setGeoStatus("Localização OK — Plus Code preenchido para a entrega.", true);
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
              if (c.nome) setVal("checkout-nome", c.nome);
              if (c.cidade) setVal("checkout-cidade", c.cidade);
              if (c.logradouro) setVal("checkout-logradouro", c.logradouro);
              if (c.numero) setVal("checkout-numero", c.numero);
              if (c.bairro) setVal("checkout-bairro", c.bairro);
              if (c.uf) setVal("checkout-uf", c.uf);
              if (c.cep) setVal("checkout-cep", c.cep);
              if (c.plus_code) {
                setVal("checkout-plus-code", c.plus_code);
                var vis = document.getElementById("checkout-plus-visivel");
                if (vis) {
                  vis.textContent = "Plus Code: " + c.plus_code;
                  vis.classList.remove("hidden");
                }
                setEnderecoModo("geo");
              }
              if (c.maps_url) setVal("checkout-maps-url", c.maps_url);
              syncEnderecoHidden();
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
