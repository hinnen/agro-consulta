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

    function catChipAtiva() {
      var on = document.querySelector(".cat-chip.is-on");
      return on ? String(on.getAttribute("data-cat") || "") : "";
    }

    function aplicarFiltros() {
      var q = String((busca && busca.value) || "")
        .toLowerCase()
        .trim();
      var cat = catChipAtiva();
      document.querySelectorAll(".produto-linha").forEach(function (el) {
        var nome = el.getAttribute("data-nome") || "";
        var matchQ = !q || nome.indexOf(q) >= 0;
        el.classList.toggle("hidden", !matchQ);
      });
      document.querySelectorAll(".secao-cat").forEach(function (sec) {
        var sc = String(sec.getAttribute("data-cat") || "");
        var matchCat = !cat || sc === cat;
        var visible = sec.querySelectorAll(".produto-linha:not(.hidden)").length > 0;
        sec.classList.toggle("hidden", !(matchCat && visible));
      });
    }

    document.querySelectorAll(".cat-chip").forEach(function (chip) {
      chip.addEventListener("click", function () {
        document.querySelectorAll(".cat-chip").forEach(function (c) {
          c.classList.remove("is-on");
        });
        chip.classList.add("is-on");
        aplicarFiltros();
      });
    });

    if (busca) {
      busca.addEventListener("input", aplicarFiltros);
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
