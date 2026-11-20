(function () {
  "use strict";

  var root = document.getElementById("zp-root");
  if (!root) return;

  var board = document.getElementById("zp-board");
  var drawer = document.getElementById("zp-drawer");
  var drawerBody = document.getElementById("zp-drawer-body");
  var drawerTitle = document.getElementById("zp-drawer-title");
  var drawerPhone = document.getElementById("zp-drawer-phone");
  var drawerOpenChat = document.getElementById("zp-open-chat");
  var chipPonte = document.getElementById("zp-chip-ponte");
  var chipAtualizado = document.getElementById("zp-chip-atualizado");
  var chipDemo = document.getElementById("zp-chip-demo");
  var selectedId = 0;
  var dados = null;

  function csrf() {
    var el = document.querySelector('input[name="csrfmiddlewaretoken"]');
    return el ? el.value : "";
  }

  function esc(s) {
    return String(s || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function fmtTel(t) {
    var d = String(t || "").replace(/\D/g, "");
    if (d.length === 13 && d.slice(0, 2) === "55") {
      return "(" + d.slice(2, 4) + ") " + d.slice(4, 9) + "-" + d.slice(9);
    }
    if (d.length === 11) {
      return "(" + d.slice(0, 2) + ") " + d.slice(2, 7) + "-" + d.slice(7);
    }
    return t || "—";
  }

  function setKpi(id, val, hint) {
    var el = document.getElementById(id);
    if (!el) return;
    var v = el.querySelector(".val");
    var h = el.querySelector(".hint");
    if (v) v.textContent = String(val);
    if (h && hint != null) h.textContent = hint;
  }

  function badgeHtml(item) {
    var bits = [];
    if (item.nao_lidas > 0) {
      bits.push('<span class="zp-badge is-unread">' + esc(item.nao_lidas) + " nova" + (item.nao_lidas > 1 ? "s" : "") + "</span>");
    }
    if (item.aguardando_loja) {
      bits.push('<span class="zp-badge is-wait">espera</span>');
    }
    if (item.vip) {
      bits.push('<span class="zp-badge is-vip">vip</span>');
    }
    return bits.join("");
  }

  function cardHtml(item) {
    var espera = item.espera_min > 0 ? item.espera_min + " min" : item.hora || "";
    return (
      '<button type="button" class="zp-card' + (selectedId === item.id ? " is-on" : "") + '" data-id="' + item.id + '">' +
        '<div class="zp-card-top">' +
          "<div>" +
            '<p class="zp-name">' + esc(item.nome || "Sem nome") + "</p>" +
            '<p class="zp-phone">' + esc(fmtTel(item.telefone)) + "</p>" +
          "</div>" +
          '<div class="zp-badges">' + badgeHtml(item) + "</div>" +
        "</div>" +
        '<p class="zp-preview">' + esc(item.ultima_preview || "Sem mensagem ainda") + "</p>" +
        '<div class="zp-foot">' +
          "<span>" + esc(espera) + "</span>" +
          "<span>" + esc((item.msgs_total || 0) + " msgs") + "</span>" +
        "</div>" +
      "</button>"
    );
  }

  function renderBoard(payload) {
    dados = payload;
    var resumo = payload.resumo || {};
    setKpi("zp-kpi-ativas", resumo.ativas || 0, (resumo.fila || 0) + " na fila");
    setKpi("zp-kpi-espera", resumo.aguardando || 0, (resumo.nao_lidas || 0) + " não lidas");
    setKpi("zp-kpi-hoje", resumo.resolvidas_hoje || 0, "arquivadas hoje");
    setKpi("zp-kpi-msgs", resumo.total_msgs || 0, "mensagens no banco");

    var ponte = payload.ponte || {};
    if (chipPonte) {
      chipPonte.className = "zp-chip";
      if (ponte.conectada) {
        chipPonte.classList.add("is-on");
        chipPonte.innerHTML = "Zap <strong>ligado</strong>";
      } else if (ponte.status === "qr") {
        chipPonte.classList.add("is-warn");
        chipPonte.innerHTML = "Zap <strong>aguardando QR</strong>";
      } else {
        chipPonte.classList.add("is-off");
        chipPonte.innerHTML = "Zap <strong>desligado</strong>";
      }
    }
    if (chipAtualizado) {
      var quando = payload.atualizado_em ? new Date(payload.atualizado_em) : new Date();
      chipAtualizado.innerHTML = "Atualizado <strong>" + quando.toLocaleTimeString("pt-BR", { hour: "2-digit", minute: "2-digit" }) + "</strong>";
    }
    if (chipDemo) {
      chipDemo.hidden = !payload.demo;
    }

    var html = "";
    (payload.colunas || []).forEach(function (col) {
      var itens = col.itens || [];
      html +=
        '<section class="zp-col" data-col="' + esc(col.id) + '">' +
          '<div class="zp-col-head">' +
            "<div><h3>" + esc(col.titulo) + "</h3><span>" + esc(col.sub) + "</span></div>" +
            '<span class="zp-col-count">' + itens.length + "</span>" +
          "</div>" +
          '<div class="zp-list">' +
            (itens.length
              ? itens.map(cardHtml).join("")
              : '<div class="zp-empty">Nenhum atendimento aqui</div>') +
          "</div>" +
        "</section>";
    });
    board.innerHTML = html;
  }

  function abrirDetalhe(item) {
    selectedId = item.id;
    drawerTitle.textContent = item.nome || "Sem nome";
    drawerPhone.textContent = fmtTel(item.telefone);
    drawerOpenChat.href = "/atendimento-whatsapp/?c=" + encodeURIComponent(item.id);
    var tags = (item.tags || []).map(function (t) {
      return '<span class="zp-badge">' + esc(t) + "</span>";
    }).join(" ");
    var timeline = (item.timeline || []).map(function (m) {
      var cls = m.direcao === "in" ? "is-in" : m.direcao === "bot" ? "is-bot" : "is-out";
      var who = m.direcao === "in" ? "Cliente" : m.direcao === "bot" ? "Bot" : (m.autor || "Loja");
      return (
        '<div class="zp-msg ' + cls + '">' +
          '<div class="who">' + esc(who) + " · " + esc(m.quando) + "</div>" +
          '<div class="txt">' + esc(m.texto) + "</div>" +
        "</div>"
      );
    }).join("");
    drawerBody.innerHTML =
      '<div class="zp-field"><div class="k">Loja</div><div class="v">' + esc(item.loja || "—") + "</div></div>" +
      '<div class="zp-field"><div class="k">Situação</div><div class="v">' +
        esc(item.arquivada ? "Resolvida" : item.aguardando_loja ? "Aguardando a loja" : item.nao_lidas > 0 ? "Com mensagem nova" : "Em andamento") +
      "</div></div>" +
      '<div class="zp-field"><div class="k">Última mensagem</div><div class="v">' + esc(item.ultima_preview || "—") + "</div></div>" +
      (item.nota ? '<div class="zp-field"><div class="k">Nota</div><div class="v">' + esc(item.nota) + "</div></div>" : "") +
      (tags ? '<div class="zp-field"><div class="k">Marcas</div><div class="v" style="display:flex;gap:.3rem;flex-wrap:wrap">' + tags + "</div></div>" : "") +
      '<div class="zp-field"><div class="k">Últimas mensagens</div><div class="zp-timeline">' +
        (timeline || '<div class="zp-empty">Sem histórico ainda</div>') +
      "</div></div>";
    drawer.classList.add("is-open");
    drawer.setAttribute("aria-hidden", "false");
    renderBoard(dados);
  }

  function fecharDetalhe() {
    drawer.classList.remove("is-open");
    drawer.setAttribute("aria-hidden", "true");
  }

  function carregar() {
    return fetch("/api/atendimento-whatsapp/painel/", { credentials: "same-origin" })
      .then(function (r) { return r.json(); })
      .then(function (j) {
        if (!j || !j.ok) throw new Error((j && j.erro) || "Falha ao carregar");
        renderBoard(j);
      })
      .catch(function (e) {
        board.innerHTML = '<div class="zp-empty">Não deu para carregar o painel. ' + esc(e.message || e) + "</div>";
      });
  }

  board.addEventListener("click", function (ev) {
    var btn = ev.target.closest(".zp-card");
    if (!btn) return;
    var id = parseInt(btn.getAttribute("data-id") || "0", 10);
    if (!id) return;
    fetch("/api/atendimento-whatsapp/painel/" + id + "/", { credentials: "same-origin" })
      .then(function (r) { return r.json(); })
      .then(function (j) {
        if (!j || !j.ok) throw new Error((j && j.erro) || "Falha");
        abrirDetalhe(j.item);
      })
      .catch(function (e) {
        alert(e.message || "Não abriu o detalhe");
      });
  });

  document.getElementById("zp-close").addEventListener("click", fecharDetalhe);
  document.getElementById("zp-drawer-mask").addEventListener("click", fecharDetalhe);
  document.getElementById("zp-refresh").addEventListener("click", function () {
    carregar();
  });
  document.addEventListener("keydown", function (ev) {
    if (ev.key === "Escape") fecharDetalhe();
  });

  carregar();
  setInterval(carregar, 20000);
})();
