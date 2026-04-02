/**
 * Resumo gerencial — KPI builder e painel (sem alterar cálculos; só apresentação).
 * Trend: exibe somente se opts.trend for string (ex.: vinda da API no futuro).
 */
(function () {
  "use strict";

  function num(v) {
    if (v == null || v === "") return 0;
    var n = parseFloat(String(v).replace(",", "."));
    return isNaN(n) ? 0 : n;
  }

  function brl(s) {
    var n = parseFloat(String(s).replace(",", "."));
    if (isNaN(n)) return "—";
    return n.toLocaleString("pt-BR", { style: "currency", currency: "BRL" });
  }

  function pct(s) {
    var n = parseFloat(String(s).replace(",", "."));
    if (isNaN(n)) return "—";
    return (n * 100).toLocaleString("pt-BR", { maximumFractionDigits: 2 }) + "%";
  }

  function escapeHtml(t) {
    if (t == null) return "";
    var d = document.createElement("div");
    d.textContent = String(t);
    return d.innerHTML;
  }

  /**
   * @param {object} o
   * @param {string} o.title
   * @param {string} o.value - já formatado (ex. BRL)
   * @param {'success'|'warning'|'danger'|'neutral'} o.tone
   * @param {string|null} [o.trend] - só exibe se truthy
   * @param {string|null} [o.subtitle]
   * @param {boolean} [o.emphasis]
   * @param {'pos'|'neg'|'zero'|null} [o.sign] - afeta cor do valor
   */
  function buildKpiCard(o) {
    var tone = o.tone || "neutral";
    var trendHtml = "";
    if (o.trend) {
      var tc =
        String(o.trend).indexOf("↑") !== -1 || String(o.trend).indexOf("+") === 0
          ? "rg-kpi__trend--up"
          : String(o.trend).indexOf("↓") !== -1
            ? "rg-kpi__trend--down"
            : "";
      trendHtml =
        '<div class="rg-kpi__trend ' +
        tc +
        '">' +
        escapeHtml(o.trend) +
        "</div>";
    } else {
      trendHtml = '<div class="rg-kpi__trend" aria-hidden="true"></div>';
    }

    var valClass = "rg-kpi__value";
    if (o.sign === "pos") valClass += " rg-kpi__value--success";
    else if (o.sign === "neg") valClass += " rg-kpi__value--danger";
    else if (o.sign === "zero") valClass += " rg-kpi__value--neutral";
    if (o.valueClass) valClass += " " + o.valueClass;

    var sub =
      o.subtitle != null && o.subtitle !== ""
        ? '<div class="rg-kpi__subtitle">' + escapeHtml(o.subtitle) + "</div>"
        : "";

    var emph = o.emphasis ? " rg-kpi--emphasis" : "";
    return (
      '<article class="rg-kpi rg-kpi--' +
      tone +
      emph +
      '" role="group">' +
      '<div class="rg-kpi__label">' +
      escapeHtml(o.title) +
      "</div>" +
      '<div class="' +
      valClass +
      '">' +
      escapeHtml(o.value) +
      "</div>" +
      sub +
      trendHtml +
      "</article>"
    );
  }

  function signFromNumber(n) {
    if (n > 0) return "pos";
    if (n < 0) return "neg";
    return "zero";
  }

  function renderKpiGrid(c) {
    var aj = c.ajustes_eliminacao || {};
    var recInt = aj.receitas_internas_eliminadas;
    var transInt = aj.transferencias_internas;

    var nRec = num(c.receita_operacional);
    var nLucro = num(c.lucro_bruto);
    var nResOp = num(c.resultado_operacional);
    var nResLiq = num(c.resultado_liquido_gerencial);
    var nCaixa = num(c.geracao_caixa);
    var nRecNaoOp = num(c.receita_nao_operacional);
    var nAportes = num(c.aportes_socios);
    var nRet = num(c.retiradas_socios);

    var subEntradas = [];
    if (nAportes !== 0) subEntradas.push("Aportes: " + brl(c.aportes_socios));
    if (nRet !== 0) subEntradas.push("Retiradas: " + brl(c.retiradas_socios));

    var rows = [];

    rows.push([
      buildKpiCard({
        title: "Receita operacional",
        value: brl(c.receita_operacional),
        tone: "success",
        sign: nRec > 0 ? "pos" : nRec < 0 ? "neg" : "zero",
        trend: c._trend_receita_operacional || null,
      }),
      buildKpiCard({
        title: "Lucro bruto",
        value: brl(c.lucro_bruto),
        tone: "success",
        sign: signFromNumber(nLucro),
        trend: c._trend_lucro_bruto || null,
      }),
      buildKpiCard({
        title: "Resultado operacional",
        value: brl(c.resultado_operacional),
        tone: "neutral",
        sign: signFromNumber(nResOp),
        emphasis: true,
        trend: c._trend_resultado_operacional || null,
      }),
    ]);

    rows.push([
      buildKpiCard({
        title: "Despesas fixas",
        value: brl(c.despesas_fixas),
        tone: "warning",
        valueClass: "rg-kpi__value--expense",
        trend: c._trend_despesas_fixas || null,
      }),
      buildKpiCard({
        title: "Despesas variáveis",
        value: brl(c.despesas_variaveis),
        tone: "warning",
        valueClass: "rg-kpi__value--expense",
        trend: c._trend_despesas_variaveis || null,
      }),
      buildKpiCard({
        title: "Despesas financeiras",
        value: brl(c.despesas_financeiras),
        tone: "warning",
        valueClass: "rg-kpi__value--expense",
        trend: c._trend_despesas_financeiras || null,
      }),
    ]);

    rows.push([
      buildKpiCard({
        title: "Geração de caixa",
        value: brl(c.geracao_caixa),
        tone: "success",
        sign: signFromNumber(nCaixa),
        trend: c._trend_geracao_caixa || null,
      }),
      buildKpiCard({
        title: "Resultado líquido gerencial",
        value: brl(c.resultado_liquido_gerencial),
        tone: "neutral",
        sign: signFromNumber(nResLiq),
        emphasis: true,
        trend: c._trend_resultado_liquido_gerencial || null,
      }),
      buildKpiCard({
        title: "Entradas / ajustes relevantes",
        value: brl(c.receita_nao_operacional),
        tone: "neutral",
        subtitle: subEntradas.length ? subEntradas.join(" · ") : null,
        trend: c._trend_receita_nao_operacional || null,
      }),
    ]);

    rows.push([
      buildKpiCard({
        title: "Entrada empréstimos",
        value: brl(c.emprestimos_entrada),
        tone: "warning",
        trend: c._trend_emprestimos_entrada || null,
      }),
      buildKpiCard({
        title: "Amortização empréstimos",
        value: brl(c.amortizacao_emprestimos),
        tone: "warning",
        valueClass: "rg-kpi__value--expense",
        trend: c._trend_amortizacao_emprestimos || null,
      }),
      buildKpiCard({
        title: "Elimin. receitas internas",
        value: brl(recInt),
        tone: "neutral",
        trend: c._trend_elim_rec_int || null,
      }),
      buildKpiCard({
        title: "Transferências internas (ajuste)",
        value: brl(transInt),
        tone: "neutral",
        trend: c._trend_transf_int || null,
      }),
    ]);

    var html = '<div class="rg-kpi-section">';
    html += '<div class="rg-kpi-row rg-kpi-row--3">' + rows[0].join("") + "</div>";
    html += '<div class="rg-kpi-row rg-kpi-row--3">' + rows[1].join("") + "</div>";
    html += '<div class="rg-kpi-row rg-kpi-row--3">' + rows[2].join("") + "</div>";
    html += '<div class="rg-kpi-row rg-kpi-row--4">' + rows[3].join("") + "</div>";
    html += "</div>";
    return html;
  }

  function mainZeros(c) {
    var keys = [
      "receita_operacional",
      "lucro_bruto",
      "resultado_operacional",
      "despesas_fixas",
      "despesas_variaveis",
      "despesas_financeiras",
    ];
    for (var i = 0; i < keys.length; i++) {
      if (num(c[keys[i]]) !== 0) return false;
    }
    return true;
  }

  function diasNoPeriodo(ini, fim) {
    if (!ini || !fim) return 30;
    var a = new Date(ini + "T12:00:00");
    var b = new Date(fim + "T12:00:00");
    var d = Math.round((b - a) / 86400000) + 1;
    return d > 0 ? d : 30;
  }

  function formatDateBR(iso) {
    if (!iso) return "—";
    var p = String(iso).split("-");
    if (p.length !== 3) return iso;
    return p[2] + "/" + p[1] + "/" + p[0];
  }

  /**
   * Inicialização do painel (DOM).
   */
  function initPainel(root) {
    if (!root) return;
    var CK = root.getAttribute("data-storage-key") || "agro_resumo_fin_sess_v1";

    var el = function (id) {
      return document.getElementById(id);
    };

    function salvarCtx() {
      try {
        sessionStorage.setItem(
          CK,
          JSON.stringify({
            modo: el("f-modo").value,
            empresa_id: el("f-empresa").value,
            grupo_id: el("f-grupo").value,
            data_inicio: el("f-ini").value,
            data_fim: el("f-fim").value,
            fonte: el("f-fonte").value,
            por: el("f-por").value,
            valor: el("f-valor").value,
            contas: el("f-contas").value,
          })
        );
      } catch (e) {}
    }

    function carregarCtx() {
      try {
        var raw = sessionStorage.getItem(CK);
        if (!raw) return;
        var o = JSON.parse(raw);
        if (o.modo) el("f-modo").value = o.modo;
        if (o.empresa_id) el("f-empresa").value = o.empresa_id;
        if (o.grupo_id) el("f-grupo").value = o.grupo_id;
        if (o.data_inicio) el("f-ini").value = o.data_inicio;
        if (o.data_fim) el("f-fim").value = o.data_fim;
        if (o.fonte) el("f-fonte").value = o.fonte;
        if (o.por) el("f-por").value = o.por;
        if (o.valor) el("f-valor").value = o.valor;
        if (o.contas !== undefined) el("f-contas").value = o.contas;
      } catch (e) {}
    }

    function toggleModo() {
      var m = el("f-modo").value;
      el("wrap-empresa").classList.toggle("hidden", m !== "empresa");
      el("wrap-grupo").classList.toggle("hidden", m !== "grupo");
      el("bloco-grupo").classList.toggle("hidden", m !== "grupo");
    }

    function toggleFonte() {
      var mongo = el("f-fonte").value === "mongo";
      ["wrap-mongo-por", "wrap-mongo-valor", "wrap-mongo-contas"].forEach(function (id) {
        el(id).classList.toggle("hidden", !mongo);
      });
    }

    function atualizarResumoFiltroVisivel() {
      var ini = el("f-ini").value;
      var fim = el("f-fim").value;
      var modo = el("f-modo").value;
      var periodo = "Período: " + formatDateBR(ini) + " a " + formatDateBR(fim);
      var entidade = "";
      if (modo === "empresa") {
        var sel = el("f-empresa");
        var oE = sel.options[sel.selectedIndex];
        entidade = "Empresa: " + (oE ? oE.text : "—");
      } else {
        var sg = el("f-grupo");
        var oG = sg.options[sg.selectedIndex];
        entidade = "Grupo: " + (oG ? oG.text : "—");
      }
      var fonte = el("f-fonte").value === "mongo" ? "Mongo (DtoLancamento)" : "Postgres (Agro)";
      el("rg-filtro-ativo").innerHTML =
        "<strong>" +
        periodo +
        "</strong> · <strong>" +
        entidade +
        "</strong> · Fonte: " +
        fonte;
    }

    function setLoading(on) {
      var sk = el("rg-kpi-skeleton");
      var grid = el("painel-cards");
      var btn = el("btn-atualizar");
      if (sk) sk.classList.toggle("is-visible", on);
      if (grid) grid.classList.toggle("hidden", on);
      if (btn) {
        btn.disabled = on;
        btn.setAttribute("aria-busy", on ? "true" : "false");
      }
    }

    function mostrarErro(msg) {
      var e = el("msg-erro");
      e.textContent = msg || "";
      e.classList.toggle("hidden", !msg);
    }

    function renderResumo(data, modo) {
      var c = modo === "grupo" ? data.consolidado : data;
      el("painel-cards").innerHTML = renderKpiGrid(c);
      el("painel-cards").classList.remove("hidden");

      var zero = el("rg-msg-zero");
      if (mainZeros(c)) {
        zero.classList.remove("hidden");
      } else {
        zero.classList.add("hidden");
      }

      var info = el("msg-mongo-info");
      var obs = c.ajustes_eliminacao && c.ajustes_eliminacao.observacao_mongo;
      if (obs) {
        info.textContent = obs;
        info.classList.remove("hidden");
      } else {
        info.textContent = "";
        info.classList.add("hidden");
      }

      if (modo === "grupo" && data.por_empresa && data.por_empresa.length) {
        var pe = el("por-empresa");
        pe.innerHTML = data.por_empresa
          .map(function (x) {
            if (x.erro) {
              return (
                '<div class="rounded-lg border border-red-100 bg-red-50 px-3 py-2 text-sm font-semibold text-red-900">Empresa #' +
                x.empresa_id +
                " — " +
                escapeHtml(x.erro) +
                "</div>"
              );
            }
            return (
              '<div class="rounded-lg border border-slate-100 bg-slate-50 px-3 py-2 text-sm font-semibold text-slate-800">Empresa #' +
              x.empresa_id +
              ' · Res. op. <span class="tabular-nums">' +
              brl(x.resultado_operacional) +
              "</span></div>"
            );
          })
          .join("");
      }
    }

    async function atualizar() {
      salvarCtx();
      atualizarResumoFiltroVisivel();
      mostrarErro("");
      el("sec-equilibrio").classList.remove("is-visible");
      setLoading(true);
      var modo = el("f-modo").value;
      var ini = el("f-ini").value;
      var fim = el("f-fim").value;
      if (!ini || !fim) {
        setLoading(false);
        mostrarErro("Informe início e fim.");
        return;
      }
      var fonte = el("f-fonte").value;
      var q =
        "modo=" +
        encodeURIComponent(modo) +
        "&data_inicio=" +
        encodeURIComponent(ini) +
        "&data_fim=" +
        encodeURIComponent(fim) +
        "&fonte=" +
        encodeURIComponent(fonte);
      if (fonte === "mongo") {
        q += "&por=" + encodeURIComponent(el("f-por").value);
        q += "&valor=" + encodeURIComponent(el("f-valor").value);
        var ct = el("f-contas").value;
        if (ct) q += "&contas=" + encodeURIComponent(ct);
      }
      if (modo === "empresa") {
        var eid = el("f-empresa").value;
        if (!eid) {
          setLoading(false);
          mostrarErro("Selecione a empresa.");
          return;
        }
        q += "&empresa_id=" + encodeURIComponent(eid);
      } else {
        var gid = el("f-grupo").value;
        if (!gid) {
          setLoading(false);
          mostrarErro("Selecione o grupo ou cadastre um no admin.");
          return;
        }
        q += "&grupo_id=" + encodeURIComponent(gid);
      }
      try {
        var r = await fetch("/api/financeiro/resumo-operacional?" + q, {
          credentials: "same-origin",
        });
        if (r.status === 401) {
          mostrarErro("Faça login (admin).");
          return;
        }
        if (!r.ok) {
          try {
            var ej = await r.json();
            var d = ej.detail != null ? ej.detail : ej.erro;
            var msg =
              typeof d === "string"
                ? d
                : Array.isArray(d)
                  ? d.join(" ")
                  : JSON.stringify(ej);
            mostrarErro(String(msg).slice(0, 450));
          } catch (e2) {
            var t = await r.text();
            mostrarErro("Erro " + r.status + (t ? ": " + t.slice(0, 200) : ""));
          }
          return;
        }
        var data = await r.json();
        renderResumo(data, modo);
        var dq = q + "&dias_periodo=" + diasNoPeriodo(ini, fim);
        var re = await fetch("/api/financeiro/gap-equilibrio?" + dq, {
          credentials: "same-origin",
        });
        if (re.ok) {
          var eq = await re.json();
          el("sec-equilibrio").classList.add("is-visible");
          el("eq-margem").textContent = pct(eq.margem_contribuicao_pct);
          el("eq-fat").textContent = brl(eq.faturamento_equilibrio);
          el("eq-dia").textContent = brl(eq.faturamento_diario_equilibrio);
        }
      } catch (e) {
        mostrarErro("Falha de rede.");
      } finally {
        setLoading(false);
      }
    }

    el("f-modo").addEventListener("change", function () {
      toggleModo();
      salvarCtx();
      atualizarResumoFiltroVisivel();
    });
    el("f-fonte").addEventListener("change", function () {
      toggleFonte();
      salvarCtx();
      atualizarResumoFiltroVisivel();
    });
    el("btn-atualizar").addEventListener("click", atualizar);
    ["f-empresa", "f-grupo", "f-ini", "f-fim", "f-por", "f-valor", "f-contas"].forEach(function (id) {
      el(id).addEventListener("change", function () {
        salvarCtx();
        atualizarResumoFiltroVisivel();
      });
    });

    el("btn-ajuda").addEventListener("click", function () {
      el("modal-ajuda").classList.remove("hidden");
    });
    el("btn-fechar-ajuda").addEventListener("click", function () {
      el("modal-ajuda").classList.add("hidden");
    });
    el("modal-ajuda").addEventListener("click", function (e) {
      if (e.target.id === "modal-ajuda") el("modal-ajuda").classList.add("hidden");
    });

    document.addEventListener("keydown", function (e) {
      if (e.key === "F5" && !e.defaultPrevented) {
        var t = e.target;
        if (t && (t.tagName === "INPUT" || t.tagName === "SELECT" || t.tagName === "TEXTAREA"))
          return;
        e.preventDefault();
        atualizar();
      }
    });

    carregarCtx();
    toggleModo();
    toggleFonte();
    var hoje = new Date();
    var iso = hoje.toISOString().slice(0, 10);
    if (!el("f-fim").value) el("f-fim").value = iso;
    if (!el("f-ini").value) {
      var u = new Date(hoje);
      u.setDate(u.getDate() - 29);
      el("f-ini").value = u.toISOString().slice(0, 10);
    }
    atualizarResumoFiltroVisivel();
    if (!window.AGRO_MANUAL_SYNC_ONLY) {
      atualizar();
    } else {
      setLoading(false);
      var info = el("msg-mongo-info");
      if (info) {
        info.textContent =
          "Modo só cache: use Atualizar ou F5 (fora de campos) para buscar indicadores na API.";
        info.classList.remove("hidden");
      }
    }

    if (typeof AgroEstoqueSync !== "undefined" && AgroEstoqueSync.mount) {
      AgroEstoqueSync.mount({
        onRefresh: async function () {
          await fetch("/api/pdv/saldos/", { cache: "no-store", credentials: "same-origin" });
        },
      });
    }
  }

  window.AgroResumoGerencial = {
    buildKpiCard: buildKpiCard,
    renderKpiGrid: renderKpiGrid,
    mainZeros: mainZeros,
    brl: brl,
    pct: pct,
    num: num,
    initPainel: initPainel,
  };

  document.addEventListener("DOMContentLoaded", function () {
    var root = document.getElementById("agro-resumo-gerencial-root");
    if (root) initPainel(root);
  });
})();

