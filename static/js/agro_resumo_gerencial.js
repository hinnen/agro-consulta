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

  function pctJa(s) {
    var n = parseFloat(String(s).replace(",", "."));
    if (isNaN(n)) return "—";
    return n.toLocaleString("pt-BR", { maximumFractionDigits: 1 }) + "%";
  }

  var CMV_KEY = "agro_dre_cmv_modo_v1";
  var CMV_HINT = {
    vendida: "Custo do cadastro × quantidade vendida (o que saiu da loja).",
    paga: "Compras lançadas no período (o que você pagou / deve de mercadoria).",
  };

  function cmvModoSalvo() {
    try {
      var m = localStorage.getItem(CMV_KEY);
      if (m === "paga" || m === "vendida") return m;
    } catch (e) {}
    return "vendida";
  }

  function salvarCmvModo(m) {
    try {
      localStorage.setItem(CMV_KEY, m);
    } catch (e) {}
  }

  function aplicarCmvNoCore(c) {
    if (!c || !c.cmv_modos) return c;
    var m = cmvModoSalvo();
    if (m === "vendida" && c.cmv_modos.ok_vendida === false) m = "paga";
    var snap = c.cmv_modos[m];
    if (!snap) return c;
    return Object.assign({}, c, snap, { cmv_modo: m });
  }

  function coreDoPayload(data, modo) {
    if (!data) return data;
    if (modo === "grupo") return data.consolidado || data;
    return data;
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
        subtitle: c.receita_fonte === "pdv" ? "Vendas do caixa (PDV)" : null,
        trend: c._trend_receita_operacional || null,
      }),
      buildKpiCard({
        title: "Lucro bruto",
        value: brl(c.lucro_bruto),
        tone: "success",
        sign: signFromNumber(nLucro),
        subtitle:
          c.markup_pct != null && c.margem_bruta_pct != null
            ? "Markup " + pctJa(c.markup_pct) + " · margem " + pctJa(c.margem_bruta_pct)
            : null,
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

  function sparkSvg(vals) {
    if (!vals || vals.length < 2) {
      return '<p class="rg-muted">Sem série diária neste período.</p>';
    }
    var max = Math.max.apply(null, vals.concat([0.01]));
    var w = 320;
    var h = 72;
    var p = 6;
    var pts = vals
      .map(function (v, i) {
        var x = p + (i / (vals.length - 1)) * (w - 2 * p);
        var y = h - p - (v / max) * (h - 2 * p);
        return x.toFixed(1) + "," + y.toFixed(1);
      })
      .join(" ");
    return (
      '<svg viewBox="0 0 ' +
      w +
      " " +
      h +
      '" class="rg-spark" preserveAspectRatio="none" aria-hidden="true"><polyline fill="none" stroke="#059669" stroke-width="2.4" stroke-linejoin="round" stroke-linecap="round" points="' +
      pts +
      '"/></svg>'
    );
  }

  function sparkVals(c) {
    var fat = (c && c.faturamento_pdv) || {};
    var por = fat.por_dia || {};
    var keys = Object.keys(por).sort();
    if (!keys.length) return [];
    return keys.slice(-7).map(function (k) {
      return num(por[k]);
    });
  }

  function peChartSvg(c) {
    var rec = num(c && c.receita_operacional);
    var cmv = num(c && c.cmv);
    var df = num(c && c.despesas_fixas);
    var dv = num(c && c.despesas_variaveis);
    var pe = num(c && c.faturamento_equilibrio);
    var varRatio = rec > 0 ? (cmv + dv) / rec : 1;
    if (!isFinite(varRatio) || varRatio < 0) varRatio = 1;
    var xmax = Math.max(pe * 1.7, rec * 1.3, df > 0 ? df * 2.2 : 1, 1);
    var ymax = Math.max(xmax, df + varRatio * xmax) * 1.08;
    if (ymax <= 0) ymax = 1;
    var W = 440;
    var H = 220;
    var padL = 44;
    var padR = 12;
    var padT = 16;
    var padB = 28;
    function xPx(x) {
      return padL + (x / xmax) * (W - padL - padR);
    }
    function yPx(y) {
      return H - padB - (y / ymax) * (H - padT - padB);
    }
    function pt(x, y) {
      return xPx(x).toFixed(1) + "," + yPx(y).toFixed(1);
    }
    var rec1 = xmax;
    var cost0 = df;
    var cost1 = df + varRatio * xmax;
    var hasPe = pe > 0 && pe < xmax * 0.98 && varRatio < 1;
    var prejuPoly = "";
    var lucroPoly = "";
    if (hasPe) {
      prejuPoly = [pt(0, 0), pt(pe, pe), pt(pe, df + varRatio * pe), pt(0, cost0)].join(" ");
      lucroPoly = [pt(pe, pe), pt(xmax, rec1), pt(xmax, cost1), pt(pe, df + varRatio * pe)].join(" ");
    } else if (varRatio < 1 && df <= 0) {
      lucroPoly = [pt(0, 0), pt(xmax, rec1), pt(xmax, cost1), pt(0, cost0)].join(" ");
    } else {
      prejuPoly = [pt(0, 0), pt(xmax, rec1), pt(xmax, cost1), pt(0, cost0)].join(" ");
    }
    var recLine = pt(0, 0) + " " + pt(xmax, rec1);
    var costLine = pt(0, cost0) + " " + pt(xmax, cost1);
    var svg =
      '<svg viewBox="0 0 ' +
      W +
      " " +
      H +
      '" class="rg-pe-chart" role="img" aria-label="Ponto de equilíbrio">';
    if (prejuPoly) {
      svg += '<polygon points="' + prejuPoly + '" fill="#ef4444"/>';
    }
    if (lucroPoly) {
      svg += '<polygon points="' + lucroPoly + '" fill="#059669"/>';
    }
    svg +=
      '<polyline fill="none" stroke="#b91c1c" stroke-width="2.2" points="' +
      costLine +
      '"/>';
    svg +=
      '<polyline fill="none" stroke="#047857" stroke-width="2.4" points="' +
      recLine +
      '"/>';
    svg +=
      '<line x1="' +
      padL +
      '" y1="' +
      yPx(0) +
      '" x2="' +
      (W - padR) +
      '" y2="' +
      yPx(0) +
      '" stroke="#94a3b8" stroke-width="1"/>';
    svg +=
      '<line x1="' +
      padL +
      '" y1="' +
      padT +
      '" x2="' +
      padL +
      '" y2="' +
      yPx(0) +
      '" stroke="#94a3b8" stroke-width="1"/>';
    svg +=
      '<text x="' +
      (padL - 6) +
      '" y="' +
      (padT + 8) +
      '" text-anchor="end" font-size="10" font-weight="800" fill="#64748b">R$</text>';
    svg +=
      '<text x="' +
      (W - padR) +
      '" y="' +
      (H - 8) +
      '" text-anchor="end" font-size="10" font-weight="800" fill="#64748b">Faturamento</text>';
    if (hasPe) {
      svg +=
        '<line x1="' +
        xPx(pe) +
        '" y1="' +
        yPx(pe) +
        '" x2="' +
        xPx(pe) +
        '" y2="' +
        yPx(0) +
        '" stroke="#475569" stroke-width="1.2" stroke-dasharray="4 3"/>';
      svg +=
        '<circle cx="' +
        xPx(pe) +
        '" cy="' +
        yPx(pe) +
        '" r="5" fill="#1e293b"/>';
      svg +=
        '<text x="' +
        xPx(pe) +
        '" y="' +
        (yPx(pe) - 10) +
        '" text-anchor="middle" font-size="10" font-weight="800" fill="#1e293b">Ponto de equilíbrio</text>';
      var midP = pe * 0.38;
      svg +=
        '<text x="' +
        xPx(midP) +
        '" y="' +
        yPx((df + varRatio * midP + midP) / 2) +
        '" text-anchor="middle" font-size="11" font-weight="900" fill="#fff">PREJUÍZO</text>';
      var midL = pe + (xmax - pe) * 0.55;
      svg +=
        '<text x="' +
        xPx(midL) +
        '" y="' +
        yPx((df + varRatio * midL + midL) / 2) +
        '" text-anchor="middle" font-size="11" font-weight="900" fill="#fff">LUCRO</text>';
    } else if (prejuPoly) {
      svg +=
        '<text x="' +
        xPx(xmax * 0.45) +
        '" y="' +
        yPx((df + varRatio * xmax * 0.45 + xmax * 0.45) / 2) +
        '" text-anchor="middle" font-size="11" font-weight="900" fill="#fff">PREJUÍZO</text>';
    }
    if (rec > 0 && rec <= xmax) {
      svg +=
        '<line x1="' +
        xPx(rec) +
        '" y1="' +
        yPx(rec) +
        '" x2="' +
        xPx(rec) +
        '" y2="' +
        yPx(0) +
        '" stroke="#047857" stroke-width="1.1" stroke-dasharray="2 3"/>';
    }
    svg +=
      '<text x="' +
      (xPx(xmax) - 4) +
      '" y="' +
      (yPx(rec1) - 6) +
      '" text-anchor="end" font-size="10" font-weight="900" fill="#047857">RECEITA</text>';
    svg +=
      '<text x="' +
      (xPx(0) + 8) +
      '" y="' +
      Math.max(yPx(cost0) - 6, padT + 12) +
      '" text-anchor="start" font-size="10" font-weight="900" fill="#b91c1c">CUSTO TOTAL</text>';
    svg += "</svg>";
    return svg;
  }

  function renderCatRows(top) {
    if (!top || !top.length) {
      return '<p class="rg-muted">Sem despesas por categoria neste recorte.</p>';
    }
    var max = Math.max.apply(
      null,
      top.map(function (r) {
        return num(r.ultimo);
      }).concat([0.01])
    );
    return top
      .slice(0, 8)
      .map(function (r) {
        var pctW = Math.min(100, (num(r.ultimo) / max) * 100);
        var tend = r.tendencia === "up" ? "↑" : r.tendencia === "down" ? "↓" : "→";
        var tendCls =
          r.tendencia === "up" ? "rg-tend--up" : r.tendencia === "down" ? "rg-tend--down" : "";
        return (
          '<div class="rg-cat">' +
          '<div class="rg-cat__meta"><span class="rg-cat__nome">' +
          escapeHtml(r.plano) +
          '</span><span class="rg-cat__val">' +
          brl(r.ultimo) +
          ' <i class="' +
          tendCls +
          '">' +
          tend +
          "</i></span></div>" +
          '<div class="rg-cat__track"><div class="rg-cat__bar" style="width:' +
          pctW.toFixed(1) +
          '%"></div></div></div>'
        );
      })
      .join("");
  }

  function renderVisualBoard(c, visual, modo) {
    var df = num(c.despesas_fixas);
    var dv = num(c.despesas_variaveis);
    var dfin = num(c.despesas_financeiras);
    var desp = df + dv + dfin;
    var rec = num(c.receita_operacional);
    var cmv = num(c.cmv);
    var lucro = num(c.lucro_bruto);
    var margem = c.margem_bruta_pct != null ? num(c.margem_bruta_pct) : rec > 0 ? (lucro / rec) * 100 : null;
    var markup = c.markup_pct != null ? num(c.markup_pct) : rec > 0 && cmv > 0 ? ((rec - cmv) / cmv) * 100 : null;
    var pe = num(c.faturamento_equilibrio);
    var pctPe = pe > 0 ? Math.min(100, (rec / pe) * 100) : 0;
    var peOk = pe > 0 && rec >= pe;
    var peHint =
      pe > 0
        ? "PE " +
          brl(pe) +
          (c.margem_contribuicao_pct != null ? " · MC " + pct(c.margem_contribuicao_pct) : "")
        : "Sem ponto de equilíbrio neste recorte";
    var varOk = visual && visual.ok && visual.variacao && visual.variacao.ok;
    var catHtml =
      modo === "grupo"
        ? '<p class="rg-muted">Abra uma empresa para ver despesas por categoria.</p>'
        : varOk
          ? renderCatRows(visual.variacao.top)
          : '<p class="rg-muted">Despesas por categoria indisponível neste recorte.</p>';
    var grupos = varOk ? visual.variacao.resumo_grupos || [] : [];
    var gruposHtml = grupos
      .map(function (g) {
        return (
          '<div class="rg-gsum rg-gsum--' +
          escapeHtml(g.key || "") +
          '"><span>' +
          escapeHtml(g.label || "") +
          "</span><strong>" +
          brl(g.ultimo) +
          "</strong></div>"
        );
      })
      .join("");
    var emp = (visual && visual.emprestimos) || {};
    var empOk = emp && emp.ok;
    var empHtml =
      '<article class="rg-card rg-card--emp"><h3>Empréstimos</h3><dl class="rg-mini">' +
      "<div><dt>Valor devido</dt><dd>" +
      (empOk ? brl(emp.valor_devido) : "—") +
      "</dd></div><div><dt>Valor pago</dt><dd>" +
      (empOk ? brl(emp.valor_pago) : "—") +
      "</dd></div><div><dt>Juros</dt><dd>" +
      (empOk ? brl(emp.juros) : "—") +
      "</dd></div><div><dt>Valor emprestado</dt><dd>" +
      (empOk ? brl(emp.valor_emprestado) : "—") +
      '</dd></div></dl><p class="rg-muted">' +
      (modo === "grupo"
        ? "Abra uma empresa para ver empréstimos."
        : "Devido = saldo em aberto · pago/juros = filtro da tela · emprestado = competência do período") +
      "</p></article>";
    return (
      '<div class="rg-board">' +
      '<div class="rg-flow">' +
      '<article class="rg-flow__kpi rg-flow__kpi--desp"><span>Despesas</span><strong>' +
      brl(desp) +
      "</strong><small>fixas " +
      brl(df) +
      " · var " +
      brl(dv) +
      " · fin " +
      brl(dfin) +
      "</small></article>" +
      '<span class="rg-flow__arrow" aria-hidden="true">→</span>' +
      '<article class="rg-flow__kpi rg-flow__kpi--rec"><span>Receita</span><strong>' +
      brl(rec) +
      "</strong><small>" +
      (c.receita_fonte === "pdv" ? "Vendas do caixa (PDV)" : "Lançamentos") +
      "</small></article>" +
      '<span class="rg-flow__arrow" aria-hidden="true">→</span>' +
      '<article class="rg-flow__kpi rg-flow__kpi--lucro"><span>% Lucro</span><strong>' +
      (margem != null ? pctJa(margem) : "—") +
      "</strong><small>margem bruta</small></article>" +
      '<article class="rg-flow__side"><div><span>CMV</span><strong>' +
      brl(cmv) +
      "</strong></div><div><span>Markup</span><strong>" +
      (markup != null ? pctJa(markup) : "—") +
      "</strong></div></article>" +
      '<article class="rg-gauge"><div class="rg-gauge__ring" style="--pct:' +
      pctPe.toFixed(1) +
      '"></div><div class="rg-gauge__center"><strong>' +
      (pe > 0 ? Math.round(pctPe) + "%" : "—") +
      "</strong><span>" +
      (peOk ? "acima do PE" : "do equilíbrio") +
      '</span></div><p class="rg-gauge__hint">' +
      peHint +
      "</p></article></div>" +
      '<div class="rg-col--charts"><article class="rg-card rg-card--pe"><h3>Ponto de equilíbrio</h3>' +
      peChartSvg(c) +
      '<p class="rg-muted">Custo = fixas + CMV + variáveis · eixo = faturamento R$ · Caixa: <b>' +
      brl(c.geracao_caixa) +
      "</b> <span>(não muda com CMV)</span></p></article></div>" +
      '<article class="rg-card rg-col--cat"><h3>Despesas por categoria</h3>' +
      (gruposHtml ? '<div class="rg-gsums">' + gruposHtml + "</div>" : "") +
      '<div class="rg-cat-list">' +
      catHtml +
      "</div></article>" +
      '<div class="rg-col--dre"><article class="rg-card"><h3>Mini DRE</h3><dl class="rg-mini"><div><dt>Receita</dt><dd>' +
      brl(rec) +
      "</dd></div><div><dt>CMV</dt><dd>" +
      brl(cmv) +
      "</dd></div><div><dt>Lucro bruto</dt><dd>" +
      brl(lucro) +
      "</dd></div><div><dt>Resultado op.</dt><dd>" +
      brl(c.resultado_operacional) +
      "</dd></div><div><dt>Líquido</dt><dd>" +
      brl(c.resultado_liquido_gerencial) +
      "</dd></div><div><dt>Caixa</dt><dd>" +
      brl(c.geracao_caixa) +
      "</dd></div></dl></article>" +
      empHtml +
      "</div></div>"
    );
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
            por: el("f-por").value,
            valor: el("f-valor").value,
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
        if (o.por) el("f-por").value = o.por;
        if (o.valor) el("f-valor").value = o.valor;
      } catch (e) {}
    }

    function toggleModo() {
      var m = el("f-modo").value;
      el("wrap-empresa").classList.toggle("hidden", m !== "empresa");
      el("wrap-grupo").classList.toggle("hidden", m !== "grupo");
      el("bloco-grupo").classList.toggle("hidden", m !== "grupo");
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
      el("rg-filtro-ativo").innerHTML =
        "<strong>" + periodo + "</strong> · <strong>" + entidade + "</strong>";
    }

    function setLoading(on) {
      var sk = el("rg-kpi-skeleton");
      var vis = el("painel-visual");
      var mais = el("rg-mais-numeros");
      var btn = el("btn-atualizar");
      if (sk) sk.classList.toggle("is-visible", on);
      if (vis) vis.classList.toggle("hidden", on);
      if (mais) mais.classList.toggle("hidden", on);
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

    function syncCmvChips(c) {
      var m = (c && c.cmv_modo) || cmvModoSalvo();
      document.querySelectorAll("[data-dre-cmv]").forEach(function (btn) {
        btn.classList.toggle("is-active", btn.getAttribute("data-dre-cmv") === m);
      });
      var hint = el("rg-cmv-hint");
      if (hint) hint.textContent = CMV_HINT[m] || CMV_HINT.vendida;
    }

    function pintarEquilibrio(c) {
      if (!c || c.faturamento_equilibrio == null) return false;
      var sec = el("sec-equilibrio");
      if (sec) {
        sec.classList.remove("is-visible");
        el("eq-margem").textContent = pct(c.margem_contribuicao_pct);
        el("eq-fat").textContent = brl(c.faturamento_equilibrio);
        el("eq-dia").textContent = brl(c.faturamento_diario_equilibrio);
      }
      return true;
    }

    var lastPayload = null;
    var lastModoTela = "empresa";

    function renderResumo(data, modo) {
      lastPayload = data;
      lastModoTela = modo;
      var bruto = coreDoPayload(data, modo);
      var c = aplicarCmvNoCore(bruto);
      syncCmvChips(c);
      var vis = el("painel-visual");
      var mais = el("rg-mais-numeros");
      if (vis) {
        vis.innerHTML = renderVisualBoard(c, data.visual, modo);
        vis.classList.remove("hidden");
      }
      el("painel-cards").innerHTML = renderKpiGrid(c);
      el("painel-cards").classList.remove("hidden");
      if (mais) mais.classList.remove("hidden");

      var zero = el("rg-msg-zero");
      if (mainZeros(c)) {
        zero.classList.remove("hidden");
      } else {
        zero.classList.add("hidden");
      }

      var info = el("msg-info");
      var aj = c.ajustes_eliminacao || {};
      var obs = aj.observacao || aj.observacao_mongo;
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
      var q =
        "modo=" +
        encodeURIComponent(modo) +
        "&data_inicio=" +
        encodeURIComponent(ini) +
        "&data_fim=" +
        encodeURIComponent(fim) +
        "&fonte=postgres" +
        "&por=" +
        encodeURIComponent(el("f-por").value) +
        "&valor=" +
        encodeURIComponent(el("f-valor").value) +
        "&contas=resultado" +
        "&incluir_visual=1";
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
        var cEq = aplicarCmvNoCore(coreDoPayload(data, modo));
        if (!pintarEquilibrio(cEq)) {
          var dq = q + "&dias_periodo=" + diasNoPeriodo(ini, fim);
          var re = await fetch("/api/financeiro/gap-equilibrio?" + dq, {
            credentials: "same-origin",
          });
          if (re.ok) {
            var eq = await re.json();
            var brutoEq = coreDoPayload(lastPayload, modo);
            if (brutoEq) {
              brutoEq.faturamento_equilibrio = eq.faturamento_equilibrio;
              brutoEq.margem_contribuicao_pct = eq.margem_contribuicao_pct;
              brutoEq.faturamento_diario_equilibrio = eq.faturamento_diario_equilibrio;
            }
            renderResumo(lastPayload, modo);
            pintarEquilibrio(aplicarCmvNoCore(coreDoPayload(lastPayload, modo)));
          }
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
    el("btn-atualizar").addEventListener("click", atualizar);
    document.querySelectorAll("[data-dre-cmv]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        salvarCmvModo(btn.getAttribute("data-dre-cmv"));
        if (!lastPayload) return;
        renderResumo(lastPayload, lastModoTela);
        pintarEquilibrio(aplicarCmvNoCore(coreDoPayload(lastPayload, lastModoTela)));
      });
    });
    ["f-empresa", "f-grupo", "f-ini", "f-fim", "f-por", "f-valor"].forEach(function (id) {
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
      var info = el("msg-info");
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
    renderVisualBoard: renderVisualBoard,
    peChartSvg: peChartSvg,
    mainZeros: mainZeros,
    brl: brl,
    pct: pct,
    pctJa: pctJa,
    num: num,
    aplicarCmvNoCore: aplicarCmvNoCore,
    initPainel: initPainel,
  };

  document.addEventListener("DOMContentLoaded", function () {
    var root = document.getElementById("agro-resumo-gerencial-root");
    if (root) initPainel(root);
  });
})();
