/**
 * Vendas por grupo: escolher colunas (localStorage) + ordenar pelo cabeçalho.
 */
(function () {
  var root = document.getElementById("vg-table-root");
  if (!root) return;

  var table = root.querySelector("table");
  var tbody = table && table.querySelector("tbody");
  var form = document.getElementById("form-relatorio-filtros");
  var comparativo = root.getAttribute("data-vg-comparativo") === "1";
  var storageKey = comparativo
    ? "agro_vg_cols_comp_v1"
    : "agro_vg_cols_periodo_v1";
  var sortKey = comparativo
    ? "agro_vg_sort_comp_v1"
    : "agro_vg_sort_periodo_v1";

  var ordDirInput = form && form.querySelector('input[name="ord_dir"]');
  var ordenarSelect = form && form.querySelector('select[name="ordenar"]');

  function parseJson(s, fallback) {
    try {
      return JSON.parse(s);
    } catch (e) {
      return fallback;
    }
  }

  /** Padrão: grupo + venda R$, margem R$ e lucro bruto % (o resto fica oculto). */
  function presetVendaMargem() {
    if (comparativo) {
      return {
        groups: {
          venda: true,
          margem_rs: true,
          margem_pct: true,
          var_venda: false,
          var_total: false,
        },
      };
    }
    return {
      ids: {
        pos: true,
        grupo: true,
        skus: false,
        qtd: false,
        ticket: false,
        valor: true,
        pct: false,
        custo: false,
        margem_rs: true,
        margem_pct: true,
        pct_margem: false,
      },
    };
  }

  function defaultVisibleCols() {
    return presetVendaMargem();
  }

  function loadPrefs() {
    var raw = localStorage.getItem(storageKey);
    if (!raw) return defaultVisibleCols();
    var p = parseJson(raw, null);
    if (!p) return defaultVisibleCols();
    return p;
  }

  function savePrefs(p) {
    try {
      localStorage.setItem(storageKey, JSON.stringify(p));
    } catch (e) {}
  }

  function colHidden(th) {
    var id = th.getAttribute("data-vg-col");
    var group = th.getAttribute("data-vg-group");
    var locked = th.getAttribute("data-vg-locked") === "1";
    if (locked) return false;
    var prefs = loadPrefs();
    if (comparativo && group) {
      var g = (prefs.groups || {})[group];
      return g === false;
    }
    if (!comparativo && id) {
      var ids = prefs.ids || {};
      if (id in ids) return ids[id] === false;
    }
    return false;
  }

  function applyVisibility() {
    if (!table) return;
    var ths = table.querySelectorAll("thead th[data-vg-col]");
    ths.forEach(function (th) {
      var hide = colHidden(th);
      var id = th.getAttribute("data-vg-col");
      th.classList.toggle("vg-col-hidden", hide);
      table.querySelectorAll('td[data-vg-col="' + id + '"]').forEach(function (td) {
        td.classList.toggle("vg-col-hidden", hide);
      });
    });
  }

  function sortIndicator(th, dir) {
    var base = th.getAttribute("data-vg-label") || th.textContent.trim();
    if (!dir) {
      th.textContent = base;
      th.removeAttribute("data-vg-sort-active");
      return;
    }
    th.setAttribute("data-vg-sort-active", dir);
    th.textContent = base + (dir === "asc" ? " ▲" : " ▼");
  }

  function clearSortIndicators() {
    table.querySelectorAll("thead th[data-vg-sort-key]").forEach(function (th) {
      sortIndicator(th, null);
    });
  }

  function compareVals(a, b, dir) {
    var na = a === null || a === undefined || a === "";
    var nb = b === null || b === undefined || b === "";
    if (na && nb) return 0;
    if (na) return 1;
    if (nb) return -1;
    var fa = parseFloat(a);
    var fb = parseFloat(b);
    var num = !isNaN(fa) && !isNaN(fb);
    var cmp;
    if (num) cmp = fa < fb ? -1 : fa > fb ? 1 : 0;
    else cmp = String(a).localeCompare(String(b), "pt-BR", { sensitivity: "base" });
    return dir === "asc" ? cmp : -cmp;
  }

  function renumberPos() {
    var n = 0;
    tbody.querySelectorAll("tr").forEach(function (tr) {
      if (tr.classList.contains("vg-empty-row")) return;
      n += 1;
      var pos = tr.querySelector('td[data-vg-col="pos"]');
      if (pos) pos.textContent = String(n);
    });
  }

  function sortByColumn(colId, sortKey) {
    if (!tbody || !sortKey) return;
    var saved = parseJson(localStorage.getItem(sortKey), {});
    var dir = "desc";
    if (saved.col === colId) dir = saved.dir === "desc" ? "asc" : "desc";
    localStorage.setItem(sortKey, JSON.stringify({ col: colId, dir: dir }));

    var rows = Array.prototype.slice.call(tbody.querySelectorAll("tr")).filter(function (tr) {
      return !tr.classList.contains("vg-empty-row");
    });
    rows.sort(function (ra, rb) {
      var ca = ra.querySelector('td[data-vg-col="' + colId + '"]');
      var cb = rb.querySelector('td[data-vg-col="' + colId + '"]');
      return compareVals(
        ca && ca.getAttribute("data-sort-val"),
        cb && cb.getAttribute("data-sort-val"),
        dir
      );
    });
    rows.forEach(function (tr) {
      tbody.appendChild(tr);
    });
    renumberPos();

    clearSortIndicators();
    var th = table.querySelector('thead th[data-vg-col="' + colId + '"]');
    if (th) sortIndicator(th, dir);

    if (ordenarSelect) ordenarSelect.value = sortKey;
    if (ordDirInput) ordDirInput.value = dir;
  }

  function bindHeaders() {
    table.querySelectorAll("thead th[data-vg-sort-key]").forEach(function (th) {
      var sortKeyAttr = th.getAttribute("data-vg-sort-key");
      var colId = th.getAttribute("data-vg-col");
      if (!th.getAttribute("data-vg-label")) {
        th.setAttribute("data-vg-label", th.textContent.trim());
      }
      th.classList.add("cursor-pointer", "select-none", "hover:text-sky-300");
      th.setAttribute("title", "Clique para ordenar (maior ↔ menor)");
      th.addEventListener("click", function () {
        sortByColumn(colId, sortKeyAttr);
      });
    });
    var saved = parseJson(localStorage.getItem(sortKey), {});
    if (saved.col) {
      var th0 = table.querySelector('thead th[data-vg-col="' + saved.col + '"]');
      if (th0) sortIndicator(th0, saved.dir || "desc");
    }
  }

  function buildPicker(panel) {
    var prefs = loadPrefs();
    if (comparativo) {
      panel.querySelectorAll("[data-vg-group-toggle]").forEach(function (cb) {
        var g = cb.getAttribute("data-vg-group-toggle");
        cb.checked = (prefs.groups || {})[g] !== false;
        cb.addEventListener("change", function () {
          var p = loadPrefs();
          p.groups = p.groups || {};
          p.groups[g] = cb.checked;
          savePrefs(p);
          applyVisibility();
        });
      });
    } else {
      panel.querySelectorAll("[data-vg-col-toggle]").forEach(function (cb) {
        var id = cb.getAttribute("data-vg-col-toggle");
        if (cb.disabled) return;
        cb.checked = (prefs.ids || {})[id] !== false;
        cb.addEventListener("change", function () {
          var p = loadPrefs();
          p.ids = p.ids || {};
          p.ids[id] = cb.checked;
          savePrefs(p);
          applyVisibility();
        });
      });
    }
    var reset = panel.querySelector("[data-vg-reset-cols]");
    if (reset) {
      reset.addEventListener("click", function () {
        try {
          localStorage.removeItem(storageKey);
          localStorage.removeItem(sortKey);
        } catch (e) {}
        window.location.reload();
      });
    }
    var preset = panel.querySelector("[data-vg-preset-venda-margem]");
    if (preset) {
      preset.addEventListener("click", function () {
        savePrefs(presetVendaMargem());
        panel.querySelectorAll("[data-vg-col-toggle]").forEach(function (cb) {
          var id = cb.getAttribute("data-vg-col-toggle");
          if (cb.disabled) return;
          var p = loadPrefs();
          cb.checked = (p.ids || {})[id] !== false;
        });
        panel.querySelectorAll("[data-vg-group-toggle]").forEach(function (cb) {
          var g = cb.getAttribute("data-vg-group-toggle");
          var p = loadPrefs();
          cb.checked = (p.groups || {})[g] !== false;
        });
        applyVisibility();
      });
    }
  }

  var picker = document.getElementById("vg-col-picker");
  if (picker) buildPicker(picker);

  applyVisibility();
  bindHeaders();

  var visaoSel = form && form.querySelector('select[name="visao"]');
  if (visaoSel) {
    document.querySelectorAll("[data-vg-visao-pill]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var v = btn.getAttribute("data-vg-visao-pill");
        if (!v || visaoSel.value === v) return;
        visaoSel.value = v;
        form.submit();
      });
    });
  }
})();
