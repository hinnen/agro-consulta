/* Dispenser A6 — ajuste de layout (posição/tamanho) + modelos salvos */
(function (global) {
  var KEY_MODELS = "dsp_layouts_v1";
  var KEY_WORKING = "dsp_layout_working_v1";
  var KEY_ACTIVE = "dsp_layout_active_name_v1";
  var MARGIN = { x: 1.6, y: 1.2 }; /* % da folha — não sai da margem */
  var MIN = {
    title: { w: 28, h: 6 },
    brand: { w: 18, h: 10 },
    pet: { w: 18, h: 16 },
    ings: { w: 16, h: 20 },
    flavorsTitle: { w: 18, h: 4 },
    flavors: { w: 20, h: 12 },
    protein: { w: 14, h: 12 }
  };
  var IDS = ["title", "brand", "pet", "ings", "flavorsTitle", "flavors", "protein"];

  var undoStack = [];
  var redoStack = [];
  var fitSnapshotFn = null;
  var fitRestoreFn = null;

  function $(id) {
    return document.getElementById(id);
  }

  function layoutEditOn() {
    var c = cardEl();
    return !!(c && c.classList.contains("is-layout-edit"));
  }

  function cloneLayout(layout) {
    if (!layout) return null;
    try {
      return JSON.parse(JSON.stringify(layout));
    } catch (e) {
      return null;
    }
  }

  function takeSnapshot() {
    return {
      layout: cloneLayout(loadWorking()),
      free: !!(cardEl() && cardEl().classList.contains("is-free-layout")),
      fit: fitSnapshotFn ? fitSnapshotFn() : null
    };
  }

  function restoreSnapshot(snap) {
    if (!snap) return;
    if (snap.fit && fitRestoreFn) fitRestoreFn(snap.fit);
    if (snap.layout && snap.layout.items) {
      saveWorking(snap.layout);
      applyLayout(snap.layout);
    } else {
      resetToFlow();
      var edit = $("dspLayoutEdit");
      if (edit) edit.checked = false;
    }
  }

  function pushHistory() {
    undoStack.push(takeSnapshot());
    if (undoStack.length > 50) undoStack.shift();
    redoStack = [];
  }

  function undo() {
    if (!undoStack.length) return false;
    redoStack.push(takeSnapshot());
    restoreSnapshot(undoStack.pop());
    return true;
  }

  function redo() {
    if (!redoStack.length) return false;
    undoStack.push(takeSnapshot());
    restoreSnapshot(redoStack.pop());
    return true;
  }

  function setFitHooks(snapFn, restoreFn) {
    fitSnapshotFn = snapFn;
    fitRestoreFn = restoreFn;
  }

  function cardEl() {
    return $("dspCard");
  }

  function itemEl(id) {
    return document.querySelector('.dsp-layout-item[data-layout="' + id + '"]');
  }

  function loadModels() {
    try {
      var raw = localStorage.getItem(KEY_MODELS);
      var obj = raw ? JSON.parse(raw) : {};
      return obj && typeof obj === "object" ? obj : {};
    } catch (e) {
      return {};
    }
  }

  function saveModels(obj) {
    try {
      localStorage.setItem(KEY_MODELS, JSON.stringify(obj));
      return true;
    } catch (e) {
      window.alert("Não deu para salvar o modelo (memória do navegador cheia).");
      return false;
    }
  }

  function loadWorking() {
    try {
      var raw = localStorage.getItem(KEY_WORKING);
      var obj = raw ? JSON.parse(raw) : null;
      return obj && obj.items ? obj : null;
    } catch (e) {
      return null;
    }
  }

  function saveWorking(layout) {
    try {
      localStorage.setItem(KEY_WORKING, JSON.stringify(layout));
    } catch (e) {}
  }

  function clearWorking() {
    try {
      localStorage.removeItem(KEY_WORKING);
      localStorage.removeItem(KEY_ACTIVE);
    } catch (e) {}
  }

  function clampBox(id, box) {
    var min = MIN[id] || { w: 10, h: 8 };
    var w = Math.max(min.w, Math.min(100 - MARGIN.x * 2, box.w));
    var h = Math.max(min.h, Math.min(100 - MARGIN.y * 2, box.h));
    var x = Math.max(MARGIN.x, Math.min(100 - MARGIN.x - w, box.x));
    var y = Math.max(MARGIN.y, Math.min(100 - MARGIN.y - h, box.y));
    return { x: x, y: y, w: w, h: h };
  }

  function snapshotFromDom() {
    var card = cardEl();
    if (!card) return null;
    var cr = card.getBoundingClientRect();
    if (!cr.width || !cr.height) return null;
    var items = {};
    IDS.forEach(function (id) {
      var el = itemEl(id);
      if (!el) return;
      var r = el.getBoundingClientRect();
      items[id] = clampBox(id, {
        x: ((r.left - cr.left) / cr.width) * 100,
        y: ((r.top - cr.top) / cr.height) * 100,
        w: (r.width / cr.width) * 100,
        h: (r.height / cr.height) * 100
      });
    });
    return { v: 1, items: items };
  }

  function clearInline(el) {
    if (!el) return;
    el.style.position = "";
    el.style.left = "";
    el.style.top = "";
    el.style.width = "";
    el.style.height = "";
    el.style.right = "";
    el.style.bottom = "";
    el.style.margin = "";
    el.style.maxWidth = "";
    el.style.maxHeight = "";
    el.style.minWidth = "";
    el.style.minHeight = "";
    el.style.transform = "";
    el.style.zIndex = "";
  }

  function applyBox(el, box) {
    if (!el || !box) return;
    el.style.position = "absolute";
    el.style.left = box.x + "%";
    el.style.top = box.y + "%";
    el.style.width = box.w + "%";
    el.style.height = box.h + "%";
    el.style.right = "auto";
    el.style.bottom = "auto";
    el.style.margin = "0";
    el.style.maxWidth = "none";
    el.style.maxHeight = "none";
    el.style.minWidth = "0";
    el.style.minHeight = "0";
    el.style.zIndex = "6";
  }

  var homes = {};

  function rememberHomes() {
    IDS.forEach(function (id) {
      var el = itemEl(id);
      if (!el || homes[id]) return;
      homes[id] = { parent: el.parentElement, next: el.nextSibling };
    });
  }

  function ensureLayer() {
    var card = cardEl();
    if (!card) return null;
    var layer = card.querySelector(".dsp-layout-layer");
    if (!layer) {
      layer = document.createElement("div");
      layer.className = "dsp-layout-layer";
      card.appendChild(layer);
    }
    return layer;
  }

  function moveToLayer() {
    rememberHomes();
    var layer = ensureLayer();
    if (!layer) return;
    IDS.forEach(function (id) {
      var el = itemEl(id);
      if (el && el.parentElement !== layer) layer.appendChild(el);
    });
  }

  function restoreHomes() {
    IDS.forEach(function (id) {
      var el = itemEl(id);
      var home = homes[id];
      if (!el || !home || !home.parent) return;
      if (home.next && home.next.parentElement === home.parent) {
        home.parent.insertBefore(el, home.next);
      } else {
        home.parent.appendChild(el);
      }
    });
    var layer = cardEl() && cardEl().querySelector(".dsp-layout-layer");
    if (layer && !layer.children.length) layer.remove();
  }

  function applyLayout(layout) {
    var card = cardEl();
    if (!card || !layout || !layout.items) return;
    moveToLayer();
    card.classList.add("is-free-layout");
    IDS.forEach(function (id) {
      var el = itemEl(id);
      var box = layout.items[id];
      if (!el || !box) return;
      applyBox(el, clampBox(id, box));
    });
    var protein = itemEl("protein");
    if (protein) protein.removeAttribute("data-pos");
  }

  function resetToFlow() {
    var card = cardEl();
    if (!card) return;
    card.classList.remove("is-free-layout", "is-layout-edit");
    IDS.forEach(function (id) {
      clearInline(itemEl(id));
    });
    restoreHomes();
    var protein = itemEl("protein");
    if (protein && !protein.getAttribute("data-pos")) {
      protein.setAttribute("data-pos", "foto");
    }
    clearWorking();
    removeHandles();
  }

  function ensureHandles() {
    IDS.forEach(function (id) {
      var el = itemEl(id);
      if (!el) return;
      if (el.querySelector(".dsp-layout-handle")) return;
      var h = document.createElement("span");
      h.className = "dsp-layout-handle no-print";
      h.setAttribute("data-resize", "1");
      h.title = "Redimensionar";
      el.appendChild(h);
    });
  }

  function removeHandles() {
    document.querySelectorAll(".dsp-layout-handle").forEach(function (h) {
      h.remove();
    });
  }

  function setEditMode(on) {
    var card = cardEl();
    var chk = $("dspLayoutEdit");
    if (chk) chk.checked = !!on;
    if (!card) return;
    if (on) {
      var working = loadWorking() || snapshotFromDom();
      if (!working) return;
      saveWorking(working);
      applyLayout(working);
      card.classList.add("is-layout-edit");
      ensureHandles();
    } else {
      card.classList.remove("is-layout-edit");
      removeHandles();
      /* mantém posições se houver working */
      var keep = loadWorking();
      if (keep) applyLayout(keep);
    }
  }

  function updateWorkingItem(id, box) {
    var working = loadWorking() || { v: 1, items: {} };
    working.items = working.items || {};
    working.items[id] = clampBox(id, box);
    saveWorking(working);
    applyBox(itemEl(id), working.items[id]);
  }

  function fillSelect() {
    var sel = $("dspLayoutSelect");
    if (!sel) return;
    var models = loadModels();
    var names = Object.keys(models).sort();
    var active = "";
    try {
      active = localStorage.getItem(KEY_ACTIVE) || "";
    } catch (e) {}
    sel.innerHTML = "";
    var opt0 = document.createElement("option");
    opt0.value = "";
    opt0.textContent = names.length ? "— escolher —" : "Nenhum modelo ainda";
    sel.appendChild(opt0);
    names.forEach(function (n) {
      var o = document.createElement("option");
      o.value = n;
      o.textContent = n;
      if (n === active) o.selected = true;
      sel.appendChild(o);
    });
  }

  function bindDrag() {
    var card = cardEl();
    if (!card) return;
    var state = null;

    function onMove(ev) {
      if (!state) return;
      if (!state.cw || !state.ch) return;
      var dx = ((ev.clientX - state.startX) / state.cw) * 100;
      var dy = ((ev.clientY - state.startY) / state.ch) * 100;
      var box = Object.assign({}, state.box);
      if (state.mode === "resize") {
        box.w = state.box.w + dx;
        box.h = state.box.h + dy;
      } else {
        box.x = state.box.x + dx;
        box.y = state.box.y + dy;
      }
      box = clampBox(state.id, box);
      applyBox(state.el, box);
      state.live = box;
    }

    function endDrag(ev) {
      if (!state) return;
      if (state.live) updateWorkingItem(state.id, state.live);
      state.el.classList.remove("is-layout-on");
      try {
        if (ev && ev.pointerId != null) state.el.releasePointerCapture(ev.pointerId);
      } catch (e) {}
      state = null;
      window.removeEventListener("pointermove", onMove, true);
      window.removeEventListener("pointerup", endDrag, true);
      window.removeEventListener("pointercancel", endDrag, true);
    }

    function onDown(ev) {
      if (!card.classList.contains("is-layout-edit")) return;
      if (ev.button !== undefined && ev.button !== 0) return;
      var handle = ev.target.closest(".dsp-layout-handle");
      var el = ev.target.closest(".dsp-layout-item[data-layout]");
      if (!el || !card.contains(el)) return;
      var id = el.getAttribute("data-layout");
      var working = loadWorking();
      if (!working || !working.items || !working.items[id]) {
        working = snapshotFromDom();
        if (!working) return;
        saveWorking(working);
      }
      if (!working.items[id]) return;
      pushHistory();
      var box = Object.assign({}, working.items[id]);
      var cr = card.getBoundingClientRect();
      state = {
        id: id,
        el: el,
        mode: handle ? "resize" : "move",
        startX: ev.clientX,
        startY: ev.clientY,
        box: box,
        cw: cr.width,
        ch: cr.height
      };
      el.classList.add("is-layout-on");
      /* move/up no window (capture): senão a peça “gruda” ou não anda */
      window.addEventListener("pointermove", onMove, true);
      window.addEventListener("pointerup", endDrag, true);
      window.addEventListener("pointercancel", endDrag, true);
      try {
        el.setPointerCapture(ev.pointerId);
      } catch (e2) {}
      ev.preventDefault();
      ev.stopPropagation();
    }

    /* capture=true: pega o clique antes do pan da foto */
    card.addEventListener("pointerdown", onDown, true);
  }

  function bindUndoKeys() {
    document.addEventListener("keydown", function (ev) {
      var tag = (ev.target && ev.target.tagName) || "";
      if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return;
      var mod = ev.ctrlKey || ev.metaKey;
      if (!mod) return;
      var key = String(ev.key || "").toLowerCase();
      if (key === "z" && !ev.shiftKey) {
        if (undo()) {
          ev.preventDefault();
        }
      } else if (key === "y" || (key === "z" && ev.shiftKey)) {
        if (redo()) {
          ev.preventDefault();
        }
      }
    });
  }

  function bindUi() {
    var edit = $("dspLayoutEdit");
    var loadBtn = $("dspLayoutLoad");
    var saveBtn = $("dspLayoutSave");
    var delBtn = $("dspLayoutDelete");
    var resetBtn = $("dspLayoutReset");

    fillSelect();

    if (edit) {
      edit.addEventListener("change", function () {
        setEditMode(edit.checked);
      });
    }

    if (loadBtn) {
      loadBtn.addEventListener("click", function () {
        var sel = $("dspLayoutSelect");
        var name = sel && sel.value;
        if (!name) {
          window.alert("Escolha um modelo na lista.");
          return;
        }
        var models = loadModels();
        var layout = models[name];
        if (!layout || !layout.items) {
          window.alert("Modelo não encontrado.");
          return;
        }
        pushHistory();
        saveWorking(layout);
        try {
          localStorage.setItem(KEY_ACTIVE, name);
        } catch (e) {}
        applyLayout(layout);
        if (edit) {
          edit.checked = true;
          setEditMode(true);
        }
        window.alert("Modelo «" + name + "» aplicado.");
      });
    }

    if (saveBtn) {
      saveBtn.addEventListener("click", function () {
        var working = loadWorking();
        if (!working || !working.items) {
          working = snapshotFromDom();
          if (!working) {
            window.alert("Não consegui ler o layout da prévia.");
            return;
          }
          applyLayout(working);
          saveWorking(working);
        }
        var name = window.prompt("Nome do modelo:", "");
        if (!name) return;
        name = String(name).trim().slice(0, 40);
        if (!name) return;
        var models = loadModels();
        models[name] = { v: 1, items: working.items, savedAt: Date.now() };
        if (!saveModels(models)) return;
        try {
          localStorage.setItem(KEY_ACTIVE, name);
        } catch (e2) {}
        fillSelect();
        window.alert("Modelo «" + name + "» salvo neste computador.");
      });
    }

    if (delBtn) {
      delBtn.addEventListener("click", function () {
        var sel = $("dspLayoutSelect");
        var name = sel && sel.value;
        if (!name) {
          window.alert("Escolha um modelo para apagar.");
          return;
        }
        if (!window.confirm("Apagar o modelo «" + name + "»?")) return;
        var models = loadModels();
        delete models[name];
        saveModels(models);
        try {
          if (localStorage.getItem(KEY_ACTIVE) === name) localStorage.removeItem(KEY_ACTIVE);
        } catch (e) {}
        fillSelect();
      });
    }

    if (resetBtn) {
      resetBtn.addEventListener("click", function () {
        if (!window.confirm("Voltar ao layout padrão da folha?")) return;
        pushHistory();
        resetToFlow();
        if (edit) edit.checked = false;
        fillSelect();
      });
    }

    /* restaura working ao abrir */
    var working = loadWorking();
    if (working) {
      requestAnimationFrame(function () {
        applyLayout(working);
      });
    }
  }

  function init() {
    if (!cardEl()) return;
    bindDrag();
    bindUi();
    bindUndoKeys();
  }

  global.DspLayoutEdit = {
    init: init,
    snapshotFromDom: snapshotFromDom,
    applyLayout: applyLayout,
    resetToFlow: resetToFlow,
    fillSelect: fillSelect,
    layoutEditOn: layoutEditOn,
    pushHistory: pushHistory,
    undo: undo,
    redo: redo,
    setFitHooks: setFitHooks
  };
})(window);
