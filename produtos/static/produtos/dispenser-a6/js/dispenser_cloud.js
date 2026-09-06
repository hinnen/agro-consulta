/* Dispenser A6 — sync biblioteca com Postgres (Centro + Vila).
   Fotos/folhas grandes: NUVEM + RAM da página — NÃO no bolso do Chrome (localStorage). */
(function (global) {
  var KEYS = {
    logos: "dsp_logos_custom_v1",
    pets: "dsp_pets_custom_v1",
    ings: "dsp_ings_custom_v1",
    flavorIcos: "dsp_flavor_icos_v1",
    customFlavors: "dsp_custom_flavors_v1",
    folhas: "dsp_folhas_v1",
    layouts: "dsp_layouts_v1",
    migrateDone: "dsp_cloud_migrate_done_v1"
  };

  /* null = ainda não semeado; depois = arrays/objetos em RAM */
  var mem = {
    logos: null,
    pets: null,
    ings: null,
    flavorIcos: null,
    customFlavors: null,
    folhas: null
  };

  function csrf() {
    var m = document.cookie.match(/(?:^|;\s*)csrftoken=([^;]+)/);
    if (m) return decodeURIComponent(m[1]);
    var el = document.querySelector("[name=csrfmiddlewaretoken]");
    return el ? el.value : "";
  }

  function api(url, opts) {
    opts = opts || {};
    var headers = opts.headers || {};
    headers["X-CSRFToken"] = csrf();
    if (opts.json != null) {
      headers["Content-Type"] = "application/json";
      opts.body = JSON.stringify(opts.json);
    }
    return fetch(url, {
      method: opts.method || "GET",
      credentials: "same-origin",
      headers: headers,
      body: opts.body
    }).then(function (r) {
      return r.json().then(function (j) {
        if (!r.ok || (j && j.ok === false)) {
          var err = (j && j.erro) || ("HTTP " + r.status);
          return Promise.reject(new Error(err));
        }
        return j;
      });
    });
  }

  function readLocal(key, fallback) {
    try {
      var raw = localStorage.getItem(key);
      if (!raw) return fallback;
      var v = JSON.parse(raw);
      return v == null ? fallback : v;
    } catch (e) {
      return fallback;
    }
  }

  function writeLocal(key, value) {
    try {
      localStorage.setItem(key, JSON.stringify(value));
      return true;
    } catch (e) {
      return false;
    }
  }

  function clearMidiaLocalStorage() {
    try {
      localStorage.removeItem(KEYS.logos);
      localStorage.removeItem(KEYS.pets);
      localStorage.removeItem(KEYS.ings);
      localStorage.removeItem(KEYS.flavorIcos);
      localStorage.removeItem(KEYS.customFlavors);
      localStorage.removeItem(KEYS.folhas);
    } catch (e) {}
  }

  function ensureMemSeeded() {
    if (mem.logos == null) {
      mem.logos = Array.isArray(readLocal(KEYS.logos, [])) ? readLocal(KEYS.logos, []) : [];
      mem.pets = Array.isArray(readLocal(KEYS.pets, [])) ? readLocal(KEYS.pets, []) : [];
      mem.ings = Array.isArray(readLocal(KEYS.ings, [])) ? readLocal(KEYS.ings, []) : [];
      var icos = readLocal(KEYS.flavorIcos, {});
      mem.flavorIcos = icos && typeof icos === "object" ? icos : {};
      var cf = readLocal(KEYS.customFlavors, {});
      mem.customFlavors = cf && typeof cf === "object" ? cf : {};
    }
    if (mem.folhas == null) {
      /* 1ª carga: herda residual do Chrome só para migrar; depois fica só RAM + Postgres */
      var fl = readLocal(KEYS.folhas, {});
      mem.folhas = fl && typeof fl === "object" ? Object.assign({}, fl) : {};
    }
  }

  function getLogos() {
    ensureMemSeeded();
    return mem.logos.slice();
  }
  function setLogos(list) {
    ensureMemSeeded();
    mem.logos = Array.isArray(list) ? list.slice() : [];
    return true;
  }

  function getPets() {
    ensureMemSeeded();
    return mem.pets.slice();
  }
  function setPets(list) {
    ensureMemSeeded();
    mem.pets = Array.isArray(list) ? list.slice() : [];
    return true;
  }

  function getIngs() {
    ensureMemSeeded();
    return mem.ings.slice();
  }
  function setIngs(list) {
    ensureMemSeeded();
    mem.ings = Array.isArray(list) ? list.slice() : [];
    return true;
  }

  function getFlavorIcos() {
    ensureMemSeeded();
    return Object.assign({}, mem.flavorIcos);
  }
  function setFlavorIcos(obj) {
    ensureMemSeeded();
    mem.flavorIcos = obj && typeof obj === "object" ? Object.assign({}, obj) : {};
    return true;
  }

  function getCustomFlavors() {
    ensureMemSeeded();
    return Object.assign({}, mem.customFlavors);
  }
  function setCustomFlavors(obj) {
    ensureMemSeeded();
    mem.customFlavors = obj && typeof obj === "object" ? Object.assign({}, obj) : {};
    if (global.DspFlavorLib && global.DspFlavorLib.registerCustoms) {
      global.DspFlavorLib.registerCustoms(mem.customFlavors);
    }
    return true;
  }

  function syncFlavorLibCustoms() {
    ensureMemSeeded();
    if (global.DspFlavorLib && global.DspFlavorLib.registerCustoms) {
      global.DspFlavorLib.registerCustoms(mem.customFlavors || {});
    }
  }

  function getFolhas() {
    ensureMemSeeded();
    return Object.assign({}, mem.folhas);
  }
  function setFolhas(obj) {
    ensureMemSeeded();
    mem.folhas = obj && typeof obj === "object" ? Object.assign({}, obj) : {};
    /* não grava folhas no Chrome — evita «memória cheia» e não pesa o PDV */
    try {
      localStorage.removeItem(KEYS.folhas);
    } catch (e) {}
    return true;
  }

  function localMidiaHasContent() {
    ensureMemSeeded();
    if (mem.logos.length || mem.pets.length || mem.ings.length) return true;
    if (Object.keys(mem.flavorIcos).length) return true;
    if (Object.keys(mem.customFlavors || {}).length) return true;
    /* residual no Chrome (antes do purge) */
    if ((readLocal(KEYS.logos, []) || []).length) return true;
    if ((readLocal(KEYS.pets, []) || []).length) return true;
    if ((readLocal(KEYS.ings, []) || []).length) return true;
    var icos = readLocal(KEYS.flavorIcos, {});
    if (icos && typeof icos === "object" && Object.keys(icos).length) return true;
    var cf = readLocal(KEYS.customFlavors, {});
    if (cf && typeof cf === "object" && Object.keys(cf).length) return true;
    return false;
  }

  function localHasContent() {
    if (localMidiaHasContent()) return true;
    ensureMemSeeded();
    if (Object.keys(mem.folhas || {}).length) return true;
    var folhas = readLocal(KEYS.folhas, {});
    var layouts = readLocal(KEYS.layouts, {});
    if (folhas && typeof folhas === "object" && Object.keys(folhas).length) return true;
    if (layouts && typeof layouts === "object") {
      var n = 0;
      Object.keys(layouts).forEach(function (k) {
        var m = layouts[k];
        if (m && m.kind === "folha") return;
        n += 1;
      });
      if (n) return true;
    }
    return false;
  }

  function collectLocalForMigrar() {
    ensureMemSeeded();
    var midias = {
      logo: mem.logos.slice(),
      pet: mem.pets.slice(),
      ing: mem.ings.slice(),
      flavor_ico: []
    };
    Object.keys(mem.flavorIcos).forEach(function (name) {
      if (!mem.flavorIcos[name]) return;
      midias.flavor_ico.push({
        id: String(name).slice(0, 80),
        label: name,
        dataUrl: mem.flavorIcos[name]
      });
    });
    var folhasMem = Object.assign({}, mem.folhas || {});
    var folhasLs = readLocal(KEYS.folhas, {});
    if (folhasLs && typeof folhasLs === "object") {
      Object.keys(folhasLs).forEach(function (n) {
        if (!folhasMem[n]) folhasMem[n] = folhasLs[n];
      });
    }
    var layouts = readLocal(KEYS.layouts, {});
    var docs = { folha: {}, layout: {}, sabor: {} };
    Object.keys(folhasMem).forEach(function (n) {
      docs.folha[n] = folhasMem[n];
    });
    if (layouts && typeof layouts === "object") {
      Object.keys(layouts).forEach(function (n) {
        var m = layouts[n];
        if (!m || m.kind === "folha") return;
        docs.layout[n] = m;
      });
    }
    Object.keys(mem.customFlavors || {}).forEach(function (k) {
      var it = mem.customFlavors[k];
      if (!it) return;
      docs.sabor[String(k).slice(0, 80)] = {
        v: 1,
        label: it.label || k,
        desc: it.desc || ""
      };
    });
    return { midias: midias, documentos: docs };
  }

  function applyBibliotecaToLocal(data) {
    if (!data) return;
    var midias = data.midias || {};
    mem.logos = Array.isArray(midias.logo) ? midias.logo.slice() : [];
    mem.pets = Array.isArray(midias.pet) ? midias.pet.slice() : [];
    mem.ings = Array.isArray(midias.ing) ? midias.ing.slice() : [];
    var icos = {};
    (midias.flavor_ico || []).forEach(function (it) {
      if (it && it.id && it.dataUrl) icos[it.id] = it.dataUrl;
    });
    mem.flavorIcos = icos;

    var docs = data.documentos || {};
    mem.folhas =
      docs.folha && typeof docs.folha === "object" ? Object.assign({}, docs.folha) : {};

    /* libera o bolso do Chrome (fotos + folhas pesadas) */
    clearMidiaLocalStorage();

    var layoutsLocal = readLocal(KEYS.layouts, {});
    var cleaned = {};
    Object.keys(layoutsLocal).forEach(function (n) {
      if (layoutsLocal[n] && layoutsLocal[n].kind === "folha") cleaned[n] = layoutsLocal[n];
    });
    var serverLayouts = docs.layout && typeof docs.layout === "object" ? docs.layout : {};
    Object.keys(serverLayouts).forEach(function (n) {
      cleaned[n] = serverLayouts[n];
    });
    /* layouts leves podem ficar no Chrome; folhas kind=folha sobem pro mem se ainda não vieram */
    Object.keys(cleaned).forEach(function (n) {
      if (cleaned[n] && cleaned[n].kind === "folha" && !mem.folhas[n]) {
        mem.folhas[n] = cleaned[n];
        delete cleaned[n];
      }
    });
    writeLocal(KEYS.layouts, cleaned);

    var cf = {};
    var serverSabores = docs.sabor && typeof docs.sabor === "object" ? docs.sabor : {};
    Object.keys(serverSabores).forEach(function (k) {
      var entry = serverSabores[k];
      if (!entry || typeof entry !== "object") return;
      cf[k] = {
        label: String(entry.label || k).trim() || k,
        desc: String(entry.desc || entry.descricao || "").trim()
      };
    });
    mem.customFlavors = cf;
    syncFlavorLibCustoms();
  }

  function getBiblioteca() {
    return api("/interno/dispenser-a6/api/biblioteca/");
  }

  function upsertMidia(tipo, item) {
    if (!item || !item.id || !item.dataUrl) {
      return Promise.reject(new Error("Imagem inválida."));
    }
    return api("/interno/dispenser-a6/api/midia/", {
      method: "POST",
      json: {
        tipo: tipo,
        item_id: item.id,
        label: item.label || item.id,
        data_url: item.dataUrl
      }
    });
  }

  function deleteMidia(tipo, itemId) {
    var q =
      "/interno/dispenser-a6/api/midia/?tipo=" +
      encodeURIComponent(tipo) +
      "&item_id=" +
      encodeURIComponent(itemId);
    return api(q, { method: "DELETE" }).catch(function (e) {
      console.warn("DspCloud del midia", e);
      return null;
    });
  }

  function upsertDocumento(tipo, nome, payload, thumb) {
    return api("/interno/dispenser-a6/api/documento/", {
      method: "POST",
      json: {
        tipo: tipo,
        nome: nome,
        payload: payload || {},
        thumb: thumb || ""
      }
    }).catch(function (e) {
      console.warn("DspCloud doc", e);
      window.alert("Não deu para gravar no sistema: " + (e && e.message ? e.message : e));
      return null;
    });
  }

  function deleteDocumento(tipo, nome) {
    var q =
      "/interno/dispenser-a6/api/documento/?tipo=" +
      encodeURIComponent(tipo) +
      "&nome=" +
      encodeURIComponent(nome);
    return api(q, { method: "DELETE" }).catch(function (e) {
      console.warn("DspCloud del doc", e);
      return null;
    });
  }

  function migrar(payload) {
    return api("/interno/dispenser-a6/api/migrar/", {
      method: "POST",
      json: payload
    });
  }

  function migrateDone() {
    try {
      return localStorage.getItem(KEYS.migrateDone) === "1";
    } catch (e) {
      return false;
    }
  }

  function setMigrateDone() {
    try {
      localStorage.setItem(KEYS.migrateDone, "1");
    } catch (e) {}
  }

  global.DspCloud = {
    KEYS: KEYS,
    getBiblioteca: getBiblioteca,
    upsertMidia: upsertMidia,
    deleteMidia: deleteMidia,
    upsertDocumento: upsertDocumento,
    deleteDocumento: deleteDocumento,
    migrar: migrar,
    applyBibliotecaToLocal: applyBibliotecaToLocal,
    collectLocalForMigrar: collectLocalForMigrar,
    localHasContent: localHasContent,
    migrateDone: migrateDone,
    setMigrateDone: setMigrateDone,
    clearMidiaLocalStorage: clearMidiaLocalStorage,
    getLogos: getLogos,
    setLogos: setLogos,
    getPets: getPets,
    setPets: setPets,
    getIngs: getIngs,
    setIngs: setIngs,
    getFlavorIcos: getFlavorIcos,
    setFlavorIcos: setFlavorIcos,
    getCustomFlavors: getCustomFlavors,
    setCustomFlavors: setCustomFlavors,
    syncFlavorLibCustoms: syncFlavorLibCustoms,
    getFolhas: getFolhas,
    setFolhas: setFolhas
  };
})(window);
