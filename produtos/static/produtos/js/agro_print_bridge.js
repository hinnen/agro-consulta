/**
 * Cliente da ponte Agro Etiqueta Print (localhost).
 * Expõe window.agroPrintBridge com silentPrint / listPrinters.
 */
(function (global) {
  'use strict';

  var LS_KEY = 'agro_etq_print_bridge_v1';
  var DEFAULT_PORT = 19192;
  var probeTimer = null;
  var ready = false;
  var lastError = '';
  var listeners = [];

  function loadCfg() {
    var out = { port: DEFAULT_PORT, preferir_direto: true };
    try {
      var raw = localStorage.getItem(LS_KEY);
      if (!raw) return out;
      var j = JSON.parse(raw);
      if (j && typeof j === 'object') {
        var p = parseInt(j.port, 10);
        if (p >= 1024 && p <= 65535) out.port = p;
        if (typeof j.preferir_direto === 'boolean') out.preferir_direto = j.preferir_direto;
      }
    } catch (_) {}
    return out;
  }

  function saveCfg(partial) {
    var cur = loadCfg();
    var next = Object.assign({}, cur, partial || {});
    try {
      localStorage.setItem(LS_KEY, JSON.stringify(next));
    } catch (_) {}
    return next;
  }

  function baseUrl() {
    return 'http://127.0.0.1:' + loadCfg().port;
  }

  function notify() {
    listeners.slice().forEach(function (fn) {
      try {
        fn({ ready: ready, lastError: lastError, cfg: loadCfg() });
      } catch (_) {}
    });
  }

  function onChange(fn) {
    if (typeof fn === 'function') listeners.push(fn);
    return function () {
      listeners = listeners.filter(function (x) {
        return x !== fn;
      });
    };
  }

  function fetchJson(path, opts) {
    opts = opts || {};
    var ctrl = typeof AbortController !== 'undefined' ? new AbortController() : null;
    var t = setTimeout(function () {
      try {
        if (ctrl) ctrl.abort();
      } catch (_) {}
    }, opts.timeoutMs || 4000);
    return fetch(baseUrl() + path, {
      method: opts.method || 'GET',
      headers: opts.body ? { 'Content-Type': 'application/json' } : undefined,
      body: opts.body ? JSON.stringify(opts.body) : undefined,
      mode: 'cors',
      cache: 'no-store',
      signal: ctrl ? ctrl.signal : undefined,
    })
      .then(function (r) {
        return r.json().then(function (j) {
          return { httpOk: r.ok, status: r.status, body: j };
        });
      })
      .finally(function () {
        clearTimeout(t);
      });
  }

  function probe() {
    return fetchJson('/health', { timeoutMs: 2500 })
      .then(function (res) {
        ready = !!(res.httpOk && res.body && res.body.ok);
        lastError = ready ? '' : 'ponte_respondeu_errado';
        notify();
        return ready;
      })
      .catch(function (e) {
        ready = false;
        lastError = String((e && e.name === 'AbortError' && 'timeout') || (e && e.message) || 'offline');
        notify();
        return false;
      });
  }

  function startWatch() {
    if (probeTimer) return;
    probe();
    probeTimer = setInterval(probe, 8000);
  }

  function listPrinters() {
    return fetchJson('/printers', { timeoutMs: 6000 }).then(function (res) {
      if (!res.httpOk || !res.body || !res.body.ok) {
        return { ok: false, printers: [], reason: (res.body && res.body.reason) || 'fail' };
      }
      return { ok: true, printers: res.body.printers || [] };
    });
  }

  function getSizeMap() {
    return fetchJson('/size-map', { timeoutMs: 4000 }).then(function (res) {
      if (!res.httpOk || !res.body || !res.body.ok) {
        return { ok: false, map: {}, reason: (res.body && res.body.reason) || 'fail' };
      }
      return { ok: true, map: res.body.map || {} };
    });
  }

  function setSizeMap(map) {
    return fetchJson('/size-map', {
      method: 'POST',
      body: { map: map || {} },
      timeoutMs: 5000,
    }).then(function (res) {
      if (!res.httpOk || !res.body || !res.body.ok) {
        return { ok: false, map: {}, reason: (res.body && res.body.reason) || 'fail' };
      }
      return { ok: true, map: res.body.map || {} };
    });
  }

  function silentPrint(payload) {
    return fetchJson('/print', {
      method: 'POST',
      body: payload || {},
      timeoutMs: 60000,
    }).then(function (res) {
      if (res.body && typeof res.body === 'object') return res.body;
      return { ok: false, reason: 'bad_response' };
    });
  }

  var api = {
    LS_KEY: LS_KEY,
    DEFAULT_PORT: DEFAULT_PORT,
    loadCfg: loadCfg,
    saveCfg: saveCfg,
    baseUrl: baseUrl,
    probe: probe,
    startWatch: startWatch,
    onChange: onChange,
    listPrinters: listPrinters,
    getSizeMap: getSizeMap,
    setSizeMap: setSizeMap,
    silentPrint: silentPrint,
    isReady: function () {
      return ready;
    },
    lastError: function () {
      return lastError;
    },
  };

  global.agroPrintBridge = api;
  if (typeof document !== 'undefined') {
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', startWatch);
    } else {
      startWatch();
    }
  }
})(typeof window !== 'undefined' ? window : this);
