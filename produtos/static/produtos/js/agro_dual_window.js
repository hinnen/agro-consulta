/**
 * SisVale — duas janelas Chrome (PDV + gestão), uma instância cada.
 * Não substitui shell Electron (iframe PDV continua lá).
 */
(function () {
  'use strict';

  var PDV_NAME = 'SistValePDV';
  var GESTAO_NAME = 'SistValeGestao';
  var FLAG_KEY = 'agro_dual_window_v1';
  var FOCUS_KEY = 'agro_dual_focus_v1';

  function pathnameNorm() {
    return String(window.location.pathname || '')
      .toLowerCase()
      .replace(/\/+$/, '') || '/';
  }

  function isPdvPath(p) {
    p = p || pathnameNorm();
    return p === '/pdv' || p.indexOf('/pdv/') === 0 || p === '/consulta' || p.indexOf('/consulta/') === 0;
  }

  function isGestaoPath(p) {
    p = p || pathnameNorm();
    if (isPdvPath(p)) return false;
    return p === '/' || p.indexOf('/dashboard') === 0 || p.indexOf('/atalhos') === 0;
  }

  function inEmbed() {
    try {
      if (window.top !== window.self) return true;
      var q = new URLSearchParams(window.location.search || '');
      return q.get('agro_inapp_embed') === '1';
    } catch (_) {
      return window.top !== window.self;
    }
  }

  function enabled() {
    if (inEmbed()) return false;
    if (window.agroShell && typeof window.agroShell.openExternal === 'function') return false;
    try {
      if (localStorage.getItem(FLAG_KEY) === '0') return false;
      if (localStorage.getItem(FLAG_KEY) === '1') return true;
      var q = new URLSearchParams(window.location.search || '');
      if (q.get('agro_dual') === '1') {
        localStorage.setItem(FLAG_KEY, '1');
        return true;
      }
    } catch (_) {}
    return true;
  }

  function absUrl(rel) {
    try {
      return new URL(rel, window.location.origin).href;
    } catch (_) {
      return String(rel || '/');
    }
  }

  function pdvUrl(sub) {
    return absUrl(sub || '/pdv/');
  }

  function gestaoUrl(sub) {
    return absUrl(sub || '/dashboard/gerencial/');
  }

  function openNamed(name, url) {
    var href = url ? String(url) : '';
    var w = null;
    try {
      w = window.open(href || '', name);
    } catch (_) {
      w = null;
    }
    if (w && !w.closed) {
      try {
        w.focus();
      } catch (_) {}
      if (href) {
        try {
          var cur = String(w.location.href || '');
          if (cur === 'about:blank' || cur.indexOf('about:') === 0) {
            w.location.href = href;
          }
        } catch (_) {}
      }
    }
    return w;
  }

  function pulseGestaoNav(url) {
    try {
      localStorage.setItem(
        FOCUS_KEY,
        JSON.stringify({ role: GESTAO_NAME, url: url, ts: Date.now() })
      );
    } catch (_) {}
  }

  function openPdv(url) {
    var href = url ? absUrl(url) : pdvUrl();
    var w = openNamed(PDV_NAME, href);
    if (!w || w.closed) {
      if (!isPdvHost()) {
        try {
          window.alert(
            'Não foi possível trazer o PDV. Use o ícone PDV na área de trabalho ou permita pop-ups.'
          );
        } catch (_) {}
      } else {
        window.location.href = href;
      }
    }
    return w;
  }

  function openGestao(url) {
    var href = url ? absUrl(url) : gestaoUrl();
    var w = openNamed(GESTAO_NAME, href);
    if ((!w || w.closed) && !isGestaoHost()) {
      window.location.href = href;
    }
    return w;
  }

  function navigateGestao(href) {
    var url = absUrl(href || gestaoUrl());
    if (isGestaoHost()) {
      if (typeof window.__agroInAppAddTab === 'function') {
        window.__agroInAppAddTab(url);
        return;
      }
      window.location.href = url;
      return;
    }
    var w = null;
    try {
      w = window.open('', GESTAO_NAME);
    } catch (_) {
      w = null;
    }
    if (w && !w.closed) {
      try {
        w.postMessage({ type: 'agro-open-inapp-tab', href: url }, window.location.origin);
        w.focus();
        return;
      } catch (_) {}
    }
    openGestao(gestaoUrl());
    pulseGestaoNav(url);
  }

  function isPdvHost() {
    if (window.name === PDV_NAME) return true;
    return enabled() && isPdvPath();
  }

  function isGestaoHost() {
    if (window.name === GESTAO_NAME) return true;
    return enabled() && !isPdvPath();
  }

  function assignWindowName() {
    if (!enabled() || inEmbed()) return;
    if (isPdvPath()) window.name = PDV_NAME;
    else if (!isPdvPath()) window.name = GESTAO_NAME;
  }

  function onStorageFocus(ev) {
    if (!ev || ev.key !== FOCUS_KEY || !ev.newValue || !isGestaoHost()) return;
    try {
      var d = JSON.parse(ev.newValue);
      if (!d || d.role !== GESTAO_NAME || !d.url) return;
      if (Date.now() - Number(d.ts || 0) > 15000) return;
      if (typeof window.__agroInAppAddTab === 'function') {
        window.__agroInAppAddTab(d.url);
      } else {
        window.location.href = d.url;
      }
    } catch (_) {}
  }

  function installTabManager() {
    if (!window.tabManager) window.tabManager = {};
    window.tabManager.addTab = function (_titulo, url, _icone) {
      var u = String(url || '').trim();
      if (!u) return;
      try {
        u = absUrl(u);
      } catch (_) {
        return;
      }
      if (enabled()) {
        var p = '';
        try {
          p = new URL(u).pathname.toLowerCase().replace(/\/+$/, '') || '/';
        } catch (_) {
          p = '';
        }
        if (isPdvPath(p)) {
          openPdv(u);
        } else {
          navigateGestao(u);
        }
        return;
      }
      if (typeof window.__agroInAppAddTab === 'function') {
        window.__agroInAppAddTab(u);
        return;
      }
      window.location.assign(u);
    };
  }

  assignWindowName();
  installTabManager();
  window.addEventListener('storage', onStorageFocus);

  window.AgroDualWindow = {
    PDV_NAME: PDV_NAME,
    GESTAO_NAME: GESTAO_NAME,
    enabled: enabled,
    isPdvHost: isPdvHost,
    isGestaoHost: isGestaoHost,
    isPdvPath: isPdvPath,
    openPdv: openPdv,
    openGestao: openGestao,
    navigateGestao: navigateGestao,
    pdvUrl: pdvUrl,
    gestaoUrl: gestaoUrl,
  };
})();
