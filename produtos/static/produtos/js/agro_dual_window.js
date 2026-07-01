/**
 * SisVale — duas janelas Chrome (PDV + gestão), uma instância cada.
 * Links do PDV → overlay balcão (AgroPdvOverlay). Gestão → janela nomeada.
 */
(function () {
  'use strict';

  var PDV_NAME = 'SistValePDV';
  var GESTAO_NAME = 'SistValeGestao';
  var FLAG_KEY = 'agro_dual_window_v1';
  var FOCUS_KEY = 'agro_dual_focus_v1';
  var APP_ROLE_KEY = 'agro_app_role_v1';

  function pathnameNorm(p) {
    if (p) {
      return String(p).toLowerCase().replace(/\/+$/, '') || '/';
    }
    return String(window.location.pathname || '')
      .toLowerCase()
      .replace(/\/+$/, '') || '/';
  }

  function isPdvPath(p) {
    p = pathnameNorm(p);
    return p === '/pdv' || p.indexOf('/pdv/') === 0 || p === '/consulta' || p.indexOf('/consulta/') === 0;
  }

  function isGestaoShellPath(p) {
    p = pathnameNorm(p);
    return p === '/' || p === '/atalhos' || p.indexOf('/atalhos/') === 0 || p.indexOf('/dashboard') === 0;
  }

  function inEmbed() {
    try {
      if (window.top !== window.self) return true;
      var q = new URLSearchParams(window.location.search || '');
      return q.get('agro_inapp_embed') === '1' || q.get('agro_pdv_overlay') === '1';
    } catch (_) {
      return window.top !== window.self;
    }
  }

  function inPdvOverlayFrame() {
    try {
      return new URLSearchParams(window.location.search || '').get('agro_pdv_overlay') === '1';
    } catch (_) {
      return false;
    }
  }

  function dualFlagOn() {
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

  function enabled() {
    if (inEmbed()) return false;
    return dualFlagOn();
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

  function postToTop(payload) {
    try {
      if (window.top && window.top !== window) {
        window.top.postMessage(payload, window.location.origin);
        return true;
      }
    } catch (_) {}
    return false;
  }

  function readAppRole() {
    var role = '';
    try {
      var q = new URLSearchParams(window.location.search || '');
      role = String(q.get('agro_app_role') || '').toLowerCase();
      if (role === 'pdv' || role === 'gestao') {
        localStorage.setItem(APP_ROLE_KEY, role);
      } else {
        role = String(localStorage.getItem(APP_ROLE_KEY) || '').toLowerCase();
      }
    } catch (_) {}
    if (role === 'pdv') window.name = PDV_NAME;
    else if (role === 'gestao') window.name = GESTAO_NAME;
    return role;
  }

  function isStandaloneApp() {
    try {
      if (window.matchMedia('(display-mode: standalone)').matches) return true;
      if (window.matchMedia('(display-mode: minimal-ui)').matches) return true;
    } catch (_) {}
    return window.navigator.standalone === true;
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
            'Não foi possível trazer o PDV. Use o atalho «SisVale PDV» na área de trabalho ou permita pop-ups.'
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
    if (inEmbed()) {
      postToTop({ type: 'agro-open-inapp-tab', href: url });
      return;
    }
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

  function openPdvPanel(href, title) {
    var url = absUrl(href);
    if (inPdvOverlayFrame()) {
      postToTop({ type: 'agro-open-inapp-tab', href: url, title: title || '' });
      return;
    }
    if (isPdvHost() && window.AgroPdvOverlay && typeof window.AgroPdvOverlay.open === 'function') {
      window.AgroPdvOverlay.open(url, title);
      return;
    }
    navigateGestao(url);
  }

  function focusPdv(url) {
    var href = url ? absUrl(url) : pdvUrl();
    if (inEmbed()) {
      if (postToTop({ type: 'agro-focus-pdv', href: href })) return;
    }
    if (dualFlagOn()) {
      openPdv(href);
      return;
    }
    if (typeof window.__agroInAppAddTab === 'function') {
      window.__agroInAppAddTab(href);
      return;
    }
    window.location.href = href;
  }

  /** «Início» no PDV → traz janela Gestão (espelho do botão PDV na gestão). */
  function focusGestao(url) {
    var href = url ? absUrl(url) : gestaoUrl();
    if (inEmbed()) {
      if (postToTop({ type: 'agro-focus-gestao', href: href })) return;
    }
    if (isGestaoHost()) {
      if (typeof window.__agroInAppAddTab === 'function') {
        window.__agroInAppAddTab(href);
      } else {
        window.location.href = href;
      }
      return;
    }
    var w = openNamed(GESTAO_NAME, href);
    if (!w || w.closed) {
      if (!isPdvHost()) {
        window.location.href = href;
        return;
      }
      try {
        window.alert(
          'Não foi possível trazer a Gestão. Use o atalho «SisVale Gestão» na área de trabalho ou permita pop-ups.'
        );
      } catch (_) {}
    }
  }

  function isPdvHost() {
    if (window.name === PDV_NAME) return true;
    if (readAppRole() === 'pdv' && !inEmbed()) return true;
    return dualFlagOn() && isPdvPath() && !inEmbed();
  }

  function isGestaoHost() {
    if (window.name === GESTAO_NAME) return true;
    if (readAppRole() === 'gestao' && !inEmbed()) return true;
    return dualFlagOn() && !isPdvPath() && !inEmbed();
  }

  function shouldRoutePdvLinkToGestao(pathname) {
    var p = pathnameNorm(pathname);
    if (isPdvPath(p)) return false;
    if (p === '/healthz') return false;
    if (p.indexOf('/admin') === 0) return false;
    return true;
  }

  function shouldOpenInPdvOverlay(pathname) {
    if (!shouldRoutePdvLinkToGestao(pathname)) return false;
    if (isGestaoShellPath(pathname)) return false;
    return true;
  }

  function assignWindowName() {
    if (!dualFlagOn() || inEmbed()) return;
    readAppRole();
    if (window.name === PDV_NAME || window.name === GESTAO_NAME) return;
    if (isPdvPath()) window.name = PDV_NAME;
    else window.name = GESTAO_NAME;
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

  function installGestaoAppGuard() {
    if (!isGestaoHost() && readAppRole() !== 'gestao') return;
    var nativeOpen = window.open;
    window.open = function (url, name, features) {
      var n = name;
      if (!n || n === '_blank') {
        try {
          var p = url ? new URL(String(url), location.origin).pathname : '';
          if (!url || location.origin === new URL(String(url), location.origin).origin) {
            if (!isPdvPath(p)) n = GESTAO_NAME;
          }
        } catch (_) {
          n = GESTAO_NAME;
        }
      }
      return nativeOpen.call(window, url, n, features);
    };

    document.addEventListener(
      'click',
      function (e) {
        if (!isGestaoHost()) return;
        var a = e.target && e.target.closest ? e.target.closest('a[href]') : null;
        if (!a) return;
        var tgt = (a.getAttribute('target') || '').toLowerCase();
        if (tgt !== '_blank') return;
        try {
          var u = new URL(a.href, window.location.origin);
          if (u.origin !== location.origin) return;
          if (isPdvPath(u.pathname)) {
            e.preventDefault();
            openPdv(u.href);
            return;
          }
          e.preventDefault();
          if (typeof window.__agroInAppAddTab === 'function') {
            window.__agroInAppAddTab(u.href);
          } else {
            openNamed(GESTAO_NAME, u.href);
          }
        } catch (_) {}
      },
      true
    );
  }

  function installTabManager() {
    if (!window.tabManager) window.tabManager = {};
    window.tabManager.addTab = function (_titulo, url, _icone) {
      var u = String(url || '').trim();
      if (!u) return;
      u = absUrl(u);
      var p = '';
      try {
        p = new URL(u).pathname;
      } catch (_) {
        p = '';
      }
      if (inEmbed()) {
        if (isPdvPath(p)) {
          focusPdv(u);
        } else {
          postToTop({ type: 'agro-open-inapp-tab', href: u });
        }
        return;
      }
      if (isPdvHost() && shouldOpenInPdvOverlay(p)) {
        openPdvPanel(u, _titulo);
        return;
      }
      if (dualFlagOn()) {
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

  function installPdvLinkRouter() {
    document.addEventListener(
      'click',
      function (e) {
        if (!isPdvHost()) return;
        if (inPdvOverlayFrame()) return;
        var a = e.target && e.target.closest ? e.target.closest('a[href]') : null;
        if (!a) return;
        if (a.id === 'agro-pdv-fab') return;
        if (a.hasAttribute('data-agro-pdv-native')) return;
        var tgt = (a.getAttribute('target') || '').toLowerCase();
        if (tgt === '_blank' || tgt === '_top') return;
        if (a.hasAttribute('download')) return;
        try {
          var u = new URL(a.href, window.location.origin);
          if (u.origin !== location.origin) return;
          if (!shouldRoutePdvLinkToGestao(u.pathname)) return;
          e.preventDefault();
          e.stopPropagation();
          if (shouldOpenInPdvOverlay(u.pathname)) {
            openPdvPanel(u.href, (a.textContent || '').trim());
          } else if (isGestaoShellPath(u.pathname)) {
            focusGestao(u.href);
          } else {
            navigateGestao(u.href);
          }
        } catch (_) {}
      },
      true
    );
  }

  function installPdvOverlayBridge() {
    window.addEventListener(
      'message',
      function (ev) {
        try {
          if (!ev || ev.origin !== location.origin) return;
          if (!isPdvHost() || inEmbed()) return;
          var d = ev.data || {};
          if (d.type === 'agro-open-inapp-tab' && d.href) {
            if (window.AgroPdvOverlay && window.AgroPdvOverlay.isOpen && window.AgroPdvOverlay.isOpen()) {
              window.AgroPdvOverlay.open(d.href, d.title || '');
            } else if (shouldOpenInPdvOverlay(new URL(d.href, location.origin).pathname)) {
              openPdvPanel(d.href, d.title || '');
            } else {
              navigateGestao(d.href);
            }
          }
        } catch (_) {}
      },
      false
    );
  }

  readAppRole();
  assignWindowName();
  installTabManager();
  installPdvLinkRouter();
  installPdvOverlayBridge();
  installGestaoAppGuard();
  window.addEventListener('storage', onStorageFocus);

  window.AgroDualWindow = {
    PDV_NAME: PDV_NAME,
    GESTAO_NAME: GESTAO_NAME,
    enabled: enabled,
    dualFlagOn: dualFlagOn,
    isPdvHost: isPdvHost,
    isGestaoHost: isGestaoHost,
    isPdvPath: isPdvPath,
    inEmbed: inEmbed,
    isStandaloneApp: isStandaloneApp,
    openPdv: openPdv,
    openGestao: openGestao,
    navigateGestao: navigateGestao,
    openPdvPanel: openPdvPanel,
    focusPdv: focusPdv,
    focusGestao: focusGestao,
    pdvUrl: pdvUrl,
    gestaoUrl: gestaoUrl,
  };
})();
