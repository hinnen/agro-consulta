/**
 * SisVale — duas janelas Chrome (PDV + gestão), uma instância cada.
 * Modo atalho (--app): localStorage + BroadcastChannel (window.open nao liga apps).
 */
(function () {
  'use strict';

  var PDV_NAME = 'SistValePDV';
  var GESTAO_NAME = 'SistValeGestao';
  var FLAG_KEY = 'agro_dual_window_v1';
  var FOCUS_PDV_KEY = 'agro_dual_focus_pdv_v1';
  var FOCUS_GESTAO_KEY = 'agro_dual_focus_gestao_v1';
  var HEARTBEAT_PDV_KEY = 'agro_dual_heartbeat_pdv_v1';
  var HEARTBEAT_GESTAO_KEY = 'agro_dual_heartbeat_gestao_v1';
  var APP_ROLE_KEY = 'agro_app_role_v1';
  var BC_NAME = 'agro_dual_window_v1';

  var broadcast = null;
  try {
    broadcast = new BroadcastChannel(BC_NAME);
  } catch (_) {
    broadcast = null;
  }

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
    } catch (_) {}
    return false;
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
      if (inPdvOverlayFrame() || (inEmbed() && q.get('agro_pdv_overlay') === '1')) {
        // Consultas no overlay do balcão — sempre extensão do PDV (não herdar gestão do localStorage).
        role = 'pdv';
      } else if (isPdvPath()) {
        // Balcão (/pdv/, /consulta/) vence agro_app_role=gestao no localStorage compartilhado.
        role = 'pdv';
      } else {
        role = String(q.get('agro_app_role') || '').toLowerCase();
        if (role === 'pdv' || role === 'gestao') {
          localStorage.setItem(APP_ROLE_KEY, role);
        } else if (isGestaoShellPath()) {
          role = 'gestao';
        } else {
          role = String(localStorage.getItem(APP_ROLE_KEY) || '').toLowerCase();
        }
      }
      if (role === 'pdv' || role === 'gestao') {
        localStorage.setItem(APP_ROLE_KEY, role);
      }
    } catch (_) {}
    return role;
  }

  function isStandaloneApp() {
    try {
      if (window.matchMedia('(display-mode: standalone)').matches) return true;
      if (window.matchMedia('(display-mode: minimal-ui)').matches) return true;
    } catch (_) {}
    return window.navigator.standalone === true;
  }

  /** Atalho .lnk Chrome --app= (nao compartilha window.name entre janelas). */
  function appShortcutMode() {
    var r = readAppRole();
    return r === 'pdv' || r === 'gestao' || isStandaloneApp();
  }

  function emitBroadcast(msg) {
    try {
      if (broadcast) broadcast.postMessage(msg);
    } catch (_) {}
  }

  function pulsePdvFocus(href) {
    var payload = { url: href || pdvUrl(), ts: Date.now() };
    try {
      localStorage.setItem(FOCUS_PDV_KEY, JSON.stringify(payload));
    } catch (_) {}
    emitBroadcast({ type: 'focus-pdv', url: payload.url, ts: payload.ts });
  }

  function pulseGestaoFocus(href) {
    var payload = { url: href || gestaoUrl(), ts: Date.now(), role: GESTAO_NAME };
    try {
      localStorage.setItem(FOCUS_GESTAO_KEY, JSON.stringify(payload));
    } catch (_) {}
    emitBroadcast({ type: 'focus-gestao', url: payload.url, ts: payload.ts });
  }

  function peerRecentlyAlive(key) {
    try {
      var t = parseInt(localStorage.getItem(key), 10);
      return Number.isFinite(t) && Date.now() - t < 10000;
    } catch (_) {}
    return false;
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

  function openPdv(url) {
    var href = url ? absUrl(url) : pdvUrl();
    if (isPdvHost()) {
      try {
        window.focus();
      } catch (_) {}
      return null;
    }
    pulsePdvFocus(href);
    if (appShortcutMode()) {
      window.setTimeout(function () {
        if (!peerRecentlyAlive(HEARTBEAT_PDV_KEY)) {
          try {
            window.alert(
              'PDV nao encontrado. Abra o atalho «SisVale PDV» na area de trabalho (deixe os dois abertos).'
            );
          } catch (_) {}
        }
      }, 450);
      return null;
    }
    var w = openNamed(PDV_NAME, href);
    if (!w || w.closed) {
      try {
        window.alert(
          'Nao foi possivel trazer o PDV. Use o atalho «SisVale PDV» ou permita pop-ups.'
        );
      } catch (_) {}
    }
    return w;
  }

  function openGestao(url) {
    var href = url ? absUrl(url) : gestaoUrl();
    if (isGestaoHost()) {
      try {
        window.focus();
      } catch (_) {}
      return null;
    }
    pulseGestaoFocus(href);
    if (appShortcutMode()) {
      window.setTimeout(function () {
        if (!peerRecentlyAlive(HEARTBEAT_GESTAO_KEY)) {
          try {
            window.alert(
              'Gestao nao encontrada. Abra o atalho «SisVale Gestao» na area de trabalho.'
            );
          } catch (_) {}
        }
      }, 450);
      return null;
    }
    var w = openNamed(GESTAO_NAME, href);
    if ((!w || w.closed) && !isPdvHost()) {
      window.location.href = href;
    }
    return w;
  }

  function navigateGestao(href) {
    var url = absUrl(href || gestaoUrl());
    var navPath = '';
    try {
      navPath = pathnameNorm(new URL(url, window.location.origin).pathname);
    } catch (_) {
      navPath = '';
    }
    if (inEmbed()) {
      postToTop({ type: 'agro-open-inapp-tab', href: url });
      return;
    }
    if (
      (isPdvHost() || isPdvPath() || readAppRole() === 'pdv') &&
      shouldOpenInPdvOverlay(navPath)
    ) {
      openPdvPanel(url);
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
    if (appShortcutMode() && readAppRole() === 'pdv') {
      var navPath = '';
      try {
        navPath = pathnameNorm(new URL(url, window.location.origin).pathname);
      } catch (_) {
        navPath = '';
      }
      if (shouldOpenInPdvOverlay(navPath)) {
        openPdvPanel(url);
        return;
      }
      if (isPdvHost()) {
        window.location.assign(url);
        return;
      }
      pulseGestaoFocus(url);
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
    openGestao(url);
  }

  function openPdvPanel(href, title) {
    var url = absUrl(href);
    if (inPdvOverlayFrame()) {
      postToTop({ type: 'agro-open-inapp-tab', href: url, title: title || '' });
      return;
    }
    var path = '';
    try {
      path = pathnameNorm(new URL(url, window.location.origin).pathname);
    } catch (_) {
      path = '';
    }
    var onPdvBalcao = isPdvHost();
    if (onPdvBalcao && shouldOpenInPdvOverlay(path)) {
      if (window.AgroPdvOverlay && typeof window.AgroPdvOverlay.open === 'function') {
        try {
          window.AgroPdvOverlay.open(url, title, { force: true });
          return;
        } catch (_) {}
      }
      window.location.assign(url);
      return;
    }
    navigateGestao(url);
  }

  function focusPdv(url) {
    var href = url ? absUrl(url) : pdvUrl();
    if (inEmbed()) {
      if (postToTop({ type: 'agro-focus-pdv', href: href })) return;
    }
    openPdv(href);
  }

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
      try {
        window.focus();
      } catch (_) {}
      return;
    }
    openGestao(href);
  }

  function isPdvHost() {
    if (inPdvOverlayFrame()) return false;
    if (inEmbed()) return false;
    if (!isPdvPath()) return false;
    return window.top === window.self;
  }

  function isGestaoHost() {
    if (window.name === GESTAO_NAME) return true;
    if (readAppRole() === 'gestao' && !inEmbed()) return true;
    return dualFlagOn() && !isPdvPath() && !inEmbed();
  }

  /** Gestão (atalho ou shell) — inclui BI dentro do iframe com agro_inapp_embed. */
  function isGestaoContext() {
    if (isPdvHost()) return false;
    if (inPdvOverlayFrame()) return false;
    if (readAppRole() === 'gestao') return true;
    if (window.name === GESTAO_NAME) return true;
    try {
      if (window.top !== window.self && window.top.name === GESTAO_NAME) return true;
    } catch (_) {}
    if (isGestaoHost()) return true;
    try {
      if (inEmbed() && new URLSearchParams(window.location.search || '').get('agro_inapp_embed') === '1') {
        return readAppRole() !== 'pdv';
      }
    } catch (_) {}
    return false;
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
    if (isPdvPath()) {
      window.name = PDV_NAME;
      return;
    }
    if (isGestaoShellPath()) {
      window.name = GESTAO_NAME;
    }
  }

  function applyPdvFocus(data) {
    if (!data) return;
    if (Date.now() - Number(data.ts || 0) > 12000) return;
    if (readAppRole() !== 'pdv' && !isPdvHost()) return;
    try {
      window.focus();
    } catch (_) {}
    if (data.url) {
      var want = absUrl(data.url).replace(/#.*$/, '');
      var cur = location.href.replace(/#.*$/, '');
      if (cur !== want) window.location.href = data.url;
    }
  }

  function applyGestaoFocus(data) {
    if (!data) return;
    if (Date.now() - Number(data.ts || 0) > 12000) return;
    // Janela PDV / balcão nunca deve receber foco Gestão (evita BI no overlay ou redirect).
    if (isPdvHost() || readAppRole() === 'pdv') return;
    if (readAppRole() !== 'gestao' && !isGestaoHost()) return;
    try {
      window.focus();
    } catch (_) {}
    if (data.url) {
      if (typeof window.__agroInAppAddTab === 'function') {
        window.__agroInAppAddTab(data.url);
      } else {
        var want = absUrl(data.url).replace(/#.*$/, '');
        var cur = location.href.replace(/#.*$/, '');
        if (cur !== want) window.location.href = data.url;
      }
    }
  }

  function installCrossAppFocusListeners() {
    window.addEventListener('storage', function (ev) {
      if (!ev) return;
      if (ev.key === FOCUS_PDV_KEY && ev.newValue) {
        try {
          applyPdvFocus(JSON.parse(ev.newValue));
        } catch (_) {}
      }
      if (ev.key === FOCUS_GESTAO_KEY && ev.newValue) {
        try {
          applyGestaoFocus(JSON.parse(ev.newValue));
        } catch (_) {}
      }
    });
    if (broadcast) {
      broadcast.onmessage = function (ev) {
        var d = (ev && ev.data) || {};
        if (d.type === 'focus-pdv') applyPdvFocus({ url: d.url, ts: d.ts || Date.now() });
        if (d.type === 'focus-gestao') applyGestaoFocus({ url: d.url, ts: d.ts || Date.now() });
      };
    }
  }

  function installHeartbeat() {
    window.setInterval(function () {
      try {
        if (readAppRole() === 'pdv' || isPdvHost()) {
          localStorage.setItem(HEARTBEAT_PDV_KEY, String(Date.now()));
        }
        if (readAppRole() === 'gestao' || isGestaoHost()) {
          localStorage.setItem(HEARTBEAT_GESTAO_KEY, String(Date.now()));
        }
      } catch (_) {}
    }, 2500);
  }

  function installGestaoAppGuard() {
    if (!isGestaoHost() && readAppRole() !== 'gestao') return;
    document.addEventListener(
      'click',
      function (e) {
        if (!isGestaoHost() && readAppRole() !== 'gestao') return;
        var a = e.target && e.target.closest ? e.target.closest('a[href]') : null;
        if (!a) return;
        var tgt = (a.getAttribute('target') || '').toLowerCase();
        if (tgt !== '_blank') return;
        try {
          var u = new URL(a.href, window.location.origin);
          if (u.origin !== location.origin) return;
          e.preventDefault();
          if (isPdvPath(u.pathname)) {
            focusPdv(u.href);
            return;
          }
          if (typeof window.__agroInAppAddTab === 'function') {
            window.__agroInAppAddTab(u.href);
          } else {
            pulseGestaoFocus(u.href);
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
      if ((isPdvHost() || isPdvPath()) && shouldOpenInPdvOverlay(p)) {
        openPdvPanel(u, _titulo);
        return;
      }
      if (isPdvPath(p)) {
        focusPdv(u);
        return;
      }
      if (isGestaoHost() || readAppRole() === 'gestao') {
        if (typeof window.__agroInAppAddTab === 'function') {
          window.__agroInAppAddTab(u);
        } else {
          navigateGestao(u);
        }
        return;
      }
      if (dualFlagOn()) {
        navigateGestao(u);
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
            var msgPath = '';
            try {
              msgPath = pathnameNorm(new URL(d.href, location.origin).pathname);
            } catch (_) {
              msgPath = '';
            }
            if (window.AgroPdvOverlay && window.AgroPdvOverlay.isOpen && window.AgroPdvOverlay.isOpen()) {
              // Não trocar overlay de caixa/consulta pelo BI (mensagem vinda da janela Gestão).
              if (shouldOpenInPdvOverlay(msgPath)) {
                window.AgroPdvOverlay.open(d.href, d.title || '', { force: true });
              }
            } else if (shouldOpenInPdvOverlay(msgPath)) {
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
  installCrossAppFocusListeners();
  installHeartbeat();
  installTabManager();
  installPdvLinkRouter();
  installPdvOverlayBridge();
  installGestaoAppGuard();

  window.AgroDualWindow = {
    PDV_NAME: PDV_NAME,
    GESTAO_NAME: GESTAO_NAME,
    enabled: enabled,
    dualFlagOn: dualFlagOn,
    appShortcutMode: appShortcutMode,
    isPdvHost: isPdvHost,
    isGestaoHost: isGestaoHost,
    isGestaoContext: isGestaoContext,
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
