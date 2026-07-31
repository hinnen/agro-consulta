/**
 * Bug report — formulário + print automático.
 * Acesso: botão flutuante 🐞 (só janela top) · Alt+B
 */
(function (global) {
  'use strict';

  var DEVICE_ID_KEY = 'agro_device_id_v1';
  var DEVICE_LABEL_KEY = 'agro_device_label_v1';
  var ROOT_ID = 'agro-bug-report-root';
  var REACH_ID = 'agro-bug-reach';
  var BUG_ICON = '\uD83D\uDC1E'; /* 🐞 */
  var HTML2CANVAS_SRC = 'https://cdn.jsdelivr.net/npm/html2canvas@1.4.1/dist/html2canvas.min.js';
  /** Acima do pane Gestão (3620) e da barra lateral (3634); abaixo do PIN. */
  var Z_FORM = 2147483645;
  var Z_REACH = 2147483637;

  var open = false;
  var sending = false;
  var html2canvasLoading = null;

  function isTopWindow() {
    try {
      return global === global.top;
    } catch (e) {
      return true;
    }
  }

  function csrfToken() {
    try {
      var m = document.cookie.match(/(?:^|;\s*)csrftoken=([^;]+)/);
      if (m) return decodeURIComponent(m[1]);
    } catch (e) {}
    var el = document.querySelector('[name=csrfmiddlewaretoken]');
    return el ? el.value : '';
  }

  function uuid() {
    try {
      if (global.crypto && typeof global.crypto.randomUUID === 'function') {
        return global.crypto.randomUUID();
      }
    } catch (e) {}
    return 'd-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 10);
  }

  function deviceId() {
    try {
      var id = localStorage.getItem(DEVICE_ID_KEY);
      if (id && id.length >= 8) return id;
      id = uuid();
      localStorage.setItem(DEVICE_ID_KEY, id);
      return id;
    } catch (e) {
      return uuid();
    }
  }

  function deviceLabel() {
    try {
      return (localStorage.getItem(DEVICE_LABEL_KEY) || '').trim();
    } catch (e) {
      return '';
    }
  }

  function setDeviceLabel(v) {
    try {
      localStorage.setItem(DEVICE_LABEL_KEY, String(v || '').trim().slice(0, 80));
    } catch (e) {}
  }

  function sugestaoNomePc() {
    var atual = deviceLabel();
    if (atual) return atual;
    try {
      var ponto = String(
        (document.body && document.body.getAttribute('data-ponto-caixa')) ||
          (document.body && document.body.getAttribute('data-pdv-ponto')) ||
          ''
      ).toLowerCase();
      if (ponto.indexOf('vila') >= 0) return 'Vila Elias';
      if (ponto.indexOf('notebook') >= 0) return 'Notebook Centro';
      if (ponto.indexOf('teste') >= 0) return 'Caixa Teste';
      if (ponto.indexOf('gaveta') >= 0 || ponto.indexOf('centro') >= 0) return 'Caixa Centro';
    } catch (e) {}
    return '';
  }

  function usuarioNomePadrao() {
    try {
      var meta = document.querySelector('meta[name="agro-user-display"]');
      if (meta && meta.content) return String(meta.content).trim();
    } catch (e) {}
    return '';
  }

  function versaoApp() {
    try {
      var meta = document.querySelector('meta[name="agro-app-version"]');
      if (meta && meta.content) return String(meta.content).trim();
      if (global.AGRO_APP_VERSION) return String(global.AGRO_APP_VERSION);
    } catch (e) {}
    return '';
  }

  function telaStr() {
    try {
      return (
        Math.round(global.screen.width || 0) +
        'x' +
        Math.round(global.screen.height || 0) +
        '@' +
        Math.round(global.devicePixelRatio || 1)
      );
    } catch (e) {
      return '';
    }
  }

  function ensureCss() {
    if (document.getElementById('agro-bug-report-css')) return;
    var st = document.createElement('style');
    st.id = 'agro-bug-report-css';
    st.textContent =
      'html.agro-bug-fab-on{--agro-bug-safe-left:3.55rem}' +
      /* Flutuante: fora da barra Gestão; com overlay Caixa vai para a direita (canto livre). */
      '#' +
      REACH_ID +
      '{position:fixed;left:max(.55rem,env(safe-area-inset-left,0px));right:auto;bottom:max(.55rem,env(safe-area-inset-bottom,0px));z-index:' +
      Z_REACH +
      ';width:2.85rem;height:2.85rem;padding:0;border-radius:999px;border:2px solid #f87171;background:#7f1d1d;color:#fecaca;font-size:1.35rem;line-height:1;display:inline-flex;align-items:center;justify-content:center;cursor:pointer;box-shadow:0 4px 16px rgba(15,23,42,.28);touch-action:manipulation}' +
      'body.agro-has-inapp-tabbar #' +
      REACH_ID +
      '{left:max(.2rem,calc((var(--agro-inapp-side-w,48px) - 2.85rem) / 2));right:auto}' +
      'html.agro-pdv-overlay-open #' +
      REACH_ID +
      '{left:auto;right:max(.55rem,env(safe-area-inset-right,0px))}' +
      '#' +
      REACH_ID +
      ':hover{background:#991b1b;color:#fff;border-color:#ef4444}' +
      '#' +
      REACH_ID +
      '[hidden]{display:none!important}' +
      /* Empurra barras inferiores / docks para não ficarem sob o 🐞 */
      'html.agro-bug-fab-on #pdv-main-footer,' +
      'html.agro-bug-fab-on #compra-mobile-nav,' +
      'html.agro-bug-fab-on #barra-carrinho,' +
      'html.agro-bug-fab-on .cf-dinheiro-footer,' +
      'html.agro-bug-fab-on footer.shrink-0.border-t,' +
      'html.agro-bug-fab-on [data-agro-bug-safe-bar="1"]' +
      '{padding-left:max(0.75rem,var(--agro-bug-safe-left))!important;box-sizing:border-box}' +
      'html.agro-pdv-overlay-open.agro-bug-fab-on #pdv-main-footer,' +
      'html.agro-pdv-overlay-open.agro-bug-fab-on #compra-mobile-nav,' +
      'html.agro-pdv-overlay-open.agro-bug-fab-on #barra-carrinho,' +
      'html.agro-pdv-overlay-open.agro-bug-fab-on .cf-dinheiro-footer,' +
      'html.agro-pdv-overlay-open.agro-bug-fab-on footer.shrink-0.border-t,' +
      'html.agro-pdv-overlay-open.agro-bug-fab-on [data-agro-bug-safe-bar="1"]' +
      '{padding-left:revert-layer;padding-left:initial}' +
      '.agro-bug-overlay{position:fixed;inset:0;z-index:' +
      Z_FORM +
      ';display:flex;align-items:center;justify-content:center;padding:1rem;background:rgba(15,23,42,.48);backdrop-filter:blur(2px);font-family:system-ui,sans-serif}' +
      '.agro-bug-panel{width:min(100%,26rem);max-height:min(94dvh,560px);overflow:auto;border-radius:1.15rem;border:2px solid rgba(239,68,68,.35);background:linear-gradient(180deg,#fff 0%,#f8fafc 100%);box-shadow:0 24px 60px rgba(15,23,42,.28);padding:1.15rem 1.25rem 1.25rem;color:#0f172a}' +
      '.agro-bug-title{margin:0 0 .25rem;font-size:1.2rem;font-weight:900}' +
      '.agro-bug-lead{margin:0 0 .75rem;font-size:.88rem;color:#64748b;line-height:1.4}' +
      '.agro-bug-label{display:block;margin:0 0 .25rem;font-size:.7rem;font-weight:800;text-transform:uppercase;letter-spacing:.04em;color:#475569}' +
      '.agro-bug-input,.agro-bug-ta{width:100%;box-sizing:border-box;border:2px solid #cbd5e1;border-radius:.75rem;padding:.55rem .7rem;font-size:.95rem;font-weight:600;color:#0f172a;background:#fff;margin-bottom:.65rem}' +
      '.agro-bug-ta{min-height:4.2rem;resize:vertical;font-weight:500}' +
      '.agro-bug-input:focus,.agro-bug-ta:focus{outline:none;border-color:#ef4444}' +
      '.agro-bug-meta{margin:0 0 .75rem;font-size:.72rem;font-weight:600;color:#94a3b8;line-height:1.35}' +
      '.agro-bug-actions{display:flex;flex-wrap:wrap;gap:.5rem}' +
      '.agro-bug-btn{flex:1 1 7rem;min-height:2.75rem;border-radius:.75rem;border:2px solid transparent;font-size:.9rem;font-weight:800;cursor:pointer}' +
      '.agro-bug-btn--muted{background:#fff;border-color:#cbd5e1;color:#475569}' +
      '.agro-bug-btn--primary{background:#dc2626;color:#fff}' +
      '.agro-bug-btn--primary:disabled{opacity:.5;cursor:not-allowed}' +
      '.agro-bug-ok{margin:0;font-size:1.05rem;font-weight:900;color:#047857;text-align:center}' +
      '.agro-bug-err{margin:.35rem 0 0;font-size:.82rem;font-weight:700;color:#b91c1c}';
    document.head.appendChild(st);
  }

  var SAFE_BAR_SEL =
    '#pdv-main-footer,#compra-mobile-nav,#barra-carrinho,.cf-dinheiro-footer,#pdv-step1-subtotal-dock,footer.shrink-0.border-t,[class*="footer"][class*="fixed"],nav.fixed.bottom-0,div.fixed.bottom-0';

  function markKnownSafeBars() {
    try {
      var nodes = document.querySelectorAll(SAFE_BAR_SEL);
      for (var i = 0; i < nodes.length; i++) {
        nodes[i].setAttribute('data-agro-bug-safe-bar', '1');
      }
    } catch (e) {}
  }

  /** Se ainda houver botão/campo sob o 🐞, empurra a barra mais próxima. */
  function dodgeFabCollisions() {
    if (!isTopWindow()) return;
    var fab = document.getElementById(REACH_ID);
    if (!fab || fab.hasAttribute('hidden') || open) return;
    var rect;
    try {
      rect = fab.getBoundingClientRect();
    } catch (e) {
      return;
    }
    if (!rect || rect.width < 4) return;
    var pts = [
      [rect.left + rect.width * 0.5, rect.top + rect.height * 0.5],
      [rect.left + 4, rect.top + rect.height * 0.5],
      [rect.right - 4, rect.top + rect.height * 0.5],
      [rect.left + rect.width * 0.5, rect.top + 4],
      [rect.left + rect.width * 0.5, rect.bottom - 4],
    ];
    var overlayOpen = false;
    try {
      overlayOpen = document.documentElement.classList.contains('agro-pdv-overlay-open');
    } catch (e2) {}
    for (var p = 0; p < pts.length; p++) {
      var hits;
      try {
        hits = document.elementsFromPoint(pts[p][0], pts[p][1]) || [];
      } catch (e3) {
        continue;
      }
      for (var i = 0; i < hits.length; i++) {
        var el = hits[i];
        if (!el || el === fab || (fab.contains && fab.contains(el))) continue;
        if (el.id === ROOT_ID || (el.closest && el.closest('#' + ROOT_ID))) continue;
        if (el.id === 'agro-display-scale-fab') continue;
        if (el.id === 'agro-inapp-sidebar-rail' || (el.closest && el.closest('#agro-inapp-sidebar-rail'))) continue;
        if (el.tagName === 'IFRAME' || el.tagName === 'HTML' || el.tagName === 'BODY') continue;
        var interactive =
          el.closest &&
          el.closest(
            'button, a, input, select, textarea, label, summary, [role="button"], [role="tab"], [role="menuitem"]'
          );
        if (!interactive) continue;
        var bar =
          interactive.closest &&
          interactive.closest(
            'footer, nav, #pdv-main-footer, #compra-mobile-nav, #barra-carrinho, .cf-dinheiro-footer, #pdv-step1-subtotal-dock, [data-agro-bug-safe-bar="1"], .agro-pdv-overlay-head, .agro-pdv-overlay-actions'
          );
        var target = bar || interactive;
        if (overlayOpen && target && target.closest && target.closest('.agro-pdv-overlay-panel')) {
          /* FAB já foi para a direita no overlay — se ainda colidir, sobe um pouco */
          fab.style.bottom = '4.25rem';
          return;
        }
        var need = Math.ceil(rect.width + 14);
        try {
          var cs = global.getComputedStyle(target);
          var pl = parseFloat(cs.paddingLeft) || 0;
          if (pl < need) {
            target.style.paddingLeft = need + 'px';
            target.setAttribute('data-agro-bug-safe-bar', '1');
          }
        } catch (e4) {}
        return;
      }
    }
    try {
      fab.style.bottom = '';
    } catch (e5) {}
  }

  function applySafeZones() {
    if (!isTopWindow()) return;
    ensureCss();
    try {
      document.documentElement.classList.add('agro-bug-fab-on');
    } catch (e) {}
    markKnownSafeBars();
    dodgeFabCollisions();
  }

  function wireSafeZones() {
    if (global.__agroBugSafeZones) return;
    global.__agroBugSafeZones = true;
    var t = null;
    function schedule() {
      if (t) clearTimeout(t);
      t = setTimeout(function () {
        t = null;
        applySafeZones();
      }, 80);
    }
    try {
      global.addEventListener('resize', schedule);
      document.addEventListener('visibilitychange', schedule);
      var mo = new MutationObserver(schedule);
      mo.observe(document.documentElement, {
        attributes: true,
        attributeFilter: ['class', 'data-pdv-step'],
      });
      if (document.body) {
        mo.observe(document.body, { attributes: true, attributeFilter: ['class', 'data-pdv-step'] });
      }
    } catch (e) {}
    schedule();
  }

  function ensureReachButton() {
    ensureCss();
    var btn = document.getElementById(REACH_ID);
    if (!isTopWindow()) {
      if (btn) btn.remove();
      return null;
    }
    if (!btn) {
      btn = document.createElement('button');
      btn.type = 'button';
      btn.id = REACH_ID;
      btn.title = 'Bug — enviar feedback (também Alt+B)';
      btn.setAttribute('aria-label', 'Bug — enviar feedback');
      btn.textContent = BUG_ICON;
      document.body.appendChild(btn);
      btn.addEventListener('click', function (ev) {
        ev.preventDefault();
        ev.stopPropagation();
        requestOpen();
      });
    } else {
      btn.textContent = BUG_ICON;
    }
    if (open) btn.setAttribute('hidden', '');
    else btn.removeAttribute('hidden');
    applySafeZones();
    return btn;
  }

  function requestOpen() {
    if (!isTopWindow()) {
      try {
        global.top.postMessage({ type: 'agro-bug-report-open' }, global.location.origin);
        return;
      } catch (e) {}
    }
    openModal();
  }

  function wireHotkey() {
    if (global.__agroBugHotkey) return;
    global.__agroBugHotkey = true;
    document.addEventListener('keydown', function (ev) {
      try {
        var altB = ev.altKey && !ev.ctrlKey && !ev.metaKey && String(ev.key || '').toLowerCase() === 'b';
        var ctrlShiftB =
          ev.ctrlKey && ev.shiftKey && !ev.altKey && String(ev.key || '').toLowerCase() === 'b';
        if (!altB && !ctrlShiftB) return;
        ev.preventDefault();
        requestOpen();
      } catch (e) {}
    });
  }

  function loadHtml2Canvas() {
    if (global.html2canvas) return Promise.resolve(global.html2canvas);
    if (html2canvasLoading) return html2canvasLoading;
    html2canvasLoading = new Promise(function (resolve, reject) {
      var s = document.createElement('script');
      s.src = HTML2CANVAS_SRC;
      s.async = true;
      s.onload = function () {
        if (global.html2canvas) resolve(global.html2canvas);
        else reject(new Error('html2canvas não carregou'));
      };
      s.onerror = function () {
        reject(new Error('Falha ao carregar html2canvas'));
      };
      document.head.appendChild(s);
    });
    return html2canvasLoading;
  }

  function maskSensitive() {
    var nodes = document.querySelectorAll(
      'input[type="password"], input[name*="pin"], input[id*="pin"], input[name*="Pin"], input[id*="Pin"], input[name*="senha"], input[id*="senha"]'
    );
    var saved = [];
    for (var i = 0; i < nodes.length; i++) {
      var el = nodes[i];
      saved.push({ el: el, value: el.value });
      try {
        el.value = el.value ? '••••••' : '';
      } catch (e) {}
    }
    return function restore() {
      for (var j = 0; j < saved.length; j++) {
        try {
          saved[j].el.value = saved[j].value;
        } catch (e2) {}
      }
    };
  }

  function capturePrint() {
    var restore = maskSensitive();
    var overlay = document.getElementById(ROOT_ID);
    var hide = [];
    [REACH_ID].forEach(function (id) {
      var el = document.getElementById(id);
      if (el) {
        hide.push(el);
        el.style.visibility = 'hidden';
      }
    });
    if (overlay) overlay.style.visibility = 'hidden';
    return loadHtml2Canvas()
      .then(function (h2c) {
        return h2c(document.body, {
          useCORS: true,
          allowTaint: true,
          logging: false,
          scale: Math.min(1, 1280 / Math.max(document.documentElement.clientWidth || 1280, 1)),
          windowWidth: document.documentElement.clientWidth,
          windowHeight: document.documentElement.clientHeight,
        });
      })
      .then(function (canvas) {
        var maxW = 1280;
        var out = canvas;
        if (canvas.width > maxW) {
          var ratio = maxW / canvas.width;
          var c2 = document.createElement('canvas');
          c2.width = maxW;
          c2.height = Math.round(canvas.height * ratio);
          c2.getContext('2d').drawImage(canvas, 0, 0, c2.width, c2.height);
          out = c2;
        }
        return out.toDataURL('image/jpeg', 0.55);
      })
      .catch(function () {
        return '';
      })
      .finally(function () {
        restore();
        hide.forEach(function (el) {
          el.style.visibility = '';
        });
        if (overlay) overlay.style.visibility = '';
      });
  }

  function closeModal() {
    var root = document.getElementById(ROOT_ID);
    if (root) root.remove();
    open = false;
    ensureReachButton();
  }

  function showOk(id) {
    var root = document.getElementById(ROOT_ID);
    if (!root) return;
    root.innerHTML =
      '<div class="agro-bug-panel" role="dialog" aria-modal="true">' +
      '<p class="agro-bug-ok">Recebido — #' +
      id +
      '</p>' +
      '<div class="agro-bug-actions" style="margin-top:1rem"><button type="button" class="agro-bug-btn agro-bug-btn--primary" id="agro-bug-ok-close">OK</button></div>' +
      '</div>';
    document.getElementById('agro-bug-ok-close').onclick = closeModal;
    setTimeout(closeModal, 2200);
  }

  function openModal() {
    if (open) return;
    ensureCss();
    open = true;
    ensureReachButton();
    var root = document.createElement('div');
    root.id = ROOT_ID;
    root.className = 'agro-bug-overlay';
    root.innerHTML =
      '<div class="agro-bug-panel" role="dialog" aria-modal="true" aria-labelledby="agro-bug-title">' +
      '<h2 class="agro-bug-title" id="agro-bug-title">Bug — enviar feedback</h2>' +
      '<p class="agro-bug-lead">Conte em poucas palavras. Print da tela (com popup/overlay aberto) vai junto automaticamente.</p>' +
      '<label class="agro-bug-label" for="agro-bug-aconteceu">O que aconteceu?</label>' +
      '<textarea class="agro-bug-ta" id="agro-bug-aconteceu" maxlength="4000" placeholder="Ex.: Cliquei em pagar e a tela travou"></textarea>' +
      '<label class="agro-bug-label" for="agro-bug-esperava">O que esperava?</label>' +
      '<textarea class="agro-bug-ta" id="agro-bug-esperava" maxlength="2000" placeholder="Ex.: Devia abrir o pagamento"></textarea>' +
      '<label class="agro-bug-label" for="agro-bug-usuario">Seu nome</label>' +
      '<input class="agro-bug-input" id="agro-bug-usuario" maxlength="120" autocomplete="name" />' +
      '<label class="agro-bug-label" for="agro-bug-pc">Nome deste computador</label>' +
      '<input class="agro-bug-input" id="agro-bug-pc" maxlength="80" placeholder="Ex.: Caixa Centro / Notebook / Vila" />' +
      '<p class="agro-bug-meta" id="agro-bug-meta"></p>' +
      '<div class="agro-bug-actions">' +
      '<button type="button" class="agro-bug-btn agro-bug-btn--muted" id="agro-bug-cancel">Cancelar</button>' +
      '<button type="button" class="agro-bug-btn agro-bug-btn--primary" id="agro-bug-send">Enviar</button>' +
      '</div>' +
      '<p class="agro-bug-err" id="agro-bug-err" hidden></p>' +
      '</div>';
    document.body.appendChild(root);
    document.getElementById('agro-bug-usuario').value = usuarioNomePadrao();
    document.getElementById('agro-bug-pc').value = sugestaoNomePc();
    document.getElementById('agro-bug-meta').textContent =
      'PC id ' +
      deviceId().slice(0, 8) +
      ' · ' +
      (location.pathname || '/') +
      (versaoApp() ? ' · v' + versaoApp() : '') +
      ' · Alt+B';
    document.getElementById('agro-bug-cancel').onclick = closeModal;
    root.addEventListener('click', function (ev) {
      if (ev.target === root) closeModal();
    });
    document.getElementById('agro-bug-send').onclick = submitReport;
    setTimeout(function () {
      document.getElementById('agro-bug-aconteceu').focus();
    }, 50);
  }

  function setErr(msg) {
    var el = document.getElementById('agro-bug-err');
    if (!el) return;
    if (!msg) {
      el.hidden = true;
      el.textContent = '';
      return;
    }
    el.hidden = false;
    el.textContent = msg;
  }

  function submitReport() {
    if (sending) return;
    var aconteceu = (document.getElementById('agro-bug-aconteceu').value || '').trim();
    if (aconteceu.length < 3) {
      setErr('Escreva o que aconteceu.');
      return;
    }
    var esperava = (document.getElementById('agro-bug-esperava').value || '').trim();
    var usuario = (document.getElementById('agro-bug-usuario').value || '').trim();
    var pcNome = (document.getElementById('agro-bug-pc').value || '').trim();
    if (pcNome) setDeviceLabel(pcNome);
    var btn = document.getElementById('agro-bug-send');
    sending = true;
    btn.disabled = true;
    btn.textContent = 'Enviando…';
    setErr('');
    capturePrint()
      .then(function (printData) {
        return fetch('/api/bug-report/', {
          method: 'POST',
          credentials: 'same-origin',
          headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrfToken() },
          body: JSON.stringify({
            o_que_aconteceu: aconteceu,
            o_que_esperava: esperava,
            usuario_nome: usuario,
            device_id: deviceId(),
            dispositivo_nome: pcNome || sugestaoNomePc(),
            url_pagina: String(location.href || '').slice(0, 500),
            user_agent: String(navigator.userAgent || '').slice(0, 400),
            tela: telaStr(),
            versao_app: versaoApp(),
            print_base64: printData || '',
          }),
        });
      })
      .then(function (r) {
        return r.text().then(function (t) {
          var j = null;
          try {
            j = JSON.parse(t);
          } catch (e) {
            j = null;
          }
          if (!j) {
            var login =
              r.status === 401 ||
              r.status === 403 ||
              /Acessar|login|entrar/i.test(String(t || '').slice(0, 800));
            return {
              j: {
                ok: false,
                erro: login
                  ? 'Sessão expirada — entre de novo no sistema e tente outra vez.'
                  : 'Resposta inválida do servidor (' + r.status + ').',
              },
            };
          }
          return { j: j };
        });
      })
      .then(function (pack) {
        sending = false;
        if (!pack.j || !pack.j.ok) {
          btn.disabled = false;
          btn.textContent = 'Enviar';
          setErr((pack.j && pack.j.erro) || 'Não deu para enviar. Tente de novo.');
          return;
        }
        showOk(pack.j.id);
      })
      .catch(function () {
        sending = false;
        btn.disabled = false;
        btn.textContent = 'Enviar';
        setErr('Falha de rede. Verifique a internet.');
      });
  }

  function wireOpenMessage() {
    if (global.__agroBugReportMsg) return;
    global.__agroBugReportMsg = true;
    global.addEventListener('message', function (ev) {
      try {
        if (ev.origin !== global.location.origin) return;
        var d = ev.data || {};
        if (d && d.type === 'agro-bug-report-open') requestOpen();
      } catch (e) {}
    });
  }

  function boot() {
    try {
      deviceId();
      wireOpenMessage();
      wireHotkey();
      wireSafeZones();
      ensureReachButton();
      global.AgroBugReport = { open: requestOpen, close: closeModal, deviceId: deviceId };
      setTimeout(ensureReachButton, 400);
      setTimeout(ensureReachButton, 1500);
    } catch (e) {}
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot);
  } else {
    boot();
  }
})(window);
