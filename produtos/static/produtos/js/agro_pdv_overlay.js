/**
 * Painel quase tela cheia sobre o PDV — consultas (vendas, fiado, caixa…) sem sair do balcão.
 */
(function () {
  'use strict';

  var ROOT_ID = 'agro-pdv-overlay';
  var STYLE_ID = 'agro-pdv-overlay-styles-v8';
  var openFlag = false;
  var chromeLocked = false;

  function titleFromUrl(url) {
    try {
      var p = new URL(url, window.location.origin).pathname.toLowerCase();
      if (p.indexOf('/vendas') === 0 || p.indexOf('/venda/') === 0) return 'Consultar vendas';
      if (p.indexOf('/fiado') === 0) return 'Crédito loja · Fiado';
      if (p.indexOf('/caixa') === 0) return 'Caixa';
      if (p.indexOf('/entregas') === 0) return 'Entregas';
      if (p.indexOf('/clientes') === 0) return 'Clientes';
      if (p.indexOf('/lancamentos') === 0) return 'Lançamentos';
      if (p.indexOf('/compras') === 0) return 'Compras';
      if (p.indexOf('/entrada-nota') === 0) return 'Entrada NF';
      if (p.indexOf('/produtos/gestao') === 0) return 'Gestão produtos';
      if (p.indexOf('/atendimento-whatsapp') === 0) return 'WhatsApp';
    } catch (_) {}
    return 'Consulta no balcão';
  }

  function overlayUrl(raw) {
    var u = new URL(String(raw || '/'), window.location.origin);
    if (!u.searchParams.has('agro_pdv_overlay')) u.searchParams.set('agro_pdv_overlay', '1');
    if (!u.searchParams.has('agro_inapp_embed')) u.searchParams.set('agro_inapp_embed', '1');
    return u.href;
  }

  /** URL real do documento no iframe (src do elemento fica desatualizado após cliques internos). */
  function frameDocumentHref(frame) {
    if (!frame) return '';
    try {
      var live = String(frame.contentWindow.location.href || '');
      if (live && live.indexOf('about:') !== 0) return live.split('#')[0];
    } catch (_) {}
    return String(frame.getAttribute('src') || frame.src || '').split('#')[0];
  }

  function normalizePageUrl(href) {
    try {
      var u = new URL(href, window.location.origin);
      var path = u.pathname.replace(/\/+$/, '') || '/';
      return u.origin + path + u.search;
    } catch (_) {
      return String(href || '');
    }
  }

  function navigateFrame(frame, rawHref, options) {
    if (!frame) return;
    options = options || {};
    var target = overlayUrl(rawHref);
    var current = frameDocumentHref(frame);
    var same = normalizePageUrl(current) === normalizePageUrl(target);
    if (same && !options.force) {
      try {
        frame.contentWindow.location.reload();
        return;
      } catch (_) {}
    }
    frame.setAttribute('src', target);
  }

  function ensureStyles() {
    var old = document.getElementById('agro-pdv-overlay-styles');
    if (old) old.remove();
    var oldV2 = document.getElementById('agro-pdv-overlay-styles-v2');
    if (oldV2) oldV2.remove();
    var oldV3 = document.getElementById('agro-pdv-overlay-styles-v3');
    if (oldV3) oldV3.remove();
    var oldV4 = document.getElementById('agro-pdv-overlay-styles-v4');
    if (oldV4) oldV4.remove();
    var oldV5 = document.getElementById('agro-pdv-overlay-styles-v5');
    if (oldV5) oldV5.remove();
    var oldV6 = document.getElementById('agro-pdv-overlay-styles-v6');
    if (oldV6) oldV6.remove();
    var oldV7 = document.getElementById('agro-pdv-overlay-styles-v7');
    if (oldV7) oldV7.remove();
    if (document.getElementById(STYLE_ID)) return;
    var st = document.createElement('style');
    st.id = STYLE_ID;
    st.textContent =
      '#' +
      ROOT_ID +
      '[hidden]{display:none!important}' +
      '#' +
      ROOT_ID +
      '{position:fixed;inset:0;z-index:2147483000;display:flex;align-items:center;justify-content:center;padding:clamp(6px,1.2vw,14px);box-sizing:border-box}' +
      '.agro-pdv-overlay-backdrop{position:absolute;inset:0;background:rgba(15,23,42,.55);backdrop-filter:blur(2px)}' +
      '.agro-pdv-overlay-panel{position:relative;z-index:1;display:flex;flex-direction:column;width:min(98vw,100%);height:min(95vh,100%);max-width:100%;border-radius:1rem;border:3px solid #10b981;background:#f8fafc;box-shadow:0 28px 80px rgba(15,23,42,.35);overflow:hidden}' +
      '.agro-pdv-overlay-panel[data-overlay-size="folha"]{width:min(94vw,64rem);height:min(92vh,52rem);max-width:94vw;max-height:92vh}' +
      '.agro-pdv-overlay-head{display:flex;align-items:center;gap:.55rem;flex-shrink:0;padding:.5rem .75rem;border-bottom:2px solid #e2e8f0;background:linear-gradient(180deg,#fff,#f1f5f9);flex-wrap:nowrap}' +
      '.agro-pdv-overlay-panel.is-chrome-hidden .agro-pdv-overlay-head{display:none!important}' +
      '.agro-pdv-overlay-panel.is-chrome-hidden{border-color:#94a3b8}' +
      '.agro-pdv-overlay-brand{flex-shrink:0;width:2.25rem;height:2.25rem;border-radius:.65rem;border:1px solid #e2e8f0;background:#fff;display:flex;align-items:center;justify-content:center;font-size:.72rem;font-weight:900;box-shadow:0 1px 3px rgba(15,23,42,.08)}' +
      '.agro-pdv-overlay-brand .g{color:#059669}.agro-pdv-overlay-brand .m{color:#f97316}' +
      '.agro-pdv-overlay-titles{flex:1;min-width:0;display:flex;flex-direction:column;gap:.1rem}' +
      '#agro-pdv-overlay-title-text{font-size:clamp(.78rem,1.4vw,.98rem);font-weight:900;text-transform:uppercase;letter-spacing:.04em;color:#0f172a;line-height:1.15;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}' +
      '#agro-pdv-overlay-subtitle-text{font-size:clamp(.65rem,1.1vw,.78rem);font-weight:700;color:#047857;line-height:1.2;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}' +
      '.agro-pdv-overlay-actions{display:flex;align-items:center;gap:.45rem;flex-shrink:0;position:relative}' +
      '#agro-pdv-overlay-help{flex-shrink:0;width:2.65rem;height:2.65rem;border-radius:999px;border:2px solid #cbd5e1;background:#fff;color:#0f172a;font-size:1rem;font-weight:900;cursor:pointer;touch-action:manipulation;display:none;align-items:center;justify-content:center;padding:0}' +
      '#agro-pdv-overlay-help:hover{background:#f8fafc;border-color:#94a3b8}' +
      '#agro-pdv-overlay-help.is-visible{display:inline-flex}' +
      '#agro-pdv-overlay-help-panel{display:none;position:absolute;right:0;top:calc(100% + .35rem);z-index:5;width:min(92vw,22rem);max-height:min(70vh,24rem);overflow:auto;padding:.75rem .9rem;border-radius:.85rem;border:2px solid #cbd5e1;background:#fff;box-shadow:0 16px 40px rgba(15,23,42,.22);font-size:.85rem;font-weight:600;line-height:1.35;color:#1e293b}' +
      '#agro-pdv-overlay-help-panel.is-open{display:block}' +
      '#agro-pdv-overlay-help-panel p{margin:0 0 .55rem}' +
      '#agro-pdv-overlay-help-panel p:last-child{margin-bottom:0}' +
      '#agro-pdv-overlay-menu{flex-shrink:0;min-height:2.65rem;padding:0 .85rem;border-radius:.75rem;border:2px solid #cbd5e1;background:#fff;color:#0f172a;font-size:.75rem;font-weight:900;text-transform:uppercase;letter-spacing:.04em;text-decoration:none;display:inline-flex;align-items:center;cursor:pointer;touch-action:manipulation}' +
      '#agro-pdv-overlay-menu:hover{background:#f8fafc;border-color:#94a3b8}' +
      '#agro-pdv-overlay-menu[hidden]{display:none!important}' +
      '#agro-pdv-overlay-close{flex-shrink:0;min-height:2.65rem;min-width:6.5rem;padding:0 1rem;border-radius:.75rem;border:2px solid #047857;background:linear-gradient(135deg,#10b981,#059669);color:#fff;font-size:.78rem;font-weight:900;text-transform:uppercase;letter-spacing:.05em;cursor:pointer;touch-action:manipulation}' +
      '#agro-pdv-overlay-close:hover{background:linear-gradient(135deg,#34d399,#10b981)}' +
      '#agro-pdv-overlay-wa{display:none;align-items:center;gap:.4rem;flex-shrink:0;margin-right:.15rem}' +
      '#agro-pdv-overlay-wa.is-on{display:inline-flex}' +
      '#agro-pdv-overlay-wa-dot{width:.65rem;height:.65rem;border-radius:999px;background:#94a3b8;flex-shrink:0}' +
      '#agro-pdv-overlay-wa-dot.on{background:#25d366;box-shadow:0 0 0 3px #dcfce7}' +
      '#agro-pdv-overlay-wa-dot.wait{background:#f59e0b;box-shadow:0 0 0 3px #fef3c7}' +
      '#agro-pdv-overlay-wa-dot.off{background:#ef4444;box-shadow:0 0 0 3px #fee2e2}' +
      '#agro-pdv-overlay-wa-pill{display:inline-flex;align-items:center;min-height:2.15rem;padding:0 .65rem;border-radius:999px;font-size:.72rem;font-weight:800;background:#f1f5f9;color:#334155;max-width:11rem;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}' +
      '#agro-pdv-overlay-wa-pill.ok{background:#ecfdf5;color:#047857}' +
      '#agro-pdv-overlay-wa-pill.warn{background:#fff7ed;color:#c2410c}' +
      '#agro-pdv-overlay-wa-pill.bad{background:#fef2f2;color:#b91c1c}' +
      '#agro-pdv-overlay-wa-trocar,#agro-pdv-overlay-wa-bot{flex-shrink:0;min-height:2.35rem;padding:0 .7rem;border-radius:.75rem;border:1px solid #e2e8f0;background:#fff;color:#334155;font-size:.68rem;font-weight:800;letter-spacing:.04em;text-transform:uppercase;cursor:pointer;touch-action:manipulation;text-decoration:none;display:inline-flex;align-items:center}' +
      '#agro-pdv-overlay-wa-trocar:hover,#agro-pdv-overlay-wa-bot:hover{border-color:#86efac;background:#f0fdf4}' +
      '#agro-pdv-overlay-wa-bot{border-color:transparent;background:#25d366;color:#fff}' +
      '#agro-pdv-overlay-wa-bot:hover{filter:brightness(.95);background:#25d366;border-color:transparent}' +
      '#agro-pdv-overlay-wa-trocar[hidden],#agro-pdv-overlay-wa-bot[hidden]{display:none!important}' +
      '#agro-pdv-overlay-frame{flex:1;min-height:0;width:100%;border:0;background:#fff}' +
      'html.agro-pdv-overlay-open,html.agro-pdv-overlay-open body{overflow:hidden!important}';
    document.head.appendChild(st);
  }

  function wireHelp(root) {
    var helpBtn = root.querySelector('#agro-pdv-overlay-help');
    var helpPanel = root.querySelector('#agro-pdv-overlay-help-panel');
    if (!helpBtn || !helpPanel || helpBtn.getAttribute('data-wired') === '1') return;
    helpBtn.setAttribute('data-wired', '1');
    helpBtn.addEventListener('click', function (e) {
      e.preventDefault();
      e.stopPropagation();
      var openNow = !helpPanel.classList.contains('is-open');
      helpPanel.classList.toggle('is-open', openNow);
      helpBtn.setAttribute('aria-expanded', openNow ? 'true' : 'false');
    });
    document.addEventListener('click', function (ev) {
      if (!helpPanel.classList.contains('is-open')) return;
      if (helpPanel.contains(ev.target) || helpBtn.contains(ev.target)) return;
      helpPanel.classList.remove('is-open');
      helpBtn.setAttribute('aria-expanded', 'false');
    });
  }

  function ensureWaChromeUi(root) {
    if (!root) return;
    var actions = root.querySelector('.agro-pdv-overlay-actions');
    if (!actions) return;
    var wa = root.querySelector('#agro-pdv-overlay-wa');
    if (!wa) {
      wa = document.createElement('div');
      wa.id = 'agro-pdv-overlay-wa';
      wa.setAttribute('aria-label', 'WhatsApp');
      wa.innerHTML =
        '<span id="agro-pdv-overlay-wa-dot" class="off" aria-hidden="true"></span>' +
        '<span id="agro-pdv-overlay-wa-pill">…</span>' +
        '<button type="button" id="agro-pdv-overlay-wa-trocar" hidden title="Desligar este Zap e ligar outro">Trocar Zap</button>' +
        '<a href="#" id="agro-pdv-overlay-wa-bot" hidden title="Configurar bot">Bot</a>';
      var help = root.querySelector('#agro-pdv-overlay-help');
      if (help) actions.insertBefore(wa, help);
      else actions.insertBefore(wa, actions.firstChild);
    }
    wireWaChrome(root);
  }

  function wireWaChrome(root) {
    if (!root || root.getAttribute('data-wa-wired') === '1') return;
    root.setAttribute('data-wa-wired', '1');
    var trocar = root.querySelector('#agro-pdv-overlay-wa-trocar');
    var bot = root.querySelector('#agro-pdv-overlay-wa-bot');
    if (trocar) {
      trocar.addEventListener('click', function (e) {
        e.preventDefault();
        var frame = root.querySelector('#agro-pdv-overlay-frame');
        try {
          if (frame && frame.contentWindow) {
            frame.contentWindow.postMessage({ type: 'agro-wa-trocar' }, window.location.origin);
          }
        } catch (_) {}
      });
    }
    if (bot) {
      bot.addEventListener('click', function (e) {
        e.preventDefault();
        var href = bot.getAttribute('data-href') || bot.getAttribute('href') || '/atendimento-whatsapp/bot/';
        open(href, 'Bot WhatsApp', { force: true });
      });
    }
  }

  function ensureHelpUi(root) {
    if (!root) return;
    var actions = root.querySelector('.agro-pdv-overlay-actions');
    if (!actions) return;
    if (!root.querySelector('#agro-pdv-overlay-help')) {
      var btn = document.createElement('button');
      btn.type = 'button';
      btn.id = 'agro-pdv-overlay-help';
      btn.setAttribute('aria-label', 'Ajuda');
      btn.setAttribute('title', 'Ajuda');
      btn.setAttribute('aria-expanded', 'false');
      btn.textContent = '?';
      var panel = document.createElement('div');
      panel.id = 'agro-pdv-overlay-help-panel';
      panel.setAttribute('role', 'region');
      panel.setAttribute('aria-label', 'Orientação');
      var menu = root.querySelector('#agro-pdv-overlay-menu');
      if (menu) {
        actions.insertBefore(btn, menu);
        actions.insertBefore(panel, menu);
      } else {
        actions.insertBefore(btn, actions.firstChild);
        actions.insertBefore(panel, actions.firstChild ? actions.firstChild.nextSibling : null);
      }
    }
    wireHelp(root);
  }

  function ensureRoot() {
    ensureStyles();
    var root = document.getElementById(ROOT_ID);
    if (root) {
      ensureHelpUi(root);
      ensureWaChromeUi(root);
      var staleBug = root.querySelector('#agro-pdv-overlay-bug');
      if (staleBug) staleBug.remove();
      return root;
    }
    root = document.createElement('div');
    root.id = ROOT_ID;
    root.setAttribute('hidden', '');
    root.setAttribute('role', 'dialog');
    root.setAttribute('aria-modal', 'true');
    root.innerHTML =
      '<div class="agro-pdv-overlay-backdrop" data-agro-pdv-overlay-dismiss></div>' +
      '<div class="agro-pdv-overlay-panel">' +
      '<header class="agro-pdv-overlay-head">' +
      '<div class="agro-pdv-overlay-brand" aria-hidden="true"><span class="g">G</span><span class="m">M</span></div>' +
      '<div class="agro-pdv-overlay-titles">' +
      '<span id="agro-pdv-overlay-title-text"></span>' +
      '<span id="agro-pdv-overlay-subtitle-text"></span>' +
      '</div>' +
      '<div class="agro-pdv-overlay-actions">' +
      '<div id="agro-pdv-overlay-wa" aria-label="WhatsApp">' +
      '<span id="agro-pdv-overlay-wa-dot" class="off" aria-hidden="true"></span>' +
      '<span id="agro-pdv-overlay-wa-pill">…</span>' +
      '<button type="button" id="agro-pdv-overlay-wa-trocar" hidden title="Desligar este Zap e ligar outro">Trocar Zap</button>' +
      '<a href="#" id="agro-pdv-overlay-wa-bot" hidden title="Configurar bot">Bot</a>' +
      '</div>' +
      '<button type="button" id="agro-pdv-overlay-help" aria-label="Ajuda" title="Ajuda" aria-expanded="false">?</button>' +
      '<div id="agro-pdv-overlay-help-panel" role="region" aria-label="Orientação"></div>' +
      '<a href="#" id="agro-pdv-overlay-menu" hidden>← Menu</a>' +
      '<button type="button" id="agro-pdv-overlay-close">Fechar</button>' +
      '</div>' +
      '</header>' +
      '<iframe id="agro-pdv-overlay-frame" title="Consulta no balcão"></iframe>' +
      '</div>';
    document.body.appendChild(root);
    root.querySelector('#agro-pdv-overlay-close').addEventListener('click', function () {
      if (chromeLocked) return;
      backOrClose();
    });
    root.querySelector('[data-agro-pdv-overlay-dismiss]').addEventListener('click', function () {
      /* Fundo nao fecha — so X / FECHAR / Esc */
    });
    wireHelp(root);
    wireWaChrome(root);
    var menuBtn = root.querySelector('#agro-pdv-overlay-menu');
    if (menuBtn) {
      menuBtn.addEventListener('click', function (e) {
        e.preventDefault();
        var href = menuBtn.getAttribute('data-href') || menuBtn.href;
        if (!href || href === '#') return;
        var lab = (menuBtn.textContent || '').trim().toUpperCase();
        if (lab === 'CSV' || lab.indexOf('BACKUP') >= 0) {
          window.open(href, '_blank');
          return;
        }
        open(href, 'Caixa', { force: true });
      });
    }
    return root;
  }

  function applyMeta(d) {
    var root = document.getElementById(ROOT_ID);
    if (!root || !d) return;
    ensureHelpUi(root);
    ensureWaChromeUi(root);
    var panel = root.querySelector('.agro-pdv-overlay-panel');
    var titleEl = root.querySelector('#agro-pdv-overlay-title-text');
    var subEl = root.querySelector('#agro-pdv-overlay-subtitle-text');
    var menuBtn = root.querySelector('#agro-pdv-overlay-menu');
    var helpBtn = root.querySelector('#agro-pdv-overlay-help');
    var helpPanel = root.querySelector('#agro-pdv-overlay-help-panel');
    var waBox = root.querySelector('#agro-pdv-overlay-wa');
    var waDot = root.querySelector('#agro-pdv-overlay-wa-dot');
    var waPill = root.querySelector('#agro-pdv-overlay-wa-pill');
    var waTrocar = root.querySelector('#agro-pdv-overlay-wa-trocar');
    var waBot = root.querySelector('#agro-pdv-overlay-wa-bot');
    if (panel && Object.prototype.hasOwnProperty.call(d, 'hideChrome')) {
      chromeLocked = !!d.hideChrome;
      panel.classList.toggle('is-chrome-hidden', chromeLocked);
    }
    if (titleEl && d.title != null) titleEl.textContent = String(d.title || '');
    if (subEl && (d.subtitle != null || d.title != null)) {
      subEl.textContent = String(d.subtitle || '');
      subEl.style.display = d.subtitle ? '' : 'none';
    }
    if (Object.prototype.hasOwnProperty.call(d, 'showWaChrome')) {
      var onWa = !!d.showWaChrome;
      if (waBox) waBox.classList.toggle('is-on', onWa);
      if (!onWa) {
        if (waTrocar) waTrocar.hidden = true;
        if (waBot) waBot.hidden = true;
      }
    }
    if (waDot && d.waDot != null) {
      waDot.className = String(d.waDot || 'off');
    }
    if (waPill && d.waStatus != null) {
      waPill.textContent = String(d.waStatus || '…');
      waPill.className = '';
      waPill.id = 'agro-pdv-overlay-wa-pill';
      var kind = String(d.waStatusKind || '');
      if (kind === 'ok' || kind === 'warn' || kind === 'bad') waPill.classList.add(kind);
    }
    if (waTrocar && Object.prototype.hasOwnProperty.call(d, 'showWaTrocar')) {
      waTrocar.hidden = !d.showWaTrocar;
    }
    if (waBot && Object.prototype.hasOwnProperty.call(d, 'showWaBot')) {
      waBot.hidden = !d.showWaBot;
      if (d.botHref) waBot.setAttribute('data-href', String(d.botHref));
    }
    if (menuBtn && Object.prototype.hasOwnProperty.call(d, 'showMenu')) {
      if (d.showMenu && d.menuHref) {
        menuBtn.hidden = false;
        menuBtn.setAttribute('data-href', String(d.menuHref));
        if (d.menuLabel) menuBtn.textContent = String(d.menuLabel);
      } else {
        menuBtn.hidden = true;
        menuBtn.removeAttribute('data-href');
      }
    }
    if (helpBtn && helpPanel && Object.prototype.hasOwnProperty.call(d, 'showHelp')) {
      var helpHtml = String(d.helpHtml || '').trim();
      if (d.showHelp && helpHtml) {
        helpPanel.innerHTML = helpHtml;
        helpBtn.classList.add('is-visible');
      } else {
        helpPanel.innerHTML = '';
        helpPanel.classList.remove('is-open');
        helpBtn.classList.remove('is-visible');
        helpBtn.setAttribute('aria-expanded', 'false');
      }
    }
  }

  function resetMeta(title) {
    applyMeta({
      title: title || 'Consulta no balcão',
      subtitle: '',
      showMenu: false,
      showHelp: false,
      helpHtml: '',
      hideChrome: false,
      showWaChrome: false,
      showWaTrocar: false,
      showWaBot: false,
      waStatus: '',
      waDot: 'off',
    });
    chromeLocked = false;
  }

  function detectOverlaySize(href) {
    try {
      var p = new URL(href, window.location.origin).pathname.toLowerCase();
      if (p.indexOf('relatorio-saldo') >= 0 || p.indexOf('relatorio-planilha') >= 0) {
        return 'folha';
      }
    } catch (_) {}
    return 'default';
  }

  function applyPanelSize(root, size) {
    var panel = root && root.querySelector('.agro-pdv-overlay-panel');
    if (!panel) return;
    if (size && size !== 'default') {
      panel.setAttribute('data-overlay-size', size);
    } else {
      panel.removeAttribute('data-overlay-size');
    }
  }

  function open(rawUrl, title, options) {
    options = options || {};
    var href = overlayUrl(rawUrl);
    try {
      var p = new URL(href, window.location.origin).pathname.toLowerCase();
      if (p === '/' || p.indexOf('/dashboard') === 0 || p.indexOf('/atalhos') === 0) {
        if (window.AgroDualWindow && typeof window.AgroDualWindow.focusGestao === 'function') {
          window.AgroDualWindow.focusGestao(href);
        }
        return;
      }
    } catch (_) {}
    options.force = true;
    var root = ensureRoot();
    var frame = root.querySelector('#agro-pdv-overlay-frame');
    applyPanelSize(root, options.size || detectOverlaySize(href));
    resetMeta(title || titleFromUrl(href));
    navigateFrame(frame, href, options);
    root.removeAttribute('hidden');
    document.documentElement.classList.add('agro-pdv-overlay-open');
    openFlag = true;
    try {
      root.querySelector('#agro-pdv-overlay-close').focus();
    } catch (_) {}
  }

  function close() {
    var root = document.getElementById(ROOT_ID);
    if (!root) return;
    var frame = root.querySelector('#agro-pdv-overlay-frame');
    if (frame) {
      try {
        frame.contentWindow.location.replace('about:blank');
      } catch (_) {}
      frame.removeAttribute('src');
    }
    root.setAttribute('hidden', '');
    document.documentElement.classList.remove('agro-pdv-overlay-open');
    openFlag = false;
    chromeLocked = false;
    // Após fechar caixa/abrir no overlay, PDV precisa saber se o turno ainda existe
    try {
      if (typeof window.AgroPdvRefreshCaixa === 'function') {
        window.AgroPdvRefreshCaixa();
      }
    } catch (_) {}
  }

  function isOpen() {
    return openFlag;
  }

  /** Esc/Fechar no pai: se o iframe está no Ver venda, volta à lista (1 nível). */
  function tryFrameBackOne() {
    var root = document.getElementById(ROOT_ID);
    var frame = root && root.querySelector('#agro-pdv-overlay-frame');
    if (!frame || !frame.contentWindow) return false;
    try {
      var win = frame.contentWindow;
      var path = String((win.location && win.location.pathname) || '').replace(/\/+$/, '') || '/';
      if (!/^\/venda\/\d+$/.test(path)) return false;
      try {
        var ref = String(win.document.referrer || '');
        if (ref && ref.indexOf('/vendas') >= 0 && win.history.length > 1) {
          win.history.back();
          return true;
        }
      } catch (_) {}
      win.location.href = overlayUrl('/vendas/');
      return true;
    } catch (_) {
      return false;
    }
  }

  function backOrClose() {
    if (tryFrameBackOne()) return;
    close();
  }

  function onKeydown(e) {
    if (!openFlag) return;
    var k = e.key || '';
    if (k === 'Escape' || k === 'F1') {
      if (chromeLocked) {
        e.preventDefault();
        e.stopPropagation();
        return;
      }
      e.preventDefault();
      e.stopPropagation();
      if (k === 'F1') {
        close();
        return;
      }
      backOrClose();
    }
  }

  window.addEventListener(
    'message',
    function (ev) {
      try {
        if (!ev || ev.origin !== location.origin) return;
        var d = ev.data || {};
        if (d.type === 'agro-pdv-overlay-close') {
          close();
          return;
        }
        if (d.type === 'agro-pdv-overlay-fiado-ok') {
          close();
          try {
            window.dispatchEvent(
              new CustomEvent('agro-fiado-cobranca-ok', {
                detail: { msg: String(d.msg || 'Fiado quitado.') },
              })
            );
          } catch (_) {}
          return;
        }
        if (d.type === 'agro-pdv-overlay-meta') {
          applyMeta(d);
          return;
        }
        if (d.type === 'agro-caixa-modal-layer') {
          applyMeta({ hideChrome: !!d.open });
          return;
        }
        if (d.type === 'agro-pdv-caixa-changed') {
          try {
            if (typeof window.AgroPdvRefreshCaixa === 'function') {
              window.AgroPdvRefreshCaixa();
            }
          } catch (_) {}
          return;
        }
        if (d.type === 'agro-open-inapp-tab' && d.href && openFlag) {
          try {
            var p = new URL(d.href, location.origin).pathname.toLowerCase();
            if (p === '/' || p.indexOf('/dashboard') === 0 || p.indexOf('/atalhos') === 0) return;
          } catch (_) {}
          open(d.href, d.title || titleFromUrl(d.href), { force: true });
        }
      } catch (_) {}
    },
    false
  );

  document.addEventListener('keydown', onKeydown, true);

  window.AgroPdvOverlay = {
    open: open,
    close: close,
    isOpen: isOpen,
    isChromeLocked: function () {
      return !!chromeLocked;
    },
    overlayUrl: overlayUrl,
    titleFromUrl: titleFromUrl,
  };
})();
