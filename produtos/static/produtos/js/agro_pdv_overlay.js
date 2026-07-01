/**
 * Painel quase tela cheia sobre o PDV — consultas (vendas, fiado, caixa…) sem sair do balcão.
 */
(function () {
  'use strict';

  var ROOT_ID = 'agro-pdv-overlay';
  var STYLE_ID = 'agro-pdv-overlay-styles';
  var openFlag = false;

  function titleFromUrl(url) {
    try {
      var p = new URL(url, window.location.origin).pathname.toLowerCase();
      if (p.indexOf('/vendas') === 0 || p.indexOf('/venda/') === 0) return 'Consultar vendas';
      if (p.indexOf('/fiado') === 0) return 'Fiado';
      if (p.indexOf('/caixa') === 0) return 'Caixa';
      if (p.indexOf('/entregas') === 0) return 'Entregas';
      if (p.indexOf('/clientes') === 0) return 'Clientes';
      if (p.indexOf('/lancamentos') === 0) return 'Lançamentos';
      if (p.indexOf('/compras') === 0) return 'Compras';
      if (p.indexOf('/entrada-nota') === 0) return 'Entrada NF';
      if (p.indexOf('/produtos/gestao') === 0) return 'Gestão produtos';
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
      '.agro-pdv-overlay-panel{position:relative;z-index:1;display:flex;flex-direction:column;width:min(98vw,100%);height:min(95vh,100%);max-width:100%;border-radius:1rem;border:3px solid #f97316;background:#f8fafc;box-shadow:0 28px 80px rgba(15,23,42,.35);overflow:hidden}' +
      '.agro-pdv-overlay-head{display:flex;align-items:center;gap:.55rem;flex-shrink:0;padding:.5rem .75rem;border-bottom:2px solid #e2e8f0;background:linear-gradient(180deg,#fff,#f1f5f9);flex-wrap:nowrap}' +
      '.agro-pdv-overlay-brand{flex-shrink:0;width:2.25rem;height:2.25rem;border-radius:.65rem;border:1px solid #e2e8f0;background:#fff;display:flex;align-items:center;justify-content:center;font-size:.72rem;font-weight:900;box-shadow:0 1px 3px rgba(15,23,42,.08)}' +
      '.agro-pdv-overlay-brand .g{color:#059669}.agro-pdv-overlay-brand .m{color:#f97316}' +
      '.agro-pdv-overlay-titles{flex:1;min-width:0;display:flex;flex-direction:column;gap:.1rem}' +
      '#agro-pdv-overlay-title-text{font-size:clamp(.78rem,1.4vw,.98rem);font-weight:900;text-transform:uppercase;letter-spacing:.04em;color:#0f172a;line-height:1.15;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}' +
      '#agro-pdv-overlay-subtitle-text{font-size:clamp(.65rem,1.1vw,.78rem);font-weight:700;color:#047857;line-height:1.2;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}' +
      '.agro-pdv-overlay-actions{display:flex;align-items:center;gap:.45rem;flex-shrink:0}' +
      '#agro-pdv-overlay-menu{flex-shrink:0;min-height:2.65rem;padding:0 .85rem;border-radius:.75rem;border:2px solid #cbd5e1;background:#fff;color:#0f172a;font-size:.75rem;font-weight:900;text-transform:uppercase;letter-spacing:.04em;text-decoration:none;display:inline-flex;align-items:center;cursor:pointer;touch-action:manipulation}' +
      '#agro-pdv-overlay-menu:hover{background:#f8fafc;border-color:#94a3b8}' +
      '#agro-pdv-overlay-menu[hidden]{display:none!important}' +
      '#agro-pdv-overlay-close{flex-shrink:0;min-height:2.65rem;min-width:6.5rem;padding:0 1rem;border-radius:.75rem;border:2px solid #c2410c;background:linear-gradient(135deg,#fb923c,#ea580c);color:#fff;font-size:.78rem;font-weight:900;text-transform:uppercase;letter-spacing:.05em;cursor:pointer;touch-action:manipulation}' +
      '#agro-pdv-overlay-close:hover{background:linear-gradient(135deg,#fdba74,#f97316)}' +
      '#agro-pdv-overlay-frame{flex:1;min-height:0;width:100%;border:0;background:#fff}' +
      'html.agro-pdv-overlay-open,html.agro-pdv-overlay-open body{overflow:hidden!important}';
    document.head.appendChild(st);
  }

  function ensureRoot() {
    ensureStyles();
    var root = document.getElementById(ROOT_ID);
    if (root) return root;
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
      '<a href="#" id="agro-pdv-overlay-menu" hidden>← Menu</a>' +
      '<button type="button" id="agro-pdv-overlay-close">Fechar</button>' +
      '</div>' +
      '</header>' +
      '<iframe id="agro-pdv-overlay-frame" title="Consulta no balcão"></iframe>' +
      '</div>';
    document.body.appendChild(root);
    root.querySelector('#agro-pdv-overlay-close').addEventListener('click', close);
    root.querySelector('[data-agro-pdv-overlay-dismiss]').addEventListener('click', close);
    var menuBtn = root.querySelector('#agro-pdv-overlay-menu');
    if (menuBtn) {
      menuBtn.addEventListener('click', function (e) {
        e.preventDefault();
        var href = menuBtn.getAttribute('data-href') || menuBtn.href;
        if (!href || href === '#') return;
        open(href, 'Caixa', { force: true });
      });
    }
    return root;
  }

  function applyMeta(d) {
    var root = document.getElementById(ROOT_ID);
    if (!root || !d) return;
    var titleEl = root.querySelector('#agro-pdv-overlay-title-text');
    var subEl = root.querySelector('#agro-pdv-overlay-subtitle-text');
    var menuBtn = root.querySelector('#agro-pdv-overlay-menu');
    if (titleEl) titleEl.textContent = String(d.title || titleEl.textContent || '');
    if (subEl) {
      subEl.textContent = String(d.subtitle || '');
      subEl.style.display = d.subtitle ? '' : 'none';
    }
    if (menuBtn) {
      if (d.showMenu && d.menuHref) {
        menuBtn.hidden = false;
        menuBtn.setAttribute('data-href', String(d.menuHref));
      } else {
        menuBtn.hidden = true;
        menuBtn.removeAttribute('data-href');
      }
    }
  }

  function resetMeta(title) {
    applyMeta({ title: title || 'Consulta no balcão', subtitle: '', showMenu: false });
  }

  function open(rawUrl, title, options) {
    options = options || {};
    var href = overlayUrl(rawUrl);
    var root = ensureRoot();
    var frame = root.querySelector('#agro-pdv-overlay-frame');
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
  }

  function isOpen() {
    return openFlag;
  }

  function onKeydown(e) {
    if (!openFlag) return;
    var k = e.key || '';
    if (k === 'Escape' || k === 'F1') {
      e.preventDefault();
      e.stopPropagation();
      close();
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
        if (d.type === 'agro-pdv-overlay-meta') {
          applyMeta(d);
          return;
        }
        if (d.type === 'agro-open-inapp-tab' && d.href && openFlag) {
          open(d.href, d.title || titleFromUrl(d.href));
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
    overlayUrl: overlayUrl,
    titleFromUrl: titleFromUrl,
  };
})();
