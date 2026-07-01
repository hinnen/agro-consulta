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
      '.agro-pdv-overlay-head{display:flex;align-items:center;gap:.75rem;flex-shrink:0;padding:.65rem .85rem;border-bottom:2px solid #e2e8f0;background:linear-gradient(180deg,#fff,#f1f5f9)}' +
      '.agro-pdv-overlay-title{flex:1;font-size:clamp(.85rem,1.6vw,1.05rem);font-weight:900;text-transform:uppercase;letter-spacing:.04em;color:#0f172a}' +
      '.agro-pdv-overlay-hint{font-size:.68rem;font-weight:700;color:#64748b;text-transform:none;letter-spacing:0}' +
      '#agro-pdv-overlay-close{flex-shrink:0;min-height:2.75rem;min-width:7.5rem;padding:0 1.1rem;border-radius:.85rem;border:2px solid #c2410c;background:linear-gradient(135deg,#fb923c,#ea580c);color:#fff;font-size:.82rem;font-weight:900;text-transform:uppercase;letter-spacing:.05em;cursor:pointer;touch-action:manipulation}' +
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
      '<div class="agro-pdv-overlay-title"><span id="agro-pdv-overlay-title-text"></span>' +
      '<span class="agro-pdv-overlay-hint"> · FECHAR ou F1 para voltar ao balcão</span></div>' +
      '<button type="button" id="agro-pdv-overlay-close">Fechar</button>' +
      '</header>' +
      '<iframe id="agro-pdv-overlay-frame" title="Consulta no balcão"></iframe>' +
      '</div>';
    document.body.appendChild(root);
    root.querySelector('#agro-pdv-overlay-close').addEventListener('click', close);
    root.querySelector('[data-agro-pdv-overlay-dismiss]').addEventListener('click', close);
    return root;
  }

  function open(rawUrl, title) {
    var href = overlayUrl(rawUrl);
    var root = ensureRoot();
    var frame = root.querySelector('#agro-pdv-overlay-frame');
    var titleEl = root.querySelector('#agro-pdv-overlay-title-text');
    if (titleEl) titleEl.textContent = String(title || titleFromUrl(href));
    if (frame && frame.getAttribute('src') !== href) frame.setAttribute('src', href);
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
