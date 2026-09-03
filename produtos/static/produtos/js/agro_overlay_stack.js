/**
 * Empilha overlays/modais: a camada de baixo fica inativa enquanto a de cima está aberta.
 * No iframe do AgroPdvOverlay também esconde a barra verde (Fechar) do pai.
 */
(function () {
  'use strict';

  var STYLE_ID = 'agro-overlay-stack-styles-v2';
  var stack = [];

  function ensureStyles() {
    var old = document.getElementById('agro-overlay-stack-styles-v1');
    if (old && old.parentNode) old.parentNode.removeChild(old);
    if (document.getElementById(STYLE_ID)) return;
    var st = document.createElement('style');
    st.id = STYLE_ID;
    /* NÃO forçar position:relative em todo layer — isso sobrescreve Tailwind `fixed`
       (ex.: modal Reemitir NFC-e em /vendas/) e vira caixa branca enorme no fluxo. */
    st.textContent =
      '.agro-stack-need-pos{position:relative}' +
      '.agro-stack-layer.agro-stack-inactive{pointer-events:none!important}' +
      '.agro-stack-layer.agro-stack-inactive::after{' +
      'content:"";position:absolute;inset:0;z-index:2147483645;' +
      'background:rgba(15,23,42,.28);pointer-events:none;border-radius:inherit}' +
      '.agro-stack-layer.agro-stack-inactive [data-agro-stack-close],' +
      '.agro-stack-layer.agro-stack-inactive .agro-stack-close{' +
      'visibility:hidden!important;pointer-events:none!important}';
    document.head.appendChild(st);
  }

  function needsContainingBlock(el) {
    try {
      var p = window.getComputedStyle(el).position;
      return !p || p === 'static';
    } catch (_) {
      return true;
    }
  }

  function syncParentChrome() {
    try {
      if (!window.top || window.top === window) return;
      window.top.postMessage(
        {
          type: 'agro-pdv-overlay-meta',
          hideChrome: stack.length > 0,
        },
        window.location.origin
      );
    } catch (_) {}
  }

  function refresh() {
    ensureStyles();
    var i;
    for (i = 0; i < stack.length; i += 1) {
      var el = stack[i];
      if (!el || !el.classList) continue;
      el.classList.add('agro-stack-layer');
      if (i < stack.length - 1) el.classList.add('agro-stack-inactive');
      else el.classList.remove('agro-stack-inactive');
    }
    syncParentChrome();
  }

  function indexOf(el) {
    var i;
    for (i = 0; i < stack.length; i += 1) {
      if (stack[i] === el) return i;
    }
    return -1;
  }

  function push(el) {
    if (!el) return;
    ensureStyles();
    var idx = indexOf(el);
    if (idx >= 0) {
      stack.splice(idx, 1);
    }
    stack.push(el);
    el.classList.add('agro-stack-layer');
    if (needsContainingBlock(el)) el.classList.add('agro-stack-need-pos');
    else el.classList.remove('agro-stack-need-pos');
    refresh();
  }

  function pop(el) {
    if (!el) return;
    var idx = indexOf(el);
    if (idx < 0) {
      el.classList.remove('agro-stack-inactive');
      el.classList.remove('agro-stack-layer');
      el.classList.remove('agro-stack-need-pos');
      return;
    }
    stack.splice(idx, 1);
    el.classList.remove('agro-stack-inactive');
    el.classList.remove('agro-stack-layer');
    el.classList.remove('agro-stack-need-pos');
    refresh();
  }

  function setOpen(el, open) {
    if (open) push(el);
    else pop(el);
  }

  /** Token virtual só para esconder chrome do pai (ex.: dialog nativo / fluxo sem nó fixo). */
  function setNested(on, token) {
    var key = token || '__agro_nested__';
    var fake = document.getElementById('agro-stack-token-' + key);
    if (on) {
      if (!fake) {
        fake = document.createElement('div');
        fake.id = 'agro-stack-token-' + key;
        fake.setAttribute('hidden', '');
        fake.style.display = 'none';
        document.documentElement.appendChild(fake);
      }
      push(fake);
    } else if (fake) {
      pop(fake);
    } else {
      syncParentChrome();
    }
  }

  function depth() {
    return stack.length;
  }

  function isLayerOpen(el) {
    if (!el) return false;
    if (el.tagName === 'DIALOG') return !!el.open;
    if (el.classList.contains('hidden')) return false;
    if (el.hasAttribute('hidden')) return false;
    if (el.classList.contains('is-open')) return true;
    if (el.classList.contains('flex')) return true;
    var st = window.getComputedStyle ? window.getComputedStyle(el) : null;
    if (st && (st.display === 'none' || st.visibility === 'hidden')) return false;
    return true;
  }

  function autoWire(el) {
    if (!el || el.getAttribute('data-agro-stack-wired') === '1') return;
    el.setAttribute('data-agro-stack-wired', '1');
    var sync = function () {
      var open = isLayerOpen(el);
      var onStack = indexOf(el) >= 0;
      if (open === onStack) return;
      setOpen(el, open);
    };
    try {
      new MutationObserver(sync).observe(el, {
        attributes: true,
        attributeFilter: ['class', 'open', 'hidden', 'aria-hidden'],
      });
    } catch (_) {}
    sync();
  }

  function autoWireAll(root) {
    var scope = root || document;
    try {
      scope.querySelectorAll('[data-agro-stack="auto"]').forEach(autoWire);
    } catch (_) {}
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () {
      autoWireAll(document);
    });
  } else {
    autoWireAll(document);
  }

  window.AgroOverlayStack = {
    push: push,
    pop: pop,
    setOpen: setOpen,
    setNested: setNested,
    depth: depth,
    refresh: refresh,
    autoWire: autoWire,
    autoWireAll: autoWireAll,
  };
})();
