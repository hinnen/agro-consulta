/**
 * Modal Configuração — F11 · efeitos leves PDV / Gestão.
 */
(function (global) {
  'use strict';

  var ROOT_ID = 'agro-perf-config-modal';
  var STYLE_ID = 'agro-perf-config-styles';

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
      '{position:fixed;inset:0;z-index:2147482000;display:flex;align-items:center;justify-content:center;padding:1rem;box-sizing:border-box}' +
      '.agro-perf-config-backdrop{position:absolute;inset:0;background:rgba(15,23,42,.55)}' +
      '.agro-perf-config-panel{position:relative;z-index:1;width:min(28rem,100%);max-height:min(90vh,100%);overflow:auto;border-radius:1rem;border:3px solid #10b981;background:#fff;box-shadow:0 24px 64px rgba(15,23,42,.28);padding:1.1rem 1.25rem}' +
      '.agro-perf-config-title{margin:0 0 .35rem;font-size:1.05rem;font-weight:900;text-transform:uppercase;color:#0f172a}' +
      '.agro-perf-config-sub{margin:0 0 1rem;font-size:.78rem;font-weight:600;color:#64748b;line-height:1.35}' +
      '.agro-perf-config-row{display:flex;align-items:center;justify-content:space-between;gap:.75rem;padding:.85rem 0;border-top:2px solid #f1f5f9}' +
      '.agro-perf-config-row:first-of-type{border-top:0}' +
      '.agro-perf-config-row label{font-size:.82rem;font-weight:800;color:#0f172a;line-height:1.3;flex:1}' +
      '.agro-perf-config-row small{display:block;font-size:.7rem;font-weight:600;color:#64748b;margin-top:.2rem}' +
      '.agro-perf-config-toggle{min-width:3.4rem;min-height:2.4rem;border-radius:.75rem;border:2px solid #cbd5e1;background:#f8fafc;font-size:.72rem;font-weight:900;text-transform:uppercase;cursor:pointer}' +
      '.agro-perf-config-toggle[aria-pressed="true"]{border-color:#059669;background:#ecfdf5;color:#047857}' +
      '.agro-perf-config-links{margin-top:.85rem;padding-top:.85rem;border-top:2px solid #f1f5f9;display:flex;flex-direction:column;gap:.5rem}' +
      '.agro-perf-config-link{display:flex;align-items:center;justify-content:space-between;gap:.5rem;min-height:2.75rem;padding:.55rem .75rem;border-radius:.75rem;border:2px solid #a7f3d0;background:#ecfdf5;color:#065f46;font-size:.78rem;font-weight:900;text-transform:uppercase;text-decoration:none}' +
      '.agro-perf-config-link:hover{background:#d1fae5;border-color:#34d399}' +
      '.agro-perf-config-link small{display:block;font-size:.65rem;font-weight:600;text-transform:none;color:#047857;margin-top:.15rem}' +
      '#agro-perf-config-close{margin-top:1rem;width:100%;min-height:2.75rem;border-radius:.75rem;border:2px solid #047857;background:linear-gradient(135deg,#10b981,#059669);color:#fff;font-size:.8rem;font-weight:900;text-transform:uppercase;cursor:pointer}';
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
    root.setAttribute('aria-labelledby', 'agro-perf-config-title');
    root.innerHTML =
      '<div class="agro-perf-config-backdrop" data-agro-perf-config-dismiss></div>' +
      '<div class="agro-perf-config-panel">' +
      '<h2 id="agro-perf-config-title" class="agro-perf-config-title">Configuração</h2>' +
      '<p class="agro-perf-config-sub">Deixe o PC mais leve sem atrapalhar venda. Atalho <kbd>F11</kbd>.</p>' +
      '<div class="agro-perf-config-row" id="agro-perf-config-row-pdv">' +
      '<label for="agro-perf-config-pdv">Menos animações no <strong>PDV</strong><small>Pulsos decorativos no balcão</small></label>' +
      '<button type="button" id="agro-perf-config-pdv" class="agro-perf-config-toggle" aria-pressed="false">Off</button>' +
      '</div>' +
      '<div class="agro-perf-config-row" id="agro-perf-config-row-gestao">' +
      '<label for="agro-perf-config-gestao">Menos animações na <strong>Gestão</strong><small>BI, cards pulsando, etc.</small></label>' +
      '<button type="button" id="agro-perf-config-gestao" class="agro-perf-config-toggle" aria-pressed="false">Off</button>' +
      '</div>' +
      '<div class="agro-perf-config-links">' +
      '<a class="agro-perf-config-link" href="/configuracao/planos-conta/">' +
      '<span>Planos de contas<small>Cadastrar / editar planos SisVale</small></span><span aria-hidden="true">›</span>' +
      '</a>' +
      '</div>' +
      '<button type="button" id="agro-perf-config-close">Fechar</button>' +
      '</div>';
    document.body.appendChild(root);
    root.querySelector('[data-agro-perf-config-dismiss]').addEventListener('click', close);
    root.querySelector('#agro-perf-config-close').addEventListener('click', close);
    root.querySelector('#agro-perf-config-pdv').addEventListener('click', function () {
      toggleScope('pdv', this);
    });
    root.querySelector('#agro-perf-config-gestao').addEventListener('click', function () {
      toggleScope('gestao', this);
    });
    return root;
  }

  function syncToggles(highlight) {
    var fx = global.agroPerfFx;
    if (!fx) return;
    var pdvBtn = document.getElementById('agro-perf-config-pdv');
    var gesBtn = document.getElementById('agro-perf-config-gestao');
    var pdvRow = document.getElementById('agro-perf-config-row-pdv');
    var gesRow = document.getElementById('agro-perf-config-row-gestao');
    if (pdvBtn) {
      var pdvOn = fx.read('pdv');
      pdvBtn.setAttribute('aria-pressed', pdvOn ? 'true' : 'false');
      pdvBtn.textContent = pdvOn ? 'On' : 'Off';
    }
    if (gesBtn) {
      var gesOn = fx.read('gestao');
      gesBtn.setAttribute('aria-pressed', gesOn ? 'true' : 'false');
      gesBtn.textContent = gesOn ? 'On' : 'Off';
    }
    if (pdvRow) pdvRow.style.opacity = highlight === 'gestao' ? '0.55' : '1';
    if (gesRow) gesRow.style.opacity = highlight === 'pdv' ? '0.55' : '1';
  }

  function toggleScope(scope, btn) {
    var fx = global.agroPerfFx;
    if (!fx) return;
    var next = !fx.read(scope);
    fx.set(scope, next);
    btn.setAttribute('aria-pressed', next ? 'true' : 'false');
    btn.textContent = next ? 'On' : 'Off';
  }

  function open(highlightScope) {
    var root = ensureRoot();
    syncToggles(highlightScope || '');
    root.removeAttribute('hidden');
    document.documentElement.classList.add('agro-perf-config-open');
    try {
      root.querySelector('#agro-perf-config-close').focus();
    } catch (_) {}
  }

  function close() {
    var root = document.getElementById(ROOT_ID);
    if (!root) return;
    root.setAttribute('hidden', '');
    document.documentElement.classList.remove('agro-perf-config-open');
  }

  function isOpen() {
    var root = document.getElementById(ROOT_ID);
    return !!(root && !root.hasAttribute('hidden'));
  }

  global.agroPerfConfig = { open: open, close: close, isOpen: isOpen };

  global.addEventListener('keydown', function (e) {
    if (e.key === 'Escape' && isOpen()) {
      e.preventDefault();
      close();
      return;
    }
    if (e.code !== 'F11' || e.altKey || e.ctrlKey || e.metaKey) return;
    var tag = e.target && e.target.tagName && String(e.target.tagName).toUpperCase();
    if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return;
    if (e.target && e.target.isContentEditable) return;
    e.preventDefault();
    var scope = '';
    try {
      if (global.agroPerfFx && global.agroPerfFx.isPdvContext && global.agroPerfFx.isPdvContext()) scope = 'pdv';
      else if (global.agroPerfFx && global.agroPerfFx.isGestaoContext && global.agroPerfFx.isGestaoContext()) scope = 'gestao';
    } catch (_) {}
    if (isOpen()) close();
    else open(scope);
  });
})(typeof window !== 'undefined' ? window : this);
