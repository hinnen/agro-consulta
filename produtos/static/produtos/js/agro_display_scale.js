/**
 * Escala global da interface (zoom do documento) — uma vez por aparelho/navegador.
 * window.AgroDisplayScale.open() reabre o ajuste; localStorage persiste entre telas.
 */
(function (global) {
  'use strict';

  var STORAGE = 'agro_display_scale_v1';
  var CONFIGURED = 'agro_display_scale_configured_v1';
  var MIN = 0.75;
  var MAX = 1.5;
  var STEP = 0.05;
  var DEFAULT = 1;
  var ROOT_ID = 'agro-display-scale-root';
  var modalOpen = false;
  var draftScale = DEFAULT;

  function clamp(n) {
    var v = Math.round(n / STEP) * STEP;
    return Math.max(MIN, Math.min(MAX, v));
  }

  function read() {
    try {
      var s = global.localStorage.getItem(STORAGE);
      var n = parseFloat(s, 10);
      if (isFinite(n) && n >= MIN && n <= MAX) return n;
    } catch (e) {}
    return DEFAULT;
  }

  function isConfigured() {
    try {
      return global.localStorage.getItem(CONFIGURED) === '1';
    } catch (e) {
      return false;
    }
  }

  function applyToRoot(scale) {
    var el = document.documentElement;
    scale = clamp(scale);
    if (!scale || scale === 1) {
      el.style.zoom = '';
      el.removeAttribute('data-agro-scale');
      el.style.removeProperty('--agro-display-scale');
    } else {
      var zs = String(Math.round(scale * 1000) / 1000);
      el.style.zoom = zs;
      el.setAttribute('data-agro-scale', zs);
      el.style.setProperty('--agro-display-scale', zs);
    }
  }

  function save(scale) {
    scale = clamp(scale);
    try {
      global.localStorage.setItem(STORAGE, String(scale));
      global.localStorage.setItem(CONFIGURED, '1');
    } catch (e) {}
    applyToRoot(scale);
    return scale;
  }

  function pctLabel(scale) {
    return Math.round(scale * 100) + '%';
  }

  function ensureFab() {
    var fab = document.getElementById('agro-display-scale-fab');
    if (fab) return fab;
    fab = document.createElement('button');
    fab.type = 'button';
    fab.id = 'agro-display-scale-fab';
    fab.className = 'agro-scale-trigger agro-scale-trigger--fab';
    fab.title = 'Ajustar tamanho da tela (textos e botões)';
    fab.setAttribute('aria-label', 'Ajustar tamanho da tela');
    fab.textContent = 'Aa';
    fab.setAttribute('hidden', '');
    document.body.appendChild(fab);
    return fab;
  }

  function setFabVisible(show) {
    var fab = ensureFab();
    if (document.getElementById('home-link-display-scale')) {
      fab.setAttribute('hidden', '');
      return;
    }
    if (show) fab.removeAttribute('hidden');
    else fab.setAttribute('hidden', '');
  }

  function closeModal() {
    var root = document.getElementById(ROOT_ID);
    if (root) root.remove();
    document.body.classList.remove('agro-scale-modal-open');
    modalOpen = false;
    setFabVisible(true);
  }

  function syncPreview(preview, scale) {
    if (!preview) return;
    if (scale === 1) {
      preview.style.zoom = '';
    } else {
      preview.style.zoom = String(scale);
    }
  }

  function syncControls(root, scale) {
    var slider = root.querySelector('[data-agro-scale-slider]');
    var label = root.querySelector('[data-agro-scale-value]');
    if (slider) slider.value = String(scale);
    if (label) label.textContent = pctLabel(scale);
    root.querySelectorAll('[data-agro-scale-preset]').forEach(function (btn) {
      var p = parseFloat(btn.getAttribute('data-agro-scale-preset'), 10);
      btn.classList.toggle('agro-scale-preset-active', Math.abs(p - scale) < 0.001);
    });
  }

  function buildModal(opts) {
    opts = opts || {};
    var firstRun = !!opts.firstRun;
    draftScale = read();

    var root = document.createElement('div');
    root.id = ROOT_ID;
    root.className = 'agro-scale-overlay';
    root.setAttribute('role', 'dialog');
    root.setAttribute('aria-modal', 'true');
    root.setAttribute('aria-labelledby', 'agro-scale-title');
    root.innerHTML =
      '<div class="agro-scale-panel">' +
      '  <h2 id="agro-scale-title" class="agro-scale-title">Ajustar tamanho da tela</h2>' +
      '  <p class="agro-scale-lead">' +
      (firstRun
        ? 'Configure <strong>uma vez neste computador</strong>. Textos e botões ficam no mesmo tamanho em todas as telas do sistema.'
        : 'Altere o tamanho geral. Vale para PDV, caixa, lançamentos e demais telas neste navegador.') +
      ' Para corrigir depois, use o botão <strong>Aa</strong> no canto superior da tela.' +
      '</p>' +
      '  <div class="agro-scale-preview-wrap">' +
      '    <p class="agro-scale-preview-label">Prévia — ajuste até ficar confortável de ler</p>' +
      '    <div class="agro-scale-preview" data-agro-scale-preview>' +
      '      <p class="agro-scale-sample-kicker">PDV · exemplo</p>' +
      '      <p class="agro-scale-sample-title">Consulta de produtos</p>' +
      '      <input type="text" class="agro-scale-sample-input" value="Buscar produto…" readonly tabindex="-1" aria-hidden="true">' +
      '      <div class="agro-scale-sample-row">' +
      '        <button type="button" class="agro-scale-sample-btn agro-scale-sample-btn--ghost" tabindex="-1">Cancelar</button>' +
      '        <button type="button" class="agro-scale-sample-btn agro-scale-sample-btn--primary" tabindex="-1">Confirmar venda</button>' +
      '      </div>' +
      '      <p class="agro-scale-sample-foot">Texto pequeno · R$ 128,50 · estoque 12 un.</p>' +
      '    </div>' +
      '  </div>' +
      '  <div class="agro-scale-controls">' +
      '    <button type="button" class="agro-scale-step" data-agro-scale-minus aria-label="Diminuir">−</button>' +
      '    <input type="range" class="agro-scale-slider" data-agro-scale-slider min="' +
      MIN +
      '" max="' +
      MAX +
      '" step="' +
      STEP +
      '" value="' +
      draftScale +
      '" aria-valuetext="' +
      pctLabel(draftScale) +
      '">' +
      '    <button type="button" class="agro-scale-step" data-agro-scale-plus aria-label="Aumentar">+</button>' +
      '    <span class="agro-scale-value" data-agro-scale-value>' +
      pctLabel(draftScale) +
      '</span>' +
      '  </div>' +
      '  <div class="agro-scale-presets">' +
      '    <button type="button" class="agro-scale-preset" data-agro-scale-preset="0.9">90%</button>' +
      '    <button type="button" class="agro-scale-preset" data-agro-scale-preset="1">Padrão</button>' +
      '    <button type="button" class="agro-scale-preset" data-agro-scale-preset="1.15">Grande</button>' +
      '    <button type="button" class="agro-scale-preset" data-agro-scale-preset="1.25">Maior</button>' +
      '  </div>' +
      '  <div class="agro-scale-actions">' +
      (firstRun
        ? '<button type="button" class="agro-scale-btn agro-scale-btn--muted" data-agro-scale-default>Usar padrão (100%)</button>'
        : '<button type="button" class="agro-scale-btn agro-scale-btn--muted" data-agro-scale-cancel>Cancelar</button>') +
      '    <button type="button" class="agro-scale-btn agro-scale-btn--primary" data-agro-scale-confirm>Confirmar</button>' +
      '  </div>' +
      '</div>';

    document.body.appendChild(root);
    document.body.classList.add('agro-scale-modal-open');
    modalOpen = true;
    setFabVisible(false);

    var preview = root.querySelector('[data-agro-scale-preview]');
    syncPreview(preview, draftScale);
    syncControls(root, draftScale);

    function setDraft(scale) {
      draftScale = clamp(scale);
      syncPreview(preview, draftScale);
      syncControls(root, draftScale);
      var slider = root.querySelector('[data-agro-scale-slider]');
      if (slider) slider.setAttribute('aria-valuetext', pctLabel(draftScale));
    }

    root.querySelector('[data-agro-scale-minus]').addEventListener('click', function () {
      setDraft(draftScale - STEP);
    });
    root.querySelector('[data-agro-scale-plus]').addEventListener('click', function () {
      setDraft(draftScale + STEP);
    });
    root.querySelector('[data-agro-scale-slider]').addEventListener('input', function (ev) {
      setDraft(parseFloat(ev.target.value, 10));
    });
    root.querySelectorAll('[data-agro-scale-preset]').forEach(function (btn) {
      btn.addEventListener('click', function () {
        setDraft(parseFloat(btn.getAttribute('data-agro-scale-preset'), 10));
      });
    });

    root.querySelector('[data-agro-scale-confirm]').addEventListener('click', function () {
      save(draftScale);
      closeModal();
    });

    var defaultBtn = root.querySelector('[data-agro-scale-default]');
    if (defaultBtn) {
      defaultBtn.addEventListener('click', function () {
        save(DEFAULT);
        closeModal();
      });
    }

    var cancelBtn = root.querySelector('[data-agro-scale-cancel]');
    if (cancelBtn) {
      cancelBtn.addEventListener('click', function () {
        applyToRoot(read());
        closeModal();
      });
    }

    if (!firstRun) {
      root.addEventListener('click', function (ev) {
        if (ev.target === root) {
          applyToRoot(read());
          closeModal();
        }
      });
      root.addEventListener('keydown', function (ev) {
        if (ev.key === 'Escape') {
          applyToRoot(read());
          closeModal();
        }
      });
    }

    root.querySelector('.agro-scale-panel').addEventListener('click', function (ev) {
      ev.stopPropagation();
    });

    var confirmBtn = root.querySelector('[data-agro-scale-confirm]');
    if (confirmBtn) confirmBtn.focus();
  }

  function open(opts) {
    if (modalOpen) return;
    buildModal(opts || {});
  }

  function bindTriggers() {
    document.querySelectorAll('#agro-display-scale-fab, #home-link-display-scale, [data-agro-display-scale-open]').forEach(function (btn) {
      if (btn.__agroScaleBound) return;
      btn.__agroScaleBound = true;
      btn.addEventListener('click', function () {
        open();
      });
    });
  }

  function boot() {
    ensureFab();
    bindTriggers();
    setFabVisible(true);
    if (isConfigured()) {
      applyToRoot(read());
    } else {
      open({ firstRun: true });
    }
  }

  global.AgroDisplayScale = {
    STORAGE_KEY: STORAGE,
    CONFIGURED_KEY: CONFIGURED,
    MIN: MIN,
    MAX: MAX,
    STEP: STEP,
    DEFAULT: DEFAULT,
    get: read,
    isConfigured: isConfigured,
    apply: applyToRoot,
    save: save,
    open: open,
    applyEarly: function () {
      if (isConfigured()) applyToRoot(read());
    },
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot);
  } else {
    boot();
  }
})(window);
