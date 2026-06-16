/**
 * Escala global (zoom no documento) — calibração automática: maior % sem quebrar layout.
 */
(function (global) {
  'use strict';

  var STORAGE_BASE = 'agro_display_scale_v1';
  var CONFIGURED_BASE = 'agro_display_scale_configured_v1';
  var MIN = 0.7;
  var MAX = 1.5;
  var STEP = 0.05;
  var DEFAULT = 1;
  var ROOT_ID = 'agro-display-scale-root';

  var modalOpen = false;
  var draftScale = DEFAULT;
  var maxSafeScale = MAX;
  var calibrating = false;

  function clamp(n) {
    var v = Math.round(n / STEP) * STEP;
    return Math.max(MIN, Math.min(MAX, v));
  }

  function estimateOsScale() {
    try {
      var iw = global.innerWidth || 0;
      var sw = global.screen && global.screen.width ? global.screen.width : 0;
      if (iw > 100 && sw > iw * 1.05) {
        return Math.min(3, Math.max(1, sw / iw));
      }
    } catch (e) {}
    var dpr = global.devicePixelRatio || 1;
    return dpr > 0 ? dpr : 1;
  }

  function calibrationCeiling() {
    var os = estimateOsScale();
    var iw = global.innerWidth || 0;
    var ceiling = MAX;
    if (os >= 1.625) ceiling = 1;
    else if (os >= 1.375) ceiling = 1.05;
    else if (os >= 1.2) ceiling = 1.15;
    if (iw > 0 && iw < 1200) ceiling = Math.min(ceiling, 0.95);
    if (iw > 0 && iw < 1050) ceiling = Math.min(ceiling, 0.85);
    return clamp(ceiling);
  }

  function profileSuffix() {
    try {
      var w = Math.round(global.innerWidth || 0);
      var h = Math.round(global.innerHeight || 0);
      var os = Math.round(estimateOsScale() * 100);
      return '_' + w + 'x' + h + '_os' + os;
    } catch (e) {
      return '_default';
    }
  }

  function storageKey() {
    return STORAGE_BASE + profileSuffix();
  }

  function configuredKey() {
    return CONFIGURED_BASE + profileSuffix();
  }

  function displayProfileLabel() {
    try {
      var w = Math.round(global.innerWidth || 0);
      var h = Math.round(global.innerHeight || 0);
      var os = Math.round(estimateOsScale() * 100);
      return (
        'Tela ' +
        w +
        '×' +
        h +
        ' · Windows ~' +
        os +
        '% · Chrome em 100%'
      );
    } catch (e2) {
      return '';
    }
  }

  function migrateLegacyStorage() {
    try {
      if (global.localStorage.getItem(storageKey()) == null && global.localStorage.getItem(STORAGE_BASE) != null) {
        global.localStorage.setItem(storageKey(), global.localStorage.getItem(STORAGE_BASE));
      }
      if (global.localStorage.getItem(configuredKey()) == null && global.localStorage.getItem(CONFIGURED_BASE) != null) {
        global.localStorage.setItem(configuredKey(), global.localStorage.getItem(CONFIGURED_BASE));
      }
    } catch (e3) {}
  }

  function read() {
    migrateLegacyStorage();
    try {
      var s = global.localStorage.getItem(storageKey());
      var n = parseFloat(s, 10);
      if (isFinite(n) && n >= MIN && n <= MAX) return n;
    } catch (e) {}
    return DEFAULT;
  }

  function isConfigured() {
    migrateLegacyStorage();
    try {
      return global.localStorage.getItem(configuredKey()) === '1';
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
    scale = clamp(Math.min(scale, maxSafeScale));
    try {
      global.localStorage.setItem(storageKey(), String(scale));
      global.localStorage.setItem(configuredKey(), '1');
    } catch (e) {}
    applyToRoot(scale);
    return scale;
  }

  function pctLabel(scale) {
    return Math.round(scale * 100) + '%';
  }

  function afterLayout(fn) {
    requestAnimationFrame(function () {
      requestAnimationFrame(fn);
    });
  }

  function isVisible(el) {
    if (!el || !el.getBoundingClientRect) return false;
    var st = window.getComputedStyle(el);
    if (st.display === 'none' || st.visibility === 'hidden' || st.opacity === '0') return false;
    var r = el.getBoundingClientRect();
    return r.width > 0 && r.height > 0;
  }

  function rectsOverlap(a, b, gap) {
    gap = gap || 0;
    return a.left < b.right - gap && a.right > b.left + gap && a.top < b.bottom - gap && a.bottom > b.top + gap;
  }

  function detectDashTopbarBreak() {
    var topbar = document.querySelector('.dash-topbar');
    if (!topbar || !isVisible(topbar)) return false;

    var tr = topbar.getBoundingClientRect();
    if (tr.height > 78) return true;

    var firstPeriod = topbar.querySelector('.period-btn');
    var periods = firstPeriod ? firstPeriod.parentElement : null;
    var syncBtn = topbar.querySelector('#btn-sync-dashboard');
    var sync = syncBtn ? syncBtn.parentElement : null;
    if (periods && sync && isVisible(periods) && isVisible(sync)) {
      var pr = periods.getBoundingClientRect();
      var sr = sync.getBoundingClientRect();
      if (rectsOverlap(pr, sr, 6)) return true;
      if (sr.left < pr.right - 2 && Math.abs(pr.top - sr.top) < 30) return true;
    }

    var blocks = topbar.querySelectorAll(':scope > div, :scope > button');
    var blockRects = [];
    for (var i = 0; i < blocks.length; i++) {
      if (!isVisible(blocks[i])) continue;
      blockRects.push(blocks[i].getBoundingClientRect());
    }
    for (var a = 0; a < blockRects.length; a++) {
      for (var c = a + 1; c < blockRects.length; c++) {
        if (Math.abs(blockRects[a].top - blockRects[c].top) < 22 && rectsOverlap(blockRects[a], blockRects[c], 4)) {
          return true;
        }
      }
    }

    var buttons = topbar.querySelectorAll('button');
    var br = [];
    for (var j = 0; j < buttons.length; j++) {
      if (!isVisible(buttons[j])) continue;
      br.push(buttons[j].getBoundingClientRect());
    }
    for (var x = 0; x < br.length; x++) {
      for (var y = x + 1; y < br.length; y++) {
        if (Math.abs(br[x].top - br[y].top) < 18 && rectsOverlap(br[x], br[y], 2)) return true;
      }
    }

    return false;
  }

  function detectPeriodButtonsWrap() {
    var container = document.querySelector('.dash-topbar-periods');
    if (!container || !isVisible(container)) return false;
    var btns = container.querySelectorAll('.period-btn');
    if (btns.length < 2) return false;
    var firstTop = btns[0].getBoundingClientRect().top;
    for (var i = 1; i < btns.length; i++) {
      if (Math.abs(btns[i].getBoundingClientRect().top - firstTop) > 8) return true;
    }
    return false;
  }

  function detectLayoutBreak() {
    var pad = 6;
    var vw = window.innerWidth;
    var doc = document.documentElement;

    if (doc.scrollWidth > vw + pad) return true;
    if (document.body && document.body.scrollWidth > vw + pad) return true;

    if (detectDashTopbarBreak()) return true;
    if (detectPeriodButtonsWrap()) return true;

    var cards = document.querySelectorAll('.glass-card, .kpi-card');
    for (var i = 0; i < cards.length; i++) {
      var el = cards[i];
      if (!isVisible(el)) continue;
      if (el.closest && el.closest('#' + ROOT_ID)) continue;
      var st = window.getComputedStyle(el);
      var r = el.getBoundingClientRect();
      if (r.top > window.innerHeight + 40 || r.bottom < -40) continue;
      if (r.right > vw + pad) return true;
      if (el.scrollWidth > el.clientWidth + 3 && (st.overflow === 'hidden' || st.overflowX === 'hidden')) return true;
    }

    var topbar = document.querySelector('.dash-topbar');
    if (topbar) {
      var tbBtns = topbar.querySelectorAll('button');
      for (var k = 0; k < tbBtns.length; k++) {
        var btn = tbBtns[k];
        if (!isVisible(btn)) continue;
        if (btn.scrollWidth > btn.clientWidth + 3) return true;
      }
    }

    return false;
  }

  function findMaxSafeScale(done) {
    if (calibrating) return;
    calibrating = true;
    var saved = read();
    var best = DEFAULT;
    var ceiling = clamp(calibrationCeiling());
    var scales = [];
    var s;
    for (s = ceiling; s >= MIN - 0.001; s -= STEP) scales.push(clamp(s));

    var idx = 0;

    function finish() {
      calibrating = false;
      applyToRoot(saved);
      maxSafeScale = Math.max(MIN, best);
      done(maxSafeScale);
    }

    function tryNext() {
      if (idx >= scales.length) {
        finish();
        return;
      }
      var scale = scales[idx++];
      applyToRoot(scale);
      afterLayout(function () {
        if (!detectLayoutBreak()) {
          best = scale;
          finish();
        } else {
          tryNext();
        }
      });
    }

    applyToRoot(MIN);
    afterLayout(tryNext);
  }

  function ensureFab() {
    var fab = document.getElementById('agro-display-scale-fab');
    if (fab) return fab;
    fab = document.createElement('button');
    fab.type = 'button';
    fab.id = 'agro-display-scale-fab';
    fab.className = 'agro-scale-trigger agro-scale-trigger--fab';
    fab.title = 'Ajustar tamanho da tela';
    fab.setAttribute('aria-label', 'Ajustar tamanho da tela');
    fab.textContent = 'Aa';
    fab.setAttribute('hidden', '');
    document.body.appendChild(fab);
    return fab;
  }

  function setFabVisible(show) {
    var fab = ensureFab();
    if (
      document.getElementById('home-link-display-scale') ||
      document.getElementById('dash-link-display-scale') ||
      document.getElementById('dash-link-display-scale-sm')
    ) {
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
    applyToRoot(read());
  }

  function syncControls(root, scale, opts) {
    opts = opts || {};
    var slider = root.querySelector('[data-agro-scale-slider]');
    var label = root.querySelector('[data-agro-scale-value]');
    var cap = root.querySelector('[data-agro-scale-cap]');
    var warn = root.querySelector('[data-agro-scale-warning]');
    var safeMax = opts.maxSafe != null ? opts.maxSafe : maxSafeScale;

    if (slider) {
      slider.max = String(safeMax);
      slider.value = String(scale);
    }
    if (label) label.textContent = pctLabel(scale);
    if (cap) cap.textContent = 'Máximo seguro neste monitor: ' + pctLabel(safeMax);

    var broken = scale > safeMax + 0.001;
    if (warn) {
      warn.hidden = !broken;
    }
    var confirmBtn = root.querySelector('[data-agro-scale-confirm]');
    if (confirmBtn) confirmBtn.disabled = broken;
  }

  function buildModal(opts) {
    opts = opts || {};
    var firstRun = !!opts.firstRun;
    var autoScale = opts.autoScale != null ? clamp(opts.autoScale) : read();
    draftScale = Math.min(autoScale, maxSafeScale);

    var root = document.createElement('div');
    root.id = ROOT_ID;
    root.className = 'agro-scale-overlay';
    root.setAttribute('role', 'dialog');
    root.setAttribute('aria-modal', 'true');
    root.setAttribute('aria-labelledby', 'agro-scale-title');
    var osPct = Math.round(estimateOsScale() * 100);
    root.innerHTML =
      '<div class="agro-scale-panel">' +
      '  <h2 id="agro-scale-title" class="agro-scale-title">Tamanho da tela</h2>' +
      '  <p class="agro-scale-lead">' +
      (firstRun
        ? 'Com <strong>Windows em ~' +
          osPct +
          '%</strong>, o ideal costuma ser <strong>100%</strong> (sem zoom extra). Calculamos o maior tamanho possível sem encavalar botões ou cards.'
        : 'Use <strong>Recalcular</strong> se mudou monitor ou escala do Windows. Com Windows alto, não passe de <strong>100%</strong>.') +
      ' Depois, o botão <strong>Aa</strong> reabre este ajuste.' +
      '</p>' +
      '  <p class="agro-scale-status" data-agro-scale-status>Tamanho ideal: <strong>' +
      pctLabel(draftScale) +
      '</strong></p>' +
      '  <p class="agro-scale-cap" data-agro-scale-cap>Máximo seguro neste monitor: ' +
      pctLabel(maxSafeScale) +
      '</p>' +
      '  <p class="agro-scale-profile" data-agro-scale-profile>Perfil: ' +
      displayProfileLabel() +
      '</p>' +
      '  <p class="agro-scale-warning" data-agro-scale-warning hidden>Acima do limite — cards ou botões podem quebrar. Diminua ou recalcule.</p>' +
      '  <div class="agro-scale-controls">' +
      '    <button type="button" class="agro-scale-step" data-agro-scale-minus aria-label="Diminuir">−</button>' +
      '    <input type="range" class="agro-scale-slider" data-agro-scale-slider min="' +
      MIN +
      '" max="' +
      maxSafeScale +
      '" step="' +
      STEP +
      '" value="' +
      draftScale +
      '">' +
      '    <button type="button" class="agro-scale-step" data-agro-scale-plus aria-label="Aumentar">+</button>' +
      '    <span class="agro-scale-value" data-agro-scale-value>' +
      pctLabel(draftScale) +
      '</span>' +
      '  </div>' +
      '  <div class="agro-scale-actions agro-scale-actions--stack">' +
      '    <button type="button" class="agro-scale-btn agro-scale-btn--muted" data-agro-scale-recalc>Recalcular ideal</button>' +
      (firstRun
        ? ''
        : '<button type="button" class="agro-scale-btn agro-scale-btn--muted" data-agro-scale-cancel>Cancelar</button>') +
      '    <button type="button" class="agro-scale-btn agro-scale-btn--primary" data-agro-scale-confirm>Confirmar ' +
      pctLabel(draftScale) +
      '</button>' +
      '  </div>' +
      '</div>';

    document.body.appendChild(root);
    document.body.classList.add('agro-scale-modal-open');
    modalOpen = true;
    setFabVisible(false);
    applyToRoot(draftScale);
    syncControls(root, draftScale);

    function setDraft(scale) {
      draftScale = clamp(Math.min(scale, maxSafeScale));
      applyToRoot(draftScale);
      afterLayout(function () {
        syncControls(root, draftScale);
        var confirmBtn = root.querySelector('[data-agro-scale-confirm]');
        if (confirmBtn) confirmBtn.textContent = 'Confirmar ' + pctLabel(draftScale);
        var status = root.querySelector('[data-agro-scale-status]');
        if (status) status.innerHTML = 'Tamanho ideal: <strong>' + pctLabel(draftScale) + '</strong>';
      });
    }

    function runRecalc() {
      var panel = root.querySelector('.agro-scale-panel');
      var recalcBtn = root.querySelector('[data-agro-scale-recalc]');
      if (panel) panel.classList.add('agro-scale-panel--busy');
      if (recalcBtn) recalcBtn.disabled = true;
      findMaxSafeScale(function (best) {
        if (panel) panel.classList.remove('agro-scale-panel--busy');
        if (recalcBtn) recalcBtn.disabled = false;
        setDraft(best);
      });
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
    root.querySelector('[data-agro-scale-recalc]').addEventListener('click', runRecalc);

    root.querySelector('[data-agro-scale-confirm]').addEventListener('click', function () {
      save(draftScale);
      closeModal();
    });

    var cancelBtn = root.querySelector('[data-agro-scale-cancel]');
    if (cancelBtn) {
      cancelBtn.addEventListener('click', closeModal);
    }

    if (!firstRun) {
      root.addEventListener('click', function (ev) {
        if (ev.target === root) closeModal();
      });
      root.addEventListener('keydown', function (ev) {
        if (ev.key === 'Escape') closeModal();
      });
    }

    root.querySelector('.agro-scale-panel').addEventListener('click', function (ev) {
      ev.stopPropagation();
    });

    root.querySelector('[data-agro-scale-confirm]').focus();
  }

  function open(opts) {
    if (modalOpen || calibrating) return;
    opts = opts || {};
    if (opts.autoScale != null && opts.skipFind) {
      maxSafeScale = clamp(opts.autoScale);
      buildModal({ firstRun: !!opts.firstRun, autoScale: maxSafeScale });
      return;
    }
    if (opts.skipAuto) {
      maxSafeScale = MAX;
      buildModal({ firstRun: false, autoScale: read() });
      return;
    }
    findMaxSafeScale(function (best) {
      buildModal({
        firstRun: !!opts.firstRun,
        autoScale: opts.autoScale != null ? opts.autoScale : best,
      });
    });
  }

  function bindTriggers() {
    document
      .querySelectorAll(
        '#agro-display-scale-fab, #home-link-display-scale, #dash-link-display-scale, #dash-link-display-scale-sm, [data-agro-display-scale-open]'
      )
      .forEach(function (btn) {
        if (btn.__agroScaleBound) return;
        btn.__agroScaleBound = true;
        btn.addEventListener('click', function () {
          open({ firstRun: false });
        });
      });
  }

  function boot() {
    ensureFab();
    bindTriggers();
    setFabVisible(true);
    if (isConfigured()) {
      applyToRoot(read());
      return;
    }
    findMaxSafeScale(function (best) {
      open({ firstRun: true, autoScale: best, skipFind: true });
    });
  }

  global.AgroDisplayScale = {
    STORAGE_KEY: storageKey,
    CONFIGURED_KEY: configuredKey,
    MIN: MIN,
    MAX: MAX,
    STEP: STEP,
    DEFAULT: DEFAULT,
    get: read,
    isConfigured: isConfigured,
    apply: applyToRoot,
    save: save,
    open: open,
    findMaxSafeScale: findMaxSafeScale,
    detectLayoutBreak: detectLayoutBreak,
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
