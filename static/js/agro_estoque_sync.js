/**
 * Agro Consulta — barra de estoque: botão manual, horário da última leitura,
 * “termômetro” (verde → vermelho conforme o tempo sem atualizar).
 * Atualização automática por inatividade só se autoIdleRefresh: true + idleMs > 0.
 */
(function (global) {
  'use strict';

  var DEFAULT_STALE_MS = 10 * 60 * 1000;
  var DEBOUNCE_AUTO_MS = 400;
  var THERMO_TICK_MS = 15000;

  function fmtHorario(d) {
    if (!(d instanceof Date) || isNaN(d.getTime())) return '—';
    return d.toLocaleString('pt-BR', {
      day: '2-digit',
      month: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    });
  }

  function paintThermo(btn, label, staleMs) {
    if (!btn) return;
    staleMs = staleMs || DEFAULT_STALE_MS;
    var fil = btn.querySelector('.agro-termo-fil');
    if (!fil) return;
    var freshAt = 0;
    if (label && label.dataset && label.dataset.gmFreshAt) {
      freshAt = parseInt(label.dataset.gmFreshAt, 10) || 0;
    }
    var now = Date.now();
    var ratio = !freshAt ? 1 : Math.min(1, (now - freshAt) / staleMs);
    fil.style.height = (ratio * 100).toFixed(1) + '%';
    var h = 142 * (1 - ratio);
    var fill = 'hsl(' + h.toFixed(0) + ',72%,42%)';
    fil.style.background = fill;
    fil.style.boxShadow = '0 0 6px ' + fill;
    var border =
      ratio < 0.12
        ? 'rgba(16,185,129,0.55)'
        : ratio < 0.45
          ? 'rgba(234,179,8,0.7)'
          : 'rgba(239,68,68,0.78)';
    btn.style.borderColor = border;
  }

  function markLabelFresh(label, staleMs, btn) {
    var node = label;
    var b = btn || document.getElementById('agro-btn-atualizar-saldos');
    if (!node) return;
    node.textContent = fmtHorario(new Date());
    node.dataset.gmFreshAt = String(Date.now());
    paintThermo(b, node, staleMs);
  }

  function mount(opts) {
    var onRefresh = opts && opts.onRefresh;
    if (typeof onRefresh !== 'function') {
      console.warn('[AgroEstoqueSync] onRefresh é obrigatório');
      return null;
    }

    var staleMs = (opts && opts.staleMs) || DEFAULT_STALE_MS;
    var btn = (opts && opts.button) || document.getElementById('agro-btn-atualizar-saldos');
    var label = (opts && opts.label) || document.getElementById('agro-saldos-ultima-atualizacao');
    var busy = false;
    var debounceTimer = null;
    var idleTimer = null;
    var thermoTimer = null;

    var useIdle =
      opts &&
      opts.autoIdleRefresh === true &&
      typeof opts.idleMs === 'number' &&
      opts.idleMs > 0;

    function setLabelText(txt) {
      if (label) label.textContent = txt;
    }

    function setLabelFresh() {
      markLabelFresh(label, staleMs, btn);
    }

    function tickThermo() {
      paintThermo(btn, label, staleMs);
    }

    function exec(reason) {
      if (busy) return Promise.resolve();
      busy = true;
      if (btn) {
        btn.disabled = true;
        btn.setAttribute('aria-busy', 'true');
        btn.classList.add('opacity-60', 'pointer-events-none');
      }
      if (reason !== 'poll') setLabelText('Atualizando estoque…');
      return Promise.resolve()
        .then(function () {
          return onRefresh(reason || 'manual');
        })
        .then(function () {
          setLabelFresh();
        })
        .catch(function (e) {
          console.error('[AgroEstoqueSync]', e);
          setLabelText('Falha ao atualizar');
        })
        .finally(function () {
          busy = false;
          if (btn) {
            btn.disabled = false;
            btn.removeAttribute('aria-busy');
            btn.classList.remove('opacity-60', 'pointer-events-none');
          }
          tickThermo();
        });
    }

    function scheduleAuto(reason) {
      clearTimeout(debounceTimer);
      debounceTimer = setTimeout(function () {
        exec(reason);
      }, DEBOUNCE_AUTO_MS);
    }

    if (btn) {
      btn.addEventListener('click', function () {
        clearTimeout(debounceTimer);
        exec('manual');
      });
    }

    function bumpIdle() {
      if (!useIdle) return;
      if (idleTimer) clearTimeout(idleTimer);
      idleTimer = setTimeout(function () {
        scheduleAuto('idle');
      }, opts.idleMs);
    }

    if (useIdle) {
      ['mousemove', 'keydown', 'click', 'scroll', 'touchstart', 'wheel'].forEach(function (ev) {
        document.addEventListener(ev, bumpIdle, { passive: true });
      });
      bumpIdle();
    }

    thermoTimer = setInterval(tickThermo, THERMO_TICK_MS);
    tickThermo();

    global.agroEstoqueSyncRefresh = function (reason) {
      return exec(reason || 'external');
    };

    return {
      refresh: exec,
      markFresh: setLabelFresh,
      tickThermo: tickThermo,
    };
  }

  global.AgroEstoqueSync = {
    mount: mount,
    formatHorario: fmtHorario,
    staleMsDefault: DEFAULT_STALE_MS,
    paintThermo: paintThermo,
    markFresh: function (el, staleMs, btnOpt) {
      markLabelFresh(el, staleMs || DEFAULT_STALE_MS, btnOpt);
    },
  };
})(typeof window !== 'undefined' ? window : this);
