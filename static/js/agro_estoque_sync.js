/**
 * Agro Consulta — barra de estoque: botão manual, horário da última atualização,
 * e atualização automática em standby (janela em segundo plano, Alt+Tab, ou inatividade).
 *
 * Cada página chama AgroEstoqueSync.mount({ onRefresh, ... }) após definir onRefresh.
 *
 * Tempos típicos (padrão):
 * - Botão manual: imediato (+ latência de rede).
 * - Auto em standby: após idleMs (padrão 45s) sem mouse/teclado/scroll + debounce 400ms.
 * - Não dispara ao Alt+Tab / blur / aba oculta (evita “reload” visual ao voltar).
 */
(function (global) {
  'use strict';

  var DEFAULT_IDLE_MS = 45000;
  var DEBOUNCE_AUTO_MS = 400;

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

  function mount(opts) {
    var onRefresh = opts && opts.onRefresh;
    if (typeof onRefresh !== 'function') {
      console.warn('[AgroEstoqueSync] onRefresh é obrigatório');
      return null;
    }

    var idleMs = (opts && opts.idleMs) || DEFAULT_IDLE_MS;
    var btn = (opts && opts.button) || document.getElementById('agro-btn-atualizar-saldos');
    var label = (opts && opts.label) || document.getElementById('agro-saldos-ultima-atualizacao');
    var busy = false;
    var debounceTimer = null;
    var idleTimer = null;

    function setLabelText(txt) {
      if (label) label.textContent = txt;
    }

    function setLabelFresh() {
      setLabelText(fmtHorario(new Date()));
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
      if (idleTimer) clearTimeout(idleTimer);
      idleTimer = setTimeout(function () {
        scheduleAuto('idle');
      }, idleMs);
    }

    ['mousemove', 'keydown', 'click', 'scroll', 'touchstart', 'wheel'].forEach(function (ev) {
      document.addEventListener(ev, bumpIdle, { passive: true });
    });
    bumpIdle();

    global.agroEstoqueSyncRefresh = function (reason) {
      return exec(reason || 'external');
    };

    global.AgroEstoqueSync = global.AgroEstoqueSync || {};
    global.AgroEstoqueSync.markFresh = function (el) {
      var node = el || label;
      if (node) node.textContent = fmtHorario(new Date());
    };

    return {
      refresh: exec,
      markFresh: function () {
        setLabelFresh();
      },
    };
  }

  global.AgroEstoqueSync = {
    mount: mount,
    formatHorario: fmtHorario,
    markFresh: function (el) {
      if (el) el.textContent = fmtHorario(new Date());
    },
  };
})(typeof window !== 'undefined' ? window : this);
