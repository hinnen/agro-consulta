/**
 * Pausa animações/gráficos quando a aba está em repouso (iframe ou mensagem do shell).
 */
(function (global) {
  'use strict';

  var forced = false;

  function setPaused(on) {
    forced = !!on;
    document.documentElement.classList.toggle('agro-tab-economy-paused', forced);
    if (forced) {
      if (typeof global.agroDashPauseCharts === 'function') global.agroDashPauseCharts();
    } else if (typeof global.agroDashResumeCharts === 'function') {
      global.agroDashResumeCharts();
    }
  }

  global.addEventListener('message', function (ev) {
    try {
      if (!ev || ev.origin !== global.location.origin) return;
      var d = ev.data || {};
      if (d.type !== 'agro-tab-economy') return;
      if (d.mode === 'pause') setPaused(true);
      if (d.mode === 'resume') setPaused(false);
    } catch (_) {}
  });

  /** Só iframes embutidos — app PDV/Gestão em janela própria não pausa ao Alt+Tab. */
  if (global.top !== global.self) {
    document.addEventListener('visibilitychange', function () {
      if (document.hidden) setPaused(true);
      else setPaused(false);
    });
  }
})(typeof window !== 'undefined' ? window : this);
