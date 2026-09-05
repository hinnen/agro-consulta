/**
 * Efeitos visuais decorativos — toggles separados PDV e Gestão (localStorage).
 */
(function (global) {
  'use strict';

  var PDV_KEY = 'agro_fx_reduced_pdv_v1';
  var GESTAO_KEY = 'agro_fx_reduced_gestao_v1';
  var LEGACY_KEY = 'agro_reduzir_efeitos_v1';

  function pathnameNorm() {
    return String(global.location.pathname || '')
      .toLowerCase()
      .replace(/\/+$/, '') || '/';
  }

  function isPdvContext() {
    var p = pathnameNorm();
    if (p === '/pdv' || p.indexOf('/pdv/') === 0 || p === '/consulta' || p.indexOf('/consulta/') === 0) {
      return true;
    }
    try {
      if (global.AgroDualWindow && typeof global.AgroDualWindow.isPdvHost === 'function' && global.AgroDualWindow.isPdvHost()) {
        return true;
      }
    } catch (_) {}
    return false;
  }

  function isGestaoContext() {
    if (isPdvContext()) return false;
    try {
      var q = new URLSearchParams(global.location.search || '');
      if (q.get('agro_app_role') === 'gestao') return true;
      if (global.localStorage.getItem('agro_app_role_v1') === 'gestao') return true;
      if (global.AgroDualWindow && typeof global.AgroDualWindow.isGestaoContext === 'function' && global.AgroDualWindow.isGestaoContext()) {
        return true;
      }
      if (document.documentElement.getAttribute('data-agro-hide-pdv') === '1') return true;
    } catch (_) {}
    var p = pathnameNorm();
    return p === '/' || p === '/atalhos' || p.indexOf('/atalhos/') === 0 || p.indexOf('/dashboard') === 0;
  }

  function migrateLegacy() {
    try {
      if (global.localStorage.getItem(LEGACY_KEY) !== '1') return;
      if (!global.localStorage.getItem(PDV_KEY)) global.localStorage.setItem(PDV_KEY, '1');
      if (!global.localStorage.getItem(GESTAO_KEY)) global.localStorage.setItem(GESTAO_KEY, '1');
    } catch (_) {}
  }

  function read(scope) {
    migrateLegacy();
    try {
      return global.localStorage.getItem(scope === 'pdv' ? PDV_KEY : GESTAO_KEY) === '1';
    } catch (_) {
      return false;
    }
  }

  function apply() {
    migrateLegacy();
    var reduced = false;
    try {
      if (isPdvContext() && read('pdv')) reduced = true;
      if (isGestaoContext() && read('gestao')) reduced = true;
    } catch (_) {}
    document.documentElement.classList.toggle('agro-fx-reduced', reduced);
  }

  function set(scope, on) {
    try {
      global.localStorage.setItem(scope === 'pdv' ? PDV_KEY : GESTAO_KEY, on ? '1' : '0');
    } catch (_) {}
    apply();
  }

  global.agroPerfFx = {
    read: read,
    set: set,
    apply: apply,
    isPdvContext: isPdvContext,
    isGestaoContext: isGestaoContext,
  };

  apply();
  global.addEventListener('storage', function (ev) {
    if (!ev || (ev.key !== PDV_KEY && ev.key !== GESTAO_KEY && ev.key !== LEGACY_KEY)) return;
    apply();
  });
})(typeof window !== 'undefined' ? window : this);
