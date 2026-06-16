(function (w) {
  'use strict';

  function getCookie(name) {
    var v = document.cookie.match('(^|;) ?' + name + '=([^;]*)(;|$)');
    return v ? decodeURIComponent(v[2]) : '';
  }

  function csrf() {
    var lista = w.AgroCadastroErpLista;
    if (lista && lista.CSRF_TOKEN) return lista.CSRF_TOKEN;
    var meta = document.querySelector('meta[name="csrfmiddlewaretoken"]');
    if (meta && meta.getAttribute('content')) return meta.getAttribute('content');
    var ck = document.cookie.match(/(?:^|; )csrftoken=([^;]*)/);
    if (ck) return decodeURIComponent(ck[1]);
    var el = document.querySelector('[name=csrfmiddlewaretoken]');
    if (el && el.value) return el.value;
    return '';
  }

  function escapeHtml(s) {
    return String(s || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function fmtMoney(n) {
    var x = Number(n);
    if (!isFinite(x)) x = 0;
    return x.toLocaleString('pt-BR', {
      style: 'currency',
      currency: 'BRL',
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    });
  }

  function resetLoading() {
    if (w.gmLoadingBar && typeof w.gmLoadingBar.reset === 'function') {
      w.gmLoadingBar.reset();
    }
  }

  function setLoading(on) {
    if (w.gmLoadingBar) {
      if (on) w.gmLoadingBar.show();
      else w.gmLoadingBar.hide();
    }
  }

  w.AgroCadastroErpUtil = {
    getCookie: getCookie,
    csrf: csrf,
    escapeHtml: escapeHtml,
    fmtMoney: fmtMoney,
    resetLoading: resetLoading,
    setLoading: setLoading
  };
})(window);
