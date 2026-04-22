(function (w) {
  'use strict';

  function getCookie(name) {
    var v = document.cookie.match('(^|;) ?' + name + '=([^;]*)(;|$)');
    return v ? decodeURIComponent(v[2]) : '';
  }

  function csrf() {
    var el = document.querySelector('[name=csrfmiddlewaretoken]');
    return el ? el.value : getCookie('csrftoken');
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
    setLoading: setLoading
  };
})(window);
