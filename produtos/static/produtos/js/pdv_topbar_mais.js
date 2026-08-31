/**
 * PDV topbar — menu Mais ⋯ + contagem de cliques (Postgres).
 */
(function () {
  'use strict';

  var URL_CLIQUE = '/api/pdv/topbar-clique/';
  var root = document.getElementById('pdv-topbar-compact');
  var mais = document.getElementById('pdv-topbar-mais');
  if (!root) return;

  function csrfToken() {
    try {
      var boot = document.getElementById('pdv-bootstrap');
      if (boot) {
        var j = JSON.parse(boot.textContent || '{}');
        if (j && j.csrfToken) return j.csrfToken;
      }
    } catch (e) {}
    var inp = document.querySelector('input[name=csrfmiddlewaretoken]');
    if (inp && inp.value) return inp.value;
    var m = document.cookie.match(/(?:^|;\s*)csrftoken=([^;]+)/);
    return m ? decodeURIComponent(m[1]) : '';
  }

  function depositoAtual() {
    try {
      var boot = document.getElementById('pdv-bootstrap');
      if (boot) {
        var j = JSON.parse(boot.textContent || '{}');
        if (j && j.deposito) return String(j.deposito);
      }
    } catch (e) {}
    var badge = document.getElementById('pdv-deposito-badge');
    var t = (badge && badge.textContent) || '';
    if (/vila/i.test(t)) return 'vila';
    if (/centro/i.test(t)) return 'centro';
    return '';
  }

  var fila = [];
  var enviando = false;

  function flushCliques() {
    if (enviando || !fila.length) return;
    enviando = true;
    var item = fila.shift();
    fetch(URL_CLIQUE, {
      method: 'POST',
      credentials: 'same-origin',
      headers: {
        'Content-Type': 'application/json',
        'X-CSRFToken': csrfToken(),
      },
      body: JSON.stringify({ botao: item, deposito: depositoAtual() }),
    })
      .catch(function () {})
      .finally(function () {
        enviando = false;
        if (fila.length) flushCliques();
      });
  }

  function registrarClique(key) {
    if (!key) return;
    fila.push(String(key));
    flushCliques();
  }

  function fecharMais() {
    if (mais && mais.open) mais.open = false;
    var saldo = document.getElementById('pdv-estoque-vila-menu');
    if (saldo && saldo.open) saldo.open = false;
  }

  root.addEventListener(
    'click',
    function (ev) {
      var keyEl = ev.target && ev.target.closest ? ev.target.closest('[data-pdv-topbar-key]') : null;
      if (keyEl) {
        registrarClique(keyEl.getAttribute('data-pdv-topbar-key'));
      }
      if (!mais || !mais.open) return;
      if (ev.target.closest('#pdv-topbar-mais > summary')) return;
      if (ev.target.closest('#pdv-estoque-vila-menu > summary')) return;
      if (
        mais.contains(ev.target) &&
        ev.target.closest('a, button, .pdv-estoque-vila-link')
      ) {
        window.setTimeout(fecharMais, 0);
      }
    },
    true
  );

  document.addEventListener('click', function (ev) {
    if (!mais || !mais.open) return;
    if (mais.contains(ev.target)) return;
    fecharMais();
  });

  document.addEventListener('keydown', function (ev) {
    if (ev.key === 'Escape' && mais && mais.open) {
      fecharMais();
    }
  });
})();
