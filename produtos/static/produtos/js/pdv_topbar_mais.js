/**
 * PDV topbar — menu Mais ⋯ + contagem de cliques (Postgres).
 * Botão + painel (não <details>) — evita fechar no mesmo clique e overflow clip.
 */
(function () {
  'use strict';

  var URL_CLIQUE = '/api/pdv/topbar-clique/';
  var root = document.getElementById('pdv-topbar-compact');
  var wrap = document.getElementById('pdv-topbar-mais');
  var btn = document.getElementById('pdv-topbar-mais-btn');
  var panel = document.getElementById('pdv-topbar-mais-panel');
  if (!root || !wrap || !btn || !panel) return;

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

  function maisAberto() {
    return !panel.classList.contains('hidden');
  }

  var panelHost = panel.parentNode;
  var panelNoBody = false;

  function posicionarPainel() {
    var r = btn.getBoundingClientRect();
    var gap = 6;
    panel.style.top = Math.round(r.bottom + gap) + 'px';
    panel.style.right = Math.max(8, Math.round(window.innerWidth - r.right)) + 'px';
    panel.style.left = 'auto';
  }

  function fecharMais() {
    panel.classList.add('hidden');
    wrap.removeAttribute('data-open');
    btn.setAttribute('aria-expanded', 'false');
    if (panelNoBody && panelHost && panel.parentNode !== panelHost) {
      panelHost.appendChild(panel);
      panelNoBody = false;
    }
    var saldo = document.getElementById('pdv-estoque-vila-menu');
    if (saldo && saldo.open) saldo.open = false;
  }

  function abrirMais() {
    if (panel.parentNode !== document.body) {
      document.body.appendChild(panel);
      panelNoBody = true;
    }
    posicionarPainel();
    panel.classList.remove('hidden');
    wrap.setAttribute('data-open', '1');
    btn.setAttribute('aria-expanded', 'true');
  }

  function toggleMais(ev) {
    if (ev) {
      ev.preventDefault();
      ev.stopPropagation();
    }
    if (maisAberto()) fecharMais();
    else abrirMais();
  }

  btn.addEventListener('click', function (ev) {
    registrarClique('mais');
    toggleMais(ev);
  });

  panel.addEventListener('click', function (ev) {
    var keyEl = ev.target && ev.target.closest ? ev.target.closest('[data-pdv-topbar-key]') : null;
    if (keyEl && keyEl.getAttribute('data-pdv-topbar-key') !== 'mais') {
      registrarClique(keyEl.getAttribute('data-pdv-topbar-key'));
    }
    if (ev.target.closest('#pdv-estoque-vila-menu > summary')) return;
    if (ev.target.closest('a, button, .pdv-estoque-vila-link')) {
      window.setTimeout(fecharMais, 0);
    }
  });

  root.addEventListener(
    'click',
    function (ev) {
      var keyEl = ev.target && ev.target.closest ? ev.target.closest('[data-pdv-topbar-key]') : null;
      if (keyEl && keyEl.getAttribute('data-pdv-topbar-key') !== 'mais') {
        registrarClique(keyEl.getAttribute('data-pdv-topbar-key'));
      }
    },
    true
  );

  document.addEventListener('click', function (ev) {
    if (!maisAberto()) return;
    if (wrap.contains(ev.target) || panel.contains(ev.target)) return;
    fecharMais();
  });

  document.addEventListener('keydown', function (ev) {
    if (ev.key === 'Escape' && maisAberto()) {
      fecharMais();
    }
  });

  window.addEventListener('resize', function () {
    if (maisAberto()) posicionarPainel();
  });
})();
