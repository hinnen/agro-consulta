/**
 * PDV topbar — WhatsApp.
 * Padrão: «Em breve…». Se Bot → Recursos «PDV abre o Zap» estiver ON → abre o chat.
 * Aviso sonoro/badge: «Aviso no PDV» (também no Bot → Recursos).
 */
(function () {
  'use strict';

  var btn = document.getElementById('pdv-topbar-whatsapp-btn');
  var box = document.getElementById('pdv-wa-em-breve');
  var ok = document.getElementById('pdv-wa-em-breve-ok');
  if (!btn) return;

  var recursos = {
    feat_pdv_abre_zap: false,
    feat_pdv_aviso_msg: false,
  };
  var lastNaoLidas = 0;
  var badge = null;

  if (box && box.parentElement !== document.body) {
    document.body.appendChild(box);
  }

  var actionsHost = document.getElementById('pdv-topbar-actions');

  function keepPlace() {
    if (!btn || !actionsHost) return;
    if (btn.parentNode !== actionsHost || actionsHost.firstElementChild !== btn) {
      actionsHost.insertBefore(btn, actionsHost.firstChild);
    }
  }
  keepPlace();
  window.setTimeout(keepPlace, 0);
  window.setTimeout(keepPlace, 600);

  function garantirBadge() {
    if (badge || !btn) return;
    badge = document.createElement('span');
    badge.id = 'pdv-wa-badge';
    badge.className =
      'absolute -right-1 -top-1 hidden min-w-[1.15rem] rounded-full bg-rose-600 px-1 text-center text-[0.65rem] font-black text-white';
    badge.textContent = '0';
    if (getComputedStyle(btn).position === 'static') btn.style.position = 'relative';
    btn.appendChild(badge);
  }

  function tocarAviso() {
    try {
      var Ctx = window.AudioContext || window.webkitAudioContext;
      if (!Ctx) return;
      var ctx = new Ctx();
      var o = ctx.createOscillator();
      var g = ctx.createGain();
      o.type = 'sine';
      o.frequency.value = 880;
      g.gain.value = 0.04;
      o.connect(g);
      g.connect(ctx.destination);
      o.start();
      setTimeout(function () {
        o.stop();
        ctx.close();
      }, 180);
    } catch (_) {}
  }

  function aplicarNaoLidas(n) {
    if (!recursos.feat_pdv_aviso_msg) {
      if (badge) badge.classList.add('hidden');
      return;
    }
    garantirBadge();
    var q = parseInt(n || 0, 10) || 0;
    if (q > lastNaoLidas && lastNaoLidas >= 0) tocarAviso();
    lastNaoLidas = q;
    if (!badge) return;
    if (q <= 0) {
      badge.classList.add('hidden');
      return;
    }
    badge.textContent = q > 99 ? '99+' : String(q);
    badge.classList.remove('hidden');
  }

  function puxarEstado() {
    fetch('/api/atendimento-whatsapp/estado/', { credentials: 'same-origin' })
      .then(function (r) {
        return r.json();
      })
      .then(function (j) {
        if (!j || !j.ok) return;
        var rec = j.recursos || {};
        recursos.feat_pdv_abre_zap = !!rec.feat_pdv_abre_zap;
        recursos.feat_pdv_aviso_msg = !!rec.feat_pdv_aviso_msg;
        aplicarNaoLidas(j.nao_lidas);
      })
      .catch(function () {});
  }

  function abrirBreve() {
    if (!box) return;
    box.classList.remove('hidden');
    box.setAttribute('aria-hidden', 'false');
  }

  function fecharBreve() {
    if (!box) return;
    box.classList.add('hidden');
    box.setAttribute('aria-hidden', 'true');
  }

  function abrirZapSemSairDoApp() {
    var href = '/atendimento-whatsapp/';
    try {
      if (window.AgroDualWindow && typeof window.AgroDualWindow.navigateGestao === 'function') {
        window.AgroDualWindow.navigateGestao(href);
        return;
      }
    } catch (_) {}
    try {
      if (window.tabManager && typeof window.tabManager.addTab === 'function') {
        window.tabManager.addTab('WhatsApp', href, 'message-circle');
        return;
      }
    } catch (_) {}
    try {
      if (typeof window.__agroInAppAddTab === 'function') {
        window.__agroInAppAddTab(href);
        return;
      }
    } catch (_) {}
    window.location.href = href;
  }

  btn.addEventListener('click', function (ev) {
    ev.preventDefault();
    ev.stopPropagation();
    if (recursos.feat_pdv_abre_zap) {
      abrirZapSemSairDoApp();
      return;
    }
    abrirBreve();
  });

  if (ok) {
    ok.addEventListener('click', function (ev) {
      ev.preventDefault();
      fecharBreve();
    });
  }

  if (box) {
    box.addEventListener('click', function (ev) {
      if (ev.target === box) fecharBreve();
    });
  }

  document.addEventListener('keydown', function (ev) {
    if (ev.key === 'Escape' && box && !box.classList.contains('hidden')) fecharBreve();
  });

  puxarEstado();
  window.setInterval(puxarEstado, 8000);
})();
