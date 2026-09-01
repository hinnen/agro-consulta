/**
 * PDV topbar — ícone WhatsApp abre o chat da loja + aviso se a ponte cair.
 */
(function () {
  'use strict';

  var btn = document.getElementById('pdv-topbar-whatsapp-btn');
  if (!btn) return;

  var actionsHost = document.getElementById('pdv-topbar-actions');
  var lastUnread = -1;

  function keepPlace() {
    if (!btn || !actionsHost) return;
    if (btn.parentNode !== actionsHost || actionsHost.firstElementChild !== btn) {
      actionsHost.insertBefore(btn, actionsHost.firstChild);
    }
  }
  keepPlace();
  window.setTimeout(keepPlace, 0);
  window.setTimeout(keepPlace, 600);

  function abrirChat() {
    var url = btn.getAttribute('data-wa-chat-url') || '/atendimento-whatsapp/';
    try {
      if (window.agroAbrirUrlExterna) {
        window.agroAbrirUrlExterna(url);
        return;
      }
    } catch (e) {}
    window.open(url, '_blank', 'noopener,noreferrer');
  }

  function beep() {
    try {
      var ctx = new (window.AudioContext || window.webkitAudioContext)();
      var o = ctx.createOscillator();
      var g = ctx.createGain();
      o.type = 'sine';
      o.frequency.value = 880;
      g.gain.value = 0.06;
      o.connect(g);
      g.connect(ctx.destination);
      o.start();
      setTimeout(function () {
        o.stop();
        ctx.close();
      }, 160);
    } catch (e) {}
  }

  function flagEl() {
    var el = document.getElementById('pdv-wa-flag');
    if (el) return el;
    el = document.createElement('span');
    el.id = 'pdv-wa-flag';
    el.className = 'pdv-wa-flag hidden';
    btn.appendChild(el);
    return el;
  }

  function pintar(j) {
    var p = (j && j.ponte) || {};
    var n = (j && j.nao_lidas) || {};
    var tot = (parseInt(n.pendente || 0, 10) || 0) + (parseInt(n.centro || 0, 10) || 0) + (parseInt(n.vila || 0, 10) || 0);
    var off = !p.ponte_viva;
    btn.classList.toggle('is-off', off);
    btn.title = off
      ? 'WhatsApp off — rode iniciar.bat neste PC'
      : tot
        ? 'WhatsApp · ' + tot + ' nova(s)'
        : 'WhatsApp das lojas — atendimento';
    var flag = flagEl();
    if (off) {
      flag.textContent = 'Off';
      flag.classList.remove('hidden');
    } else if (tot) {
      flag.textContent = tot > 9 ? '9+' : String(tot);
      flag.classList.remove('hidden');
    } else {
      flag.classList.add('hidden');
    }
    if (lastUnread >= 0 && tot > lastUnread && !off) {
      beep();
      if (window.Notification && Notification.permission === 'granted') {
        try {
          new Notification('WhatsApp loja', { body: tot + ' mensagem(ns) nova(s)', silent: true });
        } catch (e) {}
      }
    }
    lastUnread = tot;
  }

  function ping() {
    fetch('/api/atendimento-whatsapp/estado/', { credentials: 'same-origin' })
      .then(function (r) {
        return r.json().catch(function () {
          return null;
        });
      })
      .then(function (j) {
        if (j && j.ok) pintar(j);
        else pintar({ ponte: { ponte_viva: false }, nao_lidas: {} });
      })
      .catch(function () {
        pintar({ ponte: { ponte_viva: false }, nao_lidas: {} });
      });
  }

  btn.addEventListener('click', function (ev) {
    ev.preventDefault();
    ev.stopPropagation();
    try {
      if (window.Notification && Notification.permission === 'default') Notification.requestPermission();
    } catch (e) {}
    abrirChat();
  });

  ping();
  window.setInterval(ping, 8000);
})();
