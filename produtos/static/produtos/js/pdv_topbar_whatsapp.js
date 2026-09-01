/**
 * PDV topbar — ícone WhatsApp abre o chat da loja.
 */
(function () {
  'use strict';

  var btn = document.getElementById('pdv-topbar-whatsapp-btn');
  if (!btn) return;

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

  btn.addEventListener('click', function (ev) {
    ev.preventDefault();
    ev.stopPropagation();
    abrirChat();
  });
})();
