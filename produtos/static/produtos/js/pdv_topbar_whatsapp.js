/**
 * PDV topbar — ícone WhatsApp (placeholder «Em breve…»).
 */
(function () {
  'use strict';

  var btn = document.getElementById('pdv-topbar-whatsapp-btn');
  var box = document.getElementById('pdv-wa-em-breve');
  var ok = document.getElementById('pdv-wa-em-breve-ok');
  if (!btn || !box) return;

  if (box.parentElement !== document.body) {
    document.body.appendChild(box);
  }

  function abrir() {
    box.classList.remove('hidden');
    box.setAttribute('aria-hidden', 'false');
  }

  function fechar() {
    box.classList.add('hidden');
    box.setAttribute('aria-hidden', 'true');
  }

  btn.addEventListener('click', function (ev) {
    ev.preventDefault();
    ev.stopPropagation();
    abrir();
  });
  if (ok) ok.addEventListener('click', fechar);
  box.addEventListener('click', function (ev) {
    if (ev.target === box) fechar();
  });
  document.addEventListener('keydown', function (ev) {
    if (ev.key === 'Escape' && !box.classList.contains('hidden')) {
      fechar();
    }
  });
})();
