/**
 * Proteção global contra duplo envio em formulários (POST clássico).
 * - Impede um segundo submit no mesmo documento antes da navegação.
 * - Desativa o botão submitter e aplica .btn-loading (CSS em _agro_consulta_ui.html).
 * - Formulários com fetch/AJAX: marque o <form data-agro-no-double-guard="1"> para ignorar.
 */
(function () {
    'use strict';
    if (window.__agroDoubleSubmitGuardInstalled) return;
    window.__agroDoubleSubmitGuardInstalled = true;

    function pickSubmitter(form, ev) {
        var sub = ev && ev.submitter;
        if (sub && sub.form === form && (sub.type === 'submit' || sub.getAttribute('type') === 'submit')) return sub;
        return form.querySelector('button[type="submit"],input[type="submit"]');
    }

    function busySubmitControl(sub) {
        if (!sub || sub.type !== 'submit') return;
        sub.disabled = true;
        sub.classList.add('btn-loading');
        if (sub.tagName === 'INPUT') {
            if (sub.getAttribute('data-agro-orig-value') == null) {
                sub.setAttribute('data-agro-orig-value', sub.value || '');
            }
            sub.value = 'Processando…';
        } else {
            if (sub.getAttribute('data-agro-orig-text') == null) {
                sub.setAttribute('data-agro-orig-text', (sub.textContent || '').trim());
            }
            sub.textContent = 'Processando…';
        }
    }

    document.addEventListener(
        'submit',
        function (e) {
            var form = e.target;
            if (!form || form.tagName !== 'FORM') return;
            if (form.getAttribute('data-agro-no-double-guard') === '1') return;

            if (form.getAttribute('data-agro-submitting') === '1') {
                e.preventDefault();
                e.stopImmediatePropagation();
                return;
            }
            form.setAttribute('data-agro-submitting', '1');

            var sub = pickSubmitter(form, e);
            busySubmitControl(sub);
        },
        true
    );

    /** Chave nova para header ``Idempotency-Key`` / body (alinhado ao middleware ``AgroIdempotencyMiddleware``). */
    window.agroNovaChaveIdempotencia = function () {
        if (typeof crypto !== 'undefined' && crypto.randomUUID) return crypto.randomUUID();
        return 'r-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 11);
    };
})();
