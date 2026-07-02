(function () {
    if (window.AgroPdvCatalogSplash) return;

    var root = null;
    var hideTimer = null;
    var showDelayTimer = null;
    var visible = false;
    var shownAt = 0;
    /** Só mostra o splash se o carregamento passar disso (ms) — cache rápido = operador não vê nada. */
    var SHOW_DELAY_MS = 400;
    /** Se apareceu, tempo mínimo para não “piscar”. */
    var MIN_VISIBLE_MS = 200;

    function el() {
        return root || (root = document.getElementById('agro-pdv-catalog-boot'));
    }

    function showNow() {
        var node = el();
        if (!node) return;
        visible = true;
        shownAt = Date.now();
        node.classList.remove('hidden');
        node.setAttribute('aria-hidden', 'false');
        document.body.classList.add('agro-pdv-boot-lock');
    }

    window.AgroPdvCatalogSplash = {
        show: function () {
            clearTimeout(hideTimer);
            if (visible) return;
            if (showDelayTimer) return;
            showDelayTimer = setTimeout(function () {
                showDelayTimer = null;
                showNow();
            }, SHOW_DELAY_MS);
        },
        hide: function (delayMs) {
            clearTimeout(showDelayTimer);
            showDelayTimer = null;
            if (!visible) return;
            var extra = delayMs == null ? 0 : Math.max(0, Number(delayMs) || 0);
            var elapsed = shownAt ? Date.now() - shownAt : MIN_VISIBLE_MS;
            var ms = Math.max(extra, Math.max(0, MIN_VISIBLE_MS - elapsed));
            clearTimeout(hideTimer);
            hideTimer = setTimeout(function () {
                var node = el();
                if (!node) return;
                visible = false;
                shownAt = 0;
                node.classList.add('hidden');
                node.setAttribute('aria-hidden', 'true');
                document.body.classList.remove('agro-pdv-boot-lock');
            }, ms);
        },
        isVisible: function () {
            return visible;
        },
    };
})();
