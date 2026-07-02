(function () {
    if (window.AgroPdvCatalogSplash) return;

    var root = null;
    var hideTimer = null;
    var visible = false;
    var shownAt = 0;
    /** Tempo mínimo visível — evita “piscar” quando a rede responde rápido. */
    var MIN_VISIBLE_MS = 750;

    function el() {
        return root || (root = document.getElementById('agro-pdv-catalog-boot'));
    }

    window.AgroPdvCatalogSplash = {
        show: function () {
            clearTimeout(hideTimer);
            var node = el();
            if (!node) return;
            visible = true;
            shownAt = Date.now();
            node.classList.remove('hidden');
            node.setAttribute('aria-hidden', 'false');
            document.body.classList.add('agro-pdv-boot-lock');
        },
        hide: function (delayMs) {
            var extra = delayMs == null ? 0 : Math.max(0, Number(delayMs) || 0);
            var elapsed = shownAt ? Date.now() - shownAt : MIN_VISIBLE_MS;
            var ms = Math.max(extra, Math.max(0, MIN_VISIBLE_MS - elapsed));
            clearTimeout(hideTimer);
            hideTimer = setTimeout(function () {
                var node = el();
                if (!node) return;
                visible = false;
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
