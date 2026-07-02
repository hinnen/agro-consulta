(function () {
    if (window.AgroPdvCatalogSplash) return;

    var root = null;
    var hideTimer = null;
    var visible = false;

    function el() {
        return root || (root = document.getElementById('agro-pdv-catalog-boot'));
    }

    window.AgroPdvCatalogSplash = {
        show: function () {
            clearTimeout(hideTimer);
            var node = el();
            if (!node) return;
            visible = true;
            node.classList.remove('hidden');
            node.setAttribute('aria-hidden', 'false');
            document.body.classList.add('agro-pdv-boot-lock');
        },
        hide: function (delayMs) {
            var ms = delayMs == null ? 0 : Math.max(0, Number(delayMs) || 0);
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
