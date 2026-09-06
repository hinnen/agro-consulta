/**
 * Cache local PDV — TTL leve para PCs fracos (SSD + CPU limitado).
 * Venda final sempre valida no servidor; cache só acelera leitura na tela.
 */
(function (global) {
    var TTL = {
        CLIENTES_MS: 4 * 60 * 60 * 1000,
        PROMOCOES_MS: 2 * 60 * 60 * 1000,
        ENTREGAS_MS: 90 * 1000,
        OPERADORES_MS: 24 * 60 * 60 * 1000,
    };

    function readRaw(key) {
        try {
            return global.localStorage.getItem(key);
        } catch (e) {
            return null;
        }
    }

    function parseEntry(raw) {
        if (!raw) return null;
        try {
            return JSON.parse(raw);
        } catch (e) {
            return null;
        }
    }

    function savedAt(key) {
        var d = parseEntry(readRaw(key));
        if (!d) return 0;
        return Number(d.saved_at) || 0;
    }

    function isStale(key, maxAgeMs) {
        var t = savedAt(key);
        if (!t) return true;
        return Date.now() - t > maxAgeMs;
    }

    function readPayload(key) {
        return parseEntry(readRaw(key));
    }

    function writePayload(key, data) {
        try {
            var envelope = Object.assign({}, data || {}, { saved_at: Date.now() });
            global.localStorage.setItem(key, JSON.stringify(envelope));
            return true;
        } catch (e) {
            return false;
        }
    }

    global.AgroPdvOfflineCache = {
        TTL: TTL,
        savedAt: savedAt,
        isStale: isStale,
        readPayload: readPayload,
        writePayload: writePayload,
    };
})(typeof window !== 'undefined' ? window : this);
