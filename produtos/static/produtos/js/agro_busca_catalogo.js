/**
 * Busca Catálogo Agro (BCA) — combo padrão do sistema:
 * API `/api/buscar/` + motor `catalogo_agro.buscar` (Postgres Agro).
 */
(function (w) {
    'use strict';

    var AGRO_BUSCA_CATALOGO = {
        nome: 'Busca Catálogo Agro',
        sigla: 'BCA',
        api: '/api/buscar/',
        motor: 'catalogo_agro.buscar',
        banco: 'postgres',
    };

    w.AGRO_BUSCA_CATALOGO = AGRO_BUSCA_CATALOGO;

    w.fetchAgroBuscaCatalogo = function (q, opts) {
        opts = opts || {};
        var params = new URLSearchParams();
        var termo = String(q || '').trim();
        if (termo) params.set('q', termo);
        if (opts.limit != null) params.set('limit', String(opts.limit));
        if (opts.contexto) params.set('contexto', String(opts.contexto));
        if (opts.compras) params.set('compras', '1');
        if (opts.wizard) params.set('wizard', '1');
        if (opts.incluir_saldo) params.set('incluir_saldo', '1');
        if (opts.ativo) params.set('ativo', '1');
        if (opts.inativos) params.set('inativos', '1');
        var extra = opts.extra || {};
        Object.keys(extra).forEach(function (k) {
            if (extra[k] != null && extra[k] !== '') params.set(k, String(extra[k]));
        });
        var url = AGRO_BUSCA_CATALOGO.api + '?' + params.toString();
        return fetch(url, { credentials: 'same-origin', signal: opts.signal || null })
            .then(function (r) { return r.json(); });
    };

    w.agroTermoBuscaCatalogoServidor = function (termoBruto, modo) {
        var bruto = String(termoBruto || '').trim();
        if (!bruto) return false;
        if (bruto.toLowerCase() === '#prova' || bruto.indexOf('#') === 0) return true;
        if (modo === 'scanner') return false;
        if (typeof w.normalizarBuscaLocal !== 'function') return bruto.length >= 2;
        var norm = w.normalizarBuscaLocal(bruto);
        if (norm.length >= 2) return true;
        if (typeof w.pareceCodigoGmEtiqueta === 'function' && w.pareceCodigoGmEtiqueta(norm)) return true;
        if (typeof w.pareceCodigoBarrasNumerico === 'function' && w.pareceCodigoBarrasNumerico(norm)) return true;
        return false;
    };

    w.agroStatusTextoBuscaCatalogo = function (data, qtdVisivel) {
        var prova = data && data.prova_unificada;
        if (prova && prova.ok) {
            return 'Prova OK · ' + (prova.mensagem || AGRO_BUSCA_CATALOGO.nome);
        }
        var n = qtdVisivel != null ? qtdVisivel : (data && data.produtos ? data.produtos.length : 0);
        return AGRO_BUSCA_CATALOGO.sigla + ' · ' + n + ' produto(s)';
    };

    w.agroProdutoIdProvaUnificada = function (id) {
        return String(id || '') === '__prova_unificada__';
    };
})(window);
