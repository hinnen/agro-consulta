/**
 * Promoções Agro no PDV — carrega regras vigentes e calcula preço unitário.
 */
(function (global) {
    var mapa = {};
    var carregado = false;
    var empresa = 'centro';
    var apiUrl = '';

    function toNum(v, fb) {
        var n = parseFloat(v);
        return isFinite(n) ? n : fb || 0;
    }

    function criterioAtendido(promo, qtd) {
        if (!promo) return false;
        var lim = toNum(promo.qtd_x);
        if (promo.tipo === 'valor_direto') return true;
        if (lim <= 0) return false;
        if (promo.tipo === 'leve_pague') return qtd >= lim;
        if (promo.tipo === 'acima_unidades') return qtd > lim;
        return false;
    }

    function calcularPreco(promo, qtd, precoPadrao) {
        if (!promo) return precoPadrao;
        if (promo.tipo === 'valor_direto') {
            var pp = toNum(promo.preco_produto_promo, 0);
            if (pp > 0) return pp;
            var py = toNum(promo.preco_y, 0);
            return py > 0 ? py : precoPadrao;
        }
        if (criterioAtendido(promo, qtd)) {
            return toNum(promo.preco_y, precoPadrao);
        }
        return precoPadrao;
    }

    function getPromo(produtoId) {
        var pid = String(produtoId || '').trim();
        return pid ? mapa[pid] || null : null;
    }

    function resolvePreco(produtoId, quantidade, precoPadrao) {
        var padrao = toNum(precoPadrao, 0);
        var promo = getPromo(produtoId);
        var qtd = toNum(quantidade, 1);
        if (!promo) return padrao;
        return calcularPreco(promo, qtd, padrao);
    }

    function aplicarNoItem(item) {
        if (!item) return item;
        if (item.preco_manual) {
            item.promocao = getPromo(item.id);
            return item;
        }
        var padrao = toNum(item.preco_padrao != null ? item.preco_padrao : item.preco, 0);
        if (!item.preco_padrao) item.preco_padrao = padrao;
        var promo = getPromo(item.id);
        item.promocao = promo;
        item.preco = resolvePreco(item.id, item.qtd, padrao);
        return item;
    }

    function recalcCarrinho(itens) {
        if (!Array.isArray(itens)) return itens;
        itens.forEach(aplicarNoItem);
        return itens;
    }

    function setEmpresa(emp) {
        empresa = String(emp || 'centro').trim().toLowerCase() || 'centro';
    }

    function setApiUrl(url) {
        apiUrl = String(url || '').trim();
    }

    function carregar(opts) {
        opts = opts || {};
        if (opts.empresa) setEmpresa(opts.empresa);
        if (opts.apiUrl) setApiUrl(opts.apiUrl);
        if (!apiUrl) {
            carregado = true;
            return Promise.resolve(mapa);
        }
        var url = apiUrl + (apiUrl.indexOf('?') >= 0 ? '&' : '?') + 'empresa=' + encodeURIComponent(empresa) + '&tela=pdv';
        return fetch(url, { credentials: 'same-origin' })
            .then(function (r) {
                return r.json();
            })
            .then(function (d) {
                mapa = (d && d.promocoes) || {};
                carregado = true;
                return mapa;
            })
            .catch(function () {
                mapa = {};
                carregado = true;
                return mapa;
            });
    }

    global.AgroPdvPromocoes = {
        carregar: carregar,
        setEmpresa: setEmpresa,
        setApiUrl: setApiUrl,
        getPromo: getPromo,
        resolvePreco: resolvePreco,
        aplicarNoItem: aplicarNoItem,
        recalcCarrinho: recalcCarrinho,
        criterioAtendido: criterioAtendido,
        estaCarregado: function () {
            return carregado;
        },
    };
})(typeof window !== 'undefined' ? window : this);
