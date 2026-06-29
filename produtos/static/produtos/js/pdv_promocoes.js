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

    function calcTotalLinha(promo, qtd, precoPadrao) {
        if (!promo) return qtd * precoPadrao;
        if (promo.tipo === 'valor_direto') {
            return qtd * calcularPreco(promo, qtd, precoPadrao);
        }
        var lim = toNum(promo.qtd_x);
        var py = toNum(promo.preco_y);
        if (lim <= 0 || py <= 0) return qtd * precoPadrao;
        if (promo.tipo === 'leve_pague') {
            var grupos = Math.floor(qtd / lim);
            var resto = qtd - grupos * lim;
            return grupos * lim * py + resto * precoPadrao;
        }
        if (promo.tipo === 'acima_unidades' && criterioAtendido(promo, qtd)) {
            return qtd * py;
        }
        return qtd * precoPadrao;
    }

    function calcularPreco(promo, qtd, precoPadrao) {
        if (!promo) return precoPadrao;
        if (promo.tipo === 'valor_direto') {
            var pp = toNum(promo.preco_produto_promo, 0);
            if (pp > 0) return pp;
            var py = toNum(promo.preco_y, 0);
            return py > 0 ? py : precoPadrao;
        }
        if (promo.tipo === 'leve_pague') {
            if (qtd <= 0) return precoPadrao;
            return calcTotalLinha(promo, qtd, precoPadrao) / qtd;
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

    function fmtQtdLabel(q) {
        var n = toNum(q, 0);
        if (Math.abs(n - Math.round(n)) < 0.0001) return String(Math.round(n));
        return String(n.toFixed(3)).replace(/\.?0+$/, '').replace('.', ',');
    }

    function fmtBrl(v) {
        var n = toNum(v, 0);
        return 'R$ ' + n.toFixed(2).replace('.', ',');
    }

    /**
     * Textos do selo no carrinho (fase 1 FL-003) — só exibição; preço já vem de calcTotalLinha.
     */
    function resumoIndicadorPromo(promo, qtd, precoPadrao) {
        if (!promo) return null;
        qtd = toNum(qtd, 0);
        precoPadrao = toNum(precoPadrao, 0);
        if (qtd <= 0) return null;
        var lim = toNum(promo.qtd_x);
        var py = toNum(promo.preco_y);
        var nome = String(promo.nome || '').trim();

        if (promo.tipo === 'valor_direto') {
            var pp = toNum(promo.preco_produto_promo, 0) || py;
            return {
                state: 'ativo',
                badges: [
                    {
                        text: 'PROMO',
                        title: nome || (pp > 0 ? 'Preço promocional ' + fmtBrl(pp) : 'Preço promocional'),
                    },
                ],
            };
        }

        if (promo.tipo === 'leve_pague') {
            if (lim <= 0 || py <= 0) return null;
            var grupos = Math.floor(qtd / lim);
            var resto = qtd - grupos * lim;
            var unPromo = grupos * lim;
            var tituloBase =
                (nome ? nome + ' — ' : '') +
                'Leve ' +
                fmtQtdLabel(lim) +
                ' por ' +
                fmtBrl(py) +
                ' cada';
            if (grupos <= 0) {
                var falta = lim - qtd;
                return {
                    state: 'pendente',
                    badges: [
                        {
                            text: 'Faltam ' + fmtQtdLabel(falta),
                            title: tituloBase,
                        },
                    ],
                };
            }
            if (resto <= 0) {
                var txt =
                    grupos === 1
                        ? 'PROMO ' + fmtQtdLabel(lim) + '×'
                        : 'PROMO ' + fmtQtdLabel(unPromo) + '×';
                var titulo =
                    grupos === 1
                        ? tituloBase
                        : tituloBase + ' (' + fmtQtdLabel(grupos) + ' grupos)';
                return {
                    state: 'ativo',
                    badges: [{ text: txt, title: titulo }],
                };
            }
            return {
                state: 'misto',
                badges: [
                    {
                        text: fmtQtdLabel(unPromo) + ' promo',
                        title: tituloBase + ' — ' + fmtQtdLabel(unPromo) + ' un. com desconto',
                    },
                    {
                        text: '+' + fmtQtdLabel(resto) + ' normal',
                        title:
                            fmtQtdLabel(resto) +
                            ' un. ao preço de tabela (' +
                            fmtBrl(precoPadrao) +
                            ')',
                    },
                ],
            };
        }

        if (promo.tipo === 'acima_unidades') {
            if (lim <= 0 || py <= 0) return null;
            var tituloAcima =
                (nome ? nome + ' — ' : '') +
                'Acima de ' +
                fmtQtdLabel(lim) +
                ' un.: ' +
                fmtBrl(py) +
                '/un';
            if (qtd <= lim) {
                return {
                    state: 'pendente',
                    badges: [
                        {
                            text: '>' + fmtQtdLabel(lim) + ' un',
                            title: tituloAcima,
                        },
                    ],
                };
            }
            return {
                state: 'ativo',
                badges: [{ text: 'PROMO', title: tituloAcima }],
            };
        }

        return null;
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
        calcTotalLinha: calcTotalLinha,
        resolvePreco: resolvePreco,
        aplicarNoItem: aplicarNoItem,
        recalcCarrinho: recalcCarrinho,
        criterioAtendido: criterioAtendido,
        resumoIndicadorPromo: resumoIndicadorPromo,
        estaCarregado: function () {
            return carregado;
        },
    };
})(typeof window !== 'undefined' ? window : this);
