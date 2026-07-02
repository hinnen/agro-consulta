/**
 * Promoções Agro no PDV — carrega regras vigentes e calcula preço unitário.
 * Leve X / acima de X: soma quantidades entre produtos da mesma promoção (mix).
 */
(function (global) {
    var mapa = {};
    var carregado = false;
    var empresa = 'centro';
    var apiUrl = '';
    var PROMO_LS_PREFIX = 'agro_pdv_promocoes_cache_v1_';

    function promoCacheKey() {
        return PROMO_LS_PREFIX + String(empresa || 'centro');
    }

    function aplicarMapaPromo(obj) {
        mapa = obj && typeof obj === 'object' ? obj : {};
        carregado = true;
        return mapa;
    }

    function fetchPromoRede(cacheKey) {
        if (!apiUrl) {
            carregado = true;
            return Promise.resolve(mapa);
        }
        var url =
            apiUrl + (apiUrl.indexOf('?') >= 0 ? '&' : '?') + 'empresa=' + encodeURIComponent(empresa) + '&tela=pdv';
        return fetch(url, { credentials: 'same-origin' })
            .then(function (r) {
                return r.json();
            })
            .then(function (d) {
                aplicarMapaPromo((d && d.promocoes) || {});
                if (global.AgroPdvOfflineCache && global.AgroPdvOfflineCache.writePayload) {
                    global.AgroPdvOfflineCache.writePayload(cacheKey, { promocoes: mapa, empresa: empresa });
                }
                return mapa;
            })
            .catch(function () {
                if (!carregado) aplicarMapaPromo({});
                return mapa;
            });
    }

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

    function promoPoolKey(promo) {
        if (!promo || promo.id == null || promo.id === '') return '';
        if (promo.tipo === 'valor_direto') return '';
        return String(promo.tipo) + ':' + String(promo.id);
    }

    function limparAlocPromo(item) {
        if (!item) return;
        delete item.promo_unidades_promo;
        delete item.promo_unidades_normal;
        delete item.promo_qtd_pool;
        delete item.promo_linhas_pool;
        delete item.promo_grupos_pool;
        delete item.promo_mix_cor;
        delete item.promo_mix_ativo;
        delete item.promo_mix_pendente;
    }

    function alocarLevePaguePool(entries, promo) {
        var lim = toNum(promo.qtd_x);
        var py = toNum(promo.preco_y);
        var units = [];
        entries.forEach(function (e, ei) {
            var qtd = toNum(e.qtd, 0);
            for (var u = 0; u < qtd; u++) {
                units.push({ entry: e, sortKey: ei * 10000 + u, padrao: e.padrao });
            }
        });
        var totalQtd = units.length;
        var nLinhas = entries.length;
        var gruposPool = lim > 0 ? Math.floor(totalQtd / lim) : 0;
        var promoSlots = gruposPool * lim;
        var mixCor =
            nLinhas > 1 && promo.id != null ? Math.abs(parseInt(String(promo.id), 10) || 0) % 6 : -1;
        var promoCount = {};
        var normalCount = {};

        entries.forEach(function (e) {
            var id = String(e.item.id);
            promoCount[id] = 0;
            normalCount[id] = 0;
        });

        if (promoSlots > 0) {
            units.sort(function (a, b) {
                if (b.padrao !== a.padrao) return b.padrao - a.padrao;
                return a.sortKey - b.sortKey;
            });
            units.forEach(function (u, i) {
                var id = String(u.entry.item.id);
                if (i < promoSlots) promoCount[id]++;
                else normalCount[id]++;
            });
        } else {
            units.forEach(function (u) {
                normalCount[String(u.entry.item.id)]++;
            });
        }

        entries.forEach(function (e) {
            var id = String(e.item.id);
            var qPromo = promoCount[id] || 0;
            var qNormal = normalCount[id] || 0;
            var qtd = toNum(e.qtd, 0);
            var total = qPromo * py + qNormal * e.padrao;
            e.item.promocao = promo;
            e.item.promo_unidades_promo = qPromo;
            e.item.promo_unidades_normal = qNormal;
            e.item.promo_qtd_pool = totalQtd;
            e.item.promo_linhas_pool = nLinhas;
            e.item.promo_grupos_pool = gruposPool;
            e.item.promo_mix_ativo = gruposPool > 0 && qPromo > 0;
            e.item.promo_mix_pendente = gruposPool <= 0 && totalQtd > 0;
            e.item.promo_mix_cor = nLinhas > 1 && mixCor >= 0 ? mixCor : null;
            e.item.preco = qtd > 0 ? Math.round((total / qtd) * 10000) / 10000 : e.padrao;
        });
    }

    function padraoItemComForma(item, forma) {
        var padrao = toNum(item.preco_padrao != null ? item.preco_padrao : item.preco, 0);
        if (!item.preco_padrao) item.preco_padrao = padrao;
        if (
            forma &&
            global.AgroPrecosFormaPagamento &&
            global.AgroPrecosFormaPagamento.precoBaseForma
        ) {
            return global.AgroPrecosFormaPagamento.precoBaseForma(item, forma);
        }
        return padrao;
    }

    function alocarAcimaUnidadesPool(entries, promo) {
        var lim = toNum(promo.qtd_x);
        var py = toNum(promo.preco_y);
        var totalQtd = 0;
        var nLinhas = entries.length;
        entries.forEach(function (e) {
            totalQtd += toNum(e.qtd, 0);
        });
        var ativo = totalQtd > lim;
        var mixCor =
            nLinhas > 1 && promo.id != null ? Math.abs(parseInt(String(promo.id), 10) || 0) % 6 : -1;
        entries.forEach(function (e) {
            var qtd = toNum(e.qtd, 0);
            e.item.promocao = promo;
            e.item.promo_unidades_promo = ativo ? qtd : 0;
            e.item.promo_unidades_normal = ativo ? 0 : qtd;
            e.item.promo_qtd_pool = totalQtd;
            e.item.promo_linhas_pool = nLinhas;
            e.item.promo_grupos_pool = ativo ? 1 : 0;
            e.item.promo_mix_ativo = ativo;
            e.item.promo_mix_pendente = !ativo && totalQtd > 0;
            e.item.promo_mix_cor = nLinhas > 1 && mixCor >= 0 ? mixCor : null;
            e.item.preco = ativo ? py : e.padrao;
        });
    }

    function recalcCarrinhoComForma(itens, forma) {
        if (!Array.isArray(itens)) return itens;
        var pools = {};

        itens.forEach(function (item) {
            if (!item) return;
            if (item.preco_manual) {
                item.promocao = getPromo(item.id);
                limparAlocPromo(item);
                return;
            }
            var padrao = padraoItemComForma(item, forma);
            var promo = getPromo(item.id);
            if (!promo) {
                item.promocao = null;
                limparAlocPromo(item);
                item.preco = padrao;
                return;
            }
            if (promo.tipo === 'valor_direto') {
                item.promocao = promo;
                limparAlocPromo(item);
                item.preco = calcularPreco(promo, toNum(item.qtd, 1), padrao);
                return;
            }
            var key = promoPoolKey(promo);
            if (!key) {
                item.promocao = promo;
                limparAlocPromo(item);
                item.preco = calcularPreco(promo, toNum(item.qtd, 1), padrao);
                return;
            }
            if (!pools[key]) pools[key] = { promo: promo, entries: [] };
            pools[key].entries.push({ item: item, qtd: toNum(item.qtd, 1), padrao: padrao });
        });

        Object.keys(pools).forEach(function (key) {
            var pool = pools[key];
            if (pool.promo.tipo === 'leve_pague') {
                alocarLevePaguePool(pool.entries, pool.promo);
            } else if (pool.promo.tipo === 'acima_unidades') {
                alocarAcimaUnidadesPool(pool.entries, pool.promo);
            }
        });

        return agruparCarrinhoPromoAtiva(itens);
    }

    /** Promo de pool (mix) com critério atingido — usado para agrupar linhas no carrinho. */
    function poolAtivoNoItem(item) {
        if (!item || item.preco_manual) return false;
        var promo = item.promocao || getPromo(item.id);
        if (!promo || promo.tipo === 'valor_direto') return false;
        if (!promoPoolKey(promo)) return false;
        if (item.promo_grupos_pool != null) return toNum(item.promo_grupos_pool, 0) > 0;
        return false;
    }

    /**
     * Junta linhas da mesma promo ativa (mix) — bloco na posição da 1ª linha do grupo.
     * Só reordena quando o critério da promo já foi atingido.
     */
    function agruparCarrinhoPromoAtiva(itens) {
        if (!Array.isArray(itens) || itens.length < 2) return itens;

        var itemPoolKey = [];
        var poolMembers = {};

        itens.forEach(function (item, i) {
            var key = '';
            if (poolAtivoNoItem(item)) {
                var promo = item.promocao || getPromo(item.id);
                key = promoPoolKey(promo);
            }
            itemPoolKey[i] = key;
            if (key) {
                if (!poolMembers[key]) poolMembers[key] = [];
                poolMembers[key].push(item);
            }
        });

        var keys = Object.keys(poolMembers);
        if (!keys.length) return itens;

        var needsReorder = false;
        keys.forEach(function (key) {
            if (poolMembers[key].length < 2) return;
            var indices = [];
            for (var i = 0; i < itens.length; i++) {
                if (itemPoolKey[i] === key) indices.push(i);
            }
            for (var j = 1; j < indices.length; j++) {
                if (indices[j] !== indices[j - 1] + 1) {
                    needsReorder = true;
                    break;
                }
            }
        });
        if (!needsReorder) return itens;

        var emittedPools = {};
        var out = [];
        for (var i = 0; i < itens.length; i++) {
            var key = itemPoolKey[i];
            if (!key) {
                out.push(itens[i]);
                continue;
            }
            if (emittedPools[key]) continue;
            poolMembers[key].forEach(function (item) {
                out.push(item);
            });
            emittedPools[key] = true;
        }

        itens.length = 0;
        out.forEach(function (item) {
            itens.push(item);
        });
        return itens;
    }

    function aplicarNoItem(item) {
        if (!item) return item;
        if (item.preco_manual) {
            item.promocao = getPromo(item.id);
            limparAlocPromo(item);
            return item;
        }
        var padrao = toNum(item.preco_padrao != null ? item.preco_padrao : item.preco, 0);
        if (!item.preco_padrao) item.preco_padrao = padrao;
        var promo = getPromo(item.id);
        item.promocao = promo;
        item.preco = resolvePreco(item.id, item.qtd, padrao);
        limparAlocPromo(item);
        return item;
    }

    function recalcCarrinho(itens) {
        return recalcCarrinhoComForma(itens, '');
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

    function tituloPromoBase(promo, ctx) {
        var nome = String(promo.nome || '').trim();
        var mix = ctx && ctx.pooled && toNum(ctx.linhasNoPool, 0) > 1 ? ' · mix de produtos' : '';
        return (nome ? nome + ' — ' : '') + mix;
    }

    function badgePendenteStack(lineBottom, title) {
        return {
            stack: true,
            lineTop: 'PROMO',
            lineBottom: lineBottom,
            title: title,
        };
    }

    /** Soma quantidades no carrinho para a mesma promoção (mix). */
    function poolContextoFromCarrinho(item, itens) {
        if (!item) return null;
        var promo = item.promocao || getPromo(item.id);
        if (!promo || promo.tipo === 'valor_direto') return null;
        var key = promoPoolKey(promo);
        if (!key) return null;

        if (item.promo_qtd_pool != null) {
            return {
                pooled: true,
                qtdPool: toNum(item.promo_qtd_pool, 0),
                qtdLinhaPromo: toNum(item.promo_unidades_promo, 0),
                qtdLinhaNormal: toNum(item.promo_unidades_normal, 0),
                linhasNoPool: Math.max(1, toNum(item.promo_linhas_pool, 1)),
                mixMultiLinha: toNum(item.promo_linhas_pool, 0) > 1,
            };
        }

        var total = 0;
        var nLinhas = 0;
        (itens || []).forEach(function (it) {
            if (!it || it.preco_manual) return;
            var p = it.promocao || getPromo(it.id);
            if (!p || promoPoolKey(p) !== key) return;
            if (!it.promocao) it.promocao = p;
            total += toNum(it.qtd, 0);
            nLinhas += 1;
        });
        if (nLinhas <= 0) return null;
        return {
            pooled: true,
            qtdPool: total,
            qtdLinhaPromo: toNum(item.promo_unidades_promo, 0),
            qtdLinhaNormal:
                item.promo_unidades_normal != null ? toNum(item.promo_unidades_normal, 0) : toNum(item.qtd, 0),
            linhasNoPool: nLinhas,
            mixMultiLinha: nLinhas > 1,
        };
    }

    /** Linha anterior no carrinho é do mesmo mix ativo (bloco já mostrou cabeçalho). */
    function mixBlocoContextoCarrinho(item, itens) {
        var key = '';
        if (poolAtivoNoItem(item)) {
            var promo = item.promocao || getPromo(item.id);
            key = promoPoolKey(promo);
        }
        if (!key || !Array.isArray(itens) || itens.length < 2) {
            return { mixContinuacao: false, mixCabecalho: true };
        }
        if (toNum(item.promo_linhas_pool, 0) <= 1) {
            return { mixContinuacao: false, mixCabecalho: true };
        }
        var idx = -1;
        for (var i = 0; i < itens.length; i++) {
            if (itens[i] === item) {
                idx = i;
                break;
            }
        }
        if (idx <= 0) return { mixContinuacao: false, mixCabecalho: true };
        var prev = itens[idx - 1];
        var prevKey = '';
        if (prev && poolAtivoNoItem(prev)) {
            var pPromo = prev.promocao || getPromo(prev.id);
            prevKey = promoPoolKey(pPromo);
        }
        var mixContinuacao = prevKey === key;
        return { mixContinuacao: mixContinuacao, mixCabecalho: !mixContinuacao };
    }

    /**
     * ctx.pooled: usa promo_qtd_pool e unidades alocadas por linha.
     */
    function resumoIndicadorPromo(promo, qtd, precoPadrao, ctx) {
        if (!promo) return null;
        ctx = ctx || {};
        qtd = toNum(qtd, 0);
        precoPadrao = toNum(precoPadrao, 0);
        if (qtd <= 0) return null;
        var qtdPool = ctx.qtdPool != null ? toNum(ctx.qtdPool, 0) : qtd;
        var pooled = !!ctx.pooled || ctx.qtdPool != null;
        var qLinPromo = pooled && ctx.qtdLinhaPromo != null ? toNum(ctx.qtdLinhaPromo, 0) : null;
        var qLinNorm = pooled && ctx.qtdLinhaNormal != null ? toNum(ctx.qtdLinhaNormal, 0) : null;
        var linhasNoPool = pooled ? Math.max(1, toNum(ctx.linhasNoPool, 1)) : 1;
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
            var tituloBase =
                tituloPromoBase(promo, { pooled: pooled, linhasNoPool: linhasNoPool }) +
                'Leve ' +
                fmtQtdLabel(lim) +
                ' por ' +
                fmtBrl(py) +
                ' cada';
            var gruposPool = Math.floor(qtdPool / lim);
            var restoPool = qtdPool - gruposPool * lim;

            if (gruposPool <= 0) {
                var falta = Math.max(0, lim - qtdPool);
                var faltaTxt =
                    linhasNoPool > 1
                        ? 'Faltam ' + fmtQtdLabel(falta) + ' · ' + fmtQtdLabel(qtdPool) + '/' + fmtQtdLabel(lim)
                        : 'Faltam ' + fmtQtdLabel(falta);
                return {
                    state: 'pendente',
                    badges: [
                        badgePendenteStack(
                            faltaTxt,
                            tituloBase + (linhasNoPool > 1 ? ' (soma mix no carrinho)' : '')
                        ),
                    ],
                };
            }

            if (pooled && qLinPromo != null) {
                var badgesMix = [];
                var mixMulti = linhasNoPool > 1 || !!ctx.mixMultiLinha;
                if (qLinPromo > 0) {
                    if (mixMulti && ctx.mixContinuacao) {
                        badgesMix.push({
                            text: fmtQtdLabel(qLinPromo) + ' aqui',
                            mixLinha: true,
                            title:
                                tituloBase +
                                ' — ' +
                                fmtQtdLabel(qLinPromo) +
                                ' un. desta linha no bloco mix',
                        });
                    } else if (mixMulti) {
                        var unGrupo =
                            gruposPool === 1
                                ? fmtQtdLabel(lim) + ' un.'
                                : fmtQtdLabel(gruposPool) + '×' + fmtQtdLabel(lim);
                        badgesMix.push({
                            stack: true,
                            lineTop: 'MIX ' + unGrupo,
                            lineBottom: fmtQtdLabel(qLinPromo) + ' aqui',
                            title:
                                tituloBase +
                                ' — bloco de ' +
                                fmtQtdLabel(lim) +
                                ' un. no mix (' +
                                fmtQtdLabel(qtdPool) +
                                ' no carrinho) · desta linha: ' +
                                fmtQtdLabel(qLinPromo) +
                                ' com promo' +
                                (qLinNorm > 0 ? ', ' + fmtQtdLabel(qLinNorm) + ' normal' : ''),
                        });
                    } else if (
                        qLinPromo === qtd &&
                        restoPool <= 0 &&
                        gruposPool === 1
                    ) {
                        badgesMix.push({
                            text: 'PROMO ' + fmtQtdLabel(lim) + '×',
                            title: tituloBase,
                        });
                    } else {
                        badgesMix.push({
                            text: fmtQtdLabel(qLinPromo) + ' promo',
                            title: tituloBase + ' — ' + fmtQtdLabel(qLinPromo) + ' nesta linha',
                        });
                    }
                }
                if (qLinNorm > 0) {
                    badgesMix.push({
                        text: '+' + fmtQtdLabel(qLinNorm) + ' normal',
                        title:
                            fmtQtdLabel(qLinNorm) +
                            ' un. ao preço de tabela (' +
                            fmtBrl(precoPadrao) +
                            ')',
                    });
                }
                if (!badgesMix.length) return null;
                return {
                    state: qLinNorm > 0 ? 'misto' : 'ativo',
                    badges: badgesMix,
                };
            }

            var grupos = Math.floor(qtd / lim);
            var resto = qtd - grupos * lim;
            var unPromo = grupos * lim;
            if (resto <= 0) {
                var txt =
                    grupos === 1
                        ? 'PROMO ' + fmtQtdLabel(lim) + '×'
                        : 'PROMO ' + fmtQtdLabel(unPromo) + '×';
                return {
                    state: 'ativo',
                    badges: [{ text: txt, title: tituloBase }],
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
                tituloPromoBase(promo, { pooled: pooled, linhasNoPool: linhasNoPool }) +
                'Acima de ' +
                fmtQtdLabel(lim) +
                ' un.: ' +
                fmtBrl(py) +
                '/un';
            var qtdRef = pooled ? qtdPool : qtd;
            if (qtdRef <= lim) {
                var faltaAcima = lim + 1 - qtdRef;
                if (faltaAcima < 1) faltaAcima = 1;
                return {
                    state: 'pendente',
                    badges: [
                        pooled
                            ? badgePendenteStack(
                                  'Faltam ' + fmtQtdLabel(faltaAcima),
                                  tituloAcima + (linhasNoPool > 1 ? ' (soma mix)' : '')
                              )
                            : {
                                  text: '>' + fmtQtdLabel(lim) + ' un',
                                  title: tituloAcima,
                              },
                    ],
                };
            }
            if (pooled && qLinPromo != null && qLinPromo > 0) {
                return {
                    state: 'ativo',
                    badges: [
                        {
                            text: 'PROMO',
                            title:
                                tituloAcima +
                                ' — ' +
                                fmtQtdLabel(qtdPool) +
                                ' un. no mix',
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
        var cacheKey = promoCacheKey();
        var cache = global.AgroPdvOfflineCache && global.AgroPdvOfflineCache.readPayload(cacheKey);
        if (!opts.force && cache && cache.promocoes) {
            aplicarMapaPromo(cache.promocoes);
            if (!global.AgroPdvOfflineCache.isStale(cacheKey, global.AgroPdvOfflineCache.TTL.PROMOCOES_MS)) {
                return Promise.resolve(mapa);
            }
            fetchPromoRede(cacheKey);
            return Promise.resolve(mapa);
        }
        return fetchPromoRede(cacheKey);
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
        recalcCarrinhoComForma: recalcCarrinhoComForma,
        agruparCarrinhoPromoAtiva: agruparCarrinhoPromoAtiva,
        mixBlocoContextoCarrinho: mixBlocoContextoCarrinho,
        criterioAtendido: criterioAtendido,
        resumoIndicadorPromo: resumoIndicadorPromo,
        poolContextoFromCarrinho: poolContextoFromCarrinho,
        estaCarregado: function () {
            return carregado;
        },
    };
})(typeof window !== 'undefined' ? window : this);
