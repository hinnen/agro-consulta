/**
 * Preço de venda por forma de pagamento ou por 2 grupos (cadastro + PDV / orçamento).
 */
(function (global) {
    'use strict';

    function toNum(v, fb) {
        var n = parseFloat(v);
        return isFinite(n) ? n : (fb != null ? fb : 0);
    }

    function obterFormaPagamentoAtual() {
        var fp = document.getElementById('forma-pagamento-pdv');
        if (fp && fp.value) return String(fp.value).trim();
        return '';
    }

    function obterFormaDoState(state) {
        if (state && state.pagamento && state.pagamento.forma) {
            return String(state.pagamento.forma).trim();
        }
        return obterFormaPagamentoAtual();
    }

    /** Alinha com normalizar_forma_pagamento_caixa (sufixo máquina / parcelamento). */
    function formaCanonKey(raw) {
        var txt = String(raw || '').trim();
        if (!txt) return '';
        var lowFull = txt.toLowerCase();
        if (lowFull.indexOf('pix') >= 0) return 'pix';
        var base = txt
            .replace(/\s+\d+x\s*$/i, '')
            .replace(/\s*Mercado Pago.*$/i, '')
            .replace(/\s*Cielo.*$/i, '')
            .replace(/\s*Sicredi.*$/i, '')
            .replace(/\s*Sicoob.*$/i, '')
            .trim()
            .toLowerCase();
        return base
            .normalize('NFD')
            .replace(/[\u0300-\u036f]/g, '');
    }

    function formaNaLista(lista, forma) {
        if (!Array.isArray(lista) || !forma) return false;
        var f = formaCanonKey(forma);
        if (!f) return false;
        for (var i = 0; i < lista.length; i++) {
            if (formaCanonKey(lista[i]) === f) return true;
        }
        return false;
    }

    function gruposTemDados(g) {
        if (!g || typeof g !== 'object') return false;
        if (toNum(g.preco_a, 0) > 0 || toNum(g.preco_b, 0) > 0) return true;
        if (Array.isArray(g.formas_a) && g.formas_a.length) return true;
        if (Array.isArray(g.formas_b) && g.formas_b.length) return true;
        return false;
    }

    function modoItem(item) {
        var m = String((item && item.precos_modo) || '').toLowerCase().replace(/-/g, '_').replace(/\s+/g, '_');
        if (m === 'grupos' || m === 'grupo' || m === '2_grupos' || m === 'dois_grupos' || m === 'ab' || m === 'a_b') {
            return 'grupos';
        }
        /* Modo explícito «por forma» manda — não voltar para A/B por lixo antigo no JSON. */
        if (m === 'por_forma' || m === 'forma' || m === 'porforma') {
            return 'por_forma';
        }
        /* Cache slim antigo / rascunho sem modo: se tem tabela A/B, trata como grupos. */
        if (gruposTemDados(precosGruposDoItem(item))) return 'grupos';
        return 'por_forma';
    }

    function precosGruposDoItem(item) {
        if (!item) return null;
        var g = item.precos_grupos;
        if (g && typeof g === 'object') return g;
        if (item.cadastro_extras && item.cadastro_extras.precos_grupos) {
            return item.cadastro_extras.precos_grupos;
        }
        return null;
    }

    /** Para busca/carrinho: { a, b } com valores > 0, ou null. */
    function precosGruposVisiveis(produto) {
        if (!produto || modoItem(produto) !== 'grupos') return null;
        var g = precosGruposDoItem(produto);
        if (!g || typeof g !== 'object') return null;
        var a = toNum(g.preco_a, 0);
        var b = toNum(g.preco_b, 0);
        if (a <= 0 && b <= 0) return null;
        return { a: a > 0 ? a : null, b: b > 0 ? b : null };
    }

    function precoBaseForma(item, forma) {
        if (!item) return 0;
        var padrao = toNum(item.preco_padrao != null ? item.preco_padrao : item.preco, 0);
        var formaTrim = String(forma || '').trim();
        if (!formaTrim) return padrao;

        /* Tabelas % globais (Fiado / Crédito etc.) — só se elegível e resolução permitir. */
        var tab = precoViaTabelaGlobal(item, formaTrim, padrao);
        if (tab != null && tab > 0) return tab;

        if (modoItem(item) === 'grupos') {
            var g = precosGruposDoItem(item);
            if (!g || typeof g !== 'object') return padrao;
            if (formaNaLista(g.formas_a, formaTrim)) {
                var pa = toNum(g.preco_a, 0);
                if (pa > 0) return pa;
            }
            if (formaNaLista(g.formas_b, formaTrim)) {
                var pb = toNum(g.preco_b, 0);
                if (pb > 0) return pb;
            }
            return padrao;
        }

        var map = item.precos_por_forma;
        if (!map || typeof map !== 'object') return padrao;
        if (Object.prototype.hasOwnProperty.call(map, formaTrim)) {
            var pf = toNum(map[formaTrim], 0);
            if (pf > 0) return pf;
        }
        /* Chaves no map podem ser canônicas; tenta match flexível. */
        var want = formaCanonKey(formaTrim);
        var keys = Object.keys(map);
        for (var ki = 0; ki < keys.length; ki++) {
            if (formaCanonKey(keys[ki]) === want) {
                var pf2 = toNum(map[keys[ki]], 0);
                if (pf2 > 0) return pf2;
            }
        }
        return padrao;
    }

    var _tabelasCache = { tabelas: [], resolucoes: {}, loaded: false };

    function setTabelasGlobais(payload) {
        _tabelasCache.tabelas = (payload && payload.tabelas) || [];
        _tabelasCache.resolucoes = (payload && payload.resolucoes) || {};
        _tabelasCache.loaded = true;
    }

    function getTabelasGlobais() {
        return _tabelasCache;
    }

    function arredondarDezenaCentavos(v) {
        var n = toNum(v, 0);
        if (n <= 0) return 0;
        var cents = Math.round(n * 100);
        var dezena = Math.floor(cents / 10);
        var resto = cents % 10;
        var out = resto <= 4 ? dezena * 10 : (dezena + 1) * 10;
        return out / 100;
    }

    function produtoElegivelTabela(t, produto) {
        if (!t || !t.ativo || !produto) return false;
        var pid = String(produto.id || produto.produto_id || '').trim();
        var vet = t.produtos_vetados || [];
        if (pid && vet.indexOf(pid) >= 0) return false;
        var cats = t.categorias_vetadas || [];
        if (cats.length) {
            var cat = String(produto.categoria || '').trim().toLowerCase();
            for (var i = 0; i < cats.length; i++) {
                if (cat && cat === String(cats[i]).toLowerCase()) return false;
            }
        }
        return Array.isArray(t.formas) && t.formas.length > 0;
    }

    function temIndividualNaForma(item, forma) {
        if (!item || !forma) return false;
        if (modoItem(item) === 'grupos') {
            var g = precosGruposDoItem(item);
            if (!g) return false;
            return formaNaLista(g.formas_a, forma) || formaNaLista(g.formas_b, forma);
        }
        var map = item.precos_por_forma;
        if (!map || typeof map !== 'object') return false;
        var want = formaCanonKey(forma);
        var keys = Object.keys(map);
        for (var i = 0; i < keys.length; i++) {
            if (formaCanonKey(keys[i]) === want && toNum(map[keys[i]], 0) > 0) return true;
        }
        return false;
    }

    function preferenciaResolucao(slot, pid) {
        var by = _tabelasCache.resolucoes && _tabelasCache.resolucoes[String(slot)];
        if (!by || typeof by !== 'object') return 'individual';
        return String(by[String(pid)] || 'individual').toLowerCase() === 'tabela'
            ? 'tabela'
            : 'individual';
    }

    function tabelaParaForma(forma, produto) {
        var formaTrim = String(forma || '').trim();
        if (!formaTrim || !_tabelasCache.tabelas.length) return null;
        var list = _tabelasCache.tabelas.slice().sort(function (a, b) {
            return toNum(a.slot, 99) - toNum(b.slot, 99);
        });
        for (var i = 0; i < list.length; i++) {
            var t = list[i];
            if (!t.ativo) continue;
            if (!formaNaLista(t.formas, formaTrim)) continue;
            if (!produtoElegivelTabela(t, produto)) continue;
            return t;
        }
        return null;
    }

    function precoViaTabelaGlobal(item, forma, padrao) {
        var t = tabelaParaForma(forma, item);
        if (!t) return null;
        var pid = String(item.id || '').trim();
        if (temIndividualNaForma(item, forma) && preferenciaResolucao(t.slot, pid) !== 'tabela') {
            return null;
        }
        var pct = toNum(t.percentual, 0);
        var out = padrao * (1 + pct / 100);
        out = Math.round(out * 100) / 100;
        if (t.arredondar_dezena_centavos) out = arredondarDezenaCentavos(out);
        return out > 0 ? out : null;
    }

    /** Chips: [{slot, nome, preco}] para tabelas elegíveis (sobre preço padrão). */
    function precosTabelasVisiveis(produto) {
        if (!produto || !_tabelasCache.tabelas.length) return [];
        if (modoItem(produto) === 'grupos' && precosGruposVisiveis(produto)) return [];
        var padrao = toNum(
            produto.preco_padrao != null
                ? produto.preco_padrao
                : produto.preco_venda != null
                  ? produto.preco_venda
                  : produto.preco,
            0
        );
        if (padrao <= 0) return [];
        var out = [];
        var list = _tabelasCache.tabelas.slice().sort(function (a, b) {
            return toNum(a.slot, 99) - toNum(b.slot, 99);
        });
        for (var i = 0; i < list.length; i++) {
            var t = list[i];
            if (!produtoElegivelTabela(t, produto)) continue;
            var pct = toNum(t.percentual, 0);
            var preco = padrao * (1 + pct / 100);
            preco = Math.round(preco * 100) / 100;
            if (t.arredondar_dezena_centavos) preco = arredondarDezenaCentavos(preco);
            if (preco > 0) {
                out.push({
                    slot: t.slot,
                    nome: String(t.nome || ('T' + t.slot)),
                    preco: preco,
                    formas: t.formas || []
                });
            }
        }
        return out;
    }

    function carregarTabelasGlobais(url) {
        var u = url || '/api/tabelas-preco-forma/pdv/';
        return fetch(u, { credentials: 'same-origin' })
            .then(function (r) { return r.json(); })
            .then(function (j) {
                if (j && j.ok) setTabelasGlobais(j);
                return j;
            })
            .catch(function () { return null; });
    }

    function copiarPrecosPorFormaDoProduto(item, produto) {
        if (!item || !produto) return item;
        if (produto.precos_por_forma && typeof produto.precos_por_forma === 'object') {
            item.precos_por_forma = Object.assign({}, produto.precos_por_forma);
        } else if (produto.cadastro_extras && produto.cadastro_extras.precos_por_forma) {
            item.precos_por_forma = Object.assign({}, produto.cadastro_extras.precos_por_forma);
        }
        var pg = produto.precos_grupos || (produto.cadastro_extras && produto.cadastro_extras.precos_grupos);
        var modoRaw = produto.precos_modo != null
            ? produto.precos_modo
            : (produto.cadastro_extras && produto.cadastro_extras.precos_modo);
        item.precos_modo = modoItem({
            precos_modo: modoRaw,
            precos_grupos: pg,
            precos_por_forma: item.precos_por_forma
        });
        if (item.precos_modo === 'grupos' && pg && typeof pg === 'object') {
            item.precos_grupos = {
                preco_a: pg.preco_a != null ? pg.preco_a : null,
                preco_b: pg.preco_b != null ? pg.preco_b : null,
                formas_a: Array.isArray(pg.formas_a) ? pg.formas_a.slice() : [],
                formas_b: Array.isArray(pg.formas_b) ? pg.formas_b.slice() : []
            };
        } else {
            try { delete item.precos_grupos; } catch (e) { item.precos_grupos = null; }
        }
        var base = toNum(produto.preco_venda != null ? produto.preco_venda : produto.preco, 0);
        if (item.preco_padrao == null) item.preco_padrao = base;
        return item;
    }

    function aplicarPrecoBaseNoItem(item, forma) {
        if (!item || item.preco_manual) return item;
        var base = precoBaseForma(item, forma);
        item.preco = base;
        return item;
    }

    function aplicarPromocaoDepoisForma(item, forma) {
        if (!item || item.preco_manual) return item;
        if (item.preco_padrao == null) item.preco_padrao = toNum(item.preco, 0);
        var baseForma = precoBaseForma(item, forma);
        if (global.AgroPdvPromocoes && global.AgroPdvPromocoes.resolvePreco) {
            item.preco = global.AgroPdvPromocoes.resolvePreco(item.id, item.qtd, baseForma);
            if (global.AgroPdvPromocoes.getPromo) item.promocao = global.AgroPdvPromocoes.getPromo(item.id);
        } else {
            item.preco = baseForma;
        }
        return item;
    }

    function aplicarNoItem(item, forma) {
        return aplicarPromocaoDepoisForma(item, forma);
    }

    function aplicarCarrinho(itens, forma) {
        if (!Array.isArray(itens)) return itens;
        if (global.AgroPdvPromocoes && global.AgroPdvPromocoes.recalcCarrinhoComForma) {
            return global.AgroPdvPromocoes.recalcCarrinhoComForma(itens, forma);
        }
        itens.forEach(function (item) {
            if (!item || item.preco_manual) return;
            aplicarNoItem(item, forma);
        });
        return itens;
    }

    global.AgroPrecosFormaPagamento = {
        toNum: toNum,
        obterFormaPagamentoAtual: obterFormaPagamentoAtual,
        obterFormaDoState: obterFormaDoState,
        precoBaseForma: precoBaseForma,
        precosGruposVisiveis: precosGruposVisiveis,
        precosTabelasVisiveis: precosTabelasVisiveis,
        setTabelasGlobais: setTabelasGlobais,
        getTabelasGlobais: getTabelasGlobais,
        carregarTabelasGlobais: carregarTabelasGlobais,
        tabelaParaForma: tabelaParaForma,
        modoItem: modoItem,
        copiarPrecosPorFormaDoProduto: copiarPrecosPorFormaDoProduto,
        aplicarPrecoBaseNoItem: aplicarPrecoBaseNoItem,
        aplicarPromocaoDepoisForma: aplicarPromocaoDepoisForma,
        aplicarNoItem: aplicarNoItem,
        aplicarCarrinho: aplicarCarrinho
    };
})(typeof window !== 'undefined' ? window : this);
