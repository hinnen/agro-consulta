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

    function formaNaLista(lista, forma) {
        if (!Array.isArray(lista) || !forma) return false;
        var f = String(forma).trim().toLowerCase();
        for (var i = 0; i < lista.length; i++) {
            if (String(lista[i] || '').trim().toLowerCase() === f) return true;
        }
        return false;
    }

    function modoItem(item) {
        var m = String((item && item.precos_modo) || 'por_forma').toLowerCase();
        return m === 'grupos' ? 'grupos' : 'por_forma';
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
        return padrao;
    }

    function copiarPrecosPorFormaDoProduto(item, produto) {
        if (!item || !produto) return item;
        if (produto.precos_por_forma && typeof produto.precos_por_forma === 'object') {
            item.precos_por_forma = Object.assign({}, produto.precos_por_forma);
        } else if (produto.cadastro_extras && produto.cadastro_extras.precos_por_forma) {
            item.precos_por_forma = Object.assign({}, produto.cadastro_extras.precos_por_forma);
        }
        var modo = String(produto.precos_modo || (produto.cadastro_extras && produto.cadastro_extras.precos_modo) || 'por_forma');
        item.precos_modo = String(modo).toLowerCase() === 'grupos' ? 'grupos' : 'por_forma';
        var pg = produto.precos_grupos || (produto.cadastro_extras && produto.cadastro_extras.precos_grupos);
        if (pg && typeof pg === 'object') {
            item.precos_grupos = {
                preco_a: pg.preco_a != null ? pg.preco_a : null,
                preco_b: pg.preco_b != null ? pg.preco_b : null,
                formas_a: Array.isArray(pg.formas_a) ? pg.formas_a.slice() : [],
                formas_b: Array.isArray(pg.formas_b) ? pg.formas_b.slice() : []
            };
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
        modoItem: modoItem,
        copiarPrecosPorFormaDoProduto: copiarPrecosPorFormaDoProduto,
        aplicarPrecoBaseNoItem: aplicarPrecoBaseNoItem,
        aplicarPromocaoDepoisForma: aplicarPromocaoDepoisForma,
        aplicarNoItem: aplicarNoItem,
        aplicarCarrinho: aplicarCarrinho
    };
})(typeof window !== 'undefined' ? window : this);
