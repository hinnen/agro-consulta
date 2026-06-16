/**
 * Preço de venda por forma de pagamento (cadastro + PDV / orçamento).
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

    function precoBaseForma(item, forma) {
        if (!item) return 0;
        var padrao = toNum(item.preco_padrao != null ? item.preco_padrao : item.preco, 0);
        var formaTrim = String(forma || '').trim();
        if (!formaTrim) return padrao;
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
        copiarPrecosPorFormaDoProduto: copiarPrecosPorFormaDoProduto,
        aplicarPrecoBaseNoItem: aplicarPrecoBaseNoItem,
        aplicarPromocaoDepoisForma: aplicarPromocaoDepoisForma,
        aplicarNoItem: aplicarNoItem,
        aplicarCarrinho: aplicarCarrinho
    };
})(typeof window !== 'undefined' ? window : this);
