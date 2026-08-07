/**
 * Campanha PDV (inauguração Vila — % automático).
 * Preço de tela = pós-promo × fator. No envio, manda preço SEM campanha + campanha_id
 * para o servidor aplicar (fonte da verdade / NFC-e / caixa).
 */
(function (global) {
    'use strict';

    var regra = null;
    var depositoAtual = 'centro';

    function toNum(v, fb) {
        var n = parseFloat(v);
        return isFinite(n) ? n : fb != null ? fb : 0;
    }

    /**
     * boot = { ativa, regra, depositoAtual } ou regra direta (legado).
     */
    function setBootstrap(boot, deposito) {
        if (!boot) {
            regra = null;
            depositoAtual = String(deposito || 'centro').toLowerCase();
            return;
        }
        if (boot.regra) {
            regra = boot.regra;
            depositoAtual = String(
                deposito || boot.depositoAtual || 'centro'
            ).toLowerCase();
            return;
        }
        if (boot.id) {
            regra = boot;
            depositoAtual = String(deposito || boot.deposito || 'centro').toLowerCase();
            return;
        }
        regra = null;
        depositoAtual = String(deposito || 'centro').toLowerCase();
    }

    function setDeposito(dep) {
        depositoAtual = String(dep || 'centro').toLowerCase();
    }

    function getConfig() {
        return ativa() ? Object.assign({ depositoAtual: depositoAtual }, regra) : null;
    }

    function ativa() {
        if (!regra || !regra.id) return false;
        var depAlvo = String(regra.deposito || 'vila').toLowerCase();
        if (depositoAtual !== depAlvo) return false;
        var f = toNum(regra.fator, 0);
        return f > 0 && f < 1;
    }

    function fator() {
        var f = toNum(regra && regra.fator, 1);
        return f > 0 && f < 1 ? f : 1;
    }

    function id() {
        return regra && regra.id ? String(regra.id) : '';
    }

    function rotulo() {
        return (regra && regra.rotulo) || 'Campanha ativa';
    }

    function percentual() {
        return toNum(regra && regra.percentual, 0);
    }

    /** Depois do recalc de promo/forma: grava base e aplica fator na tela. */
    function aplicarNosItens(itens) {
        if (!Array.isArray(itens)) return itens;
        var on = ativa();
        var f = fator();
        itens.forEach(function (item) {
            if (!item) return;
            var base = toNum(item.preco, 0);
            item.preco_pos_promo = base;
            if (on) {
                item.preco = Math.round(base * f * 10000) / 10000;
                item.campanha_id = id();
                item.campanha_pct = percentual();
            } else {
                delete item.campanha_id;
                delete item.campanha_pct;
            }
        });
        return itens;
    }

    function precoEnvioItem(item) {
        if (!item) return 0;
        if (ativa() && item.preco_pos_promo != null) {
            return toNum(item.preco_pos_promo, toNum(item.preco, 0));
        }
        return toNum(item.preco, 0);
    }

    function metaPayload() {
        if (!ativa()) return null;
        return { campanha_id: id(), campanha_pct: percentual() };
    }

    function atualizarFaixaUi() {
        var el = document.getElementById('pdv-campanha-faixa');
        if (!el) return;
        if (ativa()) {
            el.classList.remove('hidden');
            el.textContent = rotulo() + (regra && regra.teste ? ' (teste)' : '');
        } else {
            el.classList.add('hidden');
            el.textContent = '';
        }
    }

    global.AgroPdvCampanha = {
        setBootstrap: setBootstrap,
        setConfig: setBootstrap,
        setDeposito: setDeposito,
        getConfig: getConfig,
        ativa: ativa,
        fator: fator,
        id: id,
        rotulo: rotulo,
        percentual: percentual,
        aplicarNosItens: aplicarNosItens,
        precoEnvioItem: precoEnvioItem,
        metaPayload: metaPayload,
        atualizarFaixaUi: atualizarFaixaUi,
    };
})(typeof window !== 'undefined' ? window : this);
