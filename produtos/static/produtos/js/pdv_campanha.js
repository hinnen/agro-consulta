/**
 * Campanha PDV (inauguração Vila — % automático).
 * Regra: menor entre (preço pós-promo) e (preço base × fator). Não soma os dois.
 * No envio: preco = pós-promo, preco_base = lista/forma; servidor reaplica o min.
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
            depositoAtual = normalizarDepositoJs(
                deposito || boot.depositoAtual || 'centro'
            );
            return;
        }
        if (boot.id) {
            regra = boot;
            depositoAtual = normalizarDepositoJs(deposito || boot.deposito || 'centro');
            return;
        }
        regra = null;
        depositoAtual = normalizarDepositoJs(deposito || 'centro');
    }

    function normalizarDepositoJs(dep) {
        var d = String(dep || '').trim().toLowerCase();
        if (d === '2' || d.indexOf('vila') !== -1) return 'vila';
        if (d === '1' || d.indexOf('centro') !== -1) return 'centro';
        return d || 'centro';
    }

    function setDeposito(dep) {
        depositoAtual = normalizarDepositoJs(dep);
    }

    function getConfig() {
        return ativa() ? Object.assign({ depositoAtual: depositoAtual }, regra) : null;
    }

    function ativa() {
        if (!regra || !regra.id) return false;
        var f = toNum(regra.fator, 0);
        if (!(f > 0 && f < 1)) return false;
        var depAlvo = normalizarDepositoJs(regra.deposito || 'vila');
        var dep = normalizarDepositoJs(depositoAtual);
        if (dep === depAlvo) return true;
        /* Caixa Vila no rotulo, mesmo se o cookie de deposito vier errado */
        try {
            var cx = (window.AgroPdvWizardBootstrap && window.AgroPdvWizardBootstrap.caixa) || {};
            var rot = String(cx.rotulo || cx.pontoOperacao || '').toLowerCase();
            if (depAlvo === 'vila' && rot.indexOf('vila') !== -1) return true;
        } catch (e1) {}
        return false;
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

    /** Múltiplo de R$ 0,05 mais próximo. */
    function arredondar5Centavos(v) {
        var n = toNum(v, 0);
        if (n <= 0) return 0;
        var out = Math.round(n / 0.05) * 0.05;
        out = Math.round(out * 100) / 100;
        return out > 0 ? out : 0.05;
    }

    function precoBaseItem(item) {
        if (!item) return 0;
        if (item.preco_base_forma != null) return toNum(item.preco_base_forma, 0);
        if (item.preco_padrao != null) return toNum(item.preco_padrao, 0);
        return toNum(item.preco, 0);
    }

    /** Depois do recalc de promo/forma: cobra o menor (promo vs base×fator). */
    function aplicarNosItens(itens) {
        if (!Array.isArray(itens)) return itens;
        var on = ativa();
        var f = fator();
        itens.forEach(function (item) {
            if (!item) return;
            var aposPromo = toNum(item.preco, 0);
            item.preco_pos_promo = aposPromo;
            if (on) {
                var base = precoBaseItem(item);
                if (base <= 0) base = aposPromo;
                var comCampanha = Math.round(base * f * 10000) / 10000;
                var finalP = aposPromo;
                var usouCampanha = false;
                if (comCampanha > 0 && (finalP <= 0 || comCampanha < finalP)) {
                    finalP = comCampanha;
                    usouCampanha = true;
                }
                if (usouCampanha) {
                    finalP = arredondar5Centavos(finalP);
                } else {
                    finalP = Math.round(finalP * 100) / 100;
                }
                item.preco = finalP;
                item.campanha_id = id();
                item.campanha_pct = percentual();
                item.campanha_usou = usouCampanha ? 'campanha' : 'promo_ou_lista';
            } else {
                delete item.campanha_id;
                delete item.campanha_pct;
                delete item.campanha_usou;
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

    function precoBaseEnvioItem(item) {
        return precoBaseItem(item);
    }

    function metaPayload() {
        if (!ativa()) return null;
        return {
            campanha_id: id(),
            campanha_pct: percentual(),
            campanha_modo: 'menor',
        };
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
        precoBaseEnvioItem: precoBaseEnvioItem,
        metaPayload: metaPayload,
        atualizarFaixaUi: atualizarFaixaUi,
    };
})(typeof window !== 'undefined' ? window : this);
