/**
 * Cupom de venda térmico 80mm (PDV, lista de vendas, detalhe).
 * Uso: agroImprimirCupomVenda80mm(payload) com objeto de serializar_venda_cupom_80mm.
 */
(function (global) {
    'use strict';

    function escHtml(s) {
        return String(s || '')
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    function moedaCupom(n) {
        var x = Number(n);
        if (!isFinite(x)) return '—';
        return 'R$ ' + x.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    }

    function fmtQtd(q) {
        var x = Number(q);
        if (!isFinite(x)) return String(q || '');
        if (Math.abs(x - Math.round(x)) < 0.0001) return String(Math.round(x));
        return x.toLocaleString('pt-BR', { maximumFractionDigits: 4 });
    }

    function buildCupomInnerHtml(c) {
        c = c || {};
        var itens = Array.isArray(c.itens) ? c.itens : [];
        var lines = '';
        itens.forEach(function (it) {
            var q = Number(it.qtd != null ? it.qtd : 0);
            var sub = it.subtotal != null ? Number(it.subtotal) : q * Number(it.preco != null ? it.preco : 0);
            var subTxt = isFinite(sub) ? moedaCupom(sub) : '—';
            var nome = String(it.nome || '').slice(0, 42);
            lines +=
                '<div style="display:flex;justify-content:space-between;gap:4px;margin:3px 0;font-size:10px;line-height:1.25;">' +
                '<span style="flex:1;">' +
                escHtml(fmtQtd(q) + '× ' + nome) +
                '</span><span style="white-space:nowrap;font-weight:700;">' +
                escHtml(subTxt) +
                '</span></div>';
        });
        var h = '<div class="pg">';
        h += '<div style="text-align:center;font-weight:900;font-size:14px;letter-spacing:.04em;">SISVALE</div>';
        h += '<div style="text-align:center;font-size:10px;margin:2px 0;">Cupom de venda (não fiscal)</div>';
        if (c.criado_em) {
            h += '<div style="font-size:11px;font-weight:800;margin-top:4px;">Data: ' + escHtml(c.criado_em) + '</div>';
        }
        if (c.venda_id) {
            h += '<div style="font-size:10px;font-weight:700;">Venda #' + escHtml(String(c.venda_id)) + '</div>';
        }
        if (c.segunda_via) {
            h += '<div style="text-align:center;font-size:10px;font-weight:900;margin:4px 0;border:1px dashed #000;padding:3px;">2ª VIA</div>';
        }
        if (c.devolvida) {
            h += '<div style="text-align:center;font-size:10px;font-weight:900;margin:4px 0;color:#b91c1c;">*** DEVOLVIDA ***</div>';
        }
        h += '<div style="border-top:1px dashed #000;margin:6px 0;"></div>';
        h += '<div style="font-weight:bold;font-size:11px;margin-bottom:4px;word-break:break-word;">' + escHtml(c.cliente_nome || '—') + '</div>';
        h += lines;
        h += '<div style="border-top:2px solid #000;margin:8px 0 4px;padding-top:4px;font-weight:900;font-size:13px;display:flex;justify-content:space-between;">';
        h += '<span>TOTAL</span><span>' + escHtml(c.total_texto || moedaCupom(c.total)) + '</span></div>';
        if (c.forma_pagamento) {
            h += '<div style="font-size:10px;margin-top:4px;word-break:break-word;"><strong>Pag.:</strong> ' + escHtml(c.forma_pagamento) + '</div>';
        }
        if (c.operador) {
            h += '<div style="font-size:9px;margin-top:3px;color:#334155;">Operador: ' + escHtml(c.operador) + '</div>';
        }
        if (c.caixa_id) {
            h += '<div style="font-size:9px;color:#334155;">Caixa #' + escHtml(String(c.caixa_id)) + '</div>';
        }
        h += '<div style="text-align:center;font-size:9px;margin-top:10px;">Obrigado pela preferência</div>';
        h += '</div>';
        return h;
    }

    function buildCupomDocumentHtml(c) {
        return (
            '<!DOCTYPE html><html><head><meta charset="utf-8"><title>Cupom venda</title><style>' +
            '@page{margin:0;size:80mm auto}' +
            'html,body{margin:0;padding:0}' +
            'body{font-family:system-ui,Segoe UI,sans-serif;-webkit-print-color-adjust:exact;print-color-adjust:exact}' +
            '.pg{width:72mm;margin:0 auto;padding:4mm 3mm;font-size:11px;line-height:1.35;box-sizing:border-box}' +
            '</style></head><body>' +
            buildCupomInnerHtml(c) +
            '</body></html>'
        );
    }

    function agroImprimirCupomVenda80mm(c) {
        if (!c || (!c.itens || !c.itens.length)) {
            alert('Não há itens para imprimir nesta venda.');
            return false;
        }
        var iframe = document.getElementById('agro-print-iframe-cupom-venda');
        if (!iframe) {
            iframe = document.createElement('iframe');
            iframe.id = 'agro-print-iframe-cupom-venda';
            iframe.title = 'Impressão cupom venda';
            iframe.setAttribute('aria-hidden', 'true');
            iframe.style.cssText =
                'position:fixed;right:0;bottom:0;width:0;height:0;border:0;opacity:0;pointer-events:none;';
            document.body.appendChild(iframe);
        }
        var idoc = iframe.contentDocument || iframe.contentWindow.document;
        idoc.open();
        idoc.write(buildCupomDocumentHtml(c));
        idoc.close();
        setTimeout(function () {
            try {
                iframe.contentWindow.focus();
                iframe.contentWindow.print();
            } catch (errP) {
                alert('Não foi possível abrir a impressão. Verifique a impressora térmica 80mm.');
            }
        }, 280);
        return true;
    }

    function agroCarregarEImprimirCupomVenda(vendaId, opts) {
        opts = opts || {};
        var id = parseInt(vendaId, 10);
        if (!id) return Promise.reject(new Error('Venda inválida.'));
        var qs = opts.segunda_via === false ? '?segunda_via=0' : '';
        if (window.gmLoadingBar) window.gmLoadingBar.show();
        return fetch('/venda/' + id + '/cupom/' + qs, {
            credentials: 'same-origin',
            headers: { Accept: 'application/json' }
        })
            .then(function (r) {
                return r.json().then(function (d) {
                    if (!r.ok || !d.ok) throw new Error((d && d.erro) || 'Falha ao carregar cupom.');
                    return d.cupom;
                });
            })
            .then(function (cupom) {
                agroImprimirCupomVenda80mm(cupom);
            })
            .finally(function () {
                if (window.gmLoadingBar) window.gmLoadingBar.hide();
            });
    }

    global.agroImprimirCupomVenda80mm = agroImprimirCupomVenda80mm;
    global.agroCarregarEImprimirCupomVenda = agroCarregarEImprimirCupomVenda;
    global.agroBuildCupomVenda80mmHtml = buildCupomDocumentHtml;
})(typeof window !== 'undefined' ? window : globalThis);
