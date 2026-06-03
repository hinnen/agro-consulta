/**
 * Cupom de venda térmico 80mm (PDV, lista de vendas, detalhe).
 * Uso: agroImprimirCupomVenda80mm(payload) com objeto de serializar_venda_cupom_80mm.
 */
(function (global) {
    'use strict';

    var CUPOM_TITULO_LOJA = 'Gm Agro Mais';
    var CUPOM_RODAPE_SISTEMA = 'SISTVALE';

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

    function isFiadoCupom(c) {
        if (c && c.eh_fiado) return true;
        return /fiado/i.test(String((c && c.forma_pagamento) || ''));
    }

    function parseDataCupomBr(str) {
        var s = String(str || '').trim();
        var m = s.match(/(\d{2})\/(\d{2})\/(\d{4})/);
        if (!m) return new Date();
        return new Date(parseInt(m[3], 10), parseInt(m[2], 10) - 1, parseInt(m[1], 10));
    }

    function formatDataBr(d) {
        if (!(d instanceof Date) || isNaN(d.getTime())) return '—';
        return d.toLocaleDateString('pt-BR', { day: '2-digit', month: '2-digit', year: 'numeric' });
    }

    function vencimentoCupom(c) {
        c = c || {};
        if (c.vencimento) return String(c.vencimento);
        var dias = parseInt(c.fiado_dias, 10);
        if (!isFinite(dias) || dias < 1) dias = 30;
        var base = parseDataCupomBr(c.criado_em);
        base.setDate(base.getDate() + dias);
        return formatDataBr(base);
    }

    function cupomStyles() {
        return (
            '@page{margin:0;size:80mm auto}' +
            'html,body{margin:0;padding:0}' +
            'body{font-family:system-ui,Segoe UI,sans-serif;-webkit-print-color-adjust:exact;print-color-adjust:exact}' +
            '.pg{width:78mm;max-width:78mm;margin:0 auto;padding:1.5mm 1mm;font-size:11px;line-height:1.3;box-sizing:border-box}' +
            '.titulo-loja{text-align:center;font-weight:900;font-size:16px;letter-spacing:.02em;line-height:1.15}' +
            '.nome-cliente{font-weight:900;font-size:17px;line-height:1.2;word-break:break-word;margin:6px 0 4px}' +
            '.total-linha{border-top:2px solid #000;margin:8px 0 4px;padding-top:4px;font-weight:900;font-size:17px;display:flex;justify-content:space-between;align-items:baseline;gap:4px}' +
            '.assinatura{margin-top:10px;border:2px solid #000;min-height:26mm;padding:4px 4px 2px;box-sizing:border-box}' +
            '.assinatura-titulo{font-size:9px;font-weight:800;text-transform:uppercase;letter-spacing:.04em;margin-bottom:2px}' +
            '.assinatura-area{min-height:20mm}' +
            '.rodape-sistvale{text-align:center;font-size:8px;font-weight:800;letter-spacing:.12em;margin-top:6px;color:#111}'
        );
    }

    function buildCupomInnerHtml(c) {
        c = c || {};
        var fiado = isFiadoCupom(c);
        var itens = Array.isArray(c.itens) ? c.itens : [];
        var lines = '';
        itens.forEach(function (it) {
            var q = Number(it.qtd != null ? it.qtd : 0);
            var sub = it.subtotal != null ? Number(it.subtotal) : q * Number(it.preco != null ? it.preco : 0);
            var subTxt = isFinite(sub) ? moedaCupom(sub) : '—';
            var nome = String(it.nome || '').slice(0, 48);
            lines +=
                '<div style="display:flex;justify-content:space-between;gap:3px;margin:2px 0;font-size:10px;line-height:1.22;">' +
                '<span style="flex:1;min-width:0;">' +
                escHtml(fmtQtd(q) + '× ' + nome) +
                '</span><span style="white-space:nowrap;font-weight:700;flex-shrink:0;">' +
                escHtml(subTxt) +
                '</span></div>';
        });

        var logoUrl = (global.AGRO_CUPOM_LOGO_URL || c.logo_url || '').trim();
        var h = '<div class="pg">';
        if (logoUrl) {
            h +=
                '<div style="text-align:center;margin-bottom:4px">' +
                '<img src="' +
                escHtml(logoUrl) +
                '" alt="" style="max-width:72mm;height:auto;display:block;margin:0 auto">' +
                '</div>';
        }
        h += '<div class="titulo-loja">' + escHtml(CUPOM_TITULO_LOJA) + '</div>';
        if (fiado) {
            h += '<div style="text-align:center;font-size:10px;font-weight:800;margin:3px 0 2px;letter-spacing:.06em;">COMPROVANTE FIADO</div>';
        } else {
            h += '<div style="text-align:center;font-size:10px;font-weight:800;margin:3px 0 2px;letter-spacing:.04em;">COMPROVANTE DE VENDA</div>';
        }
        if (c.criado_em) {
            h += '<div style="font-size:10px;font-weight:800;margin-top:4px;">Data: ' + escHtml(c.criado_em) + '</div>';
        }
        if (fiado) {
            h +=
                '<div style="font-size:12px;font-weight:900;margin-top:3px;">Vencimento: ' +
                escHtml(vencimentoCupom(c)) +
                '</div>';
        }
        if (c.venda_id) {
            h += '<div style="font-size:9px;font-weight:700;margin-top:2px;">Venda #' + escHtml(String(c.venda_id)) + '</div>';
        }
        if (c.segunda_via) {
            h += '<div style="text-align:center;font-size:10px;font-weight:900;margin:4px 0;border:1px dashed #000;padding:3px;">2ª VIA</div>';
        }
        if (c.devolvida) {
            h += '<div style="text-align:center;font-size:10px;font-weight:900;margin:4px 0;color:#b91c1c;">*** DEVOLVIDA ***</div>';
        }
        h += '<div style="border-top:1px dashed #000;margin:5px 0 4px;"></div>';
        h += '<div class="nome-cliente">' + escHtml(c.cliente_nome || '—') + '</div>';
        h += lines;
        h += '<div class="total-linha"><span>TOTAL</span><span>' + escHtml(c.total_texto || moedaCupom(c.total)) + '</span></div>';
        if (c.forma_pagamento) {
            h +=
                '<div style="font-size:10px;margin-top:3px;word-break:break-word;font-weight:700;"><strong>Pag.:</strong> ' +
                escHtml(c.forma_pagamento) +
                '</div>';
        }
        if (c.operador) {
            h += '<div style="font-size:9px;margin-top:3px;color:#334155;">Operador: ' + escHtml(c.operador) + '</div>';
        }
        if (c.caixa_id) {
            h += '<div style="font-size:9px;color:#334155;">Caixa #' + escHtml(String(c.caixa_id)) + '</div>';
        }
        if (fiado) {
            h +=
                '<div class="assinatura">' +
                '<div class="assinatura-titulo">Assinatura do cliente</div>' +
                '<div class="assinatura-area"></div>' +
                '</div>';
        }
        h += '<div style="text-align:center;font-size:9px;margin-top:8px;">Obrigado pela preferência</div>';
        h += '<div class="rodape-sistvale">' + escHtml(CUPOM_RODAPE_SISTEMA) + '</div>';
        h += '</div>';
        return h;
    }

    function buildCupomDocumentHtml(c) {
        return (
            '<!DOCTYPE html><html><head><meta charset="utf-8"><title>Cupom venda</title><style>' +
            cupomStyles() +
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
