/**
 * Cupom de venda térmico 80mm (PDV, lista de vendas, detalhe, orçamento entrega).
 */
(function (global) {
    'use strict';

    var CUPOM_ZAP_TEXTO = '13 9 9767-3389';
    var CUPOM_RODAPE_SISTEMA = 'SISTVALE';

    function escHtml(s) {
        return String(s || '')
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    var _jsBarcodePromise = null;

    function cupomEnsureJsBarcode(cb) {
        cb = typeof cb === 'function' ? cb : function () {};
        if (global.JsBarcode) {
            cb();
            return Promise.resolve();
        }
        if (!_jsBarcodePromise) {
            _jsBarcodePromise = new Promise(function (resolve) {
                var s = document.createElement('script');
                s.src = 'https://cdn.jsdelivr.net/npm/jsbarcode@3.11.5/dist/JsBarcode.all.min.js';
                s.async = true;
                s.onload = function () {
                    resolve();
                };
                s.onerror = function () {
                    resolve();
                };
                (document.head || document.documentElement).appendChild(s);
            });
        }
        return _jsBarcodePromise.then(cb);
    }

    function cupomPreloadRecursos() {
        cupomEnsureJsBarcode(function () {});
        try {
            var url = cupomLogoUrl();
            if (url) {
                var img = new Image();
                img.decoding = 'async';
                img.src = url;
            }
        } catch (ePre) {}
    }

    function cupomLogoUrl() {
        var u = String(global.AGRO_CUPOM_LOGO_URL || '').trim();
        if (!u) u = '/static/produtos/img/logo_termica.png';
        if (/^https?:\/\//i.test(u) || /^data:/i.test(u)) return u;
        try {
            var base = global.location && global.location.origin ? global.location.origin : '';
            if (base) return base + (u.charAt(0) === '/' ? u : '/' + u);
        } catch (eUrl) {}
        return u;
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

    function cupomEnsureFreteItem(c, itensRaw) {
        var itens = Array.isArray(itensRaw) ? itensRaw.slice() : [];
        var frete = Number((c && c.frete) || 0);
        if (!isFinite(frete) || frete <= 0.009) return itens;
        var jaTemFrete = itens.some(function (it) {
            if (!it || typeof it !== 'object') return false;
            if (it.eh_frete) return true;
            var nome = String(it.nome || '').toLowerCase();
            return nome.indexOf('taxa de entrega') >= 0 || nome === 'frete';
        });
        if (jaTemFrete) return itens;
        itens.push({
            nome: 'Taxa de entrega',
            qtd: 1,
            preco: frete,
            subtotal: frete,
            eh_frete: true
        });
        return itens;
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
            'body{font-family:system-ui,Segoe UI,sans-serif;-webkit-print-color-adjust:exact;print-color-adjust:exact;width:80mm}' +
            '.pg{width:80mm;max-width:80mm;margin:0 auto;padding:0;font-size:11px;line-height:1.28;box-sizing:border-box;page-break-inside:avoid;break-inside:avoid-page;overflow:visible}' +
            '.pg + .pg{page-break-before:always;break-before:page;margin-top:0}' +
            '.pg-avanco-corte{display:block;height:14mm;min-height:14mm;line-height:14mm;font-size:1px;color:transparent;overflow:hidden;margin:0;padding:0;user-select:none}' +
            '@media print{.pg{page-break-inside:avoid;break-inside:avoid-page}.pg + .pg{page-break-before:always;break-before:page}.pg-avanco-corte{display:block;height:14mm;min-height:14mm}}' +
            '.cupom-cabecalho{width:100%;margin:0;padding:0;text-align:center}' +
            '.cupom-logo{width:100%;margin:0;padding:0;line-height:0;display:block}' +
            '.cupom-logo img{width:100%;max-width:100%;height:auto;display:block;margin:0 auto;padding:0;border:0;vertical-align:top;image-rendering:crisp-edges;-webkit-print-color-adjust:exact;print-color-adjust:exact}' +
            '.cupom-zap{width:100%;display:flex;align-items:center;justify-content:center;gap:7px;margin:4px 0 2px;padding:0 1mm;box-sizing:border-box;font-size:16px;font-weight:900;line-height:1.1;letter-spacing:.01em}' +
            '.cupom-zap-ico{width:20px;height:20px;flex-shrink:0;display:block}' +
            '.via-rotulo{text-align:center;font-size:11px;font-weight:900;margin:5px 0 4px;border:2px solid #000;padding:4px 6px;letter-spacing:.06em}' +
            '.nome-cliente{font-weight:900;font-size:32px;line-height:1.15;word-break:break-word;overflow-wrap:break-word;text-align:center;white-space:pre-wrap;margin:8px 0 6px;letter-spacing:-0.01em}' +
            '.total-linha{border-top:3px solid #000;margin:10px 0 6px;padding-top:6px;font-weight:900;font-size:22px;display:flex;justify-content:space-between;align-items:baseline;gap:4px}' +
            '.total-linha .total-valor{font-size:38px;line-height:1;letter-spacing:-0.03em}' +
            '.total-linha span:first-child{font-size:22px}' +
            '.assinatura{margin-top:12px;border:2px solid #000;min-height:28mm;padding:5px 4px 3px;box-sizing:border-box}' +
            '.assinatura-titulo{font-size:10px;font-weight:800;text-transform:uppercase;letter-spacing:.05em;margin-bottom:3px}' +
            '.assinatura-area{min-height:22mm}' +
            '.rodape-sistvale{text-align:center;font-size:11px;font-weight:900;letter-spacing:.16em;margin-top:10px;padding:5px 4px 4px;background:#000;color:#fff;-webkit-print-color-adjust:exact;print-color-adjust:exact}'
        );
    }

    function cupomPgCorteHtml() {
        return '<div class="pg-avanco-corte" aria-hidden="true">&nbsp;</div>';
    }

    function cupomLogoHtml() {
        var url = cupomLogoUrl();
        if (!url) return '';
        return '<div class="cupom-logo"><img src="' + escHtml(url) + '" alt="Agro Mais"></div>';
    }

    function cupomCabecalhoHtml() {
        var logo = cupomLogoHtml();
        var zap = cupomZapHtml();
        if (!logo && !zap) return '';
        return '<div class="cupom-cabecalho">' + logo + zap + '</div>';
    }

    function cupomZapHtml() {
        var tel = String(global.AGRO_CUPOM_ZAP_TEXTO || CUPOM_ZAP_TEXTO || '').trim();
        if (!tel) return '';
        return (
            '<div class="cupom-zap">' +
            '<svg class="cupom-zap-ico" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" aria-hidden="true">' +
            '<path fill="#000" d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.67.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.57-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347m-5.421 7.403h-.004a9.87 9.87 0 01-5.031-1.378l-.361-.214-3.741.982.998-3.648-.235-.374a9.86 9.86 0 01-1.51-5.26c.001-5.45 4.436-9.884 9.888-9.884 2.64 0 5.122 1.03 6.988 2.898a9.825 9.825 0 012.893 6.994c-.003 5.45-4.435 9.884-9.881 9.884m8.413-18.297A11.815 11.815 0 0012.05 0C5.495 0 .16 5.335.157 11.892c0 2.096.547 4.142 1.588 5.945L.057 24l6.305-1.654a11.882 11.882 0 005.683 1.448h.005c6.554 0 11.89-5.335 11.893-11.893a11.821 11.821 0 00-3.48-8.413z"/>' +
            '</svg>' +
            '<span>' +
            escHtml(tel) +
            '</span></div>'
        );
    }

    function cupomNomeClienteHtml(nome) {
        return '<div class="nome-cliente">' + escHtml(nome || '—') + '</div>';
    }

    function cupomRodapeSistvaleHtml() {
        return '<div class="rodape-sistvale">' + escHtml(CUPOM_RODAPE_SISTEMA) + '</div>';
    }

    function buildCupomInnerHtml(c) {
        c = c || {};
        if (c.tipo === 'nfce') {
            return buildNfceCupomInnerHtml(c);
        }
        var fiado = isFiadoCupom(c);
        var subtitulo =
            c.subtitulo ||
            (fiado ? 'COMPROVANTE FIADO' : 'COMPROVANTE DE VENDA');
        var itens = cupomEnsureFreteItem(c, c.itens);
        var lines = '';
        itens.forEach(function (it) {
            var q = Number(it.qtd != null ? it.qtd : 0);
            var sub = it.subtotal != null ? Number(it.subtotal) : q * Number(it.preco != null ? it.preco : 0);
            var subTxt = isFinite(sub) ? moedaCupom(sub) : '—';
            var nome = String(it.nome || '').slice(0, 52);
            var devTotal = !!it.devolvido_total;
            var devParc = !!it.devolvido_parcial;
            var risco = devTotal ? 'text-decoration:line-through;opacity:.72;' : '';
            var rotulo = fmtQtd(q) + '× ' + nome;
            if (devParc) {
                var qRest = Number(it.qtd_restante != null ? it.qtd_restante : q);
                rotulo = fmtQtd(qRest) + '× ' + nome + ' (parc. devolvido)';
            } else if (devTotal) {
                rotulo = fmtQtd(q) + '× ' + nome + ' (devolvido)';
            }
            lines +=
                '<div style="display:flex;justify-content:space-between;gap:2px;margin:3px 0;font-size:11px;line-height:1.22;' +
                risco +
                '">' +
                '<span style="flex:1;min-width:0;">' +
                escHtml(rotulo) +
                '</span><span style="white-space:nowrap;font-weight:800;flex-shrink:0;font-size:11px;">' +
                escHtml(subTxt) +
                '</span></div>';
        });

        var h = '<div class="pg">';
        if (c.mostrar_cabecalho !== false) {
            h += cupomCabecalhoHtml();
        }
        h += '<div style="text-align:center;font-size:10px;font-weight:800;margin:4px 0 3px;letter-spacing:.05em;">' + escHtml(subtitulo) + '</div>';
        if (c.criado_em) {
            h += '<div style="font-size:10px;font-weight:800;margin-top:3px;">Data: ' + escHtml(c.criado_em) + '</div>';
        }
        if (fiado) {
            h +=
                '<div style="font-size:14px;font-weight:900;margin-top:4px;">Vencimento: ' +
                escHtml(vencimentoCupom(c)) +
                '</div>';
        }
        if (c.venda_id) {
            h += '<div style="font-size:9px;font-weight:700;margin-top:2px;">Venda #' + escHtml(String(c.venda_id)) + '</div>';
        }
        if (c.via_rotulo) {
            h += '<div class="via-rotulo">' + escHtml(String(c.via_rotulo)) + '</div>';
        } else if (c.segunda_via) {
            h += '<div style="text-align:center;font-size:10px;font-weight:900;margin:4px 0;border:1px dashed #000;padding:3px;">2ª VIA</div>';
        }
        if (c.devolvida) {
            h += '<div style="text-align:center;font-size:10px;font-weight:900;margin:4px 0;color:#b91c1c;">*** DEVOLVIDA ***</div>';
        } else if (c.tem_devolucao_parcial) {
            h += '<div style="text-align:center;font-size:10px;font-weight:900;margin:4px 0;border:1px dashed #000;padding:3px;">DEVOLUÇÃO PARCIAL</div>';
        }
        h += '<div style="border-top:1px dashed #000;margin:6px 0 4px;"></div>';
        h += cupomNomeClienteHtml(c.cliente_nome);
        if (c.telefone) {
            h +=
                '<div style="font-size:11px;font-weight:800;margin-top:3px;">Tel ' +
                escHtml(c.telefone) +
                '</div>';
        }
        if (c.endereco_linha) {
            var endGrande = c.endereco_grande !== false && c.endereco_tamanho !== 'normal';
            h +=
                '<div style="' +
                (endGrande
                    ? 'font-size:16px;font-weight:900;line-height:1.28;word-break:break-word;margin:5px 0 8px;'
                    : 'font-size:11px;font-weight:700;line-height:1.35;word-break:break-word;margin:4px 0 8px;') +
                '">' +
                escHtml(c.endereco_linha) +
                '</div>';
        }
        h += lines;
        h +=
            '<div class="total-linha"><span>TOTAL</span><span class="total-valor">' +
            escHtml(c.total_texto || moedaCupom(c.total)) +
            '</span></div>';
        if (c.tem_devolucao_parcial && c.total_original_texto) {
            h +=
                '<div style="font-size:9px;font-weight:700;text-align:right;margin-top:2px;text-decoration:line-through;opacity:.7;">Original ' +
                escHtml(c.total_original_texto) +
                '</div>';
        }
        if (c.forma_pagamento) {
            h +=
                '<div style="font-size:11px;margin-top:4px;word-break:break-word;font-weight:800;"><strong>Pag.:</strong> ' +
                escHtml(c.forma_pagamento) +
                '</div>';
        }
        if (c.operador) {
            h += '<div style="font-size:9px;margin-top:3px;color:#334155;">Operador: ' + escHtml(c.operador) + '</div>';
        }
        if (c.caixa_id) {
            h += '<div style="font-size:9px;color:#334155;">Caixa #' + escHtml(String(c.caixa_id)) + '</div>';
        }
        if (c.rodape_extra) {
            h += '<div style="font-size:9px;margin-top:6px;line-height:1.35;word-break:break-word;">' + c.rodape_extra + '</div>';
        }
        if (fiado && c.com_assinatura !== false) {
            h +=
                '<div class="assinatura">' +
                '<div class="assinatura-titulo">Assinatura do cliente</div>' +
                '<div class="assinatura-area"></div>' +
                '</div>';
        }
        h += '<div style="text-align:center;font-size:10px;margin-top:10px;font-weight:600;">Obrigado pela preferência</div>';
        h += cupomRodapeSistvaleHtml();
        h += cupomPgCorteHtml();
        h += '</div>';
        return h;
    }

    function formatChaveNfce(chave) {
        var s = String(chave || '').replace(/\D/g, '');
        if (s.length !== 44) return s;
        var out = [];
        var i;
        for (i = 0; i < s.length; i += 4) {
            out.push(s.slice(i, i + 4));
        }
        return out.join(' ');
    }

    function buildNfceCupomInnerHtml(c) {
        c = c || {};
        var itens = cupomEnsureFreteItem(c, c.itens);
        var lines = '';
        itens.forEach(function (it) {
            var q = Number(it.qtd != null ? it.qtd : 0);
            var sub = it.subtotal != null ? Number(it.subtotal) : q * Number(it.preco != null ? it.preco : 0);
            lines +=
                '<div style="display:flex;justify-content:space-between;gap:2px;margin:3px 0;font-size:11px;line-height:1.22;">' +
                '<span style="flex:1;min-width:0;">' +
                escHtml(fmtQtd(q) + '× ' + String(it.nome || '').slice(0, 48)) +
                '</span><span style="white-space:nowrap;font-weight:800;">' +
                escHtml(moedaCupom(sub)) +
                '</span></div>';
        });
        var qrImg = '';
        if (c.qr_code_url) {
            qrImg =
                '<div style="text-align:center;margin:8px 0 4px;"><img alt="QR NFC-e" style="width:42mm;max-width:100%;height:auto;" src="https://api.qrserver.com/v1/create-qr-code/?size=180x180&data=' +
                encodeURIComponent(String(c.qr_code_url)) +
                '"></div>';
        }
        var h = '<div class="pg">';
        if (c.homologacao) {
            h +=
                '<div style="text-align:center;font-size:10px;font-weight:900;border:2px dashed #000;padding:4px;margin-bottom:6px;">EMITIDA EM HOMOLOGAÇÃO — SEM VALOR FISCAL</div>';
        }
        if (c.emitente_razao_social) {
            h += '<div style="text-align:center;font-size:10px;font-weight:900;line-height:1.25;">' + escHtml(c.emitente_razao_social) + '</div>';
        }
        if (c.emitente_fantasia && c.emitente_fantasia !== c.emitente_razao_social) {
            h += '<div style="text-align:center;font-size:10px;font-weight:800;line-height:1.25;">' + escHtml(c.emitente_fantasia) + '</div>';
        } else if (!c.emitente_razao_social && c.emitente_fantasia) {
            h += '<div style="text-align:center;font-size:11px;font-weight:900;line-height:1.25;">' + escHtml(c.emitente_fantasia) + '</div>';
        }
        if (c.emitente_cnpj) {
            h += '<div style="text-align:center;font-size:9px;">CNPJ ' + escHtml(c.emitente_cnpj) + ' · IE ' + escHtml(c.emitente_ie || '—') + '</div>';
        }
        if (c.emitente_endereco) {
            h += '<div style="text-align:center;font-size:9px;line-height:1.25;margin:2px 0 4px;">' + escHtml(c.emitente_endereco) + '</div>';
        }
        h +=
            '<div style="text-align:center;font-size:10px;font-weight:900;line-height:1.2;margin:6px 0 2px;">DOCUMENTO AUXILIAR DA NOTA FISCAL DE CONSUMIDOR ELETRÔNICA</div>';
        h +=
            '<div style="text-align:center;font-size:9px;font-weight:800;line-height:1.2;margin:0 0 4px;">NÃO PERMITE APROVEITAMENTO DE CRÉDITO DE ICMS</div>';
        if (c.emissao_normal && !c.homologacao) {
            h += '<div style="text-align:center;font-size:9px;margin-bottom:4px;">EMISSÃO NORMAL</div>';
        }
        h += '<div style="font-size:10px;font-weight:800;">NFC-e nº ' + escHtml(String(c.numero_nf || '')) + ' · Série ' + escHtml(String(c.serie_nf || '')) + '</div>';
        if (c.criado_em) {
            h += '<div style="font-size:10px;">Emissão: ' + escHtml(c.criado_em) + '</div>';
        }
        if (c.protocolo) {
            h += '<div style="font-size:9px;word-break:break-all;">Protocolo: ' + escHtml(c.protocolo) + '</div>';
        }
        h += '<div style="border-top:1px dashed #000;margin:6px 0 4px;"></div>';
        h += cupomNomeClienteHtml(c.cliente_nome);
        if (c.cliente_cpf) {
            h += '<div style="font-size:10px;font-weight:800;">CPF ' + escHtml(c.cliente_cpf) + '</div>';
        } else if (c.consumidor_sem_identificacao) {
            h += '<div style="font-size:10px;font-weight:800;">Consumidor não identificado</div>';
        }
        h += lines;
        if (c.qtd_itens) {
            h += '<div style="font-size:10px;margin-top:4px;">Qtd. total de itens: ' + escHtml(String(c.qtd_itens)) + '</div>';
        }
        h +=
            '<div class="total-linha"><span>TOTAL</span><span class="total-valor">' +
            escHtml(c.total_texto || moedaCupom(c.total)) +
            '</span></div>';
        if (c.desconto_texto && Number(c.desconto || 0) >= 0) {
            h += '<div style="font-size:10px;">Desconto: ' + escHtml(c.desconto_texto) + '</div>';
        }
        if (c.forma_pagamento) {
            h += '<div style="font-size:11px;margin-top:4px;font-weight:800;">Pag.: ' + escHtml(c.forma_pagamento) + '</div>';
        }
        if (c.valor_pago_texto) {
            h += '<div style="font-size:10px;">Valor pago: ' + escHtml(c.valor_pago_texto) + '</div>';
        }
        if (Number(c.troco || 0) > 0 && c.troco_texto) {
            h += '<div style="font-size:10px;font-weight:800;">Troco: ' + escHtml(c.troco_texto) + '</div>';
        }
        if (c.ibpt_texto) {
            h +=
                '<div style="font-size:8px;line-height:1.25;margin:6px 0 4px;text-align:center;">' +
                escHtml(c.ibpt_texto) +
                '</div>';
        }
        if (c.chave) {
            h +=
                '<div style="font-size:8px;line-height:1.25;margin-top:6px;word-break:break-word;">Chave: ' +
                escHtml(formatChaveNfce(c.chave)) +
                '</div>';
        }
        h += qrImg;
        if (c.url_consulta_chave) {
            h +=
                '<div style="text-align:center;font-size:8px;line-height:1.2;margin-top:4px;word-break:break-all;">' +
                escHtml(c.url_consulta_chave) +
                '</div>';
        }
        h += '<div style="text-align:center;font-size:9px;margin-top:4px;">Consulte pela chave de acesso ou QR Code</div>';
        if (c.segunda_via) {
            h += '<div style="text-align:center;font-size:10px;font-weight:900;margin:4px 0;border:1px dashed #000;padding:3px;">2ª VIA</div>';
        }
        h += cupomRodapeSistvaleHtml();
        h += cupomPgCorteHtml();
        h += '</div>';
        return h;
    }

    function buildCupomPagesList(c) {
        c = c || {};
        var fiado = isFiadoCupom(c);
        if (fiado && !c.segunda_via) {
            var base = {};
            var k;
            for (k in c) {
                if (Object.prototype.hasOwnProperty.call(c, k)) base[k] = c[k];
            }
            return [
                buildCupomInnerHtml(
                    Object.assign({}, base, { via_rotulo: 'VIA DO CLIENTE', com_assinatura: false })
                ),
                buildCupomInnerHtml(
                    Object.assign({}, base, { via_rotulo: 'VIA DA LOJA', com_assinatura: true })
                )
            ];
        }
        return [buildCupomInnerHtml(c)];
    }

    function buildCupomPagesInnerHtml(c) {
        return buildCupomPagesList(c).join('');
    }

    function buildCupomDocumentHtml(c) {
        return (
            '<!DOCTYPE html><html><head><meta charset="utf-8"><title>Cupom venda</title><style>' +
            cupomStyles() +
            '</style></head><body>' +
            buildCupomPagesInnerHtml(c) +
            '</body></html>'
        );
    }

    function cupomEnsureIframe(iframeId, title) {
        var iframe = document.getElementById(iframeId);
        if (!iframe) {
            iframe = document.createElement('iframe');
            iframe.id = iframeId;
            iframe.title = title || 'Impressão térmica';
            iframe.setAttribute('aria-hidden', 'true');
            iframe.style.cssText =
                'position:fixed;right:0;bottom:0;width:0;height:0;border:0;opacity:0;pointer-events:none;';
            document.body.appendChild(iframe);
        }
        return iframe;
    }

    function cupomWritePageInIframe(iframe, styles, title, bodyHtml) {
        var idoc = iframe.contentDocument || iframe.contentWindow.document;
        idoc.open();
        idoc.write(
            '<!DOCTYPE html><html><head><meta charset="utf-8"><title>' +
            escHtml(title || 'Cupom') +
            '</title><style>' +
            styles +
            '</style></head><body>' +
            bodyHtml +
            '</body></html>'
        );
        idoc.close();
        return idoc;
    }

    function cupomRunJsBarcodeInDoc(idoc, barcodeVal, cb) {
        cb = typeof cb === 'function' ? cb : function () {};
        try {
            var svg = idoc.getElementById('barc-orc');
            var JB = global.JsBarcode;
            if (svg && JB) {
                JB(svg, barcodeVal, {
                    format: 'CODE128',
                    width: 1.35,
                    height: 44,
                    displayValue: true,
                    fontSize: 11,
                    margin: 0,
                    marginTop: 4,
                    marginBottom: 2
                });
            }
        } catch (eBr) {}
        cb();
    }

    function cupomLoadJsBarcodeInDoc(idoc, cb) {
        cupomEnsureJsBarcode(cb);
    }

    function cupomPrintIframeWindow(win, cb) {
        cb = typeof cb === 'function' ? cb : function () {};
        var done = false;
        var finish = function () {
            if (done) return;
            done = true;
            cb();
        };
        try {
            if ('onafterprint' in win) {
                win.onafterprint = finish;
            } else {
                setTimeout(finish, 900);
            }
            win.focus();
            win.print();
        } catch (ePr) {
            finish();
        }
    }

    function cupomImprimirPaginasSequencial(pages, opts) {
        opts = opts || {};
        pages = Array.isArray(pages) ? pages.filter(Boolean) : [];
        if (!pages.length) return Promise.resolve(false);
        var styles = opts.styles || cupomStyles();
        var iframe = cupomEnsureIframe(opts.iframeId || 'agro-print-iframe-cupom-venda', opts.title || 'Cupom');
        var gapMs = isFinite(opts.gapMs) ? opts.gapMs : 420;
        var readyDelay = isFinite(opts.readyDelay) ? opts.readyDelay : 80;
        var idx = 0;

        return new Promise(function (resolve) {
            function printNext() {
                if (idx >= pages.length) {
                    resolve(true);
                    return;
                }
                var page = pages[idx];
                var bodyHtml = typeof page === 'string' ? page : page.html || '';
                var idoc = cupomWritePageInIframe(iframe, styles, opts.title || 'Cupom', bodyHtml);

                var goPrint = function () {
                    cupomWhenImagesReady(
                        idoc,
                        function () {
                            cupomPrintIframeWindow(iframe.contentWindow, function () {
                                idx += 1;
                                if (idx < pages.length) {
                                    setTimeout(printNext, gapMs);
                                } else {
                                    resolve(true);
                                }
                            });
                        },
                        readyDelay
                    );
                };

                var barcodeVal =
                    page && page.barcodeVal != null
                        ? String(page.barcodeVal)
                        : page && page.barcode && page.barcode.val != null
                          ? String(page.barcode.val)
                          : '';
                if (barcodeVal) {
                    cupomLoadJsBarcodeInDoc(idoc, function () {
                        cupomRunJsBarcodeInDoc(idoc, barcodeVal, goPrint);
                    });
                } else {
                    goPrint();
                }
            }
            printNext();
        });
    }

    function agroImprimirCupomVenda80mm(c) {
        if (!c || (!c.itens || !c.itens.length)) {
            alert('Não há itens para imprimir nesta venda.');
            return false;
        }
        var pages = buildCupomPagesList(c);
        if (pages.length > 1) {
            cupomImprimirPaginasSequencial(pages, {
                iframeId: 'agro-print-iframe-cupom-venda',
                title: 'Cupom venda',
                styles: cupomStyles()
            }).catch(function () {
                alert('Não foi possível abrir a impressão. Verifique a impressora térmica 80mm.');
            });
            return true;
        }
        var iframe = cupomEnsureIframe('agro-print-iframe-cupom-venda', 'Impressão cupom venda');
        var idoc = cupomWritePageInIframe(iframe, cupomStyles(), 'Cupom venda', pages[0]);
        cupomWhenImagesReady(idoc, function () {
            try {
                iframe.contentWindow.focus();
                iframe.contentWindow.print();
            } catch (errP) {
                alert('Não foi possível abrir a impressão. Verifique a impressora térmica 80mm.');
            }
        }, 80);
        return true;
    }

    function cupomPrintBodyInIframe(opts) {
        opts = opts || {};
        var bodyHtml = opts.bodyHtml || '';
        if (!bodyHtml) return;
        var styles = opts.styles || cupomStyles();
        var iframe = cupomEnsureIframe(opts.iframeId || 'agro-print-iframe-generic', opts.title || 'Impressão');
        var idoc = cupomWritePageInIframe(iframe, styles, opts.title || 'Impressão', bodyHtml);
        var readyDelay = isFinite(opts.readyDelay) ? opts.readyDelay : 80;
        var barcodeVal = opts.barcodeVal != null ? String(opts.barcodeVal) : '';
        var goPrint = function () {
            cupomWhenImagesReady(
                idoc,
                function () {
                    try {
                        iframe.contentWindow.focus();
                        iframe.contentWindow.print();
                    } catch (ePr) {}
                    if (typeof opts.onDone === 'function') opts.onDone();
                },
                readyDelay
            );
        };
        if (barcodeVal) {
            cupomEnsureJsBarcode(function () {
                cupomRunJsBarcodeInDoc(idoc, barcodeVal, goPrint);
            });
        } else {
            goPrint();
        }
    }

    function cupomWhenImagesReady(doc, fn, minDelayMs) {
        fn = typeof fn === 'function' ? fn : function () {};
        minDelayMs = isFinite(minDelayMs) ? minDelayMs : 80;
        if (!doc || !doc.querySelectorAll) {
            setTimeout(fn, minDelayMs);
            return;
        }
        var imgs = doc.querySelectorAll('img');
        if (!imgs.length) {
            setTimeout(fn, minDelayMs);
            return;
        }
        var pending = 0;
        var i;
        for (i = 0; i < imgs.length; i++) {
            if (!imgs[i].complete) pending += 1;
        }
        if (pending <= 0) {
            setTimeout(fn, Math.min(minDelayMs, 40));
            return;
        }
        var done = false;
        var finish = function () {
            if (done) return;
            done = true;
            setTimeout(fn, minDelayMs);
        };
        var tick = function () {
            pending -= 1;
            if (pending <= 0) finish();
        };
        for (i = 0; i < imgs.length; i++) {
            var img = imgs[i];
            if (img.complete) {
                tick();
            } else {
                img.addEventListener('load', tick, { once: true });
                img.addEventListener('error', tick, { once: true });
            }
        }
        setTimeout(finish, 2500);
    }

    function agroCarregarEImprimirCupomVenda(vendaId, opts) {
        opts = opts || {};
        var id = parseInt(vendaId, 10);
        if (!id) return Promise.reject(new Error('Venda inválida.'));
        var qsParts = [];
        if (opts.segunda_via === false) qsParts.push('segunda_via=0');
        if (opts.interno) qsParts.push('interno=1');
        var qs = qsParts.length ? '?' + qsParts.join('&') : '';
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

    function agroEscolherImprimirCupomPosNfce(vendaId, opts) {
        opts = opts || {};
        var id = parseInt(vendaId, 10);

        function concluir(imprimiu) {
            if (typeof opts.onDone === 'function') opts.onDone(!!imprimiu);
        }

        if (!id || typeof agroCarregarEImprimirCupomVenda !== 'function') {
            concluir(false);
            return Promise.resolve(false);
        }

        return new Promise(function (resolveEscolha) {
            var overlay = document.getElementById('agro-modal-nfce-pos-imprimir');
            if (!overlay) {
                overlay = document.createElement('div');
                overlay.id = 'agro-modal-nfce-pos-imprimir';
                overlay.className =
                    'fixed inset-0 z-[120] hidden items-center justify-center p-3 bg-slate-900/65';
                overlay.setAttribute('role', 'dialog');
                overlay.setAttribute('aria-modal', 'true');
                overlay.innerHTML =
                    '<div class="w-full max-w-md rounded-2xl border-2 border-emerald-300 bg-white shadow-2xl p-4 sm:p-5">' +
                    '<h2 class="text-lg font-black text-emerald-950">NFC-e autorizada</h2>' +
                    '<p class="mt-2 text-sm font-semibold text-slate-700 leading-snug">Deseja imprimir o cupom fiscal na impressora térmica agora?</p>' +
                    '<div class="mt-4 flex flex-col-reverse sm:flex-row flex-wrap gap-2 sm:justify-end">' +
                    '<button type="button" data-agro-nfce-pos-nao class="min-h-[48px] px-4 rounded-xl border-2 border-slate-200 text-xs font-black uppercase text-slate-700 hover:bg-slate-50">Agora não</button>' +
                    '<button type="button" data-agro-nfce-pos-sim class="min-h-[48px] px-5 rounded-xl border-2 border-emerald-500 bg-emerald-500 text-xs font-black uppercase text-white hover:bg-emerald-600">Imprimir cupom</button>' +
                    '</div></div>';
                document.body.appendChild(overlay);
            }

            var btnNao = overlay.querySelector('[data-agro-nfce-pos-nao]');
            var btnSim = overlay.querySelector('[data-agro-nfce-pos-sim]');

            function fecharModal() {
                overlay.classList.add('hidden');
                overlay.classList.remove('flex');
            }

            function escolher(sim) {
                fecharModal();
                resolveEscolha(!!sim);
            }

            btnNao.onclick = function () {
                escolher(false);
            };
            btnSim.onclick = function () {
                escolher(true);
            };
            overlay.onclick = function (e) {
                if (e.target === overlay) escolher(false);
            };

            overlay.classList.remove('hidden');
            overlay.classList.add('flex');
            setTimeout(function () {
                if (btnSim) btnSim.focus();
            }, 40);
        }).then(function (quImprime) {
            if (!quImprime) {
                concluir(false);
                return false;
            }
            return agroCarregarEImprimirCupomVenda(id, { segunda_via: false })
                .then(function () {
                    concluir(true);
                    return true;
                })
                .catch(function (err) {
                    alert(
                        err && err.message
                            ? err.message
                            : 'Não foi possível imprimir o cupom fiscal.'
                    );
                    concluir(false);
                    return false;
                });
        });
    }

    global.agroImprimirCupomVenda80mm = agroImprimirCupomVenda80mm;
    global.agroCarregarEImprimirCupomVenda = agroCarregarEImprimirCupomVenda;
    global.agroEscolherImprimirCupomPosNfce = agroEscolherImprimirCupomPosNfce;
    global.agroBuildCupomVenda80mmHtml = buildCupomDocumentHtml;
    global.agroCupomInnerHtml = buildCupomInnerHtml;
    global.agroCupomPagesInnerHtml = buildCupomPagesInnerHtml;
    global.agroCupomStyles = cupomStyles;
    global.agroCupomLogoHtml = cupomLogoHtml;
    global.agroCupomPgCorteHtml = cupomPgCorteHtml;
    global.agroCupomImprimirPaginasSequencial = cupomImprimirPaginasSequencial;
    global.agroCupomNomeClienteHtml = cupomNomeClienteHtml;
    global.agroCupomCabecalhoHtml = cupomCabecalhoHtml;
    global.agroCupomRodapeSistvaleHtml = cupomRodapeSistvaleHtml;
    global.agroCupomZapHtml = cupomZapHtml;
    global.agroCupomWhenImagesReady = cupomWhenImagesReady;
    global.agroCupomEnsureJsBarcode = cupomEnsureJsBarcode;
    global.agroCupomPreloadRecursos = cupomPreloadRecursos;
    global.agroCupomPrintBodyInIframe = cupomPrintBodyInIframe;

    if (typeof global.requestIdleCallback === 'function') {
        global.requestIdleCallback(cupomPreloadRecursos, { timeout: 5000 });
    } else {
        setTimeout(cupomPreloadRecursos, 1200);
    }
})(typeof window !== 'undefined' ? window : globalThis);
