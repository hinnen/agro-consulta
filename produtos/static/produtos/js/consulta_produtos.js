(function () {
    'use strict';
    const BOOTSTRAP_EL = document.getElementById('agro-pdv-bootstrap');
    let BOOTSTRAP = {};
    if (BOOTSTRAP_EL && BOOTSTRAP_EL.textContent) {
        try {
            BOOTSTRAP = JSON.parse(BOOTSTRAP_EL.textContent);
        } catch (err) {
            if (typeof console !== 'undefined' && console.warn) {
                console.warn('Bootstrap PDV inválido:', err);
            }
        }
    } else if (window.AGRO_PDV_BOOTSTRAP) {
        BOOTSTRAP = window.AGRO_PDV_BOOTSTRAP;
    }
    const AGRO_PDV_URLS = BOOTSTRAP.urls || {};
    const AGRO_PDV_ASSETS = BOOTSTRAP.assets || {};

(function () {
    'use strict';
    const _buscaIndexCache = new WeakMap();

    function normalizarBuscaLocal(texto) {
        return String(texto || '')
            .normalize('NFD')
            .replace(/[\u0300-\u036f]/g, '')
            .toLowerCase()
            .trim();
    }

    const SINONIMOS_BUSCA_PRODUTO = {
        racao: ['alimento', 'balanceado', 'concentrado'],
        kg: ['quilo', 'kilos', 'kilo', 'kgs'],
        quilo: ['kg', 'kilos', 'kilo'],
        kilo: ['kg', 'quilo'],
        litro: ['lt', 'lts', 'ltr'],
        un: ['und', 'unidade', 'uni'],
        und: ['un', 'unidade'],
        adubo: ['fertilizante', 'fertil'],
        fertilizante: ['adubo'],
    };

    function textoBuscaProdutoNormalizado(p) {
        return normalizarBuscaLocal(String(p && p.busca_texto ? p.busca_texto : ''));
    }

    function tokensBuscaProduto(p) {
        const s = textoBuscaProdutoNormalizado(p);
        if (!s) return [];
        return s.split(/\s+/).filter(Boolean);
    }

    function buildBuscaProdutoIndex(lista) {
        const arr = Array.isArray(lista) ? lista : [];
        const cached = _buscaIndexCache.get(arr);
        if (cached && cached.len === arr.length) return cached.index;
        const tokenMap = new Map();
        const codigoMap = new Map();
        arr.forEach((p, idx) => {
            const toks = tokensBuscaProduto(p);
            toks.forEach((t) => {
                if (!t) return;
                if (!tokenMap.has(t)) tokenMap.set(t, []);
                tokenMap.get(t).push(idx);
            });
            codigosSoDigitosProduto(p).forEach((c) => {
                if (!c) return;
                if (!codigoMap.has(c)) codigoMap.set(c, []);
                codigoMap.get(c).push(idx);
            });
        });
        const index = { tokenMap, codigoMap, listRef: arr };
        _buscaIndexCache.set(arr, { len: arr.length, index });
        return index;
    }

    function _uniquePush(out, seen, idx) {
        if (idx == null) return;
        if (seen.has(idx)) return;
        seen.add(idx);
        out.push(idx);
    }

    function _indicesPorTokenParcial(index, termo) {
        const out = [];
        const seen = new Set();
        index.tokenMap.forEach((idxs, tk) => {
            if (tk === termo || tk.startsWith(termo) || tk.includes(termo)) {
                idxs.forEach((i) => _uniquePush(out, seen, i));
            }
        });
        return out;
    }

    function _indicesPorCodigo(index, digitos) {
        const out = [];
        const seen = new Set();
        index.codigoMap.forEach((idxs, code) => {
            if (code === digitos || code.startsWith(digitos) || code.endsWith(digitos) || code.includes(digitos)) {
                idxs.forEach((i) => _uniquePush(out, seen, i));
            }
        });
        return out;
    }

    function variantesPalavraBusca(palavraBruta) {
        const n = normalizarBuscaLocal(palavraBruta);
        const out = new Set();
        if (n) out.add(n);
        if (n.length < 2) return [...out];
        const extra = SINONIMOS_BUSCA_PRODUTO[n];
        if (extra) extra.forEach(x => out.add(normalizarBuscaLocal(x)));
        for (const [k, arr] of Object.entries(SINONIMOS_BUSCA_PRODUTO)) {
            if (arr.some(v => normalizarBuscaLocal(v) === n)) out.add(normalizarBuscaLocal(k));
        }
        return [...out].filter(Boolean);
    }

    function codigosSoDigitosProduto(p) {
        const raw = [p.codigo_nfe, p.codigo_barras, p.codigo, p.ean];
        return raw
            .map(x => String(x ?? '').replace(/\D/g, ''))
            .filter(c => c.length > 0);
    }

    function casaCodigoNumericoNoProduto(palavraBruta, p) {
        const d = String(palavraBruta || '').replace(/\D/g, '');
        if (d.length < 4) return false;
        return codigosSoDigitosProduto(p).some(c => {
            if (c === d) return true;
            if (d.length >= 6 && (c.endsWith(d) || c.includes(d))) return true;
            if (c.length >= 6 && d.length >= 6 && (d.endsWith(c) || d.includes(c))) return true;
            return false;
        });
    }

    function edicaoUnicaNoMaximo(a, b) {
        if (a === b) return true;
        const la = a.length;
        const lb = b.length;
        if (Math.abs(la - lb) > 1) return false;
        let i = 0;
        let j = 0;
        let usado = false;
        while (i < la && j < lb) {
            if (a[i] === b[j]) {
                i++;
                j++;
                continue;
            }
            if (usado) return false;
            usado = true;
            if (la > lb) i++;
            else if (lb > la) j++;
            else {
                i++;
                j++;
            }
        }
        return true;
    }

    function casaPalavraEmTexto(palavraNorm, tokens, hay, fuzzy) {
        if (!palavraNorm) return true;
        if (hay.includes(palavraNorm)) return true;
        return tokens.some(tk => {
            if (tk.includes(palavraNorm)) return true;
            if (palavraNorm.length >= 2 && tk.startsWith(palavraNorm)) return true;
            if (fuzzy && palavraNorm.length >= 4 && tk.length >= 4 && edicaoUnicaNoMaximo(palavraNorm, tk)) return true;
            return false;
        });
    }

    function palavraCasaNoProduto(palavraBruta, tokens, hay, fuzzy, p) {
        if (casaCodigoNumericoNoProduto(palavraBruta, p)) return true;
        return variantesPalavraBusca(palavraBruta).some(v =>
            casaPalavraEmTexto(v, tokens, hay, fuzzy, p)
        );
    }

    /**
     * @param {Array} lista - produtos (mesmo formato da API local: busca_texto, codigo_nfe, etc.)
     * @param {string} termoNormalizado - resultado de normalizarBuscaLocal(termoBruto)
     * @param {'normal'|'scanner'} modo
     */
    function filtrarProdutosBuscaInteligente(lista, termoNormalizado, modo) {
        const termo = String(termoNormalizado || '').trim();
        const palavras = termo.split(/\s+/).filter(Boolean);
        const index = buildBuscaProdutoIndex(lista);
        if (modo === 'scanner') {
            const dTerm = termo.replace(/\D/g, '');
            const candidatosIdx = dTerm.length >= 4 ? _indicesPorCodigo(index, dTerm) : null;
            const pool = Array.isArray(candidatosIdx) && candidatosIdx.length
                ? candidatosIdx.map((i) => lista[i]).filter(Boolean)
                : lista;
            return pool.filter(p => {
                const hay = textoBuscaProdutoNormalizado(p);
                if (hay.includes(termo)) return true;
                const nfe = normalizarBuscaLocal(String(p.codigo_nfe ?? ''));
                const cb = normalizarBuscaLocal(String(p.codigo_barras ?? ''));
                if (termo === nfe || termo === cb) return true;
                if (dTerm.length >= 4 && casaCodigoNumericoNoProduto(termo, p)) return true;
                return false;
            });
        }
        if (!palavras.length) return [];

        // Pré-filtro por índice local para reduzir varredura linear.
        let pool = lista;
        if (palavras.length) {
            let idxUnion = null;
            palavras.forEach((pl) => {
                const d = String(pl || '').replace(/\D/g, '');
                const local = (d.length >= 4 ? _indicesPorCodigo(index, d) : _indicesPorTokenParcial(index, pl));
                if (!local || !local.length) return;
                const asSet = new Set(local);
                if (idxUnion === null) idxUnion = asSet;
                else idxUnion = new Set([...idxUnion].filter((x) => asSet.has(x)));
            });
            if (idxUnion && idxUnion.size) {
                pool = [...idxUnion].map((i) => lista[i]).filter(Boolean);
            }
        }

        const tentar = (fuzzy) => pool.filter(p => {
            const hay = textoBuscaProdutoNormalizado(p);
            const tokens = tokensBuscaProduto(p);
            return palavras.every(pl => palavraCasaNoProduto(pl, tokens, hay, fuzzy, p));
        });

        let resultados = tentar(false);
        if (resultados.length === 0 && palavras.every(pl => pl.length >= 3)) {
            resultados = tentar(true);
        }
        return resultados;
    }

    window.normalizarBuscaLocal = normalizarBuscaLocal;
    window.filtrarProdutosBuscaInteligente = filtrarProdutosBuscaInteligente;
    window.casaCodigoNumericoNoProduto = casaCodigoNumericoNoProduto;
    window.buildBuscaProdutoIndex = buildBuscaProdutoIndex;
})();

let carrinho = [];
let sugestoesAtuais = [];
let indexSelecionado = -1;
let indexSelecionadoCliente = -1;
let debounceTimer;
let clienteDebounceTimer;
let produtoEmDestaque = null;
let clienteSelecionado = null;

(() => {
  const DELAY_MS = 320;
  const patch = (obj) => {
    if (!obj || obj.__delayedPatched || typeof obj.show !== 'function' || typeof obj.hide !== 'function') return;
    const origShow = obj.show.bind(obj);
    const origHide = obj.hide.bind(obj);
    let t = null;
    let visible = false;
    obj.show = (...args) => {
      clearTimeout(t);
      t = setTimeout(() => {
        visible = true;
        origShow(...args);
      }, DELAY_MS);
    };
    obj.hide = (...args) => {
      clearTimeout(t);
      if (!visible) return;
      visible = false;
      origHide(...args);
    };
    obj.__delayedPatched = true;
  };
  patch(window.gmLoadingBar);
  patch(window.gmLoader);
})();

let ultimoInputTime = 0;
let bufferScanner = '';
let scannerTimer = null;
let quantidadeRapida = 1;
let ultimoProdutoAdicionadoId = null;
let tempoUltimaAdicao = 0;

let baseProdutos = [];
const PDV_CACHE_KEY = 'agro_pdv_catalog_cache_v2';
const PDV_CACHE_TTL_MS = 1000 * 60 * 60 * 8;
let cacheClientesPDV = [];
let frequenciaUso = JSON.parse(localStorage.getItem('freqProdutos') || '{}');
let catalogoRapidoAtual = [];

/** Sugestões da busca principal: lista completa filtrada + paginação na UI */
let sugestoesBuscaCompletas = [];
let limiteSugestoesVisivel = 15;
let ultimoTermoBuscaPalavras = [];
const BUSCA_SUG_LIM_INI = 15;
const BUSCA_SUG_LIM_PAG = 25;
const BUSCA_SUG_LIM_MAX = 60;
let buscaOnlineMergeSeq = 0;
let mergeFetchTimer = null;

/** Sincronização de saldos em segundo plano (servidor usa cache ~5s para proteger o Mongo). */

/** Igual ao cadastro no ERP (com reticências finais). */
const CLIENTE_PADRAO_PDV = 'CONSUMIDOR NÃO IDENTIFICADO...';

function nomeClientePdv() {
    const v = String(inputCliente && inputCliente.value ? inputCliente.value : '').trim();
    return v || CLIENTE_PADRAO_PDV;
}

function ehClienteGenericoPdv(str) {
    const s = String(str || '').trim();
    if (!s) return true;
    if (s === CLIENTE_PADRAO_PDV) return true;
    if (/^consumidor\s+final$/i.test(s)) return true;
    return false;
}

function enderecoLinhaClientePdv(sel) {
    if (!sel || typeof sel !== 'object') return '';
    const e = String(sel.endereco || '').trim();
    if (e) return e;
    const parts = [sel.logradouro, sel.numero, sel.complemento, sel.bairro, sel.cidade, sel.uf, sel.cep]
        .map((x) => String(x || '').trim())
        .filter(Boolean);
    return parts.length ? parts.join(', ') : '';
}

/** PK do ClienteAgro no Django (só para id `local:123` ou campo cliente_agro_pk na lista). */
function pdvClienteAgroPkSelecionado() {
    if (!clienteSelecionado || clienteSelecionado.id == null || clienteSelecionado.id === '') return null;
    if (clienteSelecionado.cliente_agro_pk != null && clienteSelecionado.cliente_agro_pk !== '') {
        const n = Number(clienteSelecionado.cliente_agro_pk);
        return Number.isFinite(n) ? n : null;
    }
    const s = String(clienteSelecionado.id);
    if (s.startsWith('local:')) {
        const n = parseInt(s.slice(6), 10);
        return Number.isFinite(n) ? n : null;
    }
    return null;
}

/** Maps: link manual do cliente, senão busca por Plus Code, senão endereço em linha. */
function pdvUrlMapsCliente() {
    if (!clienteSelecionado || typeof clienteSelecionado !== 'object') return '';
    const manual = String(clienteSelecionado.maps_url_manual || '').trim();
    if (manual) return manual;
    const pc = String(clienteSelecionado.plus_code || '').trim();
    const end = enderecoLinhaClientePdv(clienteSelecionado);
    const q = (pc || end).trim();
    return q ? 'https://www.google.com/maps/search/?api=1&query=' + encodeURIComponent(q) : '';
}

function pdvTelefoneEntregaWa() {
    const raw = (document.body && document.body.getAttribute('data-pdv-entrega-wa')) || '';
    return String(raw).replace(/\D/g, '');
}

function montarTextoWhatsappSeparacaoPdv(orcIdOpt) {
    const nome = nomeClientePdv();
    const telCli = clienteSelecionado && clienteSelecionado.telefone
        ? String(clienteSelecionado.telefone).trim()
        : '';
    const end = enderecoLinhaClientePdv(clienteSelecionado);
    const pc = clienteSelecionado && clienteSelecionado.plus_code ? String(clienteSelecionado.plus_code).trim() : '';
    const maps = pdvUrlMapsCliente();
    let t = '*Separação / Entrega* (orçamento PDV)\n';
    t += 'Cliente: ' + nome + '\n';
    if (telCli) t += 'Tel: ' + telCli + '\n';
    if (pc) t += 'Plus Code: ' + pc + '\n';
    t += end ? 'Endereço: ' + end + '\n' : 'Endereço: (não cadastrado no cliente)\n';
    if (maps) t += 'Maps: ' + maps + '\n';
    t += '\n*Itens:*\n';
    carrinho.forEach((it) => {
        t += String(it.qtd || 0) + 'x ' + String(it.nome || '') + '\n';
    });
    const totEl = document.getElementById('total-geral');
    t += '\nTotal: ' + (totEl ? totEl.innerText : '');
    if (orcIdOpt != null) t += '\n\nRetomar no PDV: ' + pdvCodigoBarrasOrcamento(orcIdOpt);
    return t;
}

function pdvCodigoBarrasOrcamento(orcId) {
    return 'GMORC' + String(orcId);
}

function pdvMetaItemCarrinho(it) {
    let cg = String(it && it.codigo_gm ? it.codigo_gm : '').trim();
    let pr = String(it && it.prateleira ? it.prateleira : '').trim();
    if (typeof baseProdutos !== 'undefined' && baseProdutos.length) {
        const loc = baseProdutos.find((x) => String(x.id) === String(it.id));
        if (loc) {
            if (!cg) cg = String(loc.codigo_nfe || loc.codigo || '').trim();
            if (!pr) pr = String(loc.prateleira || '').trim();
        }
    }
    return { codigo_gm: cg || '—', prateleira: pr || '—' };
}

/** Entrega: 3 vias 80mm — separação, entregador (QR Maps), cupom cliente. */
function imprimirPacoteEntregaTresViasPdv(orcId, opt) {
    opt = opt || { sep: true, ent: true, cup: true };
    const pack = document.getElementById('pdv-pack-entrega-print');
    if (!pack) return;
    const barcodeVal = pdvCodigoBarrasOrcamento(orcId);
    const dh = new Date().toLocaleString('pt-BR');
    const nomeCli = nomeClientePdv();
    const primeiroNome = (nomeCli.split(/\s+/)[0] || nomeCli || '—').toUpperCase();
    const end = enderecoLinhaClientePdv(clienteSelecionado);
    const mapsUrl = pdvUrlMapsCliente();
    const qrImg = mapsUrl
        ? '<img src="https://api.qrserver.com/v1/create-qr-code/?size=200x200&margin=1&data=' + encodeURIComponent(mapsUrl) + '" alt="" style="width:36mm;height:auto;display:block;margin:6px auto 0;" />'
        : '<div style="text-align:center;margin-top:6px;font-size:10px;">(endereço não cadastrado)</div>';

    let sepItems = '';
    carrinho.forEach((it) => {
        const m = pdvMetaItemCarrinho(it);
        sepItems +=
            '<div style="border-top:1px dashed #000;margin-top:6px;padding-top:4px;">' +
            '<div><b>GM</b> ' + escapeHtml(m.codigo_gm) + '</div>' +
            '<div style="font-weight:bold;">' + escapeHtml(String(it.nome || '')) + '</div>' +
            '<div style="font-size:20px;font-weight:900;margin:4px 0;">QTD ' + escapeHtml(String(it.qtd || 0)) + '</div>' +
            '<div><b>Prat.</b> ' + escapeHtml(m.prateleira) + '</div>' +
            '</div>';
    });

    const page1 =
        '<div class="pdv-entrega-page">' +
        '<div style="text-align:center;font-weight:900;font-size:13px;">SEPARAÇÃO</div>' +
        '<div style="margin-top:4px;">' + escapeHtml(dh) + '</div>' +
        '<div style="margin-top:4px;"><b>Cliente</b> ' + escapeHtml(nomeCli) + '</div>' +
        '<div style="border-top:2px solid #000;margin:6px 0;"></div>' +
        sepItems +
        '<div style="margin-top:10px;text-align:center;">' +
        '<svg id="pdv-barcode-orc" xmlns="http://www.w3.org/2000/svg"></svg>' +
        '<div style="font-size:9px;margin-top:4px;">Bipe no buscador do PDV para retomar o orçamento</div>' +
        '</div>' +
        '</div>';

    let entItems = '';
    carrinho.forEach((it) => {
        entItems +=
            '<div style="margin-top:4px;">' +
            escapeHtml(String(it.qtd || 0) + '× ' + String(it.nome || '')) +
            '</div>';
    });

    const page2 =
        '<div class="pdv-entrega-page">' +
        '<div style="text-align:center;font-weight:900;font-size:12px;">ENTREGA</div>' +
        '<div style="font-size:26px;font-weight:900;text-align:center;line-height:1;margin:8px 0;letter-spacing:-0.02em;">' +
        escapeHtml(primeiroNome) +
        '</div>' +
        '<div style="font-size:10px;">' + escapeHtml(dh) + '</div>' +
        '<div style="margin-top:6px;"><b>Cliente</b> ' + escapeHtml(nomeCli) + '</div>' +
        '<div style="border-top:1px dashed #000;margin:6px 0;"></div>' +
        entItems +
        '<div style="margin-top:8px;"><b>Endereço</b></div>' +
        '<div style="font-size:10px;word-break:break-word;">' + escapeHtml(end || '—') + '</div>' +
        qrImg +
        '</div>';

    const totEl = document.getElementById('total-geral');
    let cupLines = '';
    carrinho.forEach((it) => {
        const sub = Number(it.preco || 0) * Number(it.qtd || 0);
        cupLines +=
            '<div style="display:flex;justify-content:space-between;gap:4px;margin:3px 0;font-size:10px;">' +
            '<span style="flex:1;">' +
            escapeHtml(String(it.qtd) + '× ' + String(it.nome || '').slice(0, 36)) +
            '</span>' +
            '<span style="white-space:nowrap;">' + escapeHtml(formatarMoeda(sub)) + '</span>' +
            '</div>';
    });

    const page3 =
        '<div class="pdv-entrega-page">' +
        '<div style="text-align:center;font-weight:900;font-size:14px;">AGRO MAIS</div>' +
        '<div style="text-align:center;font-size:10px;margin:2px 0;">Orçamento (não fiscal)</div>' +
        '<div style="font-size:10px;">' + escapeHtml(dh) + '</div>' +
        '<div style="border-top:1px dashed #000;margin:6px 0;"></div>' +
        '<div style="font-weight:bold;margin-bottom:4px;">' + escapeHtml(nomeCli) + '</div>' +
        cupLines +
        '<div style="border-top:2px solid #000;margin:8px 0 4px;padding-top:4px;font-weight:900;font-size:13px;display:flex;justify-content:space-between;">' +
        '<span>TOTAL</span><span>' + escapeHtml(totEl ? totEl.innerText : '') + '</span></div>' +
        '<div style="text-align:center;font-size:9px;margin-top:8px;">Obrigado — apresente na retirada</div>' +
        '<div style="text-align:center;font-size:8px;margin-top:4px;word-break:break-all;">Retomar: ' + escapeHtml(barcodeVal) + '</div>' +
        '</div>';

    const chunks = [];
    if (opt.sep) chunks.push(page1);
    if (opt.ent) chunks.push(page2);
    if (opt.cup) chunks.push(page3);
    if (!chunks.length) return;
    pack.innerHTML = chunks.join('');

    try {
        const svg = pack.querySelector('#pdv-barcode-orc');
        if (svg && typeof JsBarcode !== 'undefined') {
            JsBarcode(svg, barcodeVal, {
                format: 'CODE128',
                width: 1.35,
                height: 44,
                displayValue: true,
                fontSize: 11,
                margin: 0,
                marginTop: 4,
                marginBottom: 2,
            });
        }
    } catch (eBar) {
        console.warn('Barcode orçamento:', eBar);
    }

    document.body.classList.add('print-pdv-entrega-pack');
    window.print();
    document.body.classList.remove('print-pdv-entrega-pack');
    pack.innerHTML = '';
}

function abrirWhatsappSeparacaoPdv(orcIdOpt) {
    const phone = pdvTelefoneEntregaWa();
    const txt = montarTextoWhatsappSeparacaoPdv(orcIdOpt);
    if (!phone || phone.length < 10) {
        alert('WhatsApp de entregas não está configurado. Peça ao suporte para cadastrar o número da loja.');
        return;
    }
    window.open('https://wa.me/' + phone + '?text=' + encodeURIComponent(txt), '_blank', 'noopener,noreferrer');
}

/** Cards de produto: sem rótulos inventados (“Sem marca”); neutro para o operador conferir. */
function htmlValorCardProdutoPdv(val) {
    const t = String(val == null ? '' : val).trim();
    if (t) return escapeHtml(t);
    return '<span class="text-slate-300 font-normal normal-case" title="A conferir">—</span>';
}

const inputBusca = document.getElementById('busca-produto');
const autoList = document.getElementById('autocomplete-results');
const destaqueContainer = document.getElementById('produto-destaque');
const similaresContainer = document.getElementById('lista-similares');
const statusBusca = document.getElementById('status-busca');
const scanBanner = document.getElementById('scan-banner');

const inputCliente = document.getElementById('nome-cliente');
const clienteResults = document.getElementById('cliente-results');
const tipoFiltroCatalogo = document.getElementById('tipo-filtro-catalogo');
const valorFiltroCatalogo = document.getElementById('valor-filtro-catalogo');
const opcoesFiltroCatalogo = document.getElementById('opcoes-filtro-catalogo-list');
const opcoesFiltroCatalogoBox = document.getElementById('opcoes-filtro-catalogo-box');
const listaCatalogoRapido = document.getElementById('lista-catalogo-rapido');
const resumoFiltroCatalogo = document.getElementById('resumo-filtro-catalogo');
const filtroCatalogoAtivo = document.getElementById('filtro-catalogo-ativo');
const historicoResumido = document.getElementById('historico-resumido');
const btnLimparFiltroCatalogo = document.getElementById('btn-limpar-filtro-catalogo');
const listaLembretes = document.getElementById('lista-lembretes');
let alertaLembreteAtual = null;

function pdvCarrinhoDrawerEstaAberto() {
    const d = document.getElementById('pdv-drawer-carrinho');
    return !!(d && !d.classList.contains('translate-x-full'));
}

function abrirDrawerCarrinho() {
    const root = document.getElementById('pdv-drawer-carrinho');
    const bd = document.getElementById('pdv-carrinho-backdrop');
    if (!root) return;
    root.classList.remove('translate-x-full');
    root.setAttribute('aria-hidden', 'false');
    if (bd) {
        bd.classList.remove('opacity-0', 'pointer-events-none');
        bd.classList.add('opacity-100');
        bd.setAttribute('aria-hidden', 'false');
    }
    document.body.classList.add('pdv-carrinho-drawer-open');
}

/** Primeiro item no carrinho: não abre o drawer — chama após atualizarCarrinho. */
function pdvDestacarBotoesCarrinho() {
    const ids = ['btn-abrir-carrinho-pdv'];
    ids.forEach((id) => {
        const el = document.getElementById(id);
        if (!el) return;
        el.classList.remove('pdv-carrinho-attention');
        void el.offsetWidth;
        el.classList.add('pdv-carrinho-attention');
        const fin = () => el.classList.remove('pdv-carrinho-attention');
        el.addEventListener('animationend', fin, { once: true });
        window.setTimeout(fin, 1000);
    });
}

function fecharDrawerCarrinho() {
    const root = document.getElementById('pdv-drawer-carrinho');
    const bd = document.getElementById('pdv-carrinho-backdrop');
    if (!root) return;
    root.classList.add('translate-x-full');
    root.setAttribute('aria-hidden', 'true');
    if (bd) {
        bd.classList.add('opacity-0', 'pointer-events-none');
        bd.classList.remove('opacity-100');
        bd.setAttribute('aria-hidden', 'true');
    }
    document.body.classList.remove('pdv-carrinho-drawer-open');
}

/** Modal estilizado (impressão / WhatsApp após salvar com entrega). */
function pdvModalPerguntaEntrega(titulo, texto, eyebrow) {
    return new Promise((resolve) => {
        const root = document.getElementById('modal-pdv-entrega-pergunta');
        if (!root) {
            resolve(false);
            return;
        }
        const elEyebrow = document.getElementById('mpe-eyebrow');
        const elTitulo = document.getElementById('mpe-titulo');
        const elTexto = document.getElementById('mpe-texto');
        const btnSim = document.getElementById('mpe-sim');
        const btnNao = document.getElementById('mpe-nao');
        if (elEyebrow) elEyebrow.textContent = eyebrow || 'Orçamento com entrega';
        if (elTitulo) elTitulo.textContent = titulo;
        if (elTexto) elTexto.textContent = texto;
        let done = false;
        const finish = (v) => {
            if (done) return;
            done = true;
            root.classList.add('hidden');
            root.classList.remove('flex');
            document.body.classList.remove('modal-open');
            root.onclick = null;
            if (btnSim) btnSim.onclick = null;
            if (btnNao) btnNao.onclick = null;
            resolve(v);
        };
        if (btnSim) btnSim.onclick = () => finish(true);
        if (btnNao) btnNao.onclick = () => finish(false);
        root.onclick = (ev) => { if (ev.target === root) finish(false); };
        root.classList.remove('hidden');
        root.classList.add('flex');
        document.body.classList.add('modal-open');
    });
}

/** Checkboxes: separação, entregador, cupom — cancel retorna null. */
function pdvModalEscolhaImpressaoEntrega() {
    return new Promise((resolve) => {
        const root = document.getElementById('modal-pdv-entrega-impressao');
        if (!root) {
            resolve({ sep: true, ent: true, cup: true });
            return;
        }
        const btnImp = document.getElementById('mei-imprimir');
        const btnCan = document.getElementById('mei-cancelar');
        let done = false;
        const finish = (v) => {
            if (done) return;
            done = true;
            root.classList.add('hidden');
            root.classList.remove('flex');
            document.body.classList.remove('modal-open');
            root.onclick = null;
            if (btnImp) btnImp.onclick = null;
            if (btnCan) btnCan.onclick = null;
            resolve(v);
        };
        if (btnImp) {
            btnImp.onclick = () => {
                const sep = document.getElementById('mei-chk-sep').checked;
                const ent = document.getElementById('mei-chk-ent').checked;
                const cup = document.getElementById('mei-chk-cup').checked;
                if (!sep && !ent && !cup) {
                    alert('Marque ao menos uma via para imprimir.');
                    return;
                }
                finish({ sep, ent, cup });
            };
        }
        if (btnCan) btnCan.onclick = () => finish(null);
        root.onclick = (ev) => {
            if (ev.target === root) finish(null);
        };
        root.classList.remove('hidden');
        root.classList.add('flex');
        document.body.classList.add('modal-open');
    });
}

(function initPdvCarrinhoDrawer() {
    const btnAbrir = document.getElementById('btn-abrir-carrinho-pdv');
    const btnFechar = document.getElementById('btn-fechar-carrinho-pdv');
    const bd = document.getElementById('pdv-carrinho-backdrop');
    if (btnAbrir) btnAbrir.addEventListener('click', abrirDrawerCarrinho);
    if (btnFechar) btnFechar.addEventListener('click', fecharDrawerCarrinho);
    if (bd) bd.addEventListener('click', fecharDrawerCarrinho);
})();

function pickFirstValue(...valores) {
    for (const valor of valores) {
        if (valor === 0) return '0';
        if (valor !== undefined && valor !== null) {
            const txt = String(valor).trim();
            if (txt && txt !== '[object Object]') return txt;
        }
    }
    return '';
}

function extrairTextoGenerico(valor) {
    if (valor === undefined || valor === null) return '';
    if (typeof valor === 'string' || typeof valor === 'number') return String(valor).trim();
    if (Array.isArray(valor)) {
        for (const item of valor) {
            const achado = extrairTextoGenerico(item);
            if (achado) return achado;
        }
        return '';
    }
    if (typeof valor === 'object') {
        const preferidos = ['nome','name','descricao','descricao_completa','titulo','title','razao_social','razao','fantasia','nome_fantasia','display_name','categoria','marca','fornecedor','fabricante','grupo','subcategoria','label','text','value'];
        for (const chave of preferidos) {
            if (chave in valor) {
                const achado = extrairTextoGenerico(valor[chave]);
                if (achado) return achado;
            }
        }
        for (const item of Object.values(valor)) {
            const achado = extrairTextoGenerico(item);
            if (achado) return achado;
        }
    }
    return '';
}

function valorTextoValidoCatalogo(txt) {
    const v = String(txt || '').trim();
    if (!v) return false;
    const norm = normalizarBuscaLocal(v);
    if (!norm || norm.length < 2) return false;
    if (/^[0-9]+$/.test(norm)) return false;
    if (/^[a-f0-9]{24}$/i.test(v)) return false;
    if (/^[0-9a-f-]{30,}$/i.test(v)) return false;
    return true;
}

function coletarValoresPorTipo(produto, tipo) {
    if (!produto || !tipo) return [];
    const keywords = {
        marca: ['marca','brand','marca_nome','brand_name'],
        fornecedor: ['fornecedor','fornec','supplier','fabricante','distrib','vendor','parceiro','empresa','pessoa'],
        categoria: ['categoria','categorias','grupo','subcategoria','departamento','linha','familia','tipo','secao','classificacao','colecao','collection']
    };
    const seeds = {
        marca: [produto.marca, produto.marca_nome, produto.nome_marca, produto.brand, produto.marcaDescricao, produto.marca_obj, produto.marcaModel, produto.marcas, produto.brand_name],
        fornecedor: [produto.fornecedor, produto.fornecedor_nome, produto.nome_fornecedor, produto.razao_fornecedor, produto.fabricante, produto.fornecedor_obj, produto.fornecedorModel, produto.distribuidor, produto.supplier, produto.fornecedores, produto.fornecedor_padrao, produto.parceiro],
        categoria: [produto.categoria, produto.categoria_nome, produto.nome_categoria, produto.grupo, produto.grupo_nome, produto.subcategoria, produto.categoria_pai, produto.categoria_obj, produto.categorias, produto.departamento, produto.linha, produto.tipo_produto, produto.collection]
    };
    const achados = [];
    const vistos = new Set();
    const add = (txt) => {
        const limpo = String(txt || '').trim();
        const chave = normalizarBuscaLocal(limpo);
        if (!valorTextoValidoCatalogo(limpo) || vistos.has(chave)) return;
        vistos.add(chave);
        achados.push(limpo);
    };
    (seeds[tipo] || []).forEach(item => {
        const txt = extrairTextoGenerico(item);
        if (txt) add(txt);
    });
    const chavesRelacionadas = {
        marca: ['nome','descricao','label','text','title','name'],
        fornecedor: ['nome','razao_social','razao','fantasia','nome_fantasia','descricao','label','text','title','name'],
        categoria: ['nome','descricao','titulo','label','text','title','name']
    };
    const queue = [{ valor: produto, path: '', depth: 0 }];
    while (queue.length) {
        const { valor, path, depth } = queue.shift();
        if (depth > 4 || valor === undefined || valor === null) continue;
        if (Array.isArray(valor)) {
            valor.forEach((item, idx) => queue.push({ valor: item, path: `${path}[${idx}]`, depth: depth + 1 }));
            continue;
        }
        if (typeof valor === 'object') {
            for (const [chave, item] of Object.entries(valor)) {
                const novoPath = path ? `${path}.${chave}` : chave;
                const chaveNorm = normalizarBuscaLocal(novoPath);
                if (keywords[tipo].some(k => chaveNorm.includes(normalizarBuscaLocal(k)))) {
                    const txt = extrairTextoGenerico(item);
                    if (txt) add(txt);
                    if (item && typeof item === 'object') {
                        for (const relKey of chavesRelacionadas[tipo]) {
                            if (item[relKey] !== undefined) {
                                const relTxt = extrairTextoGenerico(item[relKey]);
                                if (relTxt) add(relTxt);
                            }
                        }
                    }
                }
                queue.push({ valor: item, path: novoPath, depth: depth + 1 });
            }
            continue;
        }
        if (typeof valor === 'string' || typeof valor === 'number') {
            const pathNorm = normalizarBuscaLocal(path);
            if (keywords[tipo].some(k => pathNorm.includes(normalizarBuscaLocal(k)))) add(valor);
        }
    }
    return achados;
}

function obterValorCampoProduto(produto, tipo) {
    return coletarValoresPorTipo(produto, tipo)[0] || '';
}

function prepararProduto(produto) {
    const nome = pickFirstValue(produto.nome, produto.descricao, produto.descricao_completa, produto.nome_produto);
    const codigo = pickFirstValue(produto.codigo_nfe, produto.codigo_interno, produto.codigo, produto.sku);
    const codigoBarras = pickFirstValue(produto.codigo_barras, produto.ean, produto.barras);
    const marca = obterValorCampoProduto(produto, 'marca');
    const fornecedor = obterValorCampoProduto(produto, 'fornecedor');
    const categoria = obterValorCampoProduto(produto, 'categoria');
    const buscaTexto = [nome, codigo, codigoBarras, marca, fornecedor, categoria].map(normalizarBuscaLocal).join(' ');
    return {
        ...produto,
        nome,
        codigo_nfe: codigo,
        codigo_barras: codigoBarras,
        marca,
        fornecedor,
        categoria,
        busca_texto: buscaTexto,
        saldo_centro: Number(produto.saldo_centro || produto.estoque_centro || produto.saldo || 0),
        saldo_vila: Number(produto.saldo_vila || produto.estoque_vila || produto.saldo_filial || 0),
        preco_venda: Number(produto.preco_venda || produto.preco || produto.valor || 0),
        media_venda_diaria_30d: Number(produto.media_venda_diaria_30d || 0)
    };
}

function prepararBaseProdutos(lista) {
    return (Array.isArray(lista) ? lista : []).map(prepararProduto);
}

function preencherOpcoesFiltroCatalogo() {
    const tipo = tipoFiltroCatalogo.value;
    opcoesFiltroCatalogo.innerHTML = '';
    opcoesFiltroCatalogoBox.innerHTML = '';
    valorFiltroCatalogo.value = '';
    filtroCatalogoAtivo.classList.add('hidden');

    if (!tipo || !baseProdutos.length) {
        resumoFiltroCatalogo.textContent = 'Filtro auxiliar da lista';
        opcoesFiltroCatalogoBox.classList.add('hidden');
        return;
    }

    const unicos = [...new Set(baseProdutos.flatMap(p => coletarValoresPorTipo(p, tipo)).filter(Boolean))]
        .sort((a, b) => a.localeCompare(b, 'pt-BR'))
        .slice(0, 500);

    unicos.forEach(valor => {
        const opt = document.createElement('option');
        opt.value = valor;
        opcoesFiltroCatalogo.appendChild(opt);
    });

    opcoesFiltroCatalogoBox.classList.add('hidden');
    resumoFiltroCatalogo.textContent = unicos.length ? `${unicos.length} opção(ões) para ${tipo}` : `Nenhuma opção encontrada para ${tipo} no catálogo`; 
}

function renderizarSugestoesFiltro(lista) {
    opcoesFiltroCatalogoBox.innerHTML = '';
    if (!lista.length || !tipoFiltroCatalogo.value) {
        opcoesFiltroCatalogoBox.classList.add('hidden');
        return;
    }
    lista.slice(0, 12).forEach(valor => {
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'aux-option';
        btn.textContent = valor;
        btn.onclick = () => {
            valorFiltroCatalogo.value = valor;
            opcoesFiltroCatalogoBox.classList.add('hidden');
            atualizarCatalogoRapido();
        };
        opcoesFiltroCatalogoBox.appendChild(btn);
    });
    opcoesFiltroCatalogoBox.classList.remove('hidden');
}

function renderizarCatalogoRapido(lista) {
    catalogoRapidoAtual = lista.slice(0, 100);
    listaCatalogoRapido.innerHTML = '';

    if (!catalogoRapidoAtual.length) {
        listaCatalogoRapido.innerHTML = '<div class="catalog-empty-state">Nenhum produto encontrado neste filtro.</div>';
        return;
    }

    catalogoRapidoAtual.forEach(produto => {
        const row = document.createElement('button');
        row.type = 'button';
        row.className = 'catalog-row w-full text-left px-3 py-2 transition-colors border-l-4 border-transparent';
        const marca = obterValorCampoProduto(produto, 'marca');
        const fornecedor = obterValorCampoProduto(produto, 'fornecedor');
        const categoria = obterValorCampoProduto(produto, 'categoria');
        const codStr = String(produto.codigo_nfe || produto.codigo || '').trim();
        const codHtml = codStr ? escapeHtml(codStr) : '<span class="text-slate-300">—</span>';
        row.innerHTML = `
            <div class="flex items-start justify-between gap-3">
                <div class="min-w-0">
                    <div class="text-[14px] leading-4 font-black text-slate-800 uppercase break-words">${escapeHtml(produto.nome || '')}</div>
                    <div class="mt-1 flex flex-wrap gap-x-2 gap-y-1 text-[10px] font-bold uppercase text-slate-400">
                        <span class="text-slate-500">Cód: ${codHtml}</span>
                        <span>${htmlValorCardProdutoPdv(marca)}</span>
                        <span>${htmlValorCardProdutoPdv(fornecedor)}</span>
                        <span>${htmlValorCardProdutoPdv(categoria)}</span>
                    </div>
                </div>
                <div class="shrink-0 text-right">
                    <div class="text-sm font-black text-emerald-600">${formatarMoeda(produto.preco_venda)}</div>
                    <div class="text-[10px] font-bold uppercase text-slate-400"><span class="text-emerald-700">Centro: ${Number(produto.saldo_centro || 0)}</span> • Vila: ${Number(produto.saldo_vila || 0)} • Total: ${(Number(produto.saldo_centro || 0) + Number(produto.saldo_vila || 0)).toFixed(1)}</div>
                </div>
            </div>
        `;
        row.onclick = () => {
            inputBusca.value = produto.nome || '';
            produtoEmDestaque = produto;
            renderDestaque(produto);
            autoList.classList.add('hidden');
            Array.from(listaCatalogoRapido.children).forEach(el => el.classList.remove('active'));
            row.classList.add('active');
            similaresContainer.innerHTML = '';
            focarBuscaProduto();
        };
        listaCatalogoRapido.appendChild(row);
    });
}

function atualizarCatalogoRapido() {
    /* Lista auxiliar removida do PDV — função mantida para compatibilidade com scripts legados. */
}

let buscaAvancadaResultados = [];

function abrirBuscaAvancada() {
    const m = document.getElementById('modal-busca-avancada');
    if (!m) return;
    preencherSelectBuscaAvancadaDimensao();
    m.classList.remove('hidden');
    m.classList.add('flex');
}

function fecharBuscaAvancada() {
    const m = document.getElementById('modal-busca-avancada');
    if (!m) return;
    m.classList.add('hidden');
    m.classList.remove('flex');
}

function abrirModalLembretes() {
    const m = document.getElementById('modal-lembretes');
    if (!m) return;
    renderizarLembretes();
    m.classList.remove('hidden');
    m.classList.add('flex');
    const inp = document.getElementById('lembrete-texto');
    if (inp) setTimeout(() => inp.focus(), 50);
}

function fecharModalLembretes() {
    const m = document.getElementById('modal-lembretes');
    if (!m) return;
    m.classList.add('hidden');
    m.classList.remove('flex');
}

function preencherSelectBuscaAvancadaDimensao() {
    const sel = document.getElementById('ba-valor-ref');
    const dimEl = document.getElementById('ba-dimensao');
    if (!sel || !dimEl) return;
    const dim = dimEl.value;
    sel.innerHTML = '';
    const o0 = document.createElement('option');
    o0.value = '';
    o0.textContent = '(qualquer)';
    sel.appendChild(o0);
    if (!dim || !baseProdutos.length) return;
    const unicos = [...new Set(baseProdutos.flatMap(p => coletarValoresPorTipo(p, dim)).filter(Boolean))]
        .sort((a, b) => a.localeCompare(b, 'pt-BR'))
        .slice(0, 400);
    unicos.forEach(v => {
        const o = document.createElement('option');
        o.value = v;
        o.textContent = v;
        sel.appendChild(o);
    });
}

function aplicarBuscaAvancada() {
    const dim = (document.getElementById('ba-dimensao') || {}).value || '';
    const valRef = ((document.getElementById('ba-valor-ref') || {}).value || '').trim();
    const txt = normalizarBuscaLocal((document.getElementById('ba-texto') || {}).value || '');
    const pm = parseFloat((document.getElementById('ba-preco-max') || {}).value);
    const em = parseFloat((document.getElementById('ba-estoque-max') || {}).value);

    let out = baseProdutos.slice();
    if (dim && valRef) {
        const nref = normalizarBuscaLocal(valRef);
        out = out.filter(p =>
            coletarValoresPorTipo(p, dim).some(v => {
                const nv = normalizarBuscaLocal(v);
                return nv === nref || nv.includes(nref);
            })
        );
    }
    if (txt) {
        out = filtrarProdutosBuscaInteligente(out, txt, 'normal');
    }
    if (!Number.isNaN(pm) && pm >= 0) {
        out = out.filter(p => Number(p.preco_venda || 0) <= pm);
    }
    if (!Number.isNaN(em)) {
        out = out.filter(p =>
            Number(p.saldo_centro || 0) + Number(p.saldo_vila || 0) <= em
        );
    }
    buscaAvancadaResultados = out.slice(0, 200);
    renderListaBuscaAvancada();
}

function renderListaBuscaAvancada() {
    const host = document.getElementById('lista-busca-avancada');
    if (!host) return;
    if (!buscaAvancadaResultados.length) {
        host.innerHTML = '<div class="text-center text-slate-400 font-bold text-sm py-8 px-2">Nenhum produto com esses filtros.</div>';
        return;
    }
    host.innerHTML = '';
    buscaAvancadaResultados.forEach(p => {
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'w-full text-left px-3 py-2.5 rounded-xl border border-slate-100 hover:border-orange-300 hover:bg-orange-50/50 mb-1 transition-colors bg-white';
        const mar = obterValorCampoProduto(p, 'marca');
        const tot = Number(p.saldo_centro || 0) + Number(p.saldo_vila || 0);
        const marLinha = String(mar || '').trim() ? escapeHtml(mar) : '<span class="text-slate-300">—</span>';
        btn.innerHTML = `
            <div class="font-black text-xs uppercase text-slate-800 leading-snug">${escapeHtml(p.nome || '')}</div>
            <div class="text-[10px] text-slate-500 font-bold mt-0.5">${marLinha} · ${formatarMoeda(p.preco_venda)} · est ${tot.toFixed(1)}</div>`;
        btn.onclick = () => {
            adicionarProdutoComQuantidade(p.id, p.nome, p.preco_venda, quantidadeRapida, p);
            tocarSom('add');
        };
        host.appendChild(btn);
    });
}

function renderizarHistoricoResumido() {
    let historico = [];
    try { historico = JSON.parse(localStorage.getItem('historicoOrcamentos') || '[]'); } catch(e) { historico = []; }
    historicoResumido.innerHTML = '';

    if (!historico.length) {
        historicoResumido.innerHTML = '<div class="px-3 py-4 text-center text-[11px] font-bold text-slate-400">Nenhum orçamento salvo ainda.</div>';
        return;
    }

    historico.slice(0, 5).forEach(h => {
        const item = document.createElement('button');
        item.type = 'button';
        item.className = 'w-full text-left px-3 py-2 hover:bg-slate-50 transition-colors';
        item.innerHTML = `
            <div class="flex items-start justify-between gap-2">
                <div class="min-w-0">
                    <div class="text-[15px] font-black uppercase text-slate-800 truncate tracking-wide">${escapeHtml(h.cliente)}</div>
                    <div class="text-[10px] font-bold text-slate-400">${escapeHtml(h.data)}</div>
                </div>
                <div class="text-[11px] font-black text-emerald-600 shrink-0">${escapeHtml(h.total)}</div>
            </div>
        `;
        item.onclick = () => recuperarOrcamento(h.id);
        historicoResumido.appendChild(item);
    });
}

function formatarMoeda(valor) {
    return `R$ ${Number(valor || 0).toFixed(2)}`;
}

/** Valor numérico para coluna de preço (pt-BR, sem prefixo R$). */
function formatarValorPrecoGrande(valor) {
    return Number(valor || 0).toLocaleString('pt-BR', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
    });
}

async function copiarCodigoComFeedback(texto, elFeedback) {
    const t = String(texto || '').trim();
    if (!t) return;
    const mostrarOk = () => {
        if (!elFeedback) return;
        elFeedback.classList.remove('hidden');
        clearTimeout(elFeedback._copiarT);
        elFeedback._copiarT = setTimeout(() => elFeedback.classList.add('hidden'), 1200);
    };
    try {
        await navigator.clipboard.writeText(t);
        mostrarOk();
    } catch (_) {
        try {
            const ta = document.createElement('textarea');
            ta.value = t;
            ta.setAttribute('readonly', '');
            ta.style.position = 'fixed';
            ta.style.left = '-9999px';
            document.body.appendChild(ta);
            ta.select();
            document.execCommand('copy');
            document.body.removeChild(ta);
            mostrarOk();
        } catch (__) {}
    }
}

function escapeHtml(texto) {
    return String(texto || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
}

/** Realce no nome conforme palavras digitadas (tokens alfanuméricos). */
function destacarNomePDV(nome, palavrasBrutas) {
    const s = String(nome || '');
    if (!s) return '';
    const termsNorm = [...new Set(
        (palavrasBrutas || []).map(p => normalizarBuscaLocal(p)).filter(t => t.length >= 2)
    )].sort((a, b) => b.length - a.length);
    if (!termsNorm.length) return escapeHtml(s);
    const re = /[\d.]+|[A-Za-zÀ-ÿ]+/g;
    const chunks = [];
    let last = 0;
    let m;
    while ((m = re.exec(s)) !== null) {
        if (m.index > last) chunks.push({ type: 't', v: s.slice(last, m.index) });
        const raw = m[0];
        const norm = normalizarBuscaLocal(raw);
        const hit = termsNorm.some(t =>
            norm === t ||
            (t.length >= 2 && norm.startsWith(t)) ||
            (norm.length >= 2 && t.startsWith(norm)) ||
            norm.includes(t) ||
            t.includes(norm)
        );
        chunks.push({ type: hit ? 'h' : 't', v: raw });
        last = m.index + raw.length;
    }
    if (last < s.length) chunks.push({ type: 't', v: s.slice(last) });
    return chunks.map(c => {
        const e = escapeHtml(c.v);
        return c.type === 'h'
            ? `<mark class="bg-amber-100/90 text-slate-900 rounded px-0.5 font-bold">${e}</mark>`
            : e;
    }).join('');
}

function tocarSom(tipo = 'add') {
    try {
        const AudioContextClass = window.AudioContext || window.webkitAudioContext;
        if (!AudioContextClass) return;

        const ctx = new AudioContextClass();
        const oscillator = ctx.createOscillator();
        const gain = ctx.createGain();

        oscillator.connect(gain);
        gain.connect(ctx.destination);

        if (tipo === 'add') {
            oscillator.type = 'triangle';
            oscillator.frequency.setValueAtTime(880, ctx.currentTime);
            oscillator.frequency.exponentialRampToValueAtTime(1240, ctx.currentTime + 0.08);
            gain.gain.setValueAtTime(0.001, ctx.currentTime);
            gain.gain.exponentialRampToValueAtTime(0.12, ctx.currentTime + 0.01);
            gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.12);
            oscillator.start(ctx.currentTime);
            oscillator.stop(ctx.currentTime + 0.12);
        } else if (tipo === 'erro') {
            oscillator.type = 'sawtooth';
            oscillator.frequency.setValueAtTime(240, ctx.currentTime);
            gain.gain.setValueAtTime(0.001, ctx.currentTime);
            gain.gain.exponentialRampToValueAtTime(0.08, ctx.currentTime + 0.01);
            gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.16);
            oscillator.start(ctx.currentTime);
            oscillator.stop(ctx.currentTime + 0.16);
        }
    } catch (e) {
        console.warn('Som indisponível:', e);
    }
}

function mostrarStatusBusca(texto, cor = 'slate') {
    statusBusca.classList.remove('hidden', 'text-slate-400', 'text-emerald-600', 'text-orange-500', 'text-red-500');

    if (cor === 'emerald') statusBusca.classList.add('text-emerald-600');
    else if (cor === 'orange') statusBusca.classList.add('text-orange-500');
    else if (cor === 'red') statusBusca.classList.add('text-red-500');
    else statusBusca.classList.add('text-slate-400');

    statusBusca.textContent = texto;
}

function esconderStatusBusca() {
    statusBusca.classList.add('hidden');
    statusBusca.textContent = '';
}

function mostrarBannerScanner(texto) {
    if (!scanBanner) return;
    scanBanner.textContent = texto;
    scanBanner.classList.remove('hidden');
    scanBanner.classList.remove('scan-banner');
    void scanBanner.offsetWidth;
    scanBanner.classList.add('scan-banner');

    setTimeout(() => {
        scanBanner.classList.add('hidden');
    }, 1600);
}

function flashScanner() {
    if (!inputBusca) return;
    inputBusca.classList.remove('scanner-flash');
    void inputBusca.offsetWidth;
    inputBusca.classList.add('scanner-flash');
}

function focarBuscaProduto() {
    if (!inputBusca) return;
    inputBusca.focus();
    inputBusca.select();
}

function focarBuscaCliente() {
    if (!inputCliente) return;
    inputCliente.focus();
    inputCliente.select();
}

function limparBuscaVisual() {
    if (autoList) autoList.classList.add('hidden');
    if (destaqueContainer) destaqueContainer.innerHTML = '';
    if (similaresContainer) similaresContainer.innerHTML = '';
    produtoEmDestaque = null;
    indexSelecionado = -1;
    sugestoesBuscaCompletas = [];
    limiteSugestoesVisivel = BUSCA_SUG_LIM_INI;
    ultimoTermoBuscaPalavras = [];
}

function atualizarSelecaoVisual(itens) {
    itens.forEach((item, idx) => {
        if (idx === indexSelecionado) item.classList.add('selected-suggestion');
        else item.classList.remove('selected-suggestion');
    });
}

function atualizarSelecaoVisualCliente(itens) {
    itens.forEach((item, idx) => {
        if (idx === indexSelecionadoCliente) item.classList.add('selected-suggestion');
        else item.classList.remove('selected-suggestion');
    });
}

function obterQuantidadeRapida(texto) {
    const match = String(texto || '').trim().match(/(?:\*|x)(\d+)$/i);
    if (!match) return 1;
    const qtd = parseInt(match[1], 10);
    return (!qtd || qtd < 1) ? 1 : qtd;
}

function removerSufixoQuantidade(texto) {
    return String(texto || '').trim().replace(/(?:\*|x)\d+$/i, '').trim();
}

function renderizarSugestoes() {
    similaresContainer.innerHTML = '';
    autoList.innerHTML = '';
    autoList.classList.add('hidden');

    const PLACEHOLDER = (AGRO_PDV_ASSETS.placeholderProduto || '');
    const lapiz = `<svg xmlns="http://www.w3.org/2000/svg" class="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" d="m16.862 4.487 1.687-1.688a1.875 1.875 0 1 1 2.652 2.652L10.582 16.07a4.5 4.5 0 0 1-1.897 1.13L6 18l.8-2.685a4.5 4.5 0 0 1 1.13-1.897l8.932-8.931Zm0 0L19.5 7.125M18 14v4.75A2.25 2.25 0 0 1 15.75 21H5.25A2.25 2.25 0 0 1 3 18.75V8.25A2.25 2.25 0 0 1 5.25 6H10" /></svg>`;
    const iconeCopiar = `<svg xmlns="http://www.w3.org/2000/svg" class="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" d="M15.666 3.888A2.25 2.25 0 0 0 13.5 2.25h-3c-1.03 0-1.9.693-2.166 1.638m7.332 0c.055.194.084.4.084.612v0a.75.75 0 0 1-.75.75H9a.75.75 0 0 1-.75-.75v0c0-.212.03-.418.084-.612m7.332 0c.646.049 1.288.11 1.927.184 1.1.128 1.907 1.077 1.907 2.185V19.5a3 3 0 0 1-3 3H6.75a3 3 0 0 1-3-3V8.197c0-1.108.806-2.057 1.907-2.185a48.208 48.208 0 0 1 1.927-.184" /></svg>`;

    sugestoesAtuais.forEach((s, idx) => {
        const foto = s.imagem && s.imagem !== '' && s.imagem !== 'None' ? s.imagem : PLACEHOLDER;
        const div = document.createElement('div');
        div.className =
            'suggestion-item group flex flex-row items-stretch gap-2 sm:gap-2.5 px-2 py-1.5 sm:px-2.5 sm:py-2 rounded-xl bg-white border border-slate-200/70 shadow-sm hover:shadow hover:border-slate-300/60 cursor-pointer transition-[box-shadow,border-color] duration-200';

        const marcaRaw = (s.marca && String(s.marca).trim()) ? String(s.marca).trim() : '';
        const catRaw = (s.categoria && String(s.categoria).trim()) ? String(s.categoria).trim() : '';
        const gm = String(s.codigo_nfe || s.codigo || '').trim();
        const codigoCopiar = gm || String(s.codigo_barras || '').trim();
        const c = Number(s.saldo_centro || 0);
        const v = Number(s.saldo_vila || 0);
        const totalEstoque = c + v;
        const saldoRefC = Number(s.saldo_erp_centro ?? s.saldo_centro ?? 0);
        const saldoRefV = Number(s.saldo_erp_vila ?? s.saldo_vila ?? 0);
        const codAjuste = String(gm || s.codigo_barras || s.id || '');
        const precoGrande = formatarValorPrecoGrande(s.preco_venda);
        const saldoPiso = (n) => String(Math.floor(Number(n) || 0));

        const metaPartes = [];
        if (gm) metaPartes.push(`<span class="text-slate-500">GM <strong class="font-bold text-slate-800">${escapeHtml(gm)}</strong></span>`);
        if (catRaw) metaPartes.push(`<span class="text-slate-500 truncate max-w-[10rem]" title="${escapeHtml(catRaw)}">${escapeHtml(catRaw)}</span>`);
        if (marcaRaw) metaPartes.push(`<span class="text-slate-500 truncate max-w-[8rem] font-medium text-slate-600" title="${escapeHtml(marcaRaw)}">${escapeHtml(marcaRaw)}</span>`);
        const metaLinha = metaPartes.length
            ? metaPartes.join('<span class="text-slate-300 select-none" aria-hidden="true">·</span>')
            : '<span class="text-slate-300 text-[11px]" title="A conferir">—</span>';

        div.innerHTML = `
            <figure class="shrink-0 w-11 h-11 sm:w-12 sm:h-12 rounded-lg bg-slate-100 overflow-hidden ring-1 ring-inset ring-slate-200/70 flex items-center justify-center p-0.5 self-center">
                <img src="${foto}" alt="" onerror="this.src=(window.AGRO_PDV_BOOTSTRAP && window.AGRO_PDV_BOOTSTRAP.assets ? window.AGRO_PDV_BOOTSTRAP.assets.placeholderProduto : '')" class="w-full h-full object-contain" />
            </figure>
            <div class="min-w-0 flex-1 flex flex-col gap-1 justify-center">
                <h3 class="w-full font-black text-base sm:text-lg text-slate-900 leading-tight line-clamp-2 pr-1">${destacarNomePDV(s.nome, ultimoTermoBuscaPalavras)}</h3>
                <div class="flex w-full flex-wrap items-center justify-between gap-x-2 gap-y-1">
                    <div class="flex flex-wrap items-center gap-x-1.5 gap-y-0 min-w-0 text-[10px] sm:text-[11px] leading-tight">
                        <button type="button" data-copy-gm class="inline-flex items-center justify-center w-6 h-6 rounded-full bg-slate-100 text-slate-600 hover:bg-emerald-100 hover:text-emerald-800 active:scale-95 transition-all shrink-0" title="Copiar código GM" aria-label="Copiar código">${iconeCopiar}</button>
                        <span data-copy-feedback class="hidden text-[10px] font-bold text-emerald-600 shrink-0">Copiado</span>
                        <span class="min-w-0 flex flex-wrap items-center gap-x-1.5">${metaLinha}</span>
                    </div>
                    <div class="flex flex-wrap items-center justify-end gap-1.5 sm:gap-2 shrink-0 ml-auto">
                        <div class="flex items-center gap-1 rounded-md bg-emerald-600/10 px-1.5 py-0.5 ring-1 ring-emerald-500/35">
                            <span class="text-[9px] font-black uppercase text-emerald-900">Centro</span>
                            <span class="text-base sm:text-lg font-black text-emerald-950 tabular-nums leading-none">${saldoPiso(c)}</span>
                            <button type="button" data-adj-c class="inline-flex items-center justify-center w-6 h-6 rounded-md bg-white/90 text-emerald-800 ring-1 ring-emerald-300/60 hover:bg-emerald-50 transition-colors shrink-0" aria-label="Ajustar Centro">${lapiz}</button>
                        </div>
                        <div class="flex items-center gap-1 rounded-md bg-slate-100/90 px-1.5 py-0.5 ring-1 ring-slate-200/80">
                            <span class="text-[8px] font-bold uppercase text-slate-500">Vila</span>
                            <span class="text-xs font-semibold text-slate-700 tabular-nums">${saldoPiso(v)}</span>
                            <button type="button" data-adj-v class="inline-flex items-center justify-center w-6 h-6 rounded-md bg-white text-slate-600 ring-1 ring-slate-200 hover:bg-slate-50 transition-colors shrink-0" aria-label="Ajustar Vila Elias">${lapiz}</button>
                        </div>
                        <span class="text-[9px] text-slate-400 font-semibold tabular-nums px-1 py-0.5 rounded bg-slate-50 ring-1 ring-slate-100">Σ ${saldoPiso(totalEstoque)}</span>
                    </div>
                </div>
            </div>
            <div class="shrink-0 self-stretch flex flex-col items-end justify-center pl-2 sm:pl-3 border-l border-slate-100 min-w-[4.5rem] sm:min-w-[5rem]">
                <span class="text-[9px] font-semibold text-slate-400 leading-none">R$</span>
                <span class="text-lg sm:text-xl font-black text-slate-900 tabular-nums tracking-tight leading-none mt-0.5 text-right">${precoGrande}</span>
            </div>
        `;

        const btnCopy = div.querySelector('[data-copy-gm]');
        const elFb = div.querySelector('[data-copy-feedback]');
        if (btnCopy && codigoCopiar) {
            btnCopy.addEventListener('click', e => {
                e.stopPropagation();
                e.preventDefault();
                copiarCodigoComFeedback(codigoCopiar, elFb);
            });
        } else if (btnCopy) {
            btnCopy.classList.add('opacity-40', 'pointer-events-none');
        }

        div.querySelectorAll('[data-adj-c]').forEach(btn => {
            btn.addEventListener('click', e => {
                e.stopPropagation();
                e.preventDefault();
                abrirAjuste(s.id, s.nome, saldoRefC, codAjuste, 'centro');
            });
        });
        div.querySelectorAll('[data-adj-v]').forEach(btn => {
            btn.addEventListener('click', e => {
                e.stopPropagation();
                e.preventDefault();
                abrirAjuste(s.id, s.nome, saldoRefV, codAjuste, 'vila');
            });
        });
        div.addEventListener('click', () => {
            if (pdvMaisVSlotAlvo !== null && pdvMaisVSlotAlvo >= 0) {
                atribuirProdutoAoSlotMaisVendidos(s, pdvMaisVSlotAlvo);
                produtoEmDestaque = s;
                indexSelecionado = idx;
                renderizarSugestoes();
                return;
            }
            produtoEmDestaque = s;
            indexSelecionado = idx;
            renderizarSugestoes();
        });
        div.addEventListener('dblclick', (ev) => {
            ev.preventDefault();
            adicionarProdutoComQuantidade(s.id, s.nome, s.preco_venda, quantidadeRapida, s);
        });
        const selected = (indexSelecionado === idx) || (produtoEmDestaque && String(produtoEmDestaque.id) === String(s.id));
        if (selected) div.classList.add('selected-suggestion');
        similaresContainer.appendChild(div);
    });

    if (sugestoesBuscaCompletas.length > sugestoesAtuais.length) {
        const wrap = document.createElement('div');
        wrap.className = 'px-2 pt-1 pb-2';
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className =
            'w-full py-2.5 rounded-xl border-2 border-dashed border-slate-200 bg-slate-50 text-[11px] font-black uppercase tracking-wide text-slate-600 hover:border-emerald-300 hover:bg-emerald-50/80 hover:text-emerald-900 transition-colors';
        const restante = sugestoesBuscaCompletas.length - sugestoesAtuais.length;
        const prox = Math.min(BUSCA_SUG_LIM_PAG, restante);
        btn.textContent = `Ver mais ${prox} · ${sugestoesAtuais.length} de ${sugestoesBuscaCompletas.length}`;
        btn.addEventListener('click', (e) => {
            e.preventDefault();
            limiteSugestoesVisivel = Math.min(
                limiteSugestoesVisivel + BUSCA_SUG_LIM_PAG,
                sugestoesBuscaCompletas.length
            );
            sugestoesAtuais = sugestoesBuscaCompletas.slice(0, limiteSugestoesVisivel);
            indexSelecionado = Math.min(indexSelecionado, sugestoesAtuais.length - 1);
            produtoEmDestaque = sugestoesAtuais[indexSelecionado] || sugestoesAtuais[0] || null;
            renderizarSugestoes();
            mostrarStatusBusca(
                `${sugestoesAtuais.length} de ${sugestoesBuscaCompletas.length} produto(s)`,
                'slate'
            );
        });
        wrap.appendChild(btn);
        similaresContainer.appendChild(wrap);
    }
}

function renderDestaque(p) {
    produtoEmDestaque = p;
    const ix = sugestoesAtuais.findIndex(x => String(x.id) === String(p.id));
    indexSelecionado = ix >= 0 ? ix : 0;
    if (ix >= 0) sugestoesAtuais[ix] = { ...sugestoesAtuais[ix], ...p };
    renderizarSugestoes();
}

function renderSimilar(_p) {}

function incrementarFrequencia(id) {
    frequenciaUso[id] = (frequenciaUso[id] || 0) + 1;
    localStorage.setItem('freqProdutos', JSON.stringify(frequenciaUso));
}

function normalizarIdProdutoPdv(id) {
    if (id === undefined || id === null) return '';
    return String(id);
}

function metaOpcoesFromProd(p) {
    if (!p || typeof p !== 'object') return {};
    return {
        codigo_gm: String(p.codigo_nfe || p.codigo_gm || p.codigo || '').trim(),
        prateleira: String(p.prateleira || '').trim(),
    };
}

function addCarrinho(id, nome, preco, qtd = 1, opcoes = {}) {
    const idNorm = normalizarIdProdutoPdv(id);
    if (!idNorm) return;

    const carrinhoEstavaVazio = carrinho.length === 0;

    const agora = Date.now();
    // Trava de 500ms contra leitores duplicados; toques em botões (Mais vendidos etc.) usam skipDebounce.
    if (
        !opcoes.skipDebounce
        && idNorm === ultimoProdutoAdicionadoId
        && (agora - tempoUltimaAdicao) < 500
    ) {
        return;
    }
    ultimoProdutoAdicionadoId = idNorm;
    tempoUltimaAdicao = agora;

    incrementarFrequencia(idNorm);

    const item = carrinho.find((i) => normalizarIdProdutoPdv(i.id) === idNorm);
    const cg = opcoes.codigo_gm != null ? String(opcoes.codigo_gm).trim() : '';
    const pr = opcoes.prateleira != null ? String(opcoes.prateleira).trim() : '';
    if (item) {
        item.qtd += qtd;
        if (cg && !item.codigo_gm) item.codigo_gm = cg;
        if (pr && !item.prateleira) item.prateleira = pr;
    } else {
        carrinho.push({
            id: idNorm,
            nome,
            preco: Number(preco || 0),
            qtd: qtd,
            codigo_gm: cg,
            prateleira: pr,
        });
    }

    atualizarCarrinho();
    tocarSom('add');

    if (carrinhoEstavaVazio && carrinho.length > 0 && typeof pdvDestacarBotoesCarrinho === 'function') {
        pdvDestacarBotoesCarrinho();
    }

    inputBusca.value = '';
        bufferScanner = ''; // Trava de segurança contra leitura duplicada
        clearTimeout(scannerTimer); // Desliga o cronômetro se ainda estiver rodando
    quantidadeRapida = 1;
    limparBuscaVisual();
    esconderStatusBusca();
    focarBuscaProduto();

    if (!opcoes.precoEtiquetaBalanca) {
        validarItemCarrinhoSilencioso(idNorm, preco);
    }
}

function validarItemCarrinhoSilencioso(id, precoLocal) {
    fetch(`/api/buscar-produto-id/${id}/`)
        .then(r => r.json())
        .then(d => {
            if (!d.erro && d.preco_venda !== precoLocal) {
                alert(`⚠️ ATUALIZAÇÃO DE SISTEMA:\nO preço do produto "${d.nome}" sofreu alteração no ERP!\n\nDe: ${formatarMoeda(precoLocal)}\nPara: ${formatarMoeda(d.preco_venda)}\n\nO carrinho foi corrigido automaticamente para evitar perdas.`);
                
                const item = carrinho.find((i) => normalizarIdProdutoPdv(i.id) === normalizarIdProdutoPdv(id));
                if (item) {
                    item.preco = d.preco_venda;
                    atualizarCarrinho();
                }
                
                // Atualiza a base em memória
                const pLocal = baseProdutos.find((p) => normalizarIdProdutoPdv(p.id) === normalizarIdProdutoPdv(id));
                if (pLocal) pLocal.preco_venda = d.preco_venda;
            }
        }).catch(e => console.log('Validação silenciosa falhou ou ignorada:', e));
}

function adicionarProdutoComQuantidade(id, nome, preco, qtd = 1, prodRef = null) {
    addCarrinho(id, nome, preco, qtd, prodRef ? metaOpcoesFromProd(prodRef) : {});
}

function removerItem(i) {
    carrinho.splice(i, 1);
    atualizarCarrinho();
}

function alterarQtdItem(index, delta) {
    const item = carrinho[index];
    if (!item) return;
    item.qtd += delta;
    if (item.qtd < 1) {
        carrinho.splice(index, 1);
        tocarSom('erro');
    }
    atualizarCarrinho();
    if (item && item.qtd >= 1) tocarSom('add');
}

function definirQtdItem(index, val) {
    const n = parseInt(String(val || '1'), 10);
    if (!carrinho[index]) return;
    if (!n || n < 1) {
        carrinho.splice(index, 1);
        tocarSom('erro');
    } else {
        carrinho[index].qtd = n;
    }
    atualizarCarrinho();
}

function removerUltimoItem() {
    if (!carrinho.length) return;
    carrinho.pop();
    atualizarCarrinho();
    tocarSom('erro');
}

function limparCarrinho() {
    if (!carrinho.length) return;
    if (!confirm('Limpar todo o orçamento?')) return;
    carrinho = [];
    if (inputCliente) {
        inputCliente.value = CLIENTE_PADRAO_PDV;
    }
    clienteSelecionado = null;
    const fp = document.getElementById('forma-pagamento-pdv');
    if (fp) fp.value = '';
    const chkEnt = document.getElementById('pdv-orcamento-entrega');
    if (chkEnt) chkEnt.checked = false;
    atualizarCarrinho();
    focarBuscaProduto();
}


function atualizarCarrinho() {
    const container = document.getElementById('itens-carrinho');
    const badge = document.getElementById('itens-badge');
    const cartShell = document.getElementById('cart-shell');

    let total = 0;
    let qtdItens = 0;
    container.innerHTML = '';

    carrinho.forEach((item, index) => {
        total += item.preco * item.qtd;
        qtdItens += item.qtd;

        container.innerHTML += `
            <div class="flex gap-2 py-2 px-2 rounded-lg border border-slate-100 bg-white items-start shadow-sm">
                <div class="min-w-0 flex-1">
                    <div class="text-slate-900 font-bold text-xs leading-snug line-clamp-2">${escapeHtml(item.nome)}</div>
                    <div class="text-[10px] font-semibold text-slate-500 mt-0.5">${formatarMoeda(item.preco)} <span class="text-slate-400">/ un.</span></div>
                </div>
                <div class="flex flex-col items-end gap-1 shrink-0">
                    <div class="flex items-center gap-0.5">
                        <button type="button" onclick="alterarQtdItem(${index}, -1)" class="w-8 h-8 rounded-lg bg-slate-100 hover:bg-slate-200 font-black text-base leading-none" aria-label="Menos">−</button>
                        <input type="number" min="1" value="${item.qtd}" class="w-10 text-center font-black border border-slate-200 rounded-lg py-1 text-xs" onchange="definirQtdItem(${index}, this.value)" />
                        <button type="button" onclick="alterarQtdItem(${index}, 1)" class="w-8 h-8 rounded-lg bg-slate-100 hover:bg-slate-200 font-black text-base leading-none" aria-label="Mais">+</button>
                    </div>
                    <span class="text-emerald-600 font-black text-xs tabular-nums">${formatarMoeda(item.preco * item.qtd)}</span>
                    <button type="button" onclick="removerItem(${index})" class="text-[9px] font-black uppercase text-red-500 hover:text-red-700">Remover</button>
                </div>
            </div>
        `;
    });

    if (!qtdItens) {
        container.innerHTML = '<div class="py-8 text-center text-xs font-bold text-slate-400 px-3">Vazio — busque e Enter para adicionar.</div>';
    }

    document.getElementById('total-geral').innerText = formatarMoeda(total);
    const stripTot = document.getElementById('pdv-carrinho-strip-total');
    if (stripTot) stripTot.textContent = formatarMoeda(total);
    const drawerMeta = document.getElementById('pdv-drawer-cart-meta');
    if (drawerMeta) {
        drawerMeta.textContent = qtdItens
            ? `${qtdItens} item${qtdItens === 1 ? '' : 's'}`
            : 'Nenhum item';
    }
    if (badge) {
        badge.innerHTML = qtdItens > 0
            ? `<span class="tabular-nums text-sm leading-none font-black">${qtdItens}</span><span class="text-[7px] uppercase font-bold opacity-90 leading-none">itens</span>`
            : `<span class="tabular-nums text-sm leading-none font-black">0</span><span class="text-[7px] uppercase font-bold opacity-90 leading-none">itens</span>`;
        if (qtdItens > 0) {
            badge.classList.remove('cart-badge-empty');
            badge.classList.add('cart-badge-active');
        } else {
            badge.classList.add('cart-badge-empty');
            badge.classList.remove('cart-badge-active');
        }
    }

    if (qtdItens > 0) {
        cartShell?.classList.remove('cart-shell-empty');
        cartShell?.classList.add('cart-shell-active');
    } else {
        cartShell?.classList.add('cart-shell-empty');
        cartShell?.classList.remove('cart-shell-active');
    }

    const shell = document.getElementById('cart-shell');
    if (shell && qtdItens > 0) {
        shell.classList.remove('pulse-fast');
        void shell.offsetWidth;
        shell.classList.add('pulse-fast');
    }
}

async function irParaCheckout() {
    if (!carrinho.length) {
        tocarSom('erro');
        return alert("Carrinho vazio!");
    }
    var fp = document.getElementById('forma-pagamento-pdv');
    var body = {
        itens: carrinho,
        cliente: nomeClientePdv(),
        cliente_extra: clienteSelecionado,
        forma_pagamento: fp && fp.value ? fp.value : ''
    };
    try {
        var res = await fetch(AGRO_PDV_URLS.apiPdvSalvarCheckoutDraft, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': AGRO_PDV_BOOTSTRAP.csrfToken || ''
            },
            body: JSON.stringify(body)
        });
        var j = await res.json();
        if (j.ok) {
            window.location.href = AGRO_PDV_URLS.pdvCheckout;
        } else {
            alert(j.erro || 'Não foi possível abrir a tela de fechamento.');
        }
    } catch (e) {
        alert('Erro de rede ao salvar o rascunho da venda.');
    }
}

// --- HISTÓRICO LOCAL ---
function salvarHistoricoLocal(extra) {
    if (!carrinho.length) return;
    extra = extra || {};
    let historico = [];
    try {
        const salvo = localStorage.getItem('historicoOrcamentos');
        if (salvo) historico = JSON.parse(salvo);
        if (!Array.isArray(historico)) historico = [];
    } catch(e) {
        historico = [];
    }
    
    const fpEl = document.getElementById('forma-pagamento-pdv');
    const idOrc = extra && extra.orcId != null ? Number(extra.orcId) : Date.now();
    const novo = {
        id: idOrc,
        orc_barcode: pdvCodigoBarrasOrcamento(idOrc),
        data: new Date().toLocaleString('pt-BR'),
        cliente: nomeClientePdv(),
        total: document.getElementById('total-geral').innerText,
        itens: JSON.parse(JSON.stringify(carrinho)),
        forma_pagamento: fpEl && fpEl.value ? fpEl.value : '',
        entrega: !!extra.entrega,
        cliente_extra:
            clienteSelecionado && typeof clienteSelecionado === 'object'
                ? JSON.parse(JSON.stringify(clienteSelecionado))
                : null,
    };
    historico.unshift(novo);
    if (historico.length > 20) historico.pop();
    localStorage.setItem('historicoOrcamentos', JSON.stringify(historico));
    renderizarHistoricoResumido();
}

function registrarPedidoEntregaServidor(orcId, extra) {
    extra = extra || {};
    const itens = carrinho.map(function (it) {
        const m = pdvMetaItemCarrinho(it);
        return {
            codigo_gm: m.codigo_gm,
            nome: it.nome,
            qtd: it.qtd,
            prateleira: m.prateleira,
            preco: it.preco != null ? it.preco : undefined,
        };
    });
    let operador = '';
    try { operador = (localStorage.getItem('gm_sspin_operador') || '').trim(); } catch (e) {}
    const totalEl = document.getElementById('total-geral');
    const fpEl = document.getElementById('forma-pagamento-pdv');
    const forma =
        extra.forma_pagamento != null && extra.forma_pagamento !== undefined
            ? String(extra.forma_pagamento)
            : fpEl && fpEl.value
              ? fpEl.value
              : '';
    let trocoPrecisa = extra.troco_precisa;
    if (trocoPrecisa === undefined) trocoPrecisa = null;
    const body = {
        orc_local_id: orcId,
        cliente_nome: nomeClientePdv(),
        telefone: clienteSelecionado && clienteSelecionado.telefone ? String(clienteSelecionado.telefone).trim() : '',
        endereco_linha: enderecoLinhaClientePdv(clienteSelecionado),
        plus_code: clienteSelecionado && clienteSelecionado.plus_code ? String(clienteSelecionado.plus_code).trim() : '',
        referencia_rural: clienteSelecionado && clienteSelecionado.referencia_rural ? String(clienteSelecionado.referencia_rural).trim() : '',
        maps_url_manual: clienteSelecionado && clienteSelecionado.maps_url_manual ? String(clienteSelecionado.maps_url_manual).trim() : '',
        itens: itens,
        total_texto: totalEl ? totalEl.innerText : '',
        retomar_codigo: pdvCodigoBarrasOrcamento(orcId),
        operador: operador,
        forma_pagamento: forma,
        troco_precisa: forma === 'Dinheiro' ? trocoPrecisa : null,
    };
    const capk = pdvClienteAgroPkSelecionado();
    if (capk != null) body.cliente_agro_id = capk;
    return fetch(AGRO_PDV_URLS.apiEntregaRegistrar, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': AGRO_PDV_BOOTSTRAP.csrfToken || '' },
        body: JSON.stringify(body),
    }).then(function (r) { return r.json(); }).catch(function () { return { ok: false }; });
}

async function salvarOrcamentoManual() {
    if (!carrinho.length) {
        tocarSom('erro');
        return alert('Carrinho vazio — adicione itens antes de salvar o orçamento.');
    }
    const chkEnt = document.getElementById('pdv-orcamento-entrega');
    const comEntrega = chkEnt && chkEnt.checked;
    const orcId = Date.now();
    const fpEl = document.getElementById('forma-pagamento-pdv');
    const fp = fpEl && fpEl.value ? fpEl.value : '';
    let trocoPrecisa = null;
    if (comEntrega && fp === 'Dinheiro') {
        trocoPrecisa = await pdvModalPerguntaEntrega(
            'Precisa de troco?',
            'Sim = levar troco para o cliente. Não = pagamento em valor exato (sem troco).',
            'Dinheiro'
        );
    }
    salvarHistoricoLocal({ entrega: comEntrega, orcId });
    tocarSom('add');
    if (comEntrega) {
        const escImp = await pdvModalEscolhaImpressaoEntrega();
        if (escImp) imprimirPacoteEntregaTresViasPdv(orcId, escImp);
        const zap = await pdvModalPerguntaEntrega(
            'Enviar pelo WhatsApp?',
            'Abre uma nova aba com o WhatsApp da loja e o texto pronto: cliente, telefone, endereço ou Plus Code, link do Maps e lista de itens.',
            'WhatsApp'
        );
        if (zap) abrirWhatsappSeparacaoPdv(orcId);
        const reg = await registrarPedidoEntregaServidor(orcId, {
            forma_pagamento: fp,
            troco_precisa: trocoPrecisa,
        });
        if (!reg || !reg.ok) {
            alert((reg && reg.erro) ? reg.erro : 'Não foi possível registrar o pedido no painel Entregas.');
        }
    }
    alert(
        comEntrega
            ? 'Orçamento salvo com entrega. O pedido foi registrado no painel Entregas (menu). Bipe ' + pdvCodigoBarrasOrcamento(orcId) + ' no buscador para retomar. F6 lista orçamentos.'
            : 'Orçamento salvo neste navegador. Abra Orçamentos (F6) para listar ou recuperar.'
    );
}

function abrirHistoricoLocal() {
    const container = document.getElementById('lista-historico-local');
    let historico = [];
    try { historico = JSON.parse(localStorage.getItem('historicoOrcamentos') || '[]'); } catch(e) { historico = []; }
    container.innerHTML = '';
    if (historico.length === 0) {
        container.innerHTML = '<div class="text-center text-slate-400 py-10 font-bold text-sm">Nenhum orçamento emitido hoje.</div>';
    } else {
        historico.forEach(h => {
            container.innerHTML += `
                <div class="bg-slate-50 border border-slate-200 p-4 rounded-xl flex justify-between items-center hover:bg-slate-100 transition-colors">
                    <div>
                        <div class="font-black text-slate-700 text-sm uppercase">${escapeHtml(h.cliente)}${h.entrega ? ' <span class="text-sky-600">· Entrega</span>' : ''}</div>
                        <div class="text-xs text-slate-500 font-bold">${h.data} • ${h.itens.length} itens</div>
                    </div>
                    <div class="flex items-center gap-4">
                        <span class="font-black text-emerald-600 text-lg">${h.total}</span>
                        <button onclick="recuperarOrcamento(${h.id})" class="bg-sky-500 hover:bg-sky-600 text-white px-3 py-1.5 rounded-lg text-xs font-black uppercase shadow-sm active:scale-95">Recuperar Orçamento</button>
                    </div>
                </div>
            `;
        });
    }
    document.getElementById('modal-historico-vendas').classList.remove('hidden');
    document.getElementById('modal-historico-vendas').classList.add('flex');
    document.body.classList.add('modal-open');
}

function fecharHistoricoLocal() {
    document.getElementById('modal-historico-vendas').classList.add('hidden');
    document.getElementById('modal-historico-vendas').classList.remove('flex');
    document.body.classList.remove('modal-open');
}

function recuperarOrcamento(id) {
    let historico = [];
    try { historico = JSON.parse(localStorage.getItem('historicoOrcamentos') || '[]'); } catch(e) { return; }
    const h = historico.find(x => Number(x.id) === Number(id));
    if (h) {
        if (carrinho.length > 0 && !confirm("Isso vai substituir o carrinho atual. Deseja continuar?")) return;
        carrinho = h.itens;
        document.getElementById('nome-cliente').value = ehClienteGenericoPdv(h.cliente)
            ? CLIENTE_PADRAO_PDV
            : h.cliente;
        clienteSelecionado =
            h.cliente_extra && typeof h.cliente_extra === 'object' ? h.cliente_extra : null;
        const fpRec = document.getElementById('forma-pagamento-pdv');
        if (fpRec && Object.prototype.hasOwnProperty.call(h, 'forma_pagamento')) {
            fpRec.value = h.forma_pagamento || '';
        }
        const chkRec = document.getElementById('pdv-orcamento-entrega');
        if (chkRec) chkRec.checked = !!h.entrega;
        atualizarCarrinho();
        fecharHistoricoLocal();
    }
}

/** Retomada por código de barras GMORC… no buscador / leitor. */
function recuperarOrcamentoSilenciosoPorId(oid) {
    let historico = [];
    try {
        historico = JSON.parse(localStorage.getItem('historicoOrcamentos') || '[]');
    } catch (e) {
        return false;
    }
    const h = historico.find((x) => Number(x.id) === Number(oid));
    if (!h) {
        mostrarBannerScanner('Orçamento não encontrado para este código.');
        tocarSom('erro');
        return false;
    }
    if (carrinho.length > 0 && !confirm('Substituir o carrinho pelo orçamento salvo (cód. ' + oid + ')?')) {
        return false;
    }
    carrinho = JSON.parse(JSON.stringify(h.itens));
    document.getElementById('nome-cliente').value = ehClienteGenericoPdv(h.cliente)
        ? CLIENTE_PADRAO_PDV
        : h.cliente;
    clienteSelecionado =
        h.cliente_extra && typeof h.cliente_extra === 'object' ? h.cliente_extra : null;
    const fpRec = document.getElementById('forma-pagamento-pdv');
    if (fpRec && Object.prototype.hasOwnProperty.call(h, 'forma_pagamento')) {
        fpRec.value = h.forma_pagamento || '';
    }
    const chkRec = document.getElementById('pdv-orcamento-entrega');
    if (chkRec) chkRec.checked = !!h.entrega;
    atualizarCarrinho();
    mostrarBannerScanner('Orçamento recuperado — finalize no caixa (F8).');
    tocarSom('add');
    focarBuscaProduto();
    return true;
}

function gmCsrfTokenParaFetch() {
    var m = document.querySelector('meta[name=csrfmiddlewaretoken]');
    if (m && m.getAttribute('content')) return m.getAttribute('content');
    var c = document.cookie.match(/(?:^|; )csrftoken=([^;]*)/);
    return c ? decodeURIComponent(c[1]) : (AGRO_PDV_BOOTSTRAP.csrfToken || '');
}

function posicionarSetaBalaoEstoque() {
    var wrap = document.getElementById('agro-barra-estoque-wrap');
    var root = document.getElementById('pdv-balao-aviso-estoque');
    if (!wrap || !root) return;
    var card = root.querySelector('.pdv-balao-estoque-card');
    if (!card) return;
    var wr = wrap.getBoundingClientRect();
    var cr = card.getBoundingClientRect();
    if (cr.width < 1) return;
    var x = wr.left + wr.width / 2 - cr.left;
    x = Math.max(36, Math.min(x, cr.width - 36));
    card.style.setProperty('--pdv-balao-arrow-x', Math.round(x) + 'px');
}

function mostrarBalaoAvisoEstoquePdv(mensagem) {
    var root = document.getElementById('pdv-balao-aviso-estoque');
    var txt = document.getElementById('pdv-balao-aviso-estoque-texto');
    var estBtn = document.getElementById('agro-btn-atualizar-saldos');
    if (txt) txt.textContent = mensagem || '';
    if (root) {
        root.classList.remove('hidden');
        requestAnimationFrame(function () {
            posicionarSetaBalaoEstoque();
        });
    }
    if (estBtn) {
        estBtn.classList.add('ring-4', 'ring-amber-400', 'ring-offset-2', 'animate-pulse');
        try {
            estBtn.scrollIntoView({ behavior: 'smooth', block: 'nearest', inline: 'center' });
        } catch (e) {}
    }
}

function fecharBalaoAvisoEstoquePdv() {
    var root = document.getElementById('pdv-balao-aviso-estoque');
    var estBtn = document.getElementById('agro-btn-atualizar-saldos');
    if (root) root.classList.add('hidden');
    if (estBtn) estBtn.classList.remove('ring-4', 'ring-amber-400', 'ring-offset-2', 'animate-pulse');
}

/** Exige leitura recente de saldos (botão Estoque), alinhado ao termômetro da barra. */
function pdvEstoqueLeituraRecenteParaAjuste() {
    var lab = document.getElementById('agro-saldos-ultima-atualizacao');
    if (!lab) return { ok: true };
    var freshAt = lab.dataset && lab.dataset.gmFreshAt ? parseInt(lab.dataset.gmFreshAt, 10) : 0;
    if (!freshAt) {
        return {
            ok: false,
            msg: 'Atualize o estoque antes de ajustar: clique no botão verde «Estoque» na barra superior e aguarde aparecer a data em «Última leitura». Depois disso você pode usar o lápis para ajustar o saldo.',
        };
    }
    var staleMs =
        typeof AgroEstoqueSync !== 'undefined' && AgroEstoqueSync.staleMsDefault
            ? AgroEstoqueSync.staleMsDefault
            : 600000;
    if (Date.now() - freshAt > staleMs) {
        return {
            ok: false,
            msg: 'Os saldos estão desatualizados (passou o tempo recomendado). Clique em «Estoque» para atualizar de novo e só então faça o ajuste.',
        };
    }
    return { ok: true };
}

var __pdvAjustePendente = null;

function fecharModalAjustePdv() {
    var m = document.getElementById('modal-pdv-ajuste-estoque');
    if (!m) return;
    window.__pdvSuprimirAtualizacaoSaldosSegundoPlano = false;
    __pdvAjustePendente = null;
    m.classList.add('hidden');
    m.classList.remove('flex');
}

function confirmarModalAjustePdv() {
    var p = __pdvAjustePendente;
    if (!p) return;
    var novoEl = document.getElementById('pae-novo-saldo');
    var pinEl = document.getElementById('pae-pin');
    var novo = String((novoEl && novoEl.value) || '')
        .trim()
        .replace(',', '.');
    if (novo === '' || Number.isNaN(Number(novo))) {
        alert('Informe um número válido para o novo saldo.');
        if (novoEl) novoEl.focus();
        return;
    }
    var pin = String((pinEl && pinEl.value) || '').trim();
    if (!pin) {
        alert('PIN é obrigatório.');
        if (pinEl) pinEl.focus();
        return;
    }
    var btn = document.getElementById('pae-btn-confirmar');
    var prev = btn ? btn.textContent : '';
    if (btn) {
        btn.disabled = true;
        btn.textContent = 'Enviando…';
    }
    gmEnviarAjustePdvFetch(p, novo, pin, function (ok) {
        if (btn) {
            btn.disabled = false;
            btn.textContent = prev || 'Confirmar ajuste';
        }
    });
}

function gmEnviarAjustePdvFetch(p, novo, pin, done) {
    var fd = new FormData();
    fd.append('produto_id', p.id);
    fd.append('nome_produto', String(p.nome || ''));
    fd.append('codigo_interno', String(p.codigo || ''));
    fd.append('saldo_atual', String(p.saldo != null ? p.saldo : '0'));
    fd.append('novo_saldo', novo);
    fd.append('deposito', String(p.dep || 'centro'));
    fd.append('pin', pin);
    var csrf = gmCsrfTokenParaFetch();
    fetch('/api/ajustar/', {
        method: 'POST',
        body: fd,
        headers: { 'X-CSRFToken': csrf },
        credentials: 'same-origin',
    })
        .then(function (res) {
            var ct = (res.headers.get('content-type') || '').toLowerCase();
            if (ct.indexOf('application/json') !== -1) {
                return res.json().then(function (d) {
                    return { res: res, d: d };
                });
            }
            return res.text().then(function (t) {
                return {
                    res: res,
                    d: { ok: false, erro: t ? t.slice(0, 240) : 'HTTP ' + res.status },
                };
            });
        })
        .then(function (x) {
            var d = x.d;
            if (x.res.ok && d && d.ok) {
                var btnCon = document.getElementById('pae-btn-confirmar');
                if (btnCon) {
                    btnCon.disabled = false;
                    btnCon.textContent = 'Confirmar ajuste';
                }
                fecharModalAjustePdv();
                if (typeof done === 'function') done(true);
                mostrarStatusBusca('Ajuste registrado. Atualizando saldos…', 'emerald');
                setTimeout(function () {
                    pollSaldoPdvUmaVezPromise({ force: true }).then(function () {
                        mostrarStatusBusca('Saldos atualizados.', 'emerald');
                        setTimeout(esconderStatusBusca, 2400);
                    });
                }, 0);
                return;
            }
            var msg = d && d.erro ? d.erro : 'HTTP ' + x.res.status;
            alert('Erro: ' + msg);
            if (typeof done === 'function') done(false);
        })
        .catch(function (e) {
            console.error('[gmEnviarAjustePdvFetch]', e);
            alert('Falha ao enviar ajuste. Verifique a rede ou recarregue o PDV e tente de novo.');
            if (typeof done === 'function') done(false);
        });
}

function abrirAjuste(id, nome, saldo, codigo, dep) {
    var chk = pdvEstoqueLeituraRecenteParaAjuste();
    if (!chk.ok) {
        mostrarBalaoAvisoEstoquePdv(chk.msg);
        return;
    }
    var pid = id != null && id !== '' ? String(id) : '';
    if (!pid) {
        alert('Produto sem identificador. Use «Sincronizar API» ou «Estoque» para atualizar a base e tente de novo.');
        return;
    }
    var m = document.getElementById('modal-pdv-ajuste-estoque');
    if (!m) {
        alert('Interface de ajuste indisponível. Recarregue o PDV.');
        return;
    }
    __pdvAjustePendente = { id: pid, nome: nome, saldo: saldo, codigo: codigo, dep: dep || 'centro' };
    var tit = document.getElementById('pae-titulo-dep');
    var depLabel = __pdvAjustePendente.dep === 'vila' ? 'Vila' : 'Centro';
    if (tit) tit.textContent = 'Ajuste — ' + depLabel;
    var pn = document.getElementById('pae-produto');
    if (pn) pn.textContent = String(nome || '');
    var sa = document.getElementById('pae-saldo-atual');
    if (sa) sa.textContent = String(Math.floor(Number(saldo) || 0));
    var novoEl = document.getElementById('pae-novo-saldo');
    var pinEl = document.getElementById('pae-pin');
    if (novoEl) novoEl.value = '';
    if (pinEl) pinEl.value = '';
    window.__pdvSuprimirAtualizacaoSaldosSegundoPlano = true;
    m.classList.remove('hidden');
    m.classList.add('flex');
    setTimeout(function () {
        if (novoEl) novoEl.focus();
    }, 50);
}

function forcarAtualizacao(produtoId) {
    const button = document.getElementById(`btn-sincronizar-${produtoId}`);
    const originalButtonText = '🔄 Sincronizar [F5]';

    if (button) {
        button.innerHTML = 'Buscando...';
        button.disabled = true;
    }

    fetch(`/api/buscar-produto-id/${produtoId}/`)
        .then(res => {
            if (!res.ok) throw new Error('Produto não encontrado ou erro na API');
            return res.json();
        })
        .then(produtoAtualizado => {
            if (!produtoAtualizado) throw new Error('Resposta da API sem dados do produto.');
            produtoEmDestaque = produtoAtualizado;
            renderDestaque(produtoAtualizado);

            const idx = baseProdutos.findIndex(p => p.id === produtoId);
            if(idx > -1) {
                baseProdutos[idx].preco_venda = produtoAtualizado.preco_venda;
                baseProdutos[idx].saldo_centro = produtoAtualizado.saldo_centro;
                baseProdutos[idx].saldo_vila = produtoAtualizado.saldo_vila;
            }

            // Dá o feedback de Sucesso e volta ao normal
            setTimeout(() => {
                const novoButton = document.getElementById(`btn-sincronizar-${produtoId}`);
                if (novoButton) {
                    novoButton.innerHTML = '✅ Atualizado!';
                    novoButton.classList.replace('text-sky-600', 'text-emerald-600');
                    novoButton.classList.replace('bg-sky-50', 'bg-emerald-50');
                    setTimeout(() => {
                        novoButton.innerHTML = originalButtonText;
                        novoButton.classList.replace('text-emerald-600', 'text-sky-600');
                        novoButton.classList.replace('bg-emerald-50', 'bg-sky-50');
                        novoButton.disabled = false;
                    }, 2500);
                }
            }, 50);
        })
        .catch(error => {
            console.error('Erro ao forçar atualização:', error);
            tocarSom('erro');

            if (button) {
                button.innerHTML = 'Erro!';
                setTimeout(() => {
                    button.innerHTML = originalButtonText;
                    button.disabled = false;
                }, 2000);
            }
        });
}


function obterLembretes() {
    try {
        const dados = JSON.parse(localStorage.getItem('gmLembretesCaixa') || '[]');
        return Array.isArray(dados) ? dados : [];
    } catch (e) {
        return [];
    }
}

function salvarListaLembretes(lista) {
    localStorage.setItem('gmLembretesCaixa', JSON.stringify(lista));
}

function renderizarLembretes() {
    const lembretes = obterLembretes().sort((a, b) => (a.hora || '').localeCompare(b.hora || ''));
    listaLembretes.innerHTML = '';
    if (!lembretes.length) {
        listaLembretes.innerHTML = '<div class="px-3 py-4 text-center text-[11px] font-bold text-slate-400">Nenhum lembrete cadastrado.</div>';
        return;
    }

    lembretes.forEach(lembrete => {
        const row = document.createElement('div');
        const due = !!lembrete.disparado && !lembrete.concluido;
        row.className = `px-3 py-2 ${due ? 'reminder-due' : ''} ${lembrete.concluido ? 'done-reminder' : ''}`;
        row.innerHTML = `
            <div class="flex items-start justify-between gap-2">
                <div class="flex items-start gap-2 min-w-0">
                    <label class="mt-0.5 flex items-center gap-2 cursor-pointer">
                        <input type="checkbox" ${lembrete.concluido ? 'checked' : ''} onchange="alternarLembreteFeito('${lembrete.id}', this.checked)" class="w-4 h-4 accent-sky-600">
                    </label>
                    <div class="min-w-0">
                        <div class="done-text text-[11px] font-black text-slate-700 uppercase truncate">${escapeHtml(lembrete.texto)}</div>
                        <div class="text-[10px] font-bold text-slate-400">${escapeHtml(lembrete.hora || '--:--')} ${lembrete.concluido ? '• feito' : ''}</div>
                    </div>
                </div>
                <button type="button" onclick="removerLembrete('${lembrete.id}')" class="text-[10px] font-black uppercase text-red-500 hover:text-red-600">Remover</button>
            </div>
        `;
        listaLembretes.appendChild(row);
    });
}

function salvarLembrete() {
    const textoInput = document.getElementById('lembrete-texto');
    const horaInput = document.getElementById('lembrete-hora');
    const texto = (textoInput.value || '').trim();
    const hora = horaInput.value || '';
    if (!texto || !hora) {
        alert('Preencha o lembrete e o horário.');
        return;
    }
    const lista = obterLembretes();
    lista.push({ id: String(Date.now()), texto, hora, disparado: false, data: new Date().toISOString().slice(0, 10) });
    salvarListaLembretes(lista);
    textoInput.value = '';
    horaInput.value = '';
    renderizarLembretes();
}

function removerLembrete(id) {
    const lista = obterLembretes().filter(item => item.id !== id);
    salvarListaLembretes(lista);
    renderizarLembretes();
}

function alternarLembreteFeito(id, feito) {
    const lista = obterLembretes();
    const item = lista.find(x => x.id === id);
    if (!item) return;
    item.concluido = !!feito;
    if (feito) item.disparado = false;
    salvarListaLembretes(lista);
    renderizarLembretes();
}

function abrirNovoLembreteRapido() {
    abrirModalLembretes();
}

function tocarSomLembrete() {
    try {
        const AudioContextClass = window.AudioContext || window.webkitAudioContext;
        if (!AudioContextClass) return;
        const audioCtx = new AudioContextClass();
        const tons = [1320, 980, 1320, 1560, 1320, 1560, 1320];
        tons.forEach((freq, idx) => {
            const osc = audioCtx.createOscillator();
            const gain = audioCtx.createGain();
            osc.type = idx % 2 === 0 ? 'square' : 'triangle';
            osc.frequency.setValueAtTime(freq, audioCtx.currentTime);
            osc.connect(gain);
            gain.connect(audioCtx.destination);
            const start = audioCtx.currentTime + idx * 0.11;
            gain.gain.setValueAtTime(0.001, start);
            gain.gain.exponentialRampToValueAtTime(0.26, start + 0.01);
            gain.gain.exponentialRampToValueAtTime(0.001, start + 0.12);
            osc.start(start);
            osc.stop(start + 0.13);
        });
    } catch (e) {}
}

function exibirAlertaLembrete(lembrete) {
    alertaLembreteAtual = lembrete;
    document.getElementById('alerta-lembrete-texto').textContent = `${lembrete.hora} • ${lembrete.texto}`;
    document.getElementById('alerta-lembrete').classList.remove('hidden');
    tocarSomLembrete();
}

function dispensarAlertaLembrete() {
    document.getElementById('alerta-lembrete').classList.add('hidden');
    alertaLembreteAtual = null;
}

function verificarLembretes() {
    const agora = new Date();
    const hoje = agora.toISOString().slice(0, 10);
    const hh = String(agora.getHours()).padStart(2, '0');
    const mm = String(agora.getMinutes()).padStart(2, '0');
    const horaAtual = `${hh}:${mm}`;
    const lista = obterLembretes();
    let alterou = false;

    lista.forEach(item => {
        if (item.data !== hoje) {
            item.data = hoje;
            item.disparado = false;
            alterou = true;
        }
        if (!item.concluido && !item.disparado && item.hora <= horaAtual) {
            item.disparado = true;
            alterou = true;
            exibirAlertaLembrete(item);
        }
    });

    if (alterou) {
        salvarListaLembretes(lista);
        renderizarLembretes();
    }
}

function buscarProdutos(q, modo = 'normal') {
    clearTimeout(debounceTimer);

    quantidadeRapida = obterQuantidadeRapida(q);
    const termoBruto = removerSufixoQuantidade(q);
    const rawOrc = String(termoBruto).replace(/\s/g, '').toUpperCase();
    const mOrc = rawOrc.match(/^GMORC(\d{10,20})$/);
    if (mOrc) {
        const oid = parseInt(mOrc[1], 10);
        if (Number.isFinite(oid)) {
            clearTimeout(debounceTimer);
            if (recuperarOrcamentoSilenciosoPorId(oid)) {
                if (inputBusca) inputBusca.value = '';
                limparBuscaVisual();
                esconderStatusBusca();
            }
            return;
        }
    }

    const termoBusca = normalizarBuscaLocal(termoBruto);

    const minChars = modo === 'scanner' ? 1 : 2;
    if (!termoBusca || termoBusca.trim().length < minChars) {
        limparBuscaVisual();
        esconderStatusBusca();
        return;
    }

    // Base local: resposta imediata; sem base, debounce maior até o JSON chegar
    const delay = modo === 'scanner' ? 0 : (baseProdutos.length > 0 ? 0 : 220);

    debounceTimer = setTimeout(() => {
        if (baseProdutos.length > 0) {
            executarBuscaLocal(termoBusca, modo);
        } else if (window.AGRO_MANUAL_SYNC_ONLY) {
            mostrarStatusBusca('Sem catálogo local. Use “Sincronizar API” antes de buscar.', 'orange');
            limparBuscaVisual();
        } else {
            executarBuscaAPI(termoBruto, modo); // Fallback enquanto a base carrega
        }
    }, delay);
}

function filtrarBuscaLocal(termo, modo) {
    return filtrarProdutosBuscaInteligente(baseProdutos, termo, modo);
}

function mediaParaOrdenacaoPdv(p) {
    const id = String(p.id);
    const loc = (typeof baseProdutos !== 'undefined' && baseProdutos.length)
        ? baseProdutos.find((x) => String(x.id) === id)
        : null;
    if (loc && loc.media_venda_diaria_30d != null && loc.media_venda_diaria_30d !== '') {
        return Number(loc.media_venda_diaria_30d || 0);
    }
    return Number(p.media_venda_diaria_30d || 0);
}

function ordenarSugestoesPdv(lista) {
    /* Média de vendas do catálogo carregado no início do dia (mesmo cache do servidor).
       Não usar só a média que veio no merge da API — evita “tic” e troca de ordem ao mesclar. */
    return [...lista].sort((a, b) => {
        const mA = mediaParaOrdenacaoPdv(a);
        const mB = mediaParaOrdenacaoPdv(b);
        if (mA !== mB) return mB - mA;
        const freqA = frequenciaUso[a.id] || 0;
        const freqB = frequenciaUso[b.id] || 0;
        if (freqA !== freqB) return freqB - freqA;
        return String(a.nome || '').localeCompare(String(b.nome || ''), 'pt-BR');
    });
}

function montarBuscaTextoRapido(p) {
    const partes = [p.nome, p.marca, p.codigo_nfe, p.codigo_barras, p.codigo, p.prateleira].filter(Boolean);
    return normalizarBuscaLocal(partes.join(' '));
}

/** EAN-13 etiqueta balança: 2 + CCCC + 0 + TTTTTT (centavos) + DV — mesmo padrão do ERP (Venda). */
function digitoVerificadorEan13Primeiros12(d12) {
    let sum = 0;
    for (let i = 0; i < 12; i++) {
        const n = parseInt(d12[i], 10);
        sum += (i % 2 === 0) ? n : n * 3;
    }
    const mod = sum % 10;
    return mod === 0 ? 0 : 10 - mod;
}

function parseEtiquetaBalancaEan13(digits13) {
    if (!/^\d{13}$/.test(digits13) || digits13[0] !== '2') return null;
    const codigo4 = digits13.slice(1, 5);
    const valorCent = parseInt(digits13.slice(6, 12), 10);
    if (Number.isNaN(valorCent)) return null;
    const valorReais = Math.round(valorCent) / 100;
    const dvExp = digitoVerificadorEan13Primeiros12(digits13.slice(0, 12));
    const dv = parseInt(digits13[12], 10);
    return {
        codigo4,
        valorCent,
        valorReais,
        checkOk: dv === dvExp,
    };
}

function produtoCombinaCodigoInternoBalanca4(cod4, p) {
    const raw = [p.codigo_nfe, p.codigo, p.codigo_barras];
    const candidatos = new Set([cod4, cod4.replace(/^0+/, '') || '0', cod4.padStart(5, '0'), cod4.padStart(6, '0')]);
    for (const field of raw) {
        const d = String(field ?? '').replace(/\D/g, '');
        if (!d) continue;
        for (const c of candidatos) {
            if (d === c) return true;
            if (d.length >= 4 && d.slice(-4) === cod4) return true;
            if (c.length >= 4 && d.endsWith(c)) return true;
        }
    }
    return false;
}

function encontrarProdutoPorCodigoInternoBalanca(cod4, lista) {
    return lista.find((p) => produtoCombinaCodigoInternoBalanca4(cod4, p)) || null;
}

function enriquecerProdutoBusca(p) {
    const id = String(p.id);
    const loc = baseProdutos.find(x => String(x.id) === id);
    const mediaApi = p.media_venda_diaria_30d;
    if (!loc) {
        const bt = p.busca_texto || montarBuscaTextoRapido(p);
        return {
            ...p,
            busca_texto: bt,
            prateleira: p.prateleira != null && String(p.prateleira).trim() !== '' ? p.prateleira : '',
            media_venda_diaria_30d: Number(
                mediaApi != null && mediaApi !== '' ? mediaApi : 0
            ),
            preco_etiqueta_balanca: !!p.preco_etiqueta_balanca,
        };
    }
    const prat =
        p.prateleira != null && String(p.prateleira).trim() !== ''
            ? String(p.prateleira).trim()
            : String(loc.prateleira || '').trim();
    return {
        ...loc,
        ...p,
        imagem: p.imagem || loc.imagem,
        nome: p.nome || loc.nome,
        prateleira: prat,
        preco_venda: p.preco_venda != null ? Number(p.preco_venda) : loc.preco_venda,
        saldo_centro: p.saldo_centro != null ? p.saldo_centro : loc.saldo_centro,
        saldo_vila: p.saldo_vila != null ? p.saldo_vila : loc.saldo_vila,
        saldo_erp_centro: p.saldo_erp_centro != null ? p.saldo_erp_centro : loc.saldo_erp_centro,
        saldo_erp_vila: p.saldo_erp_vila != null ? p.saldo_erp_vila : loc.saldo_erp_vila,
        busca_texto: loc.busca_texto || p.busca_texto || montarBuscaTextoRapido({ ...loc, ...p, prateleira: prat }),
        media_venda_diaria_30d: Number(
            mediaApi != null && mediaApi !== ''
                ? mediaApi
                : loc.media_venda_diaria_30d || 0
        ),
        preco_etiqueta_balanca: !!(p.preco_etiqueta_balanca || loc.preco_etiqueta_balanca),
    };
}

function extrairPalavrasParaHighlightDaBusca() {
    const q = removerSufixoQuantidade(inputBusca ? inputBusca.value : '');
    const t = normalizarBuscaLocal(q);
    return t.split(/\s+/).filter(Boolean);
}

function mesclarBuscaLocalComOnline(termoBrutoOriginal, modo, locaisOrdenados) {
    if (modo === 'scanner') {
        processarResultadosBusca(locaisOrdenados.slice(0, BUSCA_SUG_LIM_MAX), modo, false, {
            preservarOrdem: true,
        });
        return;
    }
    if (window.AGRO_MANUAL_SYNC_ONLY) {
        if (locaisOrdenados.length) {
            processarResultadosBusca(locaisOrdenados.slice(0, BUSCA_SUG_LIM_MAX), modo, false, { preservarOrdem: true });
        } else {
            mostrarStatusBusca('Sem sugestões locais. Sincronize o catálogo ou refine a busca.', 'orange');
            processarResultadosBusca([], modo, false);
        }
        return;
    }
    clearTimeout(mergeFetchTimer);
    const seq = ++buscaOnlineMergeSeq;
    const map = new Map();
    locaisOrdenados.forEach((p) => map.set(String(p.id), p));
    const ordemLocalIds = locaisOrdenados.map((p) => String(p.id));
    const idsLocal = new Set(ordemLocalIds);
    const hadLocal = locaisOrdenados.length > 0;
    if (hadLocal) {
        processarResultadosBusca(
            locaisOrdenados.slice(0, BUSCA_SUG_LIM_MAX),
            modo,
            false,
            { preservarOrdem: true }
        );
    } else {
        mostrarStatusBusca('Buscando no servidor…', 'slate');
    }
    mergeFetchTimer = setTimeout(() => {
        if (window.gmLoadingBar) window.gmLoadingBar.show();
        fetch('/api/buscar/?q=' + encodeURIComponent(termoBrutoOriginal))
            .then((res) => res.json())
            .then((data) => {
                if (seq !== buscaOnlineMergeSeq) return;
                if (data.erro) throw new Error(data.erro);
                const api = data.produtos || [];
                const apiById = new Map();
                api.forEach((raw) => {
                    const id = String(raw.id);
                    apiById.set(id, raw);
                    if (!map.has(id)) map.set(id, raw);
                });
                const locaisMesclados = ordemLocalIds.map((id) => {
                    const base = map.get(id);
                    const apiRow = apiById.get(id);
                    return apiRow ? { ...base, ...apiRow } : base;
                }).filter(Boolean);
                const extrasBrutos = api.filter((raw) => !idsLocal.has(String(raw.id)));
                const extrasOrd = ordenarSugestoesPdv(extrasBrutos);
                const selId = produtoEmDestaque ? String(produtoEmDestaque.id) : null;
                const final = [...locaisMesclados, ...extrasOrd].slice(0, BUSCA_SUG_LIM_MAX);
                processarResultadosBusca(final, modo, false, {
                    preservarOrdem: true,
                    manterSelecaoId: selId,
                });
            })
            .catch((err) => {
                if (seq !== buscaOnlineMergeSeq) return;
                console.error('Busca online:', err);
                if (!hadLocal) {
                    processarResultadosBusca([], modo, false);
                } else {
                    esconderStatusBusca();
                }
            })
            .finally(() => { if (window.gmLoadingBar) window.gmLoadingBar.hide(); });
    }, hadLocal ? 0 : 220);
}

function executarBuscaLocal(termo, modo) {
    const termoBrutoApi = removerSufixoQuantidade(inputBusca ? inputBusca.value : '');
    const digits = String(termo || '').replace(/\D/g, '');

    if (modo === 'scanner' && digits.length === 13 && digits[0] === '2') {
        const bal = parseEtiquetaBalancaEan13(digits);
        if (bal) {
            if (!bal.checkOk) {
                mostrarBannerScanner('⚠️ Etiqueta inválida (dígito verificador)');
            } else if (baseProdutos.length) {
                const prod = encontrarProdutoPorCodigoInternoBalanca(bal.codigo4, baseProdutos);
                if (prod) {
                    const linha = {
                        ...prod,
                        preco_venda: bal.valorReais,
                        preco_etiqueta_balanca: true,
                    };
                    processarResultadosBusca([linha], modo, true);
                    return;
                }
            }
        }
    }

    let resultados = filtrarBuscaLocal(termo, modo);

    if (modo === 'scanner') {
        const exato = resultados.find(p => {
            const nfe = normalizarBuscaLocal(String(p.codigo_nfe ?? ''));
            const cb = normalizarBuscaLocal(String(p.codigo_barras ?? ''));
            return nfe === termo || cb === termo || casaCodigoNumericoNoProduto(termo, p);
        });
        if (exato) {
            processarResultadosBusca([exato], modo, true);
            return;
        }
    }

    resultados = ordenarSugestoesPdv(resultados);
    mesclarBuscaLocalComOnline(termoBrutoApi, modo, resultados);
}

function executarBuscaAPI(termo, modo) {
    mostrarStatusBusca('Buscando no banco online...', 'slate');
    if (window.gmLoadingBar) window.gmLoadingBar.show();
    fetch("/api/buscar/?q=" + encodeURIComponent(termo))
        .then(res => res.json())
        .then(data => {
            if (data.erro) throw new Error(data.erro);
            processarResultadosBusca(data.produtos || [], modo, data.exact_barcode_match);
        })
        .catch(err => {
            console.error('Erro na busca:', err);
            processarResultadosBusca([], modo, false);
        })
        .finally(() => { if (window.gmLoadingBar) window.gmLoadingBar.hide(); });
}

function processarResultadosBusca(produtosEncontrados, modo, matchExato = false, opcoes = {}) {
    if (matchExato && produtosEncontrados.length === 1) {
        const produto = enriquecerProdutoBusca(produtosEncontrados[0]);
        flashScanner();
        const precoEtiqueta = !!produto.preco_etiqueta_balanca;
        const avisoPreco = precoEtiqueta ? ' (valor da etiqueta)' : '';
        mostrarBannerScanner(`✅ Código lido • ${quantidadeRapida}x ${produto.nome}${avisoPreco}`);
        addCarrinho(
            produto.id,
            produto.nome,
            produto.preco_venda,
            quantidadeRapida,
            {
                ...(precoEtiqueta ? { precoEtiquetaBalanca: true } : {}),
                ...metaOpcoesFromProd(produto),
            }
        );
        mostrarStatusBusca(`Código lido: ${quantidadeRapida}x ${produto.nome}`, 'emerald');
        setTimeout(esconderStatusBusca, 1500);
        return;
    }

    if (produtosEncontrados.length > 0) {
        const enriquecidos = produtosEncontrados.map(enriquecerProdutoBusca);
        const ordenados = opcoes.preservarOrdem
            ? enriquecidos
            : ordenarSugestoesPdv(enriquecidos);
        sugestoesBuscaCompletas = ordenados;
        limiteSugestoesVisivel = BUSCA_SUG_LIM_INI;
        sugestoesAtuais = sugestoesBuscaCompletas.slice(0, limiteSugestoesVisivel);
        ultimoTermoBuscaPalavras = extrairPalavrasParaHighlightDaBusca();
        const manter = opcoes.manterSelecaoId != null ? String(opcoes.manterSelecaoId) : null;
        if (manter) {
            const ix = sugestoesBuscaCompletas.findIndex((x) => String(x.id) === manter);
            if (ix >= 0) {
                produtoEmDestaque = sugestoesBuscaCompletas[ix];
                if (ix < sugestoesAtuais.length) {
                    indexSelecionado = ix;
                } else {
                    indexSelecionado = 0;
                    produtoEmDestaque = sugestoesAtuais[0] || null;
                }
            } else {
                indexSelecionado = 0;
                produtoEmDestaque = sugestoesAtuais[0] || null;
            }
        } else {
            indexSelecionado = 0;
            produtoEmDestaque = sugestoesAtuais[0] || null;
        }
        renderizarSugestoes();

        const total = sugestoesBuscaCompletas.length;
        const vis = sugestoesAtuais.length;
        if (total > vis) {
            mostrarStatusBusca(`${vis} de ${total} produto(s) — toque em “Ver mais” para o restante`, 'slate');
        } else {
            mostrarStatusBusca(`${total} produto(s) encontrado(s)`, 'slate');
        }
    } else {
        sugestoesBuscaCompletas = [];
        limiteSugestoesVisivel = BUSCA_SUG_LIM_INI;
        ultimoTermoBuscaPalavras = [];
        if (autoList) autoList.classList.add('hidden');
        if (destaqueContainer) destaqueContainer.innerHTML = '';
        if (similaresContainer) similaresContainer.innerHTML = '';
        produtoEmDestaque = null;
        sugestoesAtuais = [];
        mostrarStatusBusca('Nenhum produto encontrado', 'red');
        tocarSom('erro');
    }
}

if (inputBusca) {
inputBusca.addEventListener('input', function(e) {
    const q = e.target.value;
    indexSelecionado = -1;
    produtoEmDestaque = null; // Evita adicionar o item anterior se o Enter for pressionado muito rápido

    const agora = Date.now();
    const diff = agora - ultimoInputTime;
    ultimoInputTime = agora;

    const textoLimpo = removerSufixoQuantidade(q);
    const pareceCodigoOrc = /^GMORC\d{10,20}$/i.test(String(textoLimpo).replace(/\s/g, ''));
    const pareceCodigo = /^\d{6,}$/.test(textoLimpo) || pareceCodigoOrc;
    const digitacaoMuitoRapida = diff < 35;

    clearTimeout(scannerTimer);

    atualizarCatalogoRapido();

    if (digitacaoMuitoRapida || pareceCodigo) {
        bufferScanner = q.trim();
        scannerTimer = setTimeout(() => {
            buscarProdutos(bufferScanner, 'scanner');
            bufferScanner = '';
        }, 60);
        return;
    }

    buscarProdutos(q, 'normal');
});
}

if (tipoFiltroCatalogo && valorFiltroCatalogo && opcoesFiltroCatalogo && opcoesFiltroCatalogoBox) {
tipoFiltroCatalogo.addEventListener('change', () => {
    preencherOpcoesFiltroCatalogo();
    atualizarCatalogoRapido();
});

valorFiltroCatalogo.addEventListener('input', () => {
    const termo = normalizarBuscaLocal(valorFiltroCatalogo.value || '');
    if (!tipoFiltroCatalogo.value || !termo) {
        opcoesFiltroCatalogoBox.classList.add('hidden');
        atualizarCatalogoRapido();
        return;
    }
    const todos = [...opcoesFiltroCatalogo.querySelectorAll('option')].map(o => o.value).filter(Boolean);
    const sugestoes = todos.filter(v => normalizarBuscaLocal(v).includes(termo)).slice(0, 12);
    renderizarSugestoesFiltro(sugestoes);
    atualizarCatalogoRapido();
});
opcoesFiltroCatalogo.addEventListener('change', () => {
    if (opcoesFiltroCatalogo.value) {
        valorFiltroCatalogo.value = opcoesFiltroCatalogo.value;
        opcoesFiltroCatalogoBox.classList.add('hidden');
        atualizarCatalogoRapido();
    }
});
valorFiltroCatalogo.addEventListener('focus', () => {
    if (!tipoFiltroCatalogo.value) {
        opcoesFiltroCatalogoBox.classList.add('hidden');
        return;
    }
    const todos = [...opcoesFiltroCatalogo.querySelectorAll('option')].map(o => o.value).filter(Boolean);
    if (todos.length) renderizarSugestoesFiltro(todos.slice(0, 12));
    else opcoesFiltroCatalogoBox.classList.add('hidden');
});
}
document.addEventListener('click', (e) => {
    if (opcoesFiltroCatalogoBox && !opcoesFiltroCatalogoBox.contains(e.target) && e.target !== valorFiltroCatalogo) {
        opcoesFiltroCatalogoBox.classList.add('hidden');
    }
});
if (btnLimparFiltroCatalogo && tipoFiltroCatalogo && valorFiltroCatalogo && opcoesFiltroCatalogo && opcoesFiltroCatalogoBox && filtroCatalogoAtivo) {
    btnLimparFiltroCatalogo.addEventListener('click', () => {
        tipoFiltroCatalogo.value = '';
        valorFiltroCatalogo.value = '';
        opcoesFiltroCatalogo.innerHTML = '';
        opcoesFiltroCatalogoBox.classList.add('hidden');
        filtroCatalogoAtivo.classList.add('hidden');
        atualizarCatalogoRapido();
    });
}

if (inputBusca && similaresContainer) {
inputBusca.addEventListener('keydown', function(e) {
    const itens = similaresContainer.querySelectorAll('.suggestion-item');

    if (e.key === 'ArrowDown') {
        e.preventDefault();
        if (!sugestoesAtuais.length) return;
        indexSelecionado = (indexSelecionado + 1) % sugestoesAtuais.length;
        produtoEmDestaque = sugestoesAtuais[indexSelecionado] || null;
        renderizarSugestoes();
    }
    else if (e.key === 'ArrowUp') {
        e.preventDefault();
        if (!sugestoesAtuais.length) return;
        indexSelecionado = (indexSelecionado - 1 + sugestoesAtuais.length) % sugestoesAtuais.length;
        produtoEmDestaque = sugestoesAtuais[indexSelecionado] || null;
        renderizarSugestoes();
    }
    else if (e.key === 'Enter') {
        e.preventDefault();

        clearTimeout(scannerTimer); // Cancela o cronômetro do leitor
        bufferScanner = ''; // Limpa a memória do leitor

        quantidadeRapida = obterQuantidadeRapida(inputBusca.value);

        if (itens.length > 0) {
            const idx = indexSelecionado > -1 ? indexSelecionado : 0;
            const prod = sugestoesAtuais[idx];
            if (prod) {
                if (pdvMaisVSlotAlvo !== null && pdvMaisVSlotAlvo >= 0) {
                    atribuirProdutoAoSlotMaisVendidos(prod, pdvMaisVSlotAlvo);
                } else {
                    adicionarProdutoComQuantidade(prod.id, prod.nome, prod.preco_venda, quantidadeRapida, prod);
                }
            }
        } else if (produtoEmDestaque) {
            if (pdvMaisVSlotAlvo !== null && pdvMaisVSlotAlvo >= 0) {
                atribuirProdutoAoSlotMaisVendidos(produtoEmDestaque, pdvMaisVSlotAlvo);
            } else {
                addCarrinho(
                    produtoEmDestaque.id,
                    produtoEmDestaque.nome,
                    produtoEmDestaque.preco_venda,
                    quantidadeRapida,
                    metaOpcoesFromProd(produtoEmDestaque)
                );
            }
        } else if (inputBusca.value.trim().length > 0) {
            buscarProdutos(inputBusca.value.trim(), 'scanner');
        }
    }
    else if (e.key === 'Escape') {
        e.preventDefault();
        if (pdvMaisVSlotAlvo !== null) {
            pdvMaisVSlotAlvo = null;
            atualizarBannerSlotMaisVendidos();
            renderSlotsMaisVendidosPdv();
        }
        limparBuscaVisual();
        inputBusca.value = '';
        esconderStatusBusca();
        quantidadeRapida = 1;
    }
    else if (e.key === 'Backspace') {
        if (!inputBusca.value.trim() && carrinho.length) {
            e.preventDefault();
            removerUltimoItem();
        }
    }
});
}

function filtrarClientesLocais(q) {
    const n = normalizarBuscaLocal(String(q || '').trim());
    if (n.length < 1) return [];
    return cacheClientesPDV.filter((c) => {
        const nm = normalizarBuscaLocal(c.nome || '');
        const ed = normalizarBuscaLocal(c.endereco || '');
        const pc = normalizarBuscaLocal(c.plus_code || '');
        return nm.includes(n) || ed.includes(n) || pc.includes(n);
    }).slice(0, 22);
}

const PDV_CLIENTES_LS_KEY = 'agro_pdv_clientes_cache_v1';

function carregarCacheClientes(opts) {
    const force = opts && opts.force;
    const msgEl = document.getElementById('cliente-api-msg');
    if (window.AGRO_MANUAL_SYNC_ONLY && !force) {
        try {
            const raw = localStorage.getItem(PDV_CLIENTES_LS_KEY);
            if (raw) {
                const d = JSON.parse(raw);
                cacheClientesPDV = Array.isArray(d.clientes) ? d.clientes : [];
                if (msgEl) {
                    if (cacheClientesPDV.length) {
                        msgEl.classList.add('hidden');
                        msgEl.textContent = '';
                    } else {
                        msgEl.textContent = 'Lista de clientes vazia no cache. Use “Sincronizar API”.';
                        msgEl.classList.remove('hidden');
                    }
                }
                return;
            }
        } catch (_) {}
        cacheClientesPDV = [];
        if (msgEl) {
            msgEl.textContent = 'Clientes não carregados (modo só cache). Use “Sincronizar API”.';
            msgEl.classList.remove('hidden');
        }
        return;
    }
    if (window.gmLoadingBar) window.gmLoadingBar.show();
    fetch(AGRO_PDV_URLS.apiListCustomers)
        .then(r => r.json())
        .then(d => {
            cacheClientesPDV = Array.isArray(d.clientes) ? d.clientes : [];
            try {
                localStorage.setItem(PDV_CLIENTES_LS_KEY, JSON.stringify({ clientes: cacheClientesPDV, saved_at: Date.now() }));
            } catch (_) {}
            if (d.contagem_fontes && typeof console !== 'undefined' && console.info) {
                console.info('[PDV clientes — ClienteAgro]', d.contagem_fontes);
            }
            if (!msgEl) return;
            if (cacheClientesPDV.length) {
                msgEl.classList.add('hidden');
                msgEl.textContent = '';
                return;
            }
            let t = '';
            if (d.erro) t = String(d.erro);
            else {
                t = 'Nenhum cliente ativo no Agro. Use a tela Clientes (menu) e “Sincronizar ERP / Mongo” para importar, ou cadastre manualmente. '
                    + 'O histórico de orçamentos ao lado é só no navegador.';
            }
            msgEl.textContent = t;
            msgEl.classList.toggle('hidden', !t);
        })
        .catch(() => {
            cacheClientesPDV = [];
            if (msgEl) {
                msgEl.textContent = 'Falha ao carregar lista de clientes (rede ou servidor).';
                msgEl.classList.remove('hidden');
            }
        })
        .finally(() => { if (window.gmLoadingBar) window.gmLoadingBar.hide(); });
}

if (inputCliente && clienteResults) {
inputCliente.addEventListener('input', function(e) {
    const q = e.target.value;
    clienteSelecionado = null;
    indexSelecionadoCliente = -1;
    clearTimeout(clienteDebounceTimer);

    if (q.length < 1) {
        clienteResults.classList.add('hidden');
        return;
    }

    const locais = filtrarClientesLocais(q);
    if (locais.length) {
        renderizarClientes(locais.map(c => ({
            nome: c.nome,
            documento: c.documento || '',
            telefone: c.telefone || '',
            endereco: c.endereco || '',
            plus_code: c.plus_code || '',
            id: c.id
        })));
    }

    clienteDebounceTimer = setTimeout(() => {
        const locaisFiltrados = filtrarClientesLocais(q);
        if (window.AGRO_MANUAL_SYNC_ONLY) {
            if (locaisFiltrados.length > 0) {
                renderizarClientes(locaisFiltrados.map(c => ({
                    nome: c.nome,
                    documento: c.documento || '',
                    telefone: c.telefone || '',
                    endereco: c.endereco || '',
                    plus_code: c.plus_code || '',
                    id: c.id
                })));
            } else {
                clienteResults.classList.add('hidden');
            }
            return;
        }
        if (window.gmLoadingBar) window.gmLoadingBar.show();
        fetch(`${AGRO_PDV_URLS.apiBuscarClientes}?q=${encodeURIComponent(q.trim())}`)
            .then(res => res.json())
            .then(data => {
                const apiList = data.clientes || [];
                if (data.contagem_fontes && typeof console !== 'undefined' && console.info) {
                    console.info('[PDV clientes — busca ClienteAgro]', data.contagem_fontes, 'q=', q.trim());
                }
                const msgEl = document.getElementById('cliente-api-msg');
                if (msgEl && data.erro) {
                    msgEl.textContent = String(data.erro);
                    msgEl.classList.remove('hidden');
                }
                if (apiList.length > 0) {
                    renderizarClientes(apiList);
                } else if (locaisFiltrados.length > 0) {
                    renderizarClientes(locaisFiltrados.map(c => ({
                        nome: c.nome,
                        documento: c.documento || '',
                        telefone: c.telefone || '',
                        endereco: c.endereco || '',
                        plus_code: c.plus_code || '',
                        id: c.id
                    })));
                } else {
                    clienteResults.classList.add('hidden');
                }
            })
            .catch(err => console.error('Erro ao buscar clientes:', err))
            .finally(() => { if (window.gmLoadingBar) window.gmLoadingBar.hide(); });
    }, 200);
});

inputCliente.addEventListener('keydown', function(e) {
    const itens = clienteResults.querySelectorAll('.cliente-suggestion-item');

    if (e.key === 'ArrowDown') {
        e.preventDefault();
        if (!itens.length) return;
        indexSelecionadoCliente = (indexSelecionadoCliente + 1) % itens.length;
        atualizarSelecaoVisualCliente(itens);
    }
    else if (e.key === 'ArrowUp') {
        e.preventDefault();
        if (!itens.length) return;
        indexSelecionadoCliente = (indexSelecionadoCliente - 1 + itens.length) % itens.length;
        atualizarSelecaoVisualCliente(itens);
    }
    else if (e.key === 'Enter') {
        e.preventDefault();
        if (indexSelecionadoCliente > -1 && itens[indexSelecionadoCliente]) {
            itens[indexSelecionadoCliente].click();
        } else if (itens.length > 0) {
            itens[0].click();
        }
    }
    else if (e.key === 'Escape') {
        e.preventDefault();
        clienteResults.classList.add('hidden');
    }
});
}

function renderizarClientes(clientes) {
    if (!clienteResults) return;
    clienteResults.innerHTML = '';
    const hint = document.getElementById('cliente-api-msg');
    if (hint && clientes.length) {
        hint.classList.add('hidden');
        hint.textContent = '';
    }

    clientes.forEach(cliente => {
        const div = document.createElement('div');
        div.className = "cliente-suggestion-item p-3 cursor-pointer border-b border-slate-50 hover:bg-slate-100 transition-all";
        const doc = (cliente.documento && String(cliente.documento).trim() && cliente.documento !== '—')
            ? escapeHtml(cliente.documento)
            : '';
        const tel = (cliente.telefone && String(cliente.telefone).trim())
            ? escapeHtml(cliente.telefone)
            : '';
        div.innerHTML = `
            <div class="font-bold text-slate-800 uppercase tracking-wide">${escapeHtml(cliente.nome)}</div>
            ${doc ? `<div class="text-[11px] text-slate-500 font-semibold mt-0.5">${doc}</div>` : ''}
            ${tel ? `<div class="text-[10px] text-emerald-600 font-bold mt-0.5">${tel}</div>` : ''}
        `;

        div.onclick = () => {
            inputCliente.value = cliente.nome;
            const sel = {
                nome: cliente.nome,
                documento: cliente.documento,
                telefone: cliente.telefone,
                endereco: (cliente.endereco && String(cliente.endereco).trim()) ? String(cliente.endereco).trim() : '',
                logradouro: (cliente.logradouro && String(cliente.logradouro).trim()) ? String(cliente.logradouro).trim() : '',
                numero: (cliente.numero && String(cliente.numero).trim()) ? String(cliente.numero).trim() : '',
                bairro: (cliente.bairro && String(cliente.bairro).trim()) ? String(cliente.bairro).trim() : '',
                cidade: (cliente.cidade && String(cliente.cidade).trim()) ? String(cliente.cidade).trim() : '',
                uf: (cliente.uf && String(cliente.uf).trim()) ? String(cliente.uf).trim() : '',
                cep: (cliente.cep && String(cliente.cep).trim()) ? String(cliente.cep).trim() : '',
                plus_code: (cliente.plus_code && String(cliente.plus_code).trim()) ? String(cliente.plus_code).trim() : '',
                referencia_rural: (cliente.referencia_rural && String(cliente.referencia_rural).trim()) ? String(cliente.referencia_rural).trim() : '',
                maps_url_manual: (cliente.maps_url_manual && String(cliente.maps_url_manual).trim()) ? String(cliente.maps_url_manual).trim() : '',
                id: cliente.id,
                cliente_agro_pk: cliente.cliente_agro_pk != null ? cliente.cliente_agro_pk : null,
            };
            if ((!sel.telefone || !String(sel.telefone).trim()) && cacheClientesPDV.length) {
                const nm = normalizarBuscaLocal(cliente.nome);
                const m = cacheClientesPDV.find(x => normalizarBuscaLocal(x.nome || '') === nm);
                if (m) {
                    if (m.telefone) sel.telefone = m.telefone;
                    if (m.id && !sel.id) sel.id = m.id;
                    if (m.documento && (!sel.documento || sel.documento === '—')) sel.documento = m.documento;
                    if (m.endereco && !sel.endereco) sel.endereco = String(m.endereco).trim();
                    if (m.plus_code && !sel.plus_code) sel.plus_code = String(m.plus_code).trim();
                    if (m.cliente_agro_pk != null && sel.cliente_agro_pk == null) sel.cliente_agro_pk = m.cliente_agro_pk;
                    if (m.logradouro && !sel.logradouro) sel.logradouro = String(m.logradouro).trim();
                    if (m.numero && !sel.numero) sel.numero = String(m.numero).trim();
                    if (m.bairro && !sel.bairro) sel.bairro = String(m.bairro).trim();
                    if (m.cidade && !sel.cidade) sel.cidade = String(m.cidade).trim();
                    if (m.uf && !sel.uf) sel.uf = String(m.uf).trim();
                    if (m.cep && !sel.cep) sel.cep = String(m.cep).trim();
                }
            }
            clienteSelecionado = sel;
            clienteResults.classList.add('hidden');
            focarBuscaProduto();
        };

        clienteResults.appendChild(div);
    });

    clienteResults.classList.remove('hidden');
}

document.addEventListener('keydown', function(e) {
    if (document.body.classList.contains('sspin-locked')) return;
    const tag = e.target && e.target.tagName;
    const inField = tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT';

    if (e.key === 'F1') {
        const mpe = document.getElementById('modal-pdv-entrega-pergunta');
        const mei = document.getElementById('modal-pdv-entrega-impressao');
        if (mpe && !mpe.classList.contains('hidden')) return;
        if (mei && !mei.classList.contains('hidden')) return;
        e.preventDefault();
        if (pdvCarrinhoDrawerEstaAberto()) fecharDrawerCarrinho();
        else abrirDrawerCarrinho();
        return;
    }

    if (e.key === '/' && !inField) {
        e.preventDefault();
        focarBuscaProduto();
        return;
    }
    if (e.altKey && !e.ctrlKey && !e.shiftKey && (e.code === 'KeyA' || e.key === 'a' || e.key === 'A')) {
        if (inField) return;
        const mba = document.getElementById('modal-busca-avancada');
        if (mba && !mba.classList.contains('hidden')) return;
        e.preventDefault();
        abrirBuscaAvancada();
        return;
    }

    if (e.key === 'F2') {
        e.preventDefault();
        focarBuscaProduto();
    }
    else if (e.key === 'F3') {
        e.preventDefault();
        focarBuscaCliente();
    }
    else if (e.key === 'F4') {
        e.preventDefault();
        limparCarrinho();
    }
    else if (e.key === 'F6') {
        e.preventDefault();
        abrirHistoricoLocal();
    }
    else if (e.key === 'F7') {
        e.preventDefault();
        salvarOrcamentoManual();
    }
    else if (e.key === 'F8') {
        e.preventDefault();
        if (carrinho.length) irParaCheckout();
    }
    else if (e.key === 'F9') {
        e.preventDefault();
        if (produtoEmDestaque) abrirAjuste(produtoEmDestaque.id, produtoEmDestaque.nome, produtoEmDestaque.saldo_erp_centro, produtoEmDestaque.codigo_nfe || produtoEmDestaque.codigo_interno || '', 'centro');
    }
    else if (e.key === 'F5') {
        e.preventDefault();
        if (produtoEmDestaque) forcarAtualizacao(produtoEmDestaque.id);
    }
    else if (e.key === 'F10') {
        e.preventDefault();
        if (produtoEmDestaque) abrirAjuste(produtoEmDestaque.id, produtoEmDestaque.nome, produtoEmDestaque.saldo_erp_vila, produtoEmDestaque.codigo_nfe || produtoEmDestaque.codigo_interno || '', 'vila');
    }
    else if (e.key === 'Escape') {
        const mpe = document.getElementById('modal-pdv-entrega-pergunta');
        if (mpe && !mpe.classList.contains('hidden')) {
            const btnNao = document.getElementById('mpe-nao');
            if (btnNao) btnNao.click();
            e.preventDefault();
            return;
        }
        if (pdvCarrinhoDrawerEstaAberto()) {
            fecharDrawerCarrinho();
            e.preventDefault();
            return;
        }
        const mpae = document.getElementById('modal-pdv-ajuste-estoque');
        if (mpae && !mpae.classList.contains('hidden')) {
            fecharModalAjustePdv();
            e.preventDefault();
            return;
        }
        const ml = document.getElementById('modal-lembretes');
        if (ml && !ml.classList.contains('hidden')) {
            fecharModalLembretes();
        } else {
            const mba = document.getElementById('modal-busca-avancada');
            if (mba && !mba.classList.contains('hidden')) {
                fecharBuscaAvancada();
            } else {
                const mh = document.getElementById('modal-historico-vendas');
                if (mh && !mh.classList.contains('hidden')) {
                    fecharHistoricoLocal();
                } else {
                    if (autoList) autoList.classList.add('hidden');
                    if (clienteResults) clienteResults.classList.add('hidden');
                }
            }
        }
    }
});

document.addEventListener('click', (e) => {
    try {
        if (opcoesFiltroCatalogoBox && !opcoesFiltroCatalogoBox.contains(e.target) && e.target !== valorFiltroCatalogo) {
            opcoesFiltroCatalogoBox.classList.add('hidden');
        }
        if (inputBusca && autoList && !inputBusca.contains(e.target) && !autoList.contains(e.target)) {
            autoList.classList.add('hidden');
        }
        if (inputCliente && clienteResults && !inputCliente.contains(e.target) && !clienteResults.contains(e.target)) {
            clienteResults.classList.add('hidden');
        }
    } catch (err) {
        if (typeof console !== 'undefined' && console.warn) {
            console.warn('[PDV] click overlay handler', err);
        }
    }
});

function sincronizarSaldosNasListasVisiveis() {
    const patch = (arr) => {
        if (!Array.isArray(arr)) return;
        arr.forEach((s) => {
            const u = baseProdutos.find(x => String(x.id) === String(s.id));
            if (!u) return;
            s.saldo_centro = u.saldo_centro;
            s.saldo_vila = u.saldo_vila;
            s.saldo_erp_centro = u.saldo_erp_centro;
            s.saldo_erp_vila = u.saldo_erp_vila;
        });
    };
    patch(sugestoesAtuais);
    patch(sugestoesBuscaCompletas);
    if (produtoEmDestaque) {
        const u = baseProdutos.find(x => String(x.id) === String(produtoEmDestaque.id));
        if (u) {
            produtoEmDestaque = {
                ...produtoEmDestaque,
                saldo_centro: u.saldo_centro,
                saldo_vila: u.saldo_vila,
                saldo_erp_centro: u.saldo_erp_centro,
                saldo_erp_vila: u.saldo_erp_vila,
            };
        }
    }
}

function aplicarSaldosNaBasePdv(rows, opts) {
    var opt = opts || {};
    if (!opt.force && window.__pdvSuprimirAtualizacaoSaldosSegundoPlano) return false;
    if (!Array.isArray(rows) || rows.length === 0 || !baseProdutos.length) return false;
    const map = new Map();
    rows.forEach((r) => {
        if (r && r[0] != null && r.length >= 5) map.set(String(r[0]), r);
    });
    let mudou = false;
    baseProdutos.forEach((p) => {
        const row = map.get(String(p.id));
        if (!row) return;
        const nc = Number(row[1]);
        const nv = Number(row[2]);
        const nec = Number(row[3]);
        const nev = Number(row[4]);
        if (
            Number(p.saldo_centro) !== nc ||
            Number(p.saldo_vila) !== nv ||
            Number(p.saldo_erp_centro) !== nec ||
            Number(p.saldo_erp_vila) !== nev
        ) {
            mudou = true;
        }
        p.saldo_centro = nc;
        p.saldo_vila = nv;
        p.saldo_erp_centro = nec;
        p.saldo_erp_vila = nev;
    });
    if (mudou) {
        sincronizarSaldosNasListasVisiveis();
        if (sugestoesAtuais.length) renderizarSugestoes();
    }
    atualizarPrecosBotoesTopVendidos();
    return mudou;
}

const PDV_TOP_VENDIDOS_DIAS = 30;
const PDV_MV_SLOTS_KEY = 'pdv_mais_vendidos_slots_v1';
const PDV_MV_DRAWER_KEY = 'pdv_mais_vendidos_drawer_open_v1';
const PDV_MV_NUM = 10;
let pdvMaisVSlotAlvo = null;

function pdvAplicarEstadoDrawerAtalhos(abrir) {
    const body = document.getElementById('pdv-top-vendidos-body');
    const icon = document.getElementById('pdv-mv-toggle-icon');
    if (!body) return;
    if (abrir) {
        body.classList.remove('hidden');
        if (icon) icon.textContent = '▾';
    } else {
        body.classList.add('hidden');
        if (icon) icon.textContent = '▸';
    }
    try {
        localStorage.setItem(PDV_MV_DRAWER_KEY, abrir ? '1' : '0');
    } catch (_) {}
}

function pdvInitDrawerAtalhos() {
    const btn = document.getElementById('pdv-mv-toggle');
    if (!btn) return;
    let abrir = true;
    try {
        abrir = localStorage.getItem(PDV_MV_DRAWER_KEY) !== '0';
    } catch (_) {}
    pdvAplicarEstadoDrawerAtalhos(abrir);
    btn.addEventListener('click', () => {
        const body = document.getElementById('pdv-top-vendidos-body');
        const aberto = !!(body && !body.classList.contains('hidden'));
        pdvAplicarEstadoDrawerAtalhos(!aberto);
    });
}

function obterSlotsMaisVendidosPdv() {
    let raw = [];
    try {
        raw = JSON.parse(localStorage.getItem(PDV_MV_SLOTS_KEY) || '[]');
    } catch (e) {
        raw = [];
    }
    if (!Array.isArray(raw)) raw = [];
    const out = [];
    for (let i = 0; i < PDV_MV_NUM; i++) {
        const x = raw[i];
        if (x && x.id != null && String(x.id) !== '') {
            out.push({
                id: normalizarIdProdutoPdv(x.id),
                nome: String(x.nome || ''),
                preco_venda: Number(x.preco_venda || 0),
            });
        } else {
            out.push(null);
        }
    }
    return out;
}

function salvarSlotsMaisVendidosPdv(slots) {
    try {
        localStorage.setItem(PDV_MV_SLOTS_KEY, JSON.stringify(slots));
    } catch (e) {}
}

function atualizarBannerSlotMaisVendidos() {
    const el = document.getElementById('pdv-mv-slot-banner');
    if (!el) return;
    if (pdvMaisVSlotAlvo === null || pdvMaisVSlotAlvo < 0) {
        el.classList.add('hidden');
        el.textContent = '';
        return;
    }
    el.classList.remove('hidden');
    el.textContent = `Atalho ${pdvMaisVSlotAlvo + 1}: busque abaixo e toque no produto (ou Enter na linha selecionada). Esc cancela. ↻ num atalho cheio também entra em modo troca.`;
}

function atribuirProdutoAoSlotMaisVendidos(s, index) {
    const slots = obterSlotsMaisVendidosPdv();
    slots[index] = {
        id: normalizarIdProdutoPdv(s.id),
        nome: String(s.nome || ''),
        preco_venda: Number(s.preco_venda || 0),
    };
    salvarSlotsMaisVendidosPdv(slots);
    pdvMaisVSlotAlvo = null;
    atualizarBannerSlotMaisVendidos();
    renderSlotsMaisVendidosPdv();
    tocarSom('add');
}

function limparSlotMaisVendidos(i) {
    const slots = obterSlotsMaisVendidosPdv();
    slots[i] = null;
    salvarSlotsMaisVendidosPdv(slots);
    if (pdvMaisVSlotAlvo === i) {
        pdvMaisVSlotAlvo = null;
        atualizarBannerSlotMaisVendidos();
    }
    renderSlotsMaisVendidosPdv();
}

function aplicarRankingMongoNosSlotsMaisVendidos(payload) {
    const itens = (payload && payload.itens) ? payload.itens : [];
    const slots = obterSlotsMaisVendidosPdv();
    for (let i = 0; i < PDV_MV_NUM; i++) {
        const it = itens[i];
        if (it && it.id != null) {
            slots[i] = {
                id: normalizarIdProdutoPdv(it.id),
                nome: String(it.nome || ''),
                preco_venda: Number(it.preco_venda || 0),
            };
        } else {
            slots[i] = null;
        }
    }
    salvarSlotsMaisVendidosPdv(slots);
    pdvMaisVSlotAlvo = null;
    atualizarBannerSlotMaisVendidos();
    renderSlotsMaisVendidosPdv();
}

function renderSlotsMaisVendidosPdv() {
    const wrap = document.getElementById('pdv-top-vendidos-btns');
    if (!wrap) return;
    wrap.innerHTML = '';
    const slots = obterSlotsMaisVendidosPdv();
    for (let i = 0; i < PDV_MV_NUM; i++) {
        const it = slots[i];
        const row = document.createElement('div');
        row.className = 'relative min-h-[3.25rem]';
        if (pdvMaisVSlotAlvo === i) {
            row.classList.add('ring-2', 'ring-amber-500', 'ring-offset-1', 'rounded-lg');
        }

        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className =
            'text-left w-full min-h-[3.25rem] px-2.5 py-2 rounded-lg border-2 border-orange-200 bg-white hover:bg-orange-100 hover:border-orange-400 active:scale-[0.98] shadow-sm transition-all';

        const t1 = document.createElement('span');
        t1.className = 'block text-[11px] font-black leading-snug line-clamp-2';
        const t2 = document.createElement('span');
        t2.className = 'block text-[10px] font-bold tabular-nums mt-0.5';

        if (!it) {
            btn.classList.add('pr-2');
            t1.classList.add('text-slate-400');
            t1.textContent = `Atalho ${i + 1}`;
            t2.textContent = '— vago';
            t2.classList.add('text-slate-400', 'font-semibold');
            btn.setAttribute('aria-label', `Definir atalho ${i + 1}`);
            btn.title = 'Toque: escolhe este espaço. Depois toque num produto na lista (ou Enter).';
            btn.addEventListener('click', () => {
                pdvMaisVSlotAlvo = pdvMaisVSlotAlvo === i ? null : i;
                atualizarBannerSlotMaisVendidos();
                renderSlotsMaisVendidosPdv();
                if (pdvMaisVSlotAlvo === i) focarBuscaProduto();
            });
            btn._pdvTopAtualizarPreco = () => {};
        } else {
            btn.classList.add('pr-8', 'pb-5');
            const nome = String(it.nome || 'Item');
            const nomeCurto = nome.length > 44 ? `${nome.slice(0, 42)}…` : nome;
            t1.classList.add('text-slate-800');
            t1.textContent = nomeCurto;
            t2.classList.add('text-emerald-700');

            const atualizarPrecoExibido = () => {
                const idApi = normalizarIdProdutoPdv(it.id);
                const loc =
                    typeof baseProdutos !== 'undefined' && baseProdutos.length
                        ? baseProdutos.find((x) => normalizarIdProdutoPdv(x.id) === idApi)
                        : null;
                const preco = loc ? Number(loc.preco_venda) : Number(it.preco_venda);
                t2.textContent = formatarMoeda(preco);
            };
            atualizarPrecoExibido();
            btn.setAttribute('data-pdv-mv-preco', '1');
            btn.setAttribute('aria-label', `Adicionar ${nome} ao carrinho`);
            btn.title = 'Toque: coloca no carrinho. ↻: trocar produto. ✕: esvaziar atalho.';
            btn.addEventListener('click', () => {
                if (pdvMaisVSlotAlvo === i) {
                    pdvMaisVSlotAlvo = null;
                    atualizarBannerSlotMaisVendidos();
                    renderSlotsMaisVendidosPdv();
                    return;
                }
                const idApi = normalizarIdProdutoPdv(it.id);
                const loc =
                    typeof baseProdutos !== 'undefined' && baseProdutos.length
                        ? baseProdutos.find((x) => normalizarIdProdutoPdv(x.id) === idApi)
                        : null;
                const preco = loc ? Number(loc.preco_venda) : Number(it.preco_venda);
                const nomeUsar = loc && loc.nome ? loc.nome : nome;
                const q = typeof quantidadeRapida !== 'undefined' && quantidadeRapida ? quantidadeRapida : 1;
                addCarrinho(idApi, nomeUsar, preco, q, {
                    skipDebounce: true,
                    ...metaOpcoesFromProd(loc || it),
                });
            });
            btn._pdvTopAtualizarPreco = atualizarPrecoExibido;

            const btnTroca = document.createElement('button');
            btnTroca.type = 'button';
            btnTroca.className =
                'absolute top-1 right-1 z-[1] w-6 h-6 flex items-center justify-center rounded-md border border-orange-200 bg-white text-[12px] leading-none text-amber-800 hover:bg-amber-100 shadow-sm';
            btnTroca.title = 'Trocar produto deste atalho';
            btnTroca.setAttribute('aria-label', 'Trocar');
            btnTroca.textContent = '↻';
            btnTroca.addEventListener('click', (e) => {
                e.stopPropagation();
                e.preventDefault();
                pdvMaisVSlotAlvo = i;
                atualizarBannerSlotMaisVendidos();
                renderSlotsMaisVendidosPdv();
                focarBuscaProduto();
            });

            const btnX = document.createElement('button');
            btnX.type = 'button';
            btnX.className =
                'absolute bottom-1 right-1 z-[1] w-5 h-5 flex items-center justify-center rounded text-[10px] font-black text-slate-400 hover:text-red-600 hover:bg-red-50';
            btnX.title = 'Limpar atalho';
            btnX.setAttribute('aria-label', 'Limpar');
            btnX.textContent = '✕';
            btnX.addEventListener('click', (e) => {
                e.stopPropagation();
                e.preventDefault();
                limparSlotMaisVendidos(i);
            });

            row.appendChild(btnTroca);
            row.appendChild(btnX);
        }

        btn.appendChild(t1);
        btn.appendChild(t2);
        row.appendChild(btn);
        wrap.appendChild(row);
    }
}

function atualizarPrecosBotoesTopVendidos() {
    const wrap = document.getElementById('pdv-top-vendidos-btns');
    if (!wrap) return;
    wrap.querySelectorAll('button[data-pdv-mv-preco]').forEach((btn) => {
        if (typeof btn._pdvTopAtualizarPreco === 'function') btn._pdvTopAtualizarPreco();
    });
}

function importarRankingMongoParaSlotsMaisVendidos() {
    if (!confirm('Substituir os 10 atalhos pelos 10 produtos mais vendidos (Mongo, 30 dias)?')) return;
    const btnImp = document.getElementById('pdv-mv-importar-ranking');
    if (btnImp) {
        btnImp.disabled = true;
        btnImp.classList.add('opacity-50');
    }
    if (window.gmLoadingBar) window.gmLoadingBar.show();
    const u = new URL(AGRO_PDV_URLS.apiPdvTopVendidos, window.location.origin);
    u.searchParams.set('limite', '10');
    u.searchParams.set('dias', String(PDV_TOP_VENDIDOS_DIAS));
    fetch(u.toString(), { credentials: 'same-origin' })
        .then(async (r) => {
            const text = await r.text();
            const raw = text ? text.trim() : '';
            if (!raw) throw new Error(r.ok ? 'Resposta vazia' : `HTTP ${r.status}`);
            let d;
            try {
                d = JSON.parse(raw);
            } catch (err) {
                throw new Error('Resposta inválida do servidor');
            }
            if (!r.ok && (!d.itens || !d.itens.length)) {
                throw new Error(d.erro || `HTTP ${r.status}`);
            }
            if (d.erro && (!d.itens || !d.itens.length)) throw new Error(d.erro);
            aplicarRankingMongoNosSlotsMaisVendidos(d);
        })
        .catch((e) => {
            alert(e && e.message ? e.message : 'Não foi possível importar o ranking.');
        })
        .finally(() => {
            if (btnImp) {
                btnImp.disabled = false;
                btnImp.classList.remove('opacity-50');
            }
            if (window.gmLoadingBar) window.gmLoadingBar.hide();
        });
}

function pollSaldoPdvUmaVez() {
    pollSaldoPdvUmaVezPromise();
}

function pollSaldoPdvUmaVezPromise(options) {
    var pollOpts = options || {};
    return new Promise(function (resolve) {
        if (!pollOpts.force && window.__pdvSuprimirAtualizacaoSaldosSegundoPlano) {
            resolve();
            return;
        }
        if (!baseProdutos.length) {
            resolve();
            return;
        }
        var saldosUrl = AGRO_PDV_URLS.apiPdvSaldos;
        if (pollOpts.force) {
            saldosUrl += (saldosUrl.indexOf('?') >= 0 ? '&' : '?') + '_t=' + Date.now();
        }
        fetch(saldosUrl, { cache: 'no-store' })
            .then((r) => r.json())
            .then((sd) => {
                function aplicarEMarcar() {
                    if (sd.rows) aplicarSaldosNaBasePdv(sd.rows, pollOpts);
                    if (typeof AgroEstoqueSync !== 'undefined' && AgroEstoqueSync.markFresh) {
                        AgroEstoqueSync.markFresh(document.getElementById('agro-saldos-ultima-atualizacao'));
                    }
                    resolve();
                }
                if (pollOpts.force) {
                    requestAnimationFrame(aplicarEMarcar);
                } else {
                    aplicarEMarcar();
                }
            })
            .catch(function () {
                resolve();
            });
    });
}

/* Saldos: sem polling automático — use o botão Estoque (termômetro) ou ações que já disparam leitura. */

function carregarBaseLocal() {
    const manual = !!window.AGRO_MANUAL_SYNC_ONLY;
    const warmed = hidratarCatalogoPdvDoCache();
    if (manual) {
        if (warmed) {
            mostrarStatusBusca('Catálogo em cache (sem sync automático). Use “Sincronizar API”.', 'emerald');
            setTimeout(esconderStatusBusca, 4200);
        } else {
            mostrarStatusBusca('Sem cache de produtos. Clique em “Sincronizar API” no topo.', 'orange');
        }
        return;
    }
    if (!warmed) mostrarStatusBusca('Baixando produtos para memória local...', 'orange');
    sincronizarCatalogoPdvServidor(warmed).catch(function (err) {
        if (typeof console !== 'undefined' && console.error) console.error(err);
    });
}

function aplicarBasePdv(produtos, statusTxt) {
    if (!Array.isArray(produtos) || !produtos.length) return false;
    baseProdutos = prepararBaseProdutos(produtos);
    if (typeof buildBuscaProdutoIndex === 'function') {
        buildBuscaProdutoIndex(baseProdutos);
    }
    if (statusTxt) mostrarStatusBusca(statusTxt, 'emerald');
    preencherOpcoesFiltroCatalogo();
    atualizarCatalogoRapido();
    if (!window.AGRO_MANUAL_SYNC_ONLY) {
        setTimeout(pollSaldoPdvUmaVez, 0);
    }
    setTimeout(esconderStatusBusca, 2500);
    setTimeout(atualizarPrecosBotoesTopVendidos, 100);
    return true;
}

function salvarCacheCatalogoPdv(payload) {
    try {
        localStorage.setItem(
            PDV_CACHE_KEY,
            JSON.stringify({
                saved_at: Date.now(),
                catalog_version: payload.catalog_version || '',
                catalog_updated_at: payload.catalog_updated_at || '',
                produtos: Array.isArray(payload.produtos) ? payload.produtos : [],
            })
        );
    } catch (_) {}
}

function lerCacheCatalogoPdv() {
    try {
        const raw = localStorage.getItem(PDV_CACHE_KEY);
        if (!raw) return null;
        const p = JSON.parse(raw);
        if (!p || !Array.isArray(p.produtos)) return null;
        const age = Date.now() - Number(p.saved_at || 0);
        if (!window.AGRO_MANUAL_SYNC_ONLY && age > PDV_CACHE_TTL_MS) return null;
        return p;
    } catch (_) {
        return null;
    }
}

function hidratarCatalogoPdvDoCache() {
    const cached = lerCacheCatalogoPdv();
    if (!cached || !cached.produtos.length) return false;
    return aplicarBasePdv(cached.produtos, `Catálogo local carregado (${cached.produtos.length})`);
}

function aplicarDeltaCatalogoPdv(changed, removedIds) {
    const map = new Map(baseProdutos.map((p) => [String(p.id), p]));
    (Array.isArray(changed) ? changed : []).forEach((row) => {
        const p = prepararProduto(row);
        const prev = map.get(String(p.id));
        if (prev) {
            p.saldo_centro = prev.saldo_centro;
            p.saldo_vila = prev.saldo_vila;
            p.saldo_erp_centro = prev.saldo_erp_centro;
            p.saldo_erp_vila = prev.saldo_erp_vila;
        }
        map.set(String(p.id), p);
    });
    (Array.isArray(removedIds) ? removedIds : []).forEach((pid) => {
        map.delete(String(pid));
    });
    baseProdutos = Array.from(map.values());
    if (typeof buildBuscaProdutoIndex === 'function') buildBuscaProdutoIndex(baseProdutos);
    preencherOpcoesFiltroCatalogo();
    atualizarCatalogoRapido();
    if (!window.AGRO_MANUAL_SYNC_ONLY) {
        setTimeout(function () {
            pollSaldoPdvUmaVezPromise();
        }, 0);
    }
}

function sincronizarCatalogoPdvServidor(jahAquecido) {
    return new Promise(function (resolve) {
        function finish() {
            resolve();
        }
        if (!jahAquecido) {
            if (window.gmLoader) window.gmLoader.show('🐭 carregando catálogo...');
            else if (window.gmLoadingBar) window.gmLoadingBar.show();
        }
        const cached = lerCacheCatalogoPdv();
        const since = cached && cached.catalog_version ? cached.catalog_version : '';
        const u = new URL('/api/todos-produtos/delta/', window.location.origin);
        if (since) u.searchParams.set('since', since);
        fetch(u.toString())
            .then((r) => r.json())
            .then((d) => {
                if (d && d.unchanged) {
                    if (!jahAquecido && cached && cached.produtos) {
                        aplicarBasePdv(cached.produtos, `Base local pronta com ${cached.produtos.length} itens`);
                    }
                    if (window.gmLoader) window.gmLoader.hide(180);
                    else if (window.gmLoadingBar) window.gmLoadingBar.hide();
                    finish();
                    return;
                }
                if (d && d.delta) {
                    if (!baseProdutos.length && cached && cached.produtos) {
                        aplicarBasePdv(cached.produtos, `Catálogo local carregado (${cached.produtos.length})`);
                    }
                    aplicarDeltaCatalogoPdv(d.changed || [], d.removed_ids || []);
                    const novo = {
                        produtos: baseProdutos,
                        catalog_version: d.catalog_version || '',
                        catalog_updated_at: d.catalog_updated_at || '',
                    };
                    salvarCacheCatalogoPdv(novo);
                    mostrarStatusBusca(`Catálogo sincronizado (${baseProdutos.length})`, 'emerald');
                    if (window.gmLoader) window.gmLoader.hide(180);
                    else if (window.gmLoadingBar) window.gmLoadingBar.hide();
                    finish();
                    return;
                }
                if (d && Array.isArray(d.produtos)) {
                    aplicarBasePdv(d.produtos, `Base local pronta com ${d.produtos.length} itens`);
                    salvarCacheCatalogoPdv(d);
                    if (window.gmLoader) window.gmLoader.hide(180);
                    else if (window.gmLoadingBar) window.gmLoadingBar.hide();
                    finish();
                    return;
                }
                if (window.gmLoader) window.gmLoader.hide(180);
                else if (window.gmLoadingBar) window.gmLoadingBar.hide();
                finish();
            })
            .catch((e) => {
                console.error('Falha na carga local:', e);
                if (!jahAquecido) mostrarStatusBusca('Falha de rede; usando cache local.', 'orange');
                if (window.gmLoader) window.gmLoader.hide(180);
                else if (window.gmLoadingBar) window.gmLoadingBar.hide();
                finish();
            });
    });
}

function atualizarRelogioPdv() {
    const el = document.getElementById('pdv-clock');
    if (!el) return;
    el.textContent = new Date().toLocaleString('pt-BR', {
        weekday: 'short', day: '2-digit', month: '2-digit',
        hour: '2-digit', minute: '2-digit'
    });
}

function pdvMarcarSyncApiFresh() {
    var lab = document.getElementById('agro-api-sync-ultima');
    var btn = document.getElementById('agro-btn-sincronizar-api');
    if (lab && btn && typeof AgroEstoqueSync !== 'undefined' && AgroEstoqueSync.markFresh) {
        AgroEstoqueSync.markFresh(lab, undefined, btn);
    }
}

function pdvHidratarIndicadorSyncApiDoCache() {
    try {
        var raw = localStorage.getItem(PDV_CACHE_KEY);
        if (!raw) return;
        var p = JSON.parse(raw);
        var at = Number(p.saved_at || 0);
        if (!at) return;
        var lab = document.getElementById('agro-api-sync-ultima');
        var btn = document.getElementById('agro-btn-sincronizar-api');
        if (!lab || !btn || typeof AgroEstoqueSync === 'undefined') return;
        lab.dataset.gmFreshAt = String(at);
        if (AgroEstoqueSync.formatHorario) {
            lab.textContent = AgroEstoqueSync.formatHorario(new Date(at));
        } else {
            lab.textContent = new Date(at).toLocaleString('pt-BR');
        }
        if (AgroEstoqueSync.paintThermo) {
            AgroEstoqueSync.paintThermo(btn, lab, AgroEstoqueSync.staleMsDefault);
        }
    } catch (_) {}
}

window.agroPdvSincronizarComApi = function () {
    var apiBtn = document.getElementById('agro-btn-sincronizar-api');
    if (apiBtn) {
        apiBtn.disabled = true;
        apiBtn.setAttribute('aria-busy', 'true');
    }
    carregarCacheClientes({ force: true });
    sincronizarCatalogoPdvServidor(false)
        .then(function () {
            return new Promise(function (resolve) {
                setTimeout(function () {
                    pollSaldoPdvUmaVezPromise({ force: true }).then(resolve);
                }, 700);
            });
        })
        .then(function () {
            pdvMarcarSyncApiFresh();
        })
        .finally(function () {
            if (apiBtn) {
                apiBtn.disabled = false;
                apiBtn.removeAttribute('aria-busy');
            }
        });
};

window.addEventListener('load', () => {
    if (inputCliente && !String(inputCliente.value || '').trim()) {
        inputCliente.value = CLIENTE_PADRAO_PDV;
    }
    focarBuscaProduto();
    atualizarCarrinho();
    renderizarHistoricoResumido();
    renderizarLembretes();
    pdvInitDrawerAtalhos();
    renderSlotsMaisVendidosPdv();
    document.getElementById('pdv-mv-importar-ranking')?.addEventListener('click', importarRankingMongoParaSlotsMaisVendidos);
    atualizarCatalogoRapido();
    carregarBaseLocal();
    carregarCacheClientes();
    verificarLembretes();
    setInterval(verificarLembretes, 15000);
    atualizarRelogioPdv();
    setInterval(atualizarRelogioPdv, 30000);
    if (typeof AgroEstoqueSync !== 'undefined' && AgroEstoqueSync.mount) {
        AgroEstoqueSync.mount({
            onRefresh: async function (reason) {
                if (!baseProdutos.length) return;
                var explicito = reason === 'manual' || reason === 'external';
                if (window.__pdvSuprimirAtualizacaoSaldosSegundoPlano && !explicito) return;
                const r = await fetch(AGRO_PDV_URLS.apiPdvSaldos, { cache: 'no-store' });
                const sd = await r.json();
                if (sd.rows) {
                    aplicarSaldosNaBasePdv(sd.rows, {
                        force: explicito && !!window.__pdvSuprimirAtualizacaoSaldosSegundoPlano,
                    });
                }
            },
        });
    }
    pdvHidratarIndicadorSyncApiDoCache();
    setInterval(function () {
        var b = document.getElementById('agro-btn-sincronizar-api');
        var l = document.getElementById('agro-api-sync-ultima');
        if (b && l && typeof AgroEstoqueSync !== 'undefined' && AgroEstoqueSync.paintThermo) {
            AgroEstoqueSync.paintThermo(b, l, AgroEstoqueSync.staleMsDefault);
        }
    }, 15000);
    window.addEventListener('resize', function () {
        var root = document.getElementById('pdv-balao-aviso-estoque');
        if (root && !root.classList.contains('hidden')) posicionarSetaBalaoEstoque();
    });
});


(() => {
  const guard = (selectors, ms = 550) => {
    selectors.forEach((sel) => {
      document.querySelectorAll(sel).forEach((el) => {
        el.addEventListener('click', (e) => {
          const now = Date.now();
          const last = Number(el.dataset.guardTs || 0);
          if (now - last < ms) {
            e.preventDefault();
            e.stopImmediatePropagation();
            return;
          }
          el.dataset.guardTs = String(now);
        }, true);
      });
    });
  };
  guard([
    '#btn-pdv-orc-whatsapp',
    '#btn-pdv-entrega-whatsapp',
    '#btn-pdv-emitir-orc',
    '#btn-carrinho-finalizar',
    '#btn-carrinho-salvar'
  ], 650);
})();



(function () {
    function pdvRefreshLucide() {
        if (typeof lucide !== 'undefined' && typeof lucide.createIcons === 'function') {
            lucide.createIcons({ attrs: { 'stroke-width': 2 } });
        }
    }
    function pdvSyncSidebarA11y(aside, btn) {
        var exp = aside.getAttribute('data-expanded') === 'true';
        if (btn) btn.setAttribute('aria-expanded', exp ? 'true' : 'false');
    }
    function pdvApplySidebarExpanded(expanded) {
        var aside = document.getElementById('pdv-sidebar');
        var btn = document.getElementById('btn-pdv-menu');
        if (!aside) return;
        aside.setAttribute('data-expanded', expanded ? 'true' : 'false');
        try {
            localStorage.setItem('pdv_sidebar_expanded', expanded ? '1' : '0');
        } catch (e) {}
        pdvSyncSidebarA11y(aside, btn);
    }
    var aside = document.getElementById('pdv-sidebar');
    var btn = document.getElementById('btn-pdv-menu');
    if (aside && btn) {
        var startExpanded = false;
        try {
            startExpanded = localStorage.getItem('pdv_sidebar_expanded') === '1';
        } catch (e) {}
        pdvApplySidebarExpanded(startExpanded);
        btn.addEventListener('click', function () {
            pdvApplySidebarExpanded(aside.getAttribute('data-expanded') !== 'true');
        });
    }
    pdvRefreshLucide();
})();



document.addEventListener('DOMContentLoaded', function () {
    var el = document.getElementById('pdv-reabrir-draft');
    if (!el) return;
    try {
        var d = JSON.parse(el.textContent);
        if (!d.itens || !d.itens.length) return;
        carrinho = d.itens;
        if (typeof inputCliente !== 'undefined' && inputCliente && d.cliente) {
            inputCliente.value = d.cliente;
        }
        clienteSelecionado = (d.cliente_extra && typeof d.cliente_extra === 'object') ? d.cliente_extra : null;
        var fp = document.getElementById('forma-pagamento-pdv');
        if (fp && d.forma_pagamento) fp.value = d.forma_pagamento;
        if (typeof atualizarCarrinho === 'function') atualizarCarrinho();
        if (history.replaceState) history.replaceState(null, '', AGRO_PDV_URLS.pdvRootUrl || window.location.pathname);
    } catch (x) { console.error(x); }
});

document.addEventListener('DOMContentLoaded', function () {
    try {
        var params = new URLSearchParams(window.location.search || '');
        if (params.get('orcamentos') !== '1') return;
        if (typeof abrirHistoricoLocal === 'function') abrirHistoricoLocal();
        params.delete('orcamentos');
        if (history.replaceState) {
            var nextQuery = params.toString();
            var nextUrl = window.location.pathname + (nextQuery ? ('?' + nextQuery) : '') + (window.location.hash || '');
            history.replaceState(null, '', nextUrl);
        }
    } catch (err) {
        console.warn('Nao foi possivel abrir o historico por URL.', err);
    }
});
})();
