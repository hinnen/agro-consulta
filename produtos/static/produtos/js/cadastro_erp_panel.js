(function () {
  'use strict';
  var U = window.AgroCadastroErpUtil;
  if (!U) return;
  var escapeHtml = U.escapeHtml;
  var fmtMoney = U.fmtMoney;
  var setLoading = U.setLoading;
  var resetLoading = U.resetLoading;
  var C = window.AgroCadastroErpLista || {};
  var CADASTRO_ERP_MODO = C.CADASTRO_ERP_MODO || 'lista';
  var CADASTRO_ERP_PID = C.CADASTRO_ERP_PID || '';
  var API = C.API || '';
  var URL_BUSCAR_PDV = C.URL_BUSCAR_PDV || '/api/buscar/';
  var API_DETALHE_TMPL = C.API_DETALHE_TMPL || '';
  var URL_CAD_ERP_PROD_TMPL = C.URL_CAD_ERP_PROD_TMPL || '';
  var URL_OVERLAY_SALVAR = C.URL_OVERLAY_SALVAR || '';
  var URL_ERP_PENDENTES = C.URL_ERP_PENDENTES || '';
  var URL_ERP_SYNC_PENDENTES = C.URL_ERP_SYNC_PENDENTES || '';
  var PODE_EDITAR_OVERLAY = !!C.PODE_EDITAR_OVERLAY;
  var ERP_SYNC_HABILITADO = !!C.CADASTRO_ERP_SYNC_HABILITADO;
  var LOGIN_OVERLAY_HREF = C.LOGIN_OVERLAY_HREF || '';
  var btnErpPend = document.getElementById('cadastro-btn-erp-pendentes');
  var btnErpForcarTodos = document.getElementById('cadastro-btn-erp-forcar-todos');
  var lblErpPendN = document.getElementById('cadastro-erp-pend-n');

  function csrfTokErp() {
    return (U && U.csrf) ? U.csrf() : ((document.cookie.match(/csrftoken=([^;]+)/) || [])[1] || '');
  }

  function aplicarRespostaPendentesBadge(j) {
    var n = (j && j.ok && typeof j.n === 'number') ? j.n : 0;
    lblErpPendN.textContent = '(' + n + ')';
    if (n > 0) {
      if (btnErpPend) btnErpPend.classList.remove('hidden');
      if (btnErpForcarTodos) btnErpForcarTodos.classList.remove('hidden');
    } else {
      if (btnErpPend) btnErpPend.classList.add('hidden');
      if (btnErpForcarTodos) btnErpForcarTodos.classList.add('hidden');
    }
  }

  function fetchPendentesBadgePromise(opt) {
    var sig = opt && opt.signal;
    if (!ERP_SYNC_HABILITADO || !URL_ERP_PENDENTES || !lblErpPendN || !btnErpPend) return Promise.resolve();
    if (!PODE_EDITAR_OVERLAY) return Promise.resolve();
    return fetch(URL_ERP_PENDENTES, { credentials: 'same-origin', signal: sig })
      .then(function (r) { return r.json().catch(function () { return {}; }); })
      .then(aplicarRespostaPendentesBadge)
      .catch(function () { /* ignore */ });
  }

  function refreshPendentesBadge() {
    fetchPendentesBadgePromise();
  }
  window.agroCadastroErpRefreshPendentesBadge = refreshPendentesBadge;

  function jsonOuErroHumano(response) {
    return response.text().then(function (text) {
      var raw = text || '';
      var t = raw.trim();
      var st = response.status;
      if (!t.length) throw new Error('Resposta vazia do servidor (HTTP ' + st + ').');
      var headLow = t.slice(0, 12).toLowerCase();
      if (headLow.indexOf('<!doctype') === 0 || (t.charAt(0) === '<' && raw.toLowerCase().indexOf('<html') !== -1)) {
        if (st === 401 || st === 403 || /login/i.test(raw.slice(0, 900))) {
          throw new Error('Sessão expirada. Entre no sistema de novo.');
        }
        throw new Error('Servidor devolveu HTML em vez de JSON (HTTP ' + st + '). Recarregue já logado.');
      }
      try {
        return JSON.parse(t);
      } catch (_e) {
        throw new Error('Resposta inválida — não é JSON (HTTP ' + st + ').');
      }
    });
  }

  function urlDetalheProduto(id) {
    return API_DETALHE_TMPL.replace(/\/x\//, '/' + encodeURIComponent(String(id)) + '/');
  }
  function urlCadastroErpProduto(pid) {
    return URL_CAD_ERP_PROD_TMPL.replace('__AGRO_PH__', encodeURIComponent(String(pid)));
  }
  var buscaEl = document.getElementById('cadastro-busca');
  var ativosEl = document.getElementById('filtro-ativos');
  var filtrarEl = document.getElementById('cadastro-filtrar');
  var listaEl = document.getElementById('cadastro-lista');
  var metaEl = document.getElementById('cadastro-lista-meta');
  var detalheEl = document.getElementById('cadastro-detalhe');
  var erroEl = document.getElementById('cadastro-erro');
  var prevEl = document.getElementById('cadastro-prev');
  var nextEl = document.getElementById('cadastro-next');
  var pagWrap = document.getElementById('cadastro-paginacao');

  var pagina = 1;
  var porPagina = 72;
  var debounceTimer = null;
  var carregarGen = 0;
  var carregarAbort = null;
  var buscaMergeSeq = 0;
  var buscaMergeTimer = null;
  var PDV_CACHE_KEY = 'agro_pdv_catalog_cache_v2';
  var _cadastroCatLocal = null;
  var _cadastroCatById = null;
  var _cadastroCatInited = false;
  var ultimos = [];
  var modoLista = true;
  var detalheReqSeq = 0;
  var ordenacaoAtual = { campo: null, direcao: 'asc' };

  function mostrarErro(msg, opcoes) {
    if (!msg) {
      erroEl.classList.add('hidden');
      erroEl.textContent = '';
      erroEl.innerHTML = '';
      return;
    }
    var hintMongo = opcoes && opcoes.hintMongo;
    if (hintMongo) {
      erroEl.innerHTML =
        '<p class="font-bold">' + escapeHtml(msg) + '</p>' +
        '<p class="mt-2 text-sm leading-snug">O catálogo precisa do Mongo configurado no servidor (<code class="text-xs bg-white/80 px-1 rounded">VENDA_ERP_MONGO_URL</code> e <code class="text-xs bg-white/80 px-1 rounded">VENDA_ERP_MONGO_DB</code> no arquivo <code class="text-xs">.env</code>). Copie os mesmos valores do ambiente onde o PDV já funciona.</p>' +
        '<button type="button" id="cadastro-erro-ir-grupos" class="mt-3 min-h-[44px] px-4 rounded-xl text-sm font-black uppercase bg-orange-500 hover:bg-orange-600 text-white border-2 border-orange-700">Abrir aba Grupos (funciona sem Mongo)</button>';
      erroEl.classList.remove('hidden');
      var b = document.getElementById('cadastro-erro-ir-grupos');
      if (b) {
        b.addEventListener('click', function () {
          var t = document.getElementById('tab-grupos');
          if (t) t.click();
        });
      }
      return;
    }
    erroEl.innerHTML = '';
    erroEl.textContent = msg;
    erroEl.classList.remove('hidden');
  }

  function urlFetch() {
    if (!buscaEl) return API;
    var q = (buscaEl.value || '').trim();
    var params = new URLSearchParams();
    if (ativosEl && ativosEl.checked) {
      params.set('ativo', '1');
    } else if (ativosEl) {
      params.set('inativos', '1');
    }
    if (q) {
      params.set('q', q);
      params.set('limit', '80');
    } else {
      params.set('pagina', String(pagina));
      params.set('por_pagina', String(porPagina));
    }
    if (ordenacaoAtual.campo) {
      params.set('sort', ordenacaoAtual.campo);
      params.set('dir', ordenacaoAtual.direcao);
    }
    return API + '?' + params.toString();
  }

  function dlRow(label, val) {
    var s = (val === undefined || val === null) ? '' : String(val).trim();
    if (!s) return '';
    return '<div class="flex flex-col sm:flex-row sm:gap-2 border-b border-slate-100 pb-2">' +
      '<dt class="font-black text-slate-400 uppercase text-[11px] tracking-wide shrink-0 sm:w-40">' + escapeHtml(label) + '</dt>' +
      '<dd class="font-semibold text-slate-800 break-words">' + escapeHtml(s) + '</dd></div>';
  }

  function dlRowHtml(label, innerHtml) {
    return '<div class="flex flex-col sm:flex-row sm:gap-2 border-b border-slate-100 pb-2">' +
      '<dt class="font-black text-slate-400 uppercase text-[11px] tracking-wide shrink-0 sm:w-40">' + escapeHtml(label) + '</dt>' +
      '<dd class="font-semibold text-slate-800 break-words">' + innerHtml + '</dd></div>';
  }

  function badgeSim(v) {
    return v
      ? '<span class="text-emerald-900 bg-emerald-100 px-2 py-1 rounded-lg text-xs font-black uppercase">Sim</span>'
      : '<span class="text-slate-600 bg-slate-100 px-2 py-1 rounded-lg text-xs font-black uppercase">Não</span>';
  }

  function fmtNumPt(n, dec) {
    if (n === undefined || n === null || n === '') return '—';
    var x = Number(n);
    if (!isFinite(x)) return '—';
    return x.toLocaleString('pt-BR', {
      minimumFractionDigits: dec != null ? dec : 2,
      maximumFractionDigits: dec != null ? dec : 4
    });
  }

  function renderTabelaComposicao(itens) {
    if (!itens || !itens.length) {
      return '<p class="text-sm text-slate-500 py-2">Nenhum item de composição neste cadastro (ou estrutura não reconhecida no Mongo).</p>';
    }
    var h = '<div class="overflow-x-auto rounded-xl border border-slate-200"><table class="w-full text-sm"><thead><tr class="bg-slate-50 text-[10px] font-black uppercase text-slate-500">' +
      '<th class="text-left px-3 py-2">Depósito</th><th class="text-left px-3 py-2">Produto</th><th class="text-left px-3 py-2">Código</th><th class="text-right px-3 py-2">Qtd</th></tr></thead><tbody>';
    itens.forEach(function (it) {
      h += '<tr class="border-t border-slate-100">' +
        '<td class="px-3 py-2 font-semibold text-slate-700">' + escapeHtml(it.deposito || '—') + '</td>' +
        '<td class="px-3 py-2">' + escapeHtml(it.nome || '—') + '</td>' +
        '<td class="px-3 py-2 font-mono text-xs">' + escapeHtml(it.codigo || '') + '</td>' +
        '<td class="px-3 py-2 text-right font-black">' + fmtNumPt(it.quantidade, 4) + '</td></tr>';
    });
    h += '</tbody></table></div>';
    return h;
  }

  function renderSimilares(lista) {
    if (!lista || !lista.length) {
      return '<p class="text-sm text-slate-500">Nenhum similar vinculado.</p>';
    }
    var h =
      '<div class="overflow-x-auto rounded-xl border border-slate-200 bg-white">' +
      '<table class="w-full text-sm min-w-[640px]"><thead><tr class="bg-slate-900 text-white text-[10px] font-black uppercase">' +
      '<th class="text-left px-3 py-2">Código</th><th class="text-left px-3 py-2">Nome</th>' +
      '<th class="text-left px-3 py-2">Modelo</th><th class="text-left px-3 py-2">Marca</th><th class="text-left px-3 py-2">Fabricante</th>' +
      '</tr></thead><tbody>';
    lista.forEach(function (s) {
      h +=
        '<tr class="border-t border-slate-100">' +
        '<td class="px-3 py-2 font-mono text-xs font-bold">' + escapeHtml(s.codigo || '—') + '</td>' +
        '<td class="px-3 py-2 font-semibold">' + escapeHtml(s.nome || '—') + '</td>' +
        '<td class="px-3 py-2 text-slate-700">' + escapeHtml(s.modelo || '') + '</td>' +
        '<td class="px-3 py-2 text-slate-700">' + escapeHtml(s.marca || '') + '</td>' +
        '<td class="px-3 py-2 text-slate-700">' + escapeHtml(s.fabricante || '') + '</td></tr>';
    });
    h += '</tbody></table></div>';
    return h;
  }

  function buildOverlayFormHtml(p) {
    var pv = (p.preco_venda != null && isFinite(Number(p.preco_venda))) ? String(Number(p.preco_venda)).replace('.', ',') : '';
    var pc = (p.preco_custo != null && isFinite(Number(p.preco_custo))) ? String(Number(p.preco_custo)).replace('.', ',') : '';
    var av = '';
    if (p.ativo_exibicao === true) av = '1';
    else if (p.ativo_exibicao === false) av = '0';
    var ic = 'w-full min-h-[44px] px-3 rounded-xl border-2 border-emerald-400 text-base font-bold text-slate-900 bg-white';
    return (
      '<div class="mt-6 rounded-2xl border-2 border-emerald-600 bg-emerald-50/90 p-4 sm:p-5 shadow-sm">' +
      '<h4 class="text-sm font-black uppercase text-emerald-950 tracking-wide mb-1">Editar cadastro · Agro</h4>' +
      '<p class="text-xs font-bold text-slate-800 mt-1 mb-3"><span class="text-red-600 font-black">*</span> Somente os campos com asterisco são obrigatórios para salvar.</p>' +
      '<p class="text-xs text-slate-700 mb-4 leading-snug"><strong>Salvar no Agro</strong> grava no SisVale (PDV e buscas).' +
      (ERP_SYNC_HABILITADO
        ? ' <strong>Enviar ao ERP</strong> replica no ERP legado quando você quiser.'
        : ' Sincronização com a API do ERP está desligada neste ambiente.') +
      ' Nos demais campos, vazio + salvar remove o override; em «Exibir como», «Seguir catálogo» remove o forçamento de ativo/inativo.</p>' +
      '<div class="grid gap-3 sm:grid-cols-2">' +
      '<label class="block sm:col-span-2"><span class="text-[10px] font-black uppercase text-slate-600">Nome <span class="text-red-600 font-black">*</span></span>' +
      '<input type="text" id="cad-ov-nome" class="' + ic + '" maxlength="300" value="' + escapeHtml(p.nome || '') + '" autocomplete="off" /></label>' +
      '<label class="block"><span class="text-[10px] font-black uppercase text-slate-600">Marca <span class="text-red-600 font-black">*</span></span>' +
      '<input type="text" id="cad-ov-marca" class="' + ic + '" maxlength="120" value="' + escapeHtml(p.marca || '') + '" autocomplete="off" /></label>' +
      '<label class="block"><span class="text-[10px] font-black uppercase text-slate-600">Categoria <span class="text-red-600 font-black">*</span></span>' +
      '<input type="text" id="cad-ov-cat" class="' + ic + '" maxlength="200" value="' + escapeHtml(p.categoria || '') + '" autocomplete="off" /></label>' +
      '<label class="block sm:col-span-2"><span class="text-[10px] font-black uppercase text-slate-600">Fornecedor (texto)</span>' +
      '<input type="text" id="cad-ov-forn" class="' + ic + '" maxlength="300" value="' + escapeHtml(p.fornecedor || '') + '" autocomplete="off" /></label>' +
      '<label class="block"><span class="text-[10px] font-black uppercase text-slate-600">Código NFe / GM</span>' +
      '<input type="text" id="cad-ov-codnfe" class="' + ic + ' font-mono text-sm" maxlength="64" value="' + escapeHtml(String(p.codigo_nfe || p.codigo || '')) + '" autocomplete="off" /></label>' +
      '<label class="block"><span class="text-[10px] font-black uppercase text-slate-600">Código de barras <span class="text-red-600 font-black">*</span></span>' +
      '<input type="text" id="cad-ov-cb" class="' + ic + ' font-mono text-sm" maxlength="80" value="' + escapeHtml(String(p.codigo_barras || '')) + '" inputmode="numeric" autocomplete="off" /></label>' +
      '<label class="block"><span class="text-[10px] font-black uppercase text-slate-600">Subcategoria</span>' +
      '<input type="text" id="cad-ov-sub" class="' + ic + '" maxlength="200" value="' + escapeHtml(p.subcategoria || '') + '" autocomplete="off" /></label>' +
      '<label class="block"><span class="text-[10px] font-black uppercase text-slate-600">Subcategoria 2</span>' +
      '<input type="text" id="cad-ov-sub2" class="' + ic + '" maxlength="200" value="' + escapeHtml(p.subcategoria_2 || '') + '" autocomplete="off" /></label>' +
      '<label class="block"><span class="text-[10px] font-black uppercase text-slate-600">Subcategoria 3</span>' +
      '<input type="text" id="cad-ov-sub3" class="' + ic + '" maxlength="200" value="' + escapeHtml(p.subcategoria_3 || '') + '" autocomplete="off" /></label>' +
      '<label class="block"><span class="text-[10px] font-black uppercase text-slate-600">Subcategoria 4</span>' +
      '<input type="text" id="cad-ov-sub4" class="' + ic + '" maxlength="200" value="' + escapeHtml(p.subcategoria_4 || '') + '" autocomplete="off" /></label>' +
      '<label class="block"><span class="text-[10px] font-black uppercase text-slate-600">Unidade</span>' +
      '<input type="text" id="cad-ov-un" class="' + ic + '" maxlength="20" value="' + escapeHtml(p.unidade || '') + '" autocomplete="off" /></label>' +
      '<label class="block"><span class="text-[10px] font-black uppercase text-slate-600">Custo unit. (R$) <span class="text-red-600 font-black">*</span></span>' +
      '<input type="text" id="cad-ov-custo" inputmode="decimal" class="' + ic + '" value="' + escapeHtml(pc) + '" autocomplete="off" /></label>' +
      '<label class="block"><span class="text-[10px] font-black uppercase text-slate-600">Preço venda (R$) <span class="text-red-600 font-black">*</span></span>' +
      '<input type="text" id="cad-ov-preco" inputmode="decimal" class="' + ic + '" value="' + escapeHtml(pv) + '" autocomplete="off" /></label>' +
      '<label class="block"><span class="text-[10px] font-black uppercase text-slate-600">Exibir como</span>' +
      '<select id="cad-ov-ativo" class="' + ic + '">' +
      '<option value=""' + (av === '' ? ' selected' : '') + '>Seguir catálogo</option>' +
      '<option value="1"' + (av === '1' ? ' selected' : '') + '>Ativo</option>' +
      '<option value="0"' + (av === '0' ? ' selected' : '') + '>Inativo</option>' +
      '</select></label>' +
      '</div>' +
      '<label class="block mt-3"><span class="text-[10px] font-black uppercase text-slate-600">Descrição</span>' +
      '<textarea id="cad-ov-desc" rows="3" class="w-full rounded-xl border-2 border-emerald-400 px-3 py-2 text-sm font-semibold text-slate-900 bg-white">' + escapeHtml(p.descricao || '') + '</textarea></label>' +
      '<div class="mt-4 flex flex-wrap gap-2">' +
      '<button type="button" id="cadastro-overlay-salvar" class="min-h-[48px] px-5 rounded-xl bg-orange-500 text-white font-black uppercase text-sm border-2 border-orange-600 hover:bg-orange-600 shadow-sm">Salvar no Agro</button>' +
      (ERP_SYNC_HABILITADO
        ? '<button type="button" id="cadastro-overlay-sync-erp" class="min-h-[48px] px-5 rounded-xl bg-amber-600 text-white font-black uppercase text-sm border-2 border-amber-700 hover:bg-amber-700 shadow-sm" title="Produtos/Salvar no ERP legado">Enviar ao ERP</button>'
        : '') +
      '</div>' +
      '<p id="cadastro-overlay-msg" class="mt-2 text-sm font-bold hidden" role="status"></p>' +
      '</div>'
    );
  }

  function buildOverlayOuLoginHtml(p) {
    if (PODE_EDITAR_OVERLAY) {
      return buildOverlayFormHtml(p);
    }
    return (
      '<div class="mt-6 rounded-xl border-2 border-slate-200 bg-slate-100/80 p-4">' +
      '<p class="text-sm font-bold text-slate-800">Edição só no Agro (nome, preço, códigos…)</p>' +
      '<p class="text-xs text-slate-600 mt-2 leading-snug">Entre com seu usuário do sistema para ver o formulário aqui.</p>' +
      '<a href="' + String(LOGIN_OVERLAY_HREF || '').replace(/"/g, '&quot;') + '" class="mt-3 inline-flex min-h-[44px] items-center px-4 rounded-xl bg-emerald-600 text-white font-black uppercase text-xs border-2 border-emerald-800 hover:bg-emerald-700">Entrar para editar</a>' +
      '</div>'
    );
  }

  function bindCadastroOverlaySalvar(p) {
    if (!PODE_EDITAR_OVERLAY) return;
    var btn = document.getElementById('cadastro-overlay-salvar');
    var btnErp = document.getElementById('cadastro-overlay-sync-erp');
    if (!btn) return;

    function montarBody() {
      function gv(id) {
        var el = document.getElementById(id);
        return el ? el.value : '';
      }
      var body = {
        produto_id: String(p.id || ''),
        nome: gv('cad-ov-nome'),
        marca: gv('cad-ov-marca'),
        categoria: gv('cad-ov-cat'),
        fornecedor_texto: gv('cad-ov-forn'),
        unidade: gv('cad-ov-un'),
        codigo_nfe: gv('cad-ov-codnfe'),
        codigo_barras: gv('cad-ov-cb'),
        subcategoria: gv('cad-ov-sub'),
        subcategoria_2: gv('cad-ov-sub2'),
        subcategoria_3: gv('cad-ov-sub3'),
        subcategoria_4: gv('cad-ov-sub4'),
        descricao: gv('cad-ov-desc'),
        preco_custo: gv('cad-ov-custo'),
        preco_venda: gv('cad-ov-preco'),
        validar_cadastro_minimo: true
      };
      var av = gv('cad-ov-ativo');
      if (av === '') body.ativo_exibicao = null;
      else body.ativo_exibicao = av === '1';
      return body;
    }

    function enviar(syncErp) {
      syncErp = !!syncErp && ERP_SYNC_HABILITADO;
      if (btn.disabled || (btnErp && btnErp.disabled)) return;
      var msg = document.getElementById('cadastro-overlay-msg');
      function showMsg(t, ok) {
        if (!msg) return;
        msg.textContent = t || '';
        msg.classList.remove('hidden', 'text-red-700', 'text-emerald-800');
        if (t) {
          msg.classList.remove('hidden');
          msg.classList.add(ok ? 'text-emerald-800' : 'text-red-700');
        } else {
          msg.classList.add('hidden');
        }
      }
      function parseMoedaStrictLocal(s) {
        var t = String(s == null ? '' : s).trim();
        if (!t) return null;
        var t2 = t.replace(/\s/g, '').replace(/\./g, '').replace(',', '.');
        var n = parseFloat(t2);
        if (!isFinite(n) || n < 0) return null;
        return n;
      }
      function gvLoc(id) {
        var el = document.getElementById(id);
        return el ? String(el.value || '').trim() : '';
      }
      if (!gvLoc('cad-ov-nome')) {
        showMsg('Preencha o Nome (obrigatório).', false);
        return;
      }
      if (!gvLoc('cad-ov-marca')) {
        showMsg('Preencha a Marca (obrigatório).', false);
        return;
      }
      if (!gvLoc('cad-ov-cat')) {
        showMsg('Preencha a Categoria (obrigatório).', false);
        return;
      }
      if (!gvLoc('cad-ov-cb')) {
        showMsg('Preencha o Código de barras (obrigatório).', false);
        return;
      }
      if (parseMoedaStrictLocal(gvLoc('cad-ov-custo')) === null) {
        showMsg('Preencha o Custo unit. (R$) com número ≥ 0 (obrigatório).', false);
        return;
      }
      if (parseMoedaStrictLocal(gvLoc('cad-ov-preco')) === null) {
        showMsg('Preencha o Preço venda (R$) com número ≥ 0 (obrigatório).', false);
        return;
      }
      showMsg('');
      var origS = btn.textContent;
      var origE = btnErp ? btnErp.textContent : '';
      btn.disabled = true;
      if (btnErp) btnErp.disabled = true;
      if (syncErp) {
        if (btnErp) btnErp.textContent = 'Enviando…';
      } else {
        btn.textContent = 'Salvando…';
      }

      var body = montarBody();
      if (syncErp) body.sincronizar_erp = true;

      var tok = U.csrf();
      fetch(URL_OVERLAY_SALVAR, {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': tok },
        body: JSON.stringify(body)
      }).then(function (r) {
        if (r.status === 403 || r.status === 401) {
          throw new Error('Sem sessão ou permissão. Use «Entrar para editar» ou abra de novo após login.');
        }
        return jsonOuErroHumano(r);
      }).then(function (j) {
        if (!j.ok) throw new Error(j.erro || 'Falha ao salvar');
        if (syncErp) showMsg('Salvo no Agro e replicado no ERP legado.', true);
        else if (j.somente_agro) {
          showMsg(
            ERP_SYNC_HABILITADO
              ? 'Salvo no Agro. Use «Enviar ao ERP» quando quiser replicar.'
              : 'Salvo no SisVale.',
            true
          );
        } else showMsg('Salvo no Agro.', true);
        if (j.produto) {
          if (typeof window.agroPdvPatchCatalogoCache === 'function') {
            window.agroPdvPatchCatalogoCache(j.produto);
          }
          agroCadastroMergeProdutoCacheLocal(j.produto);
          renderDetalheCompleto(j.produto);
        } else {
          carregarDetalheProduto(String(p.id || ''));
        }
      }).catch(function (e) {
        showMsg(e.message || 'Erro ao salvar', false);
      }).finally(function () {
        btn.disabled = false;
        btn.textContent = origS;
        if (btnErp) {
          btnErp.disabled = false;
          btnErp.textContent = origE;
        }
      });
    }

    btn.onclick = function () {
      enviar(false);
    };
    if (btnErp && ERP_SYNC_HABILITADO) {
      btnErp.onclick = function () {
        enviar(true);
      };
    }
  }

  function renderDetalheCompleto(p) {
    if (!detalheEl) return;
    var img = p.imagem
      ? '<div class="mb-4 flex justify-center"><img src="' + String(p.imagem).replace(/"/g, '&quot;') + '" alt="" class="max-h-48 rounded-xl border border-slate-200 object-contain bg-slate-50" loading="lazy" /></div>'
      : '';
    var inativo = p.cadastro_inativo || p.inativo
      ? '<span class="inline-block mb-2 mr-2 px-2 py-1 rounded-lg bg-amber-100 text-amber-900 text-xs font-black uppercase">Cadastro inativo</span>'
      : '';
    var oculto = p.ocultar_nas_vendas
      ? '<span class="inline-block mb-2 px-2 py-1 rounded-lg bg-orange-100 text-orange-900 text-xs font-black uppercase">Ocultar nas vendas</span>'
      : '';
    var desc = p.descricao
      ? '<p class="text-sm text-slate-600 mt-3 leading-relaxed whitespace-pre-wrap">' + escapeHtml(p.descricao) + '</p>'
      : '';
    var ncm = p.ncm
      ? '<div class="mt-2 text-sm"><span class="font-bold text-slate-500">NCM</span> · ' + escapeHtml(p.ncm) + '</div>'
      : '';
    var pv = Number(p.preco_venda);
    var pc = p.preco_custo != null ? Number(p.preco_custo) : null;
    var pca = p.preco_custo_com_acrescimos != null ? Number(p.preco_custo_com_acrescimos) : null;
    var mvaRs = p.mva_lucro_reais != null ? Number(p.mva_lucro_reais) : null;
    var mvaPct = p.mva_lucro_percentual != null ? Number(p.mva_lucro_percentual) : null;
    var prev =
      '<div class="mt-4 rounded-2xl border-2 border-slate-200 bg-slate-50/80 p-3 sm:p-4">' +
      '<h4 class="text-xs font-black uppercase tracking-widest text-slate-500 mb-3">Pré-visualização · custos e precificação</h4>' +
      '<div class="grid grid-cols-2 lg:grid-cols-4 gap-3">' +
      '<div class="rounded-xl bg-white border border-slate-200 p-3"><div class="text-[10px] font-black uppercase text-slate-400">Preço de custo (R$)</div>' +
      '<div class="text-lg font-black text-slate-900 mt-1">' + (pc != null && isFinite(pc) ? fmtMoney(pc) : '—') + '</div></div>' +
      '<div class="rounded-xl bg-white border border-slate-200 p-3"><div class="text-[10px] font-black uppercase text-slate-400">(MVA) Lucro R$</div>' +
      '<div class="text-lg font-black text-emerald-800 mt-1">' + (mvaRs != null && isFinite(mvaRs) ? fmtMoney(mvaRs) : '—') + '</div></div>' +
      '<div class="rounded-xl bg-white border border-slate-200 p-3"><div class="text-[10px] font-black uppercase text-slate-400">(MVA) Lucro %</div>' +
      '<div class="text-lg font-black text-emerald-800 mt-1">' + (mvaPct != null && isFinite(mvaPct) ? fmtNumPt(mvaPct, 2) + ' %' : '—') + '</div></div>' +
      '<div class="rounded-xl border border-orange-200 p-3 bg-orange-50/70"><div class="text-[10px] font-black uppercase text-orange-700">Preço de venda (R$)</div>' +
      '<div class="text-lg font-black text-orange-700 mt-1">' + (isFinite(pv) ? fmtMoney(pv) : '—') + '</div></div>' +
      '</div>' +
      '<div class="mt-2 text-xs text-slate-500 font-semibold">Preço de custo com acréscimos: <span class="text-slate-800">' +
      (pca != null && isFinite(pca) ? fmtMoney(pca) : '—') + '</span></div></div>';

    var flags =
      '<div class="mt-4 flex flex-wrap gap-2 items-center">' +
      '<span class="text-[10px] font-black uppercase text-slate-500">Opções</span>' +
      '<span class="inline-flex items-center gap-1 rounded-xl border border-slate-200 bg-white px-2 py-1 text-xs font-bold text-slate-700">KIT ' + badgeSim(!!p.eh_kit) + '</span>' +
      '<span class="inline-flex items-center gap-1 rounded-xl border border-slate-200 bg-white px-2 py-1 text-xs font-bold text-slate-700">Custo auto ' + badgeSim(!!p.calcular_custo_automaticamente) + '</span>' +
      '<span class="inline-flex items-center gap-1 rounded-xl border border-slate-200 bg-white px-2 py-1 text-xs font-bold text-slate-700">Venda c/ estoque negativo ' + badgeSim(!!p.permite_venda_estoque_negativo) + '</span>' +
      '<span class="inline-flex items-center gap-1 rounded-xl border border-slate-200 bg-white px-2 py-1 text-xs font-bold text-slate-700">Não alertar estoque ' + badgeSim(!!p.nao_emitir_alertas_estoque) + '</span>' +
      '</div>';

    var comissaoLinha = '';
    if (p.comissao_vendedor_reais != null && isFinite(Number(p.comissao_vendedor_reais))) {
      comissaoLinha += dlRow('Comissão vendedor (R$)', fmtMoney(Number(p.comissao_vendedor_reais)));
    }
    if (p.comissao_vendedor_percentual != null && String(p.comissao_vendedor_percentual).trim() !== '') {
      comissaoLinha += dlRow('Comissão vendedor (%)', fmtNumPt(p.comissao_vendedor_percentual, 2) + ' %');
    }

    detalheEl.innerHTML =
      '<div class="max-w-3xl">' +
      inativo + oculto +
      img +
      '<h3 class="text-xl sm:text-2xl font-black text-slate-900 leading-tight">' + escapeHtml(p.nome || '—') + '</h3>' +
      prev +
      flags +
      '<dl class="mt-4 grid gap-2 text-sm">' +
      dlRow('Modelo', p.modelo) +
      dlRow('Fornecedor padrão (nome)', p.fornecedor) +
      dlRow('Fornecedor padrão (ID)', p.fornecedor_padrao_id) +
      dlRow('ID ERP', p.id) +
      dlRow('Marca', p.marca) +
      dlRow('Código', p.codigo) +
      dlRow('Código NFe', p.codigo_nfe) +
      dlRow('Código barras', p.codigo_barras) +
      dlRow('Unidade', p.unidade) +
      dlRow('Unidade de estoque', p.unidade_estoque) +
      dlRow('Categoria', p.categoria) +
      dlRow('Subcategoria 1', p.subcategoria) +
      dlRow('Subcategoria 2', p.subcategoria_2) +
      dlRow('Subcategoria 3', p.subcategoria_3) +
      dlRow('Subcategoria 4', p.subcategoria_4) +
      dlRow('Subcategoria', p.categoria_listagem) +
      dlRow('Prateleira / local', p.prateleira) +
      dlRow('Estoque mínimo', p.estoque_minimo != null ? fmtNumPt(p.estoque_minimo, 4) : '') +
      dlRow('Estoque máximo', p.estoque_maximo != null ? fmtNumPt(p.estoque_maximo, 4) : '') +
      dlRowHtml('Cadastro inativo', badgeSim(!!(p.cadastro_inativo || p.inativo))) +
      dlRowHtml('Ocultar nas vendas', badgeSim(!!p.ocultar_nas_vendas)) +
      comissaoLinha +
      '</dl>' +
      ncm +
      desc +
      '<details class="mt-6 rounded-2xl border border-slate-200 bg-white p-3 open:shadow-sm" open>' +
      '<summary class="cursor-pointer text-sm font-black uppercase text-slate-700 min-h-[44px] flex items-center">Composição (kit / insumos)</summary>' +
      '<div class="mt-3">' + renderTabelaComposicao(p.composicao) + '</div></details>' +
      '<details class="mt-3 rounded-2xl border border-slate-200 bg-white p-3">' +
      '<summary class="cursor-pointer text-sm font-black uppercase text-slate-700 min-h-[44px] flex items-center">Similares</summary>' +
      '<div class="mt-3">' + renderSimilares(p.similares) + '</div></details>' +
      buildOverlayOuLoginHtml(p) +
      '<p class="mt-4 text-xs text-slate-500 leading-relaxed">' +
      (PODE_EDITAR_OVERLAY
        ? 'Bloco verde: edição só no Agro. Blocos acima (custos, kit, composição): leitura do espelho Mongo/ERP.'
        : 'Conteúdo acima: leitura do espelho Mongo/ERP. Alterações definitivas continuam no ERP.') +
      '</p>' +
      '</div>';
    bindCadastroOverlaySalvar(p);
  }

  function renderDetalheResumido(p) {
    if (!detalheEl) return;
    if (!p) {
      detalheEl.innerHTML = '<p class="text-base font-semibold text-slate-500">Selecione um item na lista.</p>';
      return;
    }
    var img = p.imagem
      ? '<div class="mb-4 flex justify-center"><img src="' + String(p.imagem).replace(/"/g, '&quot;') + '" alt="" class="max-h-48 rounded-xl border border-slate-200 object-contain bg-slate-50" loading="lazy" /></div>'
      : '';
    var inativo = p.inativo
      ? '<span class="inline-block mb-3 px-2 py-1 rounded-lg bg-amber-100 text-amber-900 text-xs font-black uppercase">Inativo no ERP</span>'
      : '';
    detalheEl.innerHTML =
      '<div class="max-w-xl">' + inativo + img +
      '<h3 class="text-xl font-black text-slate-900">' + escapeHtml(p.nome || '—') + '</h3>' +
      '<p class="text-lg font-bold text-emerald-700 mt-2">' + fmtMoney(p.preco_venda) + '</p>' +
      '<dl class="mt-4 grid gap-2 text-sm">' +
      dlRow('ID ERP', p.id) + dlRow('Marca', p.marca) + dlRow('Código NFe', p.codigo_nfe) +
      dlRow('Código barras', p.codigo_barras) + dlRow('Fornecedor', p.fornecedor) +
      '</dl><p class="mt-3 text-sm text-slate-500">Detalhe completo indisponível.</p></div>';
  }

  function carregarDetalheProduto(id) {
    if (!detalheEl) return;
    var seq = ++detalheReqSeq;
    detalheEl.innerHTML = '<p class="text-base font-semibold text-slate-500 py-8">Carregando cadastro completo…</p>';
    fetch(urlDetalheProduto(id), { credentials: 'same-origin' })
      .then(function (r) {
        return r.json().then(function (j) { return { ok: r.ok, j: j }; }).catch(function () { return { ok: r.ok, j: {} }; });
      })
      .then(function (x) {
        if (seq !== detalheReqSeq) return;
        if (!x.ok || !x.j || !x.j.ok) {
          var row = ultimos.find(function (u) { return String(u.id) === String(id); });
          detalheEl.innerHTML = '<p class="text-red-700 font-bold">' + escapeHtml((x.j && x.j.erro) || 'Falha ao carregar detalhe') + '</p>';
          if (row) renderDetalheResumido(row);
          return;
        }
        renderDetalheCompleto(x.j.produto);
      })
      .catch(function () {
        if (seq !== detalheReqSeq) return;
        var row = ultimos.find(function (u) { return String(u.id) === String(id); });
        detalheEl.innerHTML = '<p class="text-red-700 font-bold">Erro de rede ao carregar detalhe.</p>';
        if (row) renderDetalheResumido(row);
      });
  }

  function renderDetalhe(p) {
    if (!detalheEl) return;
    if (!p) {
      detalheEl.innerHTML = '<p class="text-base font-semibold text-slate-500">Selecione um item na lista.</p>';
      return;
    }
    carregarDetalheProduto(p.id);
  }

  function ordenar(campo) {
    if (!listaEl) return;
    if (ordenacaoAtual.campo === campo) {
      ordenacaoAtual.direcao = ordenacaoAtual.direcao === 'asc' ? 'desc' : 'asc';
    } else {
      ordenacaoAtual.campo = campo;
      ordenacaoAtual.direcao = 'asc';
    }
    pagina = 1;
    carregar();
  }

  function renderLista(produtos) {
    var tbody = listaEl || document.getElementById('cadastro-lista');
    if (!tbody) return;
    ultimos = produtos || [];
    tbody.innerHTML = '';
    if (!ultimos.length) {
      var trEmpty = document.createElement('tr');
      trEmpty.innerHTML = '<td colspan="8" class="p-8 text-center text-slate-500 font-semibold">Nenhum produto encontrado.</td>';
      tbody.appendChild(trEmpty);
      return;
    }
    ultimos.forEach(function (p) {
      var tr = document.createElement('tr');
      tr.setAttribute('data-prod-id', String(p.id));
      tr.className = 'border-b border-slate-100 hover:bg-slate-50 cursor-pointer transition-colors';
      var cod = p.codigo_nfe || p.codigo || '';
      var custoTxt = p.preco_custo != null && isFinite(Number(p.preco_custo)) ? fmtMoney(Number(p.preco_custo)) : '—';
      var vendaTxt = fmtMoney(p.preco_venda);
      tr.innerHTML =
        '<td class="px-4 py-3">' +
        '<div class="font-semibold text-slate-900">' + escapeHtml(p.nome || '—') + '</div>' +
        (String(cod).trim() !== '' ? '<div class="text-xs text-slate-400"> ' + escapeHtml(String(cod)) + '</div>' : '') +
        '</td>' +
        '<td data-coluna="marca" class="px-4 py-3 text-slate-700">' + escapeHtml(p.marca || '-') + '</td>' +
        '<td data-coluna="unidade" class="px-4 py-3 text-slate-700">' + escapeHtml(p.unidade || '-') + '</td>' +
        '<td data-coluna="categoria" class="px-4 py-3 text-slate-700">' + escapeHtml(p.categoria || '-') + '</td>' +
        '<td data-coluna="subcategoria" class="px-4 py-3 text-slate-600">' +
        escapeHtml((p.categoria_listagem != null && String(p.categoria_listagem).trim()) ? String(p.categoria_listagem).trim() : (p.subcategoria || '-')) +
        '</td>' +
        '<td data-coluna="preco_custo" class="px-4 py-3 text-slate-600 whitespace-nowrap">' + custoTxt + '</td>' +
        '<td class="px-4 py-3 font-semibold text-emerald-600 whitespace-nowrap">' + vendaTxt + '</td>' +
        '<td class="px-4 py-3 text-right cadastro-acoes">' +
        '<span class="inline-flex items-center justify-end gap-2 text-lg">' +
        '<button type="button" class="cadastro-btn-edit-modal inline-flex h-9 min-w-[2.25rem] items-center justify-center rounded-lg border border-slate-200 bg-white hover:bg-slate-50 cursor-pointer" title="Editar (modal)">✏️</button>' +
        '<button type="button" class="cadastro-btn-etiqueta inline-flex h-9 min-w-[2.25rem] items-center justify-center rounded-lg border border-emerald-200 bg-emerald-50 hover:bg-emerald-100 cursor-pointer" title="Imprimir etiqueta">🖨️</button>' +
        '<span class="inline-flex h-9 min-w-[2.25rem] items-center justify-center rounded-lg border border-slate-100 bg-slate-50 text-slate-400 cursor-not-allowed select-none opacity-80" title="Exclusão somente no ERP">🗑️</span>' +
        '</span></td>';
      tbody.appendChild(tr);
      var btnEditModal = tr.querySelector('.cadastro-btn-edit-modal');
      if (btnEditModal) {
        btnEditModal.addEventListener('click', function (e) {
          e.preventDefault();
          e.stopPropagation();
          if (typeof window.abrirModalProduto === 'function') {
            window.abrirModalProduto(p);
          }
        });
      }
      var btnEtq = tr.querySelector('.cadastro-btn-etiqueta');
      if (btnEtq) {
        btnEtq.addEventListener('click', function (e) {
          e.preventDefault();
          e.stopPropagation();
          abrirModalEtiquetaCadastro(p);
        });
      }
      tr.addEventListener('click', function (e) {
        if (e.target.closest('a')) return;
        if (e.target.closest('.cadastro-acoes')) return;
        if (typeof window.abrirModalProduto === 'function') {
          window.abrirModalProduto(p);
        } else {
          window.location.href = urlCadastroErpProduto(p.id);
        }
      });
    });
    aplicarVisibilidadeColunas();
  }

  function aplicarVisibilidadeColunas() {
    var tbl = document.getElementById('cadastro-tabela-produtos');
    var menu = document.getElementById('menu-colunas');
    if (!tbl || !menu) return;
    Array.prototype.forEach.call(menu.querySelectorAll('input[type="checkbox"][data-col]'), function (input) {
      var col = input.getAttribute('data-col');
      var show = input.checked;
      Array.prototype.forEach.call(tbl.querySelectorAll('[data-coluna="' + col + '"]'), function (el) {
        el.style.display = show ? '' : 'none';
      });
    });
  }

  var btnCol = document.getElementById('btn-colunas');
  var menuCol = document.getElementById('menu-colunas');
  if (btnCol && menuCol) {
    btnCol.addEventListener('click', function (e) {
      e.stopPropagation();
      menuCol.classList.toggle('hidden');
    });
    menuCol.addEventListener('click', function (e) {
      e.stopPropagation();
    });
    document.addEventListener('click', function () {
      menuCol.classList.add('hidden');
    });
    Array.prototype.forEach.call(menuCol.querySelectorAll('input[type="checkbox"][data-col]'), function (input) {
      input.addEventListener('change', function () {
        aplicarVisibilidadeColunas();
      });
    });
  }

  /** Mesma conjunção de palavras que o PDV (AND), aplicada na lista já retornada pela API. */
  function cadastroErpFiltrarBuscaMultiPalavra(produtos, qRaw) {
    var q0 = String(qRaw || '').trim();
    if (!q0 || q0.indexOf(' ') === -1) return produtos || [];
    if (typeof filtrarProdutosBuscaInteligente !== 'function' || typeof normalizarBuscaLocal !== 'function') {
      return produtos || [];
    }
    var termoNorm = normalizarBuscaLocal(q0);
    if (!termoNorm || String(termoNorm).trim().length < 2) return produtos || [];
    var modo = /^\d{8,}$/.test(q0.replace(/\s/g, '')) ? 'scanner' : 'normal';
    var pool = (produtos || []).map(function (r) {
      return {
        id: r.id,
        nome: r.nome,
        marca: r.marca,
        codigo: r.codigo,
        codigo_nfe: r.codigo_nfe,
        codigo_barras: r.codigo_barras,
        prateleira: r.prateleira,
        busca_texto: r.busca_texto,
        index_codigos: r.index_codigos
      };
    });
    var fil = filtrarProdutosBuscaInteligente(pool, termoNorm, modo);
    if (!fil || !fil.length) return produtos || [];
    var keep = {};
    fil.forEach(function (r) { keep[String(r.id)] = true; });
    return (produtos || []).filter(function (r) { return keep[String(r.id)]; });
  }

  function urlBuscaCadastro(qRaw) {
    var params = new URLSearchParams();
    params.set('q', String(qRaw || '').trim());
    params.set('limit', '80');
    if (ativosEl && ativosEl.checked) {
      params.set('ativo', '1');
    } else if (ativosEl) {
      params.set('inativos', '1');
    }
    return API + '?' + params.toString();
  }

  function cadastroModoBuscaPdv(/* qRaw */) {
    /* Digitação na lista — modo normal (scanner só no PDV/leitor). */
    return 'normal';
  }

  function cadastroLimiteBuscaPdv() {
    return (typeof window.BUSCA_PDV_LIM_MAX === 'number' && window.BUSCA_PDV_LIM_MAX > 0)
      ? window.BUSCA_PDV_LIM_MAX
      : 60;
  }

  function atualizarMeta(data, produtos) {
    if (!metaEl || !pagWrap || !prevEl || !nextEl || !buscaEl) return;
    var q = (buscaEl.value || '').trim();
    if (data.modo === 'busca') {
      modoLista = false;
      metaEl.textContent = 'Busca · ' + (produtos.length) + ' resultado(s)';
      pagWrap.classList.add('hidden');
    } else {
      modoLista = true;
      metaEl.textContent = 'Lista A–Z · página ' + data.pagina + (data.has_more ? ' (há próxima)' : ' (fim)');
      pagWrap.classList.remove('hidden');
      prevEl.disabled = data.pagina <= 1;
      nextEl.disabled = !data.has_more;
    }
  }

  /** Catálogo local do PDV (mesmo cache da Consulta) — busca instantânea antes do Mongo. */
  function agroCadastroMergeProdutoCacheLocal(produto) {
    if (!produto || produto.id == null) return;
    var patch = {
      id: String(produto.id),
      nome: produto.nome,
      marca: produto.marca,
      codigo_nfe: produto.codigo_gm || produto.codigo_nfe || produto.codigo,
      codigo_barras: produto.codigo_barras,
      preco_venda: produto.preco_venda,
      preco_custo: produto.preco_custo,
      categoria: produto.categoria,
      subcategoria: produto.subcategoria,
      fornecedor: produto.fornecedor,
      unidade: produto.unidade,
      descricao: produto.descricao,
      inativo: !!produto.inativo
    };
    cadastroCatalogoPdvCacheArray();
    if (!Array.isArray(_cadastroCatLocal)) return;
    var pid = String(produto.id);
    var row = _cadastroCatById ? _cadastroCatById.get(pid) : null;
    if (row) {
      Object.assign(row, patch);
      return;
    }
    for (var i = 0; i < _cadastroCatLocal.length; i++) {
      if (String(_cadastroCatLocal[i].id) === pid) {
        Object.assign(_cadastroCatLocal[i], patch);
        if (_cadastroCatById) _cadastroCatById.set(pid, _cadastroCatLocal[i]);
        return;
      }
    }
  }
  window.agroCadastroMergeProdutoCacheLocal = agroCadastroMergeProdutoCacheLocal;

  function cadastroCatalogoPdvCacheArray() {
    if (_cadastroCatInited) return _cadastroCatLocal || [];
    _cadastroCatInited = true;
    _cadastroCatLocal = [];
    _cadastroCatById = new Map();
    try {
      var raw = localStorage.getItem(PDV_CACHE_KEY);
      if (!raw) return [];
      var p = JSON.parse(raw);
      if (p && Array.isArray(p.produtos) && p.produtos.length) {
        _cadastroCatLocal = p.produtos;
        _cadastroCatLocal.forEach(function (prod) {
          if (prod && prod.id != null) _cadastroCatById.set(String(prod.id), prod);
        });
      }
    } catch (e1) {
      _cadastroCatLocal = [];
      _cadastroCatById = new Map();
    }
    return _cadastroCatLocal;
  }

  function cadastroCatalogoPdvById() {
    cadastroCatalogoPdvCacheArray();
    return _cadastroCatById || new Map();
  }

  function cadastroPdvParaLinhaLista(p, catalogById) {
    var id = String(p.id || '');
    var loc = null;
    if (catalogById && id && typeof catalogById.get === 'function') {
      loc = catalogById.get(id) || null;
    }
    var src = loc ? Object.assign({}, loc, p) : p;
    var row = {
      id: src.id,
      nome: src.nome,
      marca: src.marca,
      codigo: src.codigo,
      codigo_nfe: src.codigo_nfe || src.codigo,
      codigo_barras: src.codigo_barras,
      preco_venda: src.preco_venda,
      preco_custo: src.preco_custo,
      categoria: src.categoria,
      subcategoria: src.subcategoria,
      categoria_listagem: src.categoria_listagem,
      prateleira: src.prateleira,
      fornecedor: src.fornecedor,
      imagem: src.imagem,
      inativo: !!src.inativo,
      unidade: src.unidade,
      descricao: src.descricao,
      busca_texto: src.busca_texto,
      index_codigos: src.index_codigos
    };
    if (typeof window.agroAplicarPatchPdvNoProduto === 'function') {
      row = window.agroAplicarPatchPdvNoProduto(row);
    }
    return row;
  }

  function cadastroFiltrarAtivosLocal(arr) {
    if (!Array.isArray(arr)) return [];
    if (ativosEl && !ativosEl.checked) return arr;
    return arr.filter(function (p) { return !p.inativo; });
  }

  /** Pipeline idêntico ao PDV: filtrar → ordenar por relevância (catálogo local). */
  function cadastroBuscarLocalComoPdv(qRaw) {
    var q = String(qRaw || '').trim();
    if (!q) return [];
    if (!buscaProntaParaCatalogo(q)) return [];
    var catalog = cadastroCatalogoPdvCacheArray();
    var catalogById = cadastroCatalogoPdvById();
    if (!catalog.length || typeof filtrarProdutosBuscaInteligente !== 'function' || typeof normalizarBuscaLocal !== 'function') {
      return [];
    }
    var termoNorm = normalizarBuscaLocal(q);
    if (!termoNorm) return [];
    var modo = cadastroModoBuscaPdv(q);
    var fil = filtrarProdutosBuscaInteligente(catalog, termoNorm, modo);
    fil = cadastroFiltrarAtivosLocal(fil);
    if (typeof ordenarSugestoesPdv === 'function') {
      fil = ordenarSugestoesPdv(fil, termoNorm, catalogById);
    }
    return fil.slice(0, cadastroLimiteBuscaPdv());
  }

  function cadastroFinalizarLinhasBusca(pdvRows, catalog) {
    return (pdvRows || []).map(function (p) { return cadastroPdvParaLinhaLista(p, catalog); });
  }

  function cadastroAplicarOrdenacaoCliente(rows) {
    if (!ordenacaoAtual.campo || !rows || !rows.length) return rows || [];
    var copia = (rows || []).slice();
    var campo = ordenacaoAtual.campo;
    var desc = ordenacaoAtual.direcao === 'desc';
    if (campo === 'preco_custo' || campo === 'preco_venda') {
      copia.sort(function (a, b) {
        var va = Number(a[campo]);
        var vb = Number(b[campo]);
        if (!isFinite(va)) va = -Infinity;
        if (!isFinite(vb)) vb = -Infinity;
        if (va !== vb) return desc ? vb - va : va - vb;
        return String(a.id || '').localeCompare(String(b.id || ''));
      });
      return copia;
    }
    copia.sort(function (a, b) {
      var ta = String(a[campo] || '').toLowerCase();
      var tb = String(b[campo] || '').toLowerCase();
      var c = ta.localeCompare(tb, 'pt-BR');
      if (c !== 0) return desc ? -c : c;
      return String(a.id || '').localeCompare(String(b.id || ''));
    });
    return copia;
  }

  function carregarListaPaginada(gen, sig) {
    var pLista = fetch(urlFetch(), { credentials: 'same-origin', signal: sig })
      .then(function (r) { return r.json().then(function (j) { return { ok: r.ok, j: j }; }); });
    return Promise.all([pLista, fetchPendentesBadgePromise(sig ? { signal: sig } : undefined)])
      .then(function (pair) {
        var x = pair[0];
        if (gen !== carregarGen) return;
        if (!x.j || !x.j.ok) {
          throw new Error((x.j && x.j.erro) || 'Falha ao carregar');
        }
        var produtos = x.j.produtos || [];
        if (typeof window.agroAplicarPatchPdvNoProduto === 'function') {
          produtos = produtos.map(function (p) { return window.agroAplicarPatchPdvNoProduto(p); });
        }
        produtos = cadastroAplicarOrdenacaoCliente(produtos);
        atualizarMeta(x.j, produtos);
        renderLista(produtos);
      });
  }

  function apiProdutoParaLinhaCadastro(p) {
    if (!p || p.id == null) return p;
    return {
      id: p.id,
      nome: p.nome,
      marca: p.marca,
      codigo: p.codigo,
      codigo_nfe: p.codigo_nfe || p.codigo_gm || p.codigo,
      codigo_barras: p.codigo_barras,
      preco_venda: p.preco_venda,
      preco_custo: p.preco_custo,
      categoria: p.categoria,
      subcategoria: p.subcategoria,
      categoria_listagem: p.categoria_listagem,
      prateleira: p.prateleira,
      fornecedor: p.fornecedor,
      imagem: p.imagem,
      inativo: !!p.inativo,
      unidade: p.unidade,
      descricao: p.descricao,
      busca_texto: p.busca_texto,
      index_codigos: p.index_codigos
    };
  }

  function carregarBuscaApi(qRaw, gen, sig, localBaselinePdv) {
    var catalogById = cadastroCatalogoPdvById();
    var termoNorm = (typeof normalizarBuscaLocal === 'function') ? normalizarBuscaLocal(qRaw) : String(qRaw || '').toLowerCase();
    var urlBusca = urlBuscaCadastro(qRaw);
    var pLista = fetch(urlBusca, { credentials: 'same-origin', signal: sig })
      .then(function (r) { return r.json().then(function (j) { return { ok: r.ok, j: j }; }); });
    return Promise.all([pLista, fetchPendentesBadgePromise(sig ? { signal: sig } : undefined)])
      .then(function (pair) {
        var x = pair[0];
        if (gen !== carregarGen) return;
        if (!x.j || x.j.ok === false || x.j.erro) {
          throw new Error((x.j && x.j.erro) || 'Falha ao buscar');
        }
        var apiRows = Array.isArray(x.j.produtos) ? x.j.produtos : [];
        if (!apiRows.length) {
          return fetch(URL_BUSCAR_PDV + '?q=' + encodeURIComponent(qRaw), {
            credentials: 'same-origin',
            signal: sig,
          })
            .then(function (r) { return r.json(); })
            .then(function (j2) {
              if (gen !== carregarGen) return;
              if (j2 && !j2.erro && Array.isArray(j2.produtos) && j2.produtos.length) {
                apiRows = j2.produtos;
              }
              return apiRows;
            })
            .catch(function () { return apiRows; });
        }
        return apiRows;
      })
      .then(function (apiRows) {
        if (gen !== carregarGen) return;
        apiRows = apiRows || [];
        if (pareceCodigoBusca(qRaw) && typeof filtrarProdutosBuscaInteligente === 'function' && typeof normalizarBuscaLocal === 'function') {
          var poolCod = apiRows.map(apiProdutoParaLinhaCadastro);
          var filCod = filtrarProdutosBuscaInteligente(
            poolCod,
            normalizarBuscaLocal(qRaw),
            /^\d{8,}$/.test(String(qRaw || '').replace(/\s/g, '')) ? 'scanner' : 'normal'
          );
          if (filCod.length) {
            var keepCod = {};
            filCod.forEach(function (r) { keepCod[String(r.id)] = true; });
            apiRows = apiRows.filter(function (r) { return keepCod[String(r.id)]; });
          }
        }
        var pdvRows = localBaselinePdv && localBaselinePdv.length ? localBaselinePdv : [];
        if (typeof mesclarBuscaPdvLocalComApi === 'function') {
          pdvRows = mesclarBuscaPdvLocalComApi(termoNorm, pdvRows, apiRows, catalogById, cadastroLimiteBuscaPdv());
        } else if (apiRows.length) {
          pdvRows = apiRows.slice(0, cadastroLimiteBuscaPdv());
        }
        var produtos = cadastroFinalizarLinhasBusca(pdvRows, catalogById);
        if (ordenacaoAtual.campo) {
          produtos = cadastroAplicarOrdenacaoCliente(produtos);
        }
        atualizarMeta({ modo: 'busca' }, produtos);
        renderLista(produtos);
      });
  }

  function carregar() {
    if (!listaEl) return;
    if (typeof resetLoading === 'function') resetLoading();
    var g = ++carregarGen;
    if (carregarAbort) {
      try {
        carregarAbort.abort();
      } catch (e) { /* ignore */ }
    }
    carregarAbort = typeof AbortController !== 'undefined' ? new AbortController() : null;
    mostrarErro('');
    var sig = carregarAbort ? carregarAbort.signal : undefined;
    var qBusca = (buscaEl && buscaEl.value) ? buscaEl.value.trim() : '';

    if (qBusca && !buscaProntaParaCatalogo(qBusca)) {
      if (metaEl) metaEl.textContent = 'Mín. 2 letras ou código (GM… / 4+ dígitos).';
      if (listaEl) {
        listaEl.innerHTML = '<tr><td colspan="8" class="p-6 text-center text-slate-500 font-semibold">Continue digitando para buscar no catálogo.</td></tr>';
      }
      return;
    }

    if (qBusca) {
      clearTimeout(buscaMergeTimer);
      var catalog = cadastroCatalogoPdvCacheArray();
      var locaisPdv = cadastroBuscarLocalComoPdv(qBusca);
      var linhasLocais = cadastroFinalizarLinhasBusca(locaisPdv, catalog);
      if (ordenacaoAtual.campo) {
        linhasLocais = cadastroAplicarOrdenacaoCliente(linhasLocais);
      }
      var hadLocal = linhasLocais.length > 0;
      var skipPreviewLocal = pareceCodigoBusca(qBusca);
      if (hadLocal && !skipPreviewLocal) {
        atualizarMeta({ modo: 'busca' }, linhasLocais);
        renderLista(linhasLocais);
        setLoading(false);
      } else if (skipPreviewLocal) {
        setLoading(true);
        if (listaEl) {
          listaEl.innerHTML = '<tr><td colspan="8" class="p-6 text-center text-slate-500 font-semibold">Buscando código…</td></tr>';
        }
      } else {
        setLoading(true);
        if (listaEl) {
          listaEl.innerHTML = '<tr><td colspan="8" class="p-6 text-center text-slate-500 font-semibold">Buscando no catálogo…</td></tr>';
        }
      }
      var mergeSeq = ++buscaMergeSeq;
      var delayApi = skipPreviewLocal ? 0 : (hadLocal ? 0 : (pareceCodigoBusca(qBusca) ? 100 : 220));
      buscaMergeTimer = setTimeout(function () {
        if (mergeSeq !== buscaMergeSeq || g !== carregarGen) return;
        carregarBuscaApi(qBusca, g, sig, locaisPdv)
          .catch(function (err) {
            if (err && err.name === 'AbortError') return;
            if (g !== carregarGen) return;
            if (hadLocal && !skipPreviewLocal) return;
            var m = err.message || 'Erro de rede';
            var mongo = /mongo/i.test(m);
            if (mongo) {
              mostrarErro(m, { hintMongo: true });
              if (listaEl) {
                listaEl.innerHTML = '<tr><td colspan="8" class="p-8 text-center text-slate-600 font-semibold leading-relaxed">Catálogo indisponível sem Mongo. Use o botão «Estoque» no PDV para sincronizar o catálogo local ou configure o <code class="text-xs bg-slate-100 px-1 rounded">.env</code>.</td></tr>';
              }
            } else {
              mostrarErro(m);
              if (listaEl) listaEl.innerHTML = '';
            }
            if (metaEl) metaEl.textContent = '—';
          })
          .finally(function () {
            setLoading(false);
          });
      }, delayApi);
      if (hadLocal) fetchPendentesBadgePromise(sig ? { signal: sig } : undefined);
      return;
    }

    setLoading(true);
    carregarListaPaginada(g, sig)
      .catch(function (err) {
        if (err && err.name === 'AbortError') return;
        if (g !== carregarGen) return;
        var m = err.message || 'Erro de rede';
        var mongo = /mongo/i.test(m);
        if (mongo) {
          mostrarErro(m, { hintMongo: true });
          if (listaEl) {
            listaEl.innerHTML = '<tr><td colspan="8" class="p-8 text-center text-slate-600 font-semibold leading-relaxed">Catálogo indisponível sem Mongo. Use o botão acima para ir à aba <strong>Grupos</strong> ou configure o <code class="text-xs bg-slate-100 px-1 rounded">.env</code>.</td></tr>';
          }
        } else {
          mostrarErro(m);
          if (listaEl) listaEl.innerHTML = '';
        }
        if (metaEl) metaEl.textContent = '—';
      })
      .finally(function () {
        setLoading(false);
      });
  }

  function buscaProntaParaCatalogo(q) {
    q = String(q || '').trim();
    if (!q) return false;
    if (pareceCodigoBusca(q)) return true;
    return q.length >= 2;
  }

  function pareceCodigoBusca(q) {
    q = String(q || '').trim();
    var lim = q.replace(/\W/g, '');
    if (!lim) return false;
    if (/^\d+$/.test(lim) && lim.length >= 4) return true;
    if (/^gm/i.test(lim) && lim.length >= 2) return true;
    var temL = /[a-z]/i.test(lim);
    var temN = /\d/.test(lim);
    return temL && temN && lim.length >= 3 && q.indexOf(' ') === -1;
  }

  function agendar(forcar) {
    if (!buscaEl) return;
    clearTimeout(debounceTimer);
    var q = (buscaEl.value || '').trim();
    if (!q) {
      pagina = 1;
      carregar();
      return;
    }
    if (!buscaProntaParaCatalogo(q)) {
      if (carregarAbort) {
        try { carregarAbort.abort(); } catch (e1) { /* ignore */ }
        carregarAbort = null;
      }
      carregarGen++;
      setLoading(false);
      mostrarErro('');
      if (metaEl) metaEl.textContent = 'Mín. 2 letras ou código (GM… / 4+ dígitos).';
      if (listaEl) {
        listaEl.innerHTML = '<tr><td colspan="8" class="p-6 text-center text-slate-500 font-semibold">Continue digitando para buscar no catálogo.</td></tr>';
      }
      return;
    }
    mostrarErro('');
    if (metaEl) metaEl.textContent = 'Buscando…';
    if (pareceCodigoBusca(q)) {
      clearTimeout(debounceTimer);
      pagina = 1;
      carregar();
      return;
    }
    var temCache = cadastroCatalogoPdvCacheArray().length > 0;
    var ms = forcar ? 0 : (temCache ? 100 : 320);
    debounceTimer = setTimeout(function () {
      var q2 = (buscaEl.value || '').trim();
      if (!q2) {
        pagina = 1;
        carregar();
        return;
      }
      if (!buscaProntaParaCatalogo(q2)) return;
      pagina = 1;
      carregar();
    }, ms);
  }

  if (buscaEl) {
    buscaEl.addEventListener('input', function () { agendar(false); });
    buscaEl.addEventListener('keydown', function (e) {
      if (e.key === 'Enter') {
        e.preventDefault();
        clearTimeout(debounceTimer);
        pagina = 1;
        agendar(true);
      }
    });
  }
  if (ativosEl) {
    ativosEl.addEventListener('change', function () {
      pagina = 1;
      carregar();
    });
  }
  if (filtrarEl) {
    filtrarEl.addEventListener('click', function () {
      pagina = 1;
      carregar();
    });
  }
  if (prevEl && nextEl) {
    prevEl.addEventListener('click', function () {
      if (pagina > 1) {
        pagina--;
        carregar();
      }
    });
    nextEl.addEventListener('click', function () {
      pagina++;
      carregar();
    });
  }

  var tblSort = document.getElementById('cadastro-tabela-produtos');
  if (tblSort) {
    var theadSort = tblSort.querySelector('thead');
    if (theadSort) {
      theadSort.addEventListener('click', function (e) {
        var th = e.target.closest('th[data-sort]');
        if (!th) return;
        e.preventDefault();
        ordenar(th.getAttribute('data-sort'));
      });
    }
  }

  document.addEventListener('keydown', function (e) {
    if (e.key === 'F2' && buscaEl) {
      e.preventDefault();
      buscaEl.focus();
      buscaEl.select();
    }
  });

  function erpSyncUmaRodada(limite) {
    return fetch(URL_ERP_SYNC_PENDENTES, {
      method: 'POST',
      credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrfTokErp() },
      body: JSON.stringify({ limite: limite != null ? limite : 80 })
    }).then(function (r) { return jsonOuErroHumano(r); }).then(function (j) {
      if (!j.ok) throw new Error(j.erro || 'Falha ao sincronizar');
      return j;
    });
  }

  function erpMontarMsgResumo(j, titulo) {
    var okc = j.ok_count != null ? j.ok_count : 0;
    var f = j.falhas != null ? j.falhas : 0;
    var rest = j.pendentes_restantes != null ? j.pendentes_restantes : '?';
    var msg = (titulo || 'ERP') + ': ' + okc + ' ok, ' + f + ' falha(s). Pendentes restantes: ' + rest;
    var res = j.resultados;
    if (f > 0 && res && res.length) {
      for (var ri = 0; ri < res.length; ri++) {
        var rr = res[ri];
        if (rr && rr.ok === false && rr.erp_resposta != null) {
          try {
            var det = typeof rr.erp_resposta === 'string' ? rr.erp_resposta : JSON.stringify(rr.erp_resposta);
            if (det && String(det).trim()) {
              if (det.length > 1400) det = det.slice(0, 1400) + '…';
              msg += '\n\n— Primeira falha (produto ' + String(rr.produto_id || '') + ') —\n' + det;
            }
          } catch (eDet) { /* ignore */ }
          break;
        }
      }
    }
    return msg;
  }

  if (btnErpPend && URL_ERP_SYNC_PENDENTES && PODE_EDITAR_OVERLAY && ERP_SYNC_HABILITADO) {
    btnErpPend.addEventListener('click', function () {
      if (!window.confirm('Enviar ao ERP legado um lote de até 80 produtos pendentes desta sessão?')) return;
      btnErpPend.disabled = true;
      if (btnErpForcarTodos) btnErpForcarTodos.disabled = true;
      erpSyncUmaRodada(80)
        .then(function (j) {
          window.alert(erpMontarMsgResumo(j, 'Lote ERP'));
          refreshPendentesBadge();
          carregar();
        })
        .catch(function (e) { window.alert(e.message || 'Erro ao sincronizar'); })
        .finally(function () {
          btnErpPend.disabled = false;
          if (btnErpForcarTodos) btnErpForcarTodos.disabled = false;
        });
    });
  }

  if (btnErpForcarTodos && URL_ERP_SYNC_PENDENTES && PODE_EDITAR_OVERLAY && ERP_SYNC_HABILITADO) {
    btnErpForcarTodos.addEventListener('click', function () {
      if (!window.confirm(
        'Enviar ao ERP legado todos os produtos pendentes desta sessão, em várias rodadas (até ~3200 por clique), até a fila esvaziar ou ocorrer falha. Continuar?'
      )) return;
      btnErpPend.disabled = true;
      btnErpForcarTodos.disabled = true;
      var totOk = 0;
      var totFal = 0;
      var rodadas = 0;
      var maxRodadas = 40;
      function passo() {
        return erpSyncUmaRodada(80).then(function (j) {
          rodadas += 1;
          totOk += j.ok_count != null ? j.ok_count : 0;
          totFal += j.falhas != null ? j.falhas : 0;
          var rest = j.pendentes_restantes != null ? j.pendentes_restantes : 0;
          btnErpForcarTodos.textContent = 'Enviando… ' + rodadas + 'ª rodada (' + rest + ' rest.)';
          if (totFal > 0) {
            window.alert(
              'Parado após ' + rodadas + ' rodada(s). Acumulado: ' + totOk + ' ok, ' + totFal + ' falha(s).\n\n' +
              erpMontarMsgResumo(j, 'Última rodada')
            );
            refreshPendentesBadge();
            carregar();
            return;
          }
          if (rest > 0 && rodadas < maxRodadas) return passo();
          if (rest > 0) {
            window.alert(
              'Limite de ' + maxRodadas + ' rodadas atingido. Ainda há ' + rest +
                ' pendente(s). Clique de novo em «Enviar todos ao ERP» ou use «Enviar lote».\n\nAcumulado nesta série: ' +
                totOk + ' ok, ' + totFal + ' falha(s).'
            );
          } else {
            window.alert(
              'Concluído em ' + rodadas + ' rodada(s). Total enviado com sucesso: ' + totOk + '. Fila desta sessão vazia.'
            );
          }
          refreshPendentesBadge();
          carregar();
        });
      }
      passo()
        .catch(function (e) { window.alert(e.message || 'Erro ao sincronizar'); })
        .finally(function () {
          btnErpPend.disabled = false;
          btnErpForcarTodos.disabled = false;
          btnErpForcarTodos.textContent = 'Enviar todos ao ERP (filas)';
        });
    });
  }

  if (CADASTRO_ERP_MODO === 'detalhe' && CADASTRO_ERP_PID) {
    carregarDetalheProduto(CADASTRO_ERP_PID);
  } else {
    carregar();
  }

  window.agroCadastroErpRecarregarLista = function () {
    try {
      if (CADASTRO_ERP_MODO !== 'detalhe') carregar();
    } catch (eRec) { /* ignore */ }
  };

  var btnNovoProd = document.getElementById('cadastro-btn-novo-produto');
  if (btnNovoProd) {
    btnNovoProd.addEventListener('click', function () {
      if (typeof window.abrirModalProduto === 'function') {
        window.abrirModalProduto({ id: '__novo__' });
      }
    });
  }

  var cadEtqProd = null;
  var cadEtqUiReady = false;

  function cadEtqEl(id) {
    return document.getElementById(id);
  }

  function ensureCadEtqModalOnBody() {
    var back = cadEtqEl('cad-etq-back');
    var modal = cadEtqEl('cad-etq-modal');
    if (back && back.parentElement !== document.body) document.body.appendChild(back);
    if (modal && modal.parentElement !== document.body) document.body.appendChild(modal);
  }

  function fecharModalEtiquetaCadastro() {
    cadEtqProd = null;
    var cadEtqBack = cadEtqEl('cad-etq-back');
    var cadEtqModal = cadEtqEl('cad-etq-modal');
    var cadEtqStatus = cadEtqEl('cad-etq-status');
    if (cadEtqBack) {
      cadEtqBack.classList.add('hidden');
      cadEtqBack.setAttribute('aria-hidden', 'true');
    }
    if (cadEtqModal) {
      cadEtqModal.classList.add('hidden');
      cadEtqModal.setAttribute('aria-hidden', 'true');
    }
    if (cadEtqStatus) cadEtqStatus.textContent = '';
    document.body.classList.remove('modal-open');
  }

  function abrirModalEtiquetaCadastro(p) {
    ensureCadEtqModalOnBody();
    var Core = window.AgroEtiquetasCore;
    if (!Core) {
      mostrarErro('Módulo de etiquetas indisponível.');
      return;
    }
    var cadEtqModal = cadEtqEl('cad-etq-modal');
    if (!cadEtqModal) {
      mostrarErro('Modal de etiqueta não encontrado. Recarregue a página (F5).');
      return;
    }
    cadEtqProd = p;
    var nomeEl = cadEtqEl('cad-etq-nome');
    var gmEl = cadEtqEl('cad-etq-gm');
    var cadEtqPreset = cadEtqEl('cad-etq-preset');
    var cadEtqQtd = cadEtqEl('cad-etq-qtd');
    var cadEtqBack = cadEtqEl('cad-etq-back');
    if (nomeEl) nomeEl.textContent = p.nome || '—';
    if (gmEl) {
      var gm = String(p.codigo_nfe || p.codigo_gm || p.codigo || '—');
      var pv = p.preco_venda != null && isFinite(Number(p.preco_venda))
        ? Number(p.preco_venda).toFixed(2).replace('.', ',')
        : '0,00';
      gmEl.textContent = gm + ' · R$ ' + pv;
    }
    var barrasEl = cadEtqEl('cad-etq-barras');
    if (barrasEl && Core.valorBarcodeProduto) {
      var bc = Core.valorBarcodeProduto(Core.produtoParaItem(p, 1));
      barrasEl.classList.remove('hidden', 'text-emerald-700', 'text-amber-900', 'bg-amber-50');
      if (bc.formato === 'EAN13' || bc.formato === 'EAN8') {
        var msg =
          'Barras na etiqueta: ' +
          bc.valor +
          ' (' +
          bc.formato +
          ') — leitor bipa o número, não o GM.';
        if (bc.ean_corrigido) {
          msg +=
            ' Aviso: dígito verificador ajustado automaticamente' +
            (bc.valor_original ? ' (cadastro tinha ' + bc.valor_original + ')' : '') +
            '. Confira o EAN real do produto.';
          barrasEl.classList.add('text-amber-900', 'bg-amber-50', 'rounded-lg', 'px-2', 'py-1.5');
        } else {
          barrasEl.classList.add('text-emerald-700');
        }
        barrasEl.textContent = msg;
      } else if (bc.codigo_loja || (Core.ehCodigoBarrasLojaInterno && Core.ehCodigoBarrasLojaInterno(bc.valor))) {
        barrasEl.textContent =
          'Barras interno loja: ' +
          bc.valor +
          ' (CODE128) — leitor bipa o número. Faixa 230… não é EAN de fábrica.';
        barrasEl.classList.add('text-emerald-700');
      } else {
        barrasEl.textContent =
          'Sem EAN no cadastro: a etiqueta sairá com código GM (' +
          bc.valor +
          '). Preencha «Código de barras» (aba Fiscal) e reimprima.';
        barrasEl.classList.add('text-amber-900', 'bg-amber-50', 'rounded-lg', 'px-2', 'py-1.5');
      }
    }
    if (cadEtqPreset) Core.fillPresetSelect(cadEtqPreset);
    if (cadEtqQtd) {
      cadEtqQtd.value = '1';
      setTimeout(function () {
        cadEtqQtd.focus();
        cadEtqQtd.select();
      }, 40);
    }
    if (cadEtqBack) {
      cadEtqBack.classList.remove('hidden');
      cadEtqBack.setAttribute('aria-hidden', 'false');
    }
    cadEtqModal.classList.remove('hidden');
    cadEtqModal.setAttribute('aria-hidden', 'false');
    document.body.classList.add('modal-open');
  }

  function imprimirEtiquetaCadastro() {
    var Core = window.AgroEtiquetasCore;
    var cadEtqQtd = cadEtqEl('cad-etq-qtd');
    var cadEtqPreset = cadEtqEl('cad-etq-preset');
    var cadEtqStatus = cadEtqEl('cad-etq-status');
    if (!cadEtqProd || !Core) return;
    var qtd = parseInt(cadEtqQtd && cadEtqQtd.value, 10) || 1;
    var presetId = cadEtqPreset && cadEtqPreset.value;
    var st = Core.loadStorage();
    if (presetId) {
      st.preset_ativo = presetId;
      Core.saveStorage(st);
    }
    var item = Core.produtoParaItem(cadEtqProd, qtd);
    if (cadEtqStatus) cadEtqStatus.textContent = 'Enviando…';
    Core.imprimirItens([item], {
      presetId: presetId || st.preset_ativo,
      textoRodape: st.texto_rodape_global || Core.getPresetAtivo(st).texto_rodape || '',
      origem: 'cadastro',
    }).then(function (res) {
      if (res && res.ok) {
        fecharModalEtiquetaCadastro();
        return;
      }
      if (cadEtqStatus) {
        cadEtqStatus.textContent = 'Falha: ' + (res && res.reason ? res.reason : 'erro');
      }
    });
  }

  function initCadEtqModalUi() {
    if (cadEtqUiReady) return;
    ensureCadEtqModalOnBody();
    var cadEtqBtnImp = cadEtqEl('cad-etq-imprimir');
    var cadEtqBtnCan = cadEtqEl('cad-etq-cancelar');
    var cadEtqBack = cadEtqEl('cad-etq-back');
    var cadEtqQtd = cadEtqEl('cad-etq-qtd');
    if (!cadEtqBtnImp && !cadEtqBack) return;
    cadEtqUiReady = true;
    if (cadEtqBtnImp) cadEtqBtnImp.addEventListener('click', imprimirEtiquetaCadastro);
    if (cadEtqBtnCan) cadEtqBtnCan.addEventListener('click', fecharModalEtiquetaCadastro);
    if (cadEtqBack) cadEtqBack.addEventListener('click', fecharModalEtiquetaCadastro);
    if (cadEtqQtd) {
      cadEtqQtd.addEventListener('keydown', function (e) {
        if (e.key === 'Enter') {
          e.preventDefault();
          imprimirEtiquetaCadastro();
        }
      });
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initCadEtqModalUi);
  } else {
    initCadEtqModalUi();
  }

  /** Excel export / import (fase 1) — só na lista. */
  (function initCadastroPlanilha() {
    if (CADASTRO_ERP_MODO === 'detalhe') return;
    var btnExp = document.getElementById('cadastro-btn-export-xlsx');
    var btnImp = document.getElementById('cadastro-btn-import-xlsx');
    var modal = document.getElementById('cadastro-import-modal');
    var back = document.getElementById('cadastro-import-back');
    if (!btnExp && !btnImp) return;

    function csrfTok() {
      return (U && U.csrf) ? U.csrf() : '';
    }

    function abrirImport() {
      if (!modal || !back) return;
      modal.classList.remove('hidden');
      back.classList.remove('hidden');
    }
    function fecharImport() {
      if (!modal || !back) return;
      modal.classList.add('hidden');
      back.classList.add('hidden');
    }

    if (btnExp && C.URL_EXPORT_XLSX) {
      var modalExp = document.getElementById('cadastro-export-modal');
      var backExp = document.getElementById('cadastro-export-back');
      var elCols = document.getElementById('cadastro-export-colunas');
      var elCats = document.getElementById('cadastro-export-categorias');
      var inpCatBusca = document.getElementById('cadastro-export-cat-busca');
      var elCatStatus = document.getElementById('cadastro-export-cat-status');
      var btnExpBaixar = document.getElementById('cadastro-export-baixar');
      var btnExpFec = document.getElementById('cadastro-export-fechar');
      var exportCatsCache = null;
      var exportColsDef = [
        { key: 'id', label: 'ID', fixa: true, bloqueada: true, oculta: true },
        { key: 'codigo_gm', label: 'Código GM' },
        { key: 'nome', label: 'Nome' },
        { key: 'marca', label: 'Marca' },
        { key: 'categoria', label: 'Categoria' },
        { key: 'subcategoria', label: 'Subcategoria' },
        { key: 'codigo_barras', label: 'Código barras' },
        { key: 'preco_custo', label: 'Preço custo' },
        { key: 'preco_venda', label: 'Preço venda' }
      ];

      function abrirExport() {
        if (!modalExp || !backExp) return;
        montarColunasExport();
        carregarCategoriasExport();
        modalExp.classList.remove('hidden');
        backExp.classList.remove('hidden');
      }
      function fecharExport() {
        if (!modalExp || !backExp) return;
        modalExp.classList.add('hidden');
        backExp.classList.add('hidden');
      }

      function montarColunasExport() {
        if (!elCols) return;
        elCols.innerHTML = exportColsDef.map(function (c) {
          var chk = c.fixa ? ' checked disabled' : ' checked';
          var hint = c.oculta
            ? ' <span class="text-[10px] font-black uppercase text-slate-400">(oculta no Excel)</span>'
            : (c.bloqueada ? ' <span class="text-[10px] font-black uppercase text-slate-400">(bloqueada no Excel)</span>' : '');
          return '<label class="flex items-center gap-2 min-h-[40px] px-2 rounded-lg hover:bg-slate-50">' +
            '<input type="checkbox" class="cadastro-export-col-cb w-5 h-5" data-key="' + escapeHtml(c.key) + '"' + chk + ' />' +
            '<span>' + escapeHtml(c.label) + hint + '</span></label>';
        }).join('');
      }

      function renderCategoriasExport(lista) {
        if (!elCats) return;
        var q = (inpCatBusca && inpCatBusca.value || '').trim().toLowerCase();
        var filtrada = (lista || []).filter(function (c) {
          return !q || String(c).toLowerCase().indexOf(q) >= 0;
        });
        if (!filtrada.length) {
          elCats.innerHTML = '<p class="text-slate-500 p-2">Nenhuma categoria' + (q ? ' com esse filtro' : '') + '.</p>';
        } else {
          elCats.innerHTML = filtrada.map(function (cat) {
            return '<label class="flex items-center gap-2 min-h-[40px] px-2 rounded-lg hover:bg-emerald-50/80">' +
              '<input type="checkbox" class="cadastro-export-cat-cb w-5 h-5" value="' + escapeHtml(String(cat)) + '" />' +
              '<span>' + escapeHtml(String(cat)) + '</span></label>';
          }).join('');
        }
        if (elCatStatus) {
          elCatStatus.textContent = filtrada.length + ' categoria(s) listada(s). Nenhuma marcada = todas.';
        }
      }

      function carregarCategoriasExport() {
        if (exportCatsCache) {
          renderCategoriasExport(exportCatsCache);
          return;
        }
        if (!C.URL_FACETAS) return;
        if (elCatStatus) elCatStatus.textContent = 'Carregando categorias…';
        fetch(C.URL_FACETAS, { credentials: 'same-origin', headers: { 'Accept': 'application/json' } })
          .then(function (r) { return r.json(); })
          .then(function (j) {
            exportCatsCache = (j && j.ok && j.categorias) ? j.categorias : [];
            renderCategoriasExport(exportCatsCache);
          })
          .catch(function () {
            if (elCatStatus) elCatStatus.textContent = 'Não foi possível carregar categorias — exportará todas.';
          });
      }

      if (inpCatBusca) {
        inpCatBusca.addEventListener('input', function () {
          renderCategoriasExport(exportCatsCache || []);
        });
      }

      btnExp.addEventListener('click', abrirExport);
      if (btnExpFec) btnExpFec.addEventListener('click', fecharExport);
      if (backExp) backExp.addEventListener('click', fecharExport);

      if (btnExpBaixar) {
        btnExpBaixar.addEventListener('click', function () {
          var cols = [];
          elCols.querySelectorAll('.cadastro-export-col-cb').forEach(function (cb) {
            if (cb.checked || cb.disabled) cols.push(cb.getAttribute('data-key'));
          });
          if (!cols.length) cols.push('id');
          var cats = [];
          if (elCats) {
            elCats.querySelectorAll('.cadastro-export-cat-cb:checked').forEach(function (cb) {
              if (cb.value) cats.push(cb.value);
            });
          }
          var params = new URLSearchParams();
          if (ativosEl && !ativosEl.checked) params.set('inativos', '1');
          params.set('cols', cols.join(','));
          if (cats.length) params.set('categorias', cats.join('|'));
          fecharExport();
          window.location.href = C.URL_EXPORT_XLSX + '?' + params.toString();
        });
      }
    }

    var inpArq = document.getElementById('cadastro-import-arquivo');
    var elResumo = document.getElementById('cadastro-import-resumo');
    var elErros = document.getElementById('cadastro-import-erros');
    var elPrev = document.getElementById('cadastro-import-preview');
    var btnPrev = document.getElementById('cadastro-import-previa');
    var btnApl = document.getElementById('cadastro-import-aplicar');
    var btnFec = document.getElementById('cadastro-import-fechar');
    var elProgWrap = document.getElementById('cadastro-import-progress-wrap');
    var elProgBar = document.getElementById('cadastro-import-progress-bar');
    var elProgPct = document.getElementById('cadastro-import-progress-pct');
    var elProgLabel = document.getElementById('cadastro-import-progress-label');
    var elProgDetail = document.getElementById('cadastro-import-progress-detail');
    var elProgTrack = elProgWrap ? elProgWrap.querySelector('[role="progressbar"]') : null;
    var pollTimer = null;
    var ultimaPreviaOk = false;

    function cancelPoll() {
      if (pollTimer) {
        clearTimeout(pollTimer);
        pollTimer = null;
      }
    }

    function setProgress(pct, label, detail) {
      var n = Math.max(0, Math.min(100, Math.round(pct || 0)));
      if (elProgWrap) elProgWrap.classList.remove('hidden');
      if (elProgBar) elProgBar.style.width = n + '%';
      if (elProgPct) elProgPct.textContent = n + '%';
      if (elProgLabel && label) elProgLabel.textContent = label;
      if (elProgDetail) elProgDetail.textContent = detail || '';
      if (elProgTrack) elProgTrack.setAttribute('aria-valuenow', String(n));
    }

    function hideProgress() {
      cancelPoll();
      if (elProgWrap) elProgWrap.classList.add('hidden');
      if (elProgBar) elProgBar.style.width = '0%';
      if (elProgPct) elProgPct.textContent = '0%';
      if (elProgDetail) elProgDetail.textContent = '';
      if (elProgTrack) elProgTrack.setAttribute('aria-valuenow', '0');
    }

    function limparImportUi() {
      cancelPoll();
      hideProgress();
      ultimaPreviaOk = false;
      if (btnApl) btnApl.disabled = true;
      if (elResumo) { elResumo.classList.add('hidden'); elResumo.textContent = ''; }
      if (elErros) { elErros.classList.add('hidden'); elErros.innerHTML = ''; }
      if (elPrev) { elPrev.classList.add('hidden'); elPrev.innerHTML = ''; }
    }

    function renderPrevia(j) {
      if (elResumo) {
        elResumo.className = 'text-sm font-semibold ' + (j.n_alteracoes > 0 ? 'text-emerald-800' : 'text-amber-800');
        if (j.n_alteracoes > 0) {
          elResumo.textContent = j.n_alteracoes + ' alteração(ões) · ' + j.n_ignoradas + ' ignorada(s) · ' + j.n_erros + ' erro(s) — pode confirmar abaixo.';
        } else {
          elResumo.textContent = 'Nenhuma alteração detectada (' + j.n_ignoradas + ' ignorada(s), ' + j.n_erros + ' erro(s)). Edite alguma célula (vazio não altera) ou clique «Ver prévia» de novo após corrigir.';
        }
        elResumo.classList.remove('hidden');
      }
      if (elErros && j.erros && j.erros.length) {
        elErros.innerHTML = j.erros.slice(0, 40).map(function (e) {
          return '<div>L' + (e.linha || '?') + ' · ' + escapeHtml(String(e.id || '')) + ': ' + escapeHtml(String(e.erro || '')) + '</div>';
        }).join('');
        elErros.classList.remove('hidden');
      }
      if (elPrev && j.alteracoes && j.alteracoes.length) {
        var html = '<table class="w-full text-left"><thead class="bg-slate-50"><tr><th class="p-2">Linha</th><th class="p-2">Nome</th><th class="p-2">Campos</th></tr></thead><tbody>';
        j.alteracoes.slice(0, 80).forEach(function (a) {
          var campos = (a.campos || []).map(function (c) {
            return escapeHtml(String(c.campo || '')) + ': ' + escapeHtml(String(c.de)) + ' → ' + escapeHtml(String(c.para));
          }).join('; ');
          html += '<tr class="border-t border-slate-100"><td class="p-2 font-mono">' + a.linha + '</td><td class="p-2">' + escapeHtml(String(a.nome || '')) + '</td><td class="p-2 text-slate-600">' + campos + '</td></tr>';
        });
        html += '</tbody></table>';
        elPrev.innerHTML = html;
        elPrev.classList.remove('hidden');
      }
      ultimaPreviaOk = j.n_alteracoes > 0 && (!j.n_erros || j.n_alteracoes > 0);
      if (btnApl) btnApl.disabled = !(j.n_alteracoes > 0);
    }

    function parseHttpJson(r) {
      return r.text().then(function (text) {
        var j = null;
        try {
          j = text ? JSON.parse(text) : null;
        } catch (e) {
          var msg = 'Resposta inválida do servidor';
          if (r.status === 502 || r.status === 504) {
            msg = 'Servidor demorou demais — aguarde ou tente de novo em instantes.';
          } else if (r.status === 404) {
            msg = 'Rota de prévia não encontrada — atualize a página (Ctrl+F5).';
          } else if (r.status === 403 || r.status === 401) {
            msg = 'Sessão expirada — faça login de novo.';
          } else if (text && text.indexOf('<html') >= 0) {
            msg = 'Erro no servidor (HTTP ' + r.status + '). Tente F5 ou login de novo.';
          }
          throw new Error(msg);
        }
        return { ok: r.ok, j: j };
      });
    }

    function parseXhrJson(xhr) {
      var text = xhr.responseText || '';
      try {
        return text ? JSON.parse(text) : null;
      } catch (e) {
        if (xhr.status === 502 || xhr.status === 504) {
          throw new Error('Servidor demorou demais — a planilha grande leva mais tempo. Aguarde o deploy ou edite só as linhas alteradas.');
        }
        if (xhr.status === 403 || xhr.status === 401) {
          throw new Error('Sessão expirada — faça login de novo.');
        }
        throw new Error('Resposta inválida do servidor (HTTP ' + xhr.status + '). Atualize a página (Ctrl+F5).');
      }
    }

    function pollImportJob(jobId, onOk, onFail) {
      if (!C.URL_IMPORT_PREVIEW_STATUS) {
        if (typeof onFail === 'function') onFail(new Error('Importação indisponível — atualize a página (Ctrl+F5).'));
        return;
      }
      fetch(C.URL_IMPORT_PREVIEW_STATUS + '?job=' + encodeURIComponent(jobId), {
        credentials: 'same-origin',
        headers: { 'Accept': 'application/json' }
      }).then(parseHttpJson).then(function (x) {
        var j = x.j || {};
        if (!x.ok || !j.ok) throw new Error((j && j.erro) || 'Falha na importação');
        if (j.done) {
          setProgress(100, 'Concluído', '');
          setTimeout(hideProgress, 400);
          if (typeof onOk === 'function') onOk(j.result || j);
          return;
        }
        var srvPct = j.pct || 0;
        var uiPct = 15 + Math.round(srvPct * 0.82);
        var det = '';
        if (j.total_linhas) det = j.total_linhas + ' linha(s) na planilha';
        if (j.phase) det = j.phase;
        setProgress(uiPct, j.phase || 'Gravando alterações…', det);
        pollTimer = setTimeout(function () {
          pollImportJob(jobId, onOk, onFail);
        }, 400);
      }).catch(function (e) {
        hideProgress();
        if (typeof onFail === 'function') onFail(e);
      });
    }

    function pollPreviaStatus(jobId, totalLinhas, onOk, onFail) {
      if (!C.URL_IMPORT_PREVIEW_STATUS) {
        if (typeof onFail === 'function') onFail(new Error('Prévia indisponível — atualize a página (Ctrl+F5).'));
        return;
      }
      fetch(C.URL_IMPORT_PREVIEW_STATUS + '?job=' + encodeURIComponent(jobId), {
        credentials: 'same-origin',
        headers: { 'Accept': 'application/json' }
      }).then(parseHttpJson).then(function (x) {
        var j = x.j || {};
        if (!x.ok || !j.ok) throw new Error((j && j.erro) || 'Falha na prévia');
        if (j.done) {
          setProgress(100, 'Concluído', '');
          setTimeout(hideProgress, 400);
          if (typeof onOk === 'function') onOk(j);
          return;
        }
        var srvPct = j.pct || 0;
        var uiPct = 30 + Math.round(srvPct * 0.68);
        var det = '';
        if (j.total_linhas) det = j.total_linhas + ' linha(s) na planilha';
        if (j.phase && j.phase.indexOf('linha') >= 0) det = j.phase;
        setProgress(uiPct, j.phase || 'Analisando planilha…', det);
        pollTimer = setTimeout(function () {
          pollPreviaStatus(jobId, totalLinhas, onOk, onFail);
        }, 350);
      }).catch(function (e) {
        hideProgress();
        if (typeof onFail === 'function') onFail(e);
      });
    }

    function enviarPlanilhaPreview(onOk, onFail) {
      if (!inpArq || !inpArq.files || !inpArq.files[0]) {
        var msg = 'Selecione um arquivo .xlsx ou .csv.';
        if (typeof alert !== 'undefined') alert(msg);
        if (typeof onFail === 'function') onFail(new Error(msg));
        return;
      }
      if (!C.URL_IMPORT_PREVIEW) {
        if (typeof onFail === 'function') onFail(new Error('Prévia indisponível.'));
        return;
      }
      cancelPoll();
      var fd = new FormData();
      var file = inpArq.files[0];
      fd.append('arquivo', file);
      if (btnPrev) btnPrev.disabled = true;
      if (btnApl) btnApl.disabled = true;
      setLoading(true);
      setProgress(0, 'Enviando arquivo…', file.name || '');

      var xhr = new XMLHttpRequest();
      xhr.open('POST', C.URL_IMPORT_PREVIEW, true);
      xhr.setRequestHeader('X-CSRFToken', csrfTok());
      xhr.setRequestHeader('Accept', 'application/json');

      xhr.upload.onprogress = function (ev) {
        if (!ev.lengthComputable) return;
        var upPct = Math.round((ev.loaded / ev.total) * 28);
        var mb = (ev.loaded / 1048576).toFixed(1);
        var mbTot = (ev.total / 1048576).toFixed(1);
        setProgress(upPct, 'Enviando arquivo…', mb + ' / ' + mbTot + ' MB');
      };

      xhr.onload = function () {
        var j = null;
        try {
          j = parseXhrJson(xhr);
        } catch (e) {
          hideProgress();
          setLoading(false);
          if (btnPrev) btnPrev.disabled = false;
          if (typeof onFail === 'function') onFail(e);
          else if (typeof alert !== 'undefined') alert(e.message);
          return;
        }
        if (xhr.status < 200 || xhr.status >= 300 || !j || !j.ok) {
          var err = (j && j.erro) || 'Falha ao enviar planilha';
          hideProgress();
          setLoading(false);
          if (btnPrev) btnPrev.disabled = false;
          if (typeof onFail === 'function') onFail(new Error(err));
          else if (typeof alert !== 'undefined') alert(err);
          return;
        }
        if (j.job_id) {
          setProgress(30, 'Analisando planilha…', 'Aguarde…');
          pollPreviaStatus(j.job_id, 0, function (result) {
            setLoading(false);
            if (btnPrev) btnPrev.disabled = false;
            if (typeof onOk === 'function') onOk(result);
          }, function (e) {
            setLoading(false);
            if (btnPrev) btnPrev.disabled = false;
            if (typeof alert !== 'undefined') alert(e.message || 'Erro');
            if (typeof onFail === 'function') onFail(e);
          });
          return;
        }
        setProgress(100, 'Concluído', '');
        setTimeout(hideProgress, 400);
        setLoading(false);
        if (btnPrev) btnPrev.disabled = false;
        if (typeof onOk === 'function') onOk(j);
      };

      xhr.onerror = function () {
        hideProgress();
        setLoading(false);
        if (btnPrev) btnPrev.disabled = false;
        var err = new Error('Falha de rede ao enviar planilha.');
        if (typeof onFail === 'function') onFail(err);
        else if (typeof alert !== 'undefined') alert(err.message);
      };

      xhr.send(fd);
    }

    function enviarPlanilhaAplicar(onOk, onFail) {
      if (!inpArq || !inpArq.files || !inpArq.files[0]) {
        var msg = 'Selecione um arquivo .xlsx ou .csv.';
        if (typeof alert !== 'undefined') alert(msg);
        if (typeof onFail === 'function') onFail(new Error(msg));
        return;
      }
      if (!C.URL_IMPORT_APLICAR) {
        if (typeof onFail === 'function') onFail(new Error('Gravação indisponível.'));
        return;
      }
      cancelPoll();
      var fd = new FormData();
      var file = inpArq.files[0];
      fd.append('arquivo', file);
      fd.append('nome_arquivo', file.name || '');
      if (btnPrev) btnPrev.disabled = true;
      if (btnApl) btnApl.disabled = true;
      setLoading(true);
      setProgress(0, 'Enviando planilha…', file.name || '');

      var xhr = new XMLHttpRequest();
      xhr.open('POST', C.URL_IMPORT_APLICAR, true);
      xhr.setRequestHeader('X-CSRFToken', csrfTok());
      xhr.setRequestHeader('Accept', 'application/json');

      xhr.upload.onprogress = function (ev) {
        if (!ev.lengthComputable) return;
        var upPct = Math.round((ev.loaded / ev.total) * 12);
        setProgress(upPct, 'Enviando planilha…', file.name || '');
      };

      xhr.onload = function () {
        var j = null;
        try {
          j = parseXhrJson(xhr);
        } catch (e) {
          hideProgress();
          setLoading(false);
          if (btnPrev) btnPrev.disabled = false;
          if (btnApl) btnApl.disabled = !ultimaPreviaOk;
          if (typeof onFail === 'function') onFail(e);
          return;
        }
        if (xhr.status < 200 || xhr.status >= 300 || !j || !j.ok) {
          hideProgress();
          setLoading(false);
          if (btnPrev) btnPrev.disabled = false;
          if (btnApl) btnApl.disabled = !ultimaPreviaOk;
          if (typeof onFail === 'function') onFail(new Error((j && j.erro) || 'Falha ao gravar'));
          return;
        }
        if (j.job_id) {
          setProgress(15, 'Gravando alterações…', 'Aguarde…');
          pollImportJob(j.job_id, function (result) {
            setLoading(false);
            if (btnPrev) btnPrev.disabled = false;
            if (typeof onOk === 'function') onOk(result);
          }, function (e) {
            setLoading(false);
            if (btnPrev) btnPrev.disabled = false;
            if (btnApl) btnApl.disabled = !ultimaPreviaOk;
            if (typeof onFail === 'function') onFail(e);
          });
          return;
        }
        setProgress(100, 'Concluído', '');
        setTimeout(hideProgress, 400);
        setLoading(false);
        if (btnPrev) btnPrev.disabled = false;
        if (typeof onOk === 'function') onOk(j);
      };

      xhr.onerror = function () {
        hideProgress();
        setLoading(false);
        if (btnPrev) btnPrev.disabled = false;
        if (btnApl) btnApl.disabled = !ultimaPreviaOk;
        if (typeof onFail === 'function') {
          onFail(new Error('Conexão caiu ao gravar — apague linhas não editadas e tente de novo.'));
        }
      };

      xhr.send(fd);
    }

    function enviarPlanilha(url, onOk) {
      if (!inpArq || !inpArq.files || !inpArq.files[0]) {
        if (typeof alert !== 'undefined') alert('Selecione um arquivo .xlsx ou .csv.');
        return;
      }
      if (url === C.URL_IMPORT_PREVIEW) {
        enviarPlanilhaPreview(onOk, function (e) {
          if (typeof alert !== 'undefined') alert(e.message || 'Erro');
        });
        return;
      }
      if (url === C.URL_IMPORT_APLICAR) {
        enviarPlanilhaAplicar(onOk, function (e) {
          var msg = e.message || 'Erro';
          if (msg === 'Failed to fetch') {
            msg = 'Servidor demorou demais — apague linhas não editadas ou aguarde a barra de progresso.';
          }
          if (typeof alert !== 'undefined') alert(msg);
        });
        return;
      }
    }

    if (btnImp) btnImp.addEventListener('click', function () {
      limparImportUi();
      abrirImport();
    });
    if (btnFec) btnFec.addEventListener('click', fecharImport);
    if (back) back.addEventListener('click', fecharImport);
    function rodarPrevia() {
      if (!inpArq || !inpArq.files || !inpArq.files[0]) {
        if (typeof alert !== 'undefined') alert('Selecione um arquivo .xlsx ou .csv.');
        return;
      }
      enviarPlanilha(C.URL_IMPORT_PREVIEW, renderPrevia);
    }

    if (inpArq) {
      inpArq.addEventListener('change', function () {
        limparImportUi();
        if (inpArq.files && inpArq.files[0]) rodarPrevia();
      });
    }
    if (btnPrev && C.URL_IMPORT_PREVIEW) {
      btnPrev.addEventListener('click', function () {
        limparImportUi();
        rodarPrevia();
      });
    }
    if (btnApl && C.URL_IMPORT_APLICAR) {
      btnApl.addEventListener('click', function () {
        function confirmarGravacao() {
          if (!window.confirm('Gravar alterações da planilha no SisVale?')) return;
          enviarPlanilha(C.URL_IMPORT_APLICAR, function (j) {
            fecharImport();
            if (typeof alert !== 'undefined') {
              var msg = 'Importação concluída: ' + (j.gravados || 0) + ' produto(s) atualizado(s).';
              if (j.historico_id) msg += ' Backup salvo — use «Histórico» para desfazer se precisar.';
              alert(msg);
            }
            if (typeof carregar === 'function') carregar();
          });
        }
        if (ultimaPreviaOk) {
          confirmarGravacao();
          return;
        }
        enviarPlanilha(C.URL_IMPORT_PREVIEW, function (j) {
          renderPrevia(j);
          if (j.n_alteracoes > 0) confirmarGravacao();
        });
      });
    }

    var btnHist = document.getElementById('cadastro-btn-import-historico');
    var modalHist = document.getElementById('cadastro-hist-modal');
    var backHist = document.getElementById('cadastro-hist-back');
    var elHistLista = document.getElementById('cadastro-hist-lista');
    var btnHistFec = document.getElementById('cadastro-hist-fechar');
    var rotuloCampoImport = {
      nome: 'Nome',
      marca: 'Marca',
      categoria: 'Categoria',
      subcategoria: 'Subcategoria',
      codigo_barras: 'Código barras',
      preco_custo: 'Preço custo',
      preco_venda: 'Preço venda'
    };

    function fmtDataIso(iso) {
      if (!iso) return '—';
      try {
        var d = new Date(iso);
        if (isNaN(d.getTime())) return iso;
        return d.toLocaleString('pt-BR', { day: '2-digit', month: '2-digit', year: 'numeric', hour: '2-digit', minute: '2-digit' });
      } catch (e) { return iso; }
    }

    function abrirHistorico() {
      if (!modalHist || !backHist) return;
      modalHist.classList.remove('hidden');
      backHist.classList.remove('hidden');
      carregarHistoricoImport();
    }
    function fecharHistorico() {
      if (!modalHist || !backHist) return;
      modalHist.classList.add('hidden');
      backHist.classList.add('hidden');
    }

    function carregarHistoricoImport() {
      if (!elHistLista || !C.URL_IMPORT_HISTORICO) return;
      elHistLista.innerHTML = '<p class="text-slate-500 font-semibold">Carregando…</p>';
      fetch(C.URL_IMPORT_HISTORICO, { credentials: 'same-origin', headers: { 'Accept': 'application/json' } })
        .then(parseHttpJson)
        .then(function (x) {
          var lista = (x.j && x.j.historico) ? x.j.historico : [];
          renderHistoricoImport(lista);
        })
        .catch(function () {
          elHistLista.innerHTML = '<p class="text-red-700 font-semibold">Não foi possível carregar o histórico.</p>';
        });
    }

    function renderHistoricoImport(lista) {
      if (!elHistLista) return;
      if (!lista || !lista.length) {
        elHistLista.innerHTML = '<p class="text-slate-600 font-semibold">Nenhuma importação registrada ainda.</p>';
        return;
      }
      elHistLista.innerHTML = lista.map(function (h) {
        var statusCls = h.status === 'revertido' ? 'text-slate-500' : 'text-emerald-800';
        var statusTxt = h.status === 'revertido' ? 'Desfeita' : 'Aplicada';
        var arq = h.nome_arquivo ? (' · ' + escapeHtml(h.nome_arquivo)) : '';
        var resumoHtml = '';
        (h.resumo || []).slice(0, 5).forEach(function (r) {
          var linhas = (r.detalhes || []).map(function (d) {
            var rot = rotuloCampoImport[d.campo] || d.campo;
            return escapeHtml(rot) + ': ' + escapeHtml(String(d.de)) + ' → ' + escapeHtml(String(d.para));
          }).join('; ');
          resumoHtml += '<div class="text-xs text-slate-600 mt-1 pl-2 border-l-2 border-slate-200">' +
            escapeHtml(String(r.nome || r.id || '')) + (linhas ? ' — ' + linhas : '') + '</div>';
        });
        var btnRev = h.pode_reverter
          ? '<button type="button" class="cadastro-hist-reverter min-h-[44px] px-4 rounded-xl bg-amber-600 text-white font-black uppercase text-xs border-2 border-amber-800" data-id="' + h.id + '">Desfazer</button>'
          : '<span class="text-xs font-semibold text-slate-400">Desfeita em ' + escapeHtml(fmtDataIso(h.revertido_em)) + '</span>';
        return '<article class="rounded-xl border-2 border-slate-200 p-4 bg-white">' +
          '<div class="flex flex-wrap items-start justify-between gap-2">' +
          '<div><p class="font-black text-slate-900">' + escapeHtml(fmtDataIso(h.criado_em)) + arq + '</p>' +
          '<p class="text-xs font-semibold ' + statusCls + '">' + statusTxt + ' · ' + (h.n_produtos || 0) + ' produto(s) · ' + (h.usuario || '—') + '</p></div>' +
          btnRev + '</div>' + resumoHtml + '</article>';
      }).join('');

      elHistLista.querySelectorAll('.cadastro-hist-reverter').forEach(function (btn) {
        btn.addEventListener('click', function () {
          var hid = btn.getAttribute('data-id');
          if (!hid || !C.URL_IMPORT_REVERTER) return;
          if (!window.confirm('Desfazer esta importação? Os produtos voltam aos valores anteriores.')) return;
          btn.disabled = true;
          fetch(C.URL_IMPORT_REVERTER, {
            method: 'POST',
            credentials: 'same-origin',
            headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrfTok(), 'Accept': 'application/json' },
            body: JSON.stringify({ historico_id: parseInt(hid, 10) })
          }).then(parseHttpJson).then(function (x) {
            if (!x.j || !x.j.ok) throw new Error((x.j && x.j.erro) || 'Falha ao desfazer');
            if (typeof alert !== 'undefined') {
              alert('Importação desfeita: ' + (x.j.revertidos || 0) + ' produto(s) restaurado(s).');
            }
            if (typeof carregar === 'function') carregar();
            carregarHistoricoImport();
          }).catch(function (e) {
            btn.disabled = false;
            if (typeof alert !== 'undefined') alert(e.message || 'Erro ao desfazer');
          });
        });
      });
    }

    if (btnHist) btnHist.addEventListener('click', abrirHistorico);
    if (btnHistFec) btnHistFec.addEventListener('click', fecharHistorico);
    if (backHist) backHist.addEventListener('click', fecharHistorico);
  })();
})();