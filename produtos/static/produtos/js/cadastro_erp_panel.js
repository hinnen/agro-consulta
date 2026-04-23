(function () {
  'use strict';
  var U = window.AgroCadastroErpUtil;
  if (!U) return;
  var escapeHtml = U.escapeHtml;
  var fmtMoney = U.fmtMoney;
  var setLoading = U.setLoading;
  var C = window.AgroCadastroErpLista || {};
  var CADASTRO_ERP_MODO = C.CADASTRO_ERP_MODO || 'lista';
  var CADASTRO_ERP_PID = C.CADASTRO_ERP_PID || '';
  var API = C.API || '';
  var API_DETALHE_TMPL = C.API_DETALHE_TMPL || '';
  var URL_CAD_ERP_PROD_TMPL = C.URL_CAD_ERP_PROD_TMPL || '';
  var URL_OVERLAY_SALVAR = C.URL_OVERLAY_SALVAR || '';
  var PODE_EDITAR_OVERLAY = !!C.PODE_EDITAR_OVERLAY;
  var LOGIN_OVERLAY_HREF = C.LOGIN_OVERLAY_HREF || '';
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
        '<p class="mt-2 text-sm leading-snug">O espelho ERP precisa do Mongo configurado no servidor (<code class="text-xs bg-white/80 px-1 rounded">VENDA_ERP_MONGO_URL</code> e <code class="text-xs bg-white/80 px-1 rounded">VENDA_ERP_MONGO_DB</code> no arquivo <code class="text-xs">.env</code>). Copie os mesmos valores do ambiente onde o PDV já funciona.</p>' +
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
      params.set('limit', '64');
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
    var av = '';
    if (p.ativo_exibicao === true) av = '1';
    else if (p.ativo_exibicao === false) av = '0';
    var ic = 'w-full min-h-[44px] px-3 rounded-xl border-2 border-emerald-400 text-base font-bold text-slate-900 bg-white';
    return (
      '<div class="mt-6 rounded-2xl border-2 border-emerald-600 bg-emerald-50/90 p-4 sm:p-5 shadow-sm">' +
      '<h4 class="text-sm font-black uppercase text-emerald-950 tracking-wide mb-1">Editar só no Agro</h4>' +
      '<p class="text-xs text-slate-700 mb-4 leading-snug">Grava no Agro (PDV e buscas). <strong>Não altera o ERP.</strong> Campo vazio + salvar remove o override daquele texto; em «Exibir como», «Seguir ERP» remove o forçamento de ativo/inativo.</p>' +
      '<div class="grid gap-3 sm:grid-cols-2">' +
      '<label class="block sm:col-span-2"><span class="text-[10px] font-black uppercase text-slate-600">Nome</span>' +
      '<input type="text" id="cad-ov-nome" class="' + ic + '" maxlength="300" value="' + escapeHtml(p.nome || '') + '" autocomplete="off" /></label>' +
      '<label class="block"><span class="text-[10px] font-black uppercase text-slate-600">Marca</span>' +
      '<input type="text" id="cad-ov-marca" class="' + ic + '" maxlength="120" value="' + escapeHtml(p.marca || '') + '" autocomplete="off" /></label>' +
      '<label class="block"><span class="text-[10px] font-black uppercase text-slate-600">Categoria</span>' +
      '<input type="text" id="cad-ov-cat" class="' + ic + '" maxlength="200" value="' + escapeHtml(p.categoria || '') + '" autocomplete="off" /></label>' +
      '<label class="block sm:col-span-2"><span class="text-[10px] font-black uppercase text-slate-600">Fornecedor (texto)</span>' +
      '<input type="text" id="cad-ov-forn" class="' + ic + '" maxlength="300" value="' + escapeHtml(p.fornecedor || '') + '" autocomplete="off" /></label>' +
      '<label class="block"><span class="text-[10px] font-black uppercase text-slate-600">Código NFe / GM</span>' +
      '<input type="text" id="cad-ov-codnfe" class="' + ic + ' font-mono text-sm" maxlength="64" value="' + escapeHtml(String(p.codigo_nfe || p.codigo || '')) + '" autocomplete="off" /></label>' +
      '<label class="block"><span class="text-[10px] font-black uppercase text-slate-600">Código de barras</span>' +
      '<input type="text" id="cad-ov-cb" class="' + ic + ' font-mono text-sm" maxlength="80" value="' + escapeHtml(String(p.codigo_barras || '')) + '" inputmode="numeric" autocomplete="off" /></label>' +
      '<label class="block"><span class="text-[10px] font-black uppercase text-slate-600">Subcategoria</span>' +
      '<input type="text" id="cad-ov-sub" class="' + ic + '" maxlength="200" value="' + escapeHtml(p.subcategoria || '') + '" autocomplete="off" /></label>' +
      '<label class="block"><span class="text-[10px] font-black uppercase text-slate-600">Unidade</span>' +
      '<input type="text" id="cad-ov-un" class="' + ic + '" maxlength="20" value="' + escapeHtml(p.unidade || '') + '" autocomplete="off" /></label>' +
      '<label class="block"><span class="text-[10px] font-black uppercase text-slate-600">Preço venda (R$)</span>' +
      '<input type="text" id="cad-ov-preco" inputmode="decimal" class="' + ic + '" value="' + escapeHtml(pv) + '" autocomplete="off" /></label>' +
      '<label class="block"><span class="text-[10px] font-black uppercase text-slate-600">Exibir como</span>' +
      '<select id="cad-ov-ativo" class="' + ic + '">' +
      '<option value=""' + (av === '' ? ' selected' : '') + '>Seguir ERP</option>' +
      '<option value="1"' + (av === '1' ? ' selected' : '') + '>Ativo</option>' +
      '<option value="0"' + (av === '0' ? ' selected' : '') + '>Inativo</option>' +
      '</select></label>' +
      '</div>' +
      '<label class="block mt-3"><span class="text-[10px] font-black uppercase text-slate-600">Descrição</span>' +
      '<textarea id="cad-ov-desc" rows="3" class="w-full rounded-xl border-2 border-emerald-400 px-3 py-2 text-sm font-semibold text-slate-900 bg-white">' + escapeHtml(p.descricao || '') + '</textarea></label>' +
      '<div class="mt-4">' +
      '<button type="button" id="cadastro-overlay-salvar" class="min-h-[48px] px-6 rounded-xl bg-orange-500 text-white font-black uppercase text-sm border-2 border-orange-600 hover:bg-orange-600 shadow-sm">Salvar no Agro</button>' +
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
      '<p class="text-xs text-slate-600 mt-2 leading-snug">Entre com seu usuário do sistema para ver o formulário aqui. O cadastro mestre continua no ERP.</p>' +
      '<a href="' + String(LOGIN_OVERLAY_HREF || '').replace(/"/g, '&quot;') + '" class="mt-3 inline-flex min-h-[44px] items-center px-4 rounded-xl bg-emerald-600 text-white font-black uppercase text-xs border-2 border-emerald-800 hover:bg-emerald-700">Entrar para editar</a>' +
      '</div>'
    );
  }

  function bindCadastroOverlaySalvar(p) {
    if (!PODE_EDITAR_OVERLAY) return;
    var btn = document.getElementById('cadastro-overlay-salvar');
    if (!btn) return;
    btn.onclick = function () {
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
      showMsg('');
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
        descricao: gv('cad-ov-desc'),
        preco_venda: gv('cad-ov-preco')
      };
      var av = gv('cad-ov-ativo');
      if (av === '') body.ativo_exibicao = null;
      else body.ativo_exibicao = av === '1';
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
        return r.json();
      }).then(function (j) {
        if (!j.ok) throw new Error(j.erro || 'Falha ao salvar');
        showMsg('Salvo no Agro.', true);
        carregarDetalheProduto(String(p.id || ''));
      }).catch(function (e) {
        showMsg(e.message || 'Erro ao salvar', false);
      });
    };
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
      dlRow('Subcategoria', p.subcategoria) +
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
        '<td data-coluna="subcategoria" class="px-4 py-3 text-slate-600">' + escapeHtml(p.subcategoria || '-') + '</td>' +
        '<td data-coluna="preco_custo" class="px-4 py-3 text-slate-600 whitespace-nowrap">' + custoTxt + '</td>' +
        '<td class="px-4 py-3 font-semibold text-emerald-600 whitespace-nowrap">' + vendaTxt + '</td>' +
        '<td class="px-4 py-3 text-right cadastro-acoes">' +
        '<span class="inline-flex items-center justify-end gap-2 text-lg">' +
        '<button type="button" class="cadastro-btn-edit-modal inline-flex h-9 min-w-[2.25rem] items-center justify-center rounded-lg border border-slate-200 bg-white hover:bg-slate-50 cursor-pointer" title="Editar (modal)">✏️</button>' +
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

  function carregar() {
    if (!listaEl) return;
    var g = ++carregarGen;
    if (carregarAbort) {
      try {
        carregarAbort.abort();
      } catch (e) { /* ignore */ }
    }
    carregarAbort = typeof AbortController !== 'undefined' ? new AbortController() : null;
    mostrarErro('');
    setLoading(true);
    var sig = carregarAbort ? carregarAbort.signal : undefined;
    fetch(urlFetch(), { credentials: 'same-origin', signal: sig })
      .then(function (r) { return r.json().then(function (j) { return { ok: r.ok, j: j }; }); })
      .then(function (x) {
        if (g !== carregarGen) return;
        if (!x.j || !x.j.ok) {
          throw new Error((x.j && x.j.erro) || 'Falha ao carregar');
        }
        var produtos = x.j.produtos || [];
        atualizarMeta(x.j, produtos);
        renderLista(produtos);
      })
      .catch(function (err) {
        if (err && err.name === 'AbortError') return;
        if (g !== carregarGen) return;
        var m = err.message || 'Erro de rede';
        var mongo = /mongo/i.test(m);
        if (mongo) {
          mostrarErro(m, { hintMongo: true });
          if (listaEl) {
            listaEl.innerHTML = '<tr><td colspan="8" class="p-8 text-center text-slate-600 font-semibold leading-relaxed">Lista do ERP indisponível sem Mongo. Use o botão acima para ir à aba <strong>Grupos</strong> ou configure o <code class="text-xs bg-slate-100 px-1 rounded">.env</code>.</td></tr>';
          }
        } else {
          mostrarErro(m);
          if (listaEl) listaEl.innerHTML = '';
        }
        if (metaEl) metaEl.textContent = '—';
      })
      .finally(function () {
        if (g === carregarGen) setLoading(false);
      });
  }

  function pareceCodigoBusca(q) {
    q = String(q || '').trim();
    var lim = q.replace(/\W/g, '');
    if (!lim) return false;
    if (/^\d+$/.test(lim) && lim.length >= 6) return true;
    if (/^gm/i.test(lim) && lim.length >= 3) return true;
    var temL = /[a-z]/i.test(lim);
    var temN = /\d/.test(lim);
    return temL && temN && lim.length >= 3 && q.indexOf(' ') === -1;
  }

  function agendar() {
    if (!buscaEl) return;
    clearTimeout(debounceTimer);
    var q = (buscaEl.value || '').trim();
    if (q.length === 1 && !pareceCodigoBusca(q)) {
      if (carregarAbort) {
        try { carregarAbort.abort(); } catch (e1) { /* ignore */ }
        carregarAbort = null;
      }
      carregarGen++;
      setLoading(false);
      mostrarErro('');
      if (metaEl) metaEl.textContent = 'Mín. 2 letras ou código (6+ dígitos / GM…).';
      if (listaEl) {
        listaEl.innerHTML = '<tr><td colspan="8" class="p-6 text-center text-slate-500 font-semibold">Continue digitando para buscar no espelho ERP.</td></tr>';
      }
      return;
    }
    var ms = pareceCodigoBusca(q) ? 100 : 320;
    debounceTimer = setTimeout(function () {
      var q2 = (buscaEl.value || '').trim();
      if (q2.length === 1 && !pareceCodigoBusca(q2)) return;
      pagina = 1;
      carregar();
    }, ms);
  }

  if (buscaEl) {
    buscaEl.addEventListener('input', agendar);
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

  if (CADASTRO_ERP_MODO === 'detalhe' && CADASTRO_ERP_PID) {
    carregarDetalheProduto(CADASTRO_ERP_PID);
  } else {
    carregar();
  }
})();