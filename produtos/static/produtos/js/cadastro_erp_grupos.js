(function () {
  'use strict';
  var U = window.AgroCadastroErpUtil;
  if (!U) return;
  var escapeHtml = U.escapeHtml;
  var fmtMoney = U.fmtMoney;
  var setLoading = U.setLoading;
  var G = window.AgroCadastroErpGrupos || {};
  var URL_GRUPOS = G.URL_GRUPOS || '';
  var URL_SALVAR = G.URL_SALVAR || '';
  function urlGrupo(pk) {
    return (G.URL_GRUPO_OBTER_TMPL || '').replace('999999', String(pk));
  }
  function urlExcluir(pk) {
    return (G.URL_GRUPO_EXCLUIR_TMPL || '').replace('999999', String(pk));
  }
  var API_ERP = G.API_ERP || '';

  var tabErp = document.getElementById('tab-erp');
  var tabGrupos = document.getElementById('tab-grupos');
  var panelErp = document.getElementById('panel-erp');
  var panelGrupos = document.getElementById('panel-grupos');
  var gruposBusca = document.getElementById('grupos-busca');
  var gruposLista = document.getElementById('grupos-lista');
  var gruposMeta = document.getElementById('grupos-lista-meta');
  var gruposEditor = document.getElementById('grupos-editor');
  var gruposErro = document.getElementById('grupos-erro');
  var btnNovo = document.getElementById('grupos-btn-novo');
  var btnExcluir = document.getElementById('grupos-btn-excluir');
  var modal = document.getElementById('modal-erp-pick');
  var modalQ = document.getElementById('modal-erp-q');
  var modalLista = document.getElementById('modal-erp-lista');
  var modalFechar = document.getElementById('modal-erp-fechar');

  var gruposCarregados = false;
  var listaGrupos = [];
  var selecionadoGrupoId = null;
  var rowErpTarget = null;
  var debounceGr = null;

  function mostrarErroGr(msg) {
    if (!msg) {
      gruposErro.classList.add('hidden');
      gruposErro.textContent = '';
      return;
    }
    gruposErro.textContent = msg;
    gruposErro.classList.remove('hidden');
  }

  function ativarTab(which) {
    var erpOn = which === 'erp';
    tabErp.setAttribute('aria-selected', erpOn ? 'true' : 'false');
    tabGrupos.setAttribute('aria-selected', erpOn ? 'false' : 'true');
    if (erpOn) {
      tabErp.className = 'min-h-[44px] px-4 rounded-xl text-sm font-black uppercase border-2 border-emerald-600 bg-emerald-50 text-emerald-900 shadow-sm';
      tabGrupos.className = 'min-h-[44px] px-4 rounded-xl text-sm font-black uppercase border-2 border-slate-200 bg-white text-slate-600 hover:border-orange-300 hover:text-orange-700 shadow-sm';
      panelErp.classList.remove('hidden');
      panelErp.classList.add('flex');
      panelGrupos.classList.add('hidden');
      panelGrupos.classList.remove('flex');
      panelGrupos.setAttribute('aria-hidden', 'true');
    } else {
      tabGrupos.className = 'min-h-[44px] px-4 rounded-xl text-sm font-black uppercase border-2 border-orange-500 bg-orange-50 text-orange-900 shadow-sm';
      tabErp.className = 'min-h-[44px] px-4 rounded-xl text-sm font-black uppercase border-2 border-slate-200 bg-white text-slate-600 hover:border-emerald-400 hover:text-emerald-800 shadow-sm';
      panelGrupos.classList.remove('hidden');
      panelGrupos.classList.add('flex');
      panelErp.classList.add('hidden');
      panelErp.classList.remove('flex');
      panelGrupos.setAttribute('aria-hidden', 'false');
      if (!gruposCarregados) carregarGrupos();
    }
  }

  tabErp.addEventListener('click', function () { ativarTab('erp'); });
  tabGrupos.addEventListener('click', function () { ativarTab('grupos'); });

  function renderListaGrupos() {
    gruposLista.innerHTML = '';
    var q = (gruposBusca.value || '').trim().toLowerCase();
    var filtrados = listaGrupos.filter(function (g) {
      if (!q) return true;
      return String(g.nome || '').toLowerCase().indexOf(q) !== -1;
    });
    gruposMeta.textContent = filtrados.length + ' grupo(s)' + (q ? ' · filtro' : '');
    if (!filtrados.length) {
      gruposLista.innerHTML = '<p class="p-4 text-center text-slate-500 font-semibold">Nenhum grupo. Use <strong>Novo grupo</strong>.</p>';
      return;
    }
    filtrados.forEach(function (g) {
      var btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'w-full text-left rounded-xl border-2 px-3 py-3 transition-colors bg-slate-50/50 border-slate-200 hover:border-orange-300 hover:bg-orange-50/40 focus:outline-none focus:ring-2 focus:ring-orange-400/60 min-h-[56px]';
      if (String(g.id) === String(selecionadoGrupoId)) {
        btn.className = 'w-full text-left rounded-xl border-2 px-3 py-3 transition-colors bg-orange-50 border-orange-500 shadow-sm focus:outline-none focus:ring-2 focus:ring-orange-400/60 min-h-[56px]';
      }
      var inv = !g.ativo ? '<span class="text-[10px] font-black uppercase text-amber-800 bg-amber-100 px-1.5 py-0.5 rounded ml-1">inativo</span>' : '';
      btn.innerHTML = '<div class="font-black text-base text-slate-900 leading-snug">' + escapeHtml(g.nome) + inv + '</div>' +
        '<div class="text-xs font-bold text-slate-500 mt-1">' + String(g.n_variantes || 0) + ' marca(s) · <span class="text-emerald-700">' + fmtMoney(g.preco_venda) + '</span></div>';
      btn.addEventListener('click', function () {
        selecionadoGrupoId = g.id;
        renderListaGrupos();
        carregarDetalheGrupo(g.id);
      });
      gruposLista.appendChild(btn);
    });
  }

  function editorVazio() {
    selecionadoGrupoId = null;
    btnExcluir.classList.add('hidden');
    gruposEditor.innerHTML = '<p class="text-base font-semibold text-slate-500">Selecione um grupo ou clique em <strong>Novo grupo</strong>.</p>';
  }

  function linhaVariante(v) {
    var vid = v && v.id ? String(v.id) : '';
    var marca = v && v.marca ? escapeHtml(v.marca) : '';
    var cb = v && v.codigo_barras ? escapeHtml(v.codigo_barras) : '';
    var erp = v && v.produto_erp_id ? escapeHtml(v.produto_erp_id) : '';
    return '<tr class="border-b border-slate-100 align-top" data-vid="' + escapeHtml(vid) + '">' +
      '<td class="py-2 pr-2"><label class="block text-[10px] font-black uppercase text-slate-400 mb-1">Marca</label>' +
      '<input type="text" class="grupo-in-marca w-full min-h-[44px] px-3 rounded-lg border-2 border-slate-200 text-base font-semibold" value="' + marca + '" maxlength="120" /></td>' +
      '<td class="py-2 pr-2"><label class="block text-[10px] font-black uppercase text-slate-400 mb-1">Código barras</label>' +
      '<input type="text" class="grupo-in-cb w-full min-h-[44px] px-3 rounded-lg border-2 border-slate-200 text-base font-mono font-bold" value="' + cb + '" maxlength="80" inputmode="numeric" /></td>' +
      '<td class="py-2 w-28 shrink-0"><label class="block text-[10px] font-black uppercase text-slate-400 mb-1">ERP</label>' +
      '<button type="button" class="grupo-btn-erp w-full min-h-[44px] rounded-lg text-[10px] font-black uppercase bg-slate-100 border-2 border-slate-200 text-slate-700 hover:border-emerald-400">Preencher</button>' +
      '<input type="hidden" class="grupo-in-erp" value="' + erp + '" /></td>' +
      '<td class="py-2 w-12"><label class="block text-[10px] font-black uppercase text-slate-400 mb-1">&nbsp;</label>' +
      '<button type="button" class="grupo-btn-del min-h-[44px] w-10 rounded-lg border-2 border-red-200 text-red-700 font-black hover:bg-red-50" title="Remover linha">✕</button></td></tr>';
  }

  function montarEditor(grupo, novo) {
    btnExcluir.classList.toggle('hidden', novo);
    var idVal = grupo && grupo.id ? grupo.id : '';
    var nome = grupo && grupo.nome ? escapeHtml(grupo.nome) : '';
    var pv = grupo && grupo.preco_venda != null ? String(grupo.preco_venda).replace('.', ',') : '';
    var ativo = !grupo || grupo.ativo !== false;
    var vars = (grupo && grupo.variantes) ? grupo.variantes : [];
    var tbody = vars.map(linhaVariante).join('');
    if (!tbody) tbody = linhaVariante(null);

    gruposEditor.innerHTML =
      '<input type="hidden" id="grupo-pk" value="' + escapeHtml(String(idVal)) + '" />' +
      '<div class="space-y-2">' +
      '<label class="block"><span class="text-xs font-black uppercase text-slate-500">Nome do produto (vitrine)</span>' +
      '<input type="text" id="grupo-nome" class="mt-1 w-full min-h-[48px] px-4 rounded-xl border-[3px] border-emerald-500 text-lg font-black" value="' + nome + '" maxlength="300" /></label>' +
      '<label class="block max-w-xs"><span class="text-xs font-black uppercase text-slate-500">Preço de venda (todas as marcas)</span>' +
      '<input type="text" id="grupo-preco" class="mt-1 w-full min-h-[48px] px-4 rounded-xl border-[3px] border-orange-400 text-lg font-black" value="' + pv + '" inputmode="decimal" placeholder="0,00" /></label>' +
      '<label class="inline-flex items-center gap-2 mt-2 cursor-pointer select-none">' +
      '<input type="checkbox" id="grupo-ativo" class="w-5 h-5 rounded border-slate-300 text-emerald-600"' + (ativo ? ' checked' : '') + ' />' +
      '<span class="text-sm font-black uppercase text-slate-700">Grupo ativo</span></label></div>' +
      '<div class="mt-4">' +
      '<div class="flex flex-wrap items-center justify-between gap-2 mb-2">' +
      '<span class="text-xs font-black uppercase text-slate-500">Marcas / códigos de barras</span>' +
      '<button type="button" id="grupo-add-var" class="min-h-[40px] px-3 rounded-lg text-xs font-black uppercase bg-emerald-600 text-white border-2 border-emerald-700">+ Marca</button></div>' +
      '<div class="overflow-x-auto rounded-xl border border-slate-200">' +
      '<table class="w-full text-left"><thead><tr class="bg-slate-50 text-[10px] font-black uppercase text-slate-500">' +
      '<th class="px-2 py-2">Marca</th><th class="px-2 py-2">Código barras</th><th class="px-2 py-2 w-28">ERP</th><th class="w-12"></th></tr></thead>' +
      '<tbody id="grupo-tbody-variantes">' + tbody + '</tbody></table></div></div>' +
      '<div class="flex flex-wrap gap-2 mt-6">' +
      '<button type="button" id="grupo-btn-salvar" class="min-h-[48px] px-6 rounded-xl text-sm font-black uppercase bg-emerald-600 hover:bg-emerald-700 text-white border-2 border-emerald-800">Salvar grupo</button></div>';

    var tbodyEl = document.getElementById('grupo-tbody-variantes');
    tbodyEl.addEventListener('click', function (e) {
      var t = e.target;
      if (t.classList.contains('grupo-btn-del')) {
        var tr = t.closest('tr');
        if (tbodyEl.querySelectorAll('tr').length <= 1) return;
        tr.remove();
        return;
      }
      if (t.classList.contains('grupo-btn-erp')) {
        rowErpTarget = t.closest('tr');
        abrirModalErp();
      }
    });
    document.getElementById('grupo-add-var').addEventListener('click', function () {
      tbodyEl.insertAdjacentHTML('beforeend', linhaVariante(null));
    });
    document.getElementById('grupo-btn-salvar').addEventListener('click', salvarGrupo);
  }

  function carregarDetalheGrupo(id) {
    mostrarErroGr('');
    setLoading(true);
    fetch(urlGrupo(id), { credentials: 'same-origin' })
      .then(function (r) {
        return r.json().then(function (j) { return { ok: r.ok, j: j }; }).catch(function () { return { ok: r.ok, j: {} }; });
      })
      .then(function (x) {
        if (!x.ok || !x.j || !x.j.ok) {
          throw new Error((x.j && x.j.erro) || (x.ok === false ? 'Acesso negado — faça login.' : 'Falha ao carregar'));
        }
        montarEditor(x.j.grupo, false);
      })
      .catch(function (err) {
        mostrarErroGr(err.message || 'Erro ao carregar grupo');
        editorVazio();
      })
      .finally(function () { setLoading(false); });
  }

  function novoGrupo() {
    selecionadoGrupoId = null;
    renderListaGrupos();
    montarEditor(null, true);
  }

  function coletarVariantes() {
    var rows = gruposEditor.querySelectorAll('#grupo-tbody-variantes tr');
    var out = [];
    rows.forEach(function (tr) {
      var vid = tr.getAttribute('data-vid') || '';
      var marca = (tr.querySelector('.grupo-in-marca') || {}).value || '';
      var cb = (tr.querySelector('.grupo-in-cb') || {}).value || '';
      var erp = (tr.querySelector('.grupo-in-erp') || {}).value || '';
      var idNum = vid ? parseInt(vid, 10) : NaN;
      out.push({
        id: !isNaN(idNum) ? idNum : null,
        marca: marca.trim(),
        codigo_barras: cb.trim().replace(/\s+/g, ''),
        produto_erp_id: erp.trim()
      });
    });
    return out;
  }

  function salvarGrupo() {
    mostrarErroGr('');
    var pk = (document.getElementById('grupo-pk') || {}).value || '';
    var nome = (document.getElementById('grupo-nome') || {}).value || '';
    var preco = (document.getElementById('grupo-preco') || {}).value || '';
    var ativo = !!(document.getElementById('grupo-ativo') || {}).checked;
    var variantes = coletarVariantes();
    var body = {
      id: pk ? parseInt(pk, 10) : null,
      nome: nome.trim(),
      preco_venda: preco.trim().replace(/\./g, '').replace(',', '.'),
      ativo: ativo,
      variantes: variantes
    };
    setLoading(true);
    fetch(URL_SALVAR, {
      method: 'POST',
      credentials: 'same-origin',
      headers: {
        'Content-Type': 'application/json',
        'X-CSRFToken': U.csrf()
      },
      body: JSON.stringify(body)
    })
      .then(function (r) {
        return r.json().then(function (j) { return { ok: r.ok, j: j }; }).catch(function () { return { ok: r.ok, j: {} }; });
      })
      .then(function (x) {
        if (!x.ok || !x.j || !x.j.ok) throw new Error((x.j && x.j.erro) || 'Falha ao salvar');
        if (body.id) {
          listaGrupos = listaGrupos.filter(function (g) { return String(g.id) !== String(body.id); });
        }
        var g = x.j.grupo;
        listaGrupos.push({
          id: g.id,
          nome: g.nome,
          preco_venda: g.preco_venda,
          ativo: g.ativo,
          n_variantes: (g.variantes || []).length
        });
        listaGrupos.sort(function (a, b) { return String(a.nome).localeCompare(String(b.nome), 'pt'); });
        selecionadoGrupoId = g.id;
        gruposCarregados = true;
        renderListaGrupos();
        montarEditor(g, false);
      })
      .catch(function (err) {
        mostrarErroGr(err.message || 'Erro ao salvar');
      })
      .finally(function () { setLoading(false); });
  }

  function carregarGrupos() {
    mostrarErroGr('');
    setLoading(true);
    fetch(URL_GRUPOS, { credentials: 'same-origin' })
      .then(function (r) {
        return r.json().then(function (j) { return { ok: r.ok, j: j }; }).catch(function () { return { ok: r.ok, j: {} }; });
      })
      .then(function (x) {
        if (!x.ok || !x.j || !x.j.ok) throw new Error((x.j && x.j.erro) || 'Falha ao listar (login necessário)');
        listaGrupos = x.j.grupos || [];
        gruposCarregados = true;
        renderListaGrupos();
        if (!listaGrupos.length) editorVazio();
      })
      .catch(function (err) {
        mostrarErroGr('Grupos: ' + (err.message || 'erro') + '. É necessário estar logado.');
        gruposLista.innerHTML = '';
        gruposMeta.textContent = '—';
        editorVazio();
      })
      .finally(function () { setLoading(false); });
  }

  btnNovo.addEventListener('click', function () { novoGrupo(); });

  btnExcluir.addEventListener('click', function () {
    var pk = (document.getElementById('grupo-pk') || {}).value;
    if (!pk || !confirm('Excluir este grupo e todas as marcas vinculadas?')) return;
    setLoading(true);
    fetch(urlExcluir(pk), {
      method: 'POST',
      credentials: 'same-origin',
      headers: { 'X-CSRFToken': U.csrf() }
    })
      .then(function (r) {
        return r.json().then(function (j) { return { ok: r.ok, j: j }; }).catch(function () { return { ok: r.ok, j: {} }; });
      })
      .then(function (x) {
        if (!x.ok || !x.j || !x.j.ok) throw new Error((x.j && x.j.erro) || 'Falha ao excluir');
        listaGrupos = listaGrupos.filter(function (g) { return String(g.id) !== String(pk); });
        selecionadoGrupoId = null;
        renderListaGrupos();
        editorVazio();
      })
      .catch(function (err) { mostrarErroGr(err.message || 'Erro ao excluir'); })
      .finally(function () { setLoading(false); });
  });

  gruposBusca.addEventListener('input', function () {
    clearTimeout(debounceGr);
    debounceGr = setTimeout(renderListaGrupos, 200);
  });

  function fecharModalErp() {
    modal.classList.add('hidden');
    rowErpTarget = null;
    modalLista.innerHTML = '';
    modalQ.value = '';
  }
  modalFechar.addEventListener('click', fecharModalErp);
  modal.addEventListener('click', function (e) { if (e.target === modal) fecharModalErp(); });

  function abrirModalErp() {
    modal.classList.remove('hidden');
    modalQ.focus();
    buscarModalErp();
  }

  var debounceModal = null;
  modalQ.addEventListener('input', function () {
    clearTimeout(debounceModal);
    debounceModal = setTimeout(buscarModalErp, 350);
  });

  function buscarModalErp() {
    var q = (modalQ.value || '').trim();
    modalLista.innerHTML = '<p class="p-3 text-sm text-slate-500 font-semibold">Digite para buscar…</p>';
    if (!q) return;
    fetch(API_ERP + '?q=' + encodeURIComponent(q) + '&limit=40', { credentials: 'same-origin' })
      .then(function (r) { return r.json(); })
      .then(function (data) {
        if (!data.ok) throw new Error(data.erro || 'erro');
        var prods = data.produtos || [];
        modalLista.innerHTML = '';
        if (!prods.length) {
          modalLista.innerHTML = '<p class="p-3 text-sm text-slate-500">Nenhum resultado.</p>';
          return;
        }
        prods.forEach(function (p) {
          var b = document.createElement('button');
          b.type = 'button';
          b.className = 'w-full text-left rounded-xl border-2 border-slate-200 px-3 py-2 hover:border-emerald-400 bg-slate-50/50 min-h-[52px]';
          b.innerHTML = '<div class="font-bold text-slate-900">' + escapeHtml(p.nome || '—') + '</div>' +
            '<div class="text-xs text-slate-500 mt-0.5">' + escapeHtml(p.marca || '—') + ' · EAN ' + escapeHtml(p.codigo_barras || '—') + '</div>';
          b.addEventListener('click', function () {
            if (rowErpTarget) {
              var im = rowErpTarget.querySelector('.grupo-in-marca');
              var ic = rowErpTarget.querySelector('.grupo-in-cb');
              var ie = rowErpTarget.querySelector('.grupo-in-erp');
              if (im) im.value = p.marca || '';
              if (ic) ic.value = p.codigo_barras || '';
              if (ie) ie.value = p.id != null ? String(p.id) : '';
            }
            fecharModalErp();
          });
          modalLista.appendChild(b);
        });
      })
      .catch(function () {
        modalLista.innerHTML = '<p class="p-3 text-sm text-red-600">Erro na busca.</p>';
      });
  }

  document.addEventListener('keydown', function (e) {
    if (e.key !== 'F2') return;
    if (!panelGrupos.classList.contains('hidden')) {
      e.preventDefault();
      var gn = document.getElementById('grupo-nome');
      if (gn) { gn.focus(); gn.select(); }
      else gruposBusca.focus();
    }
  });
})();