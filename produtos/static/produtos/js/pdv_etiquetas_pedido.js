/**
 * PDV — pedir etiquetas (mesmo pacote do app celular, sem câmera).
 */
(function () {
  'use strict';

  var URL_SLIM = '/api/pdv/catalogo-slim/';
  var URL_ENVIAR = '/api/produtos/etiquetas/solicitacoes/pdv/';
  var LS_DIRETO = 'agro_etq_bip_enviar_direto';
  var overlay = document.getElementById('pdv-etq-pedido-overlay');
  var btn = document.getElementById('pdv-topbar-etiquetas-btn');
  var busca = document.getElementById('pdv-etq-pedido-busca');
  var lista = document.getElementById('pdv-etq-pedido-lista');
  var sacolaEl = document.getElementById('pdv-etq-pedido-sacola');
  var totalEl = document.getElementById('pdv-etq-pedido-total');
  var statusEl = document.getElementById('pdv-etq-pedido-status');
  var btnEnviar = document.getElementById('pdv-etq-pedido-enviar');
  var chkDireto = document.getElementById('pdv-etq-pedido-direto');
  var semEl = document.getElementById('pdv-etq-pedido-sem');
  if (!overlay || !btn || !busca) return;

  var base = [];
  var sacola = [];
  var enviando = false;
  var timer = null;
  var lastCodigo = '';
  var lastAt = 0;

  function csrf() {
    var m = document.cookie.match(/(?:^|; )csrftoken=([^;]+)/);
    return m ? decodeURIComponent(m[1]) : '';
  }
  function esc(s) {
    return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/"/g, '&quot;');
  }
  function aberto() {
    return !overlay.classList.contains('hidden');
  }
  function setStatus(msg, err) {
    statusEl.textContent = msg || '';
    statusEl.className = 'mt-2 min-h-[1.25rem] text-sm font-bold ' + (err ? 'text-red-700' : 'text-slate-600');
  }
  function total() {
    return sacola.reduce(function (s, it) { return s + (parseInt(it.qtd, 10) || 1); }, 0);
  }
  function renderSacola() {
    var n = total();
    totalEl.textContent = String(n);
    btnEnviar.disabled = !sacola.length || enviando;
    btnEnviar.textContent = sacola.length ? 'Enviar ' + n + ' etiqueta' + (n === 1 ? '' : 's') : 'Enviar para a loja';
    sacolaEl.innerHTML = sacola.map(function (it, idx) {
      var q = parseInt(it.qtd, 10) || 1;
      return '<div class="flex items-center gap-2 rounded-lg border border-slate-200 px-2 py-1.5">' +
        '<span class="min-w-0 flex-1 text-sm font-black leading-tight">' + esc(it.nome || it.id) + '</span>' +
        '<button type="button" data-menos="' + idx + '" class="min-h-9 min-w-9 rounded-lg border border-slate-300 font-black">−</button>' +
        '<input data-qtd="' + idx + '" type="number" min="1" max="999" value="' + q + '" class="w-14 rounded-lg border border-slate-300 text-center font-black">' +
        '<button type="button" data-mais="' + idx + '" class="min-h-9 min-w-9 rounded-lg border border-slate-300 font-black">+</button>' +
        '<button type="button" data-rm="' + idx + '" class="min-h-9 min-w-9 rounded-lg border border-red-200 text-red-700 font-black" aria-label="Remover">×</button>' +
        '</div>';
    }).join('');
  }
  function add(prod, qtd) {
    var id = String(prod.id);
    var found = sacola.find(function (x) { return String(x.id) === id; });
    var qq = qtd > 0 ? qtd : 1;
    if (found) found.qtd = Math.min(999, (parseInt(found.qtd, 10) || 1) + qq);
    else sacola.push({ id: id, nome: String(prod.nome || '').trim(), qtd: qq });
    renderSacola();
    setStatus('Na sacola: ' + (prod.nome || id));
  }
  function renderLista(rows) {
    if (!rows || !rows.length) {
      lista.innerHTML = '<p class="py-6 text-center text-sm font-bold text-slate-500">Nenhum produto.</p>';
      return;
    }
    var slice = rows.slice(0, 40);
    lista.innerHTML = slice.map(function (p) {
      var meta = [p.codigo || p.codigo_nfe || '', p.codigo_barras || ''].filter(Boolean).join(' · ');
      return '<button type="button" class="block w-full rounded-xl border-2 border-slate-200 bg-white px-3 py-2 text-left" data-id="' + esc(p.id) + '">' +
        '<strong class="block text-sm font-black">' + esc(p.nome || 'Sem nome') + '</strong>' +
        '<span class="text-xs font-bold text-slate-500">' + esc(meta) + '</span></button>';
    }).join('') + (rows.length > 40 ? '<p class="text-center text-xs font-black text-sky-800">Refine a busca</p>' : '');
  }
  function buscar(termo) {
    var t = String(termo || '').trim();
    semEl.classList.add('hidden');
    if (t.length < 2) {
      lista.innerHTML = '<p class="py-6 text-center text-sm font-bold text-slate-500">Digite 2 letras ou bipe o código e dê Enter.</p>';
      return;
    }
    var termoN = typeof normalizarBuscaLocal === 'function' ? normalizarBuscaLocal(t) : t.toLowerCase();
    var rows = typeof filtrarProdutosBuscaInteligente === 'function'
      ? filtrarProdutosBuscaInteligente(base, termoN, 'normal')
      : base.filter(function (p) { return String(p.nome || '').toLowerCase().indexOf(t.toLowerCase()) >= 0; });
    renderLista(rows);
  }
  function matchCodigo(codigo) {
    var raw = String(codigo || '').trim();
    var c = raw.replace(/\D/g, '') || raw;
    if (!c) return [];
    return base.filter(function (p) {
      var bag = [].concat(p.codigo_barras || '', p.codigo || '', p.codigo_nfe || '').concat(p.index_codigos || []);
      return bag.some(function (x) {
        var s = String(x || '').trim();
        return s === raw || s === c || s.replace(/\D/g, '') === c;
      });
    });
  }
  function debounceCodigo(codigo) {
    var c = String(codigo || '').trim();
    var now = Date.now();
    if (c && c === lastCodigo && now - lastAt < 1400) return true;
    lastCodigo = c;
    lastAt = now;
    return false;
  }
  function operadorNome() {
    var chip = document.getElementById('gm-sspin-operador-chip');
    var t = chip && !chip.classList.contains('hidden') ? String(chip.textContent || '').trim() : '';
    return t.slice(0, 150);
  }
  async function enviar(itens) {
    if (!itens.length || enviando) return false;
    enviando = true;
    renderSacola();
    setStatus('Enviando…');
    try {
      var res = await fetch(URL_ENVIAR, {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf() },
        body: JSON.stringify({ itens: itens, operador: operadorNome(), observacao: 'PDV' }),
      });
      var j = await res.json().catch(function () { return {}; });
      enviando = false;
      if (!res.ok || !j.ok) {
        setStatus(j.erro || 'Não enviou.', true);
        renderSacola();
        return false;
      }
      setStatus('Enviado. Na tela Etiquetas do computador: Solicitações.');
      renderSacola();
      return true;
    } catch (e) {
      enviando = false;
      setStatus('Erro de rede.', true);
      renderSacola();
      return false;
    }
  }
  function tentarBip(codigo) {
    if (!base.length) {
      setStatus('Aguarde o catálogo.', true);
      return true;
    }
    if (debounceCodigo(codigo)) return true;
    var hits = matchCodigo(codigo);
    if (hits.length === 1) {
      semEl.classList.add('hidden');
      if (chkDireto.checked) {
        enviar([{ id: String(hits[0].id), nome: hits[0].nome || '', qtd: 1 }]);
      } else {
        add(hits[0], 1);
      }
      busca.value = '';
      lista.innerHTML = '<p class="py-6 text-center text-sm font-bold text-slate-500">Pode bipar o próximo.</p>';
      return true;
    }
    if (hits.length > 1) {
      renderLista(hits);
      setStatus('Vários produtos — clique no certo.', true);
      return true;
    }
    semEl.textContent = 'Código sem cadastro: ' + codigo;
    semEl.classList.remove('hidden');
    buscar(codigo);
    setStatus('Código não cadastrado.', true);
    return false;
  }
  function abrir() {
    overlay.classList.remove('hidden');
    overlay.classList.add('flex');
    setTimeout(function () { busca.focus(); }, 30);
  }
  function fechar() {
    overlay.classList.add('hidden');
    overlay.classList.remove('flex');
  }
  async function carregar() {
    if (base.length) return;
    lista.innerHTML = '<p class="py-6 text-center text-sm font-bold text-slate-500">Carregando catálogo…</p>';
    try {
      var res = await fetch(URL_SLIM, { credentials: 'same-origin' });
      var j = await res.json();
      base = (j && j.produtos) || [];
      lista.innerHTML = '<p class="py-6 text-center text-sm font-bold text-slate-500">Busque ou bipe.</p>';
    } catch (e) {
      lista.innerHTML = '<p class="py-6 text-center text-sm font-bold text-red-700">Sem catálogo.</p>';
    }
  }

  btn.addEventListener('click', function () {
    abrir();
    carregar();
  });
  document.getElementById('pdv-etq-pedido-fechar').addEventListener('click', fechar);
  overlay.addEventListener('click', function (ev) {
    if (ev.target === overlay) fechar();
  });
  busca.addEventListener('input', function () {
    var v = busca.value;
    if (timer) clearTimeout(timer);
    timer = setTimeout(function () { buscar(v); }, 160);
  });
  busca.addEventListener('keydown', function (ev) {
    if (ev.key !== 'Enter') return;
    ev.preventDefault();
    ev.stopPropagation();
    if (timer) clearTimeout(timer);
    var t = busca.value.trim();
    if (!t) return;
    if (!tentarBip(t)) buscar(t);
  });
  lista.addEventListener('click', function (ev) {
    var b = ev.target.closest ? ev.target.closest('[data-id]') : null;
    if (!b) return;
    var id = b.getAttribute('data-id');
    var prod = base.find(function (x) { return String(x.id) === String(id); });
    if (prod) add(prod, 1);
    busca.focus();
  });
  sacolaEl.addEventListener('click', function (ev) {
    var t = ev.target;
    var i = parseInt(t.getAttribute('data-menos') || t.getAttribute('data-mais') || t.getAttribute('data-rm'), 10);
    if (isNaN(i) || !sacola[i]) return;
    if (t.hasAttribute('data-menos')) sacola[i].qtd = Math.max(1, (parseInt(sacola[i].qtd, 10) || 1) - 1);
    else if (t.hasAttribute('data-mais')) sacola[i].qtd = Math.min(999, (parseInt(sacola[i].qtd, 10) || 1) + 1);
    else sacola.splice(i, 1);
    renderSacola();
  });
  sacolaEl.addEventListener('change', function (ev) {
    var inp = ev.target;
    if (!inp.hasAttribute('data-qtd')) return;
    var i = parseInt(inp.getAttribute('data-qtd'), 10);
    if (sacola[i]) sacola[i].qtd = Math.max(1, Math.min(999, parseInt(inp.value, 10) || 1));
    renderSacola();
  });
  btnEnviar.addEventListener('click', async function () {
    var ok = await enviar(sacola.slice());
    if (ok) {
      sacola = [];
      renderSacola();
      busca.focus();
    }
  });
  chkDireto.addEventListener('change', function () {
    try { localStorage.setItem(LS_DIRETO, chkDireto.checked ? '1' : '0'); } catch (e) {}
  });
  try { chkDireto.checked = localStorage.getItem(LS_DIRETO) === '1'; } catch (e) {}
  document.addEventListener('keydown', function (ev) {
    if (!aberto()) return;
    if (ev.key === 'Escape') {
      ev.preventDefault();
      ev.stopPropagation();
      fechar();
    }
  }, true);
  renderSacola();
})();
