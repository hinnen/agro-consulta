/**
 * PDV — pedir etiquetas (mesmo pacote do app celular, sem câmera).
 */
(function () {
  'use strict';

  var URL_SLIM = '/api/pdv/catalogo-slim/';
  var URL_ENVIAR = '/api/produtos/etiquetas/solicitacoes/pdv/';
  var overlay = document.getElementById('pdv-etq-pedido-overlay');
  var btn = document.getElementById('pdv-topbar-etiquetas-btn');
  var busca = document.getElementById('pdv-etq-pedido-busca');
  var lista = document.getElementById('pdv-etq-pedido-lista');
  var sacolaEl = document.getElementById('pdv-etq-pedido-sacola');
  var totalEl = document.getElementById('pdv-etq-pedido-total');
  var statusEl = document.getElementById('pdv-etq-pedido-status');
  var btnEnviar = document.getElementById('pdv-etq-pedido-enviar');
  var semEl = document.getElementById('pdv-etq-pedido-sem');
  if (!overlay || !btn || !busca) return;

  var base = [];
  var sacola = [];
  var enviando = false;
  var timer = null;
  var lastCodigo = '';
  var lastAt = 0;
  var lastRows = [];
  var selIndex = 0;

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
  function somOk() {
    try {
      var Ctx = window.AudioContext || window.webkitAudioContext;
      if (!Ctx) return;
      var ctx = new Ctx();
      var o = ctx.createOscillator();
      var g = ctx.createGain();
      o.connect(g);
      g.connect(ctx.destination);
      o.frequency.value = 880;
      g.gain.value = 0.08;
      o.start();
      o.stop(ctx.currentTime + 0.09);
      setTimeout(function () {
        var o2 = ctx.createOscillator();
        var g2 = ctx.createGain();
        o2.connect(g2);
        g2.connect(ctx.destination);
        o2.frequency.value = 1175;
        g2.gain.value = 0.07;
        o2.start();
        o2.stop(ctx.currentTime + 0.12);
      }, 110);
    } catch (e) {}
  }
  function nuvemOk(qtd) {
    var host = document.getElementById('pdv-etq-toast');
    if (!host) {
      host = document.createElement('div');
      host.id = 'pdv-etq-toast';
      host.setAttribute('role', 'status');
      document.body.appendChild(host);
    }
    host.className = 'pointer-events-none fixed bottom-4 right-4 z-[200] w-[min(22rem,calc(100vw-2rem))]';
    host.innerHTML = '<div class="rounded-2xl border-2 border-emerald-400 bg-emerald-50 px-4 py-3 text-emerald-950 shadow-2xl shadow-emerald-200/50">' +
      '<div class="flex items-start gap-3"><span class="mt-0.5 flex h-9 w-9 shrink-0 items-center justify-center rounded-full bg-white text-lg font-black">✓</span>' +
      '<div><p class="text-base font-black leading-tight">Pedido enviado</p>' +
      '<p class="mt-1 text-sm font-semibold leading-snug">' + qtd + ' etiqueta' + (qtd === 1 ? '' : 's') + '. Na tela Etiquetas: Solicitações.</p></div></div></div>';
    if (nuvemOk._t) clearTimeout(nuvemOk._t);
    nuvemOk._t = setTimeout(function () { host.innerHTML = ''; }, 4200);
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
    setStatus('No pedido: ' + (prod.nome || id));
  }
  function money(v) {
    var n = Number(v);
    if (!isFinite(n)) n = 0;
    return n.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
  }
  function gmDe(p) {
    return String((p && (p.codigo_nfe || p.codigo)) || '').trim() || '—';
  }
  function marcaDe(p) {
    return String((p && p.marca) || '').trim() || '—';
  }
  function precoDe(p) {
    if (!p) return 0;
    if (p.preco_venda != null && p.preco_venda !== '') return p.preco_venda;
    if (p.preco != null && p.preco !== '') return p.preco;
    return 0;
  }
  function aviso(html) {
    lastRows = [];
    selIndex = 0;
    lista.innerHTML = html;
  }
  function renderLista(rows, keepSel) {
    lastRows = rows || [];
    var cap = Math.min(lastRows.length, 40);
    if (!cap) {
      aviso('<p class="py-6 text-center text-sm font-bold text-slate-500">Nenhum produto.</p>');
      lastRows = [];
      return;
    }
    if (!keepSel || selIndex < 0 || selIndex >= cap) selIndex = 0;
    var head = '<thead><tr class="etq-head"><th>Produto</th><th>Marca</th><th>GM</th><th>Preço</th></tr></thead>';
    var body = '<tbody>' + lastRows.slice(0, cap).map(function (p, i) {
      var nome = String(p.nome || 'Sem nome');
      return '<tr role="option" class="etq-row' + (i === selIndex ? ' etq-sel' : '') + '" data-id="' + esc(p.id) + '" data-idx="' + i + '" title="' + esc(nome) + '" aria-selected="' + (i === selIndex ? 'true' : 'false') + '">' +
        '<td class="etq-nome">' + esc(nome) + '</td>' +
        '<td class="etq-marca">' + esc(marcaDe(p)) + '</td>' +
        '<td class="etq-gm">' + esc(gmDe(p)) + '</td>' +
        '<td class="etq-preco">' + esc(money(precoDe(p))) + '</td></tr>';
    }).join('') + '</tbody>';
    var mais = rows.length > 40 ? '<p class="py-2 text-center text-xs font-black text-sky-800">Refine a busca</p>' : '';
    lista.innerHTML = '<table class="etq-tabela">' + head + body + '</table>' + mais;
    var sel = lista.querySelector('.etq-sel');
    if (sel && sel.scrollIntoView) sel.scrollIntoView({ block: 'nearest' });
  }
  function mover(delta) {
    var cap = Math.min(lastRows.length, 40);
    if (!cap) return;
    selIndex = Math.max(0, Math.min(cap - 1, selIndex + delta));
    renderLista(lastRows, true);
  }
  function pegarSelecionado() {
    var cap = Math.min(lastRows.length, 40);
    if (!cap) return null;
    if (selIndex < 0 || selIndex >= cap) selIndex = 0;
    return lastRows[selIndex];
  }
  function buscar(termo) {
    var t = String(termo || '').trim();
    semEl.classList.add('hidden');
    if (t.length < 2) {
      aviso('<p class="py-6 text-center text-sm font-bold text-slate-500">Digite 2 letras. Setas escolhem a linha. Enter entra no pedido.</p>');
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
      var nEnv = itens.reduce(function (s, it) { return s + (parseInt(it.qtd, 10) || 1); }, 0);
      setStatus('');
      somOk();
      nuvemOk(nEnv);
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
      add(hits[0], 1);
      busca.value = '';
      aviso('<p class="py-6 text-center text-sm font-bold text-slate-500">Entrou no pedido. Pode bipar o próximo.</p>');
      return true;
    }
    if (hits.length > 1) {
      renderLista(hits);
      setStatus('Vários produtos. Setas e Enter escolhem.', true);
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
    aviso('<p class="py-6 text-center text-sm font-bold text-slate-500">Carregando catálogo…</p>');
    try {
      var res = await fetch(URL_SLIM, { credentials: 'same-origin' });
      var j = await res.json();
      base = (j && j.produtos) || [];
      aviso('<p class="py-6 text-center text-sm font-bold text-slate-500">Busque ou bipe. Setas e Enter entram no pedido.</p>');
    } catch (e) {
      aviso('<p class="py-6 text-center text-sm font-bold text-red-700">Sem catálogo.</p>');
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
    if (ev.key === 'ArrowDown' || ev.key === 'ArrowUp') {
      ev.preventDefault();
      ev.stopPropagation();
      mover(ev.key === 'ArrowDown' ? 1 : -1);
      return;
    }
    if (ev.key !== 'Enter') return;
    ev.preventDefault();
    ev.stopPropagation();
    if (timer) clearTimeout(timer);
    var t = busca.value.trim();
    if (!t) return;
    if (tentarBip(t)) return;
    if (!lastRows.length) buscar(t);
    var pick = pegarSelecionado();
    if (pick) {
      add(pick, 1);
      busca.value = '';
      aviso('<p class="py-6 text-center text-sm font-bold text-slate-500">Entrou no pedido. Pode buscar o próximo.</p>');
    }
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
