/**
 * PDV — Rápidos (Alt+6). Nove slots, apelido e foto manuais, preço do cadastro.
 * Esc fecha. 1–9 (ou clique) lança 1 unidade e fecha.
 */
(function () {
  'use strict';

  var URL_SLOTS = '/api/pdv/rapidos/';
  var overlay = document.getElementById('pdv-rapidos-overlay');
  var grid = document.getElementById('pdv-rapidos-grid');
  var btnAbrir = document.getElementById('pdv-topbar-rapidos-btn');
  var btnDefinir = document.getElementById('pdv-rapidos-definir');
  var btnVoltar = document.getElementById('pdv-rapidos-voltar');
  var btnSalvar = document.getElementById('pdv-rapidos-salvar');
  var btnFechar = document.getElementById('pdv-rapidos-fechar');
  var titulo = document.getElementById('pdv-rapidos-titulo');
  var dica = document.getElementById('pdv-rapidos-dica');
  var msgEl = document.getElementById('pdv-rapidos-msg');
  if (!overlay || !grid) return;

  if (overlay.parentElement !== document.body) {
    document.body.appendChild(overlay);
  }

  var slots = [];
  var modoDefinir = false;
  var drafts = [];
  var carregado = false;
  var salvando = false;

  function bridge() {
    return window.AgroPdvRapidosBridge || null;
  }

  function csrf() {
    var b = bridge();
    if (b && b.csrf) {
      var t = b.csrf();
      if (t) return t;
    }
    var m = document.cookie.match(/(?:^|;\s*)csrftoken=([^;]+)/);
    return m ? decodeURIComponent(m[1]) : '';
  }

  function esc(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function aberto() {
    return !overlay.classList.contains('hidden');
  }

  function setMsg(texto, tom) {
    if (!msgEl) return;
    var t = String(texto || '').trim();
    msgEl.textContent = t;
    msgEl.classList.toggle('hidden', !t);
    msgEl.classList.toggle('is-erro', tom === 'erro');
  }

  function slotVazio(n) {
    return { posicao: n, produto_id: '', codigo: '', nome: '', tem_foto: false, foto_v: 0 };
  }

  function normalizarLista(lista) {
    var map = {};
    (lista || []).forEach(function (s) {
      var n = parseInt(s && s.posicao, 10);
      if (n >= 1 && n <= 9) map[n] = s;
    });
    var out = [];
    for (var i = 1; i <= 9; i++) {
      var s = map[i] || slotVazio(i);
      out.push({
        posicao: i,
        produto_id: String(s.produto_id || '').trim(),
        codigo: String(s.codigo || '').trim(),
        nome: String(s.nome || '').trim(),
        tem_foto: !!s.tem_foto,
        foto_v: s.foto_v || 0,
      });
    }
    return out;
  }

  function precoDe(produtoId) {
    var b = bridge();
    if (!b || !b.find) return '';
    var p = b.find(produtoId);
    if (!p) return '';
    var n = p.preco_venda != null ? p.preco_venda : p.preco;
    if (n == null || n === '') return '';
    if (b.formatMoney) return b.formatMoney(n);
    return String(n);
  }

  function fotoSrc(slot, draft) {
    if (draft && draft.previewUrl) return draft.previewUrl;
    if (draft && draft.limparFoto) return '';
    if (slot && slot.tem_foto) {
      return '/api/pdv/rapidos/' + slot.posicao + '/foto/?v=' + (slot.foto_v || 0);
    }
    return '';
  }

  function codigoDe(p) {
    return String((p && (p.codigo || p.codigo_nfe || p.codigo_barras || p.ean)) || '').trim();
  }

  function renderVenda() {
    grid.innerHTML = slots
      .map(function (s) {
        var cheio = !!(s.produto_id && s.nome);
        var src = fotoSrc(s, null);
        var preco = cheio ? precoDe(s.produto_id) : '';
        var fotoHtml = src
          ? '<img src="' + esc(src) + '" alt="">'
          : '<span class="pdv-rapidos-vazio-ico" aria-hidden="true"></span>';
        return (
          '<button type="button" class="pdv-rapidos-card' +
          (cheio ? '' : ' is-vazio') +
          '" data-pos="' +
          s.posicao +
          '"' +
          (cheio ? '' : ' disabled') +
          '>' +
          '<span class="pdv-rapidos-foto">' +
          '<span class="pdv-rapidos-num">' +
          s.posicao +
          '</span>' +
          fotoHtml +
          (cheio ? '' : '<span class="pdv-rapidos-vazio-txt">vazio</span>') +
          '</span>' +
          '<span class="pdv-rapidos-corpo">' +
          '<span class="pdv-rapidos-nome">' +
          (cheio ? esc(s.nome) : '') +
          '</span>' +
          '<span class="pdv-rapidos-preco">' +
          (preco ? esc(preco) : '') +
          '</span>' +
          '</span></button>'
        );
      })
      .join('');
  }

  function renderDefinir() {
    grid.innerHTML = drafts
      .map(function (d, idx) {
        var base = slots[idx];
        var src = fotoSrc(base, d);
        var fotoHtml = src
          ? '<img src="' + esc(src) + '" alt="">'
          : '<span class="pdv-rapidos-vazio-ico" aria-hidden="true"></span><span class="pdv-rapidos-vazio-txt">Foto</span>';
        var cad = d.nome_cadastro
          ? 'Cadastro: ' + d.nome_cadastro + (d.preco_txt ? ' · ' + d.preco_txt : '')
          : d.produto_id
            ? 'Produto vinculado' + (precoDe(d.produto_id) ? ' · ' + precoDe(d.produto_id) : '')
            : 'Nenhum produto';
        return (
          '<article class="pdv-rapidos-card is-edit" data-pos="' +
          d.posicao +
          '">' +
          '<label class="pdv-rapidos-foto">' +
          '<span class="pdv-rapidos-num">' +
          d.posicao +
          '</span>' +
          fotoHtml +
          '<input type="file" accept="image/jpeg,image/png,image/webp" data-foto="' +
          d.posicao +
          '" hidden>' +
          '</label>' +
          '<div class="pdv-rapidos-corpo">' +
          '<input type="text" class="pdv-rapidos-nome-input" maxlength="32" data-nome="' +
          d.posicao +
          '" value="' +
          esc(d.nome) +
          '" placeholder="Apelido curto" autocomplete="off">' +
          '<div class="pdv-rapidos-busca-wrap">' +
          '<input type="text" class="pdv-rapidos-busca" data-busca="' +
          d.posicao +
          '" value="" placeholder="Buscar no cadastro" autocomplete="off">' +
          '<div class="pdv-rapidos-hits hidden" data-hits="' +
          d.posicao +
          '"></div>' +
          '</div>' +
          '<p class="pdv-rapidos-cadastro">' +
          esc(cad) +
          '</p>' +
          '<button type="button" class="pdv-rapidos-limpar" data-limpar="' +
          d.posicao +
          '">Limpar</button>' +
          '</div></article>'
        );
      })
      .join('');
  }

  function render() {
    if (modoDefinir) renderDefinir();
    else renderVenda();
  }

  function aplicarModo() {
    overlay.classList.toggle('is-definir', modoDefinir);
    if (titulo) titulo.textContent = modoDefinir ? 'Definir rápidos' : 'Rápidos';
    if (dica) {
      dica.textContent = modoDefinir
        ? 'Apelido e foto são seus. O preço vem do cadastro. Esc fecha sem salvar.'
        : '1 a 9 adiciona e fecha · Esc sai';
    }
    if (btnDefinir) btnDefinir.classList.toggle('hidden', modoDefinir);
    if (btnVoltar) btnVoltar.classList.toggle('hidden', !modoDefinir);
    if (btnSalvar) btnSalvar.classList.toggle('hidden', !modoDefinir);
  }

  function clonarDrafts() {
    drafts = slots.map(function (s) {
      var b = bridge();
      var p = s.produto_id && b && b.find ? b.find(s.produto_id) : null;
      var preco = '';
      if (p && b && b.formatMoney) {
        preco = b.formatMoney(p.preco_venda != null ? p.preco_venda : p.preco);
      }
      return {
        posicao: s.posicao,
        produto_id: s.produto_id,
        codigo: s.codigo,
        nome: s.nome,
        nome_cadastro: p ? String(p.nome || '').trim() : '',
        preco_txt: preco,
        fotoFile: null,
        previewUrl: '',
        limparFoto: false,
        limpar: false,
      };
    });
  }

  function carregar() {
    return fetch(URL_SLOTS, { credentials: 'same-origin' })
      .then(function (r) {
        return r.json();
      })
      .then(function (d) {
        if (!d || !d.ok) throw new Error((d && d.erro) || 'falha');
        slots = normalizarLista(d.slots);
        carregado = true;
        if (aberto() && !modoDefinir) render();
      })
      .catch(function () {
        if (!slots.length) slots = normalizarLista([]);
      });
  }

  function abrir() {
    modoDefinir = false;
    aplicarModo();
    setMsg('');
    overlay.classList.remove('hidden');
    overlay.setAttribute('aria-hidden', 'false');
    try {
      if (document.activeElement && document.activeElement !== document.body && document.activeElement.blur) {
        document.activeElement.blur();
      }
    } catch (eBlur) {}
    if (!carregado) {
      grid.innerHTML = '';
      carregar().then(render);
    } else {
      render();
    }
  }

  function fechar() {
    modoDefinir = false;
    aplicarModo();
    setMsg('');
    overlay.classList.add('hidden');
    overlay.setAttribute('aria-hidden', 'true');
    drafts.forEach(function (d) {
      if (d && d.previewUrl) {
        try {
          URL.revokeObjectURL(d.previewUrl);
        } catch (e) {}
      }
    });
    drafts = [];
  }

  function toggle() {
    if (aberto()) fechar();
    else abrir();
  }

  function entrarDefinir() {
    clonarDrafts();
    modoDefinir = true;
    aplicarModo();
    setMsg('');
    render();
  }

  function vender(pos) {
    if (modoDefinir || salvando) return;
    var slot = null;
    slots.forEach(function (s) {
      if (s.posicao === pos) slot = s;
    });
    if (!slot || !slot.produto_id || !slot.nome) return;
    var b = bridge();
    if (!b || !b.find || !b.add) {
      setMsg('PDV ainda carregando. Tente de novo.', 'erro');
      return;
    }
    var p = b.find(slot.produto_id);
    if (!p) {
      setMsg('Produto do slot ' + pos + ' não está no PDV. Atualize a página (F5).', 'erro');
      return;
    }
    fechar();
    try {
      b.add(p);
    } catch (eAdd) {}
  }

  function draftPorPos(pos) {
    for (var i = 0; i < drafts.length; i++) {
      if (drafts[i].posicao === pos) return drafts[i];
    }
    return null;
  }

  function fecharHits() {
    grid.querySelectorAll('.pdv-rapidos-hits').forEach(function (el) {
      el.classList.add('hidden');
      el.innerHTML = '';
    });
  }

  function escolherProduto(pos, produto) {
    var d = draftPorPos(pos);
    var b = bridge();
    if (!d || !produto) return;
    d.produto_id = String(produto.id || '').trim();
    d.codigo = codigoDe(produto);
    d.nome_cadastro = String(produto.nome || '').trim();
    d.preco_txt = b && b.formatMoney ? b.formatMoney(produto.preco_venda != null ? produto.preco_venda : produto.preco) : '';
    d.limpar = false;
    fecharHits();
    var nomeInput = grid.querySelector('[data-nome="' + pos + '"]');
    var focado = document.activeElement;
    render();
    var nomeDepois = grid.querySelector('[data-nome="' + pos + '"]');
    if (nomeDepois && (!nomeInput || focado === nomeInput || !String(d.nome || '').trim())) {
      nomeDepois.focus();
    }
  }

  function mostrarHits(pos, termo) {
    var box = grid.querySelector('[data-hits="' + pos + '"]');
    var b = bridge();
    if (!box) return;
    var q = String(termo || '').trim();
    if (q.length < 2 || !b || !b.buscar) {
      box.classList.add('hidden');
      box.innerHTML = '';
      return;
    }
    var lista = b.buscar(q).slice(0, 8);
    if (!lista.length) {
      box.innerHTML = '<p class="pdv-rapidos-hit-vazio">Nenhum produto</p>';
      box.classList.remove('hidden');
      return;
    }
    box.innerHTML = lista
      .map(function (p, i) {
        var preco = b.formatMoney ? b.formatMoney(p.preco_venda != null ? p.preco_venda : p.preco) : '';
        return (
          '<button type="button" class="pdv-rapidos-hit" data-hit-pos="' +
          pos +
          '" data-hit-idx="' +
          i +
          '"><span>' +
          esc(p.nome || '') +
          '</span><span>' +
          esc(preco) +
          '</span></button>'
        );
      })
      .join('');
    box._produtos = lista;
    box.classList.remove('hidden');
  }

  function limparSlot(pos) {
    var d = draftPorPos(pos);
    if (!d) return;
    if (d.previewUrl) {
      try {
        URL.revokeObjectURL(d.previewUrl);
      } catch (e) {}
    }
    d.produto_id = '';
    d.codigo = '';
    d.nome = '';
    d.nome_cadastro = '';
    d.preco_txt = '';
    d.fotoFile = null;
    d.previewUrl = '';
    d.limparFoto = true;
    d.limpar = true;
    render();
  }

  function salvar() {
    if (salvando) return;
    grid.querySelectorAll('[data-nome]').forEach(function (inp) {
      var d = draftPorPos(parseInt(inp.getAttribute('data-nome'), 10));
      if (d) d.nome = String(inp.value || '').trim();
    });
    var payload = drafts.map(function (d) {
      return {
        posicao: d.posicao,
        produto_id: d.limpar ? '' : d.produto_id,
        codigo: d.limpar ? '' : d.codigo,
        nome: d.limpar ? '' : d.nome,
        limpar: !!d.limpar || !d.produto_id,
      };
    });
    for (var i = 0; i < payload.length; i++) {
      var item = payload[i];
      var draftItem = drafts[i];
      if (item.nome && !item.produto_id) {
        setMsg('Slot ' + item.posicao + ': escolha o produto do cadastro.', 'erro');
        return;
      }
      if (item.produto_id && !item.nome) {
        setMsg('Slot ' + item.posicao + ': escreva o apelido.', 'erro');
        return;
      }
      if (draftItem && draftItem.fotoFile && (!item.produto_id || !item.nome)) {
        setMsg('Slot ' + item.posicao + ': escolha o produto e o apelido antes da foto.', 'erro');
        return;
      }
    }
    salvando = true;
    if (btnSalvar) btnSalvar.disabled = true;
    setMsg('Salvando…');
    fetch(URL_SLOTS, {
      method: 'POST',
      credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf() },
      body: JSON.stringify({ slots: payload }),
    })
      .then(function (r) {
        return r.json().then(function (j) {
          return { status: r.status, j: j };
        });
      })
      .then(function (pack) {
        if (!pack.j || !pack.j.ok) throw new Error((pack.j && pack.j.erro) || 'Falha ao salvar.');
        var fotos = drafts.filter(function (d) {
          return d.fotoFile && d.produto_id && d.nome && !d.limpar;
        });
        var seq = Promise.resolve();
        fotos.forEach(function (d) {
          seq = seq.then(function () {
            var fd = new FormData();
            if (d.fotoFile) fd.append('foto', d.fotoFile);
            else fd.append('limpar', '1');
            return fetch('/api/pdv/rapidos/' + d.posicao + '/foto/', {
              method: 'POST',
              credentials: 'same-origin',
              headers: { 'X-CSRFToken': csrf() },
              body: fd,
            }).then(function (r) {
              return r.json().then(function (j) {
                if (!j || !j.ok) throw new Error((j && j.erro) || 'Falha na foto.');
              });
            });
          });
        });
        return seq;
      })
      .then(function () {
        return carregar();
      })
      .then(function () {
        modoDefinir = false;
        aplicarModo();
        setMsg('');
        render();
      })
      .catch(function (err) {
        setMsg((err && err.message) || 'Não foi possível salvar.', 'erro');
      })
      .then(function () {
        salvando = false;
        if (btnSalvar) btnSalvar.disabled = false;
      });
  }

  if (btnAbrir) btnAbrir.addEventListener('click', abrir);
  if (btnFechar) btnFechar.addEventListener('click', fechar);
  if (btnDefinir) btnDefinir.addEventListener('click', entrarDefinir);
  if (btnVoltar) {
    btnVoltar.addEventListener('click', function () {
      modoDefinir = false;
      aplicarModo();
      setMsg('');
      render();
    });
  }
  if (btnSalvar) btnSalvar.addEventListener('click', salvar);

  grid.addEventListener('click', function (ev) {
    var limpar = ev.target.closest('[data-limpar]');
    if (limpar) {
      limparSlot(parseInt(limpar.getAttribute('data-limpar'), 10));
      return;
    }
    var hit = ev.target.closest('[data-hit-idx]');
    if (hit) {
      var box = hit.closest('.pdv-rapidos-hits');
      var lista = (box && box._produtos) || [];
      var prod = lista[parseInt(hit.getAttribute('data-hit-idx'), 10)];
      escolherProduto(parseInt(hit.getAttribute('data-hit-pos'), 10), prod);
      return;
    }
    if (modoDefinir) return;
    var card = ev.target.closest('.pdv-rapidos-card[data-pos]');
    if (!card || card.disabled) return;
    vender(parseInt(card.getAttribute('data-pos'), 10));
  });

  grid.addEventListener('input', function (ev) {
    var nome = ev.target.closest('[data-nome]');
    if (nome) {
      var d = draftPorPos(parseInt(nome.getAttribute('data-nome'), 10));
      if (d) {
        d.nome = nome.value;
        if (d.produto_id) d.limpar = false;
      }
      return;
    }
    var busca = ev.target.closest('[data-busca]');
    if (busca) mostrarHits(parseInt(busca.getAttribute('data-busca'), 10), busca.value);
  });

  grid.addEventListener('change', function (ev) {
    var inp = ev.target.closest('[data-foto]');
    if (!inp || !inp.files || !inp.files[0]) return;
    var pos = parseInt(inp.getAttribute('data-foto'), 10);
    var d = draftPorPos(pos);
    if (!d) return;
    if (d.previewUrl) {
      try {
        URL.revokeObjectURL(d.previewUrl);
      } catch (e) {}
    }
    d.fotoFile = inp.files[0];
    d.previewUrl = URL.createObjectURL(d.fotoFile);
    d.limparFoto = false;
    render();
  });

  document.addEventListener('agro-pdv-catalogo', function () {
    if (aberto() && !modoDefinir) render();
  });

  document.addEventListener(
    'keydown',
    function (ev) {
      if (ev.altKey && ev.code === 'Digit6' && !ev.ctrlKey && !ev.metaKey && !ev.shiftKey && !ev.repeat) {
        ev.preventDefault();
        ev.stopPropagation();
        toggle();
        return;
      }
      if (!aberto()) return;
      if (ev.key === 'Escape') {
        var pin = document.getElementById('modal-caixa-pin');
        if (pin && !pin.classList.contains('hidden')) return;
        if (modoDefinir && grid.querySelector('.pdv-rapidos-hits:not(.hidden)')) {
          ev.preventDefault();
          ev.stopPropagation();
          fecharHits();
          return;
        }
        ev.preventDefault();
        ev.stopPropagation();
        fechar();
        return;
      }
      if (modoDefinir) return;
      if (ev.altKey || ev.ctrlKey || ev.metaKey) return;
      var mapa = {
        Digit1: 1,
        Digit2: 2,
        Digit3: 3,
        Digit4: 4,
        Digit5: 5,
        Digit6: 6,
        Digit7: 7,
        Digit8: 8,
        Digit9: 9,
        Numpad1: 1,
        Numpad2: 2,
        Numpad3: 3,
        Numpad4: 4,
        Numpad5: 5,
        Numpad6: 6,
        Numpad7: 7,
        Numpad8: 8,
        Numpad9: 9,
      };
      var pos = mapa[ev.code];
      if (!pos) return;
      var tag = ev.target && ev.target.tagName;
      if (overlay.contains(ev.target) && (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT')) return;
      ev.preventDefault();
      ev.stopPropagation();
      vender(pos);
    },
    true
  );

  carregar();
})();
