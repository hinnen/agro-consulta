/**
 * PDV — overlay de 9 produtos rápidos (F11 / botão Rápido).
 */
(function () {
  'use strict';

  var URL_SLOTS = '/api/pdv/rapido/slots/';
  var URL_SALVAR = '/api/pdv/rapido/salvar/';
  var URL_BUSCAR = '/api/buscar/?wizard=1&q=';
  var overlay = document.getElementById('pdv-rapido-overlay');
  var grid = document.getElementById('pdv-rapido-grid');
  var editor = document.getElementById('pdv-rapido-editor');
  if (!overlay || !grid) return;
  if (overlay.parentElement !== document.body) {
    document.body.appendChild(overlay);
  }

  var slots = [];
  var editSlot = 0;
  var editProdutoId = '';
  var editFotoB64 = '';
  var editLimparFoto = false;
  var buscaTimer = null;
  var definirModo = false;

  function csrfToken() {
    try {
      var boot =
        document.getElementById('agro-pdv-wizard-bootstrap') ||
        document.getElementById('agro-pdv-bootstrap') ||
        document.getElementById('pdv-bootstrap');
      if (boot) {
        var j = JSON.parse(boot.textContent || '{}');
        if (j && j.csrfToken) return j.csrfToken;
      }
    } catch (e) {}
    var inp = document.querySelector('input[name=csrfmiddlewaretoken]');
    if (inp && inp.value) return inp.value;
    var m = document.cookie.match(/(?:^|;\s*)csrftoken=([^;]+)/);
    return m ? decodeURIComponent(m[1]) : '';
  }

  function formatMoney(n) {
    var v = Number(n);
    if (!isFinite(v)) v = 0;
    try {
      return v.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
    } catch (e2) {
      return 'R$ ' + v.toFixed(2).replace('.', ',');
    }
  }

  function aberto() {
    return !overlay.classList.contains('hidden');
  }

  function editorAberto() {
    return !!(editor && editor.classList.contains('is-open'));
  }

  function setMsg(txt) {
    var el = document.getElementById('pdv-rapido-msg');
    if (!el) return;
    if (txt) {
      el.textContent = txt;
      el.classList.remove('hidden');
    } else {
      el.textContent = '';
      el.classList.add('hidden');
    }
  }

  function fecharEditor() {
    if (!editor) return;
    editor.classList.remove('is-open');
    editor.setAttribute('hidden', '');
    editSlot = 0;
    editProdutoId = '';
    editFotoB64 = '';
    editLimparFoto = false;
  }

  function fechar() {
    fecharEditor();
    overlay.classList.add('hidden');
    overlay.classList.remove('flex');
    definirModo = false;
    syncDefinirBtn();
    setMsg('');
    if (window.AgroOverlayStack) window.AgroOverlayStack.setOpen(overlay, false);
  }

  function syncDefinirBtn() {
    var btn = document.getElementById('pdv-rapido-definir');
    if (!btn) return;
    btn.textContent = definirModo ? 'Vender' : 'Definir';
    btn.classList.toggle('bg-emerald-600', definirModo);
    btn.classList.toggle('text-white', definirModo);
    btn.classList.toggle('bg-orange-50', !definirModo);
    btn.classList.toggle('text-orange-950', !definirModo);
  }

  function slotPorN(n) {
    var i;
    for (i = 0; i < slots.length; i++) {
      if (Number(slots[i].slot) === n) return slots[i];
    }
    return { slot: n, preenchido: false, apelido: '', preco_venda: 0, foto_url: '', produto_id: '' };
  }

  function renderGrid() {
    var html = '';
    var n;
    for (n = 1; n <= 9; n++) {
      var s = slotPorN(n);
      if (s.preenchido) {
        var foto = s.tem_foto && s.foto_url
          ? '<div class="pdv-rapido-slot-foto" style="background-image:url(\'' + String(s.foto_url).replace(/'/g, '%27') + '\')"></div>'
          : '<div class="pdv-rapido-slot-foto is-blank">●</div>';
        html +=
          '<div class="pdv-rapido-slot" data-rapido-slot="' +
          n +
          '" role="button" tabindex="0">' +
          '<span class="pdv-rapido-slot-num">' +
          n +
          '</span>' +
          '<button type="button" class="pdv-rapido-slot-cfg" data-rapido-cfg="' +
          n +
          '" title="Definir este slot">⚙</button>' +
          foto +
          '<div class="pdv-rapido-slot-meta"><div class="pdv-rapido-slot-nome">' +
          escapeHtml(s.apelido || s.produto_nome || 'Produto') +
          '</div><div class="pdv-rapido-slot-preco">' +
          formatMoney(s.preco_venda) +
          '</div></div></div>';
      } else {
        html +=
          '<div class="pdv-rapido-slot is-empty" data-rapido-slot="' +
          n +
          '" role="button" tabindex="0"><span class="pdv-rapido-slot-num">' +
          n +
          '</span><span class="text-[clamp(1.1rem,0.4vw+1rem,1.4rem)] font-black text-slate-500">Definir</span></div>';
      }
    }
    grid.innerHTML = html;
  }

  function escapeHtml(s) {
    return String(s || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function carregarSlots() {
    return fetch(URL_SLOTS, { credentials: 'same-origin' })
      .then(function (r) {
        return r.json();
      })
      .then(function (data) {
        slots = data && Array.isArray(data.slots) ? data.slots : [];
        renderGrid();
      })
      .catch(function () {
        setMsg('Não deu para carregar os slots. Tente de novo.');
      });
  }

  function abrir() {
    overlay.classList.remove('hidden');
    overlay.classList.add('flex');
    definirModo = false;
    syncDefinirBtn();
    setMsg('');
    fecharEditor();
    if (window.AgroOverlayStack) window.AgroOverlayStack.setOpen(overlay, true);
    carregarSlots();
  }

  function toggle() {
    if (aberto()) fechar();
    else abrir();
  }

  function adicionarSlot(n) {
    var s = slotPorN(n);
    if (!s.preenchido || !s.produto_id) {
      abrirEditor(n);
      return;
    }
    var add = window.AgroPdvAddProductById || window.AgroPdvAddProductByCode;
    if (typeof add !== 'function') {
      setMsg('PDV ainda carregando. Tente de novo.');
      return;
    }
    Promise.resolve(add(s.produto_id, 1)).then(function (ok) {
      if (ok === false) {
        setMsg('Não deu para adicionar. Confira o produto do slot.');
        return;
      }
      fechar();
    });
  }

  function abrirEditor(n) {
    var s = slotPorN(n);
    editSlot = n;
    editProdutoId = s.produto_id || '';
    editFotoB64 = '';
    editLimparFoto = false;
    var apelido = document.getElementById('pdv-rapido-apelido');
    var titulo = document.getElementById('pdv-rapido-editor-titulo');
    var busca = document.getElementById('pdv-rapido-busca');
    var lista = document.getElementById('pdv-rapido-busca-lista');
    var prev = document.getElementById('pdv-rapido-foto-prev');
    var atual = document.getElementById('pdv-rapido-produto-atual');
    if (titulo) titulo.textContent = 'Definir slot ' + n;
    if (apelido) apelido.value = s.apelido || '';
    if (busca) busca.value = '';
    if (lista) lista.innerHTML = '';
    if (prev) {
      if (s.foto_url) prev.src = s.foto_url;
      else prev.removeAttribute('src');
    }
    if (atual) {
      atual.textContent = s.produto_id
        ? (s.produto_nome || s.produto_id) + ' · ' + formatMoney(s.preco_venda)
        : 'Nenhum produto vinculado ainda.';
    }
    if (editor) {
      editor.classList.add('is-open');
      editor.removeAttribute('hidden');
    }
    setTimeout(function () {
      if (apelido) apelido.focus();
    }, 40);
  }

  function fileToDataUrl(file) {
    return new Promise(function (resolve, reject) {
      var fr = new FileReader();
      fr.onload = function () {
        resolve(String(fr.result || ''));
      };
      fr.onerror = reject;
      fr.readAsDataURL(file);
    });
  }

  function buscarProdutos(q) {
    var termo = String(q || '').trim();
    var lista = document.getElementById('pdv-rapido-busca-lista');
    if (!lista) return;
    if (termo.length < 2) {
      lista.innerHTML = '';
      return;
    }
    fetch(URL_BUSCAR + encodeURIComponent(termo), { credentials: 'same-origin' })
      .then(function (r) {
        return r.json();
      })
      .then(function (data) {
        var rows = (data && (data.produtos || data.results)) || [];
        if (!rows.length) {
          lista.innerHTML = '<p class="px-2 py-3 text-sm font-bold text-slate-500">Nada encontrado.</p>';
          return;
        }
        lista.innerHTML = rows
          .slice(0, 12)
          .map(function (p) {
            var id = String(p.id || p.produto_id || '').trim();
            var nome = String(p.nome || '').trim();
            var preco = p.preco_venda != null ? p.preco_venda : p.preco;
            return (
              '<button type="button" class="pdv-rapido-busca-item" data-pid="' +
              escapeHtml(id) +
              '" data-pnome="' +
              escapeHtml(nome) +
              '" data-ppreco="' +
              escapeHtml(String(preco || 0)) +
              '"><span class="min-w-0 flex-1 font-black text-slate-900">' +
              escapeHtml(nome) +
              '</span><span class="shrink-0 font-black text-emerald-800">' +
              formatMoney(preco) +
              '</span></button>'
            );
          })
          .join('');
      })
      .catch(function () {
        lista.innerHTML = '<p class="px-2 py-3 text-sm font-bold text-red-600">Falha na busca.</p>';
      });
  }

  function escolherProduto(btn) {
    editProdutoId = btn.getAttribute('data-pid') || '';
    var nome = btn.getAttribute('data-pnome') || '';
    var preco = btn.getAttribute('data-ppreco') || '0';
    var atual = document.getElementById('pdv-rapido-produto-atual');
    var apelido = document.getElementById('pdv-rapido-apelido');
    if (atual) atual.textContent = nome + ' · ' + formatMoney(preco);
    if (apelido && !String(apelido.value || '').trim() && nome) {
      apelido.value = nome.slice(0, 32);
    }
    var lista = document.getElementById('pdv-rapido-busca-lista');
    if (lista) lista.innerHTML = '';
  }

  function salvarEditor() {
    if (!editSlot) return;
    var apelidoEl = document.getElementById('pdv-rapido-apelido');
    var body = {
      slot: editSlot,
      produto_id: editProdutoId,
      apelido: apelidoEl ? apelidoEl.value : '',
    };
    if (editLimparFoto) body.limpar_foto = true;
    if (editFotoB64) body.foto_base64 = editFotoB64;
    setMsg('Salvando…');
    fetch(URL_SALVAR, {
      method: 'POST',
      credentials: 'same-origin',
      headers: {
        'Content-Type': 'application/json',
        'X-CSRFToken': csrfToken(),
      },
      body: JSON.stringify(body),
    })
      .then(function (r) {
        return r.json().then(function (j) {
          return { okHttp: r.ok, j: j };
        });
      })
      .then(function (pack) {
        if (!pack.j || !pack.j.ok) {
          setMsg((pack.j && pack.j.erro) || 'Não salvou o slot.');
          return;
        }
        setMsg('');
        fecharEditor();
        return carregarSlots();
      })
      .catch(function () {
        setMsg('Falha de rede ao salvar.');
      });
  }

  function esvaziarSlot() {
    if (!editSlot) return;
    fetch(URL_SALVAR, {
      method: 'POST',
      credentials: 'same-origin',
      headers: {
        'Content-Type': 'application/json',
        'X-CSRFToken': csrfToken(),
      },
      body: JSON.stringify({ slot: editSlot, limpar: true }),
    })
      .then(function (r) {
        return r.json();
      })
      .then(function (j) {
        if (!j || !j.ok) {
          setMsg((j && j.erro) || 'Não esvaziou.');
          return;
        }
        fecharEditor();
        return carregarSlots();
      })
      .catch(function () {
        setMsg('Falha de rede ao esvaziar.');
      });
  }

  grid.addEventListener('click', function (ev) {
    var cfg = ev.target.closest ? ev.target.closest('[data-rapido-cfg]') : null;
    if (cfg) {
      ev.preventDefault();
      ev.stopPropagation();
      abrirEditor(Number(cfg.getAttribute('data-rapido-cfg')));
      return;
    }
    var btn = ev.target.closest ? ev.target.closest('[data-rapido-slot]') : null;
    if (!btn) return;
    var n = Number(btn.getAttribute('data-rapido-slot'));
    if (definirModo) abrirEditor(n);
    else adicionarSlot(n);
  });

  var btnTop = document.getElementById('pdv-topbar-rapido-btn');
  var btnStep = document.getElementById('pdv-btn-rapido');
  if (btnTop) btnTop.addEventListener('click', abrir);
  if (btnStep) btnStep.addEventListener('click', abrir);
  var btnFechar = document.getElementById('pdv-rapido-fechar');
  if (btnFechar) btnFechar.addEventListener('click', fechar);
  var btnDef = document.getElementById('pdv-rapido-definir');
  if (btnDef) {
    btnDef.addEventListener('click', function () {
      definirModo = !definirModo;
      syncDefinirBtn();
      setMsg(definirModo ? 'Toque no slot para definir nome, foto e produto.' : '');
    });
  }
  var edFechar = document.getElementById('pdv-rapido-editor-fechar');
  var edCancel = document.getElementById('pdv-rapido-editor-cancelar');
  if (edFechar) edFechar.addEventListener('click', fecharEditor);
  if (edCancel) edCancel.addEventListener('click', fecharEditor);
  var edSalvar = document.getElementById('pdv-rapido-editor-salvar');
  if (edSalvar) edSalvar.addEventListener('click', salvarEditor);
  var edLimpar = document.getElementById('pdv-rapido-slot-limpar');
  if (edLimpar) edLimpar.addEventListener('click', esvaziarSlot);
  var fotoInp = document.getElementById('pdv-rapido-foto');
  if (fotoInp) {
    fotoInp.addEventListener('change', function () {
      var f = fotoInp.files && fotoInp.files[0];
      if (!f) return;
      fileToDataUrl(f).then(function (url) {
        editFotoB64 = url;
        editLimparFoto = false;
        var prev = document.getElementById('pdv-rapido-foto-prev');
        if (prev) prev.src = url;
      });
      fotoInp.value = '';
    });
  }
  var fotoLimpar = document.getElementById('pdv-rapido-foto-limpar');
  if (fotoLimpar) {
    fotoLimpar.addEventListener('click', function () {
      editFotoB64 = '';
      editLimparFoto = true;
      var prev = document.getElementById('pdv-rapido-foto-prev');
      if (prev) prev.removeAttribute('src');
    });
  }
  var busca = document.getElementById('pdv-rapido-busca');
  if (busca) {
    busca.addEventListener('input', function () {
      clearTimeout(buscaTimer);
      buscaTimer = setTimeout(function () {
        buscarProdutos(busca.value);
      }, 180);
    });
  }
  var lista = document.getElementById('pdv-rapido-busca-lista');
  if (lista) {
    lista.addEventListener('click', function (ev) {
      var item = ev.target.closest ? ev.target.closest('[data-pid]') : null;
      if (item) escolherProduto(item);
    });
  }

  document.addEventListener(
    'keydown',
    function (ev) {
      if (ev.altKey || ev.ctrlKey || ev.metaKey) return;
      if (ev.code === 'F11') {
        ev.preventDefault();
        ev.stopImmediatePropagation();
        toggle();
        return;
      }
      if (!aberto()) return;
      if (ev.key === 'Escape') {
        ev.preventDefault();
        ev.stopImmediatePropagation();
        if (editorAberto()) fecharEditor();
        else fechar();
        return;
      }
      if (editorAberto()) return;
      var map = {
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
      var n = map[ev.code];
      if (n) {
        ev.preventDefault();
        ev.stopImmediatePropagation();
        if (definirModo) abrirEditor(n);
        else adicionarSlot(n);
      }
    },
    true
  );

  window.PdvRapido = { abrir: abrir, fechar: fechar, toggle: toggle };
})();
