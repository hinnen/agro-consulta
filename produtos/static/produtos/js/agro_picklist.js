/**
 * AgroPickList — buscar e só selecionar da lista; novo só com + / PIN (FL-024).
 * Uso: window.AgroPickList.wire(...), .assert(...), .pedirNova(...), .loadFacetas(...)
 */
(function (global) {
  'use strict';

  var FACETAS = { marcas: [], fornecedores: [], categorias: [], subcategorias: [], unidades: [] };
  var pinModalEl = null;
  var pinState = null;

  function esc(s) {
    return String(s || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function norm(s) {
    return String(s || '')
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .toLowerCase()
      .replace(/\s+/g, ' ')
      .trim();
  }

  function facetKeyFromTipo(tipo) {
    var t = String(tipo || '').toLowerCase();
    if (t === 'marca') return 'marcas';
    if (t === 'fornecedor') return 'fornecedores';
    if (t === 'categoria') return 'categorias';
    if (t === 'subcategoria') return 'subcategorias';
    if (t === 'unidade') return 'unidades';
    return t;
  }

  function arr(key) {
    var a = FACETAS[key];
    return Array.isArray(a) ? a : [];
  }

  function findExact(key, q) {
    var nq = norm(q);
    if (!nq) return '';
    var list = arr(key);
    for (var i = 0; i < list.length; i++) {
      if (norm(list[i]) === nq) return String(list[i]).trim();
    }
    return '';
  }

  function findParecidos(key, q, limit) {
    var nq = norm(q);
    if (!nq || nq.length < 1) return [];
    var list = arr(key);
    var out = [];
    for (var i = 0; i < list.length; i++) {
      var t = String(list[i]).trim();
      var nt = norm(t);
      if (!nt || nt === nq) continue;
      if (nt.indexOf(nq) >= 0 || nq.indexOf(nt) >= 0) {
        out.push(t);
        if (out.length >= (limit || 8)) break;
      }
    }
    return out;
  }

  function appendFacet(key, val) {
    var v = String(val || '').trim();
    if (!v) return;
    if (!FACETAS[key]) FACETAS[key] = [];
    if (!findExact(key, v)) FACETAS[key].push(v);
  }

  function mergeFacetas(data) {
    if (!data || typeof data !== 'object') return;
    ['marcas', 'fornecedores', 'categorias', 'subcategorias', 'unidades'].forEach(function (k) {
      if (Array.isArray(data[k])) {
        FACETAS[k] = data[k].slice();
      }
    });
  }

  function loadFacetas(url) {
    if (!url) return Promise.resolve(FACETAS);
    return fetch(url + (url.indexOf('?') >= 0 ? '&' : '?') + '_=' + Date.now(), {
      credentials: 'same-origin',
      headers: { Accept: 'application/json' },
      cache: 'no-store',
    })
      .then(function (r) {
        return r.json().catch(function () {
          return {};
        });
      })
      .then(function (j) {
        if (j && j.ok) mergeFacetas(j);
        return FACETAS;
      })
      .catch(function () {
        return FACETAS;
      });
  }

  function markCommitted(inp, canon) {
    if (!inp) return;
    var c = String(canon == null ? inp.value : canon).trim();
    inp._agroPickOk = true;
    inp._agroPickCanon = c;
    inp.classList.remove('border-amber-500', 'ring-2', 'ring-amber-300');
  }

  function markDirty(inp) {
    if (!inp) return;
    inp._agroPickOk = false;
    if (String(inp.value || '').trim()) {
      inp.classList.add('border-amber-500', 'ring-2', 'ring-amber-300');
    } else {
      inp.classList.remove('border-amber-500', 'ring-2', 'ring-amber-300');
    }
  }

  function syncFromValue(inp, facetKey) {
    if (!inp) return true;
    var v = String(inp.value || '').trim();
    if (!v) {
      markCommitted(inp, '');
      return true;
    }
    var exact = findExact(facetKey, v);
    if (exact) {
      if (inp.value !== exact) inp.value = exact;
      markCommitted(inp, exact);
      return true;
    }
    if (inp._agroPickOk && norm(inp._agroPickCanon) === norm(v)) {
      markCommitted(inp, inp._agroPickCanon || v);
      return true;
    }
    markDirty(inp);
    return false;
  }

  function assertField(inpOrId, facetKey, label, obrigatorio) {
    var inp = typeof inpOrId === 'string' ? document.getElementById(inpOrId) : inpOrId;
    var v = inp ? String(inp.value || '').trim() : '';
    if (!v) {
      return obrigatorio ? 'Escolha ' + label + ' na lista (ou use + para cadastrar novo).' : '';
    }
    if (!syncFromValue(inp, facetKey)) {
      return (
        label +
        ' «' +
        v +
        '» não está na lista. Selecione um item ou use + para cadastrar com PIN.'
      );
    }
    return '';
  }

  function renderBox(box, q, facetKey) {
    if (!box) return;
    var list = arr(facetKey);
    var nq = norm(q);
    var hit = !nq
      ? list.slice(0, 24)
      : list
          .filter(function (s) {
            return norm(s).indexOf(nq) >= 0;
          })
          .slice(0, 24);
    if (!hit.length) {
      box.innerHTML =
        '<div class="px-3 py-2.5 text-sm font-semibold text-slate-500">Nenhum resultado. Use o botão + para cadastrar novo (com PIN).</div>';
      box.classList.remove('hidden');
      return;
    }
    box.innerHTML = hit
      .map(function (t) {
        return (
          '<button type="button" class="agro-pick-opt w-full text-left px-3 py-2.5 text-base font-semibold text-slate-800 hover:bg-emerald-50 border-b border-slate-100 last:border-0">' +
          esc(t) +
          '</button>'
        );
      })
      .join('');
    box.classList.remove('hidden');
  }

  function csrfToken(cfg) {
    cfg = cfg || {};
    if (cfg.csrf) return cfg.csrf;
    var meta = document.querySelector('meta[name="csrfmiddlewaretoken"]');
    if (meta && meta.getAttribute('content')) return meta.getAttribute('content');
    var ck = document.cookie.match(/(?:^|; )csrftoken=([^;]*)/);
    if (ck) return decodeURIComponent(ck[1]);
    var el = document.querySelector('[name=csrfmiddlewaretoken]');
    return el && el.value ? el.value : '';
  }

  function ensurePinModal() {
    if (pinModalEl) return pinModalEl;
    var wrap = document.createElement('div');
    wrap.id = 'agro-picklist-pin-modal';
    wrap.className =
      'hidden fixed inset-0 z-[80] flex items-center justify-center p-4 bg-slate-900/50';
    wrap.setAttribute('role', 'dialog');
    wrap.setAttribute('aria-modal', 'true');
    wrap.innerHTML =
      '<div class="bg-white rounded-2xl border-2 border-emerald-200 shadow-xl max-w-lg w-full p-5 max-h-[min(92dvh,36rem)] flex flex-col">' +
      '<h4 id="agro-pick-pin-tit" class="text-base font-black uppercase text-slate-800 mb-1 shrink-0">Novo</h4>' +
      '<p class="text-xs font-semibold text-slate-600 mb-3 shrink-0 leading-snug">Só use se não achar na lista. Confira os parecidos e digite o PIN.</p>' +
      '<label class="block shrink-0 mb-2"><span class="text-[11px] font-black uppercase text-slate-500 mb-1 block">Nome novo</span>' +
      '<input type="text" id="agro-pick-pin-nome" class="w-full min-h-[50px] px-3 rounded-lg border-2 border-slate-200 text-base font-bold" autocomplete="off" /></label>' +
      '<div id="agro-pick-pin-parecidos-wrap" class="hidden mb-3 min-h-0 flex-1 flex flex-col">' +
      '<span class="text-[11px] font-black uppercase text-amber-800 mb-1 shrink-0">Parecidos — clique para usar o existente</span>' +
      '<div id="agro-pick-pin-parecidos" class="border-2 border-amber-200 rounded-xl bg-amber-50/60 max-h-36 overflow-y-auto [scrollbar-width:thin]"></div></div>' +
      '<label class="block shrink-0 mb-4"><span class="text-[11px] font-black uppercase text-slate-500 mb-1 block">PIN do operador</span>' +
      '<input type="password" id="agro-pick-pin-val" inputmode="numeric" maxlength="12" class="w-full min-h-[50px] px-3 rounded-lg border-2 border-slate-200 text-base font-bold tracking-widest" autocomplete="off" /></label>' +
      '<p id="agro-pick-pin-erro" class="hidden text-sm font-bold text-red-700 mb-2 shrink-0"></p>' +
      '<div class="flex justify-end gap-2 shrink-0">' +
      '<button type="button" id="agro-pick-pin-cancel" class="min-h-[50px] px-4 rounded-xl text-sm font-black uppercase border-2 border-slate-200 text-slate-700">Cancelar</button>' +
      '<button type="button" id="agro-pick-pin-ok" class="min-h-[50px] px-5 rounded-xl text-sm font-black uppercase bg-emerald-600 text-white border-2 border-emerald-700">Cadastrar com PIN</button>' +
      '</div></div>';
    document.body.appendChild(wrap);
    pinModalEl = wrap;

    var nome = document.getElementById('agro-pick-pin-nome');
    var pin = document.getElementById('agro-pick-pin-val');
    var erro = document.getElementById('agro-pick-pin-erro');
    var parecidos = document.getElementById('agro-pick-pin-parecidos');
    var parecidosWrap = document.getElementById('agro-pick-pin-parecidos-wrap');

    function renderParecidos() {
      if (!pinState || !parecidos || !parecidosWrap) return;
      var hits = findParecidos(pinState.facetKey, nome.value, 8);
      if (!hits.length) {
        parecidosWrap.classList.add('hidden');
        parecidos.innerHTML = '';
        return;
      }
      parecidos.innerHTML = hits
        .map(function (t) {
          return (
            '<button type="button" class="agro-pick-parecido w-full text-left px-3 py-2.5 text-sm font-bold text-amber-950 hover:bg-amber-100 border-b border-amber-100 last:border-0">' +
            esc(t) +
            '</button>'
          );
        })
        .join('');
      parecidosWrap.classList.remove('hidden');
    }

    nome.addEventListener('input', renderParecidos);
    parecidos.addEventListener('click', function (e) {
      var b = e.target.closest('.agro-pick-parecido');
      if (!b || !pinState) return;
      var t = (b.textContent || '').trim();
      closePin(t, true);
    });
    document.getElementById('agro-pick-pin-cancel').addEventListener('click', function () {
      closePin(null, false);
    });
    wrap.addEventListener('click', function (e) {
      if (e.target === wrap) closePin(null, false);
    });
    document.getElementById('agro-pick-pin-ok').addEventListener('click', function () {
      if (!pinState) return;
      var v = String(nome.value || '').trim();
      var p = String(pin.value || '').trim();
      erro.classList.add('hidden');
      erro.textContent = '';
      var minLen = pinState.tipo === 'unidade' ? 1 : 2;
      if (v.length < minLen) {
        erro.textContent = 'Informe o nome.';
        erro.classList.remove('hidden');
        return;
      }
      var exact = findExact(pinState.facetKey, v);
      if (exact) {
        closePin(exact, true);
        return;
      }
      if (!p) {
        erro.textContent = 'Informe o PIN do operador.';
        erro.classList.remove('hidden');
        return;
      }
      var url = pinState.urlNova;
      if (!url) {
        erro.textContent = 'API indisponível. Recarregue a página.';
        erro.classList.remove('hidden');
        return;
      }
      var btn = document.getElementById('agro-pick-pin-ok');
      btn.disabled = true;
      fetch(url, {
        method: 'POST',
        credentials: 'same-origin',
        headers: {
          'Content-Type': 'application/json',
          'X-CSRFToken': csrfToken(pinState),
        },
        body: JSON.stringify({ tipo: pinState.tipo, valor: v, pin: p }),
      })
        .then(function (r) {
          return r.json().catch(function () {
            return {};
          }).then(function (j) {
            return { okHttp: r.ok, j: j };
          });
        })
        .then(function (pack) {
          var j = pack.j || {};
          if (!pack.okHttp || !j.ok) throw new Error((j && j.erro) || 'Não foi possível cadastrar.');
          var canon = String(j.valor || v).trim();
          appendFacet(pinState.facetKey, canon);
          closePin(canon, false);
        })
        .catch(function (err) {
          erro.textContent = (err && err.message) || 'Erro ao cadastrar.';
          erro.classList.remove('hidden');
        })
        .finally(function () {
          btn.disabled = false;
        });
    });

    function closePin(valor, existente) {
      wrap.classList.add('hidden');
      var st = pinState;
      pinState = null;
      if (st && typeof st.resolve === 'function') {
        st.resolve(valor ? { valor: valor, existente: !!existente } : null);
      }
    }

    wrap._renderParecidos = renderParecidos;
    wrap._nome = nome;
    wrap._pin = pin;
    wrap._erro = erro;
    wrap._tit = document.getElementById('agro-pick-pin-tit');
    return wrap;
  }

  /**
   * @returns {Promise<{valor:string,existente?:boolean}|null>}
   */
  function pedirNova(opts) {
    opts = opts || {};
    var tipo = String(opts.tipo || '').toLowerCase();
    var facetKey = opts.facetKey || facetKeyFromTipo(tipo);
    var modal = ensurePinModal();
    return new Promise(function (resolve) {
      pinState = {
        tipo: tipo,
        facetKey: facetKey,
        urlNova: opts.urlNova || '',
        csrf: opts.csrf || '',
        resolve: resolve,
      };
      modal._tit.textContent = opts.titulo || 'Novo';
      modal._nome.value = String(opts.valorInicial || '').trim();
      modal._pin.value = '';
      modal._erro.classList.add('hidden');
      modal._erro.textContent = '';
      modal._renderParecidos();
      modal.classList.remove('hidden');
      setTimeout(function () {
        try {
          modal._nome.focus();
          modal._nome.select();
        } catch (_) {}
      }, 30);
    });
  }

  function wire(opts) {
    opts = opts || {};
    var inp = typeof opts.input === 'string' ? document.getElementById(opts.input) : opts.input;
    var box = typeof opts.box === 'string' ? document.getElementById(opts.box) : opts.box;
    var facetKey = opts.facetKey || facetKeyFromTipo(opts.tipo);
    var tipo = opts.tipo || '';
    var plusBtn =
      typeof opts.plusBtn === 'string' ? document.getElementById(opts.plusBtn) : opts.plusBtn;
    if (!inp || inp._agroPickWired) return;
    inp._agroPickWired = true;
    inp.dataset.agroPick = facetKey;

    function atualiza() {
      markDirty(inp);
      var exact = findExact(facetKey, inp.value);
      if (exact && norm(exact) === norm(inp.value)) markCommitted(inp, exact);
      if (box) renderBox(box, inp.value, facetKey);
    }

    inp.addEventListener('focus', atualiza);
    inp.addEventListener('input', atualiza);
    inp.addEventListener('blur', function () {
      setTimeout(function () {
        syncFromValue(inp, facetKey);
      }, 120);
    });

    if (box) {
      box.addEventListener('mousedown', function (e) {
        e.preventDefault();
      });
      box.addEventListener('click', function (e) {
        var b = e.target.closest('.agro-pick-opt');
        if (!b) return;
        var t = (b.textContent || '').trim();
        inp.value = t;
        markCommitted(inp, t);
        box.classList.add('hidden');
        box.innerHTML = '';
      });
      document.addEventListener('click', function (e) {
        if (e.target === inp || (box && box.contains(e.target))) return;
        if (plusBtn && (e.target === plusBtn || plusBtn.contains(e.target))) return;
        box.classList.add('hidden');
      });
    }

    if (plusBtn && !plusBtn._agroPickPlus) {
      plusBtn._agroPickPlus = true;
      plusBtn.addEventListener('click', function () {
        pedirNova({
          tipo: tipo,
          facetKey: facetKey,
          titulo: opts.tituloNovo || 'Novo',
          valorInicial: inp.value,
          urlNova: opts.urlNova,
          csrf: opts.csrf,
        }).then(function (res) {
          if (!res || !res.valor) return;
          inp.value = res.valor;
          markCommitted(inp, res.valor);
          appendFacet(facetKey, res.valor);
        });
      });
    }

    // Valor já carregado (edição) conta como válido
    if (String(inp.value || '').trim()) {
      markCommitted(inp, inp.value);
      appendFacet(facetKey, inp.value);
    }
  }

  function setFacetList(key, list) {
    FACETAS[key] = Array.isArray(list) ? list.slice() : [];
  }

  global.AgroPickList = {
    FACETAS: FACETAS,
    norm: norm,
    findExact: findExact,
    findParecidos: findParecidos,
    appendFacet: appendFacet,
    mergeFacetas: mergeFacetas,
    loadFacetas: loadFacetas,
    markCommitted: markCommitted,
    syncFromValue: syncFromValue,
    assertField: assertField,
    wire: wire,
    pedirNova: pedirNova,
    setFacetList: setFacetList,
    facetKeyFromTipo: facetKeyFromTipo,
  };
})(typeof window !== 'undefined' ? window : this);
