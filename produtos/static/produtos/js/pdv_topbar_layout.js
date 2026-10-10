/**
 * PDV topbar — layout quente/frio (Postgres · todos os PCs).
 */
(function () {
  'use strict';

  var URL_LAYOUT = '/api/pdv/topbar-layout/';
  var quenteHost = document.getElementById('pdv-topbar-quente');
  var actionsHost = document.getElementById('pdv-topbar-actions');
  var frioPanel = document.getElementById('pdv-topbar-mais-panel');
  var maisWrap = document.getElementById('pdv-topbar-mais');
  var orgBtn = document.getElementById('pdv-topbar-organizar-btn');
  var overlay = document.getElementById('pdv-topbar-organizar-overlay');
  var listaEl = document.getElementById('pdv-topbar-organizar-lista');
  var erroEl = document.getElementById('pdv-topbar-organizar-erro');
  var btnSalvar = document.getElementById('pdv-topbar-organizar-salvar');
  var btnCancelar = document.getElementById('pdv-topbar-organizar-cancelar');
  if (!quenteHost || !actionsHost || !frioPanel || !orgBtn || !overlay) return;

  /* Overlay não pode ficar dentro de outro modal hidden (ex.: cadastro rápido). */
  if (overlay.parentElement !== document.body) {
    document.body.appendChild(overlay);
  }

  var draft = { quente: [], frio: [], labels: {} };
  var chip = document.getElementById('gm-sspin-operador-chip');

  function csrfToken() {
    try {
      var boot = document.getElementById('agro-pdv-bootstrap');
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

  function nodeFor(key) {
    return document.querySelector('[data-pdv-topbar-node="' + key + '"]');
  }

  function placeBeforeMais(el) {
    if (!el || !maisWrap) return;
    actionsHost.insertBefore(el, maisWrap);
  }

  function placeInQuente(el) {
    if (!el) return;
    quenteHost.appendChild(el);
  }

  function placeInFrio(el) {
    if (!el) return;
    frioPanel.insertBefore(el, orgBtn);
  }

  function applyLayout(quente, frio) {
    var q = Array.isArray(quente) ? quente.slice() : [];
    var f = Array.isArray(frio) ? frio.slice() : [];
    q.forEach(function (key) {
      var el = nodeFor(key);
      if (!el) return;
      if (key === 'nova_venda') placeBeforeMais(el);
      else placeInQuente(el);
    });
    f.forEach(function (key) {
      var el = nodeFor(key);
      if (!el) return;
      placeInFrio(el);
    });
    if (chip && maisWrap && chip.parentNode === actionsHost) {
      actionsHost.insertBefore(chip, maisWrap);
    }
    frioPanel.appendChild(orgBtn);
  }

  function loadLayout() {
    fetch(URL_LAYOUT, { credentials: 'same-origin' })
      .then(function (r) {
        return r.json();
      })
      .then(function (data) {
        if (!data || !data.ok) return;
        draft.labels = data.labels || {};
        draft.quente = data.quente || [];
        draft.frio = data.frio || [];
        applyLayout(draft.quente, draft.frio);
      })
      .catch(function () {});
  }

  function setErro(msg) {
    if (!erroEl) return;
    if (msg) {
      erroEl.textContent = msg;
      erroEl.classList.remove('hidden');
    } else {
      erroEl.textContent = '';
      erroEl.classList.add('hidden');
    }
  }

  function renderLista() {
    if (!listaEl) return;
    var labels = draft.labels || {};
    var zonas = {};
    (draft.quente || []).forEach(function (k) {
      zonas[k] = 'quente';
    });
    (draft.frio || []).forEach(function (k) {
      zonas[k] = 'frio';
    });
    var keys = (draft.quente || []).concat(draft.frio || []);
    listaEl.innerHTML = '';
    keys.forEach(function (key) {
      var row = document.createElement('div');
      row.className = 'pdv-topbar-organizar-row';
      row.setAttribute('data-key', key);
      var nome = document.createElement('span');
      nome.textContent = labels[key] || key;
      var tog = document.createElement('div');
      tog.className = 'pdv-topbar-organizar-toggle';
      ['quente', 'frio'].forEach(function (z) {
        var b = document.createElement('button');
        b.type = 'button';
        b.setAttribute('data-zona', z);
        b.textContent = z === 'quente' ? 'Quente' : 'Frio';
        if ((zonas[key] || 'frio') === z) b.classList.add('is-on');
        b.addEventListener('click', function () {
          tog.querySelectorAll('button').forEach(function (x) {
            x.classList.remove('is-on');
          });
          b.classList.add('is-on');
        });
        tog.appendChild(b);
      });
      row.appendChild(nome);
      row.appendChild(tog);
      listaEl.appendChild(row);
    });
  }

  function openOrganizar() {
    setErro('');
    function show() {
      renderLista();
      overlay.classList.add('is-open');
      overlay.setAttribute('aria-hidden', 'false');
    }
    if ((draft.quente && draft.quente.length) || (draft.frio && draft.frio.length)) {
      show();
      return;
    }
    fetch(URL_LAYOUT, { credentials: 'same-origin' })
      .then(function (r) {
        return r.json();
      })
      .then(function (data) {
        if (data && data.ok) {
          draft.labels = data.labels || {};
          draft.quente = data.quente || [];
          draft.frio = data.frio || [];
        }
        show();
      })
      .catch(function () {
        show();
      });
  }

  function closeOrganizar() {
    overlay.classList.remove('is-open');
    overlay.setAttribute('aria-hidden', 'true');
  }

  function coletarDraft() {
    var q = [];
    var f = [];
    if (!listaEl) return { quente: q, frio: f };
    listaEl.querySelectorAll('.pdv-topbar-organizar-row').forEach(function (row) {
      var key = row.getAttribute('data-key');
      var on = row.querySelector('button.is-on');
      var z = on ? on.getAttribute('data-zona') : 'frio';
      if (z === 'quente') q.push(key);
      else f.push(key);
    });
    return { quente: q, frio: f };
  }

  function salvarAgora() {
    var body = coletarDraft();
    if (!body.quente.length) {
      setErro('Deixe pelo menos 1 atalho no Quente.');
      return;
    }
    setErro('');
    btnSalvar.disabled = true;
    fetch(URL_LAYOUT, {
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
          return { status: r.status, j: j };
        });
      })
      .then(function (pack) {
        btnSalvar.disabled = false;
        if (!pack.j || !pack.j.ok) {
          setErro((pack.j && pack.j.erro) || 'Não gravou. Tente de novo.');
          return;
        }
        draft.quente = pack.j.quente || body.quente;
        draft.frio = pack.j.frio || body.frio;
        if (pack.j.labels) draft.labels = pack.j.labels;
        applyLayout(draft.quente, draft.frio);
        closeOrganizar();
      })
      .catch(function () {
        btnSalvar.disabled = false;
        setErro('Falha de rede. Tente de novo.');
      });
  }

  function salvarComPin() {
    var run = function () {
      salvarAgora();
    };
    if (typeof window.gmSspinGarantirOperador === 'function') {
      window.gmSspinGarantirOperador(run, { titulo: 'PIN para organizar atalhos' });
    } else {
      run();
    }
  }

  orgBtn.addEventListener('click', function (ev) {
    ev.preventDefault();
    ev.stopPropagation();
    openOrganizar();
  });

  if (btnCancelar) {
    btnCancelar.addEventListener('click', function () {
      closeOrganizar();
    });
  }
  if (btnSalvar) {
    btnSalvar.addEventListener('click', function () {
      salvarComPin();
    });
  }

  overlay.addEventListener('click', function (ev) {
    if (ev.target === overlay) closeOrganizar();
  });

  document.addEventListener('keydown', function (ev) {
    if (ev.key === 'Escape' && overlay.classList.contains('is-open')) {
      closeOrganizar();
    }
  });

  loadLayout();
})();
