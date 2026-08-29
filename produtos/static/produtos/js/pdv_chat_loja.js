/**
 * PDV — chat em grupo (todos os PCs Centro + Vila).
 * Polling + bip quando chega mensagem de outro aparelho.
 */
(function () {
  'use strict';

  var DEVICE_ID_KEY = 'agro_device_id_v1';
  var SEEN_KEY = 'agro_chat_loja_seen_id_v1';
  var POLL_MS = 4000;
  var POLL_OPEN_MS = 2500;

  function boot() {
    var el =
      document.getElementById('agro-pdv-wizard-bootstrap') ||
      document.getElementById('agro-pdv-bootstrap');
    try {
      return el ? JSON.parse(el.textContent || '{}') : {};
    } catch (e) {
      return {};
    }
  }

  var bootstrap = boot();
  var urls = bootstrap.urls || {};
  var overlay = document.getElementById('pdv-chat-loja-overlay');
  if (!overlay || !urls.apiPdvChatLojaLista) return;

  var dom = {
    btnOpen: document.getElementById('pdv-chat-loja-fab'),
    btnCount: document.getElementById('pdv-chat-loja-count'),
    fechar: document.getElementById('pdv-chat-loja-fechar'),
    msgs: document.getElementById('pdv-chat-loja-msgs'),
    form: document.getElementById('pdv-chat-loja-form'),
    input: document.getElementById('pdv-chat-loja-input'),
    enviar: document.getElementById('pdv-chat-loja-enviar'),
    status: document.getElementById('pdv-chat-loja-status'),
  };

  var lastId = 0;
  var seenId = 0;
  var pollTimer = null;
  var busy = false;
  var knownIds = {};
  var cacheMsgs = [];

  function csrf() {
    var c = document.cookie.match(/csrftoken=([^;]+)/);
    return (c && c[1]) || bootstrap.csrfToken || '';
  }

  function deviceId() {
    try {
      var id = localStorage.getItem(DEVICE_ID_KEY);
      if (id && id.length >= 8) return id.slice(0, 64);
      id =
        'd' +
        Math.random().toString(36).slice(2) +
        Date.now().toString(36);
      localStorage.setItem(DEVICE_ID_KEY, id);
      return id;
    } catch (e) {
      return '';
    }
  }

  function loadSeen() {
    try {
      seenId = Math.max(0, parseInt(localStorage.getItem(SEEN_KEY) || '0', 10) || 0);
    } catch (e) {
      seenId = 0;
    }
  }

  function saveSeen(id) {
    seenId = Math.max(seenId, Number(id) || 0);
    try {
      localStorage.setItem(SEEN_KEY, String(seenId));
    } catch (e) {}
  }

  function escapeHtml(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function setStatus(msg) {
    if (!dom.status) return;
    if (!msg) {
      dom.status.classList.add('hidden');
      dom.status.textContent = '';
      return;
    }
    dom.status.textContent = msg;
    dom.status.classList.remove('hidden');
  }

  function isOpen() {
    return overlay && overlay.classList.contains('is-open');
  }

  function clBeep() {
    try {
      var Ctx = window.AudioContext || window.webkitAudioContext;
      if (!Ctx) return;
      var ctx = new Ctx();
      var o = ctx.createOscillator();
      var g = ctx.createGain();
      o.connect(g);
      g.connect(ctx.destination);
      o.type = 'sine';
      o.frequency.value = 740;
      g.gain.value = 0.1;
      o.start();
      o.stop(ctx.currentTime + 0.16);
      setTimeout(function () {
        var o2 = ctx.createOscillator();
        var g2 = ctx.createGain();
        o2.connect(g2);
        g2.connect(ctx.destination);
        o2.type = 'sine';
        o2.frequency.value = 980;
        g2.gain.value = 0.1;
        o2.start();
        o2.stop(ctx.currentTime + 0.2);
      }, 170);
    } catch (e) {}
  }

  function syncBadge(n) {
    var count = Math.max(0, Number(n) || 0);
    if (dom.btnCount) {
      if (count > 0) {
        dom.btnCount.textContent = count > 99 ? '99+' : String(count);
        dom.btnCount.classList.remove('hidden');
      } else {
        dom.btnCount.classList.add('hidden');
      }
    }
    if (dom.btnOpen) {
      if (count > 0) dom.btnOpen.classList.add('is-alerta');
      else dom.btnOpen.classList.remove('is-alerta');
    }
  }

  function unreadCount() {
    return Math.max(0, lastId - seenId);
  }

  function bubbleHtml(m) {
    var mine = m.device_id && m.device_id === deviceId();
    var meta =
      escapeHtml(m.autor || '?') +
      ' · ' +
      escapeHtml(m.origem || '') +
      ' · ' +
      escapeHtml(m.hora || '');
    return (
      '<div class="cl-row' +
      (mine ? ' cl-row--eu' : '') +
      '" data-id="' +
      escapeHtml(m.id) +
      '">' +
      '<div class="cl-bubble">' +
      '<div class="cl-meta">' +
      meta +
      '</div>' +
      '<div class="cl-text">' +
      escapeHtml(m.texto) +
      '</div></div></div>'
    );
  }

  function renderAll(list) {
    if (!dom.msgs) return;
    if (!list || !list.length) {
      dom.msgs.innerHTML = '<p class="cl-empty">Nenhuma mensagem ainda. Digite aí embaixo.</p>';
      return;
    }
    var html = '';
    for (var i = 0; i < list.length; i++) html += bubbleHtml(list[i]);
    dom.msgs.innerHTML = html;
    dom.msgs.scrollTop = dom.msgs.scrollHeight;
  }

  function appendNew(list, playSound) {
    if (!list || !list.length) return;
    var hadEmpty = dom.msgs && dom.msgs.querySelector('.cl-empty');
    if (hadEmpty) dom.msgs.innerHTML = '';
    var sound = false;
    var myDev = deviceId();
    for (var i = 0; i < list.length; i++) {
      var m = list[i];
      if (!m || !m.id || knownIds[m.id]) continue;
      knownIds[m.id] = 1;
      lastId = Math.max(lastId, Number(m.id) || 0);
      if (dom.msgs) {
        dom.msgs.insertAdjacentHTML('beforeend', bubbleHtml(m));
        dom.msgs.scrollTop = dom.msgs.scrollHeight;
      }
      if (playSound && m.device_id !== myDev) sound = true;
      cacheMsgs.push(m);
    }
    if (sound) clBeep();
    if (isOpen()) saveSeen(lastId);
    syncBadge(unreadCount());
  }

  function fetchLista(afterId, cb) {
    var base = urls.apiPdvChatLojaLista;
    var q = afterId > 0 ? '?after_id=' + afterId + '&limit=50' : '?limit=80';
    fetch(base + q, { credentials: 'same-origin', headers: { Accept: 'application/json' } })
      .then(function (r) {
        return r.json();
      })
      .then(function (data) {
        if (!data || !data.ok) {
          if (cb) cb(null, (data && data.erro) || 'Falha ao carregar');
          return;
        }
        if (cb) cb(data.mensagens || [], null);
      })
      .catch(function () {
        if (cb) cb(null, 'Sem rede');
      });
  }

  function pollOnce(playSound) {
    fetchLista(lastId > 0 ? lastId : 0, function (msgs, err) {
      if (err || !msgs) return;
      if (lastId === 0) {
        knownIds = {};
        cacheMsgs = msgs.slice();
        for (var i = 0; i < msgs.length; i++) {
          if (msgs[i] && msgs[i].id) {
            knownIds[msgs[i].id] = 1;
            lastId = Math.max(lastId, Number(msgs[i].id) || 0);
          }
        }
        if (isOpen()) renderAll(cacheMsgs);
        if (!seenId && lastId) saveSeen(lastId);
        syncBadge(unreadCount());
        return;
      }
      appendNew(msgs, playSound !== false);
    });
  }

  function schedulePoll() {
    if (pollTimer) clearInterval(pollTimer);
    var ms = isOpen() ? POLL_OPEN_MS : POLL_MS;
    pollTimer = setInterval(function () {
      pollOnce(true);
    }, ms);
  }

  function abrir() {
    overlay.classList.add('is-open');
    if (dom.btnOpen) dom.btnOpen.classList.add('is-open');
    setStatus('');
    saveSeen(lastId);
    syncBadge(0);
    if (cacheMsgs.length) {
      renderAll(cacheMsgs);
      saveSeen(lastId);
      syncBadge(0);
      pollOnce(false);
    } else {
      fetchLista(0, function (msgs, err) {
        if (err) {
          setStatus(err);
          renderAll([]);
          return;
        }
        knownIds = {};
        lastId = 0;
        cacheMsgs = (msgs || []).slice();
        for (var i = 0; i < cacheMsgs.length; i++) {
          if (cacheMsgs[i] && cacheMsgs[i].id) {
            knownIds[cacheMsgs[i].id] = 1;
            lastId = Math.max(lastId, Number(cacheMsgs[i].id) || 0);
          }
        }
        renderAll(cacheMsgs);
        saveSeen(lastId);
        syncBadge(0);
      });
    }
    schedulePoll();
    setTimeout(function () {
      if (dom.input) dom.input.focus();
    }, 50);
  }

  function fechar() {
    overlay.classList.remove('is-open');
    if (dom.btnOpen) dom.btnOpen.classList.remove('is-open');
    saveSeen(lastId);
    syncBadge(unreadCount());
    schedulePoll();
  }

  function enviar(ev) {
    if (ev) ev.preventDefault();
    if (busy) return;
    var texto = (dom.input && dom.input.value) || '';
    texto = String(texto).trim();
    if (!texto) return;
    busy = true;
    if (dom.enviar) dom.enviar.disabled = true;
    setStatus('');
    fetch(urls.apiPdvChatLojaEnviar, {
      method: 'POST',
      credentials: 'same-origin',
      headers: {
        'Content-Type': 'application/json',
        'X-CSRFToken': csrf(),
        Accept: 'application/json',
      },
      body: JSON.stringify({ texto: texto, device_id: deviceId() }),
    })
      .then(function (r) {
        return r.json();
      })
      .then(function (data) {
        busy = false;
        if (dom.enviar) dom.enviar.disabled = false;
        if (!data || !data.ok) {
          setStatus((data && data.erro) || 'Não enviou');
          return;
        }
        if (dom.input) dom.input.value = '';
        appendNew([data.mensagem], false);
        saveSeen(lastId);
        syncBadge(0);
        if (dom.input) dom.input.focus();
      })
      .catch(function () {
        busy = false;
        if (dom.enviar) dom.enviar.disabled = false;
        setStatus('Sem rede');
      });
  }

  if (dom.btnOpen) {
    dom.btnOpen.addEventListener('click', function (e) {
      e.stopPropagation();
      if (isOpen()) fechar();
      else abrir();
    });
  }
  if (dom.fechar) dom.fechar.addEventListener('click', fechar);
  document.addEventListener('mousedown', function (e) {
    if (!isOpen()) return;
    var dock = document.getElementById('pdv-chat-loja-dock');
    if (dock && !dock.contains(e.target)) fechar();
  });
  if (dom.form) dom.form.addEventListener('submit', enviar);
  document.addEventListener('keydown', function (e) {
    if (e.key === 'Escape' && isOpen()) {
      e.preventDefault();
      fechar();
    }
  });

  loadSeen();
  pollOnce(false);
  schedulePoll();
})();
