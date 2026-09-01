/**
 * Atendimento WhatsApp — filas Centro / Vila / espera.
 */
(function () {
  'use strict';

  var loja = 'pendente';
  var convId = 0;
  var afterId = 0;
  var lastSeenIn = 0;

  function csrf() {
    var inp = document.querySelector('[name=csrfmiddlewaretoken]');
    if (inp && inp.value) return inp.value;
    var m = document.cookie.match(/csrftoken=([^;]+)/);
    return m ? decodeURIComponent(m[1]) : '';
  }

  function $(id) {
    return document.getElementById(id);
  }

  function beep() {
    try {
      var ctx = new (window.AudioContext || window.webkitAudioContext)();
      var o = ctx.createOscillator();
      var g = ctx.createGain();
      o.type = 'sine';
      o.frequency.value = 880;
      g.gain.value = 0.05;
      o.connect(g);
      g.connect(ctx.destination);
      o.start();
      setTimeout(function () {
        o.stop();
        ctx.close();
      }, 140);
    } catch (e) {}
  }

  function rotuloTel(c) {
    return (c.nome ? c.nome + ' · ' : '') + (c.telefone || c.jid || '');
  }

  function fetchJson(url, opt) {
    return fetch(url, opt).then(function (r) {
      return r.json().catch(function () {
        return { ok: false, erro: 'Falha de rede' };
      });
    });
  }

  function pintarBadges(n) {
    n = n || {};
    ['pendente', 'centro', 'vila'].forEach(function (k) {
      var el = document.querySelector('[data-badge="' + k + '"]');
      if (!el) return;
      var v = parseInt(n[k] || 0, 10) || 0;
      el.textContent = String(v);
      el.classList.toggle('is-zero', v === 0);
    });
  }

  function pintarStatus(p) {
    var box = $('wa-status');
    var dot = $('wa-dot');
    var qrBox = $('wa-qr-box');
    var img = $('wa-qr-img');
    if (!box) return;
    p = p || {};
    function pill(kind, txt, title) {
      box.textContent = txt;
      box.className = 'wa-pill' + (kind ? ' ' + kind : '');
      if (title) box.title = title;
      if (dot) {
        dot.className = 'wa-dot ' + (kind === 'ok' ? 'on' : kind === 'warn' ? 'wait' : 'off');
      }
    }
    if (!p.ponte_viva) {
      pill('bad', 'Off', 'Rode whatsapp_atendimento\\iniciar.bat e deixe a janela aberta');
      if (qrBox) qrBox.classList.add('hidden');
      return;
    }
    if (p.qr) {
      pill('warn', 'QR');
      if (qrBox) qrBox.classList.remove('hidden');
      if (img) img.src = p.qr;
      return;
    }
    if (qrBox) qrBox.classList.add('hidden');
    if (p.conectada) {
      pill('ok', p.numero ? p.numero : 'Online');
      return;
    }
    pill('', p.aviso || 'Ligando');
  }

  function pintarLista(rows) {
    var el = $('wa-lista');
    if (!el) return;
    if (!rows || !rows.length) {
      el.innerHTML = '<p class="p-4 text-sm font-semibold text-slate-400">Vazio</p>';
      return;
    }
    el.innerHTML = rows
      .map(function (c) {
        var on = Number(c.id) === convId ? ' is-on' : '';
        var badge = c.nao_lidas ? ' · ' + c.nao_lidas + ' nova' : '';
        return (
          '<button type="button" class="wa-item' +
          on +
          '" data-id="' +
          c.id +
          '"><div class="wa-n">' +
          escapeHtml(rotuloTel(c)) +
          badge +
          '</div><div class="wa-p">' +
          escapeHtml(c.ultima_preview || '') +
          ' · ' +
          escapeHtml(c.hora || '') +
          '</div></button>'
        );
      })
      .join('');
  }

  function escapeHtml(s) {
    return String(s || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
  }

  function pintarMsgs(rows, append) {
    var el = $('wa-msgs');
    if (!el) return;
    var html = (rows || [])
      .map(function (m) {
        var cls = m.direcao === 'out' ? 'out' : m.direcao === 'bot' ? 'bot' : 'in';
        var who =
          m.direcao === 'out' ? m.autor || 'Loja' : m.direcao === 'bot' ? 'Bot' : 'Cliente';
        return (
          '<div class="wa-b ' +
          cls +
          '"><div class="text-[10px] font-black uppercase opacity-70">' +
          escapeHtml(who) +
          ' · ' +
          escapeHtml(m.hora || '') +
          '</div>' +
          escapeHtml(m.texto) +
          '</div>'
        );
      })
      .join('');
    if (append) el.insertAdjacentHTML('beforeend', html);
    else el.innerHTML = html || '<p class="text-sm text-slate-500">Sem mensagens.</p>';
    el.scrollTop = el.scrollHeight;
  }

  function setTab() {
    document.querySelectorAll('.wa-tab').forEach(function (b) {
      b.classList.toggle('is-on', b.getAttribute('data-loja') === loja);
    });
  }

  function carregarEstado() {
    return fetchJson('/api/atendimento-whatsapp/estado/').then(function (j) {
      if (!j || !j.ok) return;
      pintarStatus(j.ponte);
      pintarBadges(j.nao_lidas);
    });
  }

  function carregarLista() {
    return fetchJson('/api/atendimento-whatsapp/conversas/?loja=' + encodeURIComponent(loja)).then(
      function (j) {
        if (!j || !j.ok) return;
        pintarLista(j.conversas);
      }
    );
  }

  function abrirConversa(id) {
    convId = Number(id) || 0;
    afterId = 0;
    $('wa-topo-conv').textContent = 'Conversa #' + convId;
    $('wa-move').classList.remove('hidden');
    $('wa-move').classList.add('flex');
    fetchJson('/api/atendimento-whatsapp/mensagens/?conversa_id=' + convId).then(function (j) {
      var rows = (j && j.mensagens) || [];
      pintarMsgs(rows, false);
      if (rows.length) afterId = rows[rows.length - 1].id;
      rows.forEach(function (m) {
        if (m.direcao === 'in' && m.id > lastSeenIn) lastSeenIn = m.id;
      });
    });
    fetch('/api/atendimento-whatsapp/marcar-lida/', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf() },
      body: JSON.stringify({ conversa_id: convId }),
    }).then(function () {
      carregarEstado();
      carregarLista();
    });
  }

  function pollMsgs() {
    if (!convId) return Promise.resolve();
    return fetchJson(
      '/api/atendimento-whatsapp/mensagens/?conversa_id=' + convId + '&after_id=' + afterId
    ).then(function (j) {
      var rows = (j && j.mensagens) || [];
      if (!rows.length) return;
      pintarMsgs(rows, true);
      rows.forEach(function (m) {
        afterId = Math.max(afterId, m.id);
        if (m.direcao === 'in' && m.id > lastSeenIn) {
          lastSeenIn = m.id;
          beep();
        }
      });
    });
  }

  document.querySelectorAll('.wa-tab').forEach(function (b) {
    b.addEventListener('click', function () {
      loja = b.getAttribute('data-loja') || 'pendente';
      convId = 0;
      afterId = 0;
      setTab();
      $('wa-topo-conv').textContent = 'Escolha uma conversa';
      $('wa-msgs').innerHTML = '';
      $('wa-move').classList.add('hidden');
      carregarLista();
    });
  });

  $('wa-lista').addEventListener('click', function (ev) {
    var btn = ev.target.closest('[data-id]');
    if (!btn) return;
    abrirConversa(btn.getAttribute('data-id'));
  });

  $('wa-form').addEventListener('submit', function (ev) {
    ev.preventDefault();
    var inp = $('wa-input');
    var t = (inp.value || '').trim();
    if (!t || !convId) return;
    inp.value = '';
    fetchJson('/api/atendimento-whatsapp/enviar/', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf() },
      body: JSON.stringify({ conversa_id: convId, texto: t }),
    }).then(function (j) {
      if (!j || !j.ok) {
        window.alert((j && j.erro) || 'Não enviou.');
        inp.value = t;
        return;
      }
      pollMsgs();
    });
  });

  document.querySelectorAll('[data-move]').forEach(function (b) {
    b.addEventListener('click', function () {
      if (!convId) return;
      fetchJson('/api/atendimento-whatsapp/definir-loja/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf() },
        body: JSON.stringify({ conversa_id: convId, loja: b.getAttribute('data-move') }),
      }).then(function (j) {
        if (!j || !j.ok) {
          window.alert((j && j.erro) || 'Não moveu.');
          return;
        }
        carregarLista();
      });
    });
  });

  setTab();
  carregarEstado();
  carregarLista();
  setInterval(function () {
    carregarEstado();
    carregarLista();
    pollMsgs();
  }, 2500);
})();
