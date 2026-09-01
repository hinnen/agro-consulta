/**
 * Atendimento WhatsApp — filas Centro / Vila / espera.
 */
(function () {
  'use strict';

  var loja = 'pendente';
  var convId = 0;
  var convLoja = '';
  var afterId = 0;
  var lastSeenIn = 0;
  var lastUnread = -1;
  var notifyOk = false;

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

  function pedirAviso() {
    try {
      if (!window.Notification || Notification.permission !== 'default') return;
      Notification.requestPermission().then(function (p) {
        notifyOk = p === 'granted';
      });
    } catch (e) {}
  }

  function avisarNova(qtd, titulo) {
    beep();
    if (document.hidden && window.Notification && Notification.permission === 'granted') {
      try {
        new Notification(titulo || 'WhatsApp loja', {
          body: qtd > 1 ? qtd + ' mensagens novas' : 'Mensagem nova',
          silent: true,
        });
      } catch (e) {}
    }
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
    if (p.conectada) {
      pill('ok', p.numero ? p.numero : 'Online');
      if (qrBox) qrBox.classList.add('hidden');
      return;
    }
    if (qrBox) qrBox.classList.remove('hidden');
    var codeEl = $('wa-pair-code');
    if (codeEl) {
      if (p.pairing_code) {
        codeEl.textContent = String(p.pairing_code).replace(/-/g, '').replace(/(.{4})/g, '$1-').replace(/-$/, '');
        codeEl.classList.remove('hidden');
      } else {
        codeEl.classList.add('hidden');
      }
    }
    if (p.qr) {
      pill('warn', p.pairing_code ? 'Código' : 'QR');
      if (img) {
        img.src = p.qr;
        img.classList.remove('hidden');
      }
      return;
    }
    if (img) img.classList.add('hidden');
    pill('warn', p.pairing_code ? 'Código' : p.aviso || 'Ligando');
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
          '" data-loja="' +
          escapeHtml(c.loja || '') +
          '" data-nome="' +
          escapeHtml(rotuloTel(c)) +
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
        var corpo = escapeHtml(m.texto || '');
        var midia = '';
        if (m.midia_url && (m.tipo_midia === 'image' || m.tipo_midia === 'sticker')) {
          midia = '<img class="wa-pic" alt="" src="' + escapeHtml(m.midia_url) + '" />';
        } else if (m.midia_url && m.tipo_midia === 'audio') {
          midia = '<audio class="wa-aud" controls src="' + escapeHtml(m.midia_url) + '"></audio>';
        }
        return (
          '<div class="wa-b ' +
          cls +
          '"><div class="text-[10px] font-black uppercase opacity-70">' +
          escapeHtml(who) +
          ' · ' +
          escapeHtml(m.hora || '') +
          '</div>' +
          midia +
          corpo +
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
      var tot = 0;
      var n = j.nao_lidas || {};
      tot += parseInt(n.pendente || 0, 10) || 0;
      tot += parseInt(n.centro || 0, 10) || 0;
      tot += parseInt(n.vila || 0, 10) || 0;
      if (lastUnread >= 0 && tot > lastUnread) {
        avisarNova(tot - lastUnread, 'WhatsApp loja');
      }
      lastUnread = tot;
    });
  }

  function carregarLista() {
    if ($('wa-busca') && ($('wa-busca').value || '').trim()) return Promise.resolve();
    return fetchJson('/api/atendimento-whatsapp/conversas/?loja=' + encodeURIComponent(loja)).then(
      function (j) {
        if (!j || !j.ok) return;
        pintarLista(j.conversas);
      }
    );
  }

  function pintarXfer() {
    document.querySelectorAll('[data-xfer]').forEach(function (b) {
      var dest = b.getAttribute('data-xfer') || '';
      b.classList.toggle('hidden', !!(convLoja && dest === convLoja));
    });
  }

  function abrirConversa(id) {
    convId = Number(id) || 0;
    afterId = 0;
    var item = document.querySelector('#wa-lista [data-id="' + convId + '"]');
    convLoja = (item && item.getAttribute('data-loja')) || loja || '';
    $('wa-topo-nome').textContent = (item && item.getAttribute('data-nome')) || 'Conversa #' + convId;
    var hist = $('wa-hist');
    if (hist) hist.classList.remove('hidden');
    var del = $('wa-del');
    if (del) del.classList.remove('hidden');
    $('wa-move').classList.remove('hidden');
    $('wa-move').classList.add('flex');
    pintarXfer();
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
        }
      });
    });
  }

  document.querySelectorAll('.wa-tab').forEach(function (b) {
    b.addEventListener('click', function () {
      loja = b.getAttribute('data-loja') || 'pendente';
      convId = 0;
      convLoja = '';
      afterId = 0;
      setTab();
      $('wa-topo-nome').textContent = 'Escolha uma conversa';
      var hist = $('wa-hist');
      if (hist) hist.classList.add('hidden');
      var del = $('wa-del');
      if (del) del.classList.add('hidden');
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

  document.querySelectorAll('[data-xfer]').forEach(function (b) {
    b.addEventListener('click', function () {
      if (!convId) return;
      var dest = b.getAttribute('data-xfer') || '';
      var nome = dest === 'vila' ? 'Vila' : 'Centro';
      if (!window.confirm('Passar este atendimento para a ' + nome + '? O cliente é avisado no Zap.')) return;
      fetchJson('/api/atendimento-whatsapp/transferir/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf() },
        body: JSON.stringify({ conversa_id: convId, loja: dest }),
      }).then(function (j) {
        if (!j || !j.ok) {
          window.alert((j && j.erro) || 'Não passou.');
          return;
        }
        convId = 0;
        convLoja = '';
        afterId = 0;
        $('wa-topo-nome').textContent = 'Escolha uma conversa';
        $('wa-msgs').innerHTML = '';
        $('wa-move').classList.add('hidden');
        var hist = $('wa-hist');
        var del = $('wa-del');
        if (hist) hist.classList.add('hidden');
        if (del) del.classList.add('hidden');
        carregarEstado();
        carregarLista();
      });
    });
  });

  function mostrarBusca(on) {
    var hits = $('wa-busca-hits');
    var lista = $('wa-lista');
    if (!hits || !lista) return;
    hits.classList.toggle('hidden', !on);
    lista.classList.toggle('hidden', !!on);
  }

  function rotuloOrigem(o) {
    if (o === 'cadastro') return 'cadastro';
    if (o === 'zap') return 'zap';
    if (o === 'conversa') return 'já no chat';
    return 'número';
  }

  function irParaConversa(conv) {
    if (!conv || !conv.id) return;
    loja = conv.loja || 'pendente';
    setTab();
    if ($('wa-busca')) $('wa-busca').value = '';
    mostrarBusca(false);
    carregarLista();
    abrirConversa(conv.id);
  }

  function abrirHit(c) {
    if (!c) return;
    if (c.conversa_id) {
      irParaConversa({ id: c.conversa_id, loja: c.loja || 'pendente' });
      return;
    }
    fetchJson('/api/atendimento-whatsapp/abrir/', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf() },
      body: JSON.stringify({ telefone: c.telefone || c.jid || '', nome: c.nome || '' }),
    }).then(function (j) {
      if (!j || !j.ok) {
        window.alert((j && j.erro) || 'Não abriu.');
        return;
      }
      irParaConversa(j.conversa);
    });
  }

  function buscarTopo(tentativa) {
    var q = (($('wa-busca') && $('wa-busca').value) || '').trim();
    var hits = $('wa-busca-hits');
    if (!hits) return;
    if (!q) {
      mostrarBusca(false);
      return;
    }
    mostrarBusca(true);
    fetchJson('/api/atendimento-whatsapp/contatos/?q=' + encodeURIComponent(q)).then(function (j) {
      var rows = (j && j.contatos) || [];
      if (!rows.length) {
        hits.innerHTML = '<p class="p-4 text-sm font-semibold text-slate-400">Nada. Digite o telefone com DDD para abrir.</p>';
      } else {
        hits.innerHTML = rows
          .map(function (c) {
            return (
              '<button type="button" class="wa-item" data-tel="' +
              escapeHtml(c.telefone || '') +
              '" data-nome="' +
              escapeHtml(c.nome || '') +
              '" data-jid="' +
              escapeHtml(c.jid || '') +
              '" data-cid="' +
              String(c.conversa_id || 0) +
              '" data-loja="' +
              escapeHtml(c.loja || '') +
              '"><div class="wa-n">' +
              escapeHtml((c.nome || '') + (c.nome && c.telefone ? ' · ' : '') + (c.telefone || '')) +
              '</div><div class="wa-p">' +
              escapeHtml(rotuloOrigem(c.origem)) +
              '</div></button>'
            );
          })
          .join('');
      }
      var n = tentativa || 0;
      var temZap = rows.some(function (c) {
        return c.origem === 'zap';
      });
      if (q.length >= 2 && n < 2 && !temZap) {
        window.setTimeout(function () {
          buscarTopo(n + 1);
        }, 2200);
      }
    });
  }

  var buscaTimer = 0;
  var inpBusca = $('wa-busca');
  if (inpBusca) {
    inpBusca.addEventListener('input', function () {
      window.clearTimeout(buscaTimer);
      buscaTimer = window.setTimeout(buscarTopo, 220);
    });
  }
  var hitsEl = $('wa-busca-hits');
  if (hitsEl) {
    hitsEl.addEventListener('click', function (ev) {
      var btn = ev.target.closest('[data-tel], [data-jid]');
      if (!btn) return;
      abrirHit({
        telefone: btn.getAttribute('data-tel') || '',
        nome: btn.getAttribute('data-nome') || '',
        jid: btn.getAttribute('data-jid') || '',
        conversa_id: parseInt(btn.getAttribute('data-cid') || '0', 10) || 0,
        loja: btn.getAttribute('data-loja') || '',
      });
    });
  }
  var btnHist = $('wa-hist');
  if (btnHist) {
    btnHist.addEventListener('click', function () {
      if (!convId) return;
      fetchJson('/api/atendimento-whatsapp/historico/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf() },
        body: JSON.stringify({ conversa_id: convId }),
      }).then(function (j) {
        if (!j || !j.ok) {
          window.alert((j && j.erro) || 'Não puxou.');
          return;
        }
        window.setTimeout(function () {
          abrirConversa(convId);
        }, 3500);
      });
    });
  }
  var btnPair = $('wa-pair-ok');
  if (btnPair) {
    btnPair.addEventListener('click', function () {
      var tel = ($('wa-pair-tel') && $('wa-pair-tel').value) || '';
      fetchJson('/api/atendimento-whatsapp/pairing/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf() },
        body: JSON.stringify({ telefone: tel }),
      }).then(function (j) {
        if (!j || !j.ok) {
          window.alert((j && j.erro) || 'Não gerou. Ponte ligada?');
          return;
        }
        window.setTimeout(carregarEstado, 1500);
      });
    });
  }
  var btnDel = $('wa-del');
  if (btnDel) {
    btnDel.addEventListener('click', function () {
      if (!convId) return;
      if (!window.confirm('Apagar esta conversa da lista? (Só no Agro — não apaga no celular.)')) return;
      fetchJson('/api/atendimento-whatsapp/excluir/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf() },
        body: JSON.stringify({ conversa_id: convId }),
      }).then(function (j) {
        if (!j || !j.ok) {
          window.alert((j && j.erro) || 'Não apagou.');
          return;
        }
        convId = 0;
        afterId = 0;
        $('wa-topo-nome').textContent = 'Escolha uma conversa';
        $('wa-msgs').innerHTML = '';
        $('wa-move').classList.add('hidden');
        btnDel.classList.add('hidden');
        if (btnHist) btnHist.classList.add('hidden');
        carregarLista();
      });
    });
  }

  setTab();
  pedirAviso();
  document.addEventListener('click', pedirAviso, { once: true });
  carregarEstado();
  carregarLista();
  setInterval(function () {
    carregarEstado();
    carregarLista();
    pollMsgs();
  }, 2500);
})();
