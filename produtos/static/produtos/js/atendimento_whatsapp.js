/**
 * Atendimento WhatsApp — filas Centro / Vila / espera.
 */
(function () {
  'use strict';

  var TAB_KEY = 'agro_wa_loja_tab';
  var loja = 'pendente';
  var separarLojas = true;
  try {
    var tabSalva = localStorage.getItem(TAB_KEY);
    if (tabSalva === 'centro' || tabSalva === 'vila' || tabSalva === 'pendente') loja = tabSalva;
  } catch (e) {}
  var convId = 0;
  var convLoja = '';
  var afterId = 0;
  var lastSeenIn = 0;
  var lastUnread = -1;
  var notifyOk = false;
  var ehCel = !!(document.body && document.body.classList.contains('wa-cel'));

  function telaCel(chat) {
    if (!ehCel) return;
    document.body.classList.toggle('is-chat', !!chat);
  }

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
      var dica =
        loja === 'pendente'
          ? 'Fila vazia. Quem já escolheu loja está em Centro ou Vila. Religar o Zap não apaga conversa.'
          : 'Nenhuma conversa nesta aba.';
      el.innerHTML = '<p class="p-4 text-sm font-semibold text-slate-400">' + dica + '</p>';
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
        var corpoTxt = m.texto || '';
        if (corpoTxt === '[imagem]' || corpoTxt === '[áudio]') corpoTxt = '';
        var corpo = escapeHtml(corpoTxt);
        var midia = '';
        if (m.midia_url && (m.tipo_midia === 'image' || m.tipo_midia === 'sticker')) {
          midia = '<img class="wa-pic" alt="" src="' + escapeHtml(m.midia_url) + '" />';
        } else if (m.midia_url && m.tipo_midia === 'audio') {
          midia = '<audio class="wa-aud" controls src="' + escapeHtml(m.midia_url) + '"></audio>';
        } else if (m.tipo_midia === 'image' || m.tipo_midia === 'sticker') {
          midia = '<span class="text-xs text-slate-500">Foto (ainda baixando)</span>';
        } else if (m.tipo_midia === 'audio') {
          midia = '<span class="text-xs text-slate-500">Áudio (ainda baixando)</span>';
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

  function aplicarSeparacao(on) {
    separarLojas = on !== false;
    document.querySelectorAll('.wa-tabs').forEach(function (el) {
      el.classList.toggle('hidden', !separarLojas);
    });
    if (!separarLojas) {
      loja = 'todas';
      var mv = $('wa-move');
      if (mv && !convId) mv.classList.add('hidden');
    } else if (loja === 'todas') {
      loja = 'pendente';
    }
    setTab();
  }

  function carregarEstado() {
    return fetchJson('/api/atendimento-whatsapp/estado/').then(function (j) {
      if (!j || !j.ok) return;
      pintarStatus(j.ponte);
      pintarBadges(j.nao_lidas);
      if (j.bot && typeof j.bot.separar_lojas === 'boolean') {
        var mudou = j.bot.separar_lojas !== separarLojas || (!j.bot.separar_lojas && loja !== 'todas');
        aplicarSeparacao(j.bot.separar_lojas);
        if (mudou) carregarLista();
      }
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
    if (separarLojas) {
      $('wa-move').classList.remove('hidden');
      $('wa-move').classList.add('flex');
      pintarXfer();
    } else {
      $('wa-move').classList.add('hidden');
    }
    telaCel(true);
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
      try {
        localStorage.setItem(TAB_KEY, loja);
      } catch (e) {}
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
      telaCel(false);
      carregarLista();
    });
  });

  $('wa-lista').addEventListener('click', function (ev) {
    var btn = ev.target.closest('[data-id]');
    if (!btn) return;
    abrirConversa(btn.getAttribute('data-id'));
  });

  var MIDIA_TETO = 3 * 1024 * 1024;
  var recStream = null;
  var recChunks = [];
  var recObj = null;

  function enviarPayload(payload, textoVolta) {
    var btn = $('wa-send');
    if (!convId || (btn && btn.disabled)) return;
    if (btn) btn.disabled = true;
    fetchJson('/api/atendimento-whatsapp/enviar/', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf() },
      body: JSON.stringify(payload),
    })
      .then(function (j) {
        if (!j || !j.ok) {
          window.alert((j && j.erro) || 'Não enviou.');
          if (textoVolta && $('wa-input')) $('wa-input').value = textoVolta;
          return;
        }
        pollMsgs();
      })
      .catch(function () {
        window.alert('Falha de rede ao enviar.');
        if (textoVolta && $('wa-input')) $('wa-input').value = textoVolta;
      })
      .finally(function () {
        if (btn) btn.disabled = false;
        atualizarBarra();
      });
  }

  function arquivoParaB64(file, ok) {
    if (!file) return;
    if (file.size > MIDIA_TETO) {
      window.alert('Foto ou áudio grande demais (máximo 3 MB). Mande um arquivo menor.');
      return;
    }
    var r = new FileReader();
    r.onload = function () {
      var s = String(r.result || '');
      var b64 = s.indexOf(',') >= 0 ? s.split(',')[1] : s;
      ok(b64, file.type || '', file.name || '');
    };
    r.readAsDataURL(file);
  }

  var recTimer = 0;
  var recSecs = 0;

  function fmtRec(s) {
    var m = Math.floor(s / 60);
    var r = s % 60;
    return m + ':' + (r < 10 ? '0' : '') + r;
  }

  function ligarRecUi(on) {
    var mic = $('wa-mic');
    var bar = $('wa-rec-bar');
    var t = $('wa-rec-t');
    var form = $('wa-form');
    if (form) form.classList.toggle('is-rec', !!on);
    if (mic) mic.classList.toggle('is-rec', !!on);
    if (bar) bar.classList.toggle('hidden', !on);
    if (recTimer) {
      clearInterval(recTimer);
      recTimer = 0;
    }
    recSecs = 0;
    if (on) {
      if (t) t.textContent = '0:00';
      recTimer = setInterval(function () {
        recSecs += 1;
        if (t) t.textContent = fmtRec(recSecs);
      }, 1000);
    }
    atualizarBarra();
  }

  function atualizarBarra() {
    var inp = $('wa-input');
    var send = $('wa-send');
    var mic = $('wa-mic');
    var rec = !!recObj;
    var tem = !!(inp && String(inp.value || '').trim());
    var temRec = typeof window.MediaRecorder === 'function' && navigator.mediaDevices && navigator.mediaDevices.getUserMedia;
    if (send) send.classList.toggle('hidden', rec || !tem);
    if (mic && temRec) mic.classList.toggle('hidden', rec ? false : tem);
  }

  function pararRec(enviar) {
    var cancel = $('wa-mic-x');
    ligarRecUi(false);
    if (cancel) cancel.classList.add('hidden');
    if (!recObj) {
      if (recStream) recStream.getTracks().forEach(function (t) { t.stop(); });
      recStream = null;
      return;
    }
    recObj.onstop = function () {
      if (recStream) recStream.getTracks().forEach(function (t) { t.stop(); });
      recStream = null;
      var mime = (recObj && recObj.mimeType) || 'audio/webm';
      recObj = null;
      if (!enviar) {
        recChunks = [];
        return;
      }
      var blob = new Blob(recChunks, { type: mime });
      recChunks = [];
      if (!blob.size) {
        window.alert('Não gravou áudio.');
        return;
      }
      if (blob.size > MIDIA_TETO) {
        window.alert('Áudio grande demais (máximo 3 MB). Grave mais curto.');
        return;
      }
      var fr = new FileReader();
      fr.onload = function () {
        var s = String(fr.result || '');
        var b64 = s.indexOf(',') >= 0 ? s.split(',')[1] : s;
        enviarPayload({
          conversa_id: convId,
          texto: '',
          tipo_midia: 'audio',
          midia_b64: b64,
          mime: mime,
          nome_arquivo: 'audio.webm',
        });
      };
      fr.readAsDataURL(blob);
    };
    try {
      recObj.stop();
    } catch (_) {
      recObj = null;
    }
  }

  function ligarMicUi() {
    var temRec = typeof window.MediaRecorder === 'function' && navigator.mediaDevices && navigator.mediaDevices.getUserMedia;
    var mic = $('wa-mic');
    var arq = $('wa-audio-arq');
    if (temRec) {
      if (mic) mic.classList.remove('hidden');
      if (arq) arq.classList.add('hidden');
    } else {
      if (mic) mic.classList.add('hidden');
      if (arq) arq.classList.remove('hidden');
    }
  }
  ligarMicUi();
  atualizarBarra();
  var inpBarra = $('wa-input');
  if (inpBarra) {
    inpBarra.addEventListener('input', atualizarBarra);
  }

  var fotoBtn = $('wa-foto');
  var fotoInp = $('wa-foto-inp');
  if (fotoBtn && fotoInp) {
    fotoBtn.addEventListener('click', function () {
      if (!convId) {
        window.alert('Abra uma conversa primeiro.');
        return;
      }
      fotoInp.value = '';
      fotoInp.click();
    });
    fotoInp.addEventListener('change', function () {
      var f = fotoInp.files && fotoInp.files[0];
      fotoInp.value = '';
      if (!f) return;
      arquivoParaB64(f, function (b64, mime, nome) {
        enviarPayload({
          conversa_id: convId,
          texto: '',
          tipo_midia: 'image',
          midia_b64: b64,
          mime: mime || 'image/jpeg',
          nome_arquivo: nome || 'foto.jpg',
        });
      });
    });
  }

  var micBtn = $('wa-mic');
  var micX = $('wa-mic-x');
  if (micBtn) {
    micBtn.addEventListener('click', function () {
      if (!convId) {
        window.alert('Abra uma conversa primeiro.');
        return;
      }
      if (recObj) {
        pararRec(true);
        return;
      }
      navigator.mediaDevices.getUserMedia({ audio: true }).then(function (stream) {
        recStream = stream;
        recChunks = [];
        var opts = {};
        if (window.MediaRecorder.isTypeSupported && MediaRecorder.isTypeSupported('audio/webm;codecs=opus')) {
          opts.mimeType = 'audio/webm;codecs=opus';
        }
        recObj = new MediaRecorder(stream, opts);
        recObj.ondataavailable = function (e) {
          if (e.data && e.data.size) recChunks.push(e.data);
        };
        recObj.start();
        ligarRecUi(true);
        if (micX) micX.classList.remove('hidden');
      }).catch(function () {
        window.alert('Não deu para gravar. Permita o microfone ou mande um arquivo de áudio.');
      });
    });
  }
  if (micX) {
    micX.addEventListener('click', function () {
      pararRec(false);
    });
  }
  var audioArq = $('wa-audio-arq');
  var audioInp = $('wa-audio-inp');
  if (audioArq && audioInp) {
    audioArq.addEventListener('click', function () {
      if (!convId) {
        window.alert('Abra uma conversa primeiro.');
        return;
      }
      audioInp.value = '';
      audioInp.click();
    });
    audioInp.addEventListener('change', function () {
      var f = audioInp.files && audioInp.files[0];
      audioInp.value = '';
      if (!f) return;
      arquivoParaB64(f, function (b64, mime, nome) {
        enviarPayload({
          conversa_id: convId,
          texto: '',
          tipo_midia: 'audio',
          midia_b64: b64,
          mime: mime || 'audio/ogg',
          nome_arquivo: nome || 'audio.ogg',
        });
      });
    });
  }

  $('wa-form').addEventListener('submit', function (ev) {
    ev.preventDefault();
    var inp = $('wa-input');
    var t = (inp.value || '').trim();
    if (!t || !convId) return;
    inp.value = '';
    enviarPayload({ conversa_id: convId, texto: t }, t);
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
        telaCel(false);
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
    try {
      localStorage.setItem(TAB_KEY, loja);
    } catch (e) {}
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
      body: JSON.stringify({ telefone: c.telefone || '', nome: c.nome || '', jid: c.jid || '' }),
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
        var n0 = tentativa || 0;
        if (q.length >= 2 && n0 < 5) {
          hits.innerHTML = '<p class="p-4 text-sm font-semibold text-slate-400">Procurando…</p>';
        } else {
          hits.innerHTML = '<p class="p-4 text-sm font-semibold text-slate-400">Nada achado. Use <b>Importar agenda</b> (.vcf), digite o telefone com DDD, ou cadastre no Agro.</p>';
        }
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
      if (q.length >= 2 && n < 5 && !temZap) {
        window.setTimeout(function () {
          buscarTopo(n + 1);
        }, 900);
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

  var vcfInp = $('wa-vcf-inp');
  var vcfMsg = $('wa-vcf-msg');
  if (vcfInp) {
    vcfInp.addEventListener('change', function () {
      var f = vcfInp.files && vcfInp.files[0];
      if (!f) return;
      if (vcfMsg) vcfMsg.textContent = 'Importando…';
      var fd = new FormData();
      fd.append('arquivo', f);
      fetch('/api/atendimento-whatsapp/agenda-vcf/', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'X-CSRFToken': csrf() },
        body: fd,
      })
        .then(function (r) {
          return r.json().then(function (j) {
            return { okHttp: r.ok, j: j };
          });
        })
        .then(function (pack) {
          var j = pack.j || {};
          if (!pack.okHttp || !j.ok) {
            if (vcfMsg) vcfMsg.textContent = '';
            window.alert((j && j.erro) || 'Não importou a agenda.');
            return;
          }
          var txt = 'Pronto: ' + String(j.gravados || 0) + ' contato(s). Busque pelo nome.';
          if (vcfMsg) vcfMsg.textContent = txt;
          window.alert(txt);
          if (((inpBusca && inpBusca.value) || '').trim()) buscarTopo(0);
        })
        .catch(function () {
          if (vcfMsg) vcfMsg.textContent = '';
          window.alert('Falha ao enviar o arquivo.');
        })
        .finally(function () {
          vcfInp.value = '';
        });
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
        telaCel(false);
        carregarLista();
      });
    });
  }

  var btnBack = $('wa-cel-back');
  if (btnBack) {
    btnBack.addEventListener('click', function () {
      convId = 0;
      convLoja = '';
      afterId = 0;
      $('wa-topo-nome').textContent = 'Conversa';
      var hist = $('wa-hist');
      var del = $('wa-del');
      if (hist) hist.classList.add('hidden');
      if (del) del.classList.add('hidden');
      $('wa-msgs').innerHTML = '';
      $('wa-move').classList.add('hidden');
      telaCel(false);
      carregarLista();
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
