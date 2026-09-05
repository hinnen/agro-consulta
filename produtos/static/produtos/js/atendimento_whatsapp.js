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
  /** hora | espera | nova — sempre começa em horário ao abrir a tela. */
  var filtroLista = 'hora';
  var statusCache = [];
  var stAutorIdx = 0;
  var stItemIdx = 0;
  var convTel = '';
  var convNome = '';
  var convFoto = '';
  var ST_VISTOS_KEY = 'agro_wa_status_vistos_v1';
  var listaCache = [];
  var recursosWa = {};
  var respostasProntas = '';
  var xferAvisarCliente = true;
  var dlgResolve = null;

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

  function nomeExibicao(c) {
    var nome = String((c && c.nome) || '').trim();
    var tel = String((c && (c.telefone || c.jid)) || '').trim();
    if (!nome) return tel || 'Sem nome';
    var nd = nome.replace(/\D/g, '');
    var td = tel.replace(/\D/g, '');
    if (nd.length >= 10 && td && (nd === td || nd.endsWith(td) || td.endsWith(nd))) {
      return tel || nome;
    }
    return nome;
  }

  function rotuloTel(c) {
    return nomeExibicao(c);
  }

  function fetchJson(url, opt) {
    return fetch(url, opt).then(function (r) {
      return r.json().catch(function () {
        return { ok: false, erro: 'Falha de rede' };
      });
    });
  }

  function waToast(msg, ok) {
    var el = $('wa-float-toast');
    if (!el) {
      window.alert(msg);
      return;
    }
    el.textContent = String(msg || '');
    el.classList.toggle('ok', !!ok);
    el.classList.toggle('bad', !ok);
    el.classList.add('show');
    window.clearTimeout(el._t);
    el._t = window.setTimeout(function () {
      el.classList.remove('show');
    }, 3200);
  }

  function fecharDlg(val) {
    var root = $('wa-dlg');
    if (root) {
      root.classList.remove('is-on');
      root.setAttribute('aria-hidden', 'true');
    }
    var fn = dlgResolve;
    dlgResolve = null;
    if (fn) fn(val);
  }

  function waDlg(opts) {
    opts = opts || {};
    var root = $('wa-dlg');
    var tit = $('wa-dlg-tit');
    var txt = $('wa-dlg-txt');
    var inp = $('wa-dlg-inp');
    var btnOk = $('wa-dlg-ok');
    var btnCancel = $('wa-dlg-cancel');
    if (!root || !tit || !txt || !btnOk || !btnCancel) {
      if (opts.mode === 'prompt') return Promise.resolve(window.prompt(opts.text || '', opts.value || '') || '');
      if (opts.mode === 'confirm') return Promise.resolve(window.confirm(opts.text || ''));
      window.alert(opts.text || '');
      return Promise.resolve(true);
    }
    if (dlgResolve) fecharDlg(opts.mode === 'prompt' ? null : false);
    return new Promise(function (resolve) {
      dlgResolve = resolve;
      tit.textContent = opts.title || 'Atenção';
      txt.textContent = opts.text || '';
      var isPrompt = opts.mode === 'prompt';
      var isAlert = opts.mode === 'alert';
      if (inp) {
        inp.classList.toggle('hidden', !isPrompt);
        inp.value = isPrompt ? String(opts.value || '') : '';
        if (isPrompt) {
          window.setTimeout(function () {
            try {
              inp.focus();
              inp.select();
            } catch (e) {}
          }, 30);
        }
      }
      btnCancel.classList.toggle('hidden', !!isAlert);
      btnOk.textContent = opts.okLabel || 'OK';
      btnCancel.textContent = opts.cancelLabel || 'Cancelar';
      root.classList.add('is-on');
      root.setAttribute('aria-hidden', 'false');
      btnOk.onclick = function () {
        if (isPrompt) fecharDlg(inp ? String(inp.value || '') : '');
        else fecharDlg(true);
      };
      btnCancel.onclick = function () {
        fecharDlg(isPrompt ? null : false);
      };
      root.onclick = function (ev) {
        if (ev.target === root && !isAlert) fecharDlg(isPrompt ? null : false);
      };
    });
  }

  function waAlert(msg, title) {
    return waDlg({ mode: 'alert', title: title || 'Atenção', text: msg });
  }

  function waConfirm(msg, title) {
    return waDlg({ mode: 'confirm', title: title || 'Confirmar', text: msg });
  }

  function waPrompt(msg, def, title) {
    return waDlg({ mode: 'prompt', title: title || 'Nota', text: msg, value: def || '' });
  }

  document.addEventListener('keydown', function (ev) {
    var root = $('wa-dlg');
    if (!root || !root.classList.contains('is-on')) return;
    if (ev.key === 'Escape') {
      ev.preventDefault();
      var inp = $('wa-dlg-inp');
      var isPrompt = inp && !inp.classList.contains('hidden');
      fecharDlg(isPrompt ? null : false);
    } else if (ev.key === 'Enter') {
      var ok = $('wa-dlg-ok');
      if (ok) {
        ev.preventDefault();
        ok.click();
      }
    }
  });

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
      var btnTrocarOff = $('wa-trocar');
      if (btnTrocarOff) btnTrocarOff.classList.add('hidden');
      return;
    }
    var btnTrocar = $('wa-trocar');
    if (btnTrocar) btnTrocar.classList.toggle('hidden', !p.conectada);
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

  function escapeHtml(s) {
    return String(s || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
  }

  function icoPreview(kind) {
    var common = ' class="wa-p-ico" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"';
    if (kind === 'audio') {
      return '<svg' + common + '><path d="M12 3a3 3 0 0 0-3 3v6a3 3 0 0 0 6 0V6a3 3 0 0 0-3-3z"/><path d="M19 10v1a7 7 0 0 1-14 0v-1"/><path d="M12 18v3"/></svg>';
    }
    if (kind === 'sticker') {
      return '<svg' + common + '><path d="M15.5 3H5a2 2 0 0 0-2 2v14l4-2h10.5a2 2 0 0 0 2-2V5a2 2 0 0 0-2-2z"/><path d="M8 10h.01"/><path d="M12 10h.01"/><path d="M9.5 14c.7.7 1.6 1 2.5 1s1.8-.3 2.5-1"/></svg>';
    }
    if (kind === 'image') {
      return '<svg' + common + '><rect x="3" y="5" width="18" height="14" rx="2"/><circle cx="9" cy="10" r="1.5"/><path d="m21 15-4.5-4.5L8 19"/></svg>';
    }
    if (kind === 'video') {
      return '<svg' + common + '><rect x="3" y="6" width="13" height="12" rx="2"/><path d="m16 10 5-3v10l-5-3z"/></svg>';
    }
    if (kind === 'document') {
      return '<svg' + common + '><path d="M14 3H7a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V8z"/><path d="M14 3v5h5"/></svg>';
    }
    return '';
  }

  function htmlPreviewLista(preview) {
    var raw = String(preview || '').replace(/\s+/g, ' ').trim();
    var low = raw.toLowerCase();
    var kind = '';
    var label = raw;
    if (low === '[áudio]' || low === '[audio]' || low.indexOf('[áudio]') === 0 || low.indexOf('[audio]') === 0) {
      kind = 'audio';
      label = 'Áudio';
    } else if (low === '[figurinha]' || low.indexOf('[figurinha]') === 0) {
      kind = 'sticker';
      label = 'Figurinha';
    } else if (low === '[imagem]' || low.indexOf('[imagem]') === 0) {
      kind = 'image';
      label = 'Foto';
    } else if (low === '[vídeo]' || low === '[video]' || low.indexOf('[vídeo]') === 0 || low.indexOf('[video]') === 0) {
      kind = 'video';
      label = 'Vídeo';
    } else if (low === '[arquivo]' || low.indexOf('[arquivo]') === 0) {
      kind = 'document';
      label = 'Arquivo';
    }
    return icoPreview(kind) + '<span class="wa-p-txt">' + escapeHtml(label || ' ') + '</span>';
  }

  function statsLista(rows) {
    var out = { nova: 0, espera: 0 };
    (rows || []).forEach(function (c) {
      var qNao = parseInt(c.nao_lidas || 0, 10) || 0;
      var st = String(c.status || '');
      if (!st) st = qNao ? 'nova' : c.aguardando_loja ? 'espera' : 'ok';
      if (st === 'nova') out.nova += 1;
      else if (st === 'espera') out.espera += 1;
    });
    return out;
  }

  function pintarContadoresFiltro(rows) {
    var s = statsLista(rows);
    ['espera', 'nova'].forEach(function (k) {
      var el = document.querySelector('[data-filtro-n="' + k + '"]');
      if (!el) return;
      var v = parseInt(s[k] || 0, 10) || 0;
      el.textContent = String(v);
      el.classList.toggle('is-zero', v === 0);
    });
  }

  function aplicarFiltroLista(rows) {
    listaCache = Array.isArray(rows) ? rows.slice() : [];
    pintarContadoresFiltro(listaCache);
    if (filtroLista === 'hora') return listaCache;
    var ordem = filtroLista === 'nova' ? ['nova', 'espera', 'ok'] : ['espera', 'nova', 'ok'];
    return listaCache
      .map(function (c, idx) {
        var qNao = parseInt(c.nao_lidas || 0, 10) || 0;
        var st = String(c.status || '');
        if (!st) st = qNao ? 'nova' : c.aguardando_loja ? 'espera' : 'ok';
        return { c: c, idx: idx, ord: ordem.indexOf(st), st: st };
      })
      .sort(function (a, b) {
        if (a.ord !== b.ord) return a.ord - b.ord;
        return a.idx - b.idx;
      })
      .map(function (x) {
        return x.c;
      });
  }

  function pintarEstadoFiltro() {
    document.querySelectorAll('[data-filtro]').forEach(function (b) {
      var on = (b.getAttribute('data-filtro') || 'hora') === filtroLista;
      b.classList.toggle('is-on', on);
      b.setAttribute('aria-pressed', on ? 'true' : 'false');
    });
  }

  function avatarInicialHtml(titulo) {
    return (
      '<span class="wa-av-ini">' +
      escapeHtml((titulo || '?').charAt(0).toUpperCase()) +
      '</span>'
    );
  }

  function garantirAvatar(av, fotoUrl, titulo) {
    if (!av) return;
    var foto = String(fotoUrl || '').trim();
    var img = av.querySelector('img');
    if (!foto || av.getAttribute('data-foto-fail') === '1') {
      if (img || !av.querySelector('.wa-av-ini')) {
        av.innerHTML = avatarInicialHtml(titulo);
      } else {
        var ini = av.querySelector('.wa-av-ini');
        var letra = (titulo || '?').charAt(0).toUpperCase();
        if (ini && ini.textContent !== letra) ini.textContent = letra;
      }
      return;
    }
    if (img) {
      // Não troca src se já é a mesma — evita piscada a cada poll.
      if (img.getAttribute('src') !== foto) img.setAttribute('src', foto);
      return;
    }
    av.innerHTML = '<img alt="" src="' + escapeHtml(foto) + '" />';
    img = av.querySelector('img');
    if (img) {
      img.addEventListener('error', function () {
        av.setAttribute('data-foto-fail', '1');
        av.innerHTML = avatarInicialHtml(titulo);
      });
    }
  }

  function pintarLista(rows) {
    var el = $('wa-lista');
    if (!el) return;
    rows = aplicarFiltroLista(rows);
    if (!rows || !rows.length) {
      var dica =
        loja === 'pendente'
          ? 'Fila vazia. Quem já escolheu loja está em Centro ou Vila. Religar o Zap não apaga conversa.'
          : 'Nenhuma conversa nesta aba.';
      el.innerHTML = '<p class="p-4 text-sm font-semibold text-slate-400">' + dica + '</p>';
      return;
    }
    // Atualiza item a item (não recria HTML todo) — senão a foto pisca a cada 2,5s.
    // appendChild em nó existente só move o botão: a <img> não recarrega.
    var keep = {};
    var ordered = [];
    rows.forEach(function (c) {
      var id = String(c.id || '');
      if (!id) return;
      keep[id] = true;
      var qNao = parseInt(c.nao_lidas || 0, 10) || 0;
      var st = String(c.status || '');
      if (!st) st = qNao ? 'nova' : c.aguardando_loja ? 'espera' : 'ok';
      var on = Number(c.id) === convId;
      var clsExtra = st === 'nova' ? ' has-new' : st === 'espera' ? ' has-wait' : '';
      var titulo = nomeExibicao(c);
      if (c.vip) titulo = '★ ' + titulo;
      var preview = c.ultima_preview || '';
      if (c.nota) preview = '📝 ' + String(c.nota).slice(0, 40) + (preview ? ' · ' + preview : '');
      var foto = c.foto_url || '';
      var btn = el.querySelector('.wa-item[data-id="' + id + '"]');
      if (!btn) {
        btn = document.createElement('button');
        btn.type = 'button';
        btn.setAttribute('data-id', id);
        btn.innerHTML =
          '<span class="wa-av"></span><div class="wa-item-body"><div class="wa-row1"><div class="wa-n"></div><div class="wa-t"></div></div><div class="wa-row2"><div class="wa-p"></div></div></div>';
      }
      btn.className = 'wa-item' + (on ? ' is-on' : '') + clsExtra;
      btn.setAttribute('data-loja', c.loja || '');
      btn.setAttribute('data-status', st);
      btn.setAttribute('data-nome', titulo);
      btn.setAttribute('data-tel', c.telefone || '');
      btn.setAttribute('data-foto', foto);
      garantirAvatar(btn.querySelector('.wa-av'), foto, titulo);
      var nEl = btn.querySelector('.wa-n');
      var tEl = btn.querySelector('.wa-t');
      var pEl = btn.querySelector('.wa-p');
      if (nEl) nEl.textContent = titulo;
      if (tEl) tEl.textContent = c.hora || '';
      if (pEl) pEl.innerHTML = htmlPreviewLista(preview);
      var row2 = btn.querySelector('.wa-row2');
      var unreadEl = btn.querySelector('.wa-unread');
      if (qNao) {
        if (!unreadEl && row2) {
          unreadEl = document.createElement('span');
          unreadEl.className = 'wa-unread';
          row2.appendChild(unreadEl);
        }
        if (unreadEl) {
          unreadEl.title = qNao + ' não lida' + (qNao > 1 ? 's' : '');
          unreadEl.textContent = String(qNao);
        }
      } else if (unreadEl) {
        unreadEl.remove();
      }
      ordered.push(btn);
    });
    Array.prototype.slice.call(el.querySelectorAll('.wa-item')).forEach(function (old) {
      var oid = old.getAttribute('data-id') || '';
      if (!keep[oid]) old.remove();
    });
    // Tira texto de “fila vazia” se ainda estiver.
    Array.prototype.slice.call(el.children).forEach(function (ch) {
      if (!ch.classList || !ch.classList.contains('wa-item')) ch.remove();
    });
    ordered.forEach(function (btn) {
      el.appendChild(btn);
    });
  }

  function pintarMsgs(rows, append) {
    var el = $('wa-msgs');
    if (!el) return;
    var ja = {};
    if (append) {
      el.querySelectorAll('[data-msg-id]').forEach(function (n) {
        ja[n.getAttribute('data-msg-id') || ''] = true;
      });
    }
    var html = (rows || [])
      .filter(function (m) {
        if (!append) return true;
        var id = String((m && m.id) || '');
        if (!id || ja[id]) return false;
        ja[id] = true;
        return true;
      })
      .map(function (m) {
        var cls = m.direcao === 'out' ? 'out' : m.direcao === 'bot' ? 'bot' : 'in';
        if (m.apagada) cls += ' is-apagada';
        var who =
          m.direcao === 'out' ? m.autor || 'Loja' : m.direcao === 'bot' ? 'Bot' : 'Cliente';
        var corpoTxt = m.texto || '';
        if (!m.apagada && (corpoTxt === '[imagem]' || corpoTxt === '[áudio]')) corpoTxt = '';
        var corpo = escapeHtml(corpoTxt);
        var midia = '';
        if (!m.apagada && m.midia_url && (m.tipo_midia === 'image' || m.tipo_midia === 'sticker')) {
          midia = '<img class="wa-pic" alt="" src="' + escapeHtml(m.midia_url) + '" />';
        } else if (!m.apagada && m.midia_url && m.tipo_midia === 'audio') {
          midia = '<audio class="wa-aud" controls src="' + escapeHtml(m.midia_url) + '"></audio>';
        } else if (!m.apagada && (m.tipo_midia === 'image' || m.tipo_midia === 'sticker')) {
          midia = '<span class="text-xs text-slate-500">Foto (ainda baixando)</span>';
        } else if (!m.apagada && m.tipo_midia === 'audio') {
          midia = '<span class="text-xs text-slate-500">Áudio (ainda baixando)</span>';
        }
        var delBtn = '';
        if (m.pode_apagar) {
          delBtn =
            '<button type="button" class="wa-msg-del" data-apagar-msg="' +
            escapeHtml(String(m.id)) +
            '" title="Apagar no Zap do cliente">×</button>';
        }
        return (
          '<div class="wa-b ' +
          cls +
          '" data-msg-id="' +
          escapeHtml(String(m.id)) +
          '"><div class="text-[10px] font-black uppercase opacity-70">' +
          escapeHtml(who) +
          ' · ' +
          escapeHtml(m.hora || '') +
          '</div>' +
          midia +
          (m.apagada ? '<em class="wa-apagada-txt">' + corpo + '</em>' : corpo) +
          delBtn +
          '</div>'
        );
      })
      .join('');
    if (append) {
      if (html) el.insertAdjacentHTML('beforeend', html);
    } else el.innerHTML = html || '<p class="text-sm text-slate-500">Sem mensagens.</p>';
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
      el.classList.toggle('is-off', !separarLojas);
      el.setAttribute('aria-hidden', separarLojas ? 'false' : 'true');
    });
    if (!separarLojas) {
      loja = 'todas';
      try {
        localStorage.removeItem(TAB_KEY);
      } catch (e) {}
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
      recursosWa = j.recursos || {};
      respostasProntas = String(j.respostas_prontas || '');
      if (j.bot && typeof j.bot.xfer_avisar_cliente === 'boolean') {
        xferAvisarCliente = j.bot.xfer_avisar_cliente;
      }
      pintarQuick();
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

  function pintarQuick() {
    var box = $('wa-quick');
    if (!box) return;
    if (!recursosWa.feat_respostas_prontas || !convId) {
      box.classList.add('hidden');
      box.innerHTML = '';
      return;
    }
    var parts = String(respostasProntas || '')
      .split('|')
      .map(function (s) {
        return s.trim();
      })
      .filter(Boolean)
      .slice(0, 8);
    if (!parts.length) {
      box.classList.add('hidden');
      return;
    }
    box.classList.remove('hidden');
    box.innerHTML = parts
      .map(function (t) {
        return (
          '<button type="button" class="wa-ico normal-case tracking-normal text-xs" data-wa-quick="' +
          escapeHtml(t) +
          '">' +
          escapeHtml(t.length > 28 ? t.slice(0, 27) + '…' : t) +
          '</button>'
        );
      })
      .join('');
  }

  function carregarLista() {
    if ($('wa-busca') && ($('wa-busca').value || '').trim()) return Promise.resolve();
    var qLoja = separarLojas ? loja : 'todas';
    return fetchJson('/api/atendimento-whatsapp/conversas/?loja=' + encodeURIComponent(qLoja)).then(
      function (j) {
        if (!j || !j.ok) return;
        pintarLista(j.conversas);
      }
    );
  }

  function lerStatusVistos() {
    try {
      var raw = localStorage.getItem(ST_VISTOS_KEY);
      var arr = raw ? JSON.parse(raw) : [];
      return Array.isArray(arr) ? arr.map(String) : [];
    } catch (e) {
      return [];
    }
  }

  function salvarStatusVisto(id) {
    if (!id) return;
    var vistos = lerStatusVistos();
    var sid = String(id);
    if (vistos.indexOf(sid) >= 0) return;
    vistos.push(sid);
    if (vistos.length > 800) vistos = vistos.slice(-800);
    try {
      localStorage.setItem(ST_VISTOS_KEY, JSON.stringify(vistos));
    } catch (e) {}
  }

  function nomeStatusAutor(a) {
    return (a && (a.nome || a.telefone)) || 'Contato';
  }

  function digitosTel(s) {
    return String(s || '').replace(/\D/g, '');
  }

  function acharStatusIdx(tel, nome) {
    var d = digitosTel(tel);
    var n = String(nome || '')
      .trim()
      .toLowerCase();
    var i;
    for (i = 0; i < statusCache.length; i++) {
      var ad = digitosTel(statusCache[i].telefone);
      if (d && ad && d.slice(-10) === ad.slice(-10)) return i;
    }
    for (i = 0; i < statusCache.length; i++) {
      var nm = String(statusCache[i].nome || '')
        .trim()
        .toLowerCase();
      if (n && nm && (nm === n || nm.indexOf(n) === 0 || n.indexOf(nm) === 0)) return i;
    }
    return -1;
  }

  function pintarStatusDoChat() {
    var btn = $('wa-topo-status');
    var ini = $('wa-topo-st-ini');
    if (!btn) return;
    if (!convId) {
      btn.classList.add('hidden');
      return;
    }
    var idx = acharStatusIdx(convTel, convNome);
    if (idx < 0) {
      btn.classList.add('hidden');
      return;
    }
    var autor = statusCache[idx];
    var vistos = lerStatusVistos();
    var temNovo = (autor.itens || []).some(function (it) {
      return vistos.indexOf(String(it.id)) < 0;
    });
    btn.classList.remove('hidden');
    btn.classList.toggle('is-new', temNovo);
    btn.setAttribute('data-st-idx', String(idx));
    if (ini) ini.textContent = nomeStatusAutor(autor).charAt(0).toUpperCase();
  }

  function pintarStatusStrip() {
    pintarStatusDoChat();
  }

  function pintarTopoAvatar(titulo, foto) {
    var av = $('wa-topo-av');
    if (!av) return;
    if (foto) {
      av.innerHTML = '<img alt="" src="' + escapeHtml(foto) + '" />';
      return;
    }
    av.innerHTML =
      '<span id="wa-topo-ini">' + escapeHtml((titulo || '?').charAt(0).toUpperCase()) + '</span>';
  }

  function mostrarBotoesChat(on) {
    var head = $('wa-chat-head');
    if (head) head.classList.toggle('is-empty', !on);
    if (!on) {
      var st = $('wa-topo-status');
      if (st) st.classList.add('hidden');
    }
  }

  function pintarStatusViewer() {
    var autor = statusCache[stAutorIdx];
    if (!autor) return;
    var itens = autor.itens || [];
    var item = itens[stItemIdx];
    if (!item) return;
    var nome = nomeStatusAutor(autor);
    var av = $('wa-st-view-av');
    var nm = $('wa-st-view-nome');
    var hr = $('wa-st-view-hora');
    var cap = $('wa-st-caption');
    var media = $('wa-st-media');
    var bars = $('wa-st-bars');
    if (av) av.textContent = nome.charAt(0).toUpperCase();
    if (nm) nm.textContent = nome;
    if (hr) hr.textContent = item.hora || '';
    if (cap) cap.textContent = item.texto || '';
    if (bars) {
      bars.innerHTML = itens
        .map(function (_it, i) {
          var cls = 'wa-st-bar';
          if (i < stItemIdx) cls += ' is-done';
          else if (i === stItemIdx) cls += ' is-on';
          return '<div class="' + cls + '"><i></i></div>';
        })
        .join('');
    }
    if (media) {
      pararMidiaStatus();
      var html = '';
      if (item.midia_url && (item.tipo_midia === 'image' || item.tipo_midia === 'video' || !item.tipo_midia)) {
        if (item.tipo_midia === 'video') {
          html =
            '<video controls autoplay playsinline src="' + escapeHtml(item.midia_url) + '"></video>';
        } else {
          html = '<img alt="" src="' + escapeHtml(item.midia_url) + '" />';
        }
      } else if (item.texto) {
        html = '<div class="wa-st-txt">' + escapeHtml(item.texto) + '</div>';
        if (cap) cap.textContent = '';
      } else {
        html = '<div class="wa-st-txt">Status</div>';
      }
      media.innerHTML = html;
    }
    salvarStatusVisto(item.id);
    pintarStatusStrip();
  }

  function pararMidiaStatus() {
    var media = $('wa-st-media');
    if (!media) return;
    media.querySelectorAll('audio, video').forEach(function (el) {
      try {
        el.pause();
        el.removeAttribute('src');
        if (typeof el.load === 'function') el.load();
      } catch (e) {}
    });
    media.innerHTML = '';
  }

  function fecharStatusViewer() {
    pararMidiaStatus();
    var box = $('wa-st-view');
    if (box) box.classList.add('hidden');
    document.body.classList.remove('wa-st-open');
  }

  function abrirStatusViewer(idxAutor, idxItem) {
    stAutorIdx = parseInt(idxAutor, 10) || 0;
    stItemIdx = parseInt(idxItem, 10) || 0;
    var autor = statusCache[stAutorIdx];
    if (!autor || !(autor.itens || []).length) return;
    if (stItemIdx >= autor.itens.length) stItemIdx = 0;
    var box = $('wa-st-view');
    if (box) box.classList.remove('hidden');
    document.body.classList.add('wa-st-open');
    pintarStatusViewer();
  }

  function proximoStatusItem() {
    var autor = statusCache[stAutorIdx];
    if (!autor) return fecharStatusViewer();
    if (stItemIdx + 1 < (autor.itens || []).length) {
      stItemIdx += 1;
      pintarStatusViewer();
      return;
    }
    if (stAutorIdx + 1 < statusCache.length) {
      stAutorIdx += 1;
      stItemIdx = 0;
      pintarStatusViewer();
      return;
    }
    fecharStatusViewer();
  }

  function anteriorStatusItem() {
    if (stItemIdx > 0) {
      stItemIdx -= 1;
      pintarStatusViewer();
      return;
    }
    if (stAutorIdx > 0) {
      stAutorIdx -= 1;
      var prev = statusCache[stAutorIdx];
      stItemIdx = Math.max(0, ((prev && prev.itens) || []).length - 1);
      pintarStatusViewer();
    }
  }

  function carregarStatus() {
    return fetchJson('/api/atendimento-whatsapp/status/').then(function (j) {
      if (!j || !j.ok) return;
      statusCache = j.autores || [];
      pintarStatusStrip();
    });
  }

  function trocarFiltroLista(filtro) {
    filtroLista = filtro === 'espera' || filtro === 'nova' ? filtro : 'hora';
    pintarEstadoFiltro();
    pintarLista(listaCache);
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
    convNome = (item && item.getAttribute('data-nome')) || 'Conversa #' + convId;
    convTel = (item && item.getAttribute('data-tel')) || '';
    convFoto = (item && item.getAttribute('data-foto')) || '';
    var nomeEl = $('wa-topo-nome');
    if (nomeEl) nomeEl.textContent = convNome;
    pintarTopoAvatar(convNome, convFoto);
    mostrarBotoesChat(true);
    pintarStatusDoChat();
    if (separarLojas) {
      $('wa-move').classList.remove('hidden');
      $('wa-move').classList.add('flex');
      pintarXfer();
    } else {
      $('wa-move').classList.add('hidden');
    }
    telaCel(true);
    pintarQuick();
    fetchJson('/api/atendimento-whatsapp/mensagens/?conversa_id=' + convId).then(function (j) {
      var rows = (j && j.mensagens) || [];
      pintarMsgs(rows, false);
      if (rows.length) afterId = rows[rows.length - 1].id;
      rows.forEach(function (m) {
        if (m.direcao === 'in' && m.id > lastSeenIn) lastSeenIn = m.id;
      });
    });
    fetchJson('/api/atendimento-whatsapp/ficha/?conversa_id=' + convId).then(function (j) {
      var f = j && j.ficha;
      if (!f) return;
      if (f.nome) {
        convNome = nomeExibicao(f);
        if (nomeEl) nomeEl.textContent = convNome;
        pintarTopoAvatar(convNome, convFoto || f.foto_url || '');
      }
      if (f.telefone) convTel = f.telefone;
      if (f.loja) convLoja = f.loja;
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

  function limparChatAberto() {
    convId = 0;
    convLoja = '';
    convTel = '';
    convNome = '';
    convFoto = '';
    afterId = 0;
    var nomeEl = $('wa-topo-nome');
    if (nomeEl) nomeEl.textContent = 'Conversa';
    pintarTopoAvatar('?', '');
    mostrarBotoesChat(false);
    if ($('wa-msgs')) $('wa-msgs').innerHTML = '';
    if ($('wa-quick')) {
      $('wa-quick').classList.add('hidden');
      $('wa-quick').innerHTML = '';
    }
    if ($('wa-move')) $('wa-move').classList.add('hidden');
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
      setTab();
      limparChatAberto();
      telaCel(false);
      carregarLista();
    });
  });

  document.querySelectorAll('[data-filtro]').forEach(function (b) {
    b.addEventListener('click', function () {
      trocarFiltroLista(b.getAttribute('data-filtro') || 'hora');
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
        carregarLista();
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
      var txt =
        'Passar este atendimento para a ' +
        nome +
        '? ' +
        (xferAvisarCliente
          ? 'O cliente é avisado no Zap.'
          : 'O cliente NÃO recebe aviso no Zap.');
      waConfirm(txt, 'Passar atendimento').then(function (ok) {
        if (!ok) return;
        var pNota = recursosWa.feat_xfer_nota
          ? waPrompt('Nota interna para a outra loja (opcional):', '', 'Nota interna')
          : Promise.resolve('');
        pNota.then(function (nota) {
          if (nota === null) return;
          fetchJson('/api/atendimento-whatsapp/transferir/', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf() },
            body: JSON.stringify({
              conversa_id: convId,
              loja: dest,
              nota: String(nota || ''),
            }),
          }).then(function (j) {
            if (!j || !j.ok) {
              waAlert((j && j.erro) || 'Não passou.', 'Não passou');
              return;
            }
            limparChatAberto();
            telaCel(false);
            carregarEstado();
            carregarLista();
            waToast('Passou para a ' + nome + '.', true);
          });
        });
      });
    });
  });

  var quickBox = $('wa-quick');
  if (quickBox) {
    quickBox.addEventListener('click', function (ev) {
      var b = ev.target.closest('[data-wa-quick]');
      if (!b || !convId) return;
      var t = b.getAttribute('data-wa-quick') || '';
      if (!t) return;
      enviarPayload({ conversa_id: convId, texto: t }, t);
    });
  }
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
          hits.innerHTML = '<p class="p-4 text-sm font-semibold text-slate-400">Nada achado. Digite o telefone com DDD, cadastre no Agro, ou importe a agenda em <b>Bot</b>.</p>';
        }
      } else {
        hits.innerHTML = rows
          .map(function (c) {
            var titulo = nomeExibicao(c);
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
              '"><div class="wa-row1"><div class="wa-n">' +
              escapeHtml(titulo) +
              '</div></div><div class="wa-row2"><div class="wa-p"><span class="wa-p-txt">' +
              escapeHtml(rotuloOrigem(c.origem)) +
              (c.telefone && titulo !== c.telefone ? ' · toque p/ abrir' : '') +
              '</span></div></div></button>'
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
      var antes = (($('wa-msgs') && $('wa-msgs').querySelectorAll('.wa-b').length) || 0);
      btnHist.disabled = true;
      var rotulo = btnHist.textContent;
      btnHist.textContent = '…';
      fetchJson('/api/atendimento-whatsapp/historico/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf() },
        body: JSON.stringify({ conversa_id: convId }),
      }).then(function (j) {
        if (!j || !j.ok) {
          window.alert((j && j.erro) || 'Não puxou. Ponte ligada?');
          btnHist.disabled = false;
          btnHist.textContent = rotulo;
          return;
        }
        window.setTimeout(function () {
          abrirConversa(convId);
          window.setTimeout(function () {
            var depois = (($('wa-msgs') && $('wa-msgs').querySelectorAll('.wa-b').length) || 0);
            btnHist.disabled = false;
            btnHist.textContent = rotulo;
            if (depois <= antes) {
              window.alert('O Zap não mandou mensagens novas deste chat (só ~7 dias, e às vezes não libera).');
            }
          }, 2500);
        }, 4500);
      }).catch(function () {
        btnHist.disabled = false;
        btnHist.textContent = rotulo;
        window.alert('Falha ao pedir anteriores.');
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
  var btnTrocar = $('wa-trocar');
  if (btnTrocar) {
    btnTrocar.addEventListener('click', function () {
      if (
        !window.confirm(
          'Desligar este WhatsApp neste PC?\nVai precisar ler o QR ou gerar um código de novo (pode ser outro número).'
        )
      ) {
        return;
      }
      btnTrocar.disabled = true;
      fetchJson('/api/atendimento-whatsapp/trocar/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf() },
        body: '{}',
      })
        .then(function (j) {
          if (!j || !j.ok) {
            window.alert((j && j.erro) || 'Não desligou. Ponte ligada?');
            return;
          }
          window.setTimeout(carregarEstado, 1200);
          window.setTimeout(carregarEstado, 3500);
        })
        .finally(function () {
          btnTrocar.disabled = false;
        });
    });
  }
  var btnOk = $('wa-ok');
  if (btnOk) {
    btnOk.addEventListener('click', function () {
      if (!convId) return;
      fetchJson('/api/atendimento-whatsapp/concluir/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf() },
        body: JSON.stringify({ conversa_id: convId }),
      }).then(function (j) {
        if (!j || !j.ok) {
          window.alert((j && j.erro) || 'Não concluiu.');
          return;
        }
        carregarEstado();
        carregarLista();
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
        limparChatAberto();
        telaCel(false);
        carregarLista();
      });
    });
  }
  var btnLimpar = $('wa-limpar-lista');
  if (btnLimpar) {
    btnLimpar.addEventListener('click', function () {
      if (
        !window.confirm(
          'Apagar TODAS as conversas da lista neste sistema?\nSó some no SisVale. No celular o Zap fica igual.'
        )
      ) {
        return;
      }
      btnLimpar.disabled = true;
      fetchJson('/api/atendimento-whatsapp/excluir-todas/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf() },
        body: '{}',
      })
        .then(function (j) {
          if (!j || !j.ok) {
            window.alert((j && j.erro) || 'Não limpou.');
            return;
          }
          limparChatAberto();
          telaCel(false);
          carregarLista();
          carregarEstado();
        })
        .finally(function () {
          btnLimpar.disabled = false;
        });
    });
  }

  var msgsBox = $('wa-msgs');
  if (msgsBox) {
    msgsBox.addEventListener('click', function (ev) {
      var btn = ev.target && ev.target.closest ? ev.target.closest('[data-apagar-msg]') : null;
      if (!btn) return;
      ev.preventDefault();
      ev.stopPropagation();
      var mid = Number(btn.getAttribute('data-apagar-msg') || 0);
      if (!mid) return;
      if (
        !window.confirm(
          'Apagar esta mensagem no WhatsApp do cliente também? (Como “Apagar para todos”.)'
        )
      ) {
        return;
      }
      btn.disabled = true;
      btn.textContent = '…';
      fetchJson('/api/atendimento-whatsapp/apagar-mensagem/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf() },
        body: JSON.stringify({ mensagem_id: mid }),
      }).then(function (j) {
        if (!j || !j.ok) {
          window.alert((j && j.erro) || 'Não apagou.');
          btn.disabled = false;
          btn.textContent = '×';
          return;
        }
        if (!convId) return;
        fetchJson('/api/atendimento-whatsapp/mensagens/?conversa_id=' + convId).then(function (mj) {
          pintarMsgs((mj && mj.mensagens) || [], false);
        });
      });
    });
  }

  var btnBack = $('wa-cel-back');
  if (btnBack) {
    btnBack.addEventListener('click', function () {
      limparChatAberto();
      telaCel(false);
      carregarLista();
    });
  }

  function fecharFicha() {
    var box = $('wa-ficha');
    if (box) box.classList.remove('is-on');
  }

  function linhaFicha(label, valor) {
    if (!valor) return '';
    return (
      '<div class="wa-ficha-row"><dt>' +
      escapeHtml(label) +
      '</dt><dd>' +
      escapeHtml(valor) +
      '</dd></div>'
    );
  }

  function abrirFicha() {
    if (!convId) return;
    var box = $('wa-ficha');
    var body = $('wa-ficha-body');
    var link = $('wa-ficha-cadastro');
    if (!box || !body) return;
    body.innerHTML = '<p class="text-sm font-semibold text-slate-500">Carregando…</p>';
    if (link) {
      link.classList.add('hidden');
      link.removeAttribute('href');
    }
    box.classList.add('is-on');
    fetchJson('/api/atendimento-whatsapp/ficha/?conversa_id=' + convId).then(function (j) {
      if (!j || !j.ok || !j.ficha) {
        body.innerHTML = '<p class="text-sm font-semibold text-red-600">' + escapeHtml((j && j.erro) || 'Não carregou.') + '</p>';
        return;
      }
      var f = j.ficha;
      var html = '';
      html += linhaFicha('Nome no chat', f.nome || '—');
      html += linhaFicha('Telefone / WhatsApp', f.telefone || '—');
      html += linhaFicha('Fila', f.loja_label || f.loja || '—');
      if (f.agenda_nome && f.agenda_nome !== f.nome) {
        html += linhaFicha('Nome na agenda', f.agenda_nome);
      }
      if (f.cadastro) {
        html += linhaFicha('Cadastro Agro', f.cadastro.nome || '—');
        html += linhaFicha('WhatsApp no cadastro', f.cadastro.whatsapp || '—');
        if (f.cadastro.cpf) html += linhaFicha('CPF', f.cadastro.cpf);
        if (f.cadastro.endereco) html += linhaFicha('Endereço', f.cadastro.endereco);
        else if (f.cadastro.cidade) html += linhaFicha('Cidade', f.cadastro.cidade);
        if (link && f.cadastro.url) {
          link.href = f.cadastro.url;
          link.classList.remove('hidden');
        }
      } else if (f.cadastro_varios) {
        html += linhaFicha('Cadastro Agro', 'Mais de um cliente com este número');
      } else {
        html += linhaFicha('Cadastro Agro', 'Não encontrado pelo número');
      }
      body.innerHTML = html;
      var tit = $('wa-ficha-titulo');
      if (tit) tit.textContent = nomeExibicao({ nome: f.nome, telefone: f.telefone }) || 'Contato';
    });
  }

  function abrirCadastroOverlay(url) {
    var href = String(url || '').trim();
    if (!href || href === '#') return;
    if (window.AgroPdvOverlay && typeof window.AgroPdvOverlay.open === 'function') {
      window.AgroPdvOverlay.open(href, 'Cadastro', { force: true });
      return;
    }
    window.location.href = href;
  }

  var topoWho = $('wa-topo-who');
  if (topoWho) {
    topoWho.addEventListener('click', function () {
      if (!convId) return;
      abrirFicha();
    });
  }
  var topoStatus = $('wa-topo-status');
  if (topoStatus) {
    topoStatus.addEventListener('click', function () {
      var idx = topoStatus.getAttribute('data-st-idx');
      if (idx == null || idx === '') return;
      abrirStatusViewer(idx, 0);
    });
  }
  document.addEventListener('click', function (ev) {
    var fecharBtn = ev.target && ev.target.closest ? ev.target.closest('#wa-ficha-fechar') : null;
    if (fecharBtn) {
      ev.preventDefault();
      ev.stopPropagation();
      fecharFicha();
      return;
    }
    var cadBtn = ev.target && ev.target.closest ? ev.target.closest('#wa-ficha-cadastro') : null;
    if (cadBtn) {
      ev.preventDefault();
      ev.stopPropagation();
      abrirCadastroOverlay(cadBtn.getAttribute('href') || cadBtn.href || '');
      return;
    }
    var fichaBox = $('wa-ficha');
    if (fichaBox && ev.target === fichaBox) fecharFicha();
  });
  document.addEventListener('keydown', function (ev) {
    if (ev.key === 'Escape') {
      if ($('wa-st-view') && !$('wa-st-view').classList.contains('hidden')) {
        fecharStatusViewer();
        return;
      }
      fecharFicha();
    }
    if (!$('wa-st-view') || $('wa-st-view').classList.contains('hidden')) return;
    if (ev.key === 'ArrowRight') proximoStatusItem();
    if (ev.key === 'ArrowLeft') anteriorStatusItem();
  });

  var stX = $('wa-st-view-x');
  if (stX) stX.addEventListener('click', fecharStatusViewer);
  var stPrev = $('wa-st-prev');
  if (stPrev) stPrev.addEventListener('click', anteriorStatusItem);
  var stNext = $('wa-st-next');
  if (stNext) stNext.addEventListener('click', proximoStatusItem);
  var stView = $('wa-st-view');
  if (stView) {
    stView.addEventListener('click', function (ev) {
      if (ev.target === stView) fecharStatusViewer();
    });
  }

  function garantirPicLite() {
    var box = document.getElementById('wa-pic-lite');
    if (box) return box;
    box = document.createElement('div');
    box.id = 'wa-pic-lite';
    box.className = 'wa-pic-lite';
    box.setAttribute('role', 'dialog');
    box.setAttribute('aria-label', 'Foto em tela cheia');
    box.innerHTML = '<img alt="" />';
    document.body.appendChild(box);
    box.addEventListener('click', function (ev) {
      if (ev.target === box || (ev.target && ev.target.tagName === 'IMG')) {
        box.classList.remove('is-on');
        box.querySelector('img').removeAttribute('src');
      }
    });
    return box;
  }

  function abrirPicLite(url) {
    var src = String(url || '').trim();
    if (!src) return;
    var box = garantirPicLite();
    var img = box.querySelector('img');
    if (img) img.src = src;
    box.classList.add('is-on');
  }

  document.addEventListener('click', function (ev) {
    var pic = ev.target && ev.target.closest ? ev.target.closest('.wa-pic[data-wa-pic-full]') : null;
    if (!pic) return;
    ev.preventDefault();
    abrirPicLite(pic.getAttribute('data-wa-pic-full') || pic.getAttribute('src') || '');
  });

  document.addEventListener('keydown', function (ev) {
    if (ev.key !== 'Escape') return;
    var box = document.getElementById('wa-pic-lite');
    if (box && box.classList.contains('is-on')) {
      box.classList.remove('is-on');
      var img = box.querySelector('img');
      if (img) img.removeAttribute('src');
    }
  });

  function nomeOperadorPin() {
    try {
      return String(localStorage.getItem('gm_sspin_operador') || '').trim();
    } catch (_) {
      return '';
    }
  }

  function pintarOperadorPin() {
    var btn = $('wa-operador-pin');
    if (!btn) return;
    var n = nomeOperadorPin();
    if (n) {
      btn.textContent = n;
      btn.classList.remove('is-empty');
      btn.title = 'PIN: ' + n + ' — clique para trocar';
    } else {
      btn.textContent = 'PIN?';
      btn.classList.add('is-empty');
      btn.title = 'Ninguém no PIN — clique para identificar';
    }
  }

  function trocarOperadorPin() {
    if (typeof window.gmSspinSairEAbrirPin === 'function') {
      window.gmSspinSairEAbrirPin();
      return;
    }
    if (typeof window.gmSspinAbrirEntrada === 'function') {
      window.gmSspinAbrirEntrada();
      return;
    }
    window.alert('Abra o PIN do modo descanso nesta tela (recarregue a página).');
  }

  var btnOpPin = $('wa-operador-pin');
  if (btnOpPin) {
    btnOpPin.addEventListener('click', function (ev) {
      ev.preventDefault();
      ev.stopPropagation();
      trocarOperadorPin();
    });
  }
  window.addEventListener('gm-sspin-operador', pintarOperadorPin);
  pintarOperadorPin();

  setTab();
  pintarEstadoFiltro();
  pedirAviso();
  document.addEventListener('click', pedirAviso, { once: true });
  carregarEstado();
  carregarLista();
  carregarStatus();
  setInterval(function () {
    carregarEstado();
    carregarLista();
    carregarStatus();
    pollMsgs();
  }, 2500);
})();
