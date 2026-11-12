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
    if (tabSalva === 'centro' || tabSalva === 'vila' || tabSalva === 'pendente' || tabSalva === 'arquivadas' || tabSalva === 'todas') loja = tabSalva;
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
  var selModo = false;
  var selMap = {};
  var statusCache = [];
  var stAutorIdx = 0;
  var stItemIdx = 0;
  var convTel = '';
  var convNome = '';
  var convFoto = '';
  var convArquivada = false;
  var ST_VISTOS_KEY = 'agro_wa_status_vistos_v1';
  var listaCache = [];
  var recursosWa = {};
  var respostasProntas = '';
  var xferAvisarCliente = true;
  var dlgResolve = null;
  var replyToId = 0;
  var replyToTxt = '';
  var convMsgRev = 0;
  /* Array literal — .split('') parte emoji em surrogates e vira � na tela. */
  var EMOJIS_WA = [
    '😀', '😁', '😂', '🤣', '😊', '😍', '😘', '🥰', '😎', '😢', '😭', '😡',
    '👍', '👎', '❤️', '🔥', '🙏', '👏', '✅', '🎉', '🇧🇷', '🤍', '😮', '🤔',
  ];
  var reactMsgId = 0;

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

  function mostrarPainelBot(on) {
    var app = $('wa-app');
    var painel = $('wa-painel-bot');
    if (!app || !painel) return;
    app.classList.toggle('hidden', !!on);
    painel.classList.toggle('hidden', !on);
    var bBot = $('wa-btn-bot');
    var bChat = $('wa-btn-chat');
    if (bBot) bBot.classList.toggle('hidden', !!on);
    if (bChat) bChat.classList.toggle('hidden', !on);
  }
  var botJsPedido = false;
  function garantirBotJs() {
    if (botJsPedido || document.getElementById('wa-bot-js')) return;
    var b = $('wa-btn-bot');
    var src = b && b.getAttribute('data-bot-js');
    if (!src) return;
    botJsPedido = true;
    var s = document.createElement('script');
    s.id = 'wa-bot-js';
    s.src = src;
    document.body.appendChild(s);
  }
  var btnAbrirBot = $('wa-btn-bot');
  if (btnAbrirBot && $('wa-painel-bot')) {
    btnAbrirBot.addEventListener('click', function (ev) {
      ev.preventDefault();
      garantirBotJs();
      mostrarPainelBot(true);
    });
  }
  var btnVoltarChat = $('wa-btn-chat');
  if (btnVoltarChat) {
    btnVoltarChat.addEventListener('click', function () {
      mostrarPainelBot(false);
    });
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
    var o = opt || {};
    if (!o.credentials) o.credentials = 'same-origin';
    var ctrl = o.signal ? null : new AbortController();
    if (ctrl && !o.signal) {
      o.signal = ctrl.signal;
      o._agroAbort = ctrl;
    }
    var ms = typeof o.timeoutMs === 'number' ? o.timeoutMs : 0;
    var timer = null;
    if (ms > 0 && ctrl) {
      timer = window.setTimeout(function () {
        try {
          ctrl.abort();
        } catch (_) {}
      }, ms);
    }
    delete o.timeoutMs;
    return fetch(url, o)
      .then(function (r) {
        return r
          .json()
          .catch(function () {
            return {
              ok: false,
              erro:
                r.status === 403
                  ? 'Sessão/CSRF — dê F5 e entre de novo.'
                  : r.redirected || (r.url && r.url.indexOf('/entrar') >= 0)
                    ? 'Sessão expirada — dê F5 e entre de novo.'
                    : 'Falha de rede (' + r.status + ')',
            };
          })
          .then(function (j) {
            if (r.status === 403 && (!j || !j.ok)) {
              return { ok: false, erro: (j && j.erro) || 'Sessão/CSRF — dê F5 e entre de novo.' };
            }
            if (r.status >= 400 && j && j.ok == null) {
              return { ok: false, erro: (j && j.erro) || 'Erro ' + r.status };
            }
            return j;
          });
      })
      .catch(function (err) {
        var nome = err && err.name;
        if (nome === 'AbortError') {
          return { ok: false, erro: 'Demorou demais — tente de novo.' };
        }
        return { ok: false, erro: 'Falha de rede ao enviar.' };
      })
      .finally(function () {
        if (timer) window.clearTimeout(timer);
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
    var somaAtivas =
      (parseInt(n.pendente || 0, 10) || 0) +
      (parseInt(n.centro || 0, 10) || 0) +
      (parseInt(n.vila || 0, 10) || 0);
    n.todas = somaAtivas;
    ['pendente', 'centro', 'vila', 'arquivadas', 'todas'].forEach(function (k) {
      var el = document.querySelector('[data-badge="' + k + '"]');
      if (!el) return;
      var v = parseInt(n[k] || 0, 10) || 0;
      el.textContent = String(v);
      el.classList.toggle('is-zero', v === 0);
    });
  }

  function pintarAcoesArquivo() {
    var ok = $('wa-ok');
    var reb = $('wa-reabrir');
    if (ok) ok.classList.toggle('hidden', !!convArquivada);
    if (reb) reb.classList.toggle('hidden', !convArquivada);
  }

  var ehOverlay =
    /(?:\?|&)agro_pdv_overlay=1(?:&|$)/.test(String(window.location.search || '')) ||
    !!(window.frameElement && window.parent && window.parent !== window);

  var ultimaStatusTxt = '';

  function syncOverlayChrome(meta) {
    if (!ehOverlay) return;
    try {
      if (!window.parent || window.parent === window) return;
      var payload = { type: 'agro-pdv-overlay-meta', title: 'WhatsApp' };
      Object.keys(meta || {}).forEach(function (k) {
        payload[k] = meta[k];
      });
      window.parent.postMessage(payload, window.location.origin);
    } catch (_) {}
  }

  function pintarStatus(p) {
    var box = $('wa-status');
    var dot = $('wa-dot');
    var qrBox = $('wa-qr-box');
    var img = $('wa-qr-img');
    if (!box && !ehOverlay) return;
    p = p || {};
    var kind = '';
    var txt = '…';
    var title = '';
    var showTrocar = false;
    function pill(k, t, tit) {
      kind = k || '';
      txt = t || '…';
      title = tit || '';
      if (box) {
        box.textContent = txt;
        box.className = 'wa-pill' + (kind ? ' ' + kind : '');
        if (title) box.title = title;
      }
      if (dot) {
        dot.className = 'wa-dot ' + (kind === 'ok' ? 'on' : kind === 'warn' ? 'wait' : 'off');
      }
    }
    if (!p.ponte_viva) {
      pill('bad', 'Off', 'Rode whatsapp_atendimento\\iniciar.bat e deixe a janela aberta');
      if (qrBox) qrBox.classList.add('hidden');
      var btnTrocarOff = $('wa-trocar');
      if (btnTrocarOff) btnTrocarOff.classList.add('hidden');
      showTrocar = false;
    } else {
      var btnTrocar = $('wa-trocar');
      showTrocar = !!p.conectada;
      if (btnTrocar) btnTrocar.classList.toggle('hidden', !showTrocar);
      if (p.conectada) {
        pill('ok', p.numero ? p.numero : 'Online');
        if (qrBox) qrBox.classList.add('hidden');
      } else {
        if (qrBox) qrBox.classList.remove('hidden');
        var codeEl = $('wa-pair-code');
        if (codeEl) {
          if (p.pairing_code) {
            codeEl.textContent = String(p.pairing_code)
              .replace(/-/g, '')
              .replace(/(.{4})/g, '$1-')
              .replace(/-$/, '');
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
        } else {
          if (img) img.classList.add('hidden');
          pill('warn', p.pairing_code ? 'Código' : p.aviso || 'Ligando');
        }
      }
    }
    syncOverlayChrome({
      showWaChrome: true,
      showWaBot: true,
      showWaTrocar: showTrocar,
      waStatus: txt,
      waStatusKind: kind,
      waDot: kind === 'ok' ? 'on' : kind === 'warn' ? 'wait' : 'off',
      botHref: '/atendimento-whatsapp/bot/',
      subtitle: '',
    });
    ultimaStatusTxt = txt;
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

  function htmlPreviewLista(preview, conversa) {
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
    var checks = '';
    var dir = conversa ? String(conversa.ultima_direcao || '') : '';
    if (dir === 'out' || dir === 'bot') {
      checks = ackHtml({ direcao: 'out', ack: Number(conversa.ultima_ack || 0) || 1 });
    }
    return checks + icoPreview(kind) + '<span class="wa-p-txt">' + escapeHtml(label || ' ') + '</span>';
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
    var falhou = av.getAttribute('data-foto-fail') || '';
    var falhouEm = Number(av.getAttribute('data-foto-fail-em') || 0);
    if (!foto || (falhou === foto && Date.now() - falhouEm < 20000)) {
      if (fotoObs && av.hasAttribute('data-foto-quer')) {
        fotoObs.unobserve(av);
        av.removeAttribute('data-foto-quer');
        av.removeAttribute('data-foto-titulo');
      }
      if (img || !av.querySelector('.wa-av-ini')) {
        av.innerHTML = avatarInicialHtml(titulo);
      } else {
        var ini = av.querySelector('.wa-av-ini');
        var letra = (titulo || '?').charAt(0).toUpperCase();
        if (ini && ini.textContent !== letra) ini.textContent = letra;
      }
      return;
    }
    av.removeAttribute('data-foto-fail');
    if (img) {
      // Não troca src se já é a mesma — evita piscada a cada poll.
      if (img.getAttribute('src') !== foto) img.setAttribute('src', foto);
      return;
    }
    // Cada foto é uma requisição no único processo do Zap. Pedir as ~80 da lista
    // de uma vez enfileira o GET de mensagens do clique atrás delas.
    if (fotoObs) {
      av.setAttribute('data-foto-quer', foto);
      av.setAttribute('data-foto-titulo', titulo || '');
      if (!av.querySelector('.wa-av-ini')) av.innerHTML = avatarInicialHtml(titulo);
      fotoObs.observe(av);
      return;
    }
    pintarFotoAvatar(av, foto, titulo);
  }

  var fotoObs =
    typeof window.IntersectionObserver === 'function'
      ? new IntersectionObserver(
          function (entries) {
            entries.forEach(function (en) {
              if (!en.isIntersecting) return;
              var av = en.target;
              fotoObs.unobserve(av);
              var foto = av.getAttribute('data-foto-quer') || '';
              var titulo = av.getAttribute('data-foto-titulo') || '';
              av.removeAttribute('data-foto-quer');
              av.removeAttribute('data-foto-titulo');
              if (foto && !av.querySelector('img')) pintarFotoAvatar(av, foto, titulo);
            });
          },
          { rootMargin: '120px 0px' }
        )
      : null;

  function pintarFotoAvatar(av, foto, titulo) {
    av.innerHTML = '<img alt="" loading="lazy" decoding="async" src="' + escapeHtml(foto) + '" />';
    var img = av.querySelector('img');
    if (img) {
      img.addEventListener('error', function () {
        av.setAttribute('data-foto-fail', foto);
        av.setAttribute('data-foto-fail-em', String(Date.now()));
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
        loja === 'arquivadas'
          ? 'Nenhuma conversa resolvida. O ✓ no chat arquiva para cá.'
          : loja === 'pendente'
            ? 'Fila vazia. Quem já escolheu loja está em Centro ou Vila. Religar o Zap não apaga conversa.'
            : 'Nenhuma conversa nesta aba.';
      el.innerHTML = '<p class="p-4 text-sm font-semibold text-slate-400">' + dica + '</p>';
      selMap = {};
      pintarSelBar();
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
      if (selMap[id]) clsExtra += ' is-sel';
      var titulo = nomeExibicao(c);
      if (c.vip) titulo = '★ ' + titulo;
      var preview = c.ultima_preview || '';
      if (c.nota) preview = '📝 ' + String(c.nota).slice(0, 40) + (preview ? ' · ' + preview : '');
      var foto = c.foto_url || '';
      var btn = el.querySelector('.wa-item[data-id="' + id + '"]');
      if (!btn) {
        btn = document.createElement('div');
        btn.setAttribute('role', 'button');
        btn.tabIndex = 0;
        btn.setAttribute('data-id', id);
        btn.innerHTML =
          '<span class="wa-av"></span><div class="wa-item-body"><div class="wa-row1"><div class="wa-n"></div><div class="wa-t"></div></div><div class="wa-row2"><div class="wa-p"></div></div></div>';
      }
      btn.className = 'wa-item' + (on ? ' is-on' : '') + clsExtra;
      btn.setAttribute('data-loja', c.loja || '');
      btn.setAttribute('data-arquivada', c.arquivada ? '1' : '0');
      btn.setAttribute('data-status', st);
      btn.setAttribute('data-nome', titulo);
      btn.setAttribute('data-tel', c.telefone || '');
      btn.setAttribute('data-foto', foto);
      btn.setAttribute('data-msg-rev', String(c.msg_rev || 0));
      btn.setAttribute('data-digitando', c.digitando ? '1' : '0');
      garantirAvatar(btn.querySelector('.wa-av'), foto, titulo);
      var nEl = btn.querySelector('.wa-n');
      var tEl = btn.querySelector('.wa-t');
      var pEl = btn.querySelector('.wa-p');
      if (nEl) nEl.textContent = titulo;
      if (tEl) tEl.textContent = c.hora || '';
      if (pEl) pEl.innerHTML = htmlPreviewLista(preview, c);
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
    Object.keys(selMap).forEach(function (id) {
      if (!keep[id]) delete selMap[id];
    });
    pintarSelBar();
    if (convId) {
      var aberta = null;
      for (var i = 0; i < rows.length; i++) {
        if (Number(rows[i].id) === Number(convId)) {
          aberta = rows[i];
          break;
        }
      }
      if (aberta) {
        setDigitando(!!aberta.digitando);
        var rev = Number(aberta.msg_rev || 0);
        if (rev && convMsgRev && rev !== convMsgRev) {
          convMsgRev = rev;
          recarregarMsgsAbertas();
        } else if (rev) {
          convMsgRev = rev;
        }
      } else {
        setDigitando(false);
      }
    }
  }

  function ackHtml(m) {
    if (m.direcao !== 'out' && m.direcao !== 'bot') return '';
    if (m.apagada) return '';
    var n = Number(m.ack || 0);
    if (m.pendente && String(m.id || '').indexOf('tmp-') === 0) n = 0;
    var cls = 'wa-ack';
    var txt = '✓';
    if (n >= 3) {
      cls += ' is-read';
      txt = '✓✓';
    } else if (n >= 2) {
      cls += ' is-deliv';
      txt = '✓✓';
    } else if (n >= 1) {
      cls += ' is-sent';
    } else {
      cls += ' is-wait';
      txt = '⏱';
    }
    return '<span class="' + cls + '" aria-hidden="true">' + txt + '</span>';
  }

  function reacoesHtml(m) {
    var rs = m.reacoes || [];
    if (!rs.length) return '';
    return (
      '<div class="wa-reacoes">' +
      rs
        .map(function (r) {
          return (
            '<span class="wa-reacao' +
            (r.from_me ? ' is-me' : '') +
            '">' +
            escapeHtml(r.emoji || '') +
            '</span>'
          );
        })
        .join('') +
      '</div>'
    );
  }

  function pintarMsgs(rows, append) {
    var el = $('wa-msgs');
    if (!el) return;
    var ja = {};
    var lastDia = '';
    if (append) {
      el.querySelectorAll('[data-msg-id]').forEach(function (n) {
        ja[n.getAttribute('data-msg-id') || ''] = true;
      });
      var seps = el.querySelectorAll('.wa-day-sep');
      if (seps.length) lastDia = seps[seps.length - 1].getAttribute('data-dia') || '';
      else {
        var lastB = el.querySelector('.wa-b:last-of-type');
        if (lastB) lastDia = lastB.getAttribute('data-dia') || '';
      }
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
        if (m.pendente && !m.apagada && String(m.id || '').indexOf('tmp-') === 0) cls += ' is-enviando';
        var diaKey = m.data_label || m.data || '';
        var sep = '';
        if (diaKey && diaKey !== lastDia) {
          lastDia = diaKey;
          sep =
            '<div class="wa-day-sep" data-dia="' +
            escapeHtml(diaKey) +
            '"><span>' +
            escapeHtml(diaKey) +
            '</span></div>';
        }
        var quote = '';
        if (!m.apagada && (m.reply_texto || m.reply_wa_id)) {
          quote =
            '<div class="wa-quote"><span class="wa-quote-bar"></span><span class="wa-quote-txt">' +
            escapeHtml(m.reply_texto || 'Mensagem') +
            '</span></div>';
        }
        var metaHtml = '';
        var horaAck =
          (m.hora ? escapeHtml(m.hora) : '') +
          (m.direcao === 'out' || m.direcao === 'bot' ? ' ' + ackHtml(m) : '');
        if (m.direcao === 'out') {
          metaHtml =
            '<div class="wa-meta">' +
            escapeHtml(m.autor || 'Loja') +
            (horaAck ? ' · ' + horaAck : '') +
            '</div>';
        } else if (m.direcao === 'bot') {
          metaHtml = '<div class="wa-meta">Bot' + (horaAck ? ' · ' + horaAck : '') + '</div>';
        } else if (m.hora) {
          metaHtml = '<div class="wa-meta is-hora-so">' + escapeHtml(m.hora) + '</div>';
        }
        var corpoTxt = m.texto || '';
        if (!m.apagada && (corpoTxt === '[imagem]' || corpoTxt === '[áudio]')) corpoTxt = '';
        var corpo = escapeHtml(corpoTxt);
        var midia = '';
        if (!m.apagada && m.midia_url && (m.tipo_midia === 'image' || m.tipo_midia === 'sticker')) {
          midia =
            '<img class="wa-pic" alt="" src="' +
            escapeHtml(m.midia_url) +
            '" data-wa-pic-full="' +
            escapeHtml(m.midia_url) +
            '" />';
        } else if (!m.apagada && m.midia_url && m.tipo_midia === 'audio') {
          midia = '<audio class="wa-aud" controls src="' + escapeHtml(m.midia_url) + '"></audio>';
        } else if (!m.apagada && m.midia_perdida && (m.tipo_midia === 'image' || m.tipo_midia === 'sticker')) {
          midia = '<span class="text-xs text-slate-500">Foto</span>';
        } else if (!m.apagada && m.midia_perdida && m.tipo_midia === 'audio') {
          midia = '<span class="text-xs text-slate-500">Áudio</span>';
        } else if (!m.apagada && (m.tipo_midia === 'image' || m.tipo_midia === 'sticker')) {
          midia = '<span class="text-xs text-slate-500">Foto (ainda baixando)</span>';
        } else if (!m.apagada && m.tipo_midia === 'audio') {
          midia = '<span class="text-xs text-slate-500">Áudio (ainda baixando)</span>';
        }
        var acts = '';
        var idOk =
          !m.apagada &&
          String(m.id || '').indexOf('tmp-') !== 0 &&
          String(m.id || '').indexOf('previa-') !== 0;
        /* Reagir só em msg do cliente — na própria some o popup em cima do ×. Sem seta ↩ (confundia e quebrava a barra). */
        if (idOk && m.direcao === 'in') {
          acts =
            '<div class="wa-msg-acts" title="Reagir">' +
            '<button type="button" class="wa-msg-act" data-wa-act="react" data-msg-id="' +
            escapeHtml(String(m.id)) +
            '" title="Reagir com emoji">😀</button>' +
            '</div>';
        }
        if (m.pode_apagar) {
          acts +=
            '<button type="button" class="wa-msg-del" data-apagar-msg="' +
            escapeHtml(String(m.id)) +
            '" title="Apagar no Zap do cliente">×</button>';
        }
        return (
          sep +
          '<div class="wa-b ' +
          cls +
          '" data-msg-id="' +
          escapeHtml(String(m.id)) +
          '" data-dia="' +
          escapeHtml(diaKey) +
          '" data-wa-id="' +
          escapeHtml(String(m.wa_id || '')) +
          '">' +
          quote +
          metaHtml +
          midia +
          (m.apagada ? '<em class="wa-apagada-txt">' + corpo + '</em>' : corpo) +
          reacoesHtml(m) +
          acts +
          '</div>'
        );
      })
      .join('');
    var nearBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 80;
    if (append) {
      if (html) el.insertAdjacentHTML('beforeend', html);
    } else el.innerHTML = html || '<p class="text-sm text-slate-500">Sem mensagens.</p>';
    if (!append || nearBottom) el.scrollTop = el.scrollHeight;
    atualizarScrollFim();
  }

  function atualizarScrollFim() {
    var el = $('wa-msgs');
    var btn = $('wa-scroll-fim');
    if (!el || !btn) return;
    var dist = el.scrollHeight - el.scrollTop - el.clientHeight;
    btn.classList.toggle('hidden', dist < 120);
  }

  function setDigitando(on) {
    var el = $('wa-topo-digitando');
    if (el) el.classList.toggle('hidden', !on);
  }

  function setReplyBar(id, txt) {
    replyToId = Number(id) || 0;
    replyToTxt = String(txt || '').slice(0, 120);
    var bar = $('wa-reply-bar');
    var t = $('wa-reply-bar-txt');
    if (t) t.textContent = replyToTxt || 'Mensagem';
    if (bar) bar.classList.toggle('hidden', !replyToId);
    if (replyToId && $('wa-input')) $('wa-input').focus();
  }

  function limparReply() {
    setReplyBar(0, '');
  }

  function recarregarMsgsAbertas() {
    if (!convId) return;
    var idSnap = convId;
    var seqSnap = convAbreSeq;
    fetchJson('/api/atendimento-whatsapp/mensagens/?conversa_id=' + idSnap + '&limit=80', {
      timeoutMs: 15000,
    }).then(function (j) {
      if (idSnap !== convId || seqSnap !== convAbreSeq) return;
      var rows = (j && j.mensagens) || [];
      pintarMsgs(rows, false);
      guardarMsgs(idSnap, rows, false);
      if (rows.length) afterId = rows[rows.length - 1].id;
    });
  }

  function setTab() {
    document.querySelectorAll('.wa-tab').forEach(function (b) {
      b.classList.toggle('is-on', b.getAttribute('data-loja') === loja);
    });
  }

  function aplicarSeparacao(on) {
    separarLojas = on !== false;
    document.querySelectorAll('.wa-tabs').forEach(function (el) {
      el.classList.remove('hidden', 'is-off');
      el.setAttribute('aria-hidden', 'false');
    });
    document.querySelectorAll('.wa-tab').forEach(function (b) {
      var l = b.getAttribute('data-loja') || '';
      if (l === 'arquivadas') {
        b.classList.remove('hidden');
        return;
      }
      if (l === 'todas') {
        b.classList.toggle('hidden', separarLojas);
        return;
      }
      b.classList.toggle('hidden', !separarLojas);
    });
    if (!separarLojas) {
      if (loja !== 'arquivadas') loja = 'todas';
      try {
        localStorage.setItem(TAB_KEY, loja);
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
    var qLoja = chaveLista();
    return fetchJson('/api/atendimento-whatsapp/conversas/?loja=' + encodeURIComponent(qLoja)).then(
      function (j) {
        if (!j || !j.ok) return;
        var rowsLista = j.conversas || [];
        cacheListas[qLoja] = rowsLista;
        if (qLoja === chaveLista()) {
          pintarLista(rowsLista);
          alinharChatComLista(rowsLista);
          adiantarMsgsVisiveis(rowsLista);
        }
        pedirListaOutra();
      }
    );
  }

  var listaVistoChat = '';

  var msgsAdiantadas = {};
  var adiantarRodando = false;

  function adiantarMsgsVisiveis(rows) {
    var fila = [];
    for (var i = 0; i < rows.length && fila.length < 4; i++) {
      var id = String((rows[i] && rows[i].id) || '');
      if (!id || cacheMsgs[id] || msgsAdiantadas[id]) continue;
      fila.push(id);
    }
    function passo() {
      if (adiantarRodando || abrindoConversa) return;
      var cid = fila.shift();
      if (!cid) return;
      if (cacheMsgs[cid]) {
        passo();
        return;
      }
      msgsAdiantadas[cid] = true;
      adiantarRodando = true;
      fetchJson('/api/atendimento-whatsapp/mensagens/?conversa_id=' + cid + '&limit=40', { timeoutMs: 20000 }).then(
        function (j) {
          if (j && j.ok && !cacheMsgs[cid]) guardarMsgs(cid, j.mensagens || [], false);
        }
      ).finally(function () {
        adiantarRodando = false;
        if (!abrindoConversa) passo();
      });
    }
    window.setTimeout(passo, 400);
  }

  function alinharChatComLista(rows) {
    if (!convId) return;
    var aberta = null;
    for (var i = 0; i < rows.length; i++) {
      if (Number(rows[i].id) === Number(convId)) {
        aberta = rows[i];
        break;
      }
    }
    if (!aberta) return;
    if (aberta.foto_url && aberta.foto_url !== convFoto) {
      convFoto = aberta.foto_url;
      pintarTopoAvatar(convNome, convFoto);
    }
    var chave = String(aberta.ultima_em || '') + '|' + String(aberta.ultima_preview || '');
    if (listaVistoChat && listaVistoChat !== chave) pollMsgs();
    listaVistoChat = chave;
  }

  function pedirListaOutra() {
    if (listaOutraPedida) return;
    listaOutraPedida = true;
    var outra = chaveLista() === 'arquivadas' ? (separarLojas ? 'pendente' : 'todas') : 'arquivadas';
    fetchJson('/api/atendimento-whatsapp/conversas/?loja=' + encodeURIComponent(outra)).then(function (j) {
      if (!j || !j.ok) return;
      cacheListas[outra] = j.conversas || [];
    });
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

  var convAbreSeq = 0;
  var abrindoConversa = false;
  var cacheListas = {};
  var cacheMsgs = {};
  var cacheMsgsOrdem = [];
  var listaOutraPedida = false;

  function chaveLista() {
    return loja === 'arquivadas' ? 'arquivadas' : separarLojas ? loja : 'todas';
  }

  function guardarMsgs(id, rows, append) {
    var chave = String(id || '');
    if (!chave) return;
    var prev = cacheMsgs[chave] || [];
    var next = append ? prev.concat(rows || []) : (rows || []).slice();
    if (next.length > 120) next = next.slice(next.length - 120);
    cacheMsgs[chave] = next;
    var ix = cacheMsgsOrdem.indexOf(chave);
    if (ix >= 0) cacheMsgsOrdem.splice(ix, 1);
    cacheMsgsOrdem.push(chave);
    while (cacheMsgsOrdem.length > 8) {
      var velha = cacheMsgsOrdem.shift();
      delete cacheMsgs[velha];
    }
  }

  function zerarBadgeItemLista(id) {
    var btn = document.querySelector('#wa-lista .wa-item[data-id="' + String(id) + '"]');
    if (!btn) return;
    var unread = btn.querySelector('.wa-unread');
    if (unread) unread.remove();
    btn.classList.remove('has-new');
  }

  function abrirConversa(id) {
    var seq = ++convAbreSeq;
    convId = Number(id) || 0;
    afterId = 0;
    var item = document.querySelector('#wa-lista .wa-item[data-id="' + convId + '"]');
    document.querySelectorAll('#wa-lista .wa-item.is-on').forEach(function (el) {
      if (el !== item) el.classList.remove('is-on');
    });
    if (item) item.classList.add('is-on');
    convLoja = (item && item.getAttribute('data-loja')) || loja || '';
    convArquivada = !!(item && item.getAttribute('data-arquivada') === '1') || loja === 'arquivadas';
    convNome = (item && item.getAttribute('data-nome')) || 'Conversa #' + convId;
    convTel = (item && item.getAttribute('data-tel')) || '';
    convFoto = (item && item.getAttribute('data-foto')) || '';
    convMsgRev = Number((item && item.getAttribute('data-msg-rev')) || 0) || 0;
    setDigitando(!!(item && item.getAttribute('data-digitando') === '1'));
    limparReply();
    var nomeEl = $('wa-topo-nome');
    if (nomeEl) nomeEl.textContent = convNome;
    pintarTopoAvatar(convNome, convFoto);
    mostrarBotoesChat(true);
    pintarAcoesArquivo();
    pintarStatusDoChat();
    if (separarLojas && !convArquivada) {
      $('wa-move').classList.remove('hidden');
      $('wa-move').classList.add('flex');
      pintarXfer();
    } else {
      $('wa-move').classList.add('hidden');
    }
    telaCel(true);
    pintarQuick();
    var msgsEl = $('wa-msgs');
    var guardado = cacheMsgs[String(convId)];
    if (msgsEl && guardado && guardado.length) {
      pintarMsgs(guardado, false);
      afterId = guardado[guardado.length - 1].id || 0;
    } else if (msgsEl && item) {
      var prevTxt = '';
      var prevHora = '';
      var pTxt = item.querySelector('.wa-p-txt');
      var pHora = item.querySelector('.wa-t');
      prevTxt = pTxt ? String(pTxt.textContent || '').trim() : '';
      prevHora = pHora ? String(pHora.textContent || '').trim() : '';
      if (prevTxt) {
        pintarMsgs([{ id: 'previa-' + convId, direcao: 'in', texto: prevTxt, hora: prevHora, autor: '' }], false);
      } else {
        msgsEl.innerHTML = '';
      }
    } else if (msgsEl) {
      msgsEl.innerHTML = '';
    }
    zerarBadgeItemLista(convId);
    abrindoConversa = true;
    fetchJson('/api/atendimento-whatsapp/mensagens/?conversa_id=' + convId + '&limit=40', { timeoutMs: 20000 }).then(
      function (j) {
        if (seq !== convAbreSeq) return;
        if (!j || !j.ok) {
          if (msgsEl) {
            msgsEl.innerHTML =
              '<p class="text-sm font-semibold text-slate-500 px-1 py-2">Não carregou. Clique de novo.</p>';
          }
          return;
        }
        var rows = j.mensagens || [];
        pintarMsgs(rows, false);
        guardarMsgs(convId, rows, false);
        if (rows.length) afterId = rows[rows.length - 1].id;
        rows.forEach(function (m) {
          if (m.direcao === 'in' && m.id > lastSeenIn) lastSeenIn = m.id;
        });
        fetch('/api/atendimento-whatsapp/marcar-lida/', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf() },
          body: JSON.stringify({ conversa_id: convId }),
        }).catch(function () {});
        fetchJson('/api/atendimento-whatsapp/ficha/?conversa_id=' + convId).then(function (fj) {
          if (seq !== convAbreSeq) return;
          var f = fj && fj.ficha;
          if (!f) return;
          if (f.nome) {
            convNome = nomeExibicao(f);
            if (nomeEl) nomeEl.textContent = convNome;
            pintarTopoAvatar(convNome, convFoto || f.foto_url || '');
          }
          if (f.telefone) convTel = f.telefone;
          if (f.loja) convLoja = f.loja;
        });
      }
    ).finally(function () {
      if (seq === convAbreSeq) abrindoConversa = false;
    });
  }

  function limparChatAberto() {
    convId = 0;
    convLoja = '';
    convTel = '';
    convNome = '';
    convFoto = '';
    convArquivada = false;
    afterId = 0;
    convMsgRev = 0;
    limparReply();
    setDigitando(false);
    var nomeEl = $('wa-topo-nome');
    if (nomeEl) nomeEl.textContent = 'Conversa';
    pintarTopoAvatar('?', '');
    mostrarBotoesChat(false);
    pintarAcoesArquivo();
    if ($('wa-msgs')) $('wa-msgs').innerHTML = '';
    if ($('wa-quick')) {
      $('wa-quick').classList.add('hidden');
      $('wa-quick').innerHTML = '';
    }
    if ($('wa-move')) $('wa-move').classList.add('hidden');
    var ep = $('wa-emoji-panel');
    if (ep) ep.classList.add('hidden');
  }

  function pollMsgs() {
    if (!convId) return Promise.resolve();
    var idSnap = convId;
    var seqSnap = convAbreSeq;
    var afterSnap = afterId;
    return fetchJson(
      '/api/atendimento-whatsapp/mensagens/?conversa_id=' + idSnap + '&after_id=' + afterSnap
    ).then(function (j) {
      if (idSnap !== convId || seqSnap !== convAbreSeq) return;
      var rows = (j && j.mensagens) || [];
      if (!rows.length) return;
      pintarMsgs(rows, true);
      guardarMsgs(idSnap, rows, true);
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
      sairSelecao();
      var qAba = chaveLista();
      if (cacheListas[qAba]) pintarLista(cacheListas[qAba]);
      carregarLista();
    });
  });

  document.querySelectorAll('[data-filtro]').forEach(function (b) {
    b.addEventListener('click', function () {
      trocarFiltroLista(b.getAttribute('data-filtro') || 'hora');
    });
  });

  var msgsBoxErro = $('wa-msgs');
  if (msgsBoxErro) {
    msgsBoxErro.addEventListener(
      'error',
      function (ev) {
        var t = ev.target;
        if (!t || !t.classList) return;
        var txt = '';
        if (t.classList.contains('wa-pic')) txt = 'Foto';
        else if (t.classList.contains('wa-aud')) txt = 'Áudio';
        if (!txt || !t.getAttribute('src')) return;
        if (t.getAttribute('data-wa-midia-visto')) return;
        t.setAttribute('data-wa-midia-visto', '1');
        var url = t.getAttribute('src');
        fetch(url, { credentials: 'same-origin' })
          .then(function (r) {
            if (r.body && r.body.cancel) r.body.cancel();
            if (r.status !== 404) return;
            var s = document.createElement('span');
            s.className = 'text-xs text-slate-500';
            s.textContent = txt;
            if (t.parentNode) t.parentNode.replaceChild(s, t);
          })
          .catch(function () {});
      },
      true
    );
  }

  function idsMarcados() {
    return Object.keys(selMap)
      .map(function (id) {
        return Number(id) || 0;
      })
      .filter(Boolean);
  }

  function pintarSelBar() {
    var bar = $('wa-selbar');
    var nEl = $('wa-sel-n');
    var btnModo = $('wa-marcar');
    var n = idsMarcados().length;
    if (bar) bar.hidden = !selModo;
    if (nEl) nEl.textContent = n + (n === 1 ? ' marcada' : ' marcadas');
    if (btnModo) {
      btnModo.classList.toggle('is-on', selModo);
      btnModo.setAttribute('aria-pressed', selModo ? 'true' : 'false');
    }
    var naFila = loja !== 'arquivadas';
    var arq = $('wa-sel-arq');
    var reab = $('wa-sel-reab');
    if (arq) arq.hidden = !naFila;
    if (reab) reab.hidden = naFila;
    ['wa-sel-arq', 'wa-sel-reab', 'wa-sel-lidas', 'wa-sel-del'].forEach(function (id) {
      var b = $(id);
      if (b) b.disabled = n < 1;
    });
  }

  function sairSelecao() {
    selModo = false;
    selMap = {};
    document.querySelectorAll('#wa-lista .wa-item.is-sel').forEach(function (el) {
      el.classList.remove('is-sel');
    });
    pintarSelBar();
  }

  function agirSelecao(acao) {
    var ids = idsMarcados();
    if (!ids.length) return;
    if (
      acao === 'apagar' &&
      !window.confirm(
        'Apagar ' +
          ids.length +
          ' conversa' +
          (ids.length > 1 ? 's' : '') +
          ' da lista? (Só no Agro — não apaga no celular.)'
      )
    ) {
      return;
    }
    fetchJson('/api/atendimento-whatsapp/lote/', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf() },
      body: JSON.stringify({ acao: acao, ids: ids }),
    }).then(function (j) {
      if (!j || !j.ok) {
        window.alert((j && j.erro) || 'Não fez.');
        return;
      }
      if (ids.indexOf(Number(convId)) >= 0) {
        limparChatAberto();
        telaCel(false);
      }
      sairSelecao();
      carregarEstado();
      carregarLista();
    });
  }

  var btnMarcar = $('wa-marcar');
  if (btnMarcar) {
    btnMarcar.addEventListener('click', function () {
      if (selModo) sairSelecao();
      else {
        selModo = true;
        pintarSelBar();
      }
    });
  }
  var btnSelTodas = $('wa-sel-todas');
  if (btnSelTodas) {
    btnSelTodas.addEventListener('click', function () {
      document.querySelectorAll('#wa-lista .wa-item[data-id]').forEach(function (el) {
        var id = el.getAttribute('data-id') || '';
        if (!id) return;
        selMap[id] = true;
        el.classList.add('is-sel');
      });
      pintarSelBar();
    });
  }
  var btnSelFim = $('wa-sel-fim');
  if (btnSelFim) btnSelFim.addEventListener('click', sairSelecao);
  var btnSelArq = $('wa-sel-arq');
  if (btnSelArq) btnSelArq.addEventListener('click', function () { agirSelecao('arquivar'); });
  var btnSelReab = $('wa-sel-reab');
  if (btnSelReab) btnSelReab.addEventListener('click', function () { agirSelecao('reabrir'); });
  var btnSelLidas = $('wa-sel-lidas');
  if (btnSelLidas) btnSelLidas.addEventListener('click', function () { agirSelecao('lidas'); });
  var btnSelDel = $('wa-sel-del');
  if (btnSelDel) btnSelDel.addEventListener('click', function () { agirSelecao('apagar'); });

  $('wa-lista').addEventListener('click', function (ev) {
    var btn = ev.target.closest('[data-id]');
    if (!btn) return;
    if (selModo) {
      var id = btn.getAttribute('data-id') || '';
      if (!id) return;
      if (selMap[id]) delete selMap[id];
      else selMap[id] = true;
      btn.classList.toggle('is-sel', !!selMap[id]);
      pintarSelBar();
      return;
    }
    abrirConversa(btn.getAttribute('data-id'));
  });

  var menuCtx = document.createElement('div');
  menuCtx.id = 'wa-ctx';
  menuCtx.className = 'wa-ctx';
  menuCtx.hidden = true;
  document.body.appendChild(menuCtx);

  function fecharCtx() {
    menuCtx.hidden = true;
    menuCtx.innerHTML = '';
  }

  function abrirCtx(x, y, itens) {
    menuCtx.innerHTML = itens
      .map(function (it) {
        return (
          '<button type="button" class="wa-ctx-btn" data-ctx="' +
          it.id +
          '">' +
          escapeHtml(it.label) +
          '</button>'
        );
      })
      .join('');
    menuCtx.hidden = false;
    var largura = 240;
    menuCtx.style.left = Math.max(8, Math.min(x, window.innerWidth - largura - 8)) + 'px';
    menuCtx.style.top = Math.max(8, Math.min(y, window.innerHeight - 120)) + 'px';
  }

  $('wa-lista').addEventListener('contextmenu', function (ev) {
    var btn = ev.target.closest('.wa-item[data-id]');
    if (!btn) return;
    ev.preventDefault();
    var arq = btn.getAttribute('data-arquivada') === '1';
    abrirCtx(ev.clientX, ev.clientY, [
      arq
        ? { id: 'reabrir', label: 'Reabrir conversa' }
        : { id: 'arquivar', label: 'Arquivar conversa' },
      { id: 'apagar', label: 'Apagar conversa' },
    ]);
    menuCtx.setAttribute('data-cid', btn.getAttribute('data-id') || '');
  });

  menuCtx.addEventListener('click', function (ev) {
    var b = ev.target.closest('[data-ctx]');
    if (!b) return;
    ev.stopPropagation();
    var id = menuCtx.getAttribute('data-cid');
    var acao = b.getAttribute('data-ctx');
    fecharCtx();
    if (acao === 'apagar') {
      if (!window.confirm('Apagar esta conversa da lista? (Só no Agro — não apaga no celular.)')) return;
    }
    var url =
      acao === 'reabrir'
        ? '/api/atendimento-whatsapp/reabrir/'
        : acao === 'apagar'
          ? '/api/atendimento-whatsapp/excluir/'
          : '/api/atendimento-whatsapp/concluir/';
    fetchJson(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf() },
      body: JSON.stringify({ conversa_id: Number(id) }),
    }).then(function (j) {
      if (!j || !j.ok) {
        window.alert((j && j.erro) || 'Não fez.');
        return;
      }
      if (String(convId) === String(id)) {
        limparChatAberto();
        telaCel(false);
      }
      carregarEstado();
      carregarLista();
    });
  });

  document.addEventListener('click', fecharCtx);
  document.addEventListener('keydown', function (ev) {
    if (ev.key === 'Escape') fecharCtx();
  });

  var MIDIA_TETO = 3 * 1024 * 1024;
  var recStream = null;
  var recChunks = [];
  var recObj = null;

  function horaAgoraWa() {
    var d = new Date();
    var h = d.getHours();
    var m = d.getMinutes();
    return (h < 10 ? '0' : '') + h + ':' + (m < 10 ? '0' : '') + m;
  }

  var enviandoMsg = false;
  var enviandoTimer = 0;

  function liberarEnvioUi() {
    enviandoMsg = false;
    if (enviandoTimer) {
      window.clearTimeout(enviandoTimer);
      enviandoTimer = 0;
    }
    var btn = $('wa-send');
    if (btn) btn.disabled = false;
    atualizarBarra();
  }

  function avisarEnvio(msg) {
    if (typeof waToast === 'function') waToast(msg, false);
    else window.alert(msg);
  }

  function enviarPayload(payload, textoVolta) {
    var btn = $('wa-send');
    if (!convId) {
      avisarEnvio('Abra uma conversa na lista antes de enviar.');
      return;
    }
    if (!nomeOperadorPin()) {
      avisarEnvio('Digite o PIN para atender.');
      trocarOperadorPin();
      return;
    }
    if (enviandoMsg) {
      avisarEnvio('Ainda enviando… aguarde 1 instante.');
      return;
    }
    var tok = csrf();
    if (!tok) {
      avisarEnvio('Sessão sem token — Ctrl+F5 e entre de novo.');
      return;
    }
    enviandoMsg = true;
    if (btn) btn.disabled = true;
    // Limpa o campo só com o envio travado (evita seta morta + texto sumido)
    if (textoVolta && $('wa-input')) {
      $('wa-input').value = '';
      atualizarBarra();
    }
    // Trava máxima: se a rede engasgar, a seta NÃO fica morta
    enviandoTimer = window.setTimeout(function () {
      if (!enviandoMsg) return;
      liberarEnvioUi();
      avisarEnvio('Envio travou — texto restaurado. Tente de novo.');
      if (textoVolta && $('wa-input') && !$('wa-input').value) {
        $('wa-input').value = textoVolta;
        atualizarBarra();
      }
    }, 15000);

    if (replyToId && !payload.reply_to_id) payload.reply_to_id = replyToId;
    var opEnvio = nomeOperadorPin();
    if (opEnvio && !payload.operador_nome) payload.operador_nome = opEnvio;
    var replySnap = replyToTxt;
    var replyIdSnap = replyToId;
    limparReply();
    // Bolha na hora (sensação de enviado) — o Zap do cliente ainda passa pela ponte.
    var tempId = 'tmp-' + Date.now();
    var tipo = String(payload.tipo_midia || '');
    var textoPrev = String(payload.texto || '').trim();
    if (!textoPrev) {
      if (tipo === 'image' || tipo === 'sticker') textoPrev = '[imagem]';
      else if (tipo === 'audio') textoPrev = '[áudio]';
    }
    var midiaPrev = '';
    if ((tipo === 'image' || tipo === 'sticker') && payload.midia_b64) {
      midiaPrev = 'data:' + (payload.mime || 'image/jpeg') + ';base64,' + payload.midia_b64;
    }
    pintarMsgs(
      [
        {
          id: tempId,
          direcao: 'out',
          texto: textoPrev,
          autor: nomeOperadorPin() || 'Você',
          hora: horaAgoraWa(),
          tipo_midia: tipo,
          midia_url: midiaPrev,
          pendente: true,
          ack: 0,
          reply_texto: replyIdSnap ? replySnap : '',
          reply_wa_id: replyIdSnap ? 'tmp' : '',
        },
      ],
      true
    );

    fetchJson('/api/atendimento-whatsapp/enviar/', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-CSRFToken': tok },
      body: JSON.stringify(payload),
      timeoutMs: 12000,
    })
      .then(function (j) {
        var tmp = document.querySelector('[data-msg-id="' + tempId + '"]');
        if (tmp) tmp.remove();
        if (!j || !j.ok) {
          if (j && j.precisa_pin) {
            try { localStorage.removeItem('gm_sspin_operador'); } catch (_) {}
            pintarOperadorPin();
            trocarOperadorPin();
          }
          avisarEnvio((j && j.erro) || 'Não enviou.');
          if (textoVolta && $('wa-input')) {
            $('wa-input').value = textoVolta;
            atualizarBarra();
          }
          return;
        }
        if (j.mensagem) {
          pintarMsgs([j.mensagem], true);
          afterId = Math.max(afterId, j.mensagem.id || 0);
        } else {
          pollMsgs();
        }
        carregarLista();
      })
      .catch(function () {
        var tmp = document.querySelector('[data-msg-id="' + tempId + '"]');
        if (tmp) tmp.remove();
        avisarEnvio('Falha de rede ao enviar.');
        if (textoVolta && $('wa-input')) {
          $('wa-input').value = textoVolta;
          atualizarBarra();
        }
      })
      .finally(function () {
        liberarEnvioUi();
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
      enviarFotoArquivo(f);
    });
  }

  function enviarFotoArquivo(f) {
    if (!f || !convId) return;
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
  }

  document.addEventListener('paste', function (ev) {
    if (!convId) return;
    var alvo = ev.target;
    if (alvo && alvo.closest && (alvo.closest('#wa-bot-form') || alvo.closest('#wa-busca'))) return;
    var cd = ev.clipboardData;
    if (!cd || !cd.items) return;
    var file = null;
    for (var i = 0; i < cd.items.length; i++) {
      var it = cd.items[i];
      if (it.kind === 'file' && String(it.type || '').indexOf('image/') === 0) {
        file = it.getAsFile();
        break;
      }
    }
    if (!file) return;
    ev.preventDefault();
    enviarFotoArquivo(file);
  });

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

  function dispararTextoComposer() {
    var inp = $('wa-input');
    var t = ((inp && inp.value) || '').trim();
    if (!convId) {
      avisarEnvio('Abra uma conversa na lista antes de enviar.');
      return;
    }
    if (!t) return;
    if (enviandoMsg) {
      avisarEnvio('Ainda enviando… aguarde.');
      return;
    }
    // enviarPayload limpa o campo só depois de travar o envio
    enviarPayload({ conversa_id: convId, texto: t }, t);
  }

  $('wa-form').addEventListener('submit', function (ev) {
    ev.preventDefault();
    dispararTextoComposer();
  });

  var sendBtn = $('wa-send');
  if (sendBtn) {
    // type=button evita click+submit duplo (1º some o texto, 2º seta morta)
    try {
      sendBtn.type = 'button';
    } catch (_) {}
    sendBtn.addEventListener('click', function (ev) {
      ev.preventDefault();
      ev.stopPropagation();
      dispararTextoComposer();
    });
  }
  if (inpBarra) {
    inpBarra.addEventListener('keydown', function (ev) {
      if (ev.key !== 'Enter') return;
      if (ev.shiftKey) return;
      ev.preventDefault();
      dispararTextoComposer();
    });
  }

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
            var ini = (titulo || '?').trim().charAt(0).toUpperCase() || '?';
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
              '"><span class="wa-av"><span class="wa-av-ini">' +
              escapeHtml(ini) +
              '</span></span><div class="wa-item-body"><div class="wa-row1"><div class="wa-n">' +
              escapeHtml(titulo) +
              '</div></div><div class="wa-row2"><div class="wa-p"><span class="wa-p-txt">' +
              escapeHtml(rotuloOrigem(c.origem)) +
              (c.telefone && titulo !== c.telefone ? ' · toque p/ abrir' : '') +
              '</span></div></div></div></button>'
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
  function executarTrocarZap() {
    if (
      !window.confirm(
        'Desligar este WhatsApp neste PC?\nVai precisar ler o QR ou gerar um código de novo (pode ser outro número).'
      )
    ) {
      return;
    }
    var ponteOff = String(ultimaStatusTxt || '').trim().toLowerCase() === 'off';
    if (
      !ponteOff &&
      $('wa-status') &&
      String(($('wa-status').textContent || '').trim()).toLowerCase() === 'off'
    ) {
      ponteOff = true;
    }
    if (ponteOff) {
      window.alert(
        'A ponte está OFF (janela preta fechada).\n\n' +
          '1) Feche qualquer iniciar.bat\n' +
          '2) Apague a pasta whatsapp_atendimento\\auth\n' +
          '3) Abra de novo SÓ UM iniciar.bat\n' +
          '4) Volte aqui e leia o QR / código\n\n' +
          'O botão Trocar Zap só funciona com a janela preta ligada.'
      );
      return;
    }
    var btnTrocar = $('wa-trocar');
    if (btnTrocar) btnTrocar.disabled = true;
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
        window.alert(
          'Pedido enviado à ponte.\nEm alguns segundos some o Online e aparece QR/código.\nSe nada mudar, feche o iniciar.bat, apague a pasta auth e ligue de novo.'
        );
        window.setTimeout(carregarEstado, 1200);
        window.setTimeout(carregarEstado, 3500);
        window.setTimeout(carregarEstado, 8000);
      })
      .catch(function () {
        window.alert('Falha de rede ao trocar o Zap.');
      })
      .finally(function () {
        if (btnTrocar) btnTrocar.disabled = false;
      });
  }

  var btnTrocar = $('wa-trocar');
  if (btnTrocar) {
    btnTrocar.addEventListener('click', function () {
      executarTrocarZap();
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
          window.alert((j && j.erro) || 'Não arquivou.');
          return;
        }
        limparChatAberto();
        telaCel(false);
        carregarEstado();
        carregarLista();
      });
    });
  }
  var btnReabrir = $('wa-reabrir');
  if (btnReabrir) {
    btnReabrir.addEventListener('click', function () {
      if (!convId) return;
      fetchJson('/api/atendimento-whatsapp/reabrir/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf() },
        body: JSON.stringify({ conversa_id: convId }),
      }).then(function (j) {
        if (!j || !j.ok) {
          window.alert((j && j.erro) || 'Não reabriu.');
          return;
        }
        limparChatAberto();
        telaCel(false);
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
    topoWho.addEventListener('click', function (ev) {
      if (!convId) return;
      if (ev.target && ev.target.closest && ev.target.closest('#wa-topo-av')) return;
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
    var alvo = ev.target;
    if (!alvo || !alvo.closest) return;
    var av = alvo.closest('#wa-topo-av');
    if (av) {
      var fotoAv = av.querySelector('img');
      var urlAv = fotoAv ? fotoAv.getAttribute('src') || '' : '';
      if (urlAv) {
        ev.preventDefault();
        abrirPicLite(urlAv);
      }
      return;
    }
    var pic = alvo.closest('.wa-pic[data-wa-pic-full]');
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

  function nomeCurtoOperador(n) {
    n = String(n || '').trim();
    if (!n) return '';
    var parts = n.split(/\s+/).filter(Boolean);
    if (parts.length <= 1) return parts[0] || n;
    // Nome + 1º sobrenome — cabe no card sem amontoar
    return (parts[0] + ' ' + parts[1]).slice(0, 22);
  }

  function pintarOperadorPin() {
    var btn = $('wa-operador-pin');
    var nomeEl = $('wa-operador-pin-nome');
    if (!btn) return;
    var n = nomeOperadorPin();
    var curto = nomeCurtoOperador(n);
    if (nomeEl) nomeEl.textContent = curto || 'PIN?';
    else btn.textContent = curto || 'PIN?';
    if (n) {
      btn.classList.remove('is-empty');
      btn.title = 'Assinatura: ' + n + ' — clique para trocar o PIN';
      btn.setAttribute('aria-label', 'Trocar PIN — agora ' + n);
    } else {
      btn.classList.add('is-empty');
      btn.title = 'Ninguém no PIN — clique para identificar';
      btn.setAttribute('aria-label', 'Identificar com PIN');
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
  fetch('/api/pdv/operador/', { credentials: 'same-origin', headers: { Accept: 'application/json' } })
    .then(function (r) { return r.json(); })
    .then(function (j) {
      if (j && j.ok && j.fresco && j.operador) {
        try { localStorage.setItem('gm_sspin_operador', String(j.operador).trim()); } catch (_) {}
        pintarOperadorPin();
        return;
      }
      try { localStorage.removeItem('gm_sspin_operador'); } catch (_) {}
      pintarOperadorPin();
      trocarOperadorPin();
    })
    .catch(function () {});

  if (ehOverlay) {
    document.body.classList.add('wa-in-overlay');
    var topBar = document.querySelector('.wa-top');
    if (topBar) topBar.classList.add('hidden');
    syncOverlayChrome({
      showWaChrome: true,
      showWaBot: true,
      showWaTrocar: false,
      waStatus: '…',
      waDot: 'off',
      botHref: '/atendimento-whatsapp/bot/?agro_pdv_overlay=1',
    });
    window.addEventListener('message', function (ev) {
      try {
        if (!ev || ev.origin !== location.origin) return;
        var d = ev.data || {};
        if (d.type === 'agro-wa-trocar') executarTrocarZap();
      } catch (_) {}
    });
  }

  setTab();
  pintarEstadoFiltro();
  pedirAviso();
  document.addEventListener('click', pedirAviso, { once: true });
  carregarEstado();
  carregarLista();
  carregarStatus();
  var tickPoll = 0;
  function waJanelaAtiva() {
    try {
      if (document.hidden) return false;
      if (document.visibilityState && document.visibilityState !== 'visible') return false;
      // Janela Zap ao lado do PDV: sem foco = poll fundo (senão engasga a loja)
      if (typeof document.hasFocus === 'function' && !document.hasFocus()) return false;
    } catch (e) {}
    return true;
  }
  /**
   * Poll leve — o Zap aberto disputava o mesmo Render do PDV.
   * Foco no Zap: msgs 2,5s · lista/estado 10s · status 60s
   * PDV na frente / aba escondida: msgs 15s · estado 30s · lista 60s · status 120s
   */
  setInterval(function () {
    tickPoll += 1;
    if (abrindoConversa) return;
    var ativa = waJanelaAtiva();
    var msgsEvery = ativa ? 1 : 6;
    var estadoEvery = ativa ? 4 : 12;
    var listaEvery = ativa ? 4 : 24;
    var statusEvery = ativa ? 24 : 48;
    if (convId && tickPoll % msgsEvery === 0) {
      pollMsgs();
    }
    if (tickPoll % estadoEvery === 0) {
      carregarEstado();
    }
    if (tickPoll % listaEvery === 0) {
      carregarLista();
    }
    if (tickPoll % statusEvery === 0) {
      carregarStatus();
    }
  }, 2500);
  function waRefreshSeAtiva() {
    if (!waJanelaAtiva()) return;
    carregarEstado();
    if (convId) pollMsgs();
  }
  document.addEventListener('visibilitychange', waRefreshSeAtiva);
  window.addEventListener('focus', waRefreshSeAtiva);

  /* —— Passo 3/4: emoji, scroll, responder, reagir —— */
  (function initWaE34() {
    var panel = $('wa-emoji-panel');
    var emojiBtn = $('wa-emoji');

    function fecharEmojiPanel() {
      reactMsgId = 0;
      if (panel) {
        panel.classList.add('hidden');
        panel.removeAttribute('data-modo');
      }
      if (emojiBtn) emojiBtn.setAttribute('aria-expanded', 'false');
    }

    function abrirEmojiPanel(modo, msgId) {
      if (!panel) return;
      reactMsgId = Number(msgId) || 0;
      panel.setAttribute('data-modo', modo === 'react' ? 'react' : 'digitar');
      panel.classList.remove('hidden');
      if (emojiBtn) emojiBtn.setAttribute('aria-expanded', 'true');
    }

    function enviarReacao(mid, em) {
      fetchJson('/api/atendimento-whatsapp/reagir/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf() },
        body: JSON.stringify({ mensagem_id: Number(mid) || 0, emoji: String(em || '').trim().slice(0, 8) }),
      }).then(function (j) {
        if (!j || !j.ok) {
          avisarEnvio((j && j.erro) || 'Não reagiu.');
          return;
        }
        recarregarMsgsAbertas();
      });
    }

    var msgs = $('wa-msgs');
    if (msgs) {
      msgs.addEventListener('scroll', atualizarScrollFim);
      msgs.addEventListener('click', function (ev) {
        var act = ev.target.closest('[data-wa-act]');
        if (!act) return;
        var mid = act.getAttribute('data-msg-id') || '';
        var tipo = act.getAttribute('data-wa-act') || '';
        var bubble = act.closest('.wa-b');
        var txt = '';
        if (bubble) {
          var q = bubble.querySelector('.wa-quote-txt');
          var clone = bubble.cloneNode(true);
          clone.querySelectorAll('.wa-meta,.wa-msg-acts,.wa-msg-del,.wa-reacoes,.wa-quote,.wa-pic,.wa-aud').forEach(function (n) {
            n.remove();
          });
          txt = String(clone.textContent || '').trim().slice(0, 120);
          if (!txt && q) txt = String(q.textContent || '').trim();
        }
        if (tipo === 'reply') {
          setReplyBar(mid, txt || 'Mensagem');
          return;
        }
        if (tipo === 'react') {
          abrirEmojiPanel('react', mid);
        }
      });
    }
    var scrollBtn = $('wa-scroll-fim');
    if (scrollBtn) {
      scrollBtn.addEventListener('click', function () {
        var el = $('wa-msgs');
        if (el) el.scrollTop = el.scrollHeight;
        atualizarScrollFim();
      });
    }
    var replyX = $('wa-reply-x');
    if (replyX) replyX.addEventListener('click', limparReply);

    if (panel) {
      panel.innerHTML = EMOJIS_WA.map(function (e) {
        return (
          '<button type="button" class="wa-emoji-btn" data-emoji="' +
          e +
          '">' +
          e +
          '</button>'
        );
      }).join('');
      panel.addEventListener('click', function (ev) {
        var b = ev.target.closest('[data-emoji]');
        if (!b) return;
        var em = b.getAttribute('data-emoji') || '';
        var modo = panel.getAttribute('data-modo') || 'digitar';
        if (modo === 'react' && reactMsgId) {
          var mid = reactMsgId;
          fecharEmojiPanel();
          enviarReacao(mid, em);
          return;
        }
        var inp = $('wa-input');
        if (!inp) return;
        var start = inp.selectionStart != null ? inp.selectionStart : inp.value.length;
        var end = inp.selectionEnd != null ? inp.selectionEnd : start;
        inp.value = inp.value.slice(0, start) + em + inp.value.slice(end);
        try {
          inp.setSelectionRange(start + em.length, start + em.length);
        } catch (e) {}
        inp.focus();
        atualizarBarra();
        fecharEmojiPanel();
      });
    }
    if (emojiBtn && panel) {
      emojiBtn.addEventListener('click', function () {
        if (!panel.classList.contains('hidden') && (panel.getAttribute('data-modo') || 'digitar') === 'digitar') {
          fecharEmojiPanel();
          return;
        }
        abrirEmojiPanel('digitar', 0);
      });
    }
    document.addEventListener('keydown', function (ev) {
      if (ev.key === 'Escape') fecharEmojiPanel();
    });
  })();
})();
