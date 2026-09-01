/**
 * Config do bot WhatsApp — grava Postgres.
 */
(function () {
  'use strict';

  var DIAS = [
    { v: 1, n: 'Seg' },
    { v: 2, n: 'Ter' },
    { v: 3, n: 'Qua' },
    { v: 4, n: 'Qui' },
    { v: 5, n: 'Sex' },
    { v: 6, n: 'Sáb' },
    { v: 0, n: 'Dom' },
  ];

  var CHECKS = [
    'bot_ligado',
    'horario_ativo',
    'ainda_atende_fora',
    'enviar_boas_vindas',
    'repetir_menu',
    'fiado_ligado',
    'fiado_manda_menu',
    'ausencia_ligada',
  ];

  function csrf() {
    var inp = document.querySelector('[name=csrfmiddlewaretoken]');
    if (inp && inp.value) return inp.value;
    var m = document.cookie.match(/csrftoken=([^;]+)/);
    return m ? decodeURIComponent(m[1]) : '';
  }

  function $(id) {
    return document.getElementById(id);
  }

  function form() {
    return $('wa-bot-form');
  }

  function aviso(ok, t) {
    var box = $('wa-bot-msg');
    if (!box) return;
    box.textContent = t;
    box.className = 'wa-toast show ' + (ok ? 'ok' : 'bad');
  }

  function montarDias(sel) {
    var box = $('wa-bot-dias');
    if (!box) return;
    box.innerHTML = '';
    var set = {};
    (sel || []).forEach(function (d) {
      set[Number(d)] = true;
    });
    DIAS.forEach(function (d) {
      var on = !!set[d.v];
      var lab = document.createElement('label');
      lab.className = 'wa-chip' + (on ? ' is-on' : '');
      lab.innerHTML =
        '<input type="checkbox" data-dia="' +
        d.v +
        '"' +
        (on ? ' checked' : '') +
        ' />' +
        d.n;
      lab.addEventListener('change', function () {
        var inp = lab.querySelector('input');
        lab.classList.toggle('is-on', !!(inp && inp.checked));
      });
      box.appendChild(lab);
    });
  }

  function lerDias() {
    var out = [];
    document.querySelectorAll('#wa-bot-dias [data-dia]').forEach(function (el) {
      if (el.checked) out.push(parseInt(el.getAttribute('data-dia'), 10));
    });
    return out;
  }

  function preencher(bot) {
    var f = form();
    if (!f || !bot) return;
    CHECKS.forEach(function (k) {
      var el = f.querySelector('[name="' + k + '"]');
      if (el) el.checked = !!bot[k];
    });
    [
      'nome_empresa',
      'atraso_resposta_seg',
      'atraso_entre_msgs_seg',
      'horario_ini',
      'horario_fim',
      'ordem',
      'msg_boas_vindas',
      'msg_menu',
      'msg_pedir_de_novo',
      'loja1_rotulo',
      'loja1_palavras',
      'msg_ok_loja1',
      'loja2_rotulo',
      'loja2_palavras',
      'msg_ok_loja2',
      'fiado_palavras',
      'fiado_max_parcelas',
      'msg_fiado_aberto',
      'msg_fiado_vazio',
      'msg_fiado_sem_cadastro',
      'msg_fiado_varios',
      'msg_fora_horario',
      'msg_ausencia',
    ].forEach(function (k) {
      var el = f.querySelector('[name="' + k + '"]');
      if (el && bot[k] != null) el.value = bot[k];
    });
    montarDias(bot.horario_dias || []);
  }

  function coletar() {
    var f = form();
    var o = {};
    CHECKS.forEach(function (k) {
      var el = f.querySelector('[name="' + k + '"]');
      o[k] = !!(el && el.checked);
    });
    Array.prototype.forEach.call(f.querySelectorAll('input, textarea, select'), function (el) {
      var n = el.name;
      if (!n || CHECKS.indexOf(n) >= 0) return;
      if (el.type === 'number') o[n] = parseInt(el.value || '0', 10);
      else o[n] = el.value;
    });
    o.horario_dias = lerDias();
    o.loja1_id = 'centro';
    o.loja2_id = 'vila';
    return o;
  }

  function carregar() {
    return fetch('/api/atendimento-whatsapp/bot/').then(function (r) {
      return r.json();
    }).then(function (j) {
      if (!j || !j.ok) throw new Error((j && j.erro) || 'Falha');
      preencher(j.bot);
    }).catch(function (e) {
      aviso(false, e.message || 'Não carregou');
    });
  }

  function salvar(payload) {
    return fetch('/api/atendimento-whatsapp/bot/salvar/', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf() },
      body: JSON.stringify(payload),
    }).then(function (r) {
      return r.json();
    });
  }

  var f = form();
  if (!f) return;
  montarDias([1, 2, 3, 4, 5, 6]);
  carregar();
  f.addEventListener('submit', function (ev) {
    ev.preventDefault();
    salvar({ bot: coletar() }).then(function (j) {
      if (!j || !j.ok) {
        aviso(false, (j && j.erro) || 'Não salvou');
        return;
      }
      preencher(j.bot);
      aviso(true, 'Salvo');
    }).catch(function () {
      aviso(false, 'Não salvou');
    });
  });
  var rst = $('wa-bot-reset');
  if (rst) {
    rst.addEventListener('click', function () {
      if (!window.confirm('Voltar ao padrão?')) return;
      salvar({ reset: true }).then(function (j) {
        if (!j || !j.ok) {
          aviso(false, (j && j.erro) || 'Não resetou');
          return;
        }
        preencher(j.bot);
        aviso(true, 'Padrão');
      });
    });
  }
  var nav = $('wa-bot-nav');
  if (nav) {
    nav.addEventListener('click', function (ev) {
      var b = ev.target.closest('button[data-panel]');
      if (!b) return;
      var id = b.getAttribute('data-panel');
      nav.querySelectorAll('button').forEach(function (x) {
        x.classList.toggle('is-on', x === b);
      });
      document.querySelectorAll('#wa-bot-form .wa-panel').forEach(function (p) {
        p.classList.toggle('is-on', p.getAttribute('data-panel') === id);
      });
    });
  }
})();
