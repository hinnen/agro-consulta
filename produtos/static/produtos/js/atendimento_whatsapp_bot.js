/**
 * Config do bot WhatsApp — grava Postgres.
 */
(function () {
  'use strict';

  var DIAS = [
    { v: 0, n: 'Dom' },
    { v: 1, n: 'Seg' },
    { v: 2, n: 'Ter' },
    { v: 3, n: 'Qua' },
    { v: 4, n: 'Qui' },
    { v: 5, n: 'Sex' },
    { v: 6, n: 'Sab' },
  ];

  var CHECKS = [
    'bot_ligado',
    'horario_ativo',
    'ainda_atende_fora',
    'aviso_fora_ligado',
    'aviso_fora_uma_vez',
    'aviso_fora_so_texto',
    'separar_lojas',
    'enviar_boas_vindas',
    'saudacao_so_primeira_do_dia',
    'saudacao_nao_repetir_hoje',
    'saudacao_so_em_horario',
    'saudacao_depois_menu',
    'arquivo_auto_ligado',
    'arquivo_auto_nunca_com_nao_lidas',
    'repetir_menu',
    'fiado_ligado',
    'fiado_manda_menu',
    'ausencia_ligada',
    'xfer_avisar_cliente',
    'feat_pdv_abre_zap',
    'feat_pdv_aviso_msg',
    'feat_respostas_prontas',
    'feat_xfer_nota',
    'feat_fiado_pix',
    'feat_orcamento_zap',
    'feat_lembrete_fiado',
    'feat_comprovante_venda',
    'feat_entrega_status',
    'feat_pedir_loja_aviso',
    'feat_lista_espera',
    'feat_fornecedor_zap',
    'feat_menu_curto',
    'feat_audio_texto',
    'feat_relatorio_dia',
    'feat_vip_tag',
    'feat_ponte_backup',
    'feat_horario_bot',
  ];

  var RECURSOS_UI = [
    ['feat_pdv_abre_zap', 'PDV abre o Zap', 'Ícone do PDV abre o chat (em vez de «Em breve»).'],
    ['feat_pdv_aviso_msg', 'Aviso no PDV', 'Som + badge quando chega mensagem nova.'],
    ['feat_respostas_prontas', 'Respostas prontas', 'Botões de texto rápido no chat.'],
    ['feat_xfer_nota', 'Nota ao transferir', 'Observação interna ao passar Centro↔Vila.'],
    ['feat_fiado_pix', 'Fiado + Pix', 'Lembra Pix após o saldo; se pedir, manda a chave configurada.'],
    ['feat_orcamento_zap', 'Orçamento no Zap', 'No PDV: botão «Loja» ao lado do Enviar — manda pelo Zap da loja.'],
    ['feat_lembrete_fiado', 'Lembrete fiado', 'Avisar cliente marcado (atraso) — sem disparo em massa.'],
    ['feat_comprovante_venda', 'Comprovante de venda', 'Texto «sua compra» no Zap após venda.'],
    ['feat_entrega_status', 'Status de entrega', 'Avisar cliente: saiu / a caminho / chegou.'],
    ['feat_pedir_loja_aviso', 'Pedir loja → Zap', 'Avisa a outra loja quando pedido muda de status.'],
    ['feat_lista_espera', 'Lista de espera', 'Avisar quando produto sem estoque chegar.'],
    ['feat_fornecedor_zap', 'Folha p/ fornecedor', 'Atalho de Compras pelo Zap da loja.'],
    ['feat_menu_curto', 'Menu curto (F·H·A)', 'F fiado · H horário · A atendente.'],
    ['feat_audio_texto', 'Áudio → texto', 'Transcrever áudio do cliente (quando ligado).'],
    ['feat_relatorio_dia', 'Relatório do dia', 'Resumo: chats e quem atendeu (PIN).'],
    ['feat_vip_tag', 'VIP / alerta no chat', 'Marcar cliente (fiado alto, sempre Vila…).'],
    ['feat_ponte_backup', 'Ponte backup', '2º PC se o 1º cair (só prepara).'],
    ['feat_horario_bot', 'Horário reforçado', 'Textos de horário mais claros no Bot.'],
  ];

  function montarRecursos() {
    var box = $('wa-bot-recursos');
    if (!box || box.getAttribute('data-ready') === '1') return;
    box.setAttribute('data-ready', '1');
    box.innerHTML = RECURSOS_UI.map(function (r) {
      return (
        '<label class="wa-sw" title="' +
        r[2].replace(/"/g, '&quot;') +
        '"><span><b>' +
        r[1] +
        '</b><br/><span class="text-xs font-semibold text-slate-500">' +
        r[2] +
        '</span></span><input type="checkbox" name="' +
        r[0] +
        '" /></label>'
      );
    }).join('');
  }

  var FONTES_NOME = [
    { v: 'cadastro', n: 'Cadastro da loja' },
    { v: 'agenda', n: 'Nome salvo no celular' },
    { v: 'perfil', n: 'Nome do perfil no Zap' },
    { v: 'telefone', n: 'Telefone' },
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

  function montarHorarioPorDia(mapa) {
    var box = $('wa-bot-horario-dias');
    if (!box) return;
    box.innerHTML = '';
    var src = mapa && typeof mapa === 'object' ? mapa : {};
    var temMapa = Object.keys(src).length > 0;
    DIAS.forEach(function (d) {
      var item = src[String(d.v)] || src[d.v] || {};
      var temDia = !!(src[String(d.v)] || src[d.v]);
      var ativo;
      if (temDia && Object.prototype.hasOwnProperty.call(item, 'ativo')) {
        ativo = !!(item.ativo === true || item.ativo === 1 || item.ativo === '1' || item.ativo === 'true');
      } else if (temMapa) {
        ativo = false;
      } else {
        ativo = d.v !== 0;
      }
      var ini = item.ini || '08:00';
      var fim = item.fim || '18:00';
      var row = document.createElement('div');
      row.className = 'wa-horario-dia' + (ativo ? '' : ' is-off');
      row.setAttribute('data-dia', String(d.v));
      row.innerHTML =
        '<label class="wa-horario-dia-nome">' +
        '<input type="checkbox" data-hpd-ativo="1"' +
        (ativo ? ' checked' : '') +
        ' />' +
        d.n +
        '</label>' +
        '<label class="wa-mini">Abre<input type="time" data-hpd-ini="1" value="' +
        String(ini).replace(/"/g, '') +
        '"' +
        (ativo ? '' : ' disabled') +
        ' /></label>' +
        '<label class="wa-mini">Fecha<input type="time" data-hpd-fim="1" value="' +
        String(fim).replace(/"/g, '') +
        '"' +
        (ativo ? '' : ' disabled') +
        ' /></label>';
      var chk = row.querySelector('[data-hpd-ativo]');
      var sync = function () {
        var on = !!(chk && chk.checked);
        row.classList.toggle('is-off', !on);
        row.querySelectorAll('[data-hpd-ini],[data-hpd-fim]').forEach(function (inp) {
          inp.disabled = !on;
        });
      };
      if (chk) chk.addEventListener('change', sync);
      box.appendChild(row);
    });
  }

  function lerHorarioPorDia() {
    var out = {};
    document.querySelectorAll('#wa-bot-horario-dias .wa-horario-dia').forEach(function (row) {
      var d = row.getAttribute('data-dia');
      if (d == null) return;
      var chk = row.querySelector('[data-hpd-ativo]');
      var ini = row.querySelector('[data-hpd-ini]');
      var fim = row.querySelector('[data-hpd-fim]');
      out[String(d)] = {
        ativo: !!(chk && chk.checked),
        ini: (ini && ini.value) || '08:00',
        fim: (fim && fim.value) || '18:00',
      };
    });
    return out;
  }

  function montarDias(sel) {
    /* legado: mantido vazio — dias vêm de horario_por_dia */
    var box = $('wa-bot-dias');
    if (box) box.innerHTML = '';
  }

  function lerDias() {
    var out = [];
    var mapa = lerHorarioPorDia();
    Object.keys(mapa).forEach(function (k) {
      if (mapa[k] && mapa[k].ativo) out.push(parseInt(k, 10));
    });
    return out;
  }

  function montarFontes(sel) {
    var ordem = String(sel || 'cadastro,agenda,perfil,telefone').split(',');
    var limpo = [];
    ordem.forEach(function (x) {
      var k = String(x || '').trim();
      if (k && limpo.indexOf(k) < 0) limpo.push(k);
    });
    FONTES_NOME.forEach(function (f) {
      if (limpo.indexOf(f.v) < 0) limpo.push(f.v);
    });
    [1, 2, 3, 4].forEach(function (i) {
      var el = form() && form().querySelector('[name="nome_fonte_' + i + '"]');
      if (!el) return;
      el.innerHTML = FONTES_NOME.map(function (f) {
        return '<option value="' + f.v + '">' + f.n + '</option>';
      }).join('');
      el.value = limpo[i - 1] || FONTES_NOME[i - 1].v;
    });
  }

  function lerFontes() {
    var out = [];
    [1, 2, 3, 4].forEach(function (i) {
      var el = form() && form().querySelector('[name="nome_fonte_' + i + '"]');
      var v = el ? el.value : '';
      if (v && out.indexOf(v) < 0) out.push(v);
    });
    FONTES_NOME.forEach(function (f) {
      if (out.indexOf(f.v) < 0) out.push(f.v);
    });
    return out.join(',');
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
      'poll_saida_seg',
      'sync_agenda_fotos_hora',
      'saudacao_atraso_seg',
      'saudacao_midia_url',
      'arquivo_auto_horas_silencio',
      'arquivo_auto_apos_ok_horas',
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
      'aviso_fora_minutos',
      'msg_ausencia',
      'respostas_prontas',
      'msg_fiado_pix_extra',
      'pix_chave',
      'pix_titular',
      'pix_palavras',
      'msg_pix_chave',
      'msg_pix_sem_chave',
      'msg_menu_curto_extra',
      'msg_horario_loja',
      'msg_comprovante_venda',
      'msg_entrega_saiu',
      'msg_entrega_caminho',
      'msg_entrega_chegou',
      'msg_lembrete_fiado',
      'msg_lista_espera',
    ].forEach(function (k) {
      var el = f.querySelector('[name="' + k + '"]');
      if (el && bot[k] != null) el.value = bot[k];
    });
    // poll antigo (<3) deixa o input inválido e o Chrome bloqueava o Salvar
    var pollEl = f.querySelector('[name="poll_saida_seg"]');
    if (pollEl) {
      var pv = parseInt(pollEl.value || '5', 10);
      if (!(pv >= 3 && pv <= 15)) pollEl.value = '5';
    }
    montarHorarioPorDia(bot.horario_por_dia || {});
    montarFontes(bot.nome_fontes || '');
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
      if (n.indexOf('nome_fonte_') === 0) return;
      if (el.type === 'number') o[n] = parseInt(el.value || '0', 10);
      else o[n] = el.value;
    });
    o.horario_por_dia = lerHorarioPorDia();
    o.horario_dias = lerDias();
    o.nome_fontes = lerFontes();
    o.loja1_id = 'centro';
    o.loja2_id = 'vila';
    return o;
  }

  function carregar() {
    return fetch('/api/atendimento-whatsapp/bot/', { credentials: 'same-origin' }).then(function (r) {
      return r.json();
    }).then(function (j) {
      if (!j || !j.ok) throw new Error((j && j.erro) || 'Falha');
      preencher(j.bot);
      if (j.avisos && j.avisos.length) aviso(false, j.avisos[0]);
    }).catch(function (e) {
      aviso(false, e.message || 'Não carregou');
    });
  }

  function salvar(payload) {
    return fetch('/api/atendimento-whatsapp/bot/salvar/', {
      method: 'POST',
      credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf() },
      body: JSON.stringify(payload),
    }).then(function (r) {
      return r.text().then(function (txt) {
        var j = null;
        try {
          j = txt ? JSON.parse(txt) : null;
        } catch (e) {
          j = null;
        }
        if (!r.ok) {
          var msg =
            (j && j.erro) ||
            (r.status === 403
              ? 'Sessão/CSRF expirada — recarregue (Ctrl+F5) e tente de novo.'
              : 'Não salvou (HTTP ' + r.status + ').');
          throw new Error(msg);
        }
        return j || { ok: false, erro: 'Resposta inválida' };
      });
    });
  }

  function liberarSaveUi(btn) {
    if (!btn) return;
    btn.disabled = false;
    btn.textContent = 'Salvar';
  }

  function dispararSalvar() {
    var btn = $('wa-bot-save');
    if (btn && btn.disabled) return;
    var dados;
    try {
      dados = coletar();
    } catch (eColeta) {
      aviso(false, 'Não leu o formulário — recarregue a página.');
      return;
    }
    if (dados.feat_fiado_pix && !(String(dados.pix_chave || '').trim())) {
      aviso(false, 'Cole a Chave Pix na caixa verde (Recursos) antes de salvar.');
      var navPix = $('wa-bot-nav');
      var box = $('wa-pix-box');
      if (navPix) {
        var bRec = navPix.querySelector('button[data-panel="recursos"]');
        if (bRec) bRec.click();
      }
      if (box) {
        try {
          box.scrollIntoView({ behavior: 'smooth', block: 'center' });
        } catch (e) {}
      }
      var inp = $('id_pix_chave');
      if (inp) {
        try {
          inp.focus();
        } catch (e2) {}
      }
      return;
    }
    // Clamp visual: poll antigo < 8 sobe (min do input / anti-lentidão PDV)
    try {
      var poll = parseInt(dados.poll_saida_seg || 10, 10);
      if (!(poll >= 8 && poll <= 30)) dados.poll_saida_seg = 10;
    } catch (ePoll) {
      dados.poll_saida_seg = 10;
    }
    if (btn) {
      btn.disabled = true;
      btn.textContent = 'Salvando…';
    }
    salvar({ bot: dados })
      .then(function (j) {
        if (!j || !j.ok) {
          aviso(false, (j && j.erro) || 'Não salvou');
          return;
        }
        preencher(j.bot);
        if (j.avisos && j.avisos.length) aviso(false, j.avisos[0]);
        else aviso(true, 'Salvo');
      })
      .catch(function (err) {
        aviso(false, (err && err.message) || 'Não salvou');
      })
      .then(function () {
        liberarSaveUi(btn);
      });
  }

  var f = form();
  if (!f) return;
  // Handlers primeiro — se montar UI falhar, Salvar ainda funciona
  f.addEventListener('submit', function (ev) {
    ev.preventDefault();
    dispararSalvar();
  });
  var btnSave = $('wa-bot-save');
  if (btnSave) {
    btnSave.type = 'button';
    btnSave.addEventListener('click', function (ev) {
      ev.preventDefault();
      dispararSalvar();
    });
  }
  var rst = $('wa-bot-reset');
  if (rst) {
    rst.addEventListener('click', function () {
      if (!window.confirm('Voltar ao padrão?')) return;
      salvar({ reset: true })
        .then(function (j) {
          if (!j || !j.ok) {
            aviso(false, (j && j.erro) || 'Não resetou');
            return;
          }
          preencher(j.bot);
          aviso(true, 'Padrão');
        })
        .catch(function (err) {
          aviso(false, (err && err.message) || 'Não resetou');
        });
    });
  }
  try {
    montarRecursos();
    montarHorarioPorDia({});
  } catch (eUi) {
    aviso(false, 'Tela do Bot com falha parcial — Salvar ainda deve funcionar.');
  }
  carregar();
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

  function atualizarPreviaSaudacao() {
    var ta = $('wa-bv-txt');
    var prev = $('wa-bv-previa');
    if (!ta || !prev) return;
    var empEl = form().querySelector('[name="nome_empresa"]');
    var emp = (empEl && empEl.value) || 'GM Agro';
    var h = new Date();
    var hh = String(h.getHours()).padStart(2, '0') + ':' + String(h.getMinutes()).padStart(2, '0');
    var t = String(ta.value || '');
    t = t
      .replace(/\{empresa\}/g, emp)
      .replace(/\{cliente\}/g, 'Maria')
      .replace(/\{nome\}/g, 'Maria')
      .replace(/\{hora\}/g, hh)
      .replace(/\{loja\}/g, 'Centro');
    prev.textContent = t || '—';
  }
  var bvTxt = $('wa-bv-txt');
  if (bvTxt) {
    bvTxt.addEventListener('input', atualizarPreviaSaudacao);
  }
  var empInp = form().querySelector('[name="nome_empresa"]');
  if (empInp) empInp.addEventListener('input', atualizarPreviaSaudacao);
  var _preencher = preencher;
  preencher = function (bot) {
    _preencher(bot);
    atualizarPreviaSaudacao();
  };

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
          var txt = 'Pronto: ' + String(j.gravados || 0) + ' contato(s).';
          if (vcfMsg) vcfMsg.textContent = txt;
          window.alert(txt + ' Volte ao chat e busque pelo nome.');
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
})();
