/**
 * Lote A4 gôndola (18/folha) — provisório.
 * Progresso no Postgres; imprime via produtos_etiquetas_core.
 */
(function () {
  'use strict';
  var CFG = window.AGRO_ETQ_LOTE_CFG || {};
  var API = String(CFG.apiUrl || '/api/produtos/etiquetas/lote/').replace(/\/?$/, '/');
  var FOLHA = Number(CFG.folhaSize) || 18;
  var PRESET_ID = String(CFG.presetId || 'gondola');
  var Core = window.AgroEtiquetasCore;
  var loteAtivo = null;
  var busy = false;

  function $(id) {
    return document.getElementById(id);
  }

  function csrf() {
    var m = document.cookie.match(/(?:^|;\s*)csrftoken=([^;]+)/);
    if (m) return decodeURIComponent(m[1]);
    var el = document.querySelector('[name=csrfmiddlewaretoken]');
    return el ? el.value : '';
  }

  function setStatus(msg, isErr) {
    var el = $('lote-status');
    if (!el) return;
    el.textContent = msg || '';
    el.className =
      'min-h-[1.25rem] text-xs font-semibold ' + (isErr ? 'text-red-400' : 'text-slate-400');
  }

  function esc(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function postJson(url, body) {
    return fetch(url, {
      method: 'POST',
      credentials: 'same-origin',
      headers: {
        'Content-Type': 'application/json',
        'X-CSRFToken': csrf(),
      },
      body: JSON.stringify(body || {}),
    }).then(function (r) {
      return r.json().then(function (j) {
        return { http: r.status, data: j };
      });
    });
  }

  function getJson(url) {
    return fetch(url, { credentials: 'same-origin' }).then(function (r) {
      return r.json();
    });
  }

  function renderListaItens(el, itens) {
    if (!el) return;
    itens = itens || [];
    if (!itens.length) {
      el.innerHTML = '<p class="px-2 py-2 text-slate-500">—</p>';
      return;
    }
    el.innerHTML = itens
      .map(function (it, i) {
        return (
          '<div class="border-b border-slate-700/70 px-2 py-1.5">' +
          '<span class="font-bold text-slate-300">' +
          (i + 1) +
          '.</span> ' +
          '<span class="font-semibold text-white">' +
          esc(it.nome || '—') +
          '</span>' +
          '<span class="ml-1 text-slate-500">' +
          esc(it.codigo_gm || '') +
          '</span>' +
          '</div>'
        );
      })
      .join('');
  }

  function renderProgresso(lote) {
    loteAtivo = lote || null;
    var titulo = $('lote-titulo-ativo');
    var badge = $('lote-badge-status');
    var progF = $('lote-prog-folha');
    var progI = $('lote-prog-itens');
    var bar = $('lote-prog-bar');
    var btnImp = $('lote-btn-imprimir');
    var btnDes = $('lote-btn-desfazer');
    var btnCan = $('lote-btn-cancelar');

    if (!lote) {
      if (titulo) titulo.textContent = 'Nenhum lote ativo';
      if (badge) badge.textContent = '—';
      if (progF) progF.textContent = 'Folha — / —';
      if (progI) progI.textContent = '0 / 0 impressos · faltam 0';
      if (bar) bar.style.width = '0%';
      if (btnImp) {
        btnImp.disabled = true;
        btnImp.textContent = 'Imprimir próxima folha';
      }
      if (btnDes) btnDes.disabled = true;
      if (btnCan) btnCan.disabled = true;
      renderListaItens($('lote-proximos'), []);
      renderListaItens($('lote-ultimos'), []);
      return;
    }

    var total = Number(lote.total) || 0;
    var impressos = Number(lote.impressos) || 0;
    var faltam = Number(lote.faltam) || 0;
    var folhasTot = Number(lote.folhas_tot) || 0;
    var folhaAtual = Number(lote.folha_atual) || 0;
    var proximaQtd = Number(lote.proxima_qtd) || 0;
    var pct = total ? Math.min(100, Math.round((impressos / total) * 100)) : 0;

    if (titulo) titulo.textContent = lote.nome || 'Lote #' + lote.id;
    if (badge) {
      badge.textContent = lote.status || '—';
      badge.className =
        'rounded-lg border px-2 py-1 text-[10px] font-black uppercase ' +
        (lote.status === 'aberto'
          ? 'border-emerald-600 text-emerald-300'
          : lote.status === 'concluido'
            ? 'border-sky-600 text-sky-300'
            : 'border-slate-600 text-slate-300');
    }
    if (progF) {
      progF.textContent =
        folhasTot > 0
          ? 'Folha ' + folhaAtual + ' / ' + folhasTot
          : 'Folha — / —';
    }
    if (progI) {
      progI.textContent =
        impressos + ' / ' + total + ' impressos · faltam ' + faltam;
    }
    if (bar) bar.style.width = pct + '%';

    var aberto = lote.status === 'aberto' && faltam > 0;
    if (btnImp) {
      btnImp.disabled = !aberto || busy;
      btnImp.textContent =
        'Imprimir próxima folha (' + (proximaQtd || FOLHA) + ')';
    }
    if (btnDes) {
      btnDes.disabled = busy || !(Number(lote.ultima_folha_qtd) > 0 || impressos > 0);
    }
    if (btnCan) {
      btnCan.disabled = busy || lote.status === 'cancelado';
    }

    renderListaItens($('lote-proximos'), lote.proximos || []);
    renderListaItens($('lote-ultimos'), lote.ultimos_impressos || []);
  }

  function renderAbertos(lotes) {
    var box = $('lote-lista-abertos');
    if (!box) return;
    lotes = (lotes || []).filter(function (l) {
      return l.status === 'aberto';
    });
    if (!lotes.length) {
      box.innerHTML = '<p class="px-2 py-1 text-slate-500">Nenhum aberto.</p>';
      return;
    }
    box.innerHTML = lotes
      .map(function (l) {
        var sel = loteAtivo && String(loteAtivo.id) === String(l.id);
        return (
          '<button type="button" data-lote-id="' +
          esc(l.id) +
          '" class="w-full rounded-lg border px-2 py-1.5 text-left text-xs font-semibold ' +
          (sel
            ? 'border-emerald-500 bg-emerald-900/40 text-white'
            : 'border-slate-600 bg-slate-900/60 text-slate-200 hover:border-orange-500') +
          '">' +
          esc(l.nome || 'Lote #' + l.id) +
          '<span class="mt-0.5 block text-[10px] font-bold text-slate-400">' +
          (l.impressos || 0) +
          '/' +
          (l.total || 0) +
          ' · folha ' +
          (l.folha_atual || 0) +
          '/' +
          (l.folhas_tot || 0) +
          '</span></button>'
        );
      })
      .join('');
    box.querySelectorAll('[data-lote-id]').forEach(function (btn) {
      btn.addEventListener('click', function () {
        carregarLote(btn.getAttribute('data-lote-id'));
      });
    });
  }

  function listarAbertos() {
    return getJson(API + '?status=aberto&limit=20')
      .then(function (j) {
        if (j && j.ok) renderAbertos(j.lotes || []);
      })
      .catch(function () {});
  }

  function carregarLote(id) {
    if (!id) return;
    setStatus('Carregando…');
    return getJson(API + encodeURIComponent(id) + '/')
      .then(function (j) {
        if (!j || !j.ok) {
          setStatus((j && j.erro) || 'Falha ao carregar.', true);
          return;
        }
        renderProgresso(j.lote);
        setStatus('Lote carregado.');
        listarAbertos();
      })
      .catch(function () {
        setStatus('Falha de rede ao carregar.', true);
      });
  }

  function criarLote() {
    if (busy) return;
    busy = true;
    setStatus('Montando lista (pode demorar uns segundos)…');
    var btn = $('lote-btn-criar');
    if (btn) btn.disabled = true;
    var body = {
      loja: ($('lote-loja') && $('lote-loja').value) || 'vila',
      estoque_sinal: ($('lote-estoque-sinal') && $('lote-estoque-sinal').value) || '',
      somente_ativos: !($('lote-somente-ativos') && !$('lote-somente-ativos').checked),
      nome: ($('lote-nome') && $('lote-nome').value.trim()) || '',
    };
    postJson(API, body)
      .then(function (res) {
        busy = false;
        if (btn) btn.disabled = false;
        var j = res.data;
        if (!j || !j.ok) {
          setStatus((j && j.erro) || 'Não montou o lote.', true);
          return;
        }
        renderProgresso(j.lote);
        setStatus(
          'Lista pronta: ' +
            (j.lote.total || 0) +
            ' produtos · ' +
            (j.lote.folhas_tot || 0) +
            ' folhas.'
        );
        listarAbertos();
      })
      .catch(function () {
        busy = false;
        if (btn) btn.disabled = false;
        setStatus('Falha de rede ao montar.', true);
      });
  }

  function imprimirProxima() {
    if (!loteAtivo || !loteAtivo.id || busy) return;
    if (!Core || typeof Core.imprimirItens !== 'function') {
      setStatus('Módulo de impressão indisponível. F5.', true);
      return;
    }
    busy = true;
    renderProgresso(loteAtivo);
    setStatus('Preparando folha…');
    var id = loteAtivo.id;
    postJson(API + encodeURIComponent(id) + '/proxima-folha/', {})
      .then(function (res) {
        var j = res.data;
        if (!j || !j.ok || !(j.itens || []).length) {
          busy = false;
          setStatus((j && j.erro) || 'Nada para imprimir.', true);
          renderProgresso(loteAtivo);
          return null;
        }
        var itens = j.itens;
        setStatus('Abrindo impressão (' + itens.length + ')…');
        return Core.imprimirItens(itens, {
          presetId: PRESET_ID,
          origem: 'lote_a4',
        }).then(function (printRes) {
          return { itens: itens, printRes: printRes, qtd: itens.length };
        });
      })
      .then(function (pack) {
        if (!pack) return;
        if (!pack.printRes || !pack.printRes.ok) {
          busy = false;
          setStatus(
            'Impressão não concluída' +
              (pack.printRes && pack.printRes.reason ? ': ' + pack.printRes.reason : '.') +
              ' Cursor NÃO avançou.',
            true
          );
          renderProgresso(loteAtivo);
          return;
        }
        var ok = window.confirm(
          'A folha saiu ok na impressora?\n\n' +
            'Sim → marca ' +
            pack.qtd +
            ' como impressos.\n' +
            'Não → fica no mesmo ponto para tentar de novo.'
        );
        if (!ok) {
          busy = false;
          setStatus('Não marcou. Pode imprimir de novo a mesma folha.', true);
          renderProgresso(loteAtivo);
          return;
        }
        return postJson(API + encodeURIComponent(id) + '/confirmar-folha/', {
          qtd: pack.qtd,
        }).then(function (res2) {
          busy = false;
          var j2 = res2.data;
          if (!j2 || !j2.ok) {
            setStatus((j2 && j2.erro) || 'Não confirmou no servidor.', true);
            return;
          }
          renderProgresso(j2.lote);
          if (j2.lote.status === 'concluido') {
            setStatus('Lote concluído! ' + (j2.lote.total || 0) + ' etiquetas.');
          } else {
            setStatus(
              'Folha ok. Faltam ' +
                (j2.lote.faltam || 0) +
                ' · próxima folha ' +
                (j2.lote.folha_atual || 0) +
                '/' +
                (j2.lote.folhas_tot || 0) +
                '.'
            );
          }
          listarAbertos();
        });
      })
      .catch(function () {
        busy = false;
        setStatus('Falha ao imprimir/confirmar.', true);
        renderProgresso(loteAtivo);
      });
  }

  function desfazer() {
    if (!loteAtivo || !loteAtivo.id || busy) return;
    if (!window.confirm('Desfazer a última folha confirmada?')) return;
    busy = true;
    postJson(API + encodeURIComponent(loteAtivo.id) + '/desfazer-folha/', {})
      .then(function (res) {
        busy = false;
        var j = res.data;
        if (!j || !j.ok) {
          setStatus((j && j.erro) || 'Não desfez.', true);
          return;
        }
        renderProgresso(j.lote);
        setStatus('Última folha desfeita. Pode imprimir de novo.');
        listarAbertos();
      })
      .catch(function () {
        busy = false;
        setStatus('Falha ao desfazer.', true);
      });
  }

  function cancelar() {
    if (!loteAtivo || !loteAtivo.id || busy) return;
    if (!window.confirm('Cancelar este lote? O progresso para de avançar.')) return;
    busy = true;
    postJson(API + encodeURIComponent(loteAtivo.id) + '/cancelar/', {})
      .then(function (res) {
        busy = false;
        var j = res.data;
        if (!j || !j.ok) {
          setStatus((j && j.erro) || 'Não cancelou.', true);
          return;
        }
        renderProgresso(j.lote);
        setStatus('Lote cancelado.');
        listarAbertos();
      })
      .catch(function () {
        busy = false;
        setStatus('Falha ao cancelar.', true);
      });
  }

  function init() {
    $('lote-btn-criar') && $('lote-btn-criar').addEventListener('click', criarLote);
    $('lote-btn-imprimir') && $('lote-btn-imprimir').addEventListener('click', imprimirProxima);
    $('lote-btn-desfazer') && $('lote-btn-desfazer').addEventListener('click', desfazer);
    $('lote-btn-cancelar') && $('lote-btn-cancelar').addEventListener('click', cancelar);
    renderProgresso(null);
    listarAbertos().then(function () {
      /* auto-seleciona o mais recente aberto */
      getJson(API + '?status=aberto&limit=1').then(function (j) {
        if (j && j.ok && j.lotes && j.lotes[0]) {
          carregarLote(j.lotes[0].id);
        }
      });
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
