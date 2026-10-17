/**
 * Lote etiquetas — fila ou loja · progresso Postgres · pausa/auto.
 */
(function () {
  'use strict';
  var CFG = window.AGRO_ETQ_LOTE_CFG || {};
  var API = String(CFG.apiUrl || '/api/produtos/etiquetas/lote/').replace(/\/?$/, '/');
  var PRESETS_URL = String(CFG.presetsUrl || '/api/produtos/etiquetas/presets/');
  var FILA_KEY = 'agro_etq_lote_fila_v1';
  var Core = window.AgroEtiquetasCore;
  var loteAtivo = null;
  var busy = false;
  var autoStop = false;
  var filaPendente = null;
  var presetsCache = [];
  var syncingCfg = false;

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

  function sleep(ms) {
    return new Promise(function (resolve) {
      setTimeout(resolve, ms);
    });
  }

  function lerFilaStorage() {
    try {
      var raw = sessionStorage.getItem(FILA_KEY);
      if (!raw) return null;
      var j = JSON.parse(raw);
      if (!j || !Array.isArray(j.itens) || !j.itens.length) return null;
      return j;
    } catch (e) {
      return null;
    }
  }

  function limparFilaStorage() {
    try {
      sessionStorage.removeItem(FILA_KEY);
    } catch (e) {}
  }

  function configDoForm() {
    return {
      etiquetas_por_folha: Math.max(1, Math.min(54, parseInt(($('lote-etq-folha') || {}).value, 10) || 18)),
      folhas_por_vez: Math.max(1, Math.min(20, parseInt(($('lote-folhas-vez') || {}).value, 10) || 1)),
      intervalo_seg: Math.max(0, Math.min(120, parseInt(($('lote-intervalo') || {}).value, 10) || 0)),
      modo: (($('lote-modo') || {}).value || 'pausa') === 'auto' ? 'auto' : 'pausa',
    };
  }

  function presetSelecionado() {
    var sel = $('lote-preset');
    return (sel && sel.value) || String(CFG.presetId || 'gondola');
  }

  function aplicarConfigNoForm(lote) {
    if (!lote) return;
    syncingCfg = true;
    var cfg = lote.config || {};
    if ($('lote-etq-folha') && cfg.etiquetas_por_folha != null) {
      $('lote-etq-folha').value = cfg.etiquetas_por_folha;
    } else if ($('lote-etq-folha') && lote.folha_size) {
      $('lote-etq-folha').value = lote.folha_size;
    }
    if ($('lote-folhas-vez') && cfg.folhas_por_vez != null) {
      $('lote-folhas-vez').value = cfg.folhas_por_vez;
    }
    if ($('lote-intervalo') && cfg.intervalo_seg != null) {
      $('lote-intervalo').value = cfg.intervalo_seg;
    }
    if ($('lote-modo') && cfg.modo) {
      $('lote-modo').value = cfg.modo === 'auto' ? 'auto' : 'pausa';
    }
    var sel = $('lote-preset');
    if (sel && lote.preset_id) {
      sel.value = lote.preset_id;
      if (sel.value !== lote.preset_id) {
        /* preset pode não estar na lista ainda */
        var opt = document.createElement('option');
        opt.value = lote.preset_id;
        opt.textContent = lote.preset_id;
        sel.appendChild(opt);
        sel.value = lote.preset_id;
      }
    }
    syncingCfg = false;
  }

  function gradeDoPreset(presetId) {
    if (!Core || !presetsCache.length) return null;
    var p = Core.getPresetById(presetsCache, presetId);
    if (!p) return null;
    var cols = Number(p.cols_folha) || 0;
    var rows = Number(p.rows_folha) || 0;
    if (cols > 0 && rows > 0) return cols * rows;
    if (typeof Core.calcularGradeFolha === 'function') {
      try {
        var g = Core.calcularGradeFolha(
          p.folha,
          p.largura_mm,
          p.altura_mm,
          p.borda_mm,
          p.cols_folha,
          p.rows_folha
        );
        if (g && g.cols && g.rows) return g.cols * g.rows;
      } catch (e) {}
    }
    return null;
  }

  function carregarPresets() {
    return getJson(PRESETS_URL)
      .then(function (j) {
        var list = (j && j.ok && j.presets) || [];
        if (Core && typeof Core.normalizarPreset === 'function') {
          presetsCache = list.map(function (x) {
            return Core.normalizarPreset(x.payload || x);
          });
        } else {
          presetsCache = list.map(function (x) {
            return x.payload || x;
          });
        }
        var sel = $('lote-preset');
        if (!sel) return;
        var ativo = (loteAtivo && loteAtivo.preset_id) || CFG.presetId || 'gondola';
        sel.innerHTML = presetsCache
          .map(function (p) {
            return (
              '<option value="' +
              esc(p.id) +
              '"' +
              (String(p.id) === String(ativo) ? ' selected' : '') +
              '>' +
              esc(p.nome || p.id) +
              '</option>'
            );
          })
          .join('');
        if (!presetsCache.length) {
          sel.innerHTML =
            '<option value="' + esc(ativo) + '">' + esc(ativo) + '</option>';
        }
      })
      .catch(function () {
        var sel = $('lote-preset');
        if (sel && !sel.options.length) {
          sel.innerHTML =
            '<option value="gondola">gondola</option>';
        }
      });
  }

  function setTab(tab) {
    var fila = tab === 'fila';
    var btnF = $('lote-tab-fila');
    var btnL = $('lote-tab-loja');
    var panF = $('lote-painel-fila');
    var panL = $('lote-painel-loja');
    if (btnF) {
      btnF.className =
        'min-h-[40px] rounded-xl border-2 text-[11px] font-black uppercase ' +
        (fila
          ? 'border-orange-500 bg-orange-950/40 text-orange-100'
          : 'border-slate-600 bg-slate-900 text-slate-200');
    }
    if (btnL) {
      btnL.className =
        'min-h-[40px] rounded-xl border-2 text-[11px] font-black uppercase ' +
        (!fila
          ? 'border-orange-500 bg-orange-950/40 text-orange-100'
          : 'border-slate-600 bg-slate-900 text-slate-200');
    }
    if (panF) panF.classList.toggle('hidden', !fila);
    if (panL) panL.classList.toggle('hidden', fila);
  }

  function atualizarFilaInfo() {
    var info = $('lote-fila-info');
    var btn = $('lote-btn-criar-fila');
    filaPendente = lerFilaStorage();
    if (!filaPendente) {
      if (info) {
        info.innerHTML =
          'Nenhuma fila pendente. Na tela Etiquetas, monte a fila e clique em <strong class="text-orange-300">Lote A4</strong>.';
      }
      if (btn) btn.disabled = true;
      return;
    }
    var n = filaPendente.itens.length;
    var etq = filaPendente.itens.reduce(function (a, it) {
      return a + Math.max(1, parseInt(it.qtd, 10) || 1);
    }, 0);
    if (info) {
      info.textContent =
        'Fila pronta: ' + n + ' produto(s) · ' + etq + ' etiqueta(s)' +
        (filaPendente.preset_id ? ' · preset ' + filaPendente.preset_id : '') +
        '.';
    }
    if (btn) btn.disabled = false;
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

  function renderItensEdit(lote) {
    var el = $('lote-itens-edit');
    if (!el) return;
    var itens = (lote && lote.itens) || [];
    if (!itens.length) {
      el.innerHTML = '<p class="px-2 py-2 text-slate-500">—</p>';
      return;
    }
    el.innerHTML = itens
      .map(function (it, i) {
        return (
          '<div class="flex items-center gap-1 border-b border-slate-700/70 px-1 py-1" data-idx="' +
          i +
          '">' +
          '<span class="min-w-0 flex-1 truncate text-[11px] font-semibold text-white" title="' +
          esc(it.nome || '') +
          '">' +
          esc(it.nome || '—') +
          '</span>' +
          '<input type="number" min="1" max="999" value="' +
          esc(it.qtd || 1) +
          '" data-qtd-idx="' +
          i +
          '" class="w-14 min-h-[28px] rounded-lg border border-slate-600 bg-slate-950 px-1 text-center text-xs font-bold text-white" />' +
          '</div>'
        );
      })
      .join('');
    el.querySelectorAll('[data-qtd-idx]').forEach(function (inp) {
      inp.addEventListener('change', function () {
        var idx = parseInt(inp.getAttribute('data-qtd-idx'), 10);
        var q = Math.max(1, Math.min(999, parseInt(inp.value, 10) || 1));
        inp.value = q;
        salvarQtdItem(idx, q);
      });
    });
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
    var btnParar = $('lote-btn-parar');

    if (!lote) {
      if (titulo) titulo.textContent = 'Nenhum lote ativo';
      if (badge) badge.textContent = '—';
      if (progF) progF.textContent = 'Folha — / —';
      if (progI) progI.textContent = '0 / 0 impressos · faltam 0';
      if (bar) bar.style.width = '0%';
      if (btnImp) {
        btnImp.disabled = true;
        btnImp.textContent = 'Imprimir próximo';
      }
      if (btnDes) btnDes.disabled = true;
      if (btnCan) btnCan.disabled = true;
      if (btnParar) btnParar.disabled = true;
      renderListaItens($('lote-proximos'), []);
      renderListaItens($('lote-ultimos'), []);
      renderItensEdit(null);
      return;
    }

    aplicarConfigNoForm(lote);

    var total = Number(lote.total) || 0;
    var impressos = Number(lote.impressos) || 0;
    var faltam = Number(lote.faltam) || 0;
    var folhasTot = Number(lote.folhas_tot) || 0;
    var folhaAtual = Number(lote.folha_atual) || 0;
    var proximaQtd = Number(lote.proxima_qtd) || 0;
    var pct = total ? Math.min(100, Math.round((impressos / total) * 100)) : 0;

    if (titulo) {
      titulo.textContent =
        (lote.nome || 'Lote #' + lote.id) +
        (lote.origem === 'fila' ? ' · fila' : '') +
        (lote.n_produtos ? ' · ' + lote.n_produtos + ' prod.' : '');
    }
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
        folhasTot > 0 ? 'Folha ' + folhaAtual + ' / ' + folhasTot : 'Folha — / —';
    }
    if (progI) {
      progI.textContent =
        impressos + ' / ' + total + ' impressos · faltam ' + faltam;
    }
    if (bar) bar.style.width = pct + '%';

    var aberto = lote.status === 'aberto' && faltam > 0;
    if (btnImp) {
      btnImp.disabled = !aberto || busy;
      btnImp.textContent = 'Imprimir próximo (' + (proximaQtd || 0) + ')';
    }
    if (btnDes) {
      btnDes.disabled = busy || !(Number(lote.ultima_folha_qtd) > 0 || impressos > 0);
    }
    if (btnCan) {
      btnCan.disabled = busy || lote.status === 'cancelado';
    }
    if (btnParar) {
      btnParar.disabled = !busy;
    }

    renderListaItens($('lote-proximos'), lote.proximos || []);
    renderListaItens($('lote-ultimos'), lote.ultimos_impressos || []);
    renderItensEdit(lote);
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
    if (!id) return Promise.resolve();
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

  function atualizarLote(body) {
    if (!loteAtivo || !loteAtivo.id) {
      return Promise.reject(new Error('sem lote'));
    }
    return postJson(API + encodeURIComponent(loteAtivo.id) + '/atualizar/', body).then(
      function (res) {
        var j = res.data;
        if (!j || !j.ok) {
          throw new Error((j && j.erro) || 'Falha ao salvar');
        }
        renderProgresso(j.lote);
        return j.lote;
      }
    );
  }

  function salvarAjustes() {
    if (!loteAtivo || !loteAtivo.id || busy) return;
    busy = true;
    setStatus('Salvando ajustes…');
    var body = Object.assign({ preset_id: presetSelecionado() }, configDoForm());
    atualizarLote(body)
      .then(function () {
        busy = false;
        setStatus('Ajustes salvos.');
        listarAbertos();
      })
      .catch(function (e) {
        busy = false;
        setStatus((e && e.message) || 'Não salvou.', true);
        renderProgresso(loteAtivo);
      });
  }

  function salvarQtdItem(idx, qtd) {
    if (!loteAtivo || !loteAtivo.id || busy) return;
    busy = true;
    atualizarLote({ qtds: [{ index: idx, qtd: qtd }] })
      .then(function () {
        busy = false;
        setStatus('QTD atualizada.');
        listarAbertos();
      })
      .catch(function (e) {
        busy = false;
        setStatus((e && e.message) || 'Não atualizou QTD.', true);
      });
  }

  function aplicarQtdMassa() {
    if (!loteAtivo || !loteAtivo.id || busy) return;
    var q = Math.max(1, Math.min(999, parseInt(($('lote-qtd-massa') || {}).value, 10) || 1));
    busy = true;
    setStatus('Aplicando QTD ' + q + ' em todos…');
    atualizarLote({ qtd_massa: q })
      .then(function () {
        busy = false;
        setStatus('QTD em massa aplicada.');
        listarAbertos();
      })
      .catch(function (e) {
        busy = false;
        setStatus((e && e.message) || 'Não aplicou QTD.', true);
      });
  }

  function criarLoteLoja() {
    if (busy) return;
    busy = true;
    setStatus('Montando lista (pode demorar uns segundos)…');
    var btn = $('lote-btn-criar');
    if (btn) btn.disabled = true;
    var body = Object.assign(
      {
        origem: 'loja',
        loja: ($('lote-loja') && $('lote-loja').value) || 'vila',
        estoque_sinal: ($('lote-estoque-sinal') && $('lote-estoque-sinal').value) || '',
        somente_ativos: !($('lote-somente-ativos') && !$('lote-somente-ativos').checked),
        nome: ($('lote-nome') && $('lote-nome').value.trim()) || '',
        preset_id: presetSelecionado(),
      },
      configDoForm()
    );
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
            ' etiquetas · ' +
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

  function criarLoteFila() {
    if (busy) return;
    filaPendente = lerFilaStorage();
    if (!filaPendente || !filaPendente.itens || !filaPendente.itens.length) {
      setStatus('Nenhuma fila para usar.', true);
      return;
    }
    busy = true;
    setStatus('Criando lote da fila…');
    var btn = $('lote-btn-criar-fila');
    if (btn) btn.disabled = true;
    var preset =
      filaPendente.preset_id || presetSelecionado();
    var body = Object.assign(
      {
        origem: 'fila',
        itens: filaPendente.itens,
        nome: ($('lote-nome') && $('lote-nome').value.trim()) || filaPendente.nome || '',
        preset_id: preset,
      },
      configDoForm()
    );
    var g = gradeDoPreset(preset);
    if (g) {
      if ($('lote-etq-folha')) $('lote-etq-folha').value = g;
      body.etiquetas_por_folha = g;
    }
    postJson(API, body)
      .then(function (res) {
        busy = false;
        if (btn) btn.disabled = false;
        var j = res.data;
        if (!j || !j.ok) {
          setStatus((j && j.erro) || 'Não criou lote da fila.', true);
          atualizarFilaInfo();
          return;
        }
        limparFilaStorage();
        atualizarFilaInfo();
        renderProgresso(j.lote);
        setStatus(
          'Fila no lote: ' +
            (j.lote.total || 0) +
            ' etiquetas · ' +
            (j.lote.folhas_tot || 0) +
            ' folhas.'
        );
        listarAbertos();
      })
      .catch(function () {
        busy = false;
        if (btn) btn.disabled = false;
        setStatus('Falha de rede ao criar da fila.', true);
        atualizarFilaInfo();
      });
  }

  function confirmarAvanco(id, qtd) {
    return postJson(API + encodeURIComponent(id) + '/confirmar-folha/', { qtd: qtd }).then(
      function (res2) {
        var j2 = res2.data;
        if (!j2 || !j2.ok) {
          throw new Error((j2 && j2.erro) || 'Não confirmou no servidor.');
        }
        renderProgresso(j2.lote);
        listarAbertos();
        return j2.lote;
      }
    );
  }

  function imprimirUmPacote() {
    if (!loteAtivo || !loteAtivo.id) return Promise.resolve(null);
    if (!Core || typeof Core.imprimirItens !== 'function') {
      setStatus('Módulo de impressão indisponível. F5.', true);
      return Promise.resolve(null);
    }
    var id = loteAtivo.id;
    var presetId = presetSelecionado() || loteAtivo.preset_id || 'gondola';
    setStatus('Preparando impressão…');
    return postJson(API + encodeURIComponent(id) + '/proxima-folha/', {})
      .then(function (res) {
        var j = res.data;
        if (!j || !j.ok || !(j.itens || []).length) {
          setStatus((j && j.erro) || 'Nada para imprimir.', true);
          return null;
        }
        var itens = j.itens;
        setStatus('Abrindo impressão (' + itens.length + ')…');
        return Core.imprimirItens(itens, {
          presetId: j.preset_id || presetId,
          origem: 'lote_a4',
        }).then(function (printRes) {
          return { itens: itens, printRes: printRes, qtd: itens.length, id: id };
        });
      });
  }

  function imprimirProxima() {
    if (!loteAtivo || !loteAtivo.id || busy) return;
    busy = true;
    autoStop = false;
    renderProgresso(loteAtivo);

    function ciclo() {
      return imprimirUmPacote().then(function (pack) {
        if (!pack) {
          busy = false;
          renderProgresso(loteAtivo);
          return null;
        }
        if (!pack.printRes || !pack.printRes.ok) {
          busy = false;
          setStatus(
            'Impressão não concluída' +
              (pack.printRes && pack.printRes.reason ? ': ' + pack.printRes.reason : '.') +
              ' Cursor NÃO avançou.',
            true
          );
          renderProgresso(loteAtivo);
          return null;
        }
        var modo = (loteAtivo.config && loteAtivo.config.modo) || configDoForm().modo;
        if (modo === 'pausa') {
          var ok = window.confirm(
            'A impressão saiu ok na impressora?\n\n' +
              'Sim → marca ' +
              pack.qtd +
              ' como impressos.\n' +
              'Não → fica no mesmo ponto para tentar de novo.'
          );
          if (!ok) {
            busy = false;
            setStatus('Não marcou. Pode imprimir de novo o mesmo trecho.', true);
            renderProgresso(loteAtivo);
            return null;
          }
        }
        return confirmarAvanco(pack.id, pack.qtd).then(function (lote) {
          if (!lote) return null;
          if (lote.status === 'concluido') {
            busy = false;
            setStatus('Lote concluído! ' + (lote.total || 0) + ' etiquetas.');
            return null;
          }
          if (modo === 'auto' && !autoStop) {
            var seg =
              (lote.config && lote.config.intervalo_seg != null
                ? lote.config.intervalo_seg
                : configDoForm().intervalo_seg) || 0;
            setStatus(
              'OK · faltam ' +
                (lote.faltam || 0) +
                '. Próximo em ' +
                seg +
                's… (Parar auto para interromper)'
            );
            return sleep(seg * 1000).then(function () {
              if (autoStop) {
                busy = false;
                setStatus('Automático parado. Faltam ' + (lote.faltam || 0) + '.');
                renderProgresso(lote);
                return null;
              }
              return ciclo();
            });
          }
          busy = false;
          setStatus(
            'Trecho ok. Faltam ' +
              (lote.faltam || 0) +
              ' · folha ' +
              (lote.folha_atual || 0) +
              '/' +
              (lote.folhas_tot || 0) +
              '.'
          );
          return null;
        });
      });
    }

    ciclo().catch(function () {
      busy = false;
      setStatus('Falha ao imprimir/confirmar.', true);
      renderProgresso(loteAtivo);
    });
  }

  function pararAuto() {
    autoStop = true;
    setStatus('Parando automático…');
  }

  function desfazer() {
    if (!loteAtivo || !loteAtivo.id || busy) return;
    if (!window.confirm('Desfazer o último trecho confirmado?')) return;
    busy = true;
    autoStop = true;
    postJson(API + encodeURIComponent(loteAtivo.id) + '/desfazer-folha/', {})
      .then(function (res) {
        busy = false;
        var j = res.data;
        if (!j || !j.ok) {
          setStatus((j && j.erro) || 'Não desfez.', true);
          return;
        }
        renderProgresso(j.lote);
        setStatus('Último trecho desfeito. Pode imprimir de novo.');
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
    autoStop = true;
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

  function onPresetChange() {
    if (syncingCfg || !loteAtivo) return;
    var g = gradeDoPreset(presetSelecionado());
    if (g && $('lote-etq-folha')) {
      $('lote-etq-folha').value = g;
    }
  }

  function init() {
    $('lote-tab-fila') &&
      $('lote-tab-fila').addEventListener('click', function () {
        setTab('fila');
      });
    $('lote-tab-loja') &&
      $('lote-tab-loja').addEventListener('click', function () {
        setTab('loja');
      });
    $('lote-btn-criar') && $('lote-btn-criar').addEventListener('click', criarLoteLoja);
    $('lote-btn-criar-fila') &&
      $('lote-btn-criar-fila').addEventListener('click', criarLoteFila);
    $('lote-btn-imprimir') && $('lote-btn-imprimir').addEventListener('click', imprimirProxima);
    $('lote-btn-parar') && $('lote-btn-parar').addEventListener('click', pararAuto);
    $('lote-btn-desfazer') && $('lote-btn-desfazer').addEventListener('click', desfazer);
    $('lote-btn-cancelar') && $('lote-btn-cancelar').addEventListener('click', cancelar);
    $('lote-btn-salvar-cfg') &&
      $('lote-btn-salvar-cfg').addEventListener('click', salvarAjustes);
    $('lote-btn-qtd-massa') &&
      $('lote-btn-qtd-massa').addEventListener('click', aplicarQtdMassa);
    $('lote-preset') && $('lote-preset').addEventListener('change', onPresetChange);

    setTab('fila');
    atualizarFilaInfo();
    renderProgresso(null);

    carregarPresets().then(function () {
      listarAbertos().then(function () {
        if (filaPendente) {
          setTab('fila');
          setStatus('Criando lote da fila…');
          criarLoteFila();
          return;
        }
        getJson(API + '?status=aberto&limit=1').then(function (j) {
          if (j && j.ok && j.lotes && j.lotes[0]) {
            carregarLote(j.lotes[0].id);
          }
        });
      });
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
