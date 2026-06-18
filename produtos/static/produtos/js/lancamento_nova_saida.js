/**
 * Modal «Nova saída» — lançamento financeiro manual em lote.
 * Depende de window.AGRO_NOVA_SAIDA_CFG (definido no include Django).
 */
(function () {
  'use strict';

  const cfg = () => window.AGRO_NOVA_SAIDA_CFG || {};
  let rowCount = 0;
  let loteIdempotencyKey = '';
  let nlpDialogCache = { textoOriginal: '', dadosParciais: {}, perguntas: [] };
  let bound = false;

  function $(id) { return document.getElementById(id); }

  function getCookie(name) {
    const m = document.cookie.match('(^|;) ?' + name + '=([^;]*)(;|$)');
    return m ? decodeURIComponent(m[2]) : '';
  }

  function todayISO() {
    const d = new Date();
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
  }

  function tipoAtual() {
    const r = document.querySelector('input[name="agro-ns-tipo"]:checked');
    return (r && r.value === 'receber') ? 'receber' : 'pagar';
  }

  function urlAposGravarLoteManual(tipo, quitado, dc, dv, dvRaw, idMongo) {
    const c = cfg();
    const base = tipo === 'receber' ? c.urlListaReceber : c.urlListaPagar;
    const u = new URL(base, window.location.origin);
    const partes = [dc, dv, dvRaw].map((x) => String(x || '').trim()).filter((x) => x.length >= 8);
    if (partes.length) {
      let vmin = partes[0];
      let vmax = partes[0];
      partes.forEach((p) => {
        if (p < vmin) vmin = p;
        if (p > vmax) vmax = p;
      });
      u.searchParams.set('venc_de', vmin);
      u.searchParams.set('venc_ate', vmax);
    }
    if (quitado) u.searchParams.set('status', 'todos');
    u.searchParams.set('agro_fim_manual', '1');
    const idm = String(idMongo || '').trim();
    if (/^[a-fA-F0-9]{24}$/i.test(idm)) u.searchParams.set('q', idm);
    return u.pathname + u.search;
  }

  function selectSug(nome, id, inpId, hidId, ddEl) {
    const inp = $(inpId);
    const hid = $(hidId);
    if (inp) inp.value = nome;
    if (hid) hid.value = id;
    if (ddEl) ddEl.classList.add('hidden');
  }

  function attachSuggest(wrap) {
    if (!wrap || wrap.dataset.sugBound === '1') return;
    wrap.dataset.sugBound = '1';
    const campo = wrap.getAttribute('data-sug-campo');
    const inp = wrap.querySelector('input[type="text"]');
    const hid = wrap.querySelector('input[type="hidden"]');
    const dd = wrap.querySelector('.agro-ns-sug-dd');
    let timer = null;
    if (!inp || !dd) return;
    inp.addEventListener('input', () => {
      if (hid) hid.value = '';
      clearTimeout(timer);
      timer = setTimeout(async () => {
        if (inp.value.length < 2) {
          dd.classList.add('hidden');
          return;
        }
        const api = cfg().apiSug;
        if (!api) return;
        try {
          const r = await fetch(`${api}?campo=${encodeURIComponent(campo)}&q=${encodeURIComponent(inp.value)}`, { credentials: 'same-origin' });
          const j = await r.json();
          const itens = j.itens || [];
          dd.innerHTML = itens.map((it) => {
            const nome = String(it.nome || '').replace(/\\/g, '\\\\').replace(/'/g, "\\'");
            const id = String(it.id || '');
            return `<li data-nome="${nome.replace(/"/g, '&quot;')}" data-id="${id}">${it.nome}</li>`;
          }).join('');
          dd.querySelectorAll('li').forEach((li) => {
            li.addEventListener('mousedown', (ev) => {
              ev.preventDefault();
              selectSug(li.dataset.nome || '', li.dataset.id || '', inp.id, hid ? hid.id : '', dd);
            });
          });
          dd.classList.toggle('hidden', !itens.length);
        } catch (_) { /* ignore */ }
      }, 300);
    });
    inp.addEventListener('blur', () => setTimeout(() => dd.classList.add('hidden'), 200));
  }

  function calcTotal() {
    let t = 0;
    document.querySelectorAll('#agro-ns-linhas .agro-ns-in-valor').forEach((i) => {
      t += parseFloat(String(i.value || '').replace(',', '.')) || 0;
    });
    const el = $('agro-ns-total');
    if (el) el.textContent = t.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
  }

  function addRow() {
    const host = $('agro-ns-linhas');
    if (!host) return;
    rowCount += 1;
    const idStr = `r${rowCount}`;
    const div = document.createElement('div');
    div.className = 'agro-ns-linha grid grid-cols-1 md:grid-cols-12 gap-4 items-end';
    div.dataset.rowId = idStr;
    div.innerHTML = `
      <div class="md:col-span-3 flex flex-col space-y-2">
        <label class="text-xs font-bold text-slate-500 uppercase tracking-wider">Plano de Contas</label>
        <div class="relative agro-ns-sug-wrap" data-sug-campo="plano">
          <input type="text" id="agro-ns-plano-${idStr}" placeholder="Buscar plano..." autocomplete="off"
                 class="w-full px-4 py-3 border border-slate-300 rounded-xl focus:ring-2 focus:ring-emerald-500 text-slate-800 text-base">
          <input type="hidden" id="agro-ns-plano-id-${idStr}">
          <ul class="agro-ns-sug-dd hidden absolute left-0 right-0 top-full mt-1 bg-white border border-slate-200 rounded-xl shadow-xl z-[60] max-h-[200px] overflow-y-auto"></ul>
        </div>
      </div>
      <div class="md:col-span-2 flex flex-col space-y-2">
        <label class="text-xs font-bold text-slate-500 uppercase tracking-wider">Valor (R$)</label>
        <input type="text" class="agro-ns-in-valor w-full px-4 py-3 border border-slate-300 rounded-xl focus:ring-2 focus:ring-emerald-500 text-slate-800 text-base font-semibold text-right" placeholder="0,00" inputmode="decimal">
      </div>
      <div class="md:col-span-3 flex flex-col space-y-2">
        <label class="text-xs font-bold text-slate-500 uppercase tracking-wider">Descrição</label>
        <input type="text" class="agro-ns-in-desc w-full px-4 py-3 border border-slate-300 rounded-xl focus:ring-2 focus:ring-emerald-500 text-slate-800 text-base" placeholder="Ex.: Conta de Energia">
      </div>
      <div class="md:col-span-3 agro-ns-col-boleto flex flex-col space-y-2">
        <label class="text-xs font-bold text-slate-500 uppercase tracking-wider">Boleto <span class="normal-case font-semibold text-slate-400">(a pagar)</span></label>
        <input type="text" class="agro-ns-in-boleto w-full px-4 py-3 border border-slate-300 rounded-xl focus:ring-2 focus:ring-emerald-500 text-slate-800 text-sm font-mono" placeholder="Linha / barras" maxlength="60" inputmode="numeric" autocomplete="off">
      </div>
      <div class="md:col-span-1 flex items-end justify-center pb-1">
        <button type="button" class="agro-ns-rm-linha w-10 h-10 rounded-xl text-slate-400 hover:text-red-600 hover:bg-red-50 font-black text-xl transition-colors" title="Remover linha" aria-label="Remover linha">×</button>
      </div>`;
    host.appendChild(div);
    div.querySelector('.agro-ns-in-valor')?.addEventListener('input', calcTotal);
    div.querySelector('.agro-ns-rm-linha')?.addEventListener('click', () => {
      const linhas = host.querySelectorAll('.agro-ns-linha');
      if (linhas.length <= 1) return;
      div.remove();
      calcTotal();
    });
    attachSuggest(div.querySelector('.agro-ns-sug-wrap'));
    syncColBoletoVis();
  }

  function syncColBoletoVis() {
    const show = tipoAtual() === 'pagar';
    document.querySelectorAll('.agro-ns-col-boleto').forEach((el) => {
      el.style.display = show ? '' : 'none';
    });
  }

  function syncQuitadoVenc() {
    const cbQ = $('agro-ns-quitado');
    const wrapV = $('agro-ns-wrap-venc');
    const inputVen = $('agro-ns-data-ven');
    const inputComp = $('agro-ns-data-comp');
    if (!cbQ || !wrapV) return;
    wrapV.style.opacity = cbQ.checked ? '0.45' : '1';
    if (cbQ.checked && inputComp && inputComp.value && inputVen) {
      inputVen.value = inputComp.value;
    }
  }

  function syncRecOpts() {
    const cbRec = $('agro-ns-recorrente');
    const wrapRecOpts = $('agro-ns-rec-opts');
    const wrapRecContagem = $('agro-ns-rec-contagem');
    const selParcelas = $('agro-ns-rec-parcelas');
    const on = cbRec && cbRec.checked;
    if (wrapRecOpts) {
      wrapRecOpts.style.opacity = on ? '1' : '0.45';
      wrapRecOpts.querySelectorAll('input, select').forEach((el) => { el.disabled = !on; });
    }
    const mod = (document.querySelector('input[name="agro-ns-rec-modo"]:checked') || {}).value || 'sempre';
    if (wrapRecContagem) {
      const showQ = on && mod === 'normal';
      wrapRecContagem.classList.toggle('hidden', !showQ);
      if (selParcelas) selParcelas.disabled = !showQ;
    }
  }

  function resetLinhas(n) {
    const host = $('agro-ns-linhas');
    if (!host) return;
    host.innerHTML = '';
    rowCount = 0;
    const q = Math.max(1, n || 1);
    for (let i = 0; i < q; i += 1) addRow();
    calcTotal();
  }

  function resetIdempotency() {
    loteIdempotencyKey = (typeof crypto !== 'undefined' && crypto.randomUUID)
      ? crypto.randomUUID()
      : ('lote-' + Date.now() + '-' + Math.random().toString(36).slice(2));
  }

  function esconderNlpDialog() {
    $('agro-ns-nlp-escl')?.classList.add('hidden');
  }

  function renderizarPerguntasNlp(perguntas) {
    const host = $('agro-ns-nlp-campos');
    if (!host) return;
    host.innerHTML = '';
    (perguntas || []).forEach((p, idx) => {
      const lid = document.createElement('label');
      lid.className = 'block space-y-1';
      const sp = document.createElement('span');
      sp.className = 'text-xs font-bold uppercase text-indigo-900';
      sp.textContent = p.texto || p.id || '';
      lid.appendChild(sp);
      const uid = `agro-ns-nlp-r-${idx}`;
      const tin = String(p.tipo_input || 'texto').toLowerCase();
      if (tin === 'opcoes' && Array.isArray(p.opcoes) && p.opcoes.length) {
        const sel = document.createElement('select');
        sel.id = uid;
        sel.className = 'w-full px-3 py-2 rounded-xl border-2 border-indigo-200 bg-white font-semibold text-sm';
        const z = document.createElement('option');
        z.value = '';
        z.textContent = 'Escolha...';
        sel.appendChild(z);
        p.opcoes.forEach((o) => {
          const op = document.createElement('option');
          op.value = String(o.valor || '');
          op.textContent = String(o.rotulo || o.valor || '');
          sel.appendChild(op);
        });
        lid.appendChild(sel);
      } else {
        const inp = document.createElement('input');
        inp.type = 'text';
        inp.id = uid;
        inp.className = 'w-full px-3 py-2 rounded-xl border-2 border-indigo-200 bg-white text-sm';
        inp.autocomplete = 'off';
        lid.appendChild(inp);
      }
      lid.dataset.fieldId = String(p.id || '');
      host.appendChild(lid);
    });
  }

  function coletarRespostasDialogo(perguntas) {
    const r = {};
    (perguntas || []).forEach((p, idx) => {
      const el = $(`agro-ns-nlp-r-${idx}`);
      const fid = String(p.id || '').trim();
      if (!fid || !el) return;
      const v = String(el.value || '').trim();
      if (v) r[fid] = v;
    });
    return r;
  }

  async function aplicarPayloadInterpretado(j) {
    const tipo = j.tipo === 'receber' ? 'receber' : 'pagar';
    document.querySelectorAll('input[name="agro-ns-tipo"]').forEach((el) => { el.checked = el.value === tipo; });
    syncColBoletoVis();
    const inputComp = $('agro-ns-data-comp');
    const inputVen = $('agro-ns-data-ven');
    const cbQ = $('agro-ns-quitado');
    if (j.data_competencia && inputComp) inputComp.value = j.data_competencia;
    if (j.data_vencimento && inputVen) inputVen.value = j.data_vencimento;
    if (cbQ) {
      cbQ.checked = !!j.quitado_hint;
      syncQuitadoVenc();
    }
    let tr = document.querySelector('#agro-ns-linhas .agro-ns-linha');
    if (!tr) { addRow(); tr = document.querySelector('#agro-ns-linhas .agro-ns-linha'); }
    if (!tr) return;
    const wrapPl = tr.querySelector('.agro-ns-sug-wrap[data-sug-campo="plano"]');
    const inpPl = wrapPl ? wrapPl.querySelector('input[type="text"]') : null;
    const hidPl = wrapPl ? wrapPl.querySelector('input[type="hidden"]') : null;
    const linha0 = (j.linhas && j.linhas[0]) || {};
    const hint = String(linha0.plano_hint || '').trim();
    const sugNome = String(linha0.plano_sugerido_nome || '').trim();
    const sugId = String(linha0.plano_sugerido_id || '').trim();
    const valStr = String(linha0.valor || '').trim();
    const descr = linha0.descricao ? String(linha0.descricao).trim() : '';
    if (hidPl) hidPl.value = '';
    if (sugNome && inpPl && hidPl) {
      inpPl.value = sugNome;
      hidPl.value = sugId;
    } else if (inpPl && hint) {
      inpPl.value = hint;
      if (hint.length >= 2 && hidPl && cfg().apiSug) {
        const rs = await fetch(`${cfg().apiSug}?campo=plano&q=${encodeURIComponent(hint)}`, { credentials: 'same-origin' });
        const sj = await rs.json().catch(() => ({}));
        const itens = sj.itens || [];
        const hl = hint.toLowerCase();
        const pick = itens.find((x) => String(x.nome || '').trim().toLowerCase() === hl)
          || itens.find((x) => String(x.nome || '').toLowerCase().includes(hl))
          || (itens.length === 1 ? itens[0] : null);
        if (pick && pick.nome) {
          inpPl.value = pick.nome;
          hidPl.value = String(pick.id || '');
        }
      }
    }
    const vel = tr.querySelector('.agro-ns-in-valor');
    if (vel && valStr) vel.value = valStr;
    const dEl = tr.querySelector('.agro-ns-in-desc');
    if (dEl) dEl.value = descr || hint || sugNome;
    calcTotal();
    const msgA = Array.isArray(j.avisos) ? j.avisos.filter(Boolean).join('\n') : '';
    if (msgA) alert(msgA);
    if (inpPl && hidPl && !hidPl.value) inpPl.focus();
  }

  async function preencherPorTexto(opc) {
    opc = opc || {};
    const inpNlp = $('agro-ns-nlp-texto');
    const btnNlp = $('agro-ns-btn-nlp');
    const digitado = (inpNlp && inpNlp.value || '').trim();
    const tBase = opc.segundaRodada ? String(nlpDialogCache.textoOriginal || '').trim() : digitado;
    if (!tBase && !opc.segundaRodada) { alert('Digite a frase no campo de texto.'); return; }
    if (opc.segundaRodada && !tBase) { alert('Referência da frase perdida. Digite de novo.'); return; }
    if (btnNlp) btnNlp.disabled = true;
    try {
      const payload = { texto: tBase, llm: true };
      if (opc.segundaRodada) {
        payload.dados_parciais = nlpDialogCache.dadosParciais || {};
        payload.respostas_dialogo = opc.respostasDialogo || {};
      }
      const r = await fetch(cfg().apiTexto, {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': getCookie('csrftoken') },
        body: JSON.stringify(payload),
      });
      const raw = await r.text();
      let j = {};
      try { j = raw ? JSON.parse(raw) : {}; } catch (_) {
        alert('Resposta inválida do servidor.');
        return;
      }
      if (j.precisa_esclarecimento) {
        nlpDialogCache.textoOriginal = String(j.texto_original || tBase || digitado);
        nlpDialogCache.dadosParciais = j.dados_parciais || {};
        nlpDialogCache.perguntas = Array.isArray(j.perguntas) ? j.perguntas : [];
        const m = $('agro-ns-nlp-motivo');
        if (m) m.textContent = j.motivo_curto || 'Confirme ou complete:';
        renderizarPerguntasNlp(nlpDialogCache.perguntas);
        $('agro-ns-nlp-escl')?.classList.remove('hidden');
        return;
      }
      if (!j.ok) {
        alert(j.erro || 'Não foi possível interpretar.');
        return;
      }
      esconderNlpDialog();
      await aplicarPayloadInterpretado(j);
    } catch (_) {
      alert('Erro de rede ao interpretar texto.');
    } finally {
      if (btnNlp) btnNlp.disabled = false;
    }
  }

  function coletarLinhas() {
    const tipo = tipoAtual();
    const linhas = [];
    document.querySelectorAll('#agro-ns-linhas .agro-ns-linha').forEach((row) => {
      const wrap = row.querySelector('.agro-ns-sug-wrap[data-sug-campo="plano"]');
      const inpPl = wrap ? wrap.querySelector('input[type="text"]') : null;
      const hidPl = wrap ? wrap.querySelector('input[type="hidden"]') : null;
      const plano_nome = inpPl ? inpPl.value.trim() : '';
      const plano_id = hidPl ? hidPl.value.trim() : '';
      const vel = row.querySelector('.agro-ns-in-valor');
      const valor = vel ? String(vel.value || '').trim() : '';
      const dEl = row.querySelector('.agro-ns-in-desc');
      const descricao = dEl ? dEl.value.trim() : '';
      const bEl = row.querySelector('.agro-ns-in-boleto');
      const boleto_raw = bEl ? bEl.value.trim() : '';
      if (plano_nome && valor) {
        const item = { plano_conta: plano_nome, plano_conta_id: plano_id || null, valor, descricao: descricao || undefined };
        if (tipo === 'pagar' && boleto_raw) item.boleto_codigo_barras = boleto_raw;
        linhas.push(item);
      }
    });
    return linhas;
  }

  async function submitForm(ev) {
    ev.preventDefault();
    const tipo = tipoAtual();
    const inputComp = $('agro-ns-data-comp');
    const inputVen = $('agro-ns-data-ven');
    const cbQ = $('agro-ns-quitado');
    const dc = (inputComp && inputComp.value || '').trim();
    const dvRaw = (inputVen && inputVen.value || '').trim();
    const quitado = cbQ ? cbQ.checked : false;
    const dv = dvRaw || dc;
    const empresa_nome = ($('agro-ns-empresa-nome')?.value || '').trim();
    const empresa_id = ($('agro-ns-empresa-id')?.value || '').trim();
    const pessoa_nome = ($('agro-ns-pessoa-nome')?.value || '').trim();
    const pessoa_id = ($('agro-ns-pessoa-id')?.value || '').trim();
    const banco_nome = ($('agro-ns-banco-nome')?.value || '').trim();
    const banco_id = ($('agro-ns-banco-id')?.value || '').trim();
    const forma_nome = ($('agro-ns-forma-nome')?.value || '').trim();
    const forma_id = ($('agro-ns-forma-id')?.value || '').trim();
    const recorrente = $('agro-ns-recorrente')?.checked || false;
    const recMod = (document.querySelector('input[name="agro-ns-rec-modo"]:checked') || {}).value || 'sempre';
    const recParcelas = Math.max(1, Math.min(Number($('agro-ns-rec-parcelas')?.value || 1), 12));
    const linhas = coletarLinhas();

    if (!dc || !dv) { alert('Informe competência e vencimento.'); return; }
    if (!empresa_nome || !pessoa_nome || !banco_nome) { alert('Preencha loja, pessoa e conta bancária.'); return; }
    if (quitado && !banco_id) { alert('Para lançar quitado, escolha uma conta com ID do ERP na lista.'); return; }
    if (!linhas.length) { alert('Inclua ao menos uma linha com plano de conta e valor.'); return; }

    const payload = {
      tipo,
      data_competencia: dc,
      data_vencimento: dv,
      empresa_nome,
      empresa_id: empresa_id || null,
      pessoa_nome,
      pessoa_id: pessoa_id || null,
      banco_nome,
      banco_id: banco_id || null,
      forma_nome,
      forma_id: forma_id || null,
      quitado,
      recorrente,
      recorrente_modo: recorrente ? recMod : 'sempre',
      recorrente_parcelas: recorrente && recMod === 'normal' ? recParcelas : 1,
      linhas,
      idempotency_key: loteIdempotencyKey,
    };

    const btn = $('agro-ns-submit');
    if (btn) {
      btn.disabled = true;
      btn.setAttribute('aria-busy', 'true');
    }
    try {
      const r = await fetch(cfg().apiCriar, {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': getCookie('csrftoken') },
        body: JSON.stringify(payload),
      });
      const raw = await r.text();
      let j = {};
      try {
        j = raw ? JSON.parse(raw) : {};
      } catch (_) {
        const avisoDup = (r.status >= 500 || r.status === 0)
          ? '\n\nO servidor pode ter gravado mesmo assim. Confira em Lançamentos antes de tentar de novo.'
          : '';
        alert('Resposta inválida do servidor (HTTP ' + r.status + ').' + avisoDup);
        return;
      }
      const ids = Array.isArray(j.ids) ? j.ids : [];
      const erros = Array.isArray(j.erros) ? j.erros : [];
      const msgs = erros.map((e) => (e && (e.erro || e.mensagem)) ? String(e.erro || e.mensagem) : '').filter(Boolean);
      const dupBloq = Number(j.duplicidades_bloqueadas || 0);
      let erpLinha = '';
      if (ids.length && j.erp_inclusao_configurada === false) {
        erpLinha = '\n\nERP: integração de inclusão manual desligada no servidor.';
      } else if (j.erp_lancamento_ok === true) {
        erpLinha = '\n\nERP: a API respondeu OK.';
      } else if (j.erp_lancamento_ok === false) {
        erpLinha = '\n\nERP: envio à API falhou: ' + (j.aviso_api || '(sem detalhe)');
      } else if (j.aviso_api) {
        erpLinha = '\n\nERP: ' + j.aviso_api;
      }
      const dicaLista = '\n\nNa consulta: cole o ID no campo de busca, ou «Todos» + vencimento cobrindo a data do título.';
      if (!j.ok && !ids.length) {
        const dupLinha = dupBloq > 0
          ? ('\n\nAtenção: ' + dupBloq + ' lançamento(s) bloqueados por possível duplicidade.')
          : '';
        alert((msgs[0] || j.erro || j.detail || ('Falha ao gravar (HTTP ' + r.status + ').')) + dupLinha);
        return;
      }
      if (!j.ok && ids.length) {
        const det = msgs.length ? ('\n\n' + msgs.slice(0, 12).join('\n') + (msgs.length > 12 ? '\n…' : '')) : '';
        const dupLinha = dupBloq > 0 ? ('\n\nDuplicidade bloqueada: ' + dupBloq + ' lançamento(s).') : '';
        alert('Gravação parcial. IDs: ' + ids.join(', ') + det + dupLinha + erpLinha + dicaLista);
        fechar();
        dispararSucesso(tipo, quitado, dc, dv, dvRaw, ids[0] || '');
        return;
      }
      const dupLinha = dupBloq > 0 ? ('\n\nDuplicidade bloqueada: ' + dupBloq + ' lançamento(s).') : '';
      alert('Lote gravado. IDs: ' + ids.join(', ') + dupLinha + erpLinha + dicaLista);
      fechar();
      dispararSucesso(tipo, quitado, dc, dv, dvRaw, ids[0] || '');
    } catch (_) {
      alert('Erro de rede. Confira em Lançamentos se o lote já foi gravado antes de tentar outra vez.');
    } finally {
      if (btn) {
        btn.disabled = false;
        btn.removeAttribute('aria-busy');
      }
    }
  }

  function dispararSucesso(tipo, quitado, dc, dv, dvRaw, idMongo) {
    const url = urlAposGravarLoteManual(tipo, quitado, dc, dv, dvRaw, idMongo);
    if (typeof window.agroNovaSaidaOnSuccess === 'function') {
      window.agroNovaSaidaOnSuccess({ tipo, quitado, url, idMongo });
      return;
    }
    window.location.href = url;
  }

  function fechar() {
    const ov = $('agro-nova-saida-overlay');
    if (!ov) return;
    ov.classList.add('hidden');
    ov.setAttribute('aria-hidden', 'true');
    document.body.classList.remove('overflow-hidden');
  }

  function abrir(opts) {
    opts = opts || {};
    const ov = $('agro-nova-saida-overlay');
    if (!ov) return;
    if (!bound) init();
    const tipo = opts.tipo === 'receber' ? 'receber' : 'pagar';
    document.querySelectorAll('input[name="agro-ns-tipo"]').forEach((el) => { el.checked = el.value === tipo; });
    syncColBoletoVis();
    const inputComp = $('agro-ns-data-comp');
    const inputVen = $('agro-ns-data-ven');
    if (inputComp && !inputComp.value) inputComp.value = todayISO();
    if (inputVen && !inputVen.value && inputComp) inputVen.value = inputComp.value;
    resetIdempotency();
    ov.classList.remove('hidden');
    ov.setAttribute('aria-hidden', 'false');
    document.body.classList.add('overflow-hidden');
    $('agro-ns-empresa-nome')?.focus();
  }

  function init() {
    if (bound) return;
    bound = true;
    const ov = $('agro-nova-saida-overlay');
    if (!ov) return;

    resetLinhas(1);
    resetIdempotency();

    const inputComp = $('agro-ns-data-comp');
    const inputVen = $('agro-ns-data-ven');
    if (inputComp && !inputComp.value) inputComp.value = todayISO();
    if (inputVen && !inputVen.value && inputComp) inputVen.value = inputComp.value;

    ov.querySelectorAll('.agro-ns-sug-wrap').forEach(attachSuggest);

    $('agro-ns-quitado')?.addEventListener('change', syncQuitadoVenc);
    inputComp?.addEventListener('change', () => {
      const cbQ = $('agro-ns-quitado');
      if (cbQ && cbQ.checked && inputVen) inputVen.value = inputComp.value;
    });

    $('agro-ns-recorrente')?.addEventListener('change', syncRecOpts);
    document.querySelectorAll('input[name="agro-ns-rec-modo"]').forEach((r) => r.addEventListener('change', syncRecOpts));
    syncRecOpts();

    document.querySelectorAll('input[name="agro-ns-tipo"]').forEach((r) => {
      r.addEventListener('change', syncColBoletoVis);
    });

    $('agro-ns-add-linha')?.addEventListener('click', () => addRow());
    $('agro-ns-form')?.addEventListener('submit', submitForm);
    $('agro-ns-fechar')?.addEventListener('click', fechar);
    $('agro-ns-cancelar')?.addEventListener('click', fechar);
    $('agro-ns-btn-nlp')?.addEventListener('click', () => preencherPorTexto());
    $('agro-ns-nlp-aplicar')?.addEventListener('click', () => {
      const rsp = coletarRespostasDialogo(nlpDialogCache.perguntas);
      if (!Object.keys(rsp).length) {
        alert('Responda às perguntas antes de aplicar.');
        return;
      }
      preencherPorTexto({ segundaRodada: true, respostasDialogo: rsp });
    });
    $('agro-ns-nlp-texto')?.addEventListener('keydown', (ev) => {
      if (ev.key === 'Enter') {
        ev.preventDefault();
        preencherPorTexto();
      }
    });

    ov.addEventListener('click', (ev) => {
      if (ev.target === ov) fechar();
    });
    document.addEventListener('keydown', (ev) => {
      if (ev.key === 'Escape' && ov && !ov.classList.contains('hidden')) fechar();
    });

    document.addEventListener('click', (ev) => {
      const btn = ev.target.closest('[data-agro-nova-saida-open]');
      if (!btn) return;
      ev.preventDefault();
      const tipo = btn.getAttribute('data-tipo') || 'pagar';
      abrir({ tipo });
    });
  }

  window.AgroNovaSaida = { open: abrir, close: fechar, init };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
