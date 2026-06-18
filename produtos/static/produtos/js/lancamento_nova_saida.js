/**
 * Modal «Nova saída» — um card por lançamento (campos completos por linha).
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

  function syncTemaTipo() {
    const panel = $('agro-ns-panel');
    if (!panel) return;
    const rec = tipoAtual() === 'receber';
    panel.classList.toggle('agro-ns-panel--receber', rec);
    panel.classList.toggle('agro-ns-panel--pagar', !rec);
  }

  function urlAposGravarLoteManual(tipo, quitado, dc, dvMin, dvMax, idMongo) {
    const c = cfg();
    const base = tipo === 'receber' ? c.urlListaReceber : c.urlListaPagar;
    const u = new URL(base, window.location.origin);
    const partes = [dc, dvMin, dvMax].map((x) => String(x || '').trim()).filter((x) => x.length >= 8);
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
        if (inp.value.length < 2) { dd.classList.add('hidden'); return; }
        const api = cfg().apiSug;
        if (!api) return;
        try {
          const r = await fetch(`${api}?campo=${encodeURIComponent(campo)}&q=${encodeURIComponent(inp.value)}`, { credentials: 'same-origin' });
          const j = await r.json();
          const itens = j.itens || [];
          dd.innerHTML = itens.map((it) => {
            const nome = String(it.nome || '').replace(/"/g, '&quot;');
            return `<li data-nome="${nome}" data-id="${String(it.id || '')}">${it.nome}</li>`;
          }).join('');
          dd.querySelectorAll('li').forEach((li) => {
            li.addEventListener('mousedown', (ev) => {
              ev.preventDefault();
              inp.value = li.dataset.nome || '';
              if (hid) hid.value = li.dataset.id || '';
              dd.classList.add('hidden');
              const card = wrap.closest('.agro-ns-card');
              if (card) atualizarResumoCard(card);
            });
          });
          dd.classList.toggle('hidden', !itens.length);
        } catch (_) { /* ignore */ }
      }, 300);
    });
    inp.addEventListener('blur', () => setTimeout(() => dd.classList.add('hidden'), 200));
  }

  function attachSugAll(root) {
    (root || document).querySelectorAll('.agro-ns-sug-wrap').forEach(attachSuggest);
  }

  function fmtDataBr(iso) {
    const s = String(iso || '').trim();
    if (s.length < 10) return '—';
    const p = s.slice(0, 10).split('-');
    if (p.length !== 3) return s;
    return `${p[2]}/${p[1]}/${p[0]}`;
  }

  function atualizarResumoCard(card) {
    const txt = card.querySelector('.agro-ns-card-resumo-txt');
    if (!txt) return;
    const plan = sugVal(card, 'plano').nome || 'Sem plano';
    const pes = sugVal(card, 'cliente').nome;
    const valorRaw = String(card.querySelector('.agro-ns-in-valor')?.value || '').trim();
    const valor = valorRaw || '0,00';
    const ven = fmtDataBr(card.querySelector('.agro-ns-in-ven')?.value);
    const quit = card.querySelector('.agro-ns-in-quitado')?.checked ? ' · Quitado' : '';
    const pessoa = pes ? ` · ${pes}` : '';
    txt.textContent = `${plan}${pessoa} · R$ ${valor} · Venc. ${ven}${quit}`;
  }

  function retrairCard(card) {
    if (!card) return;
    atualizarResumoCard(card);
    card.classList.remove('agro-ns-card--expandido');
    card.classList.add('agro-ns-card--retraido');
  }

  function expandirCard(card) {
    if (!card) return;
    document.querySelectorAll('#agro-ns-linhas .agro-ns-card').forEach((c) => {
      if (c !== card) retrairCard(c);
    });
    card.classList.remove('agro-ns-card--retraido');
    card.classList.add('agro-ns-card--expandido');
    requestAnimationFrame(() => {
      card.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    });
  }

  function focarCard(card) {
    if (!card) return;
    card.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    const first = card.querySelector('.agro-ns-sug-wrap[data-sug-campo="empresa"] input[type="text"]')
      || card.querySelector('input:not([type="hidden"])');
    first?.focus({ preventScroll: true });
  }

  function bindResumoUpdates(card) {
    card.querySelectorAll('input, select').forEach((el) => {
      const ev = el.type === 'checkbox' || el.type === 'date' || el.tagName === 'SELECT' ? 'change' : 'input';
      el.addEventListener(ev, () => atualizarResumoCard(card));
    });
  }

  function bindResumoClick(card) {
    card.querySelector('.agro-ns-card-resumo')?.addEventListener('click', () => {
      expandirCard(card);
      focarCard(card);
    });
  }

  function syncQuitadoLinha(card) {
    const cbQ = card.querySelector('.agro-ns-in-quitado');
    const wrap = card.querySelector('.agro-ns-wrap-ven');
    if (!wrap || !cbQ) return;
    wrap.classList.toggle('is-quitado', cbQ.checked);
  }

  function bindLinhaDatas(card) {
    const comp = card.querySelector('.agro-ns-in-comp');
    const ven = card.querySelector('.agro-ns-in-ven');
    const cbQ = card.querySelector('.agro-ns-in-quitado');
    comp?.addEventListener('change', () => {
      if (cbQ?.checked && ven) ven.value = comp.value || ven.value;
    });
    cbQ?.addEventListener('change', () => {
      syncQuitadoLinha(card);
      if (cbQ.checked && comp?.value && ven) ven.value = comp.value;
    });
    syncQuitadoLinha(card);
  }

  function syncRecLinha(card) {
    const cb = card.querySelector('.agro-ns-rec-cb');
    const opts = card.querySelector('.agro-ns-rec-opts');
    const cont = card.querySelector('.agro-ns-rec-contagem');
    const sel = card.querySelector('.agro-ns-rec-parcelas');
    const on = cb && cb.checked;
    if (opts) {
      opts.style.opacity = on ? '1' : '0.45';
      opts.querySelectorAll('input, select').forEach((el) => { el.disabled = !on; });
    }
    const mod = (card.querySelector('input[name^="agro-ns-rec-modo-"]:checked') || {}).value || 'sempre';
    if (cont) {
      const show = on && mod === 'normal';
      cont.classList.toggle('hidden', !show);
      if (sel) sel.disabled = !show;
    }
  }

  function bindRecLinha(card) {
    const cb = card.querySelector('.agro-ns-rec-cb');
    cb?.addEventListener('change', () => syncRecLinha(card));
    card.querySelectorAll('input[name^="agro-ns-rec-modo-"]').forEach((r) => {
      r.addEventListener('change', () => syncRecLinha(card));
    });
    syncRecLinha(card);
  }

  function copiarValoresCard(origem, destino) {
    if (!origem || !destino) return;
    origem.querySelectorAll('.agro-ns-sug-wrap').forEach((wrap, i) => {
      const dWrap = destino.querySelectorAll('.agro-ns-sug-wrap')[i];
      if (!dWrap) return;
      const oIn = wrap.querySelector('input[type="text"]');
      const oH = wrap.querySelector('input[type="hidden"]');
      const dIn = dWrap.querySelector('input[type="text"]');
      const dH = dWrap.querySelector('input[type="hidden"]');
      if (oIn && dIn) dIn.value = oIn.value;
      if (oH && dH) dH.value = oH.value;
    });
    const comp = origem.querySelector('.agro-ns-in-comp');
    const ven = origem.querySelector('.agro-ns-in-ven');
    const dComp = destino.querySelector('.agro-ns-in-comp');
    const dVen = destino.querySelector('.agro-ns-in-ven');
    if (comp && dComp) dComp.value = comp.value;
    if (ven && dVen) dVen.value = ven.value;
    const oQ = origem.querySelector('.agro-ns-in-quitado');
    const dQ = destino.querySelector('.agro-ns-in-quitado');
    if (oQ && dQ) {
      dQ.checked = oQ.checked;
      syncQuitadoLinha(destino);
    }
  }

  function atualizarNumeracaoCards() {
    const cards = document.querySelectorAll('#agro-ns-linhas .agro-ns-card');
    cards.forEach((card, idx) => {
      card.querySelectorAll('.agro-ns-card-num strong').forEach((num) => {
        num.textContent = String(idx + 1);
      });
      const rm = card.querySelector('.agro-ns-rm-linha');
      if (rm) rm.classList.toggle('hidden', cards.length <= 1);
      atualizarResumoCard(card);
    });
  }

  function cardHtml(idStr) {
    return `
    <article class="agro-ns-card agro-ns-linha agro-ns-card--expandido" data-row-id="${idStr}">
      <button type="button" class="agro-ns-card-resumo" title="Expandir lançamento">
        <span class="agro-ns-card-num">Lançamento <strong>1</strong></span>
        <span class="agro-ns-card-resumo-txt">—</span>
        <span class="agro-ns-card-resumo-edit">Editar</span>
      </button>
      <div class="agro-ns-card-inner">
      <div class="agro-ns-card-head">
        <span class="agro-ns-card-num">Lançamento <strong>1</strong></span>
        <button type="button" class="agro-ns-rm-linha hidden" title="Remover lançamento">Remover</button>
      </div>
      <div class="agro-ns-card-row agro-ns-card-row--4">
        <div class="flex flex-col gap-1 min-w-0">
          <label class="agro-ns-label">Loja</label>
          <div class="relative agro-ns-sug-wrap" data-sug-campo="empresa">
            <input type="text" placeholder="Buscar loja…" autocomplete="off" class="agro-ns-input agro-ns-input-icon">
            <input type="hidden" class="agro-ns-hid-empresa">
            <svg class="agro-ns-ico" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"></path></svg>
            <ul class="agro-ns-sug-dd hidden absolute left-0 right-0 top-full mt-0.5 bg-white border-2 border-slate-200 rounded-xl shadow-xl overflow-y-auto"></ul>
          </div>
        </div>
        <div class="flex flex-col gap-1 min-w-0">
          <label class="agro-ns-label">Pessoa</label>
          <div class="relative agro-ns-sug-wrap" data-sug-campo="cliente">
            <input type="text" placeholder="Cliente / fornecedor…" autocomplete="off" class="agro-ns-input agro-ns-input-icon">
            <input type="hidden" class="agro-ns-hid-pessoa">
            <svg class="agro-ns-ico" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z"></path></svg>
            <ul class="agro-ns-sug-dd hidden absolute left-0 right-0 top-full mt-0.5 bg-white border-2 border-slate-200 rounded-xl shadow-xl overflow-y-auto"></ul>
          </div>
        </div>
        <div class="flex flex-col gap-1 min-w-0">
          <label class="agro-ns-label">Conta / Caixa</label>
          <div class="relative agro-ns-sug-wrap" data-sug-campo="banco">
            <input type="text" placeholder="Buscar conta…" autocomplete="off" class="agro-ns-input agro-ns-input-icon">
            <input type="hidden" class="agro-ns-hid-banco">
            <svg class="agro-ns-ico" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 10h18M7 15h1m4 0h1m-7 4h12a3 3 0 003-3V8a3 3 0 00-3-3H6a3 3 0 00-3 3v8a3 3 0 003 3z"></path></svg>
            <ul class="agro-ns-sug-dd hidden absolute left-0 right-0 top-full mt-0.5 bg-white border-2 border-slate-200 rounded-xl shadow-xl overflow-y-auto"></ul>
          </div>
        </div>
        <div class="flex flex-col gap-1 min-w-0">
          <label class="agro-ns-label">Forma pagamento</label>
          <div class="relative agro-ns-sug-wrap" data-sug-campo="forma">
            <input type="text" placeholder="Forma…" autocomplete="off" class="agro-ns-input">
            <input type="hidden" class="agro-ns-hid-forma">
            <ul class="agro-ns-sug-dd hidden absolute left-0 right-0 top-full mt-0.5 bg-white border-2 border-slate-200 rounded-xl shadow-xl overflow-y-auto"></ul>
          </div>
        </div>
      </div>
      <div class="agro-ns-card-row agro-ns-card-row--plano">
        <div class="flex flex-col gap-1 min-w-0">
          <label class="agro-ns-label">Plano de contas</label>
          <div class="relative agro-ns-sug-wrap" data-sug-campo="plano">
            <input type="text" id="agro-ns-plano-${idStr}" placeholder="Buscar plano…" autocomplete="off" class="agro-ns-input">
            <input type="hidden" id="agro-ns-plano-id-${idStr}">
            <ul class="agro-ns-sug-dd hidden absolute left-0 right-0 top-full mt-0.5 bg-white border-2 border-slate-200 rounded-xl shadow-xl overflow-y-auto"></ul>
          </div>
        </div>
        <div class="flex flex-col gap-1 min-w-0">
          <label class="agro-ns-label">Valor (R$)</label>
          <input type="text" class="agro-ns-input agro-ns-in-valor" placeholder="0,00" inputmode="decimal">
        </div>
        <div class="flex flex-col gap-1 min-w-0">
          <label class="agro-ns-label">Competência</label>
          <input type="date" class="agro-ns-input agro-ns-in-comp">
        </div>
        <div class="flex flex-col gap-1 min-w-0 agro-ns-wrap-ven">
          <label class="agro-ns-label">Vencimento</label>
          <div class="flex items-stretch gap-2 min-w-0">
            <input type="date" class="agro-ns-input agro-ns-in-ven flex-1 min-w-0">
            <label class="agro-ns-quitado-chip shrink-0 cursor-pointer self-stretch flex items-center" title="Já pago ou recebido">
              <input type="checkbox" class="agro-ns-in-quitado sr-only">
              <span class="agro-ns-quitado-chip-btn h-full">Quitado</span>
            </label>
          </div>
        </div>
      </div>
      <div class="agro-ns-card-row-desc">
        <label class="agro-ns-label block mb-1">Descrição</label>
        <input type="text" class="agro-ns-input agro-ns-in-desc w-full">
      </div>
      <details class="agro-ns-card-rec">
        <summary>Recorrência mensal (opcional)</summary>
        <div class="agro-ns-card-rec-body space-y-2">
          <label class="flex items-center gap-3 cursor-pointer">
            <input type="checkbox" class="agro-ns-rec-cb agro-ns-chk">
            <span class="agro-ns-body font-bold">Ativar recorrência neste lançamento</span>
          </label>
          <div class="agro-ns-rec-opts opacity-45 space-y-2">
            <fieldset class="rounded-xl border border-slate-200 bg-white px-3 py-2 space-y-2">
              <legend class="agro-ns-hint font-black uppercase px-1">Modo</legend>
              <label class="flex items-start gap-2 cursor-pointer agro-ns-body">
                <input type="radio" name="agro-ns-rec-modo-${idStr}" value="sempre" checked class="mt-1 shrink-0">
                <span><strong>Sempre</strong> — próximo só após quitar.</span>
              </label>
              <label class="flex items-start gap-2 cursor-pointer agro-ns-body">
                <input type="radio" name="agro-ns-rec-modo-${idStr}" value="normal" class="mt-1 shrink-0">
                <span><strong>Normal</strong> — N títulos já criados.</span>
              </label>
            </fieldset>
            <div class="agro-ns-rec-contagem hidden">
              <label class="agro-ns-hint font-black uppercase block mb-1">Quantidade</label>
              <select class="agro-ns-rec-parcelas agro-ns-input max-w-xs" disabled>
                <option value="1" selected>1</option><option value="2">2</option><option value="3">3</option>
                <option value="4">4</option><option value="6">6</option><option value="12">12</option>
              </select>
            </div>
          </div>
        </div>
      </details>
      </div>
    </article>`;
  }

  function addRow(opts) {
    opts = opts || {};
    const host = $('agro-ns-linhas');
    if (!host) return null;
    rowCount += 1;
    const idStr = `r${rowCount}`;
    const prev = host.querySelector('.agro-ns-card:last-child');
    const wrap = document.createElement('div');
    wrap.innerHTML = cardHtml(idStr);
    const card = wrap.firstElementChild;
    host.appendChild(card);

    const t = todayISO();
    if (!prev) {
      card.querySelector('.agro-ns-in-comp').value = t;
      card.querySelector('.agro-ns-in-ven').value = t;
    } else {
      copiarValoresCard(prev, card);
    }

    card.querySelector('.agro-ns-rm-linha')?.addEventListener('click', (ev) => {
      ev.stopPropagation();
      if (host.querySelectorAll('.agro-ns-card').length <= 1) return;
      const eraExpandido = card.classList.contains('agro-ns-card--expandido');
      card.remove();
      atualizarNumeracaoCards();
      if (eraExpandido) {
        const ultimo = host.querySelector('.agro-ns-card:last-child');
        if (ultimo) { expandirCard(ultimo); focarCard(ultimo); }
      } else {
        document.querySelectorAll('#agro-ns-linhas .agro-ns-card').forEach(atualizarResumoCard);
      }
    });

    attachSugAll(card);
    bindLinhaDatas(card);
    bindRecLinha(card);
    bindResumoUpdates(card);
    bindResumoClick(card);
    atualizarNumeracaoCards();
    atualizarResumoCard(card);

    expandirCard(card);
    if (opts.collapsePrevious) focarCard(card);
    return card;
  }

  function syncQuitadoVenc() {
    document.querySelectorAll('#agro-ns-linhas .agro-ns-card').forEach((card) => syncQuitadoLinha(card));
  }

  function algumQuitado(linhas) {
    return (linhas || []).some((l) => !!l.quitado);
  }

  function resumoDatasLinhas(linhas) {
    const comps = linhas.map((l) => l.data_competencia).filter(Boolean).sort();
    const vens = linhas.map((l) => l.data_vencimento).filter(Boolean).sort();
    const dc = comps[0] || todayISO();
    const dvMin = vens[0] || dc;
    const dvMax = vens[vens.length - 1] || dvMin;
    return { dc, dvMin, dvMax };
  }

  function resetLinhas(n) {
    const host = $('agro-ns-linhas');
    if (!host) return;
    host.innerHTML = '';
    rowCount = 0;
    for (let i = 0; i < Math.max(1, n || 1); i += 1) addRow({ collapsePrevious: false });
  }

  function resetIdempotency() {
    loteIdempotencyKey = (typeof crypto !== 'undefined' && crypto.randomUUID)
      ? crypto.randomUUID()
      : ('lote-' + Date.now() + '-' + Math.random().toString(36).slice(2));
  }

  function sugVal(card, campo) {
    const wrap = card.querySelector(`.agro-ns-sug-wrap[data-sug-campo="${campo}"]`);
    if (!wrap) return { nome: '', id: '' };
    const inp = wrap.querySelector('input[type="text"]');
    const hid = wrap.querySelector('input[type="hidden"]');
    return { nome: inp ? inp.value.trim() : '', id: hid ? hid.value.trim() : '' };
  }

  function coletarLinhas() {
    const linhas = [];
    let n = 0;
    document.querySelectorAll('#agro-ns-linhas .agro-ns-card').forEach((card) => {
      n += 1;
      const emp = sugVal(card, 'empresa');
      const pes = sugVal(card, 'cliente');
      const ban = sugVal(card, 'banco');
      const form = sugVal(card, 'forma');
      const plan = sugVal(card, 'plano');
      const valor = String(card.querySelector('.agro-ns-in-valor')?.value || '').trim();
      const descricao = String(card.querySelector('.agro-ns-in-desc')?.value || '').trim();
      const data_competencia = String(card.querySelector('.agro-ns-in-comp')?.value || '').trim();
      const data_vencimento = String(card.querySelector('.agro-ns-in-ven')?.value || '').trim();
      const recCb = card.querySelector('.agro-ns-rec-cb');
      const recorrente = !!(recCb && recCb.checked);
      const recMod = (card.querySelector('input[name^="agro-ns-rec-modo-"]:checked') || {}).value || 'sempre';
      const recParcelas = Math.max(1, Math.min(Number(card.querySelector('.agro-ns-rec-parcelas')?.value || 1), 12));
      const quitado = !!card.querySelector('.agro-ns-in-quitado')?.checked;

      const vazio = !plan.nome && !valor && !descricao && !emp.nome && !pes.nome && !ban.nome;
      if (vazio) return;

      linhas.push({
        empresa_nome: emp.nome,
        empresa_id: emp.id || null,
        pessoa_nome: pes.nome,
        pessoa_id: pes.id || null,
        banco_nome: ban.nome,
        banco_id: ban.id || null,
        forma_nome: form.nome,
        forma_id: form.id || null,
        plano_conta: plan.nome,
        plano_conta_id: plan.id || null,
        valor,
        descricao: descricao || undefined,
        data_competencia,
        data_vencimento: data_vencimento || data_competencia,
        recorrente,
        recorrente_modo: recorrente ? recMod : 'sempre',
        recorrente_parcelas: recorrente && recMod === 'normal' ? recParcelas : 1,
        quitado,
        _num: n,
      });
    });
    return linhas;
  }

  function validarLinhas(linhasRaw) {
    if (!linhasRaw.length) {
      alert('Preencha ao menos um lançamento (plano e valor).');
      return false;
    }
    for (let i = 0; i < linhasRaw.length; i += 1) {
      const ln = linhasRaw[i];
      const num = ln._num || i + 1;
      if (!ln.plano_conta || !ln.valor) {
        alert(`Lançamento ${num}: informe plano de contas e valor.`);
        return false;
      }
      if (!ln.empresa_nome || !ln.pessoa_nome || !ln.banco_nome) {
        alert(`Lançamento ${num}: preencha loja, pessoa e conta.`);
        return false;
      }
      if (!ln.data_competencia || !ln.data_vencimento) {
        alert(`Lançamento ${num}: informe competência e vencimento.`);
        return false;
      }
      if (ln.quitado && !ln.banco_id) {
        alert(`Lançamento ${num}: para quitado, escolha conta com ID do ERP na lista.`);
        return false;
      }
    }
    return true;
  }

  async function submitForm(ev) {
    ev.preventDefault();
    const tipo = tipoAtual();
    const linhasRaw = coletarLinhas();
    if (!validarLinhas(linhasRaw)) return;

    const linhas = linhasRaw.map(({ _num, ...rest }) => rest);
    const quitadoLote = algumQuitado(linhas);
    const { dc, dvMin, dvMax } = resumoDatasLinhas(linhas);
    const cab = linhas[0] || {};

    const payload = {
      tipo,
      data_competencia: dc,
      data_vencimento: dvMin,
      empresa_nome: cab.empresa_nome || '',
      empresa_id: cab.empresa_id || null,
      pessoa_nome: cab.pessoa_nome || '',
      pessoa_id: cab.pessoa_id || null,
      banco_nome: cab.banco_nome || '',
      banco_id: cab.banco_id || null,
      forma_nome: cab.forma_nome || '',
      forma_id: cab.forma_id || null,
      quitado: quitadoLote,
      recorrente: false,
      linhas,
      idempotency_key: loteIdempotencyKey,
    };

    const btn = $('agro-ns-submit');
    if (btn) { btn.disabled = true; btn.setAttribute('aria-busy', 'true'); }
    try {
      const r = await fetch(cfg().apiCriar, {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': getCookie('csrftoken') },
        body: JSON.stringify(payload),
      });
      const raw = await r.text();
      let j = {};
      try { j = raw ? JSON.parse(raw) : {}; } catch (_) {
        alert('Resposta inválida do servidor (HTTP ' + r.status + ').');
        return;
      }
      const ids = Array.isArray(j.ids) ? j.ids : [];
      const erros = Array.isArray(j.erros) ? j.erros : [];
      const msgs = erros.map((e) => (e && (e.erro || e.mensagem)) ? String(e.erro || e.mensagem) : '').filter(Boolean);
      const dupBloq = Number(j.duplicidades_bloqueadas || 0);
      let erpLinha = '';
      if (ids.length && j.erp_inclusao_configurada === false) erpLinha = '\n\nERP: integração desligada no servidor.';
      else if (j.erp_lancamento_ok === true) erpLinha = '\n\nERP: OK.';
      else if (j.erp_lancamento_ok === false) erpLinha = '\n\nERP: falhou — ' + (j.aviso_api || '');
      const dicaLista = '\n\nNa lista: filtro «Todos» + vencimento cobrindo as datas.';
      if (!j.ok && !ids.length) {
        alert((msgs[0] || j.erro || 'Falha ao gravar.') + (dupBloq ? `\n\nDuplicidade bloqueada: ${dupBloq}.` : ''));
        return;
      }
      if (!j.ok && ids.length) {
        alert('Gravação parcial. IDs: ' + ids.join(', ') + erpLinha + dicaLista);
      } else {
        alert('Lote gravado. IDs: ' + ids.join(', ') + erpLinha + dicaLista);
      }
      fechar();
      dispararSucesso(tipo, quitadoLote, dc, dvMin, dvMax, ids[0] || '');
    } catch (_) {
      alert('Erro de rede. Confira em Lançamentos se já gravou antes de repetir.');
    } finally {
      if (btn) { btn.disabled = false; btn.removeAttribute('aria-busy'); }
    }
  }

  function dispararSucesso(tipo, quitado, dc, dvMin, dvMax, idMongo) {
    const url = urlAposGravarLoteManual(tipo, quitado, dc, dvMin, dvMax, idMongo);
    if (typeof window.agroNovaSaidaOnSuccess === 'function') {
      window.agroNovaSaidaOnSuccess({ tipo, quitado, url, idMongo });
      return;
    }
    window.location.href = url;
  }

  function esconderNlpDialog() { $('agro-ns-nlp-escl')?.classList.add('hidden'); }

  function renderizarPerguntasNlp(perguntas) {
    const host = $('agro-ns-nlp-campos');
    if (!host) return;
    host.innerHTML = '';
    (perguntas || []).forEach((p, idx) => {
      const lid = document.createElement('label');
      lid.className = 'block space-y-1';
      const sp = document.createElement('span');
      sp.className = 'agro-ns-label';
      sp.textContent = p.texto || p.id || '';
      lid.appendChild(sp);
      const uid = `agro-ns-nlp-r-${idx}`;
      if (String(p.tipo_input || '').toLowerCase() === 'opcoes' && Array.isArray(p.opcoes) && p.opcoes.length) {
        const sel = document.createElement('select');
        sel.id = uid;
        sel.className = 'agro-ns-input';
        sel.appendChild(new Option('Escolha...', ''));
        p.opcoes.forEach((o) => sel.appendChild(new Option(String(o.rotulo || o.valor), String(o.valor || ''))));
        lid.appendChild(sel);
      } else {
        const inp = document.createElement('input');
        inp.type = 'text';
        inp.id = uid;
        inp.className = 'agro-ns-input';
        lid.appendChild(inp);
      }
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
    syncTemaTipo();
    let card = document.querySelector('#agro-ns-linhas .agro-ns-card');
    if (!card) { resetLinhas(1); card = document.querySelector('#agro-ns-linhas .agro-ns-card'); }
    if (!card) return;

    const cbQ = card.querySelector('.agro-ns-in-quitado');
    if (cbQ) { cbQ.checked = !!j.quitado_hint; syncQuitadoLinha(card); }

    const linha0 = (j.linhas && j.linhas[0]) || {};
    const dcHint = j.data_competencia || linha0.data_competencia;
    const dvHint = j.data_vencimento || linha0.data_vencimento;
    if (dcHint) card.querySelector('.agro-ns-in-comp').value = dcHint;
    if (dvHint) card.querySelector('.agro-ns-in-ven').value = dvHint;

    const hint = String(linha0.plano_hint || '').trim();
    const sugNome = String(linha0.plano_sugerido_nome || '').trim();
    const sugId = String(linha0.plano_sugerido_id || '').trim();
    const valStr = String(linha0.valor || '').trim();
    const descr = linha0.descricao ? String(linha0.descricao).trim() : '';
    const wrapPl = card.querySelector('.agro-ns-sug-wrap[data-sug-campo="plano"]');
    const inpPl = wrapPl?.querySelector('input[type="text"]');
    const hidPl = wrapPl?.querySelector('input[type="hidden"]');
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
        if (pick?.nome) { inpPl.value = pick.nome; hidPl.value = String(pick.id || ''); }
      }
    }
    const vel = card.querySelector('.agro-ns-in-valor');
    if (vel && valStr) vel.value = valStr;
    const dEl = card.querySelector('.agro-ns-in-desc');
    if (dEl) dEl.value = descr || hint || sugNome;
    atualizarResumoCard(card);
    if (Array.isArray(j.avisos) && j.avisos.filter(Boolean).length) alert(j.avisos.filter(Boolean).join('\n'));
  }

  async function preencherPorTexto(opc) {
    opc = opc || {};
    const inpNlp = $('agro-ns-nlp-texto');
    const tBase = opc.segundaRodada ? String(nlpDialogCache.textoOriginal || '').trim() : String(inpNlp?.value || '').trim();
    if (!tBase) { alert('Digite a frase.'); return; }
    const btnNlp = $('agro-ns-btn-nlp');
    if (btnNlp) btnNlp.disabled = true;
    try {
      const payload = { texto: tBase, llm: true };
      if (opc.segundaRodada) {
        payload.dados_parciais = nlpDialogCache.dadosParciais || {};
        payload.respostas_dialogo = opc.respostasDialogo || {};
      }
      const r = await fetch(cfg().apiTexto, {
        method: 'POST', credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': getCookie('csrftoken') },
        body: JSON.stringify(payload),
      });
      const j = JSON.parse(await r.text() || '{}');
      if (j.precisa_esclarecimento) {
        nlpDialogCache = { textoOriginal: String(j.texto_original || tBase), dadosParciais: j.dados_parciais || {}, perguntas: j.perguntas || [] };
        $('agro-ns-nlp-motivo').textContent = j.motivo_curto || 'Confirme:';
        renderizarPerguntasNlp(nlpDialogCache.perguntas);
        $('agro-ns-nlp-escl')?.classList.remove('hidden');
        return;
      }
      if (!j.ok) { alert(j.erro || 'Não foi possível interpretar.'); return; }
      esconderNlpDialog();
      await aplicarPayloadInterpretado(j);
    } catch (_) {
      alert('Erro de rede.');
    } finally {
      if (btnNlp) btnNlp.disabled = false;
    }
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
    syncTemaTipo();
    const inpNlp = $('agro-ns-nlp-texto');
    if (inpNlp) inpNlp.value = '';
    esconderNlpDialog();
    resetLinhas(1);
    resetIdempotency();
    ov.classList.remove('hidden');
    ov.setAttribute('aria-hidden', 'false');
    document.body.classList.add('overflow-hidden');
    const first = document.querySelector('#agro-ns-linhas .agro-ns-card .agro-ns-sug-wrap[data-sug-campo="empresa"] input[type="text"]');
    first?.focus();
  }

  function init() {
    if (bound) return;
    bound = true;
    resetLinhas(1);
    resetIdempotency();
    syncTemaTipo();

    document.querySelectorAll('input[name="agro-ns-tipo"]').forEach((r) => {
      r.addEventListener('change', syncTemaTipo);
    });
    $('agro-ns-add-linha')?.addEventListener('click', () => addRow({ collapsePrevious: true }));
    $('agro-ns-form')?.addEventListener('submit', submitForm);
    $('agro-ns-fechar')?.addEventListener('click', fechar);
    $('agro-ns-cancelar')?.addEventListener('click', fechar);
    $('agro-ns-btn-nlp')?.addEventListener('click', () => preencherPorTexto());
    $('agro-ns-nlp-aplicar')?.addEventListener('click', () => {
      const rsp = coletarRespostasDialogo(nlpDialogCache.perguntas);
      if (!Object.keys(rsp).length) { alert('Responda às perguntas.'); return; }
      preencherPorTexto({ segundaRodada: true, respostasDialogo: rsp });
    });

    const ov = $('agro-nova-saida-overlay');
    ov?.addEventListener('click', (ev) => { if (ev.target === ov) fechar(); });
    document.addEventListener('keydown', (ev) => {
      if (ev.key === 'Escape' && ov && !ov.classList.contains('hidden')) fechar();
    });
    document.addEventListener('click', (ev) => {
      const btn = ev.target.closest('[data-agro-nova-saida-open]');
      if (!btn) return;
      ev.preventDefault();
      abrir({ tipo: btn.getAttribute('data-tipo') || 'pagar' });
    });
  }

  window.AgroNovaSaida = { open: abrir, close: fechar, init };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
