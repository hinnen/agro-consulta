/**
 * Pseudo-plano «Empréstimo (entrada + pagamento)» — modal Nova saída e lote manual.
 */
(function (global) {
  'use strict';

  const DEFAULT_ID = '__AGRO_EMPRESTIMO_DUAL__';
  const DEFAULT_LABEL = 'Empréstimo (entrada + pagamento)';

  function cfg() {
    return global.AGRO_LANC_EMPRESTIMO_CFG || {};
  }

  function dualId() {
    return String(cfg().dual_id || DEFAULT_ID).trim();
  }

  function dualLabel() {
    return String(cfg().dual_label || DEFAULT_LABEL).trim();
  }

  function isDualId(id) {
    return String(id || '').trim() === dualId();
  }

  function isDualLabel(nome) {
    return String(nome || '').trim() === dualLabel();
  }

  function isDualItem(it) {
    if (!it) return false;
    if (it._emprestimo_dual || it.emprestimo_dual) return true;
    return isDualId(it.id) || isDualLabel(it.nome);
  }

  function queryMatchesDual(q) {
    const ql = String(q || '').trim().toLowerCase();
    if (ql.length < 2) return false;
    const label = dualLabel().toLowerCase();
    if (label.includes(ql) || ql.includes('emprest')) return true;
    const tokens = ['emprestimo', 'empréstimo', 'entrada', 'pagamento'];
    return tokens.some((t) => ql.includes(t) && (label.includes(t) || t.startsWith('emprest')));
  }

  function injectSuggestItems(itens, q) {
    const list = Array.isArray(itens) ? itens.slice() : [];
    if (!queryMatchesDual(q)) return list;
    if (list.some(isDualItem)) return list;
    list.unshift({ id: dualId(), nome: dualLabel(), _emprestimo_dual: true, emprestimo_dual: true });
    return list;
  }

  function isCardDual(card) {
    return !!(card && card.dataset && card.dataset.emprestimoDual === '1');
  }

  function isRowDual(tr) {
    return !!(tr && tr.dataset && tr.dataset.emprestimoDual === '1');
  }

  function syncHeaderEmprestimoAjuda() {
    const det = document.getElementById('agro-ns-emprestimo-ajuda');
    if (!det) return;
    const algum = document.querySelector('#agro-ns-linhas .agro-ns-card[data-emprestimo-dual="1"]');
    det.classList.toggle('hidden', !algum);
    if (!algum && det.open) det.open = false;
  }

  function ativarModoDualModal(card) {
    if (!card || isCardDual(card)) return;
    card.dataset.emprestimoDual = '1';
    const planWrap = card.querySelector('.agro-ns-sug-wrap[data-sug-campo="plano"]');
    const planInp = planWrap?.querySelector('input[type="text"]');
    const planHid = planWrap?.querySelector('input[type="hidden"]');
    if (planInp) planInp.value = dualLabel();
    if (planHid) planHid.value = dualId();

    card.querySelector('.agro-ns-valor-normal')?.classList.add('hidden');
    card.querySelector('.agro-ns-valor-dual')?.classList.remove('hidden');
    card.querySelector('.agro-ns-valor-lbl-normal')?.classList.add('hidden');
    card.querySelector('.agro-ns-card-rec')?.classList.add('hidden');

    const cbRec = card.querySelector('.agro-ns-rec-cb');
    if (cbRec) {
      cbRec.checked = false;
      cbRec.disabled = true;
    }
    card.classList.add('agro-ns-card--emprestimo-dual');
    syncHeaderEmprestimoAjuda();
    if (typeof window.__agroNsSyncParc === 'function') window.__agroNsSyncParc(card);
  }

  function desativarModoDualModal(card) {
    if (!card || !isCardDual(card)) return;
    delete card.dataset.emprestimoDual;
    card.querySelector('.agro-ns-valor-normal')?.classList.remove('hidden');
    card.querySelector('.agro-ns-valor-dual')?.classList.add('hidden');
    card.querySelector('.agro-ns-valor-lbl-normal')?.classList.remove('hidden');
    card.querySelector('.agro-ns-card-rec')?.classList.remove('hidden');
    const cbRec = card.querySelector('.agro-ns-rec-cb');
    if (cbRec) cbRec.disabled = false;
    card.classList.remove('agro-ns-card--emprestimo-dual');
    card.querySelector('.agro-ns-in-valor-entrada')?.value && (card.querySelector('.agro-ns-in-valor-entrada').value = '');
    card.querySelector('.agro-ns-in-valor-saida')?.value && (card.querySelector('.agro-ns-in-valor-saida').value = '');
    syncHeaderEmprestimoAjuda();
    if (typeof window.__agroNsSyncParc === 'function') window.__agroNsSyncParc(card);
  }

  function onPlanoSelectModal(wrap, nome, id) {
    const card = wrap?.closest('.agro-ns-card');
    if (!card) return;
    if (isDualItem({ id, nome })) {
      ativarModoDualModal(card);
    } else if (isCardDual(card)) {
      desativarModoDualModal(card);
    }
  }

  function coletarLinhaDualModal(card, base) {
    const ve = String(card.querySelector('.agro-ns-in-valor-entrada')?.value || '').trim();
    const vs = String(card.querySelector('.agro-ns-in-valor-saida')?.value || '').trim();
    return {
      ...base,
      emprestimo_dual: true,
      plano_conta: dualLabel(),
      plano_conta_id: dualId(),
      valor_entrada: ve,
      valor_saida: vs,
      recorrente: false,
    };
  }

  function validarLinhaDualModal(ln, num) {
    if (!ln.valor_entrada || !ln.valor_saida) {
      alert(`Lançamento ${num}: informe valor de entrada e valor de saída.`);
      return false;
    }
    if (!ln.empresa_nome || !ln.pessoa_nome) {
      alert(`Lançamento ${num}: preencha loja e pessoa.`);
      return false;
    }
    if (!ln.banco_nome) {
      alert(`Lançamento ${num}: informe conta bancária para a saída / pagamento.`);
      return false;
    }
    const manual = ln.parcelas_manual_saida;
    const quitParc = Array.isArray(manual) && manual.some((p) => p && p.quitado);
    const quitLinha = !!ln.quitado || quitParc;
    if (quitLinha && !ln.banco_id) {
      alert(`Lançamento ${num}: parcela quitada exige conta com ID do ERP na lista.`);
      return false;
    }
    if (!ln.data_competencia || !ln.data_vencimento) {
      alert(`Lançamento ${num}: informe competência e vencimento da saída.`);
      return false;
    }
    return true;
  }

  function resumoDualModal(card) {
    const ve = String(card.querySelector('.agro-ns-in-valor-entrada')?.value || '').trim() || '0,00';
    const vs = String(card.querySelector('.agro-ns-in-valor-saida')?.value || '').trim() || '0,00';
    const pes = card.querySelector('.agro-ns-sug-wrap[data-sug-campo="cliente"] input[type="text"]')?.value?.trim();
    const ven = card.querySelector('.agro-ns-in-ven')?.value;
    const quit = card.querySelector('.agro-ns-in-quitado')?.checked ? ' · Quitado saída' : '';
    const pessoa = pes ? ` · ${pes}` : '';
    const venBr = ven && ven.length >= 10 ? `${ven.slice(8, 10)}/${ven.slice(5, 7)}/${ven.slice(0, 4)}` : '—';
    return `${dualLabel()}${pessoa} · Entr. R$ ${ve} · Saída R$ ${vs} · Venc. ${venBr}${quit}`;
  }

  function ativarModoDualManual(tr) {
    if (!tr || isRowDual(tr)) return;
    tr.dataset.emprestimoDual = '1';
    const planWrap = tr.querySelector('.sug-wrap[data-sug-campo="plano"]');
    const planInp = planWrap?.querySelector('input[type="text"]');
    const planHid = planWrap?.querySelector('input[type="hidden"]');
    if (planInp) planInp.value = dualLabel();
    if (planHid) planHid.value = dualId();

    const tdVal = tr.querySelector('.td-valor-manual');
    if (tdVal) {
      tdVal.innerHTML = `
        <div class="space-y-1">
          <label class="block text-[8px] font-black uppercase text-emerald-700">Entrada (R$)</label>
          <input type="text" class="in-valor-entrada w-full h-9 px-3 rounded-lg border-2 border-emerald-100 text-sm font-black tabular-nums" placeholder="0,00" inputmode="decimal">
          <label class="block text-[8px] font-black uppercase text-amber-700">Saída / pagamento (R$)</label>
          <input type="text" class="in-valor-saida w-full h-9 px-3 rounded-lg border-2 border-amber-100 text-sm font-black tabular-nums" placeholder="0,00" inputmode="decimal" oninput="calcTotal()">
        </div>`;
      tdVal.querySelector('.in-valor-saida')?.addEventListener('input', () => {
        if (typeof global.calcTotal === 'function') global.calcTotal();
      });
      tdVal.querySelector('.in-valor-entrada')?.addEventListener('input', () => {
        if (typeof global.calcTotal === 'function') global.calcTotal();
      });
    }
    tr.classList.add('linha--emprestimo-dual');
  }

  function desativarModoDualManual(tr) {
    if (!tr || !isRowDual(tr)) return;
    delete tr.dataset.emprestimoDual;
    const tdVal = tr.querySelector('.td-valor-manual');
    if (tdVal) {
      tdVal.innerHTML = '<input type="text" class="in-valor w-full h-10 px-4 rounded-lg border-2 border-slate-100 text-sm font-black tabular-nums text-slate-700" placeholder="0,00" oninput="calcTotal()">';
    }
    tr.classList.remove('linha--emprestimo-dual');
  }

  function onPlanoSelectManual(wrap, nome, id) {
    const tr = wrap?.closest('tr.linha');
    if (!tr) return;
    if (isDualItem({ id, nome })) {
      ativarModoDualManual(tr);
    } else if (isRowDual(tr)) {
      desativarModoDualManual(tr);
    }
  }

  function coletarLinhaDualManual(tr, baseHeader) {
    const ve = String(tr.querySelector('.in-valor-entrada')?.value || '').trim();
    const vs = String(tr.querySelector('.in-valor-saida')?.value || '').trim();
    const dEl = tr.querySelector('.in-desc');
    const descricao = dEl ? dEl.value.trim() : '';
    return {
      emprestimo_dual: true,
      plano_conta: dualLabel(),
      plano_conta_id: dualId(),
      valor_entrada: ve,
      valor_saida: vs,
      descricao: descricao || undefined,
      empresa_nome: baseHeader.empresa_nome,
      empresa_id: baseHeader.empresa_id,
      pessoa_nome: baseHeader.pessoa_nome,
      pessoa_id: baseHeader.pessoa_id,
      banco_nome: baseHeader.banco_nome,
      banco_id: baseHeader.banco_id,
      forma_nome: baseHeader.forma_nome,
      forma_id: baseHeader.forma_id,
      data_competencia: baseHeader.data_competencia,
      data_vencimento: baseHeader.data_vencimento,
      quitado: baseHeader.quitado,
    };
  }

  global.AgroLancEmprestimoDual = {
    cfg,
    dualId,
    dualLabel,
    isDualItem,
    injectSuggestItems,
    isCardDual,
    isRowDual,
    onPlanoSelectModal,
    onPlanoSelectManual,
    coletarLinhaDualModal,
    coletarLinhaDualManual,
    validarLinhaDualModal,
    resumoDualModal,
    ativarModoDualModal,
    desativarModoDualModal,
    syncHeaderEmprestimoAjuda,
  };
})(typeof window !== 'undefined' ? window : globalThis);
