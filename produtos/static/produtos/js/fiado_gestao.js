(function () {
  'use strict';

  const CFG = window.AGRO_FIADO || {};
  const urls = CFG.urls || {};

  const el = {
    busca: document.getElementById('fiado-busca'),
    btnAtualizar: document.getElementById('fiado-btn-atualizar'),
    tbody: document.getElementById('fiado-tbody-clientes'),
    kpiTotal: document.getElementById('fiado-kpi-total'),
    kpiClientes: document.getElementById('fiado-kpi-clientes'),
    modalBaixa: document.getElementById('fiado-modal-baixa'),
    formBaixa: document.getElementById('fiado-form-baixa'),
    baixaResumo: document.getElementById('fiado-baixa-resumo'),
    baixaValor: document.getElementById('fiado-baixa-valor'),
    baixaForma: document.getElementById('fiado-baixa-forma'),
    baixaObs: document.getElementById('fiado-baixa-obs'),
    baixaCancelar: document.getElementById('fiado-baixa-cancelar'),
    modalLimite: document.getElementById('fiado-modal-limite'),
    btnLimiteAvulso: document.getElementById('fiado-btn-limite-avulso'),
    limiteBusca: document.getElementById('fiado-limite-busca'),
    limiteResultados: document.getElementById('fiado-limite-resultados'),
    limiteAvulsoValor: document.getElementById('fiado-limite-avulso-valor'),
    formLimiteAvulso: document.getElementById('fiado-form-limite-avulso'),
    limiteFechar: document.getElementById('fiado-limite-fechar'),
  };

  let baixaCliente = null;
  let limiteAvulsoPk = null;
  let debounceTimer = null;
  let clientesCache = [];

  function csrfToken() {
    const m = document.cookie.match(/csrftoken=([^;]+)/);
    return m ? decodeURIComponent(m[1]) : '';
  }

  function fmtMoeda(v) {
    const n = Number(v) || 0;
    return 'R$ ' + n.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }

  function esc(s) {
    return String(s || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  async function fetchJson(url, opts) {
    const r = await fetch(url, opts);
    const j = await r.json().catch(function () { return {}; });
    if (!r.ok || j.ok === false) {
      throw new Error(j.erro || j.mensagem || 'Falha na requisição.');
    }
    return j;
  }

  function situacaoClass(s) {
    if (s === 'vencido') return 'text-red-700 bg-red-50';
    if (s === 'parcial') return 'text-amber-800 bg-amber-50';
    return 'text-emerald-800 bg-emerald-50';
  }

  function atualizarKpis(resumo) {
    if (!resumo) return;
    if (el.kpiTotal) el.kpiTotal.textContent = fmtMoeda(resumo.total_saldo_aberto);
    if (el.kpiClientes) el.kpiClientes.textContent = String(resumo.clientes_com_saldo || 0);
  }

  function renderClientes(clientes) {
    clientesCache = clientes || [];
    if (!el.tbody) return;
    if (!clientesCache.length) {
      el.tbody.innerHTML = '<tr><td colspan="9" class="px-4 py-10 text-center text-sm font-bold text-slate-500">Nenhum cliente com saldo em aberto.</td></tr>';
      return;
    }
    el.tbody.innerHTML = clientesCache.map(function (c) {
      const pk = c.cliente_agro_pk;
      const destaque = CFG.clientePrePk && pk === CFG.clientePrePk ? ' bg-orange-50 ring-2 ring-inset ring-orange-300' : '';
      return (
        '<tr class="border-t border-slate-100' + destaque + '" data-pk="' + esc(pk || '') + '">' +
        '<td class="font-black text-slate-900 max-w-[14rem] truncate" title="' + esc(c.cliente_nome) + '">' + esc(c.cliente_nome) + '</td>' +
        '<td class="text-xs font-bold text-slate-500 tabular-nums">' + esc(c.cliente_codigo || '—') + '</td>' +
        '<td class="text-center font-bold tabular-nums">' + (c.titulos_abertos || 0) + '</td>' +
        '<td class="font-bold whitespace-nowrap">' + esc(c.vencimento_mais_antigo_texto || '—') + '</td>' +
        '<td class="text-right tabular-nums font-semibold">' + fmtMoeda(c.valor_bruto) + '</td>' +
        '<td class="text-right tabular-nums text-slate-600">' + fmtMoeda(c.valor_pago) + '</td>' +
        '<td class="text-right tabular-nums font-black text-orange-800 text-base">' + fmtMoeda(c.saldo_aberto) + '</td>' +
        '<td><span class="inline-block rounded-lg px-2 py-0.5 text-[10px] font-black uppercase ' + situacaoClass(c.situacao_resumo) + '">' + esc(c.situacao_label) + '</span></td>' +
        '<td class="text-right whitespace-nowrap">' +
        '<button type="button" class="fiado-btn-baixa min-h-[40px] px-3 rounded-xl bg-orange-600 text-white text-[10px] font-black uppercase" data-pk="' + esc(pk || '') + '" data-nome="' + esc(c.cliente_nome) + '" data-codigo="' + esc(c.cliente_codigo || '') + '" data-saldo="' + c.saldo_aberto + '">Baixa</button>' +
        (pk ? ' <button type="button" class="fiado-btn-limite min-h-[40px] px-2 rounded-xl border border-emerald-200 bg-emerald-50 text-emerald-900 text-[10px] font-black uppercase" data-pk="' + pk + '" data-limite="' + (c.limite_fiado_local || c.limite || 0) + '" data-nome="' + esc(c.cliente_nome) + '">Limite</button>' : '') +
        '</td></tr>'
      );
    }).join('');
  }

  async function recarregar() {
    const q = el.busca ? el.busca.value.trim() : '';
    const qs = new URLSearchParams({ q: q, apenas_saldo: '1' });
    try {
      if (window.gmLoadingBar) window.gmLoadingBar.show();
      const [cli, res] = await Promise.all([
        fetchJson(urls.clientes + '?' + qs.toString()),
        fetchJson(urls.resumo),
      ]);
      renderClientes(cli.clientes || []);
      atualizarKpis(res);
    } catch (e) {
      alert(e.message || String(e));
    } finally {
      if (window.gmLoadingBar) window.gmLoadingBar.hide();
    }
  }

  function abrirBaixa(c) {
    baixaCliente = c;
    if (el.baixaResumo) {
      el.baixaResumo.textContent = (c.nome || '') + ' — saldo ' + fmtMoeda(c.saldo);
    }
    if (el.baixaValor) el.baixaValor.value = Number(c.saldo || 0).toFixed(2).replace('.', ',');
    if (el.baixaObs) el.baixaObs.value = '';
    if (el.modalBaixa && el.modalBaixa.showModal) el.modalBaixa.showModal();
  }

  async function confirmarBaixa(ev) {
    ev.preventDefault();
    if (!baixaCliente) return;
    const body = {
      cliente_agro_pk: baixaCliente.pk || null,
      cliente_nome: baixaCliente.nome || '',
      cliente_codigo: baixaCliente.codigo || '',
      valor: el.baixaValor ? el.baixaValor.value : '',
      forma_pagamento: el.baixaForma ? el.baixaForma.value : 'Dinheiro',
      observacao: el.baixaObs ? el.baixaObs.value : '',
      registrar_caixa: !!CFG.caixaAberto,
    };
    try {
      if (window.gmLoadingBar) window.gmLoadingBar.show();
      await fetchJson(urls.baixaCliente, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrfToken() },
        body: JSON.stringify(body),
      });
      if (el.modalBaixa && el.modalBaixa.close) el.modalBaixa.close();
      baixaCliente = null;
      await recarregar();
    } catch (e) {
      alert(e.message || String(e));
    } finally {
      if (window.gmLoadingBar) window.gmLoadingBar.hide();
    }
  }

  async function salvarLimite(pk, valor) {
    await fetchJson(urls.limite, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrfToken() },
      body: JSON.stringify({ cliente_agro_pk: pk, limite: valor }),
    });
  }

  async function buscarClientesLimite(q) {
    if (!el.limiteResultados) return;
    if ((q || '').length < 2) {
      el.limiteResultados.innerHTML = '';
      return;
    }
    const j = await fetchJson(urls.buscarCliente + '?q=' + encodeURIComponent(q));
    el.limiteResultados.innerHTML = (j.clientes || []).map(function (c) {
      return (
        '<button type="button" class="w-full text-left px-3 py-2 hover:bg-emerald-50 fiado-pick-cliente" data-pk="' + c.pk + '" data-limite="' + (c.limite_fiado_local || 0) + '">' +
        '<span class="font-black text-sm">' + esc(c.nome) + '</span></button>'
      );
    }).join('') || '<p class="p-2 text-xs text-slate-500">Nenhum cliente.</p>';
  }

  if (el.tbody) {
    el.tbody.addEventListener('click', function (ev) {
      const bBaixa = ev.target.closest('.fiado-btn-baixa');
      if (bBaixa) {
        abrirBaixa({
          pk: bBaixa.getAttribute('data-pk') ? parseInt(bBaixa.getAttribute('data-pk'), 10) : null,
          nome: bBaixa.getAttribute('data-nome') || '',
          codigo: bBaixa.getAttribute('data-codigo') || '',
          saldo: parseFloat(bBaixa.getAttribute('data-saldo') || '0'),
        });
        return;
      }
      const bLim = ev.target.closest('.fiado-btn-limite');
      if (bLim && el.modalLimite) {
        limiteAvulsoPk = parseInt(bLim.getAttribute('data-pk'), 10);
        if (el.limiteAvulsoValor) {
          el.limiteAvulsoValor.value = parseFloat(bLim.getAttribute('data-limite') || '0').toFixed(2).replace('.', ',');
        }
        if (el.limiteBusca) el.limiteBusca.value = bLim.getAttribute('data-nome') || '';
        el.modalLimite.showModal();
      }
    });
  }

  if (el.formBaixa) el.formBaixa.addEventListener('submit', confirmarBaixa);
  if (el.baixaCancelar && el.modalBaixa) {
    el.baixaCancelar.addEventListener('click', function () { el.modalBaixa.close(); });
  }
  if (el.btnAtualizar) el.btnAtualizar.addEventListener('click', recarregar);
  if (el.busca) {
    el.busca.addEventListener('input', function () {
      clearTimeout(debounceTimer);
      debounceTimer = setTimeout(recarregar, 280);
    });
  }

  if (el.btnLimiteAvulso && el.modalLimite) {
    el.btnLimiteAvulso.addEventListener('click', function () {
      limiteAvulsoPk = null;
      if (el.limiteBusca) el.limiteBusca.value = '';
      if (el.limiteAvulsoValor) el.limiteAvulsoValor.value = '';
      if (el.limiteResultados) el.limiteResultados.innerHTML = '';
      el.modalLimite.showModal();
    });
  }
  if (el.limiteFechar && el.modalLimite) {
    el.limiteFechar.addEventListener('click', function () { el.modalLimite.close(); });
  }
  if (el.limiteBusca) {
    el.limiteBusca.addEventListener('input', function () {
      clearTimeout(debounceTimer);
      debounceTimer = setTimeout(function () {
        buscarClientesLimite(el.limiteBusca.value).catch(function () {});
      }, 300);
    });
  }
  if (el.limiteResultados) {
    el.limiteResultados.addEventListener('click', function (ev) {
      const b = ev.target.closest('.fiado-pick-cliente');
      if (!b) return;
      limiteAvulsoPk = parseInt(b.getAttribute('data-pk'), 10);
      if (el.limiteAvulsoValor) {
        el.limiteAvulsoValor.value = parseFloat(b.getAttribute('data-limite') || '0').toFixed(2).replace('.', ',');
      }
    });
  }
  if (el.formLimiteAvulso) {
    el.formLimiteAvulso.addEventListener('submit', async function (ev) {
      ev.preventDefault();
      if (!limiteAvulsoPk) {
        alert('Selecione um cliente.');
        return;
      }
      try {
        await salvarLimite(limiteAvulsoPk, el.limiteAvulsoValor ? el.limiteAvulsoValor.value : '0');
        el.modalLimite.close();
        await recarregar();
      } catch (e) {
        alert(e.message || String(e));
      }
    });
  }

  document.addEventListener('keydown', function (ev) {
    if (ev.key === '/' && document.activeElement !== el.busca) {
      const t = document.activeElement && document.activeElement.tagName;
      if (t !== 'INPUT' && t !== 'TEXTAREA' && t !== 'SELECT') {
        ev.preventDefault();
        el.busca && el.busca.focus();
      }
    }
  });

  recarregar();
})();
