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
    modalCliente: document.getElementById('fiado-modal-cliente'),
    cliModalNome: document.getElementById('fiado-cli-modal-nome'),
    cliModalMeta: document.getElementById('fiado-cli-modal-meta'),
    cliModalSaldo: document.getElementById('fiado-cli-modal-saldo'),
    cliModalFechar: document.getElementById('fiado-cli-modal-fechar'),
    tbodyTitulos: document.getElementById('fiado-tbody-titulos'),
    titSelTodos: document.getElementById('fiado-tit-sel-todos'),
    titSelInfo: document.getElementById('fiado-tit-sel-info'),
    btnBaixaSel: document.getElementById('fiado-btn-baixa-sel'),
    btnBaixaTotalCli: document.getElementById('fiado-btn-baixa-total-cli'),
    btnAtualizarTitulos: document.getElementById('fiado-btn-atualizar-titulos'),
    modalBaixa: document.getElementById('fiado-modal-baixa'),
    formBaixa: document.getElementById('fiado-form-baixa'),
    baixaTitulo: document.getElementById('fiado-baixa-titulo'),
    baixaResumo: document.getElementById('fiado-baixa-resumo'),
    baixaDica: document.getElementById('fiado-baixa-dica'),
    baixaValor: document.getElementById('fiado-baixa-valor'),
    baixaForma: document.getElementById('fiado-baixa-forma'),
    baixaObs: document.getElementById('fiado-baixa-obs'),
    baixaCancelar: document.getElementById('fiado-baixa-cancelar'),
    modalEditar: document.getElementById('fiado-modal-editar'),
    formEditar: document.getElementById('fiado-form-editar'),
    editarResumo: document.getElementById('fiado-editar-resumo'),
    editarDoc: document.getElementById('fiado-editar-doc'),
    editarVenc: document.getElementById('fiado-editar-venc'),
    editarValor: document.getElementById('fiado-editar-valor'),
    editarDesc: document.getElementById('fiado-editar-desc'),
    editarCancelar: document.getElementById('fiado-editar-cancelar'),
    modalLimite: document.getElementById('fiado-modal-limite'),
    btnLimiteAvulso: document.getElementById('fiado-btn-limite-avulso'),
    limiteBusca: document.getElementById('fiado-limite-busca'),
    limiteResultados: document.getElementById('fiado-limite-resultados'),
    limiteAvulsoValor: document.getElementById('fiado-limite-avulso-valor'),
    formLimiteAvulso: document.getElementById('fiado-form-limite-avulso'),
    limiteFechar: document.getElementById('fiado-limite-fechar'),
    emptyBanner: document.getElementById('fiado-empty-banner'),
    formImportar: document.getElementById('fiado-form-importar'),
    importArquivo: document.getElementById('fiado-import-arquivo'),
    btnImportar: document.getElementById('fiado-btn-importar'),
    importMsg: document.getElementById('fiado-import-msg'),
  };

  let baixaCtx = null;
  let editarTituloId = null;
  let limiteAvulsoPk = null;
  let debounceTimer = null;
  let clientesCache = [];
  let clienteModal = null;
  let titulosCache = [];
  let selecionados = new Set();

  function setModalBodyLock(on) {
    document.body.classList.toggle('fiado-modal-aberto', !!on);
  }

  function modalClienteAberto() {
    return !!(el.modalCliente && el.modalCliente.open);
  }

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

  function isoToInputDate(iso) {
    if (!iso) return '';
    return String(iso).slice(0, 10);
  }

  function tituloVencido(t) {
    const iso = isoToInputDate(t && t.vencimento);
    if (!iso) return false;
    const hoje = new Date();
    hoje.setHours(0, 0, 0, 0);
    const venc = new Date(iso + 'T00:00:00');
    return venc < hoje && (Number(t.saldo_aberto) || 0) > 0;
  }

  function marcarClienteAtivo(cli) {
    if (!el.tbody || !cli) return;
    el.tbody.querySelectorAll('.fiado-cli-row').forEach(function (row) {
      const pk = row.getAttribute('data-pk');
      const nome = row.getAttribute('data-nome') || '';
      const cod = row.getAttribute('data-codigo') || '';
      const matchPk = cli.pk && pk && parseInt(pk, 10) === cli.pk;
      const matchNome = !cli.pk && nome === cli.nome && cod === (cli.codigo || '');
      row.classList.toggle('fiado-cli-row-ativo', !!(matchPk || matchNome));
    });
  }

  function limparClienteAtivo() {
    if (!el.tbody) return;
    el.tbody.querySelectorAll('.fiado-cli-row-ativo').forEach(function (row) {
      row.classList.remove('fiado-cli-row-ativo');
    });
  }

  function sincronizarLinhasSelecionadas() {
    if (!el.tbodyTitulos) return;
    el.tbodyTitulos.querySelectorAll('tr[data-id]').forEach(function (row) {
      const id = parseInt(row.getAttribute('data-id'), 10);
      row.classList.toggle('fiado-tit-sel', selecionados.has(id));
      const chk = row.querySelector('.fiado-tit-chk');
      if (chk) chk.checked = selecionados.has(id);
    });
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
    if (s === 'quitado') return 'text-slate-600 bg-slate-100';
    if (s === 'cancelado') return 'text-slate-500 bg-slate-50';
    return 'text-emerald-800 bg-emerald-50';
  }

  function situacaoTituloClass(s) {
    if (s === 'parcial') return 'text-amber-800 bg-amber-50';
    if (s === 'quitado') return 'text-slate-600 bg-slate-100';
    if (s === 'cancelado') return 'text-slate-500 bg-slate-50';
    return 'text-emerald-800 bg-emerald-50';
  }

  function atualizarKpis(resumo) {
    if (!resumo) return;
    if (el.kpiTotal) el.kpiTotal.textContent = fmtMoeda(resumo.total_saldo_aberto);
    if (el.kpiClientes) el.kpiClientes.textContent = String(resumo.clientes_com_saldo || 0);
  }

  function atualizarEmptyBanner(resumo) {
    if (!el.emptyBanner || !resumo) return;
    const vazio = (resumo.titulos_abertos || 0) === 0;
    el.emptyBanner.classList.toggle('hidden', !vazio);
  }

  function mostrarImportMsg(texto, ok) {
    if (!el.importMsg) return;
    el.importMsg.textContent = texto || '';
    el.importMsg.classList.remove('hidden', 'text-emerald-900', 'text-red-800');
    el.importMsg.classList.add(ok ? 'text-emerald-900' : 'text-red-800');
  }

  function clienteFromRow(c) {
    return {
      pk: c.cliente_agro_pk || null,
      nome: c.cliente_nome || '',
      codigo: c.cliente_codigo || '',
      saldo: c.saldo_aberto || 0,
      titulos: c.titulos_abertos || 0,
    };
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
      const destaque = CFG.clientePrePk && pk === CFG.clientePrePk ? ' ring-2 ring-inset ring-orange-300 bg-orange-50' : '';
      return (
        '<tr class="fiado-cli-row border-t border-slate-100' + destaque + '" data-pk="' + esc(pk || '') + '" data-nome="' + esc(c.cliente_nome) + '" data-codigo="' + esc(c.cliente_codigo || '') + '" data-saldo="' + c.saldo_aberto + '" data-titulos="' + (c.titulos_abertos || 0) + '">' +
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

  function titulosQueryParams(cli) {
    const qs = new URLSearchParams({ situacao: 'abertos', limit: '500' });
    if (cli.pk) qs.set('cliente_agro_pk', String(cli.pk));
    else {
      qs.set('cliente_nome', cli.nome || '');
      if (cli.codigo) qs.set('cliente_codigo', cli.codigo);
    }
    return qs.toString();
  }

  function saldoTitulos(list) {
    return (list || []).reduce(function (acc, t) { return acc + (Number(t.saldo_aberto) || 0); }, 0);
  }

  function atualizarSelecaoUi() {
    const n = selecionados.size;
    let saldoSel = 0;
    titulosCache.forEach(function (t) {
      if (selecionados.has(t.id)) saldoSel += Number(t.saldo_aberto) || 0;
    });
    if (el.titSelInfo) {
      el.titSelInfo.textContent = n
        ? n + ' selecionado(s) · ' + fmtMoeda(saldoSel)
        : '0 selecionados';
    }
    if (el.btnBaixaSel) el.btnBaixaSel.disabled = n === 0;
    if (el.titSelTodos) {
      const abertos = titulosCache.filter(function (t) { return t.saldo_aberto > 0; });
      el.titSelTodos.checked = abertos.length > 0 && abertos.every(function (t) { return selecionados.has(t.id); });
      el.titSelTodos.indeterminate = n > 0 && !el.titSelTodos.checked;
    }
  }

  function renderTitulos(titulos) {
    titulosCache = titulos || [];
    selecionados.clear();
    if (!el.tbodyTitulos) return;
    if (!titulosCache.length) {
      el.tbodyTitulos.innerHTML = '<tr><td colspan="9" class="px-4 py-10 text-center text-sm font-bold text-slate-500">Nenhum lançamento em aberto.</td></tr>';
      atualizarSelecaoUi();
      return;
    }
    el.tbodyTitulos.innerHTML = titulosCache.map(function (t) {
      const parcela = t.parcela_total > 1 ? (t.parcela_num + '/' + t.parcela_total) : '—';
      return (
        '<tr class="border-t border-slate-100" data-id="' + t.id + '">' +
        '<td><input type="checkbox" class="fiado-tit-chk h-4 w-4 rounded border-slate-300" data-id="' + t.id + '" aria-label="Selecionar"></td>' +
        '<td class="font-bold text-slate-900 max-w-[10rem] truncate" title="' + esc(t.numero_documento) + '">' + esc(t.numero_documento || '—') + '</td>' +
        '<td class="tabular-nums font-semibold">' + parcela + '</td>' +
        '<td class="font-bold whitespace-nowrap">' + esc(t.vencimento_texto || '—') + '</td>' +
        '<td class="text-right tabular-nums">' + fmtMoeda(t.valor_bruto) + '</td>' +
        '<td class="text-right tabular-nums text-slate-600">' + fmtMoeda(t.valor_pago) + '</td>' +
        '<td class="text-right tabular-nums font-black text-orange-800">' + fmtMoeda(t.saldo_aberto) + '</td>' +
        '<td><span class="inline-block rounded-lg px-2 py-0.5 text-[10px] font-black uppercase ' + situacaoTituloClass(t.situacao) + '">' + esc(t.situacao_label) + '</span></td>' +
        '<td class="text-right whitespace-nowrap">' +
        '<button type="button" class="fiado-btn-baixa-tit min-h-[38px] px-2.5 rounded-xl bg-orange-600 text-white text-[10px] font-black uppercase" data-id="' + t.id + '" data-saldo="' + t.saldo_aberto + '" data-doc="' + esc(t.numero_documento || '') + '">Baixa</button> ' +
        '<button type="button" class="fiado-btn-editar-tit min-h-[38px] px-2.5 rounded-xl border border-emerald-200 bg-emerald-50 text-emerald-900 text-[10px] font-black uppercase" data-id="' + t.id + '">Editar</button>' +
        '</td></tr>'
      );
    }).join('');
    atualizarSelecaoUi();
  }

  async function carregarTitulosCliente(cli) {
    if (!cli) return;
    clienteModal = cli;
    if (el.cliModalNome) el.cliModalNome.textContent = cli.nome || '—';
    if (el.cliModalMeta) {
      el.cliModalMeta.textContent = (cli.codigo ? 'Cód. ' + cli.codigo + ' · ' : '') + (cli.titulos || 0) + ' título(s)';
    }
    try {
      if (window.gmLoadingBar) window.gmLoadingBar.show();
      const j = await fetchJson(urls.titulos + '?' + titulosQueryParams(cli));
      renderTitulos(j.titulos || []);
      const saldo = saldoTitulos(j.titulos);
      if (el.cliModalSaldo) el.cliModalSaldo.textContent = fmtMoeda(saldo);
      clienteModal.saldo = saldo;
    } catch (e) {
      alert(e.message || String(e));
    } finally {
      if (window.gmLoadingBar) window.gmLoadingBar.hide();
    }
  }

  function abrirModalCliente(cli) {
    if (!el.modalCliente) return;
    carregarTitulosCliente(cli).then(function () {
      if (el.modalCliente.showModal) {
        el.modalCliente.showModal();
        setModalBodyLock(true);
      }
    });
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
      atualizarEmptyBanner(res);
      if (clienteModal && el.modalCliente && el.modalCliente.open) {
        const atual = (cli.clientes || []).find(function (c) {
          if (clienteModal.pk && c.cliente_agro_pk === clienteModal.pk) return true;
          return c.cliente_nome === clienteModal.nome && String(c.cliente_codigo || '') === String(clienteModal.codigo || '');
        });
        if (atual) {
          clienteModal = clienteFromRow(atual);
          await carregarTitulosCliente(clienteModal);
        } else {
          el.modalCliente.close();
          setModalBodyLock(false);
          clienteModal = null;
        }
      }
    } catch (e) {
      alert(e.message || String(e));
    } finally {
      if (window.gmLoadingBar) window.gmLoadingBar.hide();
    }
  }

  function abrirBaixa(ctx) {
    baixaCtx = ctx;
    const modo = ctx.modo || 'cliente';
    if (el.baixaTitulo) {
      el.baixaTitulo.textContent = modo === 'titulo' ? 'Baixa do lançamento' : modo === 'selecionados' ? 'Baixa selecionados' : 'Baixa de fiado';
    }
    if (el.baixaResumo) {
      if (modo === 'titulo') {
        el.baixaResumo.textContent = (ctx.doc || 'Lançamento') + ' — saldo ' + fmtMoeda(ctx.saldo);
      } else if (modo === 'selecionados') {
        el.baixaResumo.textContent = (ctx.nome || '') + ' — ' + (ctx.ids || []).length + ' título(s) · ' + fmtMoeda(ctx.saldo);
      } else {
        el.baixaResumo.textContent = (ctx.nome || '') + ' — saldo ' + fmtMoeda(ctx.saldo);
      }
    }
    if (el.baixaDica) {
      if (modo === 'titulo') {
        el.baixaDica.textContent = 'Informe o valor recebido (parcial ou total). Com caixa aberto, entra como reforço no turno.';
      } else if (modo === 'selecionados') {
        el.baixaDica.textContent = 'O valor é aplicado nos títulos selecionados (vencimento mais antigo primeiro). Um único reforço no caixa.';
      } else {
        el.baixaDica.textContent = 'O valor quita os títulos mais antigos primeiro. Com caixa aberto, entra como reforço no turno.';
      }
    }
    if (el.baixaValor) el.baixaValor.value = Number(ctx.saldo || 0).toFixed(2).replace('.', ',');
    if (el.baixaObs) el.baixaObs.value = '';
    if (el.modalBaixa && el.modalBaixa.showModal) el.modalBaixa.showModal();
  }

  async function confirmarBaixa(ev) {
    ev.preventDefault();
    if (!baixaCtx) return;
    const valor = el.baixaValor ? el.baixaValor.value : '';
    const forma = el.baixaForma ? el.baixaForma.value : 'Dinheiro';
    const obs = el.baixaObs ? el.baixaObs.value : '';
    const registrarCaixa = !!CFG.caixaAberto;
    try {
      if (window.gmLoadingBar) window.gmLoadingBar.show();
      const modo = baixaCtx.modo || 'cliente';
      if (modo === 'titulo') {
        await fetchJson(urls.baixa, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrfToken() },
          body: JSON.stringify({
            titulo_id: baixaCtx.tituloId,
            valor: valor,
            forma_pagamento: forma,
            observacao: obs,
            registrar_caixa: registrarCaixa,
          }),
        });
      } else if (modo === 'selecionados') {
        await fetchJson(urls.baixaSelecionados, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrfToken() },
          body: JSON.stringify({
            titulo_ids: baixaCtx.ids || [],
            valor: valor,
            forma_pagamento: forma,
            observacao: obs,
            registrar_caixa: registrarCaixa,
          }),
        });
      } else {
        await fetchJson(urls.baixaCliente, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrfToken() },
          body: JSON.stringify({
            cliente_agro_pk: baixaCtx.pk || null,
            cliente_nome: baixaCtx.nome || '',
            cliente_codigo: baixaCtx.codigo || '',
            valor: valor,
            forma_pagamento: forma,
            observacao: obs,
            registrar_caixa: registrarCaixa,
          }),
        });
      }
      if (el.modalBaixa && el.modalBaixa.close) el.modalBaixa.close();
      baixaCtx = null;
      await recarregar();
    } catch (e) {
      alert(e.message || String(e));
    } finally {
      if (window.gmLoadingBar) window.gmLoadingBar.hide();
    }
  }

  function abrirEditar(t) {
    editarTituloId = t.id;
    if (el.editarResumo) {
      el.editarResumo.textContent = (t.numero_documento || 'Lançamento #' + t.id) + ' · saldo ' + fmtMoeda(t.saldo_aberto);
    }
    if (el.editarDoc) el.editarDoc.value = t.numero_documento || '';
    if (el.editarVenc) el.editarVenc.value = isoToInputDate(t.vencimento);
    if (el.editarValor) el.editarValor.value = Number(t.valor_bruto || 0).toFixed(2).replace('.', ',');
    if (el.editarDesc) el.editarDesc.value = t.descricao || '';
    if (el.modalEditar && el.modalEditar.showModal) el.modalEditar.showModal();
  }

  async function confirmarEditar(ev) {
    ev.preventDefault();
    if (!editarTituloId) return;
    try {
      if (window.gmLoadingBar) window.gmLoadingBar.show();
      await fetchJson(urls.tituloEditar, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrfToken() },
        body: JSON.stringify({
          titulo_id: editarTituloId,
          numero_documento: el.editarDoc ? el.editarDoc.value : '',
          vencimento: el.editarVenc ? el.editarVenc.value : '',
          valor_bruto: el.editarValor ? el.editarValor.value : '',
          descricao: el.editarDesc ? el.editarDesc.value : '',
        }),
      });
      if (el.modalEditar && el.modalEditar.close) el.modalEditar.close();
      editarTituloId = null;
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
        ev.stopPropagation();
        abrirBaixa({
          modo: 'cliente',
          pk: bBaixa.getAttribute('data-pk') ? parseInt(bBaixa.getAttribute('data-pk'), 10) : null,
          nome: bBaixa.getAttribute('data-nome') || '',
          codigo: bBaixa.getAttribute('data-codigo') || '',
          saldo: parseFloat(bBaixa.getAttribute('data-saldo') || '0'),
        });
        return;
      }
      const bLim = ev.target.closest('.fiado-btn-limite');
      if (bLim) {
        ev.stopPropagation();
        if (el.modalLimite) {
          limiteAvulsoPk = parseInt(bLim.getAttribute('data-pk'), 10);
          if (el.limiteAvulsoValor) {
            el.limiteAvulsoValor.value = parseFloat(bLim.getAttribute('data-limite') || '0').toFixed(2).replace('.', ',');
          }
          if (el.limiteBusca) el.limiteBusca.value = bLim.getAttribute('data-nome') || '';
          el.modalLimite.showModal();
        }
        return;
      }
      const row = ev.target.closest('.fiado-cli-row');
      if (!row) return;
      abrirModalCliente({
        pk: row.getAttribute('data-pk') ? parseInt(row.getAttribute('data-pk'), 10) : null,
        nome: row.getAttribute('data-nome') || '',
        codigo: row.getAttribute('data-codigo') || '',
        saldo: parseFloat(row.getAttribute('data-saldo') || '0'),
        titulos: parseInt(row.getAttribute('data-titulos') || '0', 10),
      });
    });
  }

  if (el.tbodyTitulos) {
    el.tbodyTitulos.addEventListener('click', function (ev) {
      const chk = ev.target.closest('.fiado-tit-chk');
      if (chk) {
        const id = parseInt(chk.getAttribute('data-id'), 10);
        if (chk.checked) selecionados.add(id);
        else selecionados.delete(id);
        atualizarSelecaoUi();
        return;
      }
      const bBaixa = ev.target.closest('.fiado-btn-baixa-tit');
      if (bBaixa) {
        abrirBaixa({
          modo: 'titulo',
          tituloId: parseInt(bBaixa.getAttribute('data-id'), 10),
          doc: bBaixa.getAttribute('data-doc') || '',
          saldo: parseFloat(bBaixa.getAttribute('data-saldo') || '0'),
        });
        return;
      }
      const bEdit = ev.target.closest('.fiado-btn-editar-tit');
      if (bEdit) {
        const id = parseInt(bEdit.getAttribute('data-id'), 10);
        const t = titulosCache.find(function (x) { return x.id === id; });
        if (t) abrirEditar(t);
      }
    });
  }

  if (el.titSelTodos) {
    el.titSelTodos.addEventListener('change', function () {
      selecionados.clear();
      if (el.titSelTodos.checked) {
        titulosCache.forEach(function (t) {
          if (t.saldo_aberto > 0) selecionados.add(t.id);
        });
      }
      if (el.tbodyTitulos) {
        el.tbodyTitulos.querySelectorAll('.fiado-tit-chk').forEach(function (chk) {
          const id = parseInt(chk.getAttribute('data-id'), 10);
          chk.checked = selecionados.has(id);
        });
      }
      atualizarSelecaoUi();
    });
  }

  if (el.btnBaixaSel) {
    el.btnBaixaSel.addEventListener('click', function () {
      if (!clienteModal || selecionados.size === 0) return;
      const ids = Array.from(selecionados);
      let saldo = 0;
      titulosCache.forEach(function (t) {
        if (selecionados.has(t.id)) saldo += Number(t.saldo_aberto) || 0;
      });
      abrirBaixa({
        modo: 'selecionados',
        ids: ids,
        nome: clienteModal.nome,
        saldo: saldo,
      });
    });
  }

  if (el.btnBaixaTotalCli && clienteModal !== undefined) {
    el.btnBaixaTotalCli.addEventListener('click', function () {
      if (!clienteModal) return;
      abrirBaixa({
        modo: 'cliente',
        pk: clienteModal.pk,
        nome: clienteModal.nome,
        codigo: clienteModal.codigo,
        saldo: clienteModal.saldo,
      });
    });
  }

  if (el.btnAtualizarTitulos) {
    el.btnAtualizarTitulos.addEventListener('click', function () {
      if (clienteModal) carregarTitulosCliente(clienteModal);
    });
  }

  if (el.cliModalFechar && el.modalCliente) {
    el.cliModalFechar.addEventListener('click', function () {
      el.modalCliente.close();
      setModalBodyLock(false);
    });
  }
  if (el.modalCliente) {
    el.modalCliente.addEventListener('close', function () {
      if (!modalClienteAberto()) setModalBodyLock(false);
    });
    el.modalCliente.addEventListener('cancel', function (ev) {
      ev.preventDefault();
      el.modalCliente.close();
      setModalBodyLock(false);
    });
  }

  if (el.formBaixa) el.formBaixa.addEventListener('submit', confirmarBaixa);
  if (el.baixaCancelar && el.modalBaixa) {
    el.baixaCancelar.addEventListener('click', function () { el.modalBaixa.close(); });
  }
  if (el.formEditar) el.formEditar.addEventListener('submit', confirmarEditar);
  if (el.editarCancelar && el.modalEditar) {
    el.editarCancelar.addEventListener('click', function () { el.modalEditar.close(); });
  }
  if (el.btnAtualizar) el.btnAtualizar.addEventListener('click', recarregar);

  if (el.formImportar && urls.importar) {
    el.formImportar.addEventListener('submit', async function (ev) {
      ev.preventDefault();
      const arquivo = el.importArquivo && el.importArquivo.files && el.importArquivo.files[0];
      if (!arquivo) {
        mostrarImportMsg('Selecione um arquivo CSV ou XLSX.', false);
        return;
      }
      const fd = new FormData();
      fd.append('arquivo', arquivo);
      if (el.btnImportar) el.btnImportar.disabled = true;
      mostrarImportMsg('Importando…', true);
      try {
        if (window.gmLoadingBar) window.gmLoadingBar.show();
        const j = await fetchJson(urls.importar, {
          method: 'POST',
          headers: { 'X-CSRFToken': csrfToken() },
          body: fd,
        });
        const partes = [];
        if (j.criados != null) partes.push(j.criados + ' novo(s)');
        if (j.atualizados != null && j.atualizados > 0) partes.push(j.atualizados + ' atualizado(s)');
        if (j.resumo && j.resumo.total_saldo_aberto != null) {
          partes.push('saldo ' + fmtMoeda(j.resumo.total_saldo_aberto));
        }
        mostrarImportMsg('Importação concluída: ' + (partes.join(' · ') || 'ok'), true);
        if (el.formImportar) el.formImportar.reset();
        await recarregar();
      } catch (e) {
        mostrarImportMsg(e.message || String(e), false);
      } finally {
        if (el.btnImportar) el.btnImportar.disabled = false;
        if (window.gmLoadingBar) window.gmLoadingBar.hide();
      }
    });
  }
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
    if (ev.key === 'Escape') {
      if (el.modalBaixa && el.modalBaixa.open) return;
      if (el.modalEditar && el.modalEditar.open) return;
      if (el.modalLimite && el.modalLimite.open) return;
    }
    if (ev.key === '/' && document.activeElement !== el.busca) {
      const t = document.activeElement && document.activeElement.tagName;
      if (t !== 'INPUT' && t !== 'TEXTAREA' && t !== 'SELECT') {
        ev.preventDefault();
        el.busca && el.busca.focus();
      }
    }
  });

  recarregar().then(function () {
    if (CFG.clientePrePk) {
      const c = clientesCache.find(function (x) { return x.cliente_agro_pk === CFG.clientePrePk; });
      if (c) abrirModalCliente(clienteFromRow(c));
    }
  });
})();
