(function () {
  'use strict';

  const CFG = window.AGRO_FIADO || {};
  const urls = CFG.urls || {};

  const el = {
    busca: document.getElementById('fiado-busca'),
    btnAtualizar: document.getElementById('fiado-btn-atualizar'),
    tbody: document.getElementById('fiado-tbody-clientes'),
    kpiVendidoMes: document.getElementById('fiado-kpi-vendido-mes'),
    kpiVendidoAnt: document.getElementById('fiado-kpi-vendido-ant'),
    kpiPagoMes: document.getElementById('fiado-kpi-pago-mes'),
    kpiPagoAnt: document.getElementById('fiado-kpi-pago-ant'),
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
    btnRecibos: document.getElementById('fiado-btn-recibos'),
    btnAtualizarTitulos: document.getElementById('fiado-btn-atualizar-titulos'),
    recibosLista: document.getElementById('fiado-recibos-lista'),
    recibosResumo: document.getElementById('fiado-recibos-resumo'),
    recibosFechar: document.getElementById('fiado-recibos-fechar'),
    modalRecibos: document.getElementById('fiado-modal-recibos'),
    modalVenda: document.getElementById('fiado-modal-venda'),
    vendaFrame: document.getElementById('fiado-venda-frame'),
    vendaTitulo: document.getElementById('fiado-venda-titulo'),
    vendaFechar: document.getElementById('fiado-venda-fechar'),
    modalBaixa: document.getElementById('fiado-modal-baixa'),
    baixaPassoEscolha: document.getElementById('fiado-baixa-passo-escolha'),
    formBaixaParcial: document.getElementById('fiado-form-baixa-parcial'),
    baixaTitulo: document.getElementById('fiado-baixa-titulo'),
    baixaResumo: document.getElementById('fiado-baixa-resumo'),
    baixaSaldoHero: document.getElementById('fiado-baixa-saldo-hero'),
    baixaDica: document.getElementById('fiado-baixa-dica'),
    baixaBtnTotal: document.getElementById('fiado-baixa-btn-total'),
    baixaBtnParcial: document.getElementById('fiado-baixa-btn-parcial'),
    baixaValor: document.getElementById('fiado-baixa-valor'),
    baixaCancelar: document.getElementById('fiado-baixa-cancelar'),
    baixaVoltar: document.getElementById('fiado-baixa-voltar'),
    baixaConfirmar: document.getElementById('fiado-baixa-confirmar'),
    modalEditar: document.getElementById('fiado-modal-editar'),
    formEditar: document.getElementById('fiado-form-editar'),
    editarResumo: document.getElementById('fiado-editar-resumo'),
    editarDoc: document.getElementById('fiado-editar-doc'),
    editarVenc: document.getElementById('fiado-editar-venc'),
    editarValor: document.getElementById('fiado-editar-valor'),
    editarDesc: document.getElementById('fiado-editar-desc'),
    editarCancelar: document.getElementById('fiado-editar-cancelar'),
    emptyBanner: document.getElementById('fiado-empty-banner'),
    formImportar: document.getElementById('fiado-form-importar'),
    importArquivo: document.getElementById('fiado-import-arquivo'),
    btnImportar: document.getElementById('fiado-btn-importar'),
    importMsg: document.getElementById('fiado-import-msg'),
  };

  let baixaCtx = null;
  let baixaPassoAtual = 'escolha';
  let editarTituloId = null;
  let debounceTimer = null;
  let clientesCache = [];
  let clienteModal = null;
  let titulosCache = [];
  let selecionados = new Set();
  let limiteEditandoPk = null;

  function setModalBodyLock(on) {
    document.body.classList.toggle('fiado-modal-aberto', !!on);
    try {
      if (window.AgroOverlayStack) {
        window.AgroOverlayStack.setNested(!!on, 'fiado-cliente');
        return;
      }
    } catch (_) {}
    try {
      if (window.top && window.top !== window) {
        window.top.postMessage(
          {
            type: 'agro-pdv-overlay-meta',
            hideChrome: !!on,
          },
          window.location.origin
        );
      }
    } catch (_) {}
  }

  function modalClienteAberto() {
    return !!(el.modalCliente && el.modalCliente.open);
  }

  function csrfToken() {
    const meta = document.querySelector('meta[name="csrfmiddlewaretoken"]');
    if (meta && meta.getAttribute('content')) return meta.getAttribute('content');
    const m = document.cookie.match(/csrftoken=([^;]+)/);
    return m ? decodeURIComponent(m[1]) : '';
  }

  function fmtMoeda(v) {
    const n = Number(v) || 0;
    return 'R$ ' + n.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }

  function fmtMoedaHtml(v, extraCls) {
    const n = Number(v) || 0;
    const val = n.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    return (
      '<span class="fiado-moeda' + (extraCls ? ' ' + extraCls : '') + '">' +
      '<span class="fiado-moeda-sym">R$</span>' +
      '<span class="fiado-moeda-val">' + val + '</span></span>'
    );
  }

  function fmtLimiteCampo(v) {
    const n = Number(v) || 0;
    return n.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
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
      let msg = j.erro || j.mensagem || '';
      if (!msg && r.status === 403) msg = 'Sessão expirada ou CSRF inválido — recarregue a página (F5) e tente de novo.';
      if (!msg) msg = 'Falha na requisição (HTTP ' + r.status + ').';
      throw new Error(msg);
    }
    return j;
  }

  function situacaoClass(s) {
    if (s === 'vencido') return 'text-red-700 bg-red-50';
    if (s === 'parcial') return 'text-amber-800 bg-amber-50';
    if (s === 'quitado') return 'text-slate-600 bg-slate-100';
    if (s === 'zerado') return 'text-slate-600 bg-slate-100';
    if (s === 'cancelado') return 'text-slate-500 bg-slate-50';
    return 'text-emerald-800 bg-emerald-50';
  }

  function situacaoTituloClass(s) {
    if (s === 'vencido') return 'text-red-700 bg-red-50';
    if (s === 'parcial') return 'text-amber-800 bg-amber-50';
    if (s === 'quitado') return 'text-slate-600 bg-slate-100';
    if (s === 'cancelado') return 'text-slate-500 bg-slate-50';
    return 'text-emerald-800 bg-emerald-50';
  }

  function atualizarKpis(resumo) {
    if (!resumo) return;
    if (el.kpiTotal) el.kpiTotal.textContent = fmtMoeda(resumo.total_saldo_aberto);
    if (el.kpiClientes) el.kpiClientes.textContent = String(resumo.clientes_com_saldo || 0);
    if (el.kpiVendidoMes && resumo.vendido_mes != null) el.kpiVendidoMes.textContent = fmtMoeda(resumo.vendido_mes);
    if (el.kpiVendidoAnt && resumo.vendido_mes_anterior != null) el.kpiVendidoAnt.textContent = fmtMoeda(resumo.vendido_mes_anterior);
    if (el.kpiPagoMes && resumo.pago_mes != null) el.kpiPagoMes.textContent = fmtMoeda(resumo.pago_mes);
    if (el.kpiPagoAnt && resumo.pago_mes_anterior != null) el.kpiPagoAnt.textContent = fmtMoeda(resumo.pago_mes_anterior);
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
      const limiteVal = c.limite_fiado_local != null ? c.limite_fiado_local : (c.limite || 0);
      const destaque = CFG.clientePrePk && pk === CFG.clientePrePk ? ' ring-2 ring-inset ring-orange-300 bg-orange-50' : '';
      const limiteTxt = esc(fmtLimiteCampo(limiteVal));
      const limiteCel = pk
        ? '<div class="fiado-limite-cell">' +
          '<button type="button" class="fiado-limite-valor" data-pk="' + pk + '" data-valor="' + limiteTxt + '" title="Clique para editar o limite deste cliente" aria-label="Editar limite">' +
          limiteTxt +
          '</button></div>'
        : '<span class="block text-right tabular-nums font-black text-slate-400" title="Cadastro sem vínculo — não dá para editar limite aqui">' + limiteTxt + '</span>';
      return (
        '<tr class="fiado-cli-row border-t border-slate-100' + destaque + '" data-pk="' + esc(pk || '') + '" data-nome="' + esc(c.cliente_nome) + '" data-codigo="' + esc(c.cliente_codigo || '') + '" data-saldo="' + c.saldo_aberto + '" data-titulos="' + (c.titulos_abertos || 0) + '">' +
        '<td class="font-black text-slate-900 max-w-[16rem] truncate" title="' + esc(c.cliente_nome) + '">' + esc(c.cliente_nome) + '</td>' +
        '<td class="text-center font-bold tabular-nums">' + (c.titulos_abertos || 0) + '</td>' +
        '<td class="font-bold whitespace-nowrap">' + esc(c.vencimento_mais_antigo_texto || '—') + '</td>' +
        '<td class="text-right tabular-nums font-semibold">' + fmtMoeda(c.valor_bruto) + '</td>' +
        '<td class="text-right tabular-nums text-slate-600">' + fmtMoeda(c.valor_pago) + '</td>' +
        '<td class="text-right tabular-nums font-black text-orange-800">' + fmtMoeda(c.saldo_aberto) + '</td>' +
        '<td class="text-right">' + limiteCel + '</td>' +
        '<td><span class="inline-block rounded-lg px-2 py-0.5 text-[10px] font-black uppercase ' + situacaoClass(c.situacao_resumo) + '">' + esc(c.situacao_label) + '</span></td>' +
        '<td class="text-right whitespace-nowrap">' +
        '<button type="button" class="fiado-btn-baixa fiado-acao-slot bg-orange-600 text-white shadow-sm hover:bg-orange-700" data-pk="' + esc(pk || '') + '" data-nome="' + esc(c.cliente_nome) + '" data-codigo="' + esc(c.cliente_codigo || '') + '" data-saldo="' + c.saldo_aberto + '">Baixa</button>' +
        '</td></tr>'
      );
    }).join('');
  }

  function titulosQueryParams(cli) {
    const qs = new URLSearchParams({ situacao: 'abertos', limit: '500' });
    const nome = String(cli.nome || '').trim();
    if (nome) {
      qs.set('cliente_nome', nome);
    } else if (cli.pk) {
      qs.set('cliente_agro_pk', String(cli.pk));
    } else if (cli.codigo) {
      qs.set('cliente_codigo', cli.codigo);
    }
    return qs.toString();
  }

  function clientePkMatch(a, b) {
    if (a == null || b == null || a === '' || b === '') return false;
    return Number(a) === Number(b);
  }

  function scrollParaClientePk(pk) {
    if (!pk || !el.tbody) return;
    const row = el.tbody.querySelector('.fiado-cli-row[data-pk="' + String(pk) + '"]');
    if (row && row.scrollIntoView) row.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
  }

  async function abrirClientePrePk() {
    const pk = CFG.clientePrePk;
    if (!pk) return;
    const naLista = clientesCache.find(function (x) {
      return clientePkMatch(x.cliente_agro_pk, pk);
    });
    if (naLista) {
      abrirModalCliente(clienteFromRow(naLista));
      scrollParaClientePk(pk);
      return;
    }
    try {
      const credUrl =
        (urls.clienteCredito || '/api/fiado/cliente-credito/') +
        '?cliente_agro_pk=' +
        encodeURIComponent(String(pk));
      const cred = await fetchJson(credUrl);
      const nomeCli = String(cred.cliente_nome || '').trim();
      const titParams = { situacao: 'abertos', limit: '500' };
      if (nomeCli) {
        titParams.cliente_nome = nomeCli;
      } else {
        titParams.cliente_agro_pk = String(pk);
      }
      const titUrl = urls.titulos + '?' + new URLSearchParams(titParams).toString();
      const tit = await fetchJson(titUrl);
      const lista = tit.titulos || [];
      abrirModalCliente({
        pk: Number(pk),
        nome: cred.cliente_nome || '',
        codigo: cred.cliente_id || '',
        saldo: saldoTitulos(lista) || cred.usado || 0,
        titulos: lista.length,
      });
    } catch (e) {
      console.warn('[fiado] abrir cliente pre pk', e);
    }
  }

  function saldoTitulos(list) {
    return (list || []).reduce(function (acc, t) { return acc + (Number(t.saldo_aberto) || 0); }, 0);
  }

  function imprimirReciboFiado(row) {
    const opts = {
      recibo_id: row && row.recibo_id != null ? row.recibo_id : null,
      baixas_ids: row && Array.isArray(row.baixas_ids) ? row.baixas_ids : [],
      segunda_via: true,
    };
    if (typeof window.agroCarregarEImprimirReciboFiado !== 'function') {
      alert('Módulo de impressão não carregou. Dê Ctrl+F5 e tente de novo.');
      return;
    }
    window.agroCarregarEImprimirReciboFiado(opts).catch(function (err) {
      alert((err && err.message) || 'Não foi possível imprimir o recibo.');
    });
  }

  function vendaDetalheUrl(vendaId) {
    const base = String(urls.vendaDetalheBase || '').trim();
    if (!base || !vendaId) return '';
    let url = base.replace(/0\/?$/, String(vendaId) + '/');
    try {
      const u = new URL(url, window.location.origin);
      u.searchParams.set('agro_fiado_embed', '1');
      u.searchParams.set('agro_inapp_embed', '1');
      if (new URLSearchParams(window.location.search || '').get('agro_pdv_overlay') === '1') {
        u.searchParams.set('agro_pdv_overlay', '1');
      }
      return u.href;
    } catch (_) {
      return url;
    }
  }

  function fecharVendaOverlay() {
    if (el.vendaFrame) {
      try {
        el.vendaFrame.src = 'about:blank';
      } catch (_) {}
    }
    if (el.modalVenda && el.modalVenda.open) {
      el.modalVenda.close();
    }
    try {
      if (window.AgroOverlayStack && el.modalVenda) {
        window.AgroOverlayStack.setOpen(el.modalVenda, false);
      }
    } catch (_) {}
  }

  function abrirVendaOverlay(vendaId) {
    const url = vendaDetalheUrl(vendaId);
    if (!url) {
      alert('Pedido sem venda vinculada.');
      return;
    }
    if (!el.modalVenda || !el.vendaFrame) {
      window.open(url, '_blank');
      return;
    }
    if (el.vendaTitulo) el.vendaTitulo.textContent = 'Pedido / venda #' + vendaId;
    el.vendaFrame.src = url;
    if (el.modalVenda.showModal) el.modalVenda.showModal();
    else el.modalVenda.setAttribute('open', '');
    try {
      if (window.AgroOverlayStack) window.AgroOverlayStack.setOpen(el.modalVenda, true);
    } catch (_) {}
  }

  function renderRecibos(recibos) {
    if (!el.recibosLista) return;
    const rows = recibos || [];
    if (!rows.length) {
      el.recibosLista.innerHTML = '<p class="rounded-xl border border-slate-200 bg-slate-50 px-3 py-4 text-sm font-bold text-slate-500">Nenhum recibo recente para este cliente.</p>';
      return;
    }
    el.recibosLista.innerHTML = rows
      .map(function (r) {
        const recId = r.recibo_id != null ? String(r.recibo_id) : '';
        const baixas = Array.isArray(r.baixas_ids) ? r.baixas_ids.join(',') : '';
        return (
          '<div class="flex items-center gap-2 min-h-[40px] rounded-xl border border-orange-200 bg-white px-2.5 py-1.5">' +
          '<div class="min-w-0 flex-1">' +
          '<p class="text-xs font-black text-slate-900 tabular-nums leading-tight">' +
          esc(r.criado_em || '—') +
          ' · ' +
          esc(r.valor_texto || fmtMoeda(r.valor)) +
          '</p>' +
          '<p class="text-[10px] font-bold text-slate-600 truncate">' +
          esc(r.forma || '—') +
          (r.operador ? ' · ' + esc(r.operador) : '') +
          '</p></div>' +
          '<button type="button" class="fiado-btn-reimprimir shrink-0 min-h-[36px] px-3 rounded-xl border-2 border-orange-400 bg-orange-50 text-orange-950 text-[10px] font-black uppercase hover:bg-orange-100" data-recibo="' +
          esc(recId) +
          '" data-baixas="' +
          esc(baixas) +
          '">Reimprimir</button></div>'
        );
      })
      .join('');
  }

  async function carregarRecibosCliente(cli) {
    if (!cli || !urls.recibos) {
      renderRecibos([]);
      return;
    }
    const qs = new URLSearchParams();
    if (cli.pk || cli.cliente_agro_pk) qs.set('cliente_agro_pk', String(cli.pk || cli.cliente_agro_pk));
    if (cli.nome || cli.cliente_nome) qs.set('cliente_nome', String(cli.nome || cli.cliente_nome || ''));
    if (cli.codigo || cli.cliente_codigo) qs.set('cliente_codigo', String(cli.codigo || cli.cliente_codigo || ''));
    try {
      const j = await fetchJson(urls.recibos + '?' + qs.toString());
      renderRecibos(j.recibos || []);
    } catch (e) {
      renderRecibos([]);
    }
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
      const sit = t.situacao_resumo || t.situacao || '';
      const rowCls =
        sit === 'vencido' || t.vencido ? ' fiado-tit-vencido' : '';
      const verSlot = t.venda_agro_id
        ? '<button type="button" class="fiado-btn-ver-tit fiado-acao-slot border-2 border-sky-200 bg-sky-50 text-sky-900 hover:bg-sky-100" data-venda-id="' + t.venda_agro_id + '">Ver</button>'
        : '<span class="fiado-pill-legado">Sistema antigo</span>';
      return (
        '<tr class="border-b border-slate-100' + rowCls + '" data-id="' + t.id + '">' +
        '<td><input type="checkbox" class="fiado-tit-chk rounded border-slate-300" data-id="' + t.id + '" aria-label="Selecionar"></td>' +
        '<td class="font-bold text-slate-900 truncate" title="' + esc(t.numero_documento) + '">' +
        (t.venda_agro_id
          ? '<button type="button" class="fiado-link-pedido text-left font-bold" data-venda-id="' + t.venda_agro_id + '">' + esc(t.numero_documento || '—') + '</button>'
          : '<span>' + esc(t.numero_documento || '—') + '</span>') +
        '</td>' +
        '<td class="tabular-nums font-semibold">' + parcela + '</td>' +
        '<td class="font-bold whitespace-nowrap' +
        (sit === 'vencido' || t.vencido ? ' fiado-tit-venc-data' : '') +
        '">' +
        esc(t.vencimento_texto || '—') +
        '</td>' +
        '<td class="text-right whitespace-nowrap">' + fmtMoedaHtml(t.valor_bruto) + '</td>' +
        '<td class="text-right whitespace-nowrap text-slate-600">' + fmtMoedaHtml(t.valor_pago, 'text-slate-600') + '</td>' +
        '<td class="text-right whitespace-nowrap">' + fmtMoedaHtml(t.saldo_aberto, 'text-orange-800') + '</td>' +
        '<td><span class="inline-block rounded-lg px-2 py-0.5 text-[0.78rem] font-black uppercase ' + situacaoTituloClass(sit) + '">' + esc(t.situacao_label || '—') + '</span></td>' +
        '<td>' +
        '<div class="fiado-tit-acoes">' +
        '<button type="button" class="fiado-btn-baixa-tit fiado-acao-slot bg-orange-600 text-white shadow-sm hover:bg-orange-700" data-id="' + t.id + '" data-saldo="' + t.saldo_aberto + '" data-doc="' + esc(t.numero_documento || '') + '">Baixa</button>' +
        verSlot +
        '<button type="button" class="fiado-btn-editar-tit fiado-acao-slot border-2 border-emerald-200 bg-emerald-50 text-emerald-900 hover:bg-emerald-100" data-id="' + t.id + '">Editar</button>' +
        '</div></td></tr>'
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
      const j = await fetchJson(urls.titulos + '?' + titulosQueryParams(cli));
      renderTitulos(j.titulos || []);
      const saldo = saldoTitulos(j.titulos);
      if (el.cliModalSaldo) el.cliModalSaldo.textContent = fmtMoeda(saldo);
      clienteModal.saldo = saldo;
      if (el.recibosResumo) {
        el.recibosResumo.textContent = (cli.nome || 'Cliente') + ' · últimos recibos para reimpressão';
      }
    } catch (e) {
      if (el.tbodyTitulos) {
        el.tbodyTitulos.innerHTML =
          '<tr><td colspan="9" class="px-4 py-8 text-center text-sm font-bold text-red-700">' +
          esc(e.message || String(e)) +
          '</td></tr>';
      }
    }
  }

  function abrirModalCliente(cli) {
    if (!el.modalCliente) return;
    clienteModal = cli;
    if (el.cliModalNome) el.cliModalNome.textContent = cli.nome || '—';
    if (el.cliModalMeta) {
      el.cliModalMeta.textContent =
        (cli.codigo ? 'Cód. ' + cli.codigo + ' · ' : '') + (cli.titulos || 0) + ' título(s)';
    }
    if (el.cliModalSaldo) el.cliModalSaldo.textContent = fmtMoeda(cli.saldo || 0);
    if (el.tbodyTitulos) {
      el.tbodyTitulos.innerHTML =
        '<tr><td colspan="9" class="px-4 py-10 text-center text-sm font-bold text-slate-500">Carregando lançamentos…</td></tr>';
    }
    selecionados.clear();
    atualizarSelecaoUi();
    if (el.modalCliente.showModal) {
      el.modalCliente.showModal();
      setModalBodyLock(true);
    }
    carregarTitulosCliente(cli);
  }

  async function recarregar() {
    const q = el.busca ? el.busca.value.trim() : '';
    const qs = new URLSearchParams({ q: q, apenas_saldo: q ? '0' : '1' });
    try {
      if (window.gmLoadingBar) window.gmLoadingBar.show();
      const cli = await fetchJson(urls.clientes + '?' + qs.toString());
      const res = cli.resumo || {};
      renderClientes(cli.clientes || []);
      atualizarKpis(res);
      atualizarEmptyBanner(res);
      if (clienteModal && el.modalCliente && el.modalCliente.open) {
        const atual = (cli.clientes || []).find(function (c) {
          if (clienteModal.pk && clientePkMatch(c.cliente_agro_pk, clienteModal.pk)) return true;
          return c.cliente_nome === clienteModal.nome && String(c.cliente_codigo || '') === String(clienteModal.codigo || '');
        });
        if (atual) {
          clienteModal = clienteFromRow(atual);
          await carregarTitulosCliente(clienteModal);
        } else if (clienteModal.pk) {
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

  function inPdvOverlay() {
    try {
      if (window.top && window.top !== window) return true;
      return new URLSearchParams(window.location.search || '').get('agro_pdv_overlay') === '1';
    } catch (_) {
      return false;
    }
  }

  function parseValorMoedaBr(raw) {
    const s = String(raw || '').trim();
    if (!s) return 0;
    const n = parseFloat(s.replace(/\./g, '').replace(',', '.'));
    return Number.isFinite(n) ? n : 0;
  }

  function cobrancaParamsFromCtx(ctx) {
    const modo = ctx.modo || 'cliente';
    const o = { modo: modo };
    if (modo === 'titulo' && ctx.tituloId) o.titulo_id = String(ctx.tituloId);
    if (modo === 'selecionados' && ctx.ids && ctx.ids.length) {
      o.titulo_ids = ctx.ids.join(',');
    }
    if (modo === 'cliente') {
      if (ctx.pk) o.cliente_agro_pk = String(ctx.pk);
      if (ctx.nome) o.cliente_nome = ctx.nome;
      if (ctx.codigo) o.cliente_codigo = ctx.codigo;
    }
    if (ctx.valor != null && Number(ctx.valor) > 0) {
      o.valor = String(Number(ctx.valor).toFixed(2)).replace('.', ',');
    }
    return o;
  }

  function redirectToPdvCobranca(ctx) {
    if (!CFG.caixaAberto) {
      if (!window.confirm('O caixa não está aberto neste navegador. Abra o caixa no PDV antes de confirmar. Ir mesmo assim?')) {
        return;
      }
    }
    try {
      if (window.top && window.top !== window) {
        window.top.postMessage(
          {
            type: 'agro-fiado-cobranca-start',
            params: cobrancaParamsFromCtx(ctx),
          },
          window.location.origin
        );
        return;
      }
    } catch (_) {}
    const base = (CFG.pdvHome || '/pdv/').split('?')[0];
    const p = new URLSearchParams();
    p.set('fiado_cobranca', '1');
    const modo = ctx.modo || 'cliente';
    p.set('modo', modo);
    if (modo === 'titulo' && ctx.tituloId) p.set('titulo_id', String(ctx.tituloId));
    if (modo === 'selecionados' && ctx.ids && ctx.ids.length) {
      p.set('titulo_ids', ctx.ids.join(','));
    }
    if (modo === 'cliente') {
      if (ctx.pk) p.set('cliente_agro_pk', String(ctx.pk));
      if (ctx.nome) p.set('cliente_nome', ctx.nome);
      if (ctx.codigo) p.set('cliente_codigo', ctx.codigo);
    }
    if (ctx.valor != null && Number(ctx.valor) > 0) {
      p.set('valor', String(Number(ctx.valor).toFixed(2)).replace('.', ','));
    }
    window.location.href = base + '?' + p.toString();
  }

  function fecharModalBaixa() {
    if (el.modalBaixa && el.modalBaixa.close) el.modalBaixa.close();
    baixaCtx = null;
    baixaPassoAtual = 'escolha';
  }

  function mostrarPassoBaixa(passo) {
    baixaPassoAtual = passo === 'parcial' ? 'parcial' : 'escolha';
    if (el.baixaPassoEscolha) {
      el.baixaPassoEscolha.classList.toggle('hidden', baixaPassoAtual !== 'escolha');
    }
    if (el.formBaixaParcial) {
      el.formBaixaParcial.classList.toggle('hidden', baixaPassoAtual !== 'parcial');
    }
    if (el.baixaTitulo) {
      el.baixaTitulo.textContent =
        baixaPassoAtual === 'parcial' ? 'Quanto vai receber hoje?' : 'Como o cliente vai pagar?';
    }
  }

  function irParaPdvBaixa(valorNum) {
    if (!baixaCtx) return;
    const saldoMax = Number(baixaCtx.saldo) || 0;
    const v = valorNum != null ? Number(valorNum) : saldoMax;
    if (v <= 0) {
      alert('Informe um valor maior que zero.');
      return;
    }
    if (v > saldoMax + 0.02) {
      alert('Valor maior que o saldo em aberto (' + fmtMoeda(saldoMax) + ').');
      return;
    }
    const ctx = Object.assign({}, baixaCtx, { valor: Math.round(v * 100) / 100 });
    fecharModalBaixa();
    redirectToPdvCobranca(ctx);
  }

  function confirmarBaixaTotal() {
    if (!baixaCtx) return;
    irParaPdvBaixa(Number(baixaCtx.saldo) || 0);
  }

  function abrirPassoParcialBaixa() {
    if (!baixaCtx) return;
    const saldo = Number(baixaCtx.saldo) || 0;
    mostrarPassoBaixa('parcial');
    if (el.baixaValor) {
      el.baixaValor.value = saldo.toFixed(2).replace('.', ',');
      setTimeout(function () {
        el.baixaValor.focus();
        el.baixaValor.select();
      }, 60);
    }
  }

  function abrirBaixa(ctx) {
    baixaCtx = ctx || null;
    if (!baixaCtx) return;
    const saldo = Number(baixaCtx.saldo) || 0;
    let resumo = '';
    const modo = baixaCtx.modo || 'cliente';
    if (modo === 'titulo') {
      resumo = (baixaCtx.doc || 'Lançamento') + ' · saldo ' + fmtMoeda(saldo);
    } else if (modo === 'selecionados') {
      const qtd = baixaCtx.ids && baixaCtx.ids.length ? baixaCtx.ids.length : 0;
      resumo = (baixaCtx.nome || 'Cliente') + ' · ' + qtd + ' título(s) · saldo ' + fmtMoeda(saldo);
    } else {
      resumo = (baixaCtx.nome || 'Cliente') + ' · saldo ' + fmtMoeda(saldo);
    }
    if (el.baixaResumo) el.baixaResumo.textContent = resumo;
    if (el.baixaSaldoHero) el.baixaSaldoHero.textContent = fmtMoeda(saldo);
    mostrarPassoBaixa('escolha');
    if (el.modalBaixa && el.modalBaixa.showModal) {
      el.modalBaixa.showModal();
      setTimeout(function () {
        if (el.baixaBtnTotal) el.baixaBtnTotal.focus();
      }, 60);
    }
  }

  function confirmarBaixaParcial(ev) {
    if (ev) ev.preventDefault();
    if (!baixaCtx) return;
    const valorNum = parseValorMoedaBr(el.baixaValor ? el.baixaValor.value : '');
    irParaPdvBaixa(valorNum);
  }

  function onBaixaModalKeydown(ev) {
    if (!el.modalBaixa || !el.modalBaixa.open) return;
    const key = ev.key || '';
    if (baixaPassoAtual === 'escolha') {
      if (key === 'Enter') {
        ev.preventDefault();
        confirmarBaixaTotal();
        return;
      }
      if (key === 'p' || key === 'P') {
        ev.preventDefault();
        abrirPassoParcialBaixa();
        return;
      }
      if (key === 'Escape') {
        ev.preventDefault();
        fecharModalBaixa();
      }
      return;
    }
    if (key === 'Escape') {
      ev.preventDefault();
      mostrarPassoBaixa('escolha');
      setTimeout(function () {
        if (el.baixaBtnTotal) el.baixaBtnTotal.focus();
      }, 40);
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

  function htmlBotaoLimite(pk, valorFmt) {
    const txt = esc(valorFmt);
    return (
      '<button type="button" class="fiado-limite-valor" data-pk="' + pk + '" data-valor="' + txt + '" title="Clique para editar o limite deste cliente" aria-label="Editar limite">' +
      txt +
      '</button>'
    );
  }

  function finalizarEdicaoLimite(cell, pk, valorFmt, flashOk) {
    if (!cell) return;
    cell.innerHTML = htmlBotaoLimite(pk, valorFmt);
    if (limiteEditandoPk === pk) limiteEditandoPk = null;
    if (flashOk) {
      const btn = cell.querySelector('.fiado-limite-valor');
      if (btn) {
        btn.style.borderColor = 'rgb(16 185 129)';
        btn.style.background = 'rgb(209 250 229)';
        setTimeout(function () {
          btn.style.borderColor = '';
          btn.style.background = '';
        }, 900);
      }
    }
  }

  function iniciarEdicaoLimite(btn) {
    if (!btn || !el.tbody) return;
    const pk = parseInt(btn.getAttribute('data-pk'), 10);
    if (!pk) return;
    const cell = btn.closest('.fiado-limite-cell');
    if (!cell) return;
    if (limiteEditandoPk && limiteEditandoPk !== pk) {
      const outro = el.tbody.querySelector('.fiado-limite-input[data-pk="' + limiteEditandoPk + '"]');
      if (outro) gravarLimiteNaLinha(outro);
    }
    const original = btn.getAttribute('data-valor') || '0,00';
    limiteEditandoPk = pk;
    cell.innerHTML =
      '<input type="text" inputmode="decimal" class="fiado-limite-input" value="' + esc(original) +
      '" data-pk="' + pk + '" data-original="' + esc(original) +
      '" aria-label="Limite do cliente" title="Enter grava · Esc cancela">';
    const inp = cell.querySelector('.fiado-limite-input');
    if (inp) {
      try {
        inp.focus();
        inp.select();
      } catch (_) {}
    }
  }

  async function gravarLimiteNaLinha(inp) {
    if (!inp || inp.disabled || inp.classList.contains('is-saving')) return;
    const pk = parseInt(inp.getAttribute('data-pk'), 10);
    if (!pk) return;
    const cell = inp.closest('.fiado-limite-cell');
    const original = inp.getAttribute('data-original') || '';
    const atual = String(inp.value || '').trim();
    if (atual === original) {
      finalizarEdicaoLimite(cell, pk, original || '0,00', false);
      return;
    }
    const valorNum = parseValorMoedaBr(atual);
    if (valorNum < 0) {
      alert('Limite não pode ser negativo.');
      inp.value = original;
      finalizarEdicaoLimite(cell, pk, original || '0,00', false);
      return;
    }
    inp.classList.add('is-saving');
    inp.disabled = true;
    try {
      await salvarLimite(pk, atual || '0');
      const fmt = fmtLimiteCampo(valorNum);
      const row = clientesCache.find(function (c) { return Number(c.cliente_agro_pk) === pk; });
      if (row) {
        row.limite_fiado_local = valorNum;
        row.limite = valorNum;
      }
      finalizarEdicaoLimite(cell, pk, fmt, true);
    } catch (e) {
      alert(e.message || String(e));
      finalizarEdicaoLimite(cell, pk, original || '0,00', false);
    }
  }

  if (el.tbody) {
    el.tbody.addEventListener('click', function (ev) {
      if (ev.target.closest('.fiado-limite-input')) {
        ev.stopPropagation();
        return;
      }
      const bLimValor = ev.target.closest('.fiado-limite-valor');
      if (bLimValor) {
        ev.stopPropagation();
        ev.preventDefault();
        iniciarEdicaoLimite(bLimValor);
        return;
      }
      const bBaixa = ev.target.closest('.fiado-btn-baixa');
      if (bBaixa) {
        ev.stopPropagation();
        abrirBaixa({
          modo: 'cliente',
          pk: (function () {
            var raw = bBaixa.getAttribute('data-pk');
            if (!raw) return null;
            var n = parseInt(raw, 10);
            return Number.isFinite(n) ? n : null;
          })(),
          nome: bBaixa.getAttribute('data-nome') || '',
          codigo: bBaixa.getAttribute('data-codigo') || '',
          saldo: parseFloat(bBaixa.getAttribute('data-saldo') || '0'),
        });
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
    el.tbody.addEventListener('keydown', function (ev) {
      const inp = ev.target.closest('.fiado-limite-input');
      if (!inp) return;
      if (ev.key === 'Enter') {
        ev.preventDefault();
        inp.blur();
      }
      if (ev.key === 'Escape') {
        ev.preventDefault();
        const pk = parseInt(inp.getAttribute('data-pk'), 10);
        const cell = inp.closest('.fiado-limite-cell');
        const original = inp.getAttribute('data-original') || '0,00';
        finalizarEdicaoLimite(cell, pk, original, false);
      }
    });
    el.tbody.addEventListener('focusout', function (ev) {
      const inp = ev.target.closest('.fiado-limite-input');
      if (!inp) return;
      // Aguarda um tick: se o foco saiu do input (não ficou no mesmo), grava
      setTimeout(function () {
        if (!inp.isConnected) return;
        if (document.activeElement === inp) return;
        gravarLimiteNaLinha(inp);
      }, 0);
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
      const bVer = ev.target.closest('.fiado-btn-ver-tit, .fiado-link-pedido');
      if (bVer) {
        abrirVendaOverlay(parseInt(bVer.getAttribute('data-venda-id'), 10));
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

  if (el.btnBaixaTotalCli) {
    el.btnBaixaTotalCli.addEventListener('click', function () {
      if (!clienteModal) return;
      let saldo = 0;
      titulosCache.forEach(function (t) {
        if (Number(t.saldo_aberto) > 0) saldo += Number(t.saldo_aberto) || 0;
      });
      if (saldo <= 0 && clienteModal.saldo) saldo = Number(clienteModal.saldo) || 0;
      abrirBaixa({
        modo: 'cliente',
        pk: clienteModal.pk,
        nome: clienteModal.nome,
        codigo: clienteModal.codigo,
        saldo: saldo,
      });
    });
  }

  if (el.btnAtualizarTitulos) {
    el.btnAtualizarTitulos.addEventListener('click', function () {
      if (clienteModal) carregarTitulosCliente(clienteModal);
    });
  }

  if (el.btnRecibos) {
    el.btnRecibos.addEventListener('click', function () {
      if (!clienteModal || !el.modalRecibos || !el.modalRecibos.showModal) return;
      if (el.recibosResumo) {
        el.recibosResumo.textContent = (clienteModal.nome || 'Cliente') + ' · últimos recibos para reimpressão';
      }
      if (el.recibosLista) {
        el.recibosLista.innerHTML = '<p class="rounded-xl border border-slate-200 bg-slate-50 px-3 py-4 text-sm font-bold text-slate-500">Carregando recibos…</p>';
      }
      el.modalRecibos.showModal();
      carregarRecibosCliente(clienteModal);
    });
  }

  if (el.recibosLista) {
    el.recibosLista.addEventListener('click', function (ev) {
      const btn = ev.target.closest('.fiado-btn-reimprimir');
      if (!btn) return;
      const recRaw = btn.getAttribute('data-recibo') || '';
      const recId = recRaw ? parseInt(recRaw, 10) : null;
      const baixas = String(btn.getAttribute('data-baixas') || '')
        .split(',')
        .map(function (x) {
          return parseInt(String(x).trim(), 10);
        })
        .filter(function (n) {
          return !!n;
        });
      imprimirReciboFiado({
        recibo_id: Number.isFinite(recId) ? recId : null,
        baixas_ids: baixas,
      });
    });
  }
  if (el.recibosFechar && el.modalRecibos) {
    el.recibosFechar.addEventListener('click', function () {
      el.modalRecibos.close();
    });
  }

  if (el.vendaFechar) {
    el.vendaFechar.addEventListener('click', fecharVendaOverlay);
  }
  if (el.modalVenda) {
    el.modalVenda.addEventListener('cancel', function (ev) {
      ev.preventDefault();
      fecharVendaOverlay();
    });
    el.modalVenda.addEventListener('close', function () {
      if (el.vendaFrame) {
        try {
          el.vendaFrame.src = 'about:blank';
        } catch (_) {}
      }
      try {
        if (window.AgroOverlayStack) window.AgroOverlayStack.setOpen(el.modalVenda, false);
      } catch (_) {}
    });
  }
  window.addEventListener('message', function (ev) {
    try {
      if (!ev || ev.origin !== window.location.origin) return;
      if (ev.data && ev.data.type === 'fiado-venda-overlay-close') fecharVendaOverlay();
    } catch (_) {}
  });

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

  if (el.formBaixaParcial) el.formBaixaParcial.addEventListener('submit', confirmarBaixaParcial);
  if (el.baixaBtnTotal) el.baixaBtnTotal.addEventListener('click', confirmarBaixaTotal);
  if (el.baixaBtnParcial) el.baixaBtnParcial.addEventListener('click', abrirPassoParcialBaixa);
  if (el.baixaCancelar) el.baixaCancelar.addEventListener('click', fecharModalBaixa);
  if (el.baixaVoltar) {
    el.baixaVoltar.addEventListener('click', function () {
      mostrarPassoBaixa('escolha');
      if (el.baixaBtnTotal) el.baixaBtnTotal.focus();
    });
  }
  if (el.modalBaixa) {
    el.modalBaixa.addEventListener('keydown', onBaixaModalKeydown);
    el.modalBaixa.addEventListener('close', function () {
      baixaCtx = null;
      baixaPassoAtual = 'escolha';
    });
    el.modalBaixa.addEventListener('cancel', function (ev) {
      ev.preventDefault();
      if (baixaPassoAtual === 'parcial') {
        mostrarPassoBaixa('escolha');
        if (el.baixaBtnTotal) el.baixaBtnTotal.focus();
        return;
      }
      fecharModalBaixa();
    });
  }
  if (el.formEditar) el.formEditar.addEventListener('submit', confirmarEditar);
  if (el.editarCancelar && el.modalEditar) {
    el.editarCancelar.addEventListener('click', function () { el.modalEditar.close(); });
  }
  if (el.modalRecibos) {
    el.modalRecibos.addEventListener('cancel', function (ev) {
      ev.preventDefault();
      el.modalRecibos.close();
    });
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

  document.addEventListener('keydown', function (ev) {
    if (ev.key === 'Escape') {
      if (el.modalBaixa && el.modalBaixa.open) return;
      if (el.modalEditar && el.modalEditar.open) return;
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
    return abrirClientePrePk();
  });
})();
