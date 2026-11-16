(() => {
  'use strict';

  const CFG = window.SISTAPP_DASHBOARD_CFG || {};
  const API_URL = CFG.apiResumoUrl || '';
  const HUB_URL = CFG.hubUrl || '/vendas/lojas/';
  const OPERADOR = CFG.operador || '';

  const ICON = {
    dashboard: '<rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/>',
    trend: '<path d="m3 17 6-6 4 4 8-8"/><path d="M15 7h6v6"/>',
    bag: '<path d="M6 7h12l1 14H5L6 7Z"/><path d="M9 9V6a3 3 0 0 1 6 0v3"/>',
    wallet: '<rect x="3" y="6" width="18" height="15" rx="3"/><path d="M3 10h18M6 6V4h13"/><path d="M16 15h2"/>',
    target: '<circle cx="12" cy="12" r="9"/><circle cx="12" cy="12" r="5"/><circle cx="12" cy="12" r="1"/>',
    calendar: '<rect x="3" y="5" width="18" height="16" rx="2"/><path d="M8 3v4m8-4v4M3 10h18"/>',
    filter: '<path d="M4 7h16M7 12h10m-7 5h4"/>',
    refresh: '<path d="M20 11a8 8 0 1 0-2.4 5.7"/><path d="M20 4v7h-7"/>',
    eye: '<path d="M2 12s4-7 10-7 10 7 10 7-4 7-10 7S2 12 2 12Z"/><circle cx="12" cy="12" r="3"/>',
    eyeoff: '<path d="M3 3 21 21M10.6 5.1A11 11 0 0 1 12 5c6 0 10 7 10 7a16 16 0 0 1-3.4 4.2M6.1 6.1C3.5 8 2 12 2 12s4 7 10 7a10 10 0 0 0 4.2-.9"/><path d="M10 10a3 3 0 0 0 4 4"/>',
    arrow: '<path d="M5 12h14m-6-6 6 6-6 6"/>',
    chevron: '<path d="m9 18 6-6-6-6"/>',
    up: '<path d="m6 15 6-6 6 6"/>',
    down: '<path d="m6 9 6 6 6-6"/>',
    close: '<path d="M18 6 6 18M6 6l12 12"/>',
    search: '<circle cx="11" cy="11" r="7"/><path d="m20 20-4-4"/>',
    clock: '<circle cx="12" cy="12" r="9"/><path d="M12 7v5l4 2"/>',
    store: '<path d="M4 10V21h16V10M3 10l2-6h14l2 6M3 10c0 3 5 3 5 0 0 3 5 3 5 0 0 3 5 3 5 0 0 3 3 3 3 0"/>',
    box: '<path d="m12 2 9 5v10l-9 5-9-5V7l9-5ZM3 7l9 5 9-5M12 12v10"/>',
    layers: '<path d="m12 2 9 5-9 5-9-5 9-5Zm-9 10 9 5 9-5M3 17l9 5 9-5"/>',
    credit: '<rect x="2" y="5" width="20" height="14" rx="3"/><path d="M2 10h20"/>',
    receipt: '<path d="M5 3h14v18l-3-2-4 2-4-2-3 2V3Z"/><path d="M8 8h8M8 12h8M8 16h4"/>',
    info: '<circle cx="12" cy="12" r="10"/><path d="M12 11v5m0-9h.01"/>',
    download: '<path d="M12 3v12m-5-5 5 5 5-5M4 17v4h16v-4"/>',
    moon: '<path d="M20 14A8 8 0 0 1 10 4 8.5 8.5 0 1 0 20 14Z"/>',
    sun: '<circle cx="12" cy="12" r="4"/><path d="M12 2v2m0 16v2M4.9 4.9l1.4 1.4m11.4 11.4 1.4 1.4M2 12h2m16 0h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4"/>',
    edit: '<path d="m4 20 5-.8L20 8a2 2 0 0 0-4-4L5 15l-1 5Z"/>',
    check: '<path d="m4 12 5 5L20 6"/>',
    share: '<path d="M12 16V3m-5 5 5-5 5 5M4 14v6h16v-6"/>',
    chart: '<path d="M3 3v18h18M7 16v-5M12 16V7m5 9v-8"/>',
    menu: '<path d="M4 7h16M4 12h16M4 17h16"/>',
    bolt: '<path d="m13 2-9 11h7l-1 9 10-12h-7l1-8Z"/>',
    coin: '<circle cx="12" cy="12" r="9"/><path d="M15.5 8.5h-5a2.5 2.5 0 0 0 0 5h3a2.5 2.5 0 0 1 0 5h-5M12 6v12"/>',
    file: '<path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9l-7-7Zm0 0v7h7"/>',
    alert: '<path d="m12 3 10 18H2L12 3Zm0 7v4m0 3h.01"/>',
    copy: '<rect x="8" y="8" width="13" height="13" rx="2"/><path d="M16 8V5a2 2 0 0 0-2-2H5a2 2 0 0 0-2 2v9a2 2 0 0 0 2 2h3"/>',
    back: '<path d="m15 18-6-6 6-6"/>'
  };
  const icon = (name) =>
    `<svg class="ic" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${ICON[name] || ICON.info}</svg>`;

  const STORES = [
    { id: 'centro', name: 'Centro', short: 'Centro' },
    { id: 'vila', name: 'Vila Elias', short: 'Vila Elias' }
  ];
  const money = (n) =>
    new Intl.NumberFormat('pt-BR', {
      style: 'currency',
      currency: 'BRL',
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    }).format(Number(n) || 0);
  const integer = (n) => new Intl.NumberFormat('pt-BR', { maximumFractionDigits: 0 }).format(n || 0);
  const pct = (n) =>
    new Intl.NumberFormat('pt-BR', { maximumFractionDigits: 1, minimumFractionDigits: 1 }).format(
      Number(n) || 0
    ) + '%';
  const dayKey = (d) =>
    [d.getFullYear(), String(d.getMonth() + 1).padStart(2, '0'), String(d.getDate()).padStart(2, '0')].join(
      '-'
    );
  const parseDay = (s) => {
    const [y, m, d] = s.split('-').map(Number);
    return new Date(y, m - 1, d);
  };
  const dateLabel = (s, opts = { day: '2-digit', month: 'short' }) =>
    parseDay(s).toLocaleDateString('pt-BR', opts).replace('.', '');
  const cloneDate = (d) => new Date(d.getFullYear(), d.getMonth(), d.getDate());
  const today = cloneDate(new Date());
  const todayKey = dayKey(today);
  const addDays = (d, amount) => {
    const x = cloneDate(d);
    x.setDate(x.getDate() + amount);
    return x;
  };
  const round = (n) => Math.round(n * 100) / 100;
  const sum = (list, key) =>
    round(
      list.reduce((t, x) => t + (typeof key === 'function' ? key(x) : Number(x[key] || 0)), 0)
    );
  const esc = (s) =>
    String(s).replace(/[&<>"']/g, (x) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[x]));

  const persisted = (() => {
    try {
      return JSON.parse(localStorage.getItem('gm_mobile_dash_v1')) || {};
    } catch {
      return {};
    }
  })();

  const state = {
    view: 'overview',
    store: persisted.store || 'all',
    period: persisted.period || '30',
    metric: 'revenue',
    selectedBucket: -1,
    customStart: persisted.customStart || '',
    customEnd: persisted.customEnd || '',
    hidden: false,
    dark: !!persisted.dark,
    loading: true,
    error: '',
    live: null,
    fetchSeq: 0
  };

  let abortCtrl = null;
  let refreshTimer = null;

  const save = () => {
    try {
      localStorage.setItem(
        'gm_mobile_dash_v1',
        JSON.stringify({
          store: state.store,
          period: state.period,
          customStart: state.customStart,
          customEnd: state.customEnd,
          dark: state.dark
        })
      );
    } catch (_) {}
  };

  const padStore = (id) =>
    id === 'all' ? 'As duas lojas' : id === 'centro' ? 'Loja Centro' : 'Vila Elias';

  function range() {
    if (state.period === 'custom' && state.customStart && state.customEnd) {
      return { start: state.customStart, end: state.customEnd };
    }
    const days = Number(state.period) || 30;
    return { start: dayKey(addDays(today, -days + 1)), end: todayKey };
  }

  function periodText() {
    const r = range();
    return `${dateLabel(r.start, { day: '2-digit', month: 'short', year: 'numeric' })} — ${dateLabel(r.end, {
      day: '2-digit',
      month: 'short',
      year: 'numeric'
    })}`;
  }

  function lojaParam() {
    return state.store === 'all' ? 'todas' : state.store;
  }

  function num(v) {
    if (v == null || v === '') return null;
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }

  function hide(v, format = money) {
    if (v == null || Number.isNaN(Number(v))) return '—';
    return state.hidden ? 'R$ •••••' : format(v);
  }

  function title(text, subtitle = '', button = '') {
    return `<div class="section-title"><div><h2>${text}</h2>${subtitle ? `<p>${subtitle}</p>` : ''}</div>${button}</div>`;
  }

  function miniIcon(name, variant = '') {
    return `<span class="stat-icon ${variant}">${icon(name)}</span>`;
  }

  const navItems = [
    ['overview', 'dashboard', 'Resumo'],
    ['sales', 'trend', 'Vendas'],
    ['products', 'bag', 'Produtos'],
    ['finance', 'wallet', 'Financeiro'],
    ['goals', 'target', 'Metas']
  ];

  function statusNotice() {
    if (state.loading) {
      return `<div class="demo-notice">${icon('clock')} <span><b>Carregando faturamento…</b> Aguarde a resposta do servidor.</span></div>`;
    }
    if (state.error) {
      return `<div class="demo-notice" style="background:#fff0f2;border-color:#f5c2c8;color:#9f1239">${icon('alert')} <span><b>Erro ao carregar.</b> ${esc(state.error)} <button type="button" data-action="refresh" style="margin-left:8px;border:0;background:transparent;color:inherit;font-weight:850;text-decoration:underline">Tentar de novo</button></span></div>`;
    }
    const live = state.live;
    if (!live) {
      return `<div class="demo-notice">${icon('info')} <span>Sem dados carregados.</span></div>`;
    }
    if (live.sem_vendas) {
      return `<div class="demo-notice">${icon('info')} <span><b>Sem vendas no período</b> · ${esc(periodText())}. Não há registros neste ambiente para os filtros atuais.</span></div>`;
    }
    const hora = live.atualizado_hora || '—';
    return `<div class="demo-notice" style="background:#edf8f3;border-color:#b7e7d7;color:#087b61">${icon('check')} <span><b>Dados reais (PostgreSQL).</b> Faturamento PDV · Atualizado às ${esc(hora)}${OPERADOR ? ' · ' + esc(OPERADOR) : ''}</span></div>`;
  }

  function buildTop() {
    const now = new Date();
    const name = now.getHours() < 12 ? 'Bom dia' : now.getHours() < 18 ? 'Boa tarde' : 'Boa noite';
    return `<header class="topbar"><div class="brand"><div class="brand-mark">GM</div><div><div class="brand-word"><span>Sist</span><b>App</b></div><span class="brand-sub">GM AGRO MAIS · GESTÃO</span></div></div><div class="head-actions"><a class="icon-btn" href="${esc(HUB_URL)}" aria-label="Voltar ao SistApp" title="Voltar ao início">${icon('back')}</a><button class="icon-btn" data-action="toggle-hide" aria-label="${state.hidden ? 'Mostrar' : 'Ocultar'} valores" title="Ocultar/mostrar valores">${icon(state.hidden ? 'eyeoff' : 'eye')}</button><button class="icon-btn" data-action="refresh" aria-label="Atualizar indicadores" title="Atualizar">${icon('refresh')}</button><button class="icon-btn" data-action="options" aria-label="Mais opções" title="Opções">${icon('menu')}</button></div></header><section class="intro"><div><div class="eyebrow">${name} · ${today.toLocaleDateString('pt-BR', { weekday: 'long', day: 'numeric', month: 'long' })}</div><h1>${({ overview: 'Painel gerencial', sales: 'Análise de vendas', products: 'Produtos e categorias', finance: 'Visão financeira', goals: 'Metas da operação' })[state.view]}</h1><p>${({ overview: 'Sua operação, em um só lugar.', sales: 'Explore faturamento, pagamentos e movimentações.', products: 'Descubra quais itens movimentam suas lojas.', finance: 'Acompanhe receitas, custos e resultados.', goals: 'Veja o progresso e simule novos objetivos.' })[state.view]}</p></div><span class="intro-badge">Modo gerencial</span></section><div class="controls"><div class="stores" role="group" aria-label="Filtrar por loja">${[['all', 'Consolidado'], ['centro', 'Centro'], ['vila', 'Vila Elias']].map(([id, label]) => `<button type="button" class="${state.store === id ? 'active' : ''}" data-action="store" data-id="${id}" aria-pressed="${state.store === id}">${label}</button>`).join('')}</div><div class="filters"><button class="filter-button" data-action="period-sheet">${icon('calendar')} ${state.period === 'custom' ? 'Personalizado' : state.period + ' dias'} ${icon('down')}</button></div></div>${statusNotice()}`;
  }

  function bottomNav() {
    return `<nav class="bottom-nav" aria-label="Navegação do dashboard"><div class="nav-wrap">${navItems.map(([v, ic, label]) => `<button type="button" class="nav-item ${state.view === v ? 'active' : ''}" data-action="nav" data-id="${v}" aria-current="${state.view === v ? 'page' : 'false'}">${icon(ic)}<span>${label}</span></button>`).join('')}</div></nav>`;
  }

  function statCard(key, iconName, variant, label, value, trend) {
    return `<button class="stat" data-action="stat" data-id="${key}">${miniIcon(iconName, variant)}<span class="stat-arrow">${icon('chevron')}</span><span class="stat-label">${label}</span><strong class="stat-number">${value}</strong><span class="stat-trend ${trend && String(trend)[0] === '-' ? 'negative' : ''}">${trend || 'Toque para analisar'}</span></button>`;
  }

  function emptyBlock(msg) {
    return `<div class="empty">${icon('info')}${esc(msg)}</div>`;
  }

  function liveAgg() {
    const live = state.live;
    if (!live || !live.indicadores) {
      return { revenue: null, orders: null, ticket: null, delta: null, porLoja: null, serie: [] };
    }
    const ind = live.indicadores;
    return {
      revenue: num(ind.faturamento),
      orders: ind.numero_vendas == null ? null : Number(ind.numero_vendas),
      ticket: num(ind.ticket_medio),
      delta: ind.comparativo_pct == null ? null : Number(ind.comparativo_pct),
      porLoja: live.por_loja || null,
      serie: Array.isArray(live.serie) ? live.serie : []
    };
  }

  function buckets() {
    const r = range();
    const list = [];
    const start = parseDay(r.start);
    const end = parseDay(r.end);
    const days = Math.max(1, Math.round((end - start) / 86400000) + 1);
    const grouping = days <= 9 ? 'day' : days <= 35 ? 'step' : days <= 100 ? 'week' : 'month';
    if (grouping === 'month') {
      let cursor = new Date(start.getFullYear(), start.getMonth(), 1);
      while (cursor <= end) {
        const s = cursor < start ? start : cursor;
        const next = new Date(cursor.getFullYear(), cursor.getMonth() + 1, 1);
        let e = addDays(next, -1);
        if (e > end) e = end;
        list.push({
          start: dayKey(s),
          end: dayKey(e),
          label: cursor.toLocaleDateString('pt-BR', { month: 'short' }).replace('.', '')
        });
        cursor = next;
      }
    } else {
      const step = grouping === 'day' ? 1 : grouping === 'step' ? 3 : 7;
      let cursor = start;
      while (cursor <= end) {
        let next = addDays(cursor, step - 1);
        if (next > end) next = end;
        list.push({
          start: dayKey(cursor),
          end: dayKey(next),
          label:
            grouping === 'day'
              ? dateLabel(dayKey(cursor), { day: '2-digit' })
              : dateLabel(dayKey(cursor), { day: '2-digit', month: 'short' })
        });
        cursor = addDays(next, 1);
      }
    }
    const serie = liveAgg().serie;
    const byDay = {};
    for (const p of serie) {
      byDay[p.data] = {
        revenue: num(p.faturamento) || 0,
        orders: Number(p.numero_vendas || 0)
      };
    }
    return list.map((b) => {
      let revenue = 0;
      let orders = 0;
      let cur = parseDay(b.start);
      const last = parseDay(b.end);
      while (cur <= last) {
        const k = dayKey(cur);
        const row = byDay[k];
        if (row) {
          revenue = round(revenue + row.revenue);
          orders += row.orders;
        }
        cur = addDays(cur, 1);
      }
      return { ...b, totals: { revenue, orders } };
    });
  }

  function barChart() {
    const a = liveAgg();
    if (state.loading) return emptyBlock('Carregando gráfico…');
    if (state.error) return emptyBlock('Gráfico indisponível até carregar os dados.');
    if (!state.live) return emptyBlock('Sem dados neste ambiente');
    const bs = buckets();
    const metric = state.metric;
    const values = bs.map((b) => b.totals[metric === 'orders' ? 'orders' : 'revenue'] || 0);
    const max = Math.max(1, ...values);
    let active =
      state.selectedBucket >= 0 && state.selectedBucket < bs.length ? state.selectedBucket : bs.length - 1;
    const current = bs[active] || { totals: { revenue: 0, orders: 0 }, start: todayKey, end: todayKey };
    const headVal =
      metric === 'orders' ? integer(sum(bs, (x) => x.totals.orders)) : hide(sum(bs, (x) => x.totals.revenue));
    return `<div class="chart-head"><div><strong>${headVal}</strong><small style="display:block;margin-top:3px">${metric === 'orders' ? 'vendas no período' : 'faturamento no período'}</small></div><div class="segmented"><button data-action="metric" data-id="revenue" class="${metric === 'revenue' ? 'active' : ''}">R$</button><button data-action="metric" data-id="orders" class="${metric === 'orders' ? 'active' : ''}">Vendas</button></div></div><div class="bars-chart"><div class="chart-grid"><i></i><i></i><i></i><i></i></div><div class="bar-list" role="group" aria-label="Gráfico interativo de vendas">${bs
      .map(
        (b, i) =>
          `<button class="bar-hit ${active === i ? 'active' : ''}" data-action="bucket" data-id="${i}" aria-label="${esc(
            b.start === b.end ? dateLabel(b.start) : dateLabel(b.start) + ' a ' + dateLabel(b.end)
          )}: ${metric === 'orders' ? integer(b.totals.orders) + ' vendas' : money(b.totals.revenue)}" aria-pressed="${active === i}"><div class="bar-track"><div class="bar-fill" style="height:${Math.max(2, (values[i] / max) * 100).toFixed(2)}%"></div></div><small>${esc(b.label)}</small></button>`
      )
      .join('')}</div></div><div class="chart-selection"><div><strong>${
      current.start === current.end
        ? dateLabel(current.start, { day: '2-digit', month: 'long' })
        : dateLabel(current.start) + ' até ' + dateLabel(current.end)
    }</strong><small>${
      metric === 'orders' ? integer(current.totals.orders) + ' vendas' : hide(current.totals.revenue)
    } · ${padStore(state.store)}</small></div><button data-action="bucket-detail" data-id="${active}">Detalhar ${icon('chevron')}</button></div>`;
  }

  function storeCompare() {
    const a = liveAgg();
    if (!a.porLoja) return `<div class="card">${title('Comparativo das lojas')}${emptyBlock(state.loading ? 'Carregando…' : 'Ainda não integrado')}</div>`;
    const bs = [
      { id: 'centro', name: 'Centro', revenue: num(a.porLoja.centro) || 0 },
      { id: 'vila', name: 'Vila Elias', revenue: num(a.porLoja.vila) || 0 }
    ];
    const total = sum(bs, 'revenue') || 1;
    return `<div class="card">${title('Comparativo das lojas', 'Toque em uma loja para aplicar o filtro')}<div class="comparison">${bs
      .map(
        (s, i) =>
          `<div><button class="store-row" data-action="store" data-id="${s.id}"><h3><span class="store-dot ${i ? 'orange' : ''}"></span>${s.name}</h3><strong>${hide(s.revenue)}</strong></button><div class="progress-wrap"><div class="progress ${i ? 'orange' : ''}"><div style="width:${Math.max(0, (s.revenue / total) * 100)}%"></div></div><div class="progress-caption"><span>${pct((s.revenue / total) * 100)} do total</span><span>Faturamento PDV</span></div></div></div>`
      )
      .join('')}</div></div>`;
  }

  function notIntegratedCard(heading, sub) {
    return `<div class="card">${title(heading, sub)}${emptyBlock('Ainda não integrado')}</div>`;
  }

  function overview() {
    const a = liveAgg();
    const delta = a.delta;
    const ordersLbl = a.orders == null ? '—' : integer(a.orders);
    const ticketLbl = a.ticket == null ? '—' : hide(a.ticket);
    const revLbl = state.loading ? '…' : a.revenue == null && state.error ? '—' : hide(a.revenue);
    return `<div class="sections screen-enter"><div class="overview-grid"><div class="sections"><div class="hero"><div class="hero-top"><span class="hero-label">${icon('trend')} Faturamento do período</span><button class="hero-arrow" data-action="stat" data-id="revenue" aria-label="Detalhar faturamento">${icon('arrow')}</button></div><div class="hero-value">${revLbl}</div><div class="hero-foot"><span class="change">${icon(delta !== null && delta < 0 ? 'down' : 'up')} ${delta === null ? '—' : pct(Math.abs(delta))}</span><span>vs. período anterior</span></div><div class="hero-bottom"><div><label>Vendas registradas</label><strong>${ordersLbl}</strong></div><div><label>Ticket médio</label><strong>${ticketLbl}</strong></div></div></div><div class="stat-grid">${statCard('orders', 'receipt', 'orange', 'Total de vendas', ordersLbl, a.orders == null ? 'Ainda não integrado' : 'Cupons no período')}${statCard('ticket', 'wallet', 'blue', 'Ticket médio', ticketLbl, a.ticket == null ? 'Ainda não integrado' : 'Faturamento ÷ vendas')}${statCard('margin', 'trend', '', 'Margem bruta', '—', 'Ainda não integrado')}${statCard('goal', 'target', 'purple', 'Meta mensal', '—', 'Ainda não integrado')}</div></div><div class="sections"><div class="card">${title('Evolução das vendas', 'Toque nas barras para analisar cada período')}${barChart()}</div><div class="card">${title('Meta deste mês', 'Etapa 1')}${emptyBlock('Ainda não integrado')}</div></div></div><div class="desktop-double">${storeCompare()}${notIntegratedCard('Categorias em destaque', 'Participação no faturamento')}</div>${notIntegratedCard('Produtos que mais vendem', 'Ranking do período')}<div class="button-row"><button class="outline-btn" data-action="export">${icon('download')} Exportar CSV</button><button class="outline-btn" data-action="share">${icon('share')} Compartilhar resumo</button><a class="primary-btn" href="${esc(HUB_URL)}" style="text-decoration:none">${icon('back')} Voltar ao SistApp</a></div><p class="footnote">Faturamento = VendaAgro (PostgreSQL) com abatimento de devoluções no dia do evento — mesma regra do painel Vendas. Produtos, financeiro e metas: próximas etapas.</p></div>`;
  }

  function sales() {
    const a = liveAgg();
    return `<div class="sections screen-enter"><div class="stat-grid">${statCard('revenue', 'trend', '', 'Faturamento', state.loading ? '…' : hide(a.revenue), 'Receita PDV do período')}${statCard('orders', 'receipt', 'orange', 'Quantidade', a.orders == null ? '—' : integer(a.orders), 'Vendas no período')}${statCard('ticket', 'wallet', 'blue', 'Ticket médio', a.ticket == null ? '—' : hide(a.ticket), 'Valor médio')}${statCard('margin', 'chart', 'purple', 'Margem bruta', '—', 'Ainda não integrado')}</div><div class="card">${title('Evolução por período', 'Toque em uma barra para explorar os detalhes')}${barChart()}</div>${notIntegratedCard('Formas de pagamento', 'Disponível na Etapa 2')}${notIntegratedCard('Movimentações recentes', 'Vendas individuais na Etapa 2')}<div class="note">Nesta etapa só o faturamento consolidado e a série diária estão conectados ao PostgreSQL.</div></div>`;
  }

  function products() {
    return `<div class="sections screen-enter"><div class="stat-grid">${statCard('products', 'bag', 'orange', 'Produtos', '—', 'Ainda não integrado')}${statCard('revenue', 'trend', '', 'Faturamento', state.loading ? '…' : hide(liveAgg().revenue), 'Período selecionado')}</div>${notIntegratedCard('Categorias', 'Etapa 2')}${emptyBlock('Ainda não integrado — ranking de produtos virá na Etapa 2.')}<div class="note">Nenhum dado demonstrativo é exibido nesta tela.</div></div>`;
  }

  function finance() {
    return `<div class="sections screen-enter"><div class="finance-hero"><label>Resultado operacional</label><strong>Sem permissão</strong><small>Margens, CMV, despesas e lucro não estão liberados nesta etapa. Política administrativa pendente.</small></div>${emptyBlock('Sem permissão / Ainda não integrado')}<div class="note">Aba Financeiro permanece indisponível para perfis comuns até definição explícita de papéis. Faturamento bruto do período está na aba Resumo/Vendas.</div></div>`;
  }

  function goals() {
    return `<div class="sections screen-enter"><div class="goal-card"><p>${icon('target')} Meta mensal · ${padStore(state.store)}</p><h2>—</h2><span class="muted" style="font-size:12px">Metas do servidor ainda não integradas</span><div class="progress"><div style="width:0%"></div></div><div class="goal-progress"><span>Ainda não integrado</span><strong>—</strong></div></div>${emptyBlock('Ainda não integrado')}<div class="note">Esta etapa não grava nem lê metas financeiras. Evite confundir com dados simulados do protótipo original.</div></div>`;
  }

  function render(keepPosition = false) {
    document.body.classList.toggle('dark', state.dark);
    const priorY = window.scrollY;
    document.getElementById('app').innerHTML =
      buildTop() +
      (state.view === 'overview'
        ? overview()
        : state.view === 'sales'
          ? sales()
          : state.view === 'products'
            ? products()
            : state.view === 'finance'
              ? finance()
              : goals()) +
      bottomNav();
    if (!keepPosition) window.scrollTo({ top: 0, behavior: 'instant' });
    else window.scrollTo({ top: priorY, behavior: 'instant' });
    save();
  }

  function showSheet(titleText, content) {
    const portal = document.getElementById('portal');
    portal.innerHTML = `<div class="overlay" data-action="overlay-close"><div class="sheet" role="dialog" aria-modal="true" aria-label="${esc(titleText)}"><div class="sheet-head"><div class="handle"></div><div class="sheet-title"><h2>${esc(titleText)}</h2><button class="sheet-close" data-action="close-sheet" aria-label="Fechar">${icon('close')}</button></div></div><div class="sheet-body">${content}</div></div></div>`;
    document.body.style.overflow = 'hidden';
  }

  function closeSheet() {
    document.getElementById('portal').innerHTML = '';
    document.body.style.overflow = '';
  }

  let toastTimer;
  function toast(s) {
    document.querySelector('.toast')?.remove();
    const t = document.createElement('div');
    t.className = 'toast';
    t.setAttribute('role', 'status');
    t.textContent = s;
    document.body.appendChild(t);
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => t.remove(), 3000);
  }

  function detailGrid(vals) {
    return `<div class="detail-grid">${vals.map(([title, value]) => `<div><label>${esc(title)}</label><strong>${value}</strong></div>`).join('')}</div>`;
  }

  function statDetail(id) {
    const a = liveAgg();
    const desc = {
      revenue: ['Faturamento do período', hide(a.revenue), 'Soma PDV (VendaAgro) com abatimento de devoluções no dia do evento.'],
      orders: ['Quantidade de vendas', a.orders == null ? '—' : integer(a.orders), 'Cupons com criado_em no intervalo.'],
      ticket: ['Ticket médio', a.ticket == null ? '—' : hide(a.ticket), 'Faturamento dividido pelo número de vendas.'],
      margin: ['Margem bruta', '—', 'Ainda não integrado nesta etapa.'],
      goal: ['Meta mensal', '—', 'Ainda não integrado nesta etapa.'],
      products: ['Produtos', '—', 'Ainda não integrado nesta etapa.']
    };
    const [heading, value, text] = desc[id] || desc.revenue;
    showSheet(
      heading,
      `<div class="detail-amount">${value}</div><p class="detail-sub">${text}</p>${detailGrid([
        ['Loja', padStore(state.store)],
        ['Período', periodText()],
        ['Vendas', a.orders == null ? '—' : integer(a.orders)],
        ['Ticket médio', a.ticket == null ? '—' : hide(a.ticket)]
      ])}`
    );
  }

  function bucketDetail(i) {
    const b = buckets()[i];
    if (!b) return;
    const a = b.totals;
    showSheet(
      'Detalhes do período',
      `<p class="detail-sub">${dateLabel(b.start, { day: '2-digit', month: 'long', year: 'numeric' })} até ${dateLabel(b.end, {
        day: '2-digit',
        month: 'long',
        year: 'numeric'
      })} · ${padStore(state.store)}</p><div class="detail-amount">${hide(a.revenue)}</div>${detailGrid([
        ['Vendas', integer(a.orders)],
        ['Ticket médio', a.orders ? hide(a.revenue / a.orders) : '—']
      ])}<div class="note">Categorias e demais detalhamentos virão na Etapa 2.</div>`
    );
  }

  function periodSheet() {
    showSheet(
      'Selecionar período',
      `<p class="detail-sub" style="margin-bottom:15px">Escolha um intervalo para atualizar todas as telas.</p><div class="pill-row" style="flex-wrap:wrap">${[
        ['7', '7 dias'],
        ['30', '30 dias'],
        ['90', '90 dias'],
        ['365', '12 meses']
      ]
        .map(
          ([v, l]) =>
            `<button class="pill ${state.period === v ? 'active' : ''}" data-action="period" data-id="${v}">${l}</button>`
        )
        .join('')}</div><div style="margin:19px 0 15px;border-top:1px solid var(--line)"></div><form id="custom-period-form" class="form-stack"><div class="field-grid"><label><span class="field-label">Data inicial</span><input class="field-input" type="date" name="start" required max="${todayKey}" value="${state.customStart || dayKey(addDays(today, -29))}"></label><label><span class="field-label">Data final</span><input class="field-input" type="date" name="end" required max="${todayKey}" value="${state.customEnd || todayKey}"></label></div><button type="submit" class="primary-btn">${icon('calendar')} Aplicar datas</button></form>`
    );
  }

  function options() {
    showSheet(
      'Opções do painel',
      `<div class="form-stack"><button class="outline-btn" style="justify-content:flex-start" data-action="toggle-theme">${icon(state.dark ? 'sun' : 'moon')} ${state.dark ? 'Usar tema claro' : 'Usar tema escuro'}</button><button class="outline-btn" style="justify-content:flex-start" data-action="toggle-hide">${icon(state.hidden ? 'eyeoff' : 'eye')} ${state.hidden ? 'Mostrar valores' : 'Ocultar valores'}</button><a class="outline-btn" style="justify-content:flex-start;text-decoration:none" href="${esc(HUB_URL)}">${icon('back')} Voltar ao SistApp</a></div><p class="detail-sub" style="margin-top:23px">SistApp Dashboard · Etapa 1 · faturamento PostgreSQL somente leitura.</p>`
    );
  }

  async function share() {
    const a = liveAgg();
    if (a.revenue == null) {
      toast('Sem dados reais para compartilhar');
      return;
    }
    const txt = `SistApp · faturamento real\n${padStore(state.store)} · ${periodText()}\nFaturamento: ${money(a.revenue)}\nVendas: ${a.orders ?? '—'}\nTicket médio: ${a.ticket == null ? '—' : money(a.ticket)}`;
    try {
      if (navigator.share) await navigator.share({ title: 'SistApp - resumo', text: txt });
      else if (navigator.clipboard) {
        await navigator.clipboard.writeText(txt);
        toast('Resumo copiado');
      } else toast('Compartilhamento indisponível neste navegador');
    } catch (e) {
      if (e.name !== 'AbortError') toast('Não foi possível compartilhar');
    }
  }

  function navigate(v) {
    state.view = v;
    state.selectedBucket = -1;
    closeSheet();
    render();
  }

  async function loadLive(opts = {}) {
    const silent = !!opts.silent;
    const r = range();
    const seq = ++state.fetchSeq;
    if (abortCtrl) abortCtrl.abort();
    abortCtrl = new AbortController();
    if (!silent) {
      state.loading = true;
      state.error = '';
      render(true);
    }
    try {
      const url = new URL(API_URL, window.location.origin);
      url.searchParams.set('loja', lojaParam());
      url.searchParams.set('inicio', r.start);
      url.searchParams.set('fim', r.end);
      const res = await fetch(url.toString(), {
        credentials: 'same-origin',
        headers: { Accept: 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
        signal: abortCtrl.signal
      });
      const data = await res.json().catch(() => ({}));
      if (seq !== state.fetchSeq) return;
      if (res.status === 401 || data.precisa_pin) {
        window.location.href = `/vendas/lojas/pin/?next=${encodeURIComponent(window.location.pathname)}`;
        return;
      }
      if (res.status === 403 || data.sem_acesso_gerencia) {
        state.loading = false;
        state.live = null;
        state.error = data.erro || 'Sem permissão';
        render(true);
        return;
      }
      if (!res.ok || !data.ok) {
        state.loading = false;
        state.live = null;
        state.error = data.erro || 'Falha ao carregar o resumo';
        render(true);
        return;
      }
      state.live = data;
      state.loading = false;
      state.error = '';
      render(true);
      if (!silent) toast(`Atualizado às ${data.atualizado_hora || '—'}`);
    } catch (e) {
      if (e.name === 'AbortError') return;
      if (seq !== state.fetchSeq) return;
      state.loading = false;
      state.live = null;
      state.error = 'Servidor indisponível. Tente novamente.';
      render(true);
    }
  }

  function scheduleRefresh() {
    clearInterval(refreshTimer);
    refreshTimer = setInterval(() => {
      if (document.visibilityState === 'visible') loadLive({ silent: true });
    }, 60000);
  }

  document.addEventListener('click', (e) => {
    const el = e.target.closest('[data-action]');
    if (!el) return;
    const action = el.dataset.action;
    const id = el.dataset.id;
    if (action === 'overlay-close') {
      if (e.target === el) closeSheet();
      return;
    }
    if (action === 'close-sheet') {
      closeSheet();
      return;
    }
    if (action === 'nav') {
      navigate(id);
      return;
    }
    if (action === 'store') {
      state.store = id;
      state.selectedBucket = -1;
      loadLive();
      return;
    }
    if (action === 'period-sheet') {
      periodSheet();
      return;
    }
    if (action === 'period') {
      state.period = id;
      state.selectedBucket = -1;
      closeSheet();
      loadLive();
      return;
    }
    if (action === 'metric') {
      state.metric = id;
      render(true);
      return;
    }
    if (action === 'bucket') {
      state.selectedBucket = Number(id);
      render(true);
      return;
    }
    if (action === 'bucket-detail') {
      bucketDetail(Number(id));
      return;
    }
    if (action === 'stat') {
      statDetail(id);
      return;
    }
    if (action === 'refresh') {
      loadLive();
      return;
    }
    if (action === 'toggle-hide') {
      state.hidden = !state.hidden;
      closeSheet();
      render(true);
      return;
    }
    if (action === 'toggle-theme') {
      state.dark = !state.dark;
      closeSheet();
      render(true);
      return;
    }
    if (action === 'options') {
      options();
      return;
    }
    if (action === 'export') {
      toast('Exportação bloqueada nesta etapa (sem CSV de simulação)');
      return;
    }
    if (action === 'share') {
      share();
      return;
    }
  });

  document.addEventListener('submit', (e) => {
    if (e.target.id === 'custom-period-form') {
      e.preventDefault();
      const data = new FormData(e.target);
      const s = data.get('start');
      const end = data.get('end');
      if (s > end) {
        toast('Data inicial deve ser anterior à final');
        return;
      }
      if (end > todayKey) {
        toast('Data final não pode ser futura');
        return;
      }
      const days = Math.round((parseDay(end) - parseDay(s)) / 86400000) + 1;
      if (days > 366) {
        toast('Intervalo máximo de 366 dias');
        return;
      }
      state.customStart = s;
      state.customEnd = end;
      state.period = 'custom';
      state.selectedBucket = -1;
      closeSheet();
      loadLive();
    }
  });

  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') closeSheet();
  });

  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible') loadLive({ silent: true });
  });

  render();
  loadLive();
  scheduleRefresh();
})();
