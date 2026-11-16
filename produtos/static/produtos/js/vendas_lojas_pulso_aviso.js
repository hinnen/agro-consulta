/**
 * Aviso GM Vendas no pulso (notificação do celular).
 * Só enquanto o dashboard do SistApp está aberto e visível.
 * Usa a sessão já autenticada. Não guarda PIN, senha nem token.
 */
(function (root, factory) {
  'use strict';
  const api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root) root.AgroPulsoAviso = api;
})(typeof window !== 'undefined' ? window : globalThis, function () {
  'use strict';

  const INTERVALO_MS = 5 * 60 * 1000;
  const TAG = 'gm-vendas-hoje';
  const SESSAO = 'agro_pulso_aviso';

  function reais(raw) {
    if (raw == null || raw === '') return '—';
    const n = Number(String(raw).replace(/\s/g, '').replace(',', '.'));
    if (!Number.isFinite(n)) return '—';
    return n.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
  }

  function horaDe(payload) {
    const direta = payload && payload.atualizado_hora;
    if (direta) return String(direta);
    const iso = payload && payload.atualizado_em;
    if (!iso) return '—';
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return '—';
    return d.toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' });
  }

  function textoAviso(payload, stale) {
    const lojas = (payload && payload.por_loja) || {};
    const linhas = [
      'Hoje ' + reais(lojas.total),
      'Centro ' + reais(lojas.centro),
      'Vila Elias ' + reais(lojas.vila),
      horaDe(payload)
    ];
    if (stale) linhas.unshift('Último valor · sem conexão');
    return linhas.join('\n');
  }

  function urlHoje(apiUrl, hojeIso) {
    const base = typeof location !== 'undefined' ? location.origin : 'http://local';
    const url = new URL(apiUrl, base);
    url.searchParams.set('loja', 'todas');
    url.searchParams.set('inicio', hojeIso);
    url.searchParams.set('fim', hojeIso);
    return url.toString();
  }

  function hojeLocalIso(agora) {
    const d = agora || new Date();
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return y + '-' + m + '-' + day;
  }

  function criarControlador(deps) {
    let timer = null;
    let ultimo = null;
    let ligado = false;

    function parar() {
      if (timer) clearInterval(timer);
      timer = null;
    }

    async function tick() {
      if (!ligado || !deps.ativo()) return;
      try {
        const data = await deps.buscar();
        if (!data || data.ok === false) throw new Error('resumo');
        ultimo = data;
        deps.avisar(textoAviso(data, false));
      } catch (err) {
        if (ultimo) deps.avisar(textoAviso(ultimo, true));
        else deps.avisar('Sem conexão. Ainda não há valor de hoje.');
      }
    }

    function iniciar() {
      parar();
      if (!ligado || !deps.ativo()) return;
      timer = setInterval(tick, INTERVALO_MS);
    }

    return {
      tick: tick,
      iniciar: iniciar,
      parar: parar,
      estaLigado: function () { return ligado; },
      ligar: function () { ligado = true; },
      desligar: function () {
        ligado = false;
        parar();
      }
    };
  }

  function alternar(opts) {
    opts = opts || {};
    const tela = typeof opts.avisarTela === 'function' ? opts.avisarTela : function () {};
    if (typeof Notification === 'undefined') {
      tela('Este celular não mostra aviso na pulseira');
      return;
    }
    if (Notification.permission === 'denied') {
      tela('O celular bloqueou o aviso. Libere nas configurações do navegador.');
      return;
    }
    const pedir = Notification.permission === 'granted'
      ? Promise.resolve('granted')
      : Notification.requestPermission();
    Promise.resolve(pedir).then(function (perm) {
      if (perm !== 'granted') {
        tela('Sem permissão, o pulso não recebe o aviso');
        return;
      }
      try { sessionStorage.setItem(SESSAO, '1'); } catch (e) { /* segue na memória */ }
      garantir(opts.apiUrl, tela);
      tela('Aviso ligado. Só atualiza com esta tela aberta, a cada 5 minutos.');
    });
  }

  let controlador = null;

  function garantir(apiUrl, tela) {
    if (controlador) {
      controlador.ligar();
      controlador.iniciar();
      controlador.tick();
      return;
    }
    controlador = criarControlador({
      ativo: function () {
        return document.visibilityState === 'visible';
      },
      buscar: function () {
        return fetch(urlHoje(apiUrl, hojeLocalIso(new Date())), {
          credentials: 'same-origin',
          headers: { Accept: 'application/json', 'X-Requested-With': 'XMLHttpRequest' }
        }).then(function (res) {
          if (res.status === 401 || res.status === 403) {
            try { sessionStorage.removeItem(SESSAO); } catch (e) { /* ok */ }
            if (controlador) controlador.desligar();
            tela('Entre de novo com o PIN para avisar o pulso');
            throw new Error('auth');
          }
          return res.json();
        });
      },
      avisar: function (texto) {
        try {
          const n = new Notification('GM Vendas', { body: texto, tag: TAG, renotify: true });
          n.onclick = function () { window.focus(); };
        } catch (e) {
          tela('Não foi possível mostrar o aviso');
        }
      }
    });
    controlador.ligar();
    controlador.iniciar();
    controlador.tick();
    if (!window.__agroPulsoVis) {
      window.__agroPulsoVis = true;
      document.addEventListener('visibilitychange', function () {
        if (!controlador || !controlador.estaLigado()) return;
        if (document.visibilityState === 'visible') {
          controlador.iniciar();
          controlador.tick();
        } else {
          controlador.parar();
        }
      });
    }
  }

  function retomarSePedido(apiUrl) {
    let pedido = false;
    try { pedido = sessionStorage.getItem(SESSAO) === '1'; } catch (e) { pedido = false; }
    if (!pedido || typeof Notification === 'undefined' || Notification.permission !== 'granted') return;
    garantir(apiUrl, function () {});
  }

  if (typeof document !== 'undefined') {
    document.addEventListener('DOMContentLoaded', function () {
      const cfg = window.SISTAPP_DASHBOARD_CFG || {};
      if (cfg.apiResumoUrl) retomarSePedido(cfg.apiResumoUrl);
    });
  }

  return {
    INTERVALO_MS: INTERVALO_MS,
    TAG: TAG,
    textoAviso: textoAviso,
    urlHoje: urlHoje,
    hojeLocalIso: hojeLocalIso,
    criarControlador: criarControlador,
    alternar: alternar
  };
});
