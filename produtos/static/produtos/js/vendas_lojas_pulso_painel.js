/**
 * Aviso de vendas de hoje no pulso, a partir do painel que o celular já abre.
 * Só com esta tela aberta. Usa o PIN já feito. Não guarda senha.
 */
(function (root, factory) {
  'use strict';
  const api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root) root.AgroPulsoPainel = api;
})(typeof window !== 'undefined' ? window : globalThis, function () {
  'use strict';

  const INTERVALO_MS = 5 * 60 * 1000;
  const TAG = 'gm-vendas-hoje';
  const SESSAO = 'agro_pulso_painel';

  function campo(html, titulo) {
    const re = new RegExp(
      'data-titulo="' + titulo + '"[\\s\\S]{0,120}?data-vendido="([^"]*)"',
      'i'
    );
    const m = String(html || '').match(re);
    return m ? m[1] : '';
  }

  function lerTotais(html) {
    const centro = campo(html, 'Centro');
    const vila = campo(html, 'Vila Elias');
    const total = campo(html, 'Total das duas lojas');
    if (!centro || !vila || !total) return null;
    return { centro: centro, vila: vila, total: total };
  }

  function horaAgora(d) {
    const dt = d || new Date();
    const h = String(dt.getHours()).padStart(2, '0');
    const m = String(dt.getMinutes()).padStart(2, '0');
    return h + ':' + m;
  }

  function textoAviso(totais, hora, stale) {
    const linhas = [
      'Hoje R$ ' + (totais.total || '—'),
      'Centro R$ ' + (totais.centro || '—'),
      'Vila Elias R$ ' + (totais.vila || '—'),
      hora || '—'
    ];
    if (stale) linhas.unshift('Último valor · sem conexão');
    return linhas.join('\n');
  }

  function urlHoje(origem) {
    return origem.replace(/\/$/, '') + '/vendas/lojas/painel/?periodo=hoje';
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
        const html = await deps.buscar();
        const totais = lerTotais(html);
        if (!totais) throw new Error('painel');
        ultimo = { totais: totais, hora: deps.hora() };
        deps.avisar(textoAviso(ultimo.totais, ultimo.hora, false));
      } catch (err) {
        if (ultimo) deps.avisar(textoAviso(ultimo.totais, ultimo.hora, true));
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

  let controlador = null;

  function avisarCelular(texto) {
    const n = new Notification('GM Vendas', { body: texto, tag: TAG, renotify: true });
    n.onclick = function () { window.focus(); };
  }

  function garantir(tela) {
    if (controlador) {
      controlador.ligar();
      controlador.iniciar();
      controlador.tick();
      return;
    }
    controlador = criarControlador({
      ativo: function () { return document.visibilityState === 'visible'; },
      hora: horaAgora,
      buscar: function () {
        return fetch(urlHoje(location.origin), {
          credentials: 'same-origin',
          headers: { Accept: 'text/html', 'X-Requested-With': 'XMLHttpRequest' }
        }).then(function (res) {
          if (res.status === 401 || res.status === 403 || res.redirected) {
            try { sessionStorage.removeItem(SESSAO); } catch (e) { /* ok */ }
            if (controlador) controlador.desligar();
            tela('Entre de novo com o PIN para avisar o pulso');
            throw new Error('auth');
          }
          return res.text();
        });
      },
      avisar: function (texto) {
        try { avisarCelular(texto); }
        catch (e) { tela('Não foi possível mostrar o aviso'); }
      }
    });
    controlador.ligar();
    controlador.iniciar();
    controlador.tick();
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
      try { sessionStorage.setItem(SESSAO, '1'); } catch (e) { /* ok */ }
      garantir(tela);
      tela('Aviso ligado. Só atualiza com esta tela aberta, a cada 5 minutos.');
    });
  }

  function retomar(tela) {
    let pedido = false;
    try { pedido = sessionStorage.getItem(SESSAO) === '1'; } catch (e) { pedido = false; }
    if (!pedido || typeof Notification === 'undefined' || Notification.permission !== 'granted') return;
    garantir(tela || function () {});
  }

  return {
    INTERVALO_MS: INTERVALO_MS,
    lerTotais: lerTotais,
    textoAviso: textoAviso,
    urlHoje: urlHoje,
    criarControlador: criarControlador,
    alternar: alternar,
    retomar: retomar
  };
});
