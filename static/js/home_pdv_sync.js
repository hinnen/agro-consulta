/**
 * Home administrativa — sincroniza catálogo/saldos no localStorage (mesmo cache do PDV)
 * sem navegar para a consulta. Termômetros + “última leitura” iguais ao PDV.
 */
(function () {
  'use strict';

  var PDV_CACHE_KEY = 'agro_pdv_catalog_cache_v2';
  var LS_SALDOS_FRESH = 'agro_pdv_saldos_fresh_at';

  function readBootstrap() {
    var el = document.getElementById('home-pdv-bootstrap');
    if (!el || !el.textContent) return { urls: {} };
    try {
      return JSON.parse(el.textContent);
    } catch (e) {
      return { urls: {} };
    }
  }

  function homeLerCacheBruto() {
    try {
      var raw = localStorage.getItem(PDV_CACHE_KEY);
      if (!raw) return { produtos: [], catalog_version: '', catalog_updated_at: '' };
      var p = JSON.parse(raw);
      return {
        produtos: Array.isArray(p.produtos) ? p.produtos : [],
        catalog_version: p.catalog_version || '',
        catalog_updated_at: p.catalog_updated_at || '',
      };
    } catch (e) {
      return { produtos: [], catalog_version: '', catalog_updated_at: '' };
    }
  }

  function homeSalvarCache(produtos, version, updatedAt) {
    try {
      localStorage.setItem(
        PDV_CACHE_KEY,
        JSON.stringify({
          saved_at: Date.now(),
          catalog_version: version || '',
          catalog_updated_at: updatedAt || '',
          produtos: Array.isArray(produtos) ? produtos : [],
        }),
      );
    } catch (e) {}
  }

  /** Atualiza só saved_at (delta unchanged — usuário ainda “sincronizou”). */
  function homeTouchCatalogSavedAt() {
    var raw = localStorage.getItem(PDV_CACHE_KEY);
    if (!raw) return;
    try {
      var p = JSON.parse(raw);
      if (!p || !Array.isArray(p.produtos)) return;
      p.saved_at = Date.now();
      localStorage.setItem(PDV_CACHE_KEY, JSON.stringify(p));
    } catch (e) {}
  }

  function homeAplicarSaldosNoCache(rows) {
    if (!Array.isArray(rows) || !rows.length) return false;
    var map = new Map();
    rows.forEach(function (r) {
      if (r && r[0] != null && r.length >= 5) map.set(String(r[0]), r);
    });
    var raw = localStorage.getItem(PDV_CACHE_KEY);
    if (!raw) return false;
    var p;
    try {
      p = JSON.parse(raw);
    } catch (e) {
      return false;
    }
    if (!Array.isArray(p.produtos) || !p.produtos.length) return false;
    p.produtos.forEach(function (prod) {
      var row = map.get(String(prod.id));
      if (!row) return;
      prod.saldo_centro = Number(row[1]);
      prod.saldo_vila = Number(row[2]);
      prod.saldo_erp_centro = Number(row[3]);
      prod.saldo_erp_vila = Number(row[4]);
    });
    p.saved_at = Date.now();
    try {
      localStorage.setItem(PDV_CACHE_KEY, JSON.stringify(p));
    } catch (e) {}
    return true;
  }

  async function homeFetchSaldos() {
    var urls = readBootstrap().urls || {};
    if (!urls.apiPdvSaldos) throw new Error('URL saldos ausente');
    var sep = urls.apiPdvSaldos.indexOf('?') >= 0 ? '&' : '?';
    var r = await fetch(urls.apiPdvSaldos + sep + '_t=' + Date.now(), {
      cache: 'no-store',
      credentials: 'same-origin',
    });
    return r.json();
  }

  async function homeFetchDeltaAndMerge() {
    var urls = readBootstrap().urls || {};
    if (!urls.apiTodosProdutosDelta) throw new Error('URL delta ausente');
    var st = homeLerCacheBruto();
    var since = st.catalog_version || '';
    var u = new URL(urls.apiTodosProdutosDelta, window.location.origin);
    if (since) u.searchParams.set('since', since);
    var r = await fetch(u.toString(), { credentials: 'same-origin' });
    var d = await r.json();
    if (d && d.unchanged) {
      homeTouchCatalogSavedAt();
      return;
    }
    if (d && d.delta) {
      var map = new Map();
      st.produtos.forEach(function (p) {
        map.set(String(p.id), Object.assign({}, p));
      });
      (d.changed || []).forEach(function (row) {
        var id = String(row.id != null ? row.id : '');
        var prev = map.get(id);
        var merged = Object.assign({}, row);
        if (prev) {
          merged.saldo_centro = prev.saldo_centro;
          merged.saldo_vila = prev.saldo_vila;
          merged.saldo_erp_centro = prev.saldo_erp_centro;
          merged.saldo_erp_vila = prev.saldo_erp_vila;
        }
        map.set(id, merged);
      });
      (d.removed_ids || []).forEach(function (pid) {
        map.delete(String(pid));
      });
      homeSalvarCache(Array.from(map.values()), d.catalog_version, d.catalog_updated_at);
      return;
    }
    if (d && Array.isArray(d.produtos)) {
      homeSalvarCache(d.produtos, d.catalog_version, d.catalog_updated_at);
    }
  }

  async function homeWarmClientes() {
    var urls = readBootstrap().urls || {};
    if (!urls.apiListCustomers) return;
    await fetch(urls.apiListCustomers, { credentials: 'same-origin', cache: 'no-store' }).catch(
      function () {},
    );
  }

  function homeMarcarSaldosFreshPersist() {
    try {
      localStorage.setItem(LS_SALDOS_FRESH, String(Date.now()));
    } catch (e) {}
  }

  function homeHidratarSaldosDoLS() {
    var lab = document.getElementById('agro-saldos-ultima-atualizacao');
    var btn = document.getElementById('agro-btn-atualizar-saldos');
    if (!lab || typeof AgroEstoqueSync === 'undefined') return;
    var at = parseInt(localStorage.getItem(LS_SALDOS_FRESH) || '0', 10);
    if (!at) return;
    lab.dataset.gmFreshAt = String(at);
    if (AgroEstoqueSync.formatHorario) lab.textContent = AgroEstoqueSync.formatHorario(new Date(at));
    else lab.textContent = new Date(at).toLocaleString('pt-BR');
    if (btn && AgroEstoqueSync.paintThermo)
      AgroEstoqueSync.paintThermo(btn, lab, AgroEstoqueSync.staleMsDefault);
  }

  function homeHidratarApiSyncDoCache() {
    var lab = document.getElementById('agro-api-sync-ultima');
    var btn = document.getElementById('agro-btn-sincronizar-api');
    if (!lab || !btn || typeof AgroEstoqueSync === 'undefined') return;
    try {
      var raw = localStorage.getItem(PDV_CACHE_KEY);
      if (!raw) return;
      var p = JSON.parse(raw);
      var at = Number(p.saved_at || 0);
      if (!at) return;
      lab.dataset.gmFreshAt = String(at);
      if (AgroEstoqueSync.formatHorario) lab.textContent = AgroEstoqueSync.formatHorario(new Date(at));
      else lab.textContent = new Date(at).toLocaleString('pt-BR');
      if (AgroEstoqueSync.paintThermo)
        AgroEstoqueSync.paintThermo(btn, lab, AgroEstoqueSync.staleMsDefault);
    } catch (e) {}
  }

  async function homeSincronizarApiCompleto() {
    var btn = document.getElementById('agro-btn-sincronizar-api');
    if (btn) {
      btn.disabled = true;
      btn.setAttribute('aria-busy', 'true');
    }
    if (window.gmLoadingBar) window.gmLoadingBar.show();
    try {
      await homeWarmClientes();
      await homeFetchDeltaAndMerge();
      await new Promise(function (r) {
        setTimeout(r, 700);
      });
      var sd = await homeFetchSaldos();
      if (sd && sd.rows) homeAplicarSaldosNoCache(sd.rows);
      homeMarcarSaldosFreshPersist();
      if (typeof AgroEstoqueSync !== 'undefined' && AgroEstoqueSync.markFresh) {
        AgroEstoqueSync.markFresh(
          document.getElementById('agro-saldos-ultima-atualizacao'),
          undefined,
          document.getElementById('agro-btn-atualizar-saldos'),
        );
      }
      homeHidratarApiSyncDoCache();
    } catch (e) {
      if (typeof console !== 'undefined' && console.error) console.error(e);
      alert('Não foi possível sincronizar. Verifique a rede e tente de novo.');
    } finally {
      if (window.gmLoadingBar) window.gmLoadingBar.hide();
      if (btn) {
        btn.disabled = false;
        btn.removeAttribute('aria-busy');
      }
    }
  }

  function init() {
    if (!document.getElementById('home-pdv-bootstrap')) return;
    homeHidratarSaldosDoLS();
    homeHidratarApiSyncDoCache();

    if (typeof AgroEstoqueSync !== 'undefined' && AgroEstoqueSync.mount) {
      AgroEstoqueSync.mount({
        onRefresh: async function () {
          var sd = await homeFetchSaldos();
          if (sd && sd.rows) homeAplicarSaldosNoCache(sd.rows);
          homeMarcarSaldosFreshPersist();
        },
      });
    }

    var apiBtn = document.getElementById('agro-btn-sincronizar-api');
    if (apiBtn) {
      apiBtn.addEventListener('click', function (e) {
        e.preventDefault();
        homeSincronizarApiCompleto();
      });
    }

    setInterval(function () {
      var sb = document.getElementById('agro-btn-atualizar-saldos');
      var sl = document.getElementById('agro-saldos-ultima-atualizacao');
      var ab = document.getElementById('agro-btn-sincronizar-api');
      var al = document.getElementById('agro-api-sync-ultima');
      if (typeof AgroEstoqueSync !== 'undefined' && AgroEstoqueSync.paintThermo) {
        if (sb && sl) AgroEstoqueSync.paintThermo(sb, sl, AgroEstoqueSync.staleMsDefault);
        if (ab && al) AgroEstoqueSync.paintThermo(ab, al, AgroEstoqueSync.staleMsDefault);
      }
    }, 15000);
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
