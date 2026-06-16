(function (w) {
  'use strict';

  function getCookie(name) {
    var v = document.cookie.match('(^|;) ?' + name + '=([^;]*)(;|$)');
    return v ? decodeURIComponent(v[2]) : '';
  }

  function csrf() {
    var lista = w.AgroCadastroErpLista;
    if (lista && lista.CSRF_TOKEN) return lista.CSRF_TOKEN;
    var meta = document.querySelector('meta[name="csrfmiddlewaretoken"]');
    if (meta && meta.getAttribute('content')) return meta.getAttribute('content');
    var ck = document.cookie.match(/(?:^|; )csrftoken=([^;]*)/);
    if (ck) return decodeURIComponent(ck[1]);
    var el = document.querySelector('[name=csrfmiddlewaretoken]');
    if (el && el.value) return el.value;
    return '';
  }

  function escapeHtml(s) {
    return String(s || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function fmtMoney(n) {
    var x = Number(n);
    if (!isFinite(x)) x = 0;
    return x.toLocaleString('pt-BR', {
      style: 'currency',
      currency: 'BRL',
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    });
  }

  function resetLoading() {
    if (w.gmLoadingBar && typeof w.gmLoadingBar.reset === 'function') {
      w.gmLoadingBar.reset();
    }
  }

  function setLoading(on) {
    if (w.gmLoadingBar) {
      if (on) w.gmLoadingBar.show();
      else w.gmLoadingBar.hide();
    }
  }

  var PDV_CACHE_KEY = 'agro_pdv_catalog_cache_v2';
  var PDV_PATCH_QUEUE_KEY = 'agro_pdv_catalog_patch_queue_v1';
  var PDV_PATCH_QUEUE_MAX = 24;

  function enqueuePdvCatalogoPatch(patch) {
    try {
      var raw = localStorage.getItem(PDV_PATCH_QUEUE_KEY);
      var q = raw ? JSON.parse(raw) : null;
      if (!q || !Array.isArray(q.items)) q = { updated_at: 0, items: [] };
      var pid = String(patch.id);
      q.items = q.items.filter(function (it) {
        var p = (it && it.patch) ? it.patch : it;
        return p && String(p.id) !== pid;
      });
      q.items.push({ patch: patch, ts: Date.now() });
      if (q.items.length > PDV_PATCH_QUEUE_MAX) {
        q.items = q.items.slice(q.items.length - PDV_PATCH_QUEUE_MAX);
      }
      q.updated_at = Date.now();
      localStorage.setItem(PDV_PATCH_QUEUE_KEY, JSON.stringify(q));
    } catch (e2) { /* ignore */ }
  }

  function gestaoProdutoParaPatchPdv(p) {
    if (!p || p.id == null || p.id === '') return null;
    var patch = {
      id: String(p.id),
      nome: p.nome,
      marca: p.marca,
      codigo_nfe: p.codigo_gm || p.codigo_nfe || p.codigo,
      codigo_barras: p.codigo_barras,
      preco_venda: p.preco_venda,
      preco_custo: p.preco_custo,
      categoria: p.categoria,
      subcategoria: p.subcategoria,
      fornecedor: p.fornecedor,
      unidade: p.unidade,
      descricao: p.descricao,
      inativo: !!p.inativo
    };
    if (p.cashback_percentual != null && isFinite(Number(p.cashback_percentual))) {
      patch.cashback_percentual = Number(p.cashback_percentual);
    }
    if (p.precos_por_forma && typeof p.precos_por_forma === 'object') {
      patch.precos_por_forma = p.precos_por_forma;
    }
    return patch;
  }

  /** Atualiza um produto no cache local do PDV (localStorage) após save no cadastro. */
  function patchPdvCatalogoCache(produto) {
    var patch = gestaoProdutoParaPatchPdv(produto);
    if (!patch) return false;
    try {
      var raw = localStorage.getItem(PDV_CACHE_KEY);
      var cache = raw ? JSON.parse(raw) : null;
      if (!cache || !Array.isArray(cache.produtos)) {
        cache = { saved_at: Date.now(), catalog_version: '', catalog_updated_at: '', produtos: [] };
      }
      var pid = String(patch.id);
      var found = false;
      for (var i = 0; i < cache.produtos.length; i++) {
        if (String(cache.produtos[i].id) === pid) {
          cache.produtos[i] = Object.assign({}, cache.produtos[i], patch);
          found = true;
          break;
        }
      }
      if (!found) cache.produtos.push(patch);
      cache.saved_at = Date.now();
      localStorage.setItem(PDV_CACHE_KEY, JSON.stringify(cache));
      enqueuePdvCatalogoPatch(patch);
      return true;
    } catch (e1) {
      return false;
    }
  }

  w.agroPdvPatchCatalogoCache = patchPdvCatalogoCache;
  w.AGRO_PDV_PATCH_QUEUE_KEY = PDV_PATCH_QUEUE_KEY;

  w.AgroCadastroErpUtil = {
    getCookie: getCookie,
    csrf: csrf,
    escapeHtml: escapeHtml,
    fmtMoney: fmtMoney,
    resetLoading: resetLoading,
    setLoading: setLoading,
    patchPdvCatalogoCache: patchPdvCatalogoCache
  };
})(window);
