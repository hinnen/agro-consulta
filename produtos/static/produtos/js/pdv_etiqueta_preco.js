/**
 * PDV — aviso para trocar etiqueta quando o preço da prateleira muda.
 */
(function () {
  var btn = document.getElementById('pdv-topbar-etiqueta-preco-btn');
  var countEl = document.getElementById('pdv-topbar-etiqueta-preco-count');
  if (!btn || !countEl) return;

  var itens = [];
  var loja = '';
  var painel = null;

  function csrf() {
    var m = document.querySelector('meta[name=csrfmiddlewaretoken]');
    return m ? m.getAttribute('content') : '';
  }

  function esc(s) {
    return String(s || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function pintar() {
    var n = itens.length;
    countEl.textContent = String(n);
    btn.classList.toggle('hidden', n === 0);
    if (painel) desenhar();
  }

  function carregar() {
    fetch('/api/pdv/etiqueta-preco/', { credentials: 'same-origin' })
      .then(function (r) { return r.json(); })
      .then(function (j) {
        if (!j || !j.ok) return;
        itens = j.itens || [];
        loja = j.loja || '';
        pintar();
      })
      .catch(function () {});
  }

  function post(acao, extra) {
    var body = Object.assign({ acao: acao }, extra || {});
    return fetch('/api/pdv/etiqueta-preco/', {
      method: 'POST',
      credentials: 'same-origin',
      headers: {
        'Content-Type': 'application/json',
        'X-CSRFToken': csrf(),
      },
      body: JSON.stringify(body),
    }).then(function (r) { return r.json(); });
  }

  function cupom() {
    var linhas = itens.map(function (it) {
      return (
        '<div style="margin:0 0 8px;border-bottom:1px dashed #000;padding-bottom:6px;">' +
        '<div style="font-weight:900;font-size:13px;">' + esc(it.nome) + '</div>' +
        (it.codigo ? '<div style="font-size:11px;">' + esc(it.codigo) + '</div>' : '') +
        '<div style="font-size:12px;">De R$ ' + esc(it.preco_de) + '</div>' +
        '<div style="font-weight:900;font-size:16px;">Para R$ ' + esc(it.preco_para) + '</div>' +
        '</div>'
      );
    }).join('');
    var html =
      '<!DOCTYPE html><html><head><meta charset="utf-8"><title>Etiquetas para trocar</title>' +
      '<style>@page{margin:0;size:80mm auto}html,body{margin:0;padding:0;width:80mm}' +
      'body{font-family:system-ui,sans-serif;color:#000;background:#fff}' +
      '.pg{width:80mm;box-sizing:border-box;padding:4mm 3mm}</style></head><body><div class="pg">' +
      '<div style="text-align:center;font-weight:900;font-size:16px;">TROCAR ETIQUETA</div>' +
      '<div style="text-align:center;font-weight:800;margin-bottom:8px;">' + esc(loja) + ' · ' + itens.length + '</div>' +
      linhas +
      '<div style="text-align:center;font-size:11px;font-weight:800;">Marque Já troquei no PDV</div>' +
      '</div></body></html>';
    var w = window.open('', 'etq-preco-80', 'width=420,height=700');
    if (!w) return;
    w.document.open();
    w.document.write(html);
    w.document.close();
    w.focus();
    setTimeout(function () { try { w.print(); } catch (e) {} }, 250);
  }

  function desenhar() {
    if (!painel) return;
    var corpo = itens.map(function (it) {
      return (
        '<div class="flex items-start gap-2 border-b border-slate-200 py-2">' +
        '<div class="min-w-0 flex-1">' +
        '<div class="text-sm font-black text-slate-900">' + esc(it.nome) + '</div>' +
        '<div class="text-xs font-bold text-slate-500">' + esc(it.codigo) + '</div>' +
        '<div class="text-sm font-bold text-slate-600">Era R$ ' + esc(it.preco_de) + '</div>' +
        '<div class="text-base font-black text-orange-700">Agora R$ ' + esc(it.preco_para) + '</div>' +
        '</div>' +
        '<button type="button" data-ok="' + it.id + '" class="shrink-0 rounded-xl border-2 border-emerald-700 bg-emerald-600 px-3 py-2 text-sm font-black text-white">Já troquei</button>' +
        '</div>'
      );
    }).join('');
    painel.querySelector('[data-lista]').innerHTML = corpo || '<p class="py-4 text-center font-bold text-slate-500">Nada pendente.</p>';
    painel.querySelector('[data-loja]').textContent = loja + ' · ' + itens.length;
  }

  function abrir() {
    if (!painel) {
      painel = document.createElement('div');
      painel.className = 'fixed inset-0 z-[80] flex items-center justify-center bg-slate-900/50 p-3';
      painel.innerHTML =
        '<div class="flex max-h-[90dvh] w-full max-w-xl flex-col rounded-2xl border-2 border-orange-600 bg-white shadow-2xl">' +
        '<div class="flex items-center justify-between gap-2 border-b border-orange-200 bg-orange-50 px-3 py-2">' +
        '<div><div class="text-base font-black uppercase text-orange-900">Trocar etiqueta</div>' +
        '<div data-loja class="text-xs font-bold text-slate-600"></div></div>' +
        '<button type="button" data-fechar class="rounded-xl border-2 border-slate-400 bg-white px-3 py-2 text-sm font-black">Fechar</button></div>' +
        '<div data-lista class="min-h-0 flex-1 overflow-y-auto px-3"></div>' +
        '<div class="flex flex-wrap gap-2 border-t border-slate-200 p-3">' +
        '<button type="button" data-cupom class="rounded-xl border-2 border-slate-800 bg-slate-900 px-3 py-2 text-sm font-black text-white">Lista 80mm</button>' +
        '<button type="button" data-etq class="rounded-xl border-2 border-orange-800 bg-orange-500 px-3 py-2 text-sm font-black text-white">Imprimir etiquetas</button>' +
        '</div></div>';
      document.body.appendChild(painel);
      painel.addEventListener('click', function (ev) {
        var t = ev.target;
        if (t === painel || t.closest('[data-fechar]')) {
          painel.classList.add('hidden');
          return;
        }
        if (t.closest('[data-cupom]')) {
          cupom();
          return;
        }
        if (t.closest('[data-etq]')) {
          post('etiquetas').then(function (j) {
            if (j && j.ok && j.url) window.open(j.url, '_blank');
          });
          return;
        }
        var ok = t.closest('[data-ok]');
        if (!ok) return;
        post('ok', { id: Number(ok.getAttribute('data-ok')) }).then(function (j) {
          if (!j || !j.ok) return;
          itens = j.itens || [];
          loja = j.loja || loja;
          pintar();
        });
      });
    }
    painel.classList.remove('hidden');
    desenhar();
  }

  btn.addEventListener('click', abrir);
  carregar();
  setInterval(carregar, 45000);
})();
