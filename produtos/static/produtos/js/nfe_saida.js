(function () {
  'use strict';

  var CFG = window.NFE_SAIDA_CFG || {};
  var state = {
    docId: null,
    doc: null,
    step: 1,
    dirty: false,
    cert: null,
    listaFiltro: 'em_aberto',
  };

  function $(id) { return document.getElementById(id); }
  function qs(sel, root) { return (root || document).querySelector(sel); }

  function getCookie(name) {
    var v = document.cookie.match('(^|;) ?' + name + '=([^;]*)(;|$)');
    return v ? decodeURIComponent(v[2]) : '';
  }
  var csrftoken = (qs('[name=csrfmiddlewaretoken]') || {}).value || getCookie('csrftoken');

  function fetchJson(url, opts) {
    opts = opts || {};
    opts.headers = opts.headers || {};
    if (opts.method && opts.method !== 'GET') {
      opts.headers['X-CSRFToken'] = csrftoken;
      opts.headers['Content-Type'] = opts.headers['Content-Type'] || 'application/json';
    }
    return fetch(url, opts).then(function (r) {
      return r.json().then(function (j) {
        return { status: r.status, json: j };
      });
    });
  }

  function fmtMoeda(v) {
    return new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(Number(v) || 0);
  }

  function onlyDigits(s, max) {
    return String(s || '').replace(/\D/g, '').slice(0, max || 20);
  }

  function totalDoc() {
    var t = 0;
    (state.doc && state.doc.itens || []).forEach(function (ln) {
      t += Number(ln.v_total) || 0;
    });
    return t;
  }

  function setMsg(text, tone) {
    var el = $('nfe-saida-msg');
    if (!el) return;
    if (!text) {
      el.classList.add('hidden');
      return;
    }
    el.classList.remove('hidden');
    el.className = 'fixed top-14 left-3 right-3 max-w-2xl mx-auto z-[60] rounded-xl border-2 px-3 py-2 text-sm font-bold shadow-lg ' +
      (tone === 'err' ? 'border-red-500 bg-red-50 text-red-900' : tone === 'warn' ? 'border-amber-500 bg-amber-50 text-amber-950' : 'border-emerald-500 bg-emerald-50 text-emerald-950');
    el.textContent = text;
    setTimeout(function () { el.classList.add('hidden'); }, tone === 'err' ? 12000 : 6000);
  }

  function docTravado() {
    return state.doc && state.doc.status === 'autorizada';
  }

  function renderRail() {
    var dest = (state.doc && state.doc.destinatario) || {};
    $('rail-dest-nome').textContent = dest.nome || '—';
    $('rail-dest-doc').textContent = dest.documento ? (dest.tipo_doc || '').toUpperCase() + ' ' + dest.documento : 'Sem documento';
    $('rail-total').textContent = fmtMoeda(totalDoc());
    $('rail-itens').textContent = String((state.doc && state.doc.itens || []).length);
    var amb = Number((state.doc && state.doc.tp_amb) || (state.cert && state.cert.tp_amb) || 2);
    $('rail-amb').textContent = amb === 1 ? 'Produção' : 'Homologação';
    var sem = $('rail-semaforo');
    if (!sem || !state.doc) return;
    sem.innerHTML = '';
    (state.doc.checklist || []).forEach(function (row) {
      var li = document.createElement('li');
      li.className = row.ok ? 'text-emerald-800' : 'text-red-800 font-black';
      li.textContent = (row.ok ? '✓ ' : '✗ ') + row.label + (row.det ? ' — ' + row.det : '');
      sem.appendChild(li);
    });
    var vref = state.doc.venda_agro_id;
    var diff = $('rail-venda-diff');
    if (diff) {
      if (vref && state.doc.venda_total_ref != null && Math.abs(totalDoc() - Number(state.doc.venda_total_ref)) > 0.02) {
        diff.classList.remove('hidden');
        diff.textContent = 'Total diferente da venda #' + vref;
      } else {
        diff.classList.add('hidden');
      }
    }
    var nf = $('rail-nfce-chip');
    if (nf && state.doc.nfce_venda && state.doc.nfce_venda.autorizada) {
      nf.classList.remove('hidden');
    } else if (nf) {
      nf.classList.add('hidden');
    }
  }

  function bindDestForm() {
    var dest = state.doc.destinatario || {};
    var map = ['nome', 'documento', 'ie', 'email', 'cep', 'uf', 'cidade', 'bairro', 'logradouro', 'numero', 'complemento'];
    map.forEach(function (k) {
      var el = $('dest-' + k);
      if (el) el.value = dest[k] || '';
    });
    var indf = $('dest-ind-final');
    if (indf) indf.checked = Number(dest.ind_final) !== 0;
    var tipo = $('dest-tipo-doc');
    if (tipo) {
      var d = onlyDigits(dest.documento, 14);
      tipo.textContent = d.length === 14 ? 'Pessoa jurídica (CNPJ)' : d.length === 11 ? 'Pessoa física (CPF)' : 'Informe CPF ou CNPJ';
    }
  }

  function readDestForm() {
    if (!state.doc) return;
    var dest = state.doc.destinatario || {};
    ['nome', 'documento', 'ie', 'email', 'cep', 'uf', 'cidade', 'bairro', 'logradouro', 'numero', 'complemento'].forEach(function (k) {
      var el = $('dest-' + k);
      if (el) dest[k] = el.value.trim();
    });
    dest.documento = onlyDigits(dest.documento, 14);
    dest.tipo_doc = dest.documento.length === 14 ? 'cnpj' : dest.documento.length === 11 ? 'cpf' : '';
    var indf = $('dest-ind-final');
    dest.ind_final = indf && indf.checked ? 1 : 0;
    state.doc.destinatario = dest;
    state.dirty = true;
    renderRail();
  }

  function renderItens() {
    var tb = $('nfe-itens-body');
    if (!tb) return;
    tb.innerHTML = '';
    (state.doc.itens || []).forEach(function (ln, idx) {
      var tr = document.createElement('tr');
      tr.className = 'border-b border-slate-100';
      if (docTravado()) {
        tr.innerHTML =
          '<td class="p-2 font-bold">' + ln.descricao + '</td>' +
          '<td class="p-2 text-right tabular-nums">' + ln.qtd + '</td>' +
          '<td class="p-2 text-right tabular-nums">' + fmtMoeda(ln.v_total) + '</td>';
      } else {
        tr.innerHTML =
          '<td class="p-1"><input data-it="descricao" data-idx="' + idx + '" class="nfe-in w-full min-h-[44px]" value="' + (ln.descricao || '') + '"></td>' +
          '<td class="p-1 w-20"><input data-it="qtd" data-idx="' + idx + '" class="nfe-in w-full min-h-[44px] text-right" value="' + (ln.qtd || '') + '"></td>' +
          '<td class="p-1 w-24"><input data-it="v_unit" data-idx="' + idx + '" class="nfe-in w-full min-h-[44px] text-right" value="' + (ln.v_unit || '') + '"></td>' +
          '<td class="p-1 w-28"><input data-it="ncm" data-idx="' + idx + '" class="nfe-in w-full min-h-[44px] font-mono" value="' + (ln.ncm || '') + '"></td>' +
          '<td class="p-1 w-20"><input data-it="cfop" data-idx="' + idx + '" class="nfe-in w-full min-h-[44px] font-mono" value="' + (ln.cfop || '') + '"></td>' +
          '<td class="p-1 w-20"><input data-it="csosn" data-idx="' + idx + '" class="nfe-in w-full min-h-[44px] font-mono" value="' + (ln.csosn || '') + '"></td>' +
          '<td class="p-1 text-right font-black tabular-nums">' + fmtMoeda(ln.v_total) + '</td>' +
          '<td class="p-1"><button type="button" data-rm="' + idx + '" class="min-h-[40px] px-2 rounded-lg border-2 border-red-300 text-red-800 font-black text-xs">×</button></td>';
      }
      tb.appendChild(tr);
    });
    tb.querySelectorAll('[data-it]').forEach(function (inp) {
      inp.addEventListener('change', onItemChange);
    });
    tb.querySelectorAll('[data-rm]').forEach(function (btn) {
      btn.addEventListener('click', function () {
        var i = parseInt(btn.getAttribute('data-rm'), 10);
        if (!confirm('Remover esta linha?')) return;
        state.doc.itens.splice(i, 1);
        state.dirty = true;
        recalcItens();
        renderItens();
        renderRail();
      });
    });
  }

  function onItemChange(ev) {
    var inp = ev.target;
    var idx = parseInt(inp.getAttribute('data-idx'), 10);
    var field = inp.getAttribute('data-it');
    if (!state.doc.itens[idx]) return;
    state.doc.itens[idx][field] = inp.value;
    recalcLinha(state.doc.itens[idx]);
    state.dirty = true;
    renderItens();
    renderRail();
  }

  function recalcLinha(ln) {
    var q = parseFloat(String(ln.qtd || '0').replace(',', '.')) || 0;
    var vu = parseFloat(String(ln.v_unit || '0').replace(',', '.')) || 0;
    ln.v_total = Math.round(q * vu * 100) / 100;
  }

  function recalcItens() {
    (state.doc.itens || []).forEach(recalcLinha);
  }

  function renderPagamentos() {
    var wrap = $('nfe-pagamentos');
    if (!wrap) return;
    wrap.innerHTML = '';
    (state.doc.pagamento || []).forEach(function (p, idx) {
      var row = document.createElement('div');
      row.className = 'flex flex-wrap gap-2 items-center';
      row.innerHTML =
        '<input data-pg="forma" data-idx="' + idx + '" class="nfe-in flex-1 min-h-[48px]" placeholder="Forma" value="' + (p.forma || '') + '">' +
        '<select data-pg="tPag" data-idx="' + idx + '" class="nfe-in min-h-[48px]">' +
        ['01', '03', '04', '05', '15', '17', '90', '99'].map(function (c) {
          return '<option value="' + c + '"' + (p.tPag === c ? ' selected' : '') + '>' + c + '</option>';
        }).join('') +
        '</select>' +
        '<input data-pg="valor" data-idx="' + idx + '" class="nfe-in w-32 min-h-[48px] text-right" value="' + (p.valor || '') + '">';
      wrap.appendChild(row);
    });
    wrap.querySelectorAll('[data-pg]').forEach(function (el) {
      el.addEventListener('change', function () {
        var idx = parseInt(el.getAttribute('data-idx'), 10);
        var f = el.getAttribute('data-pg');
        state.doc.pagamento[idx][f] = f === 'valor' ? parseFloat(el.value) || 0 : el.value;
        state.dirty = true;
        renderRail();
      });
    });
  }

  function renderFiscal() {
    var f = state.doc.fiscal || {};
    var nat = $('fiscal-natureza');
    if (nat) nat.value = f.natureza || 'VENDA';
    var ic = $('fiscal-inf-compl');
    if (ic) ic.value = state.doc.inf_complementar || f.inf_compl || '';
  }

  function readFiscal() {
    state.doc.fiscal = state.doc.fiscal || {};
    state.doc.fiscal.natureza = ($('fiscal-natureza') || {}).value || 'VENDA';
    state.doc.inf_complementar = ($('fiscal-inf-compl') || {}).value || '';
    state.dirty = true;
  }

  function showStep(n) {
    state.step = n;
    document.querySelectorAll('[data-nfe-step-panel]').forEach(function (p) {
      p.classList.toggle('hidden', parseInt(p.getAttribute('data-nfe-step-panel'), 10) !== n);
    });
    document.querySelectorAll('[data-nfe-w-go]').forEach(function (b) {
      var s = parseInt(b.getAttribute('data-nfe-w-go'), 10);
      b.classList.toggle('nfe-wiz-on', s === n);
    });
    if (n === 2) {
      var busca = $('nfe-busca-prod');
      if (busca) setTimeout(function () { busca.focus(); }, 80);
    }
    if (n === 1) bindDestForm();
    if (n === 4) renderPagamentos();
    if (n === 3) renderFiscal();
    if (n === 5) refreshChecklist();
    if (n === 6) {
      var st = $('emit-status');
      if (st && state.doc.status === 'autorizada') {
        st.textContent = 'Autorizada · chave ' + (state.doc.chave || '');
      }
    }
    var btnNext = $('btn-nfe-next');
    var btnEmit = $('btn-nfe-emitir');
    if (btnNext) btnNext.classList.toggle('hidden', n >= 6);
    if (btnEmit) btnEmit.classList.toggle('hidden', n !== 6 || docTravado());
  }

  function refreshChecklist() {
    fetchJson(CFG.urlApi + encodeURIComponent(state.docId) + '/').then(function (res) {
      if (res.json.ok && res.json.doc) {
        state.doc.checklist = res.json.doc.checklist;
        renderRail();
        var ul = $('checklist-ul');
        if (ul) {
          ul.innerHTML = '';
          (state.doc.checklist || []).forEach(function (row) {
            var li = document.createElement('li');
            li.className = 'text-lg font-black ' + (row.ok ? 'text-emerald-800' : 'text-red-800');
            li.textContent = (row.ok ? '✓ ' : '✗ ') + row.label;
            ul.appendChild(li);
          });
        }
      }
    });
  }

  function openEditor(id) {
    state.docId = id;
    $('nfe-saida-view-lista').classList.add('hidden');
    $('nfe-saida-view-editor').classList.remove('hidden');
    fetchJson(CFG.urlApi + encodeURIComponent(id) + '/').then(function (res) {
      if (!res.json.ok) {
        setMsg(res.json.erro || 'Erro ao abrir', 'err');
        return;
      }
      state.doc = res.json.doc;
      state.dirty = false;
      bindDestForm();
      renderItens();
      renderPagamentos();
      renderFiscal();
      renderRail();
      showStep(1);
      $('editor-venda-tag').textContent = state.doc.venda_agro_id ? 'Venda #' + state.doc.venda_agro_id : 'Avulsa';
      if (state.doc.status === 'rejeitada') {
        setMsg(state.doc.x_motivo || 'Rejeitada na última tentativa', 'warn');
      }
    });
  }

  function closeEditor(force) {
    if (!force && state.dirty && !confirm('Sair sem salvar alterações?')) return;
    $('nfe-saida-view-editor').classList.add('hidden');
    $('nfe-saida-view-lista').classList.remove('hidden');
    state.doc = null;
    state.docId = null;
    loadLista();
  }

  function salvarRascunho(silent) {
    if (!state.docId || !state.doc || docTravado()) return Promise.resolve();
    readDestForm();
    readFiscal();
    return fetchJson(CFG.urlSalvar, {
      method: 'POST',
      body: JSON.stringify({ id: state.docId, doc: state.doc }),
    }).then(function (res) {
      if (res.json.ok) {
        state.dirty = false;
        var hint = $('nfe-autosave-hint');
        if (hint) {
          var d = new Date();
          hint.textContent = 'Salvo ' + d.toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' });
        }
        if (!silent) setMsg('Rascunho salvo.', 'ok');
      } else if (!silent) {
        setMsg(res.json.erro || 'Falha ao salvar', 'err');
      }
      return res.json;
    });
  }

  function loadLista() {
    var q = ($('nfe-lista-busca') || {}).value || '';
    fetchJson(CFG.urlListar + '?filtro=' + encodeURIComponent(state.listaFiltro) + '&q=' + encodeURIComponent(q)).then(function (res) {
      var box = $('nfe-lista-body');
      if (!box) return;
      box.innerHTML = '';
      if (!res.json.ok) {
        box.textContent = res.json.erro || 'Erro';
        return;
      }
      (res.json.itens || []).forEach(function (row) {
        var div = document.createElement('div');
        div.className = 'nfe-lista-row grid md:grid-cols-[1fr_auto_auto] gap-2 items-center p-3 rounded-xl border-2 border-slate-200 bg-white';
        div.innerHTML =
          '<div><div class="font-black text-slate-900">' + row.lista_destinatario + '</div>' +
          '<div class="text-xs font-bold text-slate-500">' + row.status_label + (row.venda_agro_id ? ' · Venda #' + row.venda_agro_id : '') + '</div></div>' +
          '<div class="text-right font-black tabular-nums">' + fmtMoeda(row.lista_total) + '<div class="text-xs text-slate-500">' + row.lista_numero_serie + '</div></div>' +
          '<div class="flex flex-wrap gap-2 justify-end">' +
          '<button type="button" class="min-h-[44px] px-3 rounded-xl bg-emerald-700 text-white font-black text-xs uppercase" data-open="' + row.id + '">Abrir</button>' +
          (row.status === 'autorizada' ? '<a class="min-h-[44px] px-3 inline-flex items-center rounded-xl border-2 border-sky-500 text-sky-800 font-black text-xs uppercase" href="' + CFG.urlApi + row.id + '/danfe/" target="_blank">DANFE</a>' : '') +
          '</div>';
        box.appendChild(div);
      });
      box.querySelectorAll('[data-open]').forEach(function (btn) {
        btn.addEventListener('click', function () {
          openEditor(btn.getAttribute('data-open'));
        });
      });
    });
  }

  function novaNota() {
    fetchJson(CFG.urlNova, { method: 'POST', body: '{}' }).then(function (res) {
      if (res.json.ok) openEditor(res.json.id);
      else setMsg(res.json.erro || 'Erro', 'err');
    });
  }

  function deVenda(vendaId) {
    fetchJson(CFG.urlDeVenda, { method: 'POST', body: JSON.stringify({ venda_id: vendaId }) }).then(function (res) {
      if (res.json.ok) openEditor(res.json.id);
      else setMsg(res.json.erro || 'Erro', 'err');
    });
  }

  function buscarProduto() {
    var term = ($('nfe-busca-prod') || {}).value || '';
    if (!term.trim()) return;
    fetchJson(CFG.urlBuscar + '?q=' + encodeURIComponent(term.trim())).then(function (res) {
      var data = res.json;
      var list = data.produtos || data.results || data.itens || [];
      if (!list.length) {
        setMsg('Nenhum produto.', 'warn');
        return;
      }
      var p = list[0];
      var ln = {
        produto_id: String(p.id || p.produto_id || p._id || ''),
        codigo: p.codigo || p.codigo_interno || '',
        descricao: p.nome || p.descricao || '',
        qtd: 1,
        un: p.unidade || 'UN',
        ncm: p.ncm || '23099020',
        cfop: p.cfop || '5102',
        csosn: p.csosn || '102',
        origem: '0',
        v_unit: parseFloat(p.preco_venda || p.preco || p.valor || 0) || 0,
        v_total: parseFloat(p.preco_venda || p.preco || p.valor || 0) || 0,
      };
      state.doc.itens = state.doc.itens || [];
      state.doc.itens.push(ln);
      state.dirty = true;
      $('nfe-busca-prod').value = '';
      renderItens();
      renderRail();
    });
  }

  function buscarCliente() {
    var term = ($('nfe-busca-cli') || {}).value || '';
    fetchJson(CFG.urlBuscarClientes + '?q=' + encodeURIComponent(term)).then(function (res) {
      var list = res.json.clientes || res.json.itens || [];
      if (!list.length) {
        setMsg('Cliente não encontrado.', 'warn');
        return;
      }
      var c = list[0];
      state.doc.destinatario = state.doc.destinatario || {};
      state.doc.destinatario.cliente_agro_id = c.id;
      state.doc.destinatario.nome = c.nome || '';
      state.doc.destinatario.documento = onlyDigits(c.cpf || c.documento, 14);
      state.doc.destinatario.cep = c.cep || '';
      state.doc.destinatario.uf = c.uf || '';
      state.doc.destinatario.cidade = c.cidade || '';
      state.doc.destinatario.bairro = c.bairro || '';
      state.doc.destinatario.logradouro = c.logradouro || '';
      state.doc.destinatario.numero = c.numero || '';
      bindDestForm();
      state.dirty = true;
      renderRail();
    });
  }

  function emitirComPin() {
    if (docTravado()) return;
    var pin = ($('nfe-pin-emitir') || {}).value || '';
    function tentar(p) {
      fetchJson(CFG.urlApi + encodeURIComponent(state.docId) + '/emitir/', {
        method: 'POST',
        body: JSON.stringify({ pin: p }),
      }).then(function (res) {
        if (res.status === 403 && window.gmSspinErroPedePin && window.gmSspinErroPedePin(res.json.erro)) {
          if (window.gmSspinGarantirOperador) {
            window.gmSspinGarantirOperador(function () {
              emitirComPin();
            }, { titulo: 'PIN para emitir NF-e' });
          }
          return;
        }
        if (res.json.ok) {
          state.doc = res.json.doc;
          setMsg('NF-e autorizada' + (res.json.simulado ? ' (simulação homologação)' : ''), 'ok');
          showStep(6);
          renderRail();
          renderItens();
        } else {
          setMsg(res.json.erro || res.json.x_motivo || 'Rejeitada', 'err');
          if (res.json.doc) state.doc = res.json.doc;
          renderRail();
        }
      });
    }
    salvarRascunho(true).then(function () {
      if (pin) tentar(pin);
      else if (window.gmSspinGarantirOperador) {
        window.gmSspinGarantirOperador(function () {
          tentar('');
        }, { titulo: 'PIN para emitir NF-e 55' });
      } else {
        tentar(pin);
      }
    });
  }

  function init() {
    fetchJson(CFG.urlCert).then(function (res) {
      state.cert = res.json;
      var band = $('nfe-homolog-band');
      if (band && Number(res.json.tp_amb) === 2) band.classList.remove('hidden');
    });
    document.querySelectorAll('[data-nfe-filtro]').forEach(function (chip) {
      chip.addEventListener('click', function () {
        state.listaFiltro = chip.getAttribute('data-nfe-filtro');
        document.querySelectorAll('[data-nfe-filtro]').forEach(function (c) {
          c.classList.toggle('nfe-chip-on', c === chip);
        });
        loadLista();
      });
    });
    $('btn-nfe-nova') && $('btn-nfe-nova').addEventListener('click', novaNota);
    $('btn-nfe-refresh') && $('btn-nfe-refresh').addEventListener('click', loadLista);
    $('btn-nfe-voltar-lista') && $('btn-nfe-voltar-lista').addEventListener('click', function () { closeEditor(false); });
    $('btn-nfe-salvar') && $('btn-nfe-salvar').addEventListener('click', function () { salvarRascunho(false); });
    $('btn-nfe-next') && $('btn-nfe-next').addEventListener('click', function () {
      if (state.step === 1) readDestForm();
      if (state.step === 3) readFiscal();
      if (state.step < 6) showStep(state.step + 1);
    });
    $('btn-nfe-prev') && $('btn-nfe-prev').addEventListener('click', function () {
      if (state.step > 1) showStep(state.step - 1);
    });
    document.querySelectorAll('[data-nfe-w-go]').forEach(function (b) {
      b.addEventListener('click', function () {
        showStep(parseInt(b.getAttribute('data-nfe-w-go'), 10));
      });
    });
    $('btn-nfe-busca-prod') && $('btn-nfe-busca-prod').addEventListener('click', buscarProduto);
    $('nfe-busca-prod') && $('nfe-busca-prod').addEventListener('keydown', function (e) {
      if (e.key === 'Enter') { e.preventDefault(); buscarProduto(); }
    });
    $('btn-nfe-busca-cli') && $('btn-nfe-busca-cli').addEventListener('click', buscarCliente);
    $('btn-nfe-puxar-venda') && $('btn-nfe-puxar-venda').addEventListener('click', function () {
      var id = ($('nfe-puxar-venda-id') || {}).value;
      if (id) deVenda(parseInt(id, 10));
    });
    $('btn-nfe-danfe') && $('btn-nfe-danfe').addEventListener('click', function () {
      if (state.docId) window.open(CFG.urlApi + state.docId + '/danfe/', '_blank');
    });
    $('btn-nfe-emitir') && $('btn-nfe-emitir').addEventListener('click', emitirComPin);
    $('nfe-lista-busca') && $('nfe-lista-busca').addEventListener('input', function () {
      clearTimeout(window._nfeListaBuscaT);
      window._nfeListaBuscaT = setTimeout(loadLista, 300);
    });
    document.querySelectorAll('.dest-field').forEach(function (el) {
      el.addEventListener('change', function () { readDestForm(); });
    });
    $('btn-nfe-add-pag') && $('btn-nfe-add-pag').addEventListener('click', function () {
      state.doc.pagamento = state.doc.pagamento || [];
      state.doc.pagamento.push({ forma: 'Dinheiro', tPag: '01', valor: totalDoc() });
      state.dirty = true;
      renderPagamentos();
    });
    setInterval(function () {
      if (state.docId && state.dirty && !docTravado()) salvarRascunho(true);
    }, 45000);
    document.addEventListener('keydown', function (e) {
      if (e.key === 'F4' && $('nfe-saida-view-editor').classList.contains('hidden')) {
        e.preventDefault(); novaNota();
      }
      if (e.key === 'F8' && state.docId) {
        e.preventDefault();
        window.open(CFG.urlApi + state.docId + '/danfe/', '_blank');
      }
      if (e.key === 'F10' && state.step === 6) {
        e.preventDefault();
        emitirComPin();
      }
    });
    loadLista();
    if (CFG.vendaId) {
      deVenda(CFG.vendaId);
    } else if (CFG.docId) {
      openEditor(CFG.docId);
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
