/**
 * PDV — overlay Pesar granel (Urano US20/2 POP-S via Chrome Web Serial).
 * F10 / botão Pesar · códigos 1–199 (sem zeros) · auto-add ao estabilizar.
 */
(function () {
  'use strict';

  var CFG_KEY = 'agro_pdv_balanca_cfg_v1';
  var BAUD = 9600;
  /* Urano US20/2 POP-S USE-P2 na loja: 9600 8N1 (Gemini + teste COM4). */
  var STOP_BITS = 1;
  var STABLE_MS = 500;
  var MIN_KG = 0.001;
  var MAX_KG = 99.999;
  var CODE_MIN = 1;
  var CODE_MAX = 199;
  var POLL_MS = 280;
  var RESET_DELTA_KG = 0.015;

  var overlay = document.getElementById('pdv-balanca-overlay');
  if (!overlay) return;

  var dom = {
    btnOpen: document.getElementById('pdv-topbar-balanca-btn'),
    fechar: document.getElementById('pdv-balanca-fechar'),
    conectar: document.getElementById('pdv-balanca-conectar'),
    codigo: document.getElementById('pdv-balanca-codigo'),
    produto: document.getElementById('pdv-balanca-produto'),
    peso: document.getElementById('pdv-balanca-peso'),
    estavel: document.getElementById('pdv-balanca-estavel'),
    total: document.getElementById('pdv-balanca-total'),
    status: document.getElementById('pdv-balanca-status'),
    connChip: document.getElementById('pdv-balanca-conn-chip'),
    addManual: document.getElementById('pdv-balanca-add-manual'),
  };

  var port = null;
  var reader = null;
  var writer = null;
  var readLoopActive = false;
  var pollTimer = null;
  var buf = '';
  var lastKg = 0;
  var stableKg = 0;
  var stableSince = 0;
  var isStable = false;
  var connected = false;
  var produtoAtual = null;
  var resolveTimer = null;
  var resolveSeq = 0;
  var busyAdd = false;
  var armAfterKg = null;
  var lastAddedAt = 0;

  function bootUrls() {
    var el =
      document.getElementById('agro-pdv-wizard-bootstrap') ||
      document.getElementById('agro-pdv-bootstrap');
    try {
      var b = el ? JSON.parse(el.textContent || '{}') : {};
      return (b && b.urls) || {};
    } catch (e) {
      return {};
    }
  }

  function loadCfg() {
    try {
      return JSON.parse(localStorage.getItem(CFG_KEY) || '{}') || {};
    } catch (e) {
      return {};
    }
  }

  function saveCfg(patch) {
    var next = Object.assign(loadCfg(), patch || {});
    try {
      localStorage.setItem(CFG_KEY, JSON.stringify(next));
    } catch (e0) {}
    return next;
  }

  function fmtKg(n) {
    if (!n || n < MIN_KG) return '—,— kg';
    return (
      n.toLocaleString('pt-BR', {
        minimumFractionDigits: 3,
        maximumFractionDigits: 3,
      }) + ' kg'
    );
  }

  function fmtMoney(n) {
    return (
      'R$ ' +
      (Number(n) || 0).toLocaleString('pt-BR', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      })
    );
  }

  function setStatus(msg, tone) {
    if (!dom.status) return;
    dom.status.textContent = msg || '';
    dom.status.className =
      'min-h-[1.25rem] text-sm font-bold ' +
      (tone === 'err'
        ? 'text-red-700'
        : tone === 'ok'
          ? 'text-emerald-700'
          : 'text-slate-600');
  }

  function setConnChip(label, tone) {
    if (!dom.connChip) return;
    dom.connChip.textContent = label;
    dom.connChip.className =
      'bl-chip' +
      (tone === 'ok' ? ' is-ok' : tone === 'err' ? ' is-err' : ' is-warn');
  }

  function isOpen() {
    return overlay && !overlay.classList.contains('hidden');
  }

  function parseGranelCode(raw) {
    var s = String(raw || '').trim().replace(/\D/g, '');
    if (!s) return null;
    var n = parseInt(s, 10);
    if (!Number.isFinite(n) || n < CODE_MIN || n > CODE_MAX) return null;
    return n;
  }

  function codeVariants(n) {
    var s = String(n);
    var out = [s];
    if (s.length < 2) out.push(s.padStart(2, '0'));
    if (s.length < 3) out.push(s.padStart(3, '0'));
    if (s.length < 4) out.push(s.padStart(4, '0'));
    return out;
  }

  function numericCodeOf(val) {
    var dig = String(val == null ? '' : val).replace(/\D/g, '');
    if (!dig) return null;
    var n = parseInt(dig, 10);
    return Number.isFinite(n) ? n : null;
  }

  function productCodes(p) {
    var list = [p.codigo, p.codigo_nfe, p.codigo_barras, p.Codigo, p.CodigoBarras];
    if (Array.isArray(p.index_codigos)) list = list.concat(p.index_codigos);
    return list;
  }

  function productMatchesGranelNum(p, num) {
    var codes = productCodes(p);
    for (var i = 0; i < codes.length; i++) {
      if (numericCodeOf(codes[i]) === num) return true;
    }
    return false;
  }

  function uniqById(arr) {
    var seen = {};
    var out = [];
    (arr || []).forEach(function (p) {
      if (!p || typeof p !== 'object') return;
      var id = String(p.id || p.Id || p.produto_id || '').trim();
      if (!id || seen[id]) return;
      seen[id] = true;
      out.push(p);
    });
    return out;
  }

  function fetchBusca(q) {
    var urls = bootUrls();
    var base = urls.apiBuscarProdutos || '/api/buscar/';
    var url =
      base +
      (base.indexOf('?') >= 0 ? '&' : '?') +
      'wizard=1&wizard_catalog=1&q=' +
      encodeURIComponent(q);
    return fetch(url, { credentials: 'same-origin' })
      .then(function (r) {
        return r.json();
      })
      .then(function (j) {
        return Array.isArray(j && j.produtos) ? j.produtos : [];
      })
      .catch(function () {
        return [];
      });
  }

  function resolveProduto(num) {
    var variants = codeVariants(num);
    return Promise.all(variants.map(fetchBusca)).then(function (lists) {
      var flat = [];
      lists.forEach(function (L) {
        flat = flat.concat(L || []);
      });
      var hits = uniqById(flat).filter(function (p) {
        return productMatchesGranelNum(p, num);
      });
      if (!hits.length) {
        return { ok: false, erro: 'Produto ' + num + ' não encontrado.' };
      }
      if (hits.length > 1) {
        return {
          ok: false,
          erro:
            'Código ' +
            num +
            ' ambíguo (' +
            hits.length +
            ' produtos). Ajuste o cadastro.',
          ambiguo: true,
        };
      }
      return { ok: true, produto: hits[0] };
    });
  }

  function precoKg(p) {
    if (!p) return 0;
    var n = Number(
      p.preco_venda != null
        ? p.preco_venda
        : p.preco != null
          ? p.preco
          : p.preco_padrao != null
            ? p.preco_padrao
            : 0
    );
    return Number.isFinite(n) ? n : 0;
  }

  function updateTotalUi() {
    if (!dom.total) return;
    var kg = isStable ? stableKg : lastKg;
    var pk = precoKg(produtoAtual);
    dom.total.textContent = fmtMoney(kg * pk);
  }

  function updatePesoUi() {
    if (!dom.peso) return;
    var show = lastKg >= MIN_KG ? lastKg : 0;
    dom.peso.textContent = fmtKg(show);
    dom.peso.className =
      'bl-peso mt-1 ' + (isStable && show >= MIN_KG ? 'is-live' : 'is-wait');
    if (dom.estavel) {
      if (!connected) {
        dom.estavel.textContent = 'Balança desconectada';
      } else if (show < MIN_KG) {
        dom.estavel.textContent = 'Coloque o produto na balança';
      } else if (isStable) {
        dom.estavel.textContent = 'Peso estável';
        dom.estavel.className = 'mt-1 text-xs font-bold text-emerald-700';
      } else {
        dom.estavel.textContent = 'Estabilizando…';
        dom.estavel.className = 'mt-1 text-xs font-bold text-amber-700';
      }
    }
    updateTotalUi();
    maybeAutoAdd();
  }

  function onWeightSample(kg) {
    if (!Number.isFinite(kg) || kg < 0) return;
    if (kg > MAX_KG) return;
    var now = Date.now();
    lastKg = kg;

    if (armAfterKg != null) {
      if (kg < MIN_KG || Math.abs(kg - armAfterKg) >= RESET_DELTA_KG) {
        armAfterKg = null;
      } else {
        updatePesoUi();
        return;
      }
    }

    /* Sempre hold local (STABLE_MS) — evita add no ramp-up. */
    if (Math.abs(kg - stableKg) <= 0.002 && kg >= MIN_KG) {
      if (!stableSince) stableSince = now;
      if (now - stableSince >= STABLE_MS) {
        isStable = true;
        stableKg = kg;
      }
    } else {
      stableKg = kg;
      stableSince = now;
      isStable = false;
    }
    updatePesoUi();
  }

  /**
   * USE-P2 (US20/2 POP-S): frame [STX]dddddd[CR]
   * Ex.: STX + "001000" + CR → 1,000 kg (6 dígitos = gramas).
   * Aceita também peso com ponto/vírgula (outros firmwares).
   */
  function parseWeightFromChunk(text) {
    if (!text) return null;
    var raw = String(text);
    if (
      /instav|instável|unstable|------/i.test(raw) ||
      /sobrecarga|overload/i.test(raw)
    ) {
      return null;
    }

    /* Principal: STX + 6 dígitos (gramas) → kg */
    var mStx = raw.match(/\x02\s*(\d{4,7})\s*/);
    if (!mStx) mStx = raw.match(/(?:^|[^\d])(\d{6})(?:[^\d]|$)/);
    if (mStx) {
      var grams = parseInt(mStx[1], 10);
      if (Number.isFinite(grams) && grams >= 0) {
        var kgG = grams / 1000;
        if (kgG >= MIN_KG && kgG <= MAX_KG) return { kg: kgG };
      }
    }

    /* Fallback: "1.000" / "1,250" */
    var cleaned = raw.replace(/[^\x20-\x7E\r\n]/g, ' ');
    var m =
      cleaned.match(/(?:^|[\s,;])([+-]?\d{1,2}[.,]\d{1,3})\s*(?:kg)?(?:$|[\s\r\n,;])/i) ||
      cleaned.match(/([+-]?\d{1,2}[.,]\d{3})/);
    if (!m) return null;
    var n = parseFloat(m[1].replace(',', '.'));
    if (!Number.isFinite(n)) return null;
    n = Math.abs(n);
    if (n > MAX_KG) return null;
    return { kg: n };
  }

  function feedSerialText(chunk) {
    buf += chunk;
    if (buf.length > 4000) buf = buf.slice(-2000);
    /* Frames terminam em CR (USE-P2); LF também. */
    var parts = buf.split(/[\r\n]+/);
    if (parts.length > 1) {
      buf = parts.pop() || '';
      for (var i = 0; i < parts.length; i++) {
        var parsed = parseWeightFromChunk(parts[i]);
        if (parsed) onWeightSample(parsed.kg);
      }
    } else if (buf.indexOf('\x02') >= 0 && /\d{6}/.test(buf)) {
      /* Buffer ainda sem CR, mas já tem STX+dígitos — tenta. */
      var p2 = parseWeightFromChunk(buf);
      if (p2) onWeightSample(p2.kg);
    }
  }

  async function ensurePort() {
    if (!('serial' in navigator)) {
      throw new Error('Este Chrome não tem Web Serial. Use Chrome atualizado.');
    }
    if (port && port.readable) return port;
    var ports = await navigator.serial.getPorts();
    if (ports && ports.length) {
      port = ports[0];
      return port;
    }
    port = await navigator.serial.requestPort();
    saveCfg({
      asked: 1,
      modelo: 'US20/2 POP-S',
      protocolo: 'USE-P2',
      baud: BAUD,
      stopBits: STOP_BITS,
      hintPorta: 'COM4',
    });
    return port;
  }

  async function openPort() {
    var p = await ensurePort();
    if (!p.readable) {
      await p.open({
        baudRate: BAUD,
        dataBits: 8,
        parity: 'none',
        stopBits: STOP_BITS,
        flowControl: 'none',
      });
    }
    connected = true;
    setConnChip('COM ok', 'ok');
    setStatus('Balança conectada (USE-P2 · 9600 8N1). Escolha COM4 se pedir.', 'ok');
    startReadLoop();
    startPoll();
  }

  async function closePortSoft() {
    stopPoll();
    readLoopActive = false;
    try {
      if (reader) {
        try {
          await reader.cancel();
        } catch (e1) {}
        try {
          reader.releaseLock();
        } catch (e2) {}
        reader = null;
      }
    } catch (e3) {}
    try {
      if (writer) {
        try {
          writer.releaseLock();
        } catch (e4) {}
        writer = null;
      }
    } catch (e5) {}
    connected = false;
    setConnChip('Sem porta', 'warn');
  }

  function startPoll() {
    stopPoll();
    pollTimer = setInterval(function () {
      if (!port || !port.writable) return;
      try {
        if (!writer) writer = port.writable.getWriter();
        /* ENQ — pedido de peso (vários firmwares Urano) */
        writer.write(new Uint8Array([0x05])).catch(function () {});
      } catch (e) {}
    }, POLL_MS);
  }

  function stopPoll() {
    if (pollTimer) {
      clearInterval(pollTimer);
      pollTimer = null;
    }
  }

  async function startReadLoop() {
    if (!port || !port.readable || readLoopActive) return;
    readLoopActive = true;
    var decoder = new TextDecoderStream();
    var input = port.readable.pipeThrough(decoder);
    reader = input.getReader();
    try {
      while (readLoopActive) {
        var r = await reader.read();
        if (r.done) break;
        if (r.value) feedSerialText(r.value);
      }
    } catch (e) {
      if (isOpen()) {
        setStatus('Leitura interrompida. Toque em Conectar.', 'err');
        setConnChip('Erro', 'err');
      }
      connected = false;
    } finally {
      try {
        if (reader) reader.releaseLock();
      } catch (e6) {}
      reader = null;
      readLoopActive = false;
    }
  }

  function setProdutoUi(p, msg) {
    produtoAtual = p || null;
    if (!dom.produto) return;
    if (p) {
      var nome = String(p.nome || p.Nome || '').trim();
      var un = String(p.unidade || p.Unidade || '').trim().toUpperCase();
      dom.produto.textContent =
        nome +
        (un ? ' · ' + un : '') +
        ' · ' +
        fmtMoney(precoKg(p)) +
        '/kg';
      if (un && un !== 'KG' && un !== 'KG.' && un.indexOf('KG') < 0) {
        setStatus('Atenção: unidade do cadastro não é KG (' + un + ').', 'err');
      }
    } else {
      dom.produto.textContent = msg || '';
    }
    updateTotalUi();
  }

  function scheduleResolve() {
    if (resolveTimer) clearTimeout(resolveTimer);
    resolveTimer = setTimeout(runResolve, 180);
  }

  function runResolve() {
    var num = parseGranelCode(dom.codigo && dom.codigo.value);
    if (num == null) {
      setProdutoUi(null, '');
      if (dom.codigo && String(dom.codigo.value || '').trim()) {
        setStatus('Código deve ser de 1 a 199.', 'err');
      } else {
        setStatus('');
      }
      return Promise.resolve(false);
    }
    var seq = ++resolveSeq;
    setStatus('Buscando produto ' + num + '…');
    return resolveProduto(num).then(function (res) {
      if (seq !== resolveSeq) return false;
      if (!res.ok) {
        setProdutoUi(null, res.erro || 'Não encontrado');
        setStatus(res.erro || 'Não encontrado', 'err');
        return false;
      }
      setProdutoUi(res.produto);
      setStatus('Produto ok. Aguarde peso estável.', 'ok');
      maybeAutoAdd();
      return true;
    });
  }

  function canAddNow() {
    if (busyAdd) return false;
    if (!produtoAtual) return false;
    if (!isStable || stableKg < MIN_KG) return false;
    if (armAfterKg != null) return false;
    if (Date.now() - lastAddedAt < 700) return false;
    return true;
  }

  function maybeAutoAdd() {
    if (!isOpen()) return;
    if (!canAddNow()) return;
    doAdd(false);
  }

  function doAdd(manual) {
    if (!produtoAtual) {
      setStatus('Digite o código do produto (1–199).', 'err');
      return;
    }
    var codeNow = parseGranelCode(dom.codigo && dom.codigo.value);
    if (codeNow != null && !productMatchesGranelNum(produtoAtual, codeNow)) {
      setStatus('Aguardando produto do código digitado…', 'err');
      return;
    }
    var kg = isStable ? stableKg : lastKg;
    if (kg < MIN_KG) {
      setStatus('Sem peso na balança.', 'err');
      return;
    }
    if (!manual && !isStable) {
      setStatus('Aguarde o peso estabilizar.', 'err');
      return;
    }
    if (busyAdd) return;
    busyAdd = true;

    /* Pesar granel = sempre KG (peso físico), independente do cadastro. */
    var row = Object.assign({}, produtoAtual, { unidade: 'KG' });
    var State = window.AgroPdvState;
    var addPromise;
    try {
      if (window.AgroPdvAddResolvedProduct) {
        addPromise = window.AgroPdvAddResolvedProduct(row, kg);
      } else if (State && State.addItem) {
        addPromise = Promise.resolve(!!State.addItem(row, kg));
      } else {
        addPromise = Promise.resolve(false);
      }
    } catch (e) {
      addPromise = Promise.resolve(false);
    }

    Promise.resolve(addPromise)
      .then(function (added) {
        busyAdd = false;
        if (!added) {
          setStatus('Não deu para adicionar. Confira o caixa e o produto.', 'err');
          return;
        }
        lastAddedAt = Date.now();
        armAfterKg = kg;
        isStable = false;
        setStatus(
          'Adicionado: ' +
            fmtKg(kg) +
            ' · ' +
            String(row.nome || '').slice(0, 40),
          'ok'
        );
        if (dom.codigo) {
          dom.codigo.value = '';
          dom.codigo.focus();
        }
        setProdutoUi(null, '');
        try {
          if (typeof window.AudioContext !== 'undefined') {
            var ctx = new window.AudioContext();
            var o = ctx.createOscillator();
            var g = ctx.createGain();
            o.connect(g);
            g.connect(ctx.destination);
            o.frequency.value = 880;
            g.gain.value = 0.04;
            o.start();
            setTimeout(function () {
              o.stop();
              ctx.close();
            }, 90);
          }
        } catch (eAud) {}
      })
      .catch(function () {
        busyAdd = false;
        setStatus('Falha ao adicionar.', 'err');
      });
  }

  function openOverlay() {
    var st =
      window.AgroPdvState && window.AgroPdvState.getState
        ? window.AgroPdvState.getState()
        : null;
    if (st && st.currentStep && st.currentStep !== 'produtos') {
      setStatus('Pesar só na etapa Produtos. Volte antes.', 'err');
      if (typeof window.showPdvAviso === 'function') {
        window.showPdvAviso('Pesar granel só na etapa Produtos.', {
          title: 'Pesar',
          tone: 'error',
        });
      }
      return;
    }
    overlay.classList.remove('hidden');
    overlay.classList.add('flex');
    setStatus('');
    if (dom.codigo) {
      dom.codigo.value = '';
      setTimeout(function () {
        dom.codigo.focus();
      }, 40);
    }
    setProdutoUi(null, '');
    armAfterKg = null;
    openPort().catch(function (err) {
      connected = false;
      setConnChip('Sem porta', 'warn');
      setStatus(
        (err && err.message) ||
          'Toque em Conectar e escolha a porta COM da balança.',
        'err'
      );
    });
  }

  function closeOverlay() {
    overlay.classList.add('hidden');
    overlay.classList.remove('flex');
    /* Mantém a porta aberta no PC para reabrir rápido; só limpa UI. */
    setStatus('');
  }

  function toggleOverlay() {
    if (isOpen()) closeOverlay();
    else openOverlay();
  }

  if (dom.btnOpen) {
    dom.btnOpen.addEventListener('click', function (ev) {
      ev.preventDefault();
      openOverlay();
    });
  }
  if (dom.fechar) {
    dom.fechar.addEventListener('click', function () {
      closeOverlay();
    });
  }
  if (dom.conectar) {
    dom.conectar.addEventListener('click', function () {
      closePortSoft().then(function () {
        port = null;
        return navigator.serial
          .requestPort()
          .then(function (p) {
            port = p;
            saveCfg({ asked: 1, modelo: 'US20/2 POP-S', protocolo: 'USE-P2' });
            return openPort();
          })
          .catch(function (err) {
            setStatus(
              (err && err.message) || 'Porta não escolhida.',
              'err'
            );
          });
      });
    });
  }
  if (dom.addManual) {
    dom.addManual.addEventListener('click', function () {
      doAdd(true);
    });
  }
  if (dom.codigo) {
    dom.codigo.addEventListener('input', function () {
      var v = String(dom.codigo.value || '').replace(/\D/g, '').slice(0, 4);
      if (v !== dom.codigo.value) dom.codigo.value = v;
      scheduleResolve();
    });
    dom.codigo.addEventListener('keydown', function (ev) {
      if (ev.key === 'Enter') {
        ev.preventDefault();
        ev.stopPropagation();
        Promise.resolve(runResolve()).then(function (ok) {
          if (ok) doAdd(true);
        });
      }
    });
  }

  overlay.addEventListener('click', function (ev) {
    if (ev.target === overlay) closeOverlay();
  });

  function outroOverlayAberto() {
    return !!document.querySelector(
      '#pdv-pedir-loja-overlay:not(.hidden),#pdv-uso-loja-overlay:not(.hidden),' +
        '#pdv-repasse-vila-overlay:not(.hidden),#pdv-quick-client-modal:not(.hidden),' +
        '#pdv-quick-client-edit-overlay:not(.hidden),#pdv-quick-product-edit-overlay:not(.hidden)'
    );
  }

  document.addEventListener(
    'keydown',
    function (ev) {
      if (ev.code === 'F10' && !ev.altKey && !ev.ctrlKey && !ev.metaKey) {
        if (outroOverlayAberto()) return;
        ev.preventDefault();
        toggleOverlay();
        return;
      }
      if (!isOpen()) return;
      if (ev.key === 'Escape') {
        ev.preventDefault();
        ev.stopPropagation();
        closeOverlay();
      }
    },
    true
  );

  window.AgroPdvBalanca = {
    open: openOverlay,
    close: closeOverlay,
    isOpen: isOpen,
    /* prova local sem hardware: AgroPdvBalanca.mockKg(1.25) */
    mockKg: function (kg) {
      connected = true;
      setConnChip('Mock', 'ok');
      var n = Number(kg) || 0;
      onWeightSample(n);
      /* mock: força estável após hold mínimo */
      lastKg = n;
      stableKg = n;
      stableSince = Date.now() - STABLE_MS - 10;
      isStable = n >= MIN_KG;
      updatePesoUi();
    },
    /* Simula frame USE-P2: AgroPdvBalanca.mockFrame('\x02001000\r') → 1 kg */
    mockFrame: function (frame) {
      connected = true;
      setConnChip('Mock', 'ok');
      feedSerialText(String(frame || ''));
    },
  };
})();
