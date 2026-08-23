/**
 * PDV — overlay Pesar granel (Urano USE-P2 / USE-PII via Chrome Web Serial).
 * F10 / botão Pesar · códigos 1–199 · auto-add ao estabilizar.
 *
 * A COM4 manda ESC N 1 + "0,00" (não STX, não sufixo kg). Não tratar ESC como impressora.
 */
(function () {
  'use strict';

  var P2 = window.AgroUseP2 || {};
  var CFG_KEY = 'agro_pdv_balanca_cfg_v1';
  var BAUD = (P2.SERIAL_DEFAULTS && P2.SERIAL_DEFAULTS.baudRate) || 9600;
  var STOP_BITS = 2;
  var STABLE_MS = P2.STABLE_MS || 380;
  var MIN_KG = P2.MIN_WEIGHT_KG || 0.02;
  var MAX_KG = P2.MAX_WEIGHT_KG || 30;
  var CODE_MIN = 1;
  var CODE_MAX = 199;
  var POLL_MS = 450;

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
    rx: document.getElementById('pdv-balanca-rx'),
    semPorta: document.getElementById('pdv-balanca-sem-porta'),
    stopBits: document.getElementById('pdv-balanca-stopbits'),
    simChips: document.getElementById('pdv-balanca-sim-chips'),
  };

  var port = null;
  var reader = null;
  var writer = null;
  var readLoopActive = false;
  var pollTimer = null;
  var serialBuf = new Uint8Array(0);
  var weightHistory = [];
  var lastRawHint = '';
  var lastByteAt = 0;
  var silenceTimer = null;
  var lastKg = null;
  var isStable = false;
  var hadBytes = false;
  var connected = false;
  var scaleMode = 'idle';
  var produtoAtual = null;
  var resolveTimer = null;
  var resolveSeq = 0;
  var busyAdd = false;
  var lastAutoKey = '';
  var lastAddedAt = 0;
  var simTimer = null;
  var simWeightKg = 0;
  var stopBits = STOP_BITS;

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
    if (n === null || n === undefined || !Number.isFinite(n)) return '—,— kg';
    if (P2.formatKg) return P2.formatKg(n) + ' kg';
    return (
      n.toLocaleString('pt-BR', {
        minimumFractionDigits: 2,
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
    var list = [
      p.codigo,
      p.codigo_nfe,
      p.codigo_interno,
      p.codigo_barras,
      p.codigo_gm,
      p.Codigo,
      p.CodigoBarras,
      p.codigoGm,
    ];
    if (Array.isArray(p.index_codigos)) list = list.concat(p.index_codigos);
    return list;
  }

  /** Casa 10 com barcode 0010 / 10. GM0010 só como reserva (não conta sozinho se houver barras). */
  function barcodeOf(p) {
    return String(p.codigo_barras || p.CodigoBarras || '').trim();
  }

  function gmGranelNum(c) {
    var m = String(c || '').trim().match(/^GM0*(\d{1,3})(?:-.*)?$/i);
    if (!m) return null;
    var n = parseInt(m[1], 10);
    return n >= CODE_MIN && n <= CODE_MAX ? n : null;
  }

  function productMatchesGranelNum(p, num) {
    var codes = productCodes(p);
    for (var i = 0; i < codes.length; i++) {
      var c = String(codes[i] == null ? '' : codes[i]).trim();
      if (!c) continue;
      if (/^\d{1,4}$/.test(c) && parseInt(c, 10) === num) return true;
      if (gmGranelNum(c) === num) return true;
    }
    return false;
  }

  function productLabelShort(p) {
    var nome = String(p.nome || p.Nome || '').trim() || '(sem nome)';
    var cb = barcodeOf(p);
    var gm = String(p.codigo_nfe || p.codigo || '').trim();
    var bits = [];
    if (cb) bits.push('barras ' + cb);
    if (gm) bits.push(gm);
    return nome.slice(0, 42) + (bits.length ? ' [' + bits.join(' · ') + ']' : '');
  }

  /** Preferência: barcode numérico (0010/010/10) > demais; sem barcode, lista completa. */
  function preferHits(hits, num, typedRaw) {
    hits = hits || [];
    var byBar = hits.filter(function (p) {
      var b = barcodeOf(p);
      return b && /^\d{1,4}$/.test(b) && parseInt(b, 10) === num;
    });
    if (byBar.length >= 1) return byBar;
    return hits;
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

  function catalogListsLocal() {
    var out = [];
    function pushPayload(raw) {
      if (!raw) return;
      try {
        var d = typeof raw === 'string' ? JSON.parse(raw) : raw;
        var list = (d && d.produtos) || (Array.isArray(d) ? d : null);
        if (list && list.length) out.push(list);
      } catch (e) {}
    }
    try {
      pushPayload(sessionStorage.getItem('agro_pdv_wizard_catalog_v11'));
    } catch (e1) {}
    try {
      pushPayload(localStorage.getItem('agro_pdv_catalog_cache_v2'));
    } catch (e2) {}
    try {
      if (window.AgroPdvWizardCatalog && Array.isArray(window.AgroPdvWizardCatalog)) {
        out.push(window.AgroPdvWizardCatalog);
      }
    } catch (e3) {}
    return out;
  }

  function hitsFromLists(lists, num) {
    var flat = [];
    (lists || []).forEach(function (L) {
      flat = flat.concat(L || []);
    });
    return uniqById(flat).filter(function (p) {
      return productMatchesGranelNum(p, num);
    });
  }

  function packResolveHits(hits, num, pad4, typedRaw) {
    hits = preferHits(hits || [], num, typedRaw);
    if (!hits.length) {
      return {
        ok: false,
        erro:
          'Produto ' +
          num +
          ' não encontrado. Cadastre barras ' +
          pad4 +
          ' ou GM' +
          pad4 +
          '.',
      };
    }
    if (hits.length > 1) {
      var nomes = hits
        .slice(0, 5)
        .map(productLabelShort)
        .join(' · ');
      return {
        ok: false,
        erro:
          'Código ' +
          num +
          ' ambíguo (' +
          hits.length +
          '): ' +
          nomes +
          '. Deixe só 1 com barras ' +
          pad4 +
          '.',
        ambiguo: true,
      };
    }
    return { ok: true, produto: hits[0] };
  }

  function resolveProduto(num, typedRaw) {
    var pad4 = String(num).padStart(4, '0');
    var localHits = hitsFromLists(catalogListsLocal(), num);
    /* Sempre consulta API — cache local sozinho pode preferir GM errado. */
    var variants = codeVariants(num).concat([
      pad4,
      'GM' + pad4,
      'GM' + pad4 + '-1',
      'GM' + String(num),
      'GM' + String(num) + '-1',
      String(typedRaw || '').trim(),
    ]);
    var seenQ = {};
    var uniqQ = [];
    variants.forEach(function (q) {
      var s = String(q || '').trim();
      if (!s || seenQ[s]) return;
      seenQ[s] = true;
      uniqQ.push(s);
    });
    return Promise.all(uniqQ.map(fetchBusca)).then(function (lists) {
      var hits = hitsFromLists(lists.concat(catalogListsLocal()), num);
      if (!hits.length && localHits.length) hits = localHits;
      return packResolveHits(hits, num, pad4, typedRaw);
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
    var kg = lastKg != null && Number.isFinite(lastKg) ? lastKg : 0;
    var pk = precoKg(produtoAtual);
    dom.total.textContent = fmtMoney(kg * pk);
  }

  function currentMode() {
    if (scaleMode === 'sim') return 'sim';
    if (connected) return 'serial';
    return 'idle';
  }

  function updatePesoUi() {
    if (!dom.peso) return;
    var show = hadBytes && lastKg !== null && Number.isFinite(lastKg) ? lastKg : null;
    dom.peso.textContent = fmtKg(show);
    dom.peso.className =
      'bl-peso mt-1 ' + (isStable && show !== null && show >= MIN_KG ? 'is-live' : 'is-wait');
    if (dom.estavel) {
      if (scaleMode === 'sim') {
        dom.estavel.textContent =
          show === 0
            ? 'Simulador · dump real USE-P2 (0,00 kg no prato)'
            : isStable
              ? 'Simulador · peso estável'
              : 'Simulador · peso ao vivo';
        dom.estavel.className = 'mt-1 text-xs font-bold text-emerald-700';
      } else if (!connected) {
        dom.estavel.textContent = 'Balança desconectada';
        dom.estavel.className = 'mt-1 text-xs font-bold text-slate-500';
      } else if (!hadBytes) {
        dom.estavel.textContent = 'Porta aberta · esperando bytes · confira cabo e USE-P2';
        dom.estavel.className = 'mt-1 text-xs font-bold text-amber-700';
      } else if (show !== null && show < MIN_KG) {
        dom.estavel.textContent = 'Prato vazio · 0,00 kg é leitura válida';
        dom.estavel.className = 'mt-1 text-xs font-bold text-slate-600';
      } else if (isStable) {
        dom.estavel.textContent = 'Peso estável';
        dom.estavel.className = 'mt-1 text-xs font-bold text-emerald-700';
      } else {
        dom.estavel.textContent = 'Peso ao vivo · aguardando estabilizar';
        dom.estavel.className = 'mt-1 text-xs font-bold text-amber-700';
      }
    }
    updateTotalUi();
    maybeAutoAdd();
  }

  function applyReading(reading, hexHint, glyphHint) {
    reading = reading || {};
    hadBytes = !!reading.hadBytes;
    if (hexHint) setRxHint(hexHint, glyphHint);
    if (reading.weightKg !== null && Number.isFinite(reading.weightKg)) {
      var now = Date.now();
      lastKg = reading.weightKg;
      weightHistory = weightHistory.slice(-12).concat([{ kg: lastKg, at: now }]);
      isStable = P2.isWeightStable
        ? P2.isWeightStable(weightHistory, now, STABLE_MS)
        : false;
    }
    updatePesoUi();
  }

  function onWeightSample(kg) {
    if (!Number.isFinite(kg) || kg < 0 || kg > MAX_KG) return;
    applyReading(
      { weightKg: kg, hadBytes: true, source: 'mock' },
      '',
      ''
    );
  }

  function feedSerialBytes(u8) {
    if (!u8 || !u8.length) return;
    lastByteAt = Date.now();
    var arr = u8 instanceof Uint8Array ? u8 : Uint8Array.from(u8);
    serialBuf = P2.mergeSerialBuffer
      ? P2.mergeSerialBuffer(serialBuf, arr)
      : arr;
    var reading = P2.parseUseP2 ? P2.parseUseP2(serialBuf) : { hadBytes: true, weightKg: null };
    applyReading(
      reading,
      P2.bytesToHex ? P2.bytesToHex(arr) : bytesToHint(Array.prototype.slice.call(arr)),
      P2.bytesToGlyphs ? P2.bytesToGlyphs(arr) : bytesToAsciiHint(Array.prototype.slice.call(arr))
    );
  }

  function feedSerialText(chunk) {
    if (!chunk) return;
    var u8 = new Uint8Array(String(chunk).length);
    for (var i = 0; i < String(chunk).length; i++) u8[i] = String(chunk).charCodeAt(i) & 0xff;
    feedSerialBytes(u8);
  }

  function bytesToHint(arr) {
    return (arr || [])
      .slice(0, 16)
      .map(function (b) {
        return ('0' + b.toString(16)).slice(-2);
      })
      .join(' ');
  }

  function portInfoOf(p) {
    try {
      return (p && typeof p.getInfo === 'function' && p.getInfo()) || {};
    } catch (e) {
      return {};
    }
  }

  function savePortFingerprint(p) {
    var info = portInfoOf(p);
    saveCfg({
      asked: 1,
      modelo: 'Urano USE-P2',
      protocolo: 'USE-P2',
      baud: BAUD,
      stopBits: stopBits,
      hintPorta: 'COM4',
      usbVendorId: info.usbVendorId != null ? info.usbVendorId : null,
      usbProductId: info.usbProductId != null ? info.usbProductId : null,
    });
  }

  function findSavedPort(ports) {
    var cfg = loadCfg();
    if (cfg.usbVendorId == null) return null;
    for (var i = 0; i < (ports || []).length; i++) {
      var info = portInfoOf(ports[i]);
      if (
        info.usbVendorId === cfg.usbVendorId &&
        (cfg.usbProductId == null || info.usbProductId === cfg.usbProductId)
      ) {
        return ports[i];
      }
    }
    return null;
  }

  function bytesToAsciiHint(arr) {
    if (P2.bytesToGlyphs) return P2.bytesToGlyphs(Uint8Array.from(arr || []));
    var out = '';
    for (var i = 0; i < (arr || []).length && i < 16; i++) {
      var b = arr[i];
      if (b === 0x1b) out += 'ESC ';
      else if (b === 0x02) out += 'STX ';
      else if (b === 0x03) out += 'ETX ';
      else if (b === 0x05) out += 'ENQ ';
      else if (b === 0x0d) out += 'CR ';
      else if (b === 0x0a) out += 'LF ';
      else if (b >= 0x20 && b <= 0x7e) out += String.fromCharCode(b) + ' ';
      else out += '? ';
    }
    return out.trim();
  }

  function setRxHint(txt, asciiExtra) {
    lastRawHint = txt || '';
    if (dom.rx) {
      if (!lastRawHint) {
        dom.rx.textContent = connected || scaleMode === 'sim' ? 'RX: (aguardando bytes…)' : '';
      } else {
        dom.rx.textContent =
          'RX: ' + lastRawHint + (asciiExtra ? '  ·  ' + asciiExtra : '');
      }
    }
  }

  function startSilenceWatch() {
    stopSilenceWatch();
    lastByteAt = Date.now();
    setRxHint('');
    silenceTimer = setInterval(function () {
      if (!connected || !isOpen() || scaleMode === 'sim') return;
      if (hadBytes) return;
      if (Date.now() - lastByteAt < 2800) return;
      if (dom.estavel) {
        dom.estavel.textContent =
          'Sem bytes da balança · confira COM4 / USE-P2 / cabo';
        dom.estavel.className = 'mt-1 text-xs font-bold text-red-700';
      }
      if (!lastRawHint) {
        setRxHint('(nenhum byte em 3s)');
      }
    }, 1200);
  }

  function stopSilenceWatch() {
    if (silenceTimer) {
      clearInterval(silenceTimer);
      silenceTimer = null;
    }
  }

  async function requestPortOrExplain() {
    try {
      return await navigator.serial.requestPort();
    } catch (err) {
      var message = err && err.message ? err.message : String(err);
      if (/No port selected/i.test(message)) {
        throw new Error(
          'Nenhuma porta escolhida. No seletor do Chrome, marque USB Serial Port (COM4) e confirme.'
        );
      }
      throw err;
    }
  }

  function resetReadState() {
    serialBuf = new Uint8Array(0);
    weightHistory = [];
    lastKg = null;
    isStable = false;
    hadBytes = false;
    lastAutoKey = '';
  }

  async function ensurePort(opts) {
    opts = opts || {};
    if (!('serial' in navigator)) {
      throw new Error(
        'Este Chrome não tem Web Serial. Abra em http://localhost no computador da balança (Windows + COM4).'
      );
    }

    if (opts.forcePick) {
      port = await requestPortOrExplain();
      savePortFingerprint(port);
      return port;
    }

    if (port) {
      return port;
    }

    var ports = await navigator.serial.getPorts();
    var saved = findSavedPort(ports);
    if (saved) {
      port = saved;
      return port;
    }

    if (ports && ports.length === 1) {
      port = ports[0];
      return port;
    }
    if (ports && ports.length > 1) {
      throw new Error(
        'Há várias portas COM. Toque em CONECTAR e escolha USB Serial Port (COM4).'
      );
    }
    port = await requestPortOrExplain();
    savePortFingerprint(port);
    return port;
  }

  async function openPort() {
    stopSimulator();
    var p = await ensurePort();
    if (!p.readable) {
      await p.open({
        baudRate: BAUD,
        dataBits: 8,
        parity: 'none',
        stopBits: stopBits,
        flowControl: 'none',
      });
    }
    scaleMode = 'serial';
    connected = true;
    resetReadState();
    setConnChip('COM ok', 'ok');
    setStatus('Balança conectada (USE-P2 · 9600 8N' + stopBits + ').', 'ok');
    if (dom.simChips) {
      dom.simChips.classList.add('hidden');
      dom.simChips.style.display = 'none';
    }
    startReadLoop();
    startPoll();
    startSilenceWatch();
  }

  async function closePortSoft() {
    stopPoll();
    stopSilenceWatch();
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
        writer.write(new Uint8Array([P2.ENQ || 0x05])).catch(function () {});
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
    /* Bytes crus — STX (0x02) não se perde como no TextDecoderStream. */
    reader = port.readable.getReader();
    try {
      while (readLoopActive) {
        var r = await reader.read();
        if (r.done) break;
        if (r.value) feedSerialBytes(r.value);
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
    var typedRaw = dom.codigo ? String(dom.codigo.value || '').trim() : '';
    setStatus('Buscando produto ' + num + '…');
    return resolveProduto(num, typedRaw).then(function (res) {
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
    if (!P2.canAddToCart) {
      return isStable && lastKg != null && lastKg >= MIN_KG;
    }
    return P2.canAddToCart({
      hasProduct: true,
      weightKg: lastKg,
      mode: currentMode(),
      minKg: MIN_KG,
    });
  }

  function maybeAutoAdd() {
    if (!isOpen()) return;
    var code = produtoAtual ? parseGranelCode(dom.codigo && dom.codigo.value) : null;
    var decision = P2.decideAutoAdd
      ? P2.decideAutoAdd({
          open: true,
          code: code,
          weightKg: lastKg,
          stable: isStable,
          lastKey: lastAutoKey,
          minKg: MIN_KG,
        })
      : { add: canAddNow() && isStable, nextKey: lastAutoKey };
    lastAutoKey = decision.nextKey;
    if (!decision.add) return;
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
    var kg = lastKg;
    if (kg == null || kg < MIN_KG) {
      setStatus('Peso mínimo 20 g. Coloque o produto no prato.', 'err');
      return;
    }
    if (!manual && !isStable) {
      setStatus('Aguarde o peso estabilizar.', 'err');
      return;
    }
    if (Date.now() - lastAddedAt < 700) return;
    if (busyAdd) return;
    busyAdd = true;
    if (codeNow != null && P2.autoAddKey) {
      lastAutoKey = P2.autoAddKey(codeNow, kg);
    }

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

  function stopSimulator() {
    if (simTimer) {
      clearInterval(simTimer);
      simTimer = null;
    }
  }

  function simTick() {
    var frame;
    if (simWeightKg === 0 && P2.hexToBytes && P2.USER_DUMP_HEX) {
      frame = P2.hexToBytes(P2.USER_DUMP_HEX);
    } else if (P2.encodeUseP2Frame) {
      frame = P2.encodeUseP2Frame({
        weightKg: simWeightKg,
        price: 6.9,
        total: Math.round(simWeightKg * 6.9 * 100) / 100,
      });
    } else {
      return;
    }
    feedSerialBytes(frame);
  }

  function startSimulator() {
    stopSimulator();
    closePortSoft().then(function () {
      scaleMode = 'sim';
      connected = true;
      resetReadState();
      simWeightKg = 0;
      setConnChip('SEM PORTA', 'ok');
      setStatus('Simulador · dump real USE-P2 (0,00 kg). Sem cabo.', 'ok');
      if (dom.simChips) {
        dom.simChips.classList.remove('hidden');
        dom.simChips.style.display = 'flex';
      }
      simTick();
      simTimer = setInterval(simTick, 180);
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
    lastAutoKey = '';
    var cfgBits = loadCfg().stopBits;
    if (cfgBits === 1 || cfgBits === 2) stopBits = cfgBits;
    if (dom.stopBits) dom.stopBits.value = String(stopBits);
    var labOpen = document.getElementById('pdv-balanca-stopbits-label');
    if (labOpen) labOpen.textContent = String(stopBits);
    openPort().catch(function (err) {
      connected = false;
      scaleMode = 'idle';
      setConnChip('Sem porta', 'warn');
      setStatus(
        (err && err.message) ||
          'Toque em CONECTAR (COM4) ou SEM PORTA para simular.',
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
      closePortSoft()
        .then(function () {
          var pClose = Promise.resolve();
          try {
            if (port && typeof port.close === 'function') {
              pClose = Promise.resolve(port.close()).catch(function () {});
            }
          } catch (eClose) {}
          return pClose.then(function () {
            port = null;
            scaleMode = 'idle';
            resetReadState();
            return ensurePort({ forcePick: true }).then(function () {
              return openPort();
            });
          });
        })
        .catch(function (err) {
          setStatus((err && err.message) || 'Porta não escolhida.', 'err');
          setConnChip('Sem porta', 'warn');
        });
    });
  }
  if (dom.semPorta) {
    dom.semPorta.addEventListener('click', function () {
      startSimulator();
    });
  }
  if (dom.stopBits) {
    var cfg0 = loadCfg();
    if (cfg0.stopBits === 1 || cfg0.stopBits === 2) {
      stopBits = cfg0.stopBits;
      dom.stopBits.value = String(stopBits);
    }
    dom.stopBits.addEventListener('change', function () {
      var n = Number(dom.stopBits.value);
      stopBits = n === 1 ? 1 : 2;
      saveCfg({ stopBits: stopBits });
      var lab = document.getElementById('pdv-balanca-stopbits-label');
      if (lab) lab.textContent = String(stopBits);
      setStatus('Stop bits ' + stopBits + '. Toque CONECTAR de novo se a porta já estava aberta.', 'ok');
    });
  }
  if (dom.simChips) {
    dom.simChips.addEventListener('click', function (ev) {
      var btn = ev.target && ev.target.closest ? ev.target.closest('[data-sim-kg]') : null;
      if (!btn) return;
      simWeightKg = Number(btn.getAttribute('data-sim-kg')) || 0;
      weightHistory = [];
      isStable = false;
      simTick();
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
      scaleMode = 'sim';
      connected = true;
      setConnChip('Mock', 'ok');
      var n = Number(kg) || 0;
      weightHistory = [];
      var t = Date.now();
      weightHistory = [
        { kg: n, at: t - 300 },
        { kg: n, at: t - 200 },
        { kg: n, at: t - 100 },
      ];
      lastKg = n;
      hadBytes = true;
      isStable = n >= MIN_KG;
      updatePesoUi();
    },
    mockFrame: function (frame) {
      scaleMode = 'sim';
      connected = true;
      setConnChip('Mock', 'ok');
      if (frame instanceof Uint8Array) feedSerialBytes(frame);
      else feedSerialText(String(frame || ''));
    },
    startSimulator: startSimulator,
  };
})();
