/**
 * Busca Catálogo Agro (BCA) + pacote local (ordem A).
 * API `/api/buscar/` · lista no PC: hoje → último bom → servidor.
 * Nunca apaga o último pacote bom se o servidor falhar.
 */
(function (w) {
    'use strict';

    var AGRO_BUSCA_CATALOGO = {
        nome: 'Busca Catálogo Agro',
        sigla: 'BCA',
        api: '/api/buscar/',
        motor: 'catalogo_agro.buscar',
        banco: 'postgres',
    };

    var LS_PACOTE = 'agro_bca_pacote_v1';
    var LS_LEGACY_PDV = 'agro_pdv_catalog_cache_v2';
    var DELTA_URL = '/api/todos-produtos/delta/';
    var SERVER_FAST_MS = 2000;
    var FETCH_TIMEOUT_MS = 25000;
    var MAX_LOCAL = 48;
    /** Não baixar o catálogo inteiro a cada tecla — isso engasgava o PDV (caixa/fechar venda). */
    var DELTA_MIN_INTERVAL_MS = 5 * 60 * 1000;

    var _mem = null;
    var _ensurePromise = null;
    var _lastDeltaAt = 0;
    var _statusListeners = [];

    w.AGRO_BUSCA_CATALOGO = AGRO_BUSCA_CATALOGO;

    function dayKey(d) {
        d = d || new Date();
        var y = d.getFullYear();
        var m = String(d.getMonth() + 1).padStart(2, '0');
        var day = String(d.getDate()).padStart(2, '0');
        return y + '-' + m + '-' + day;
    }

    function stripAccents(s) {
        try {
            return String(s || '')
                .normalize('NFD')
                .replace(/[\u0300-\u036f]/g, '')
                .toLowerCase();
        } catch (e) {
            return String(s || '').toLowerCase();
        }
    }

    function onlyDigits(s) {
        return String(s || '').replace(/\D/g, '');
    }

    function notifyStatus() {
        var st = getStatus();
        _statusListeners.forEach(function (fn) {
            try {
                fn(st);
            } catch (e) {}
        });
        try {
            w.dispatchEvent(new CustomEvent('agro-pacote-status', { detail: st }));
        } catch (e2) {}
        var chip = document.getElementById('agro-pacote-chip');
        if (chip) {
            chip.textContent = st.label;
            chip.className = st.chipClass;
            chip.title = st.hint;
            chip.hidden = false;
        }
    }

    function readJson(key) {
        try {
            var raw = localStorage.getItem(key);
            if (!raw) return null;
            return JSON.parse(raw);
        } catch (e) {
            return null;
        }
    }

    function writeJson(key, obj) {
        try {
            localStorage.setItem(key, JSON.stringify(obj));
            return true;
        } catch (e) {
            return false;
        }
    }

    function normalizeRows(produtos) {
        if (!Array.isArray(produtos)) return [];
        return produtos.filter(function (p) {
            return p && (p.id != null || p.Id != null);
        });
    }

    /**
     * Lê pacote: prioridade hoje → last no mesmo blob → legacy PDV.
     * Nunca remove LS_PACOTE aqui.
     */
    function getPacote() {
        if (_mem && _mem.produtos && _mem.produtos.length) return _mem;

        var p = readJson(LS_PACOTE);
        if (p && Array.isArray(p.produtos) && p.produtos.length) {
            var hoje = dayKey();
            var fonte = p.day === hoje ? 'hoje' : 'last';
            _mem = {
                produtos: p.produtos,
                catalog_version: p.catalog_version || '',
                catalog_updated_at: p.catalog_updated_at || '',
                saved_at: Number(p.saved_at || 0),
                day: p.day || '',
                fonte: fonte,
            };
            return _mem;
        }

        var leg = readJson(LS_LEGACY_PDV);
        if (leg && Array.isArray(leg.produtos) && leg.produtos.length) {
            _mem = {
                produtos: leg.produtos,
                catalog_version: leg.catalog_version || '',
                catalog_updated_at: leg.catalog_updated_at || '',
                saved_at: Number(leg.saved_at || 0),
                day: '',
                fonte: 'legacy',
            };
            return _mem;
        }

        return { produtos: [], fonte: 'none', saved_at: 0, catalog_version: '', day: '' };
    }

    function getStatus() {
        var p = getPacote();
        if (!p.produtos || !p.produtos.length) {
            return {
                level: 'red',
                label: 'só servidor',
                hint: 'Sem lista no PC — busca depende do servidor.',
                chipClass:
                    'inline-flex items-center rounded-md border border-red-300 bg-red-50 px-2 py-0.5 text-[10px] font-black uppercase tracking-wide text-red-800',
                n: 0,
                fonte: 'none',
            };
        }
        if (p.fonte === 'hoje') {
            return {
                level: 'green',
                label: 'lista de hoje',
                hint: 'Pacote local de hoje (' + p.produtos.length + ' itens).',
                chipClass:
                    'inline-flex items-center rounded-md border border-emerald-300 bg-emerald-50 px-2 py-0.5 text-[10px] font-black uppercase tracking-wide text-emerald-900',
                n: p.produtos.length,
                fonte: 'hoje',
            };
        }
        return {
            level: 'yellow',
            label: 'lista antiga',
            hint: 'Usando último pacote bom neste PC (' + p.produtos.length + ' itens). Servidor atualiza quando puder.',
            chipClass:
                'inline-flex items-center rounded-md border border-amber-300 bg-amber-50 px-2 py-0.5 text-[10px] font-black uppercase tracking-wide text-amber-900',
            n: p.produtos.length,
            fonte: p.fonte || 'last',
        };
    }

    /** Grava só se vier lista não vazia — nunca zera o last good. */
    function savePacoteFromServer(payload) {
        var rows = normalizeRows(payload && payload.produtos);
        if (!rows.length) return false;
        var entry = {
            day: dayKey(),
            saved_at: Date.now(),
            catalog_version: (payload && payload.catalog_version) || '',
            catalog_updated_at: (payload && payload.catalog_updated_at) || '',
            produtos: rows,
        };
        writeJson(LS_PACOTE, entry);
        /* Espelho PDV/Consulta — compatível com chaves antigas. */
        writeJson(LS_LEGACY_PDV, {
            saved_at: entry.saved_at,
            catalog_version: entry.catalog_version,
            catalog_updated_at: entry.catalog_updated_at,
            produtos: rows,
        });
        _mem = {
            produtos: rows,
            catalog_version: entry.catalog_version,
            catalog_updated_at: entry.catalog_updated_at,
            saved_at: entry.saved_at,
            day: entry.day,
            fonte: 'hoje',
        };
        notifyStatus();
        return true;
    }

    function scoreProduct(p, q) {
        var ql = stripAccents(q);
        if (!ql) return 0;
        var qd = onlyDigits(q);
        var nome = stripAccents(p.nome || '');
        var marca = stripAccents(p.marca || '');
        var cod = stripAccents(p.codigo || p.codigo_nfe || '');
        var ean = onlyDigits(p.codigo_barras || '');
        var bt = stripAccents(p.busca_texto || '');
        var s = 0;
        if (cod && (cod === ql || cod.indexOf(ql) === 0)) s += 120;
        if (qd.length >= 6 && ean && ean === qd) s += 200;
        if (qd.length >= 6 && ean && ean.indexOf(qd) === 0) s += 80;
        if (Array.isArray(p.index_codigos)) {
            for (var i = 0; i < p.index_codigos.length; i++) {
                var x = stripAccents(p.index_codigos[i]);
                var xd = onlyDigits(p.index_codigos[i]);
                if (x === ql) s += 110;
                else if (x.indexOf(ql) === 0) s += 70;
                if (qd.length >= 6 && xd === qd) s += 150;
            }
        }
        var tokens = ql.split(/\s+/).filter(Boolean);
        var hit = 0;
        tokens.forEach(function (t) {
            if (nome.indexOf(t) !== -1 || marca.indexOf(t) !== -1 || bt.indexOf(t) !== -1) hit++;
        });
        if (tokens.length && hit === tokens.length) s += 40 + hit * 8;
        else if (hit) s += hit * 5;
        if (nome.indexOf(ql) === 0) s += 25;
        return s;
    }

    function searchLocal(q, limit) {
        limit = limit || MAX_LOCAL;
        var termo = String(q || '').trim();
        if (!termo || termo.length < 2) return [];
        var pac = getPacote();
        var rows = pac.produtos || [];
        if (!rows.length) return [];
        return rows
            .map(function (p) {
                return { p: p, s: scoreProduct(p, termo) };
            })
            .filter(function (x) {
                return x.s > 0;
            })
            .sort(function (a, b) {
                if (b.s !== a.s) return b.s - a.s;
                return stripAccents(a.p.nome || '').localeCompare(stripAccents(b.p.nome || ''));
            })
            .slice(0, limit)
            .map(function (x) {
                return x.p;
            });
    }

    function fetchWithTimeout(url, opts, ms) {
        opts = opts || {};
        ms = ms || FETCH_TIMEOUT_MS;
        var ctrl = typeof AbortController !== 'undefined' ? new AbortController() : null;
        var timer = null;
        if (ctrl) {
            timer = setTimeout(function () {
                try {
                    ctrl.abort();
                } catch (e) {}
            }, ms);
            if (opts.signal) {
                opts.signal.addEventListener('abort', function () {
                    try {
                        ctrl.abort();
                    } catch (e2) {}
                });
            }
            opts = Object.assign({}, opts, { signal: ctrl.signal });
        }
        return fetch(url, opts).finally(function () {
            if (timer) clearTimeout(timer);
        });
    }

    function fetchDeltaFull() {
        return fetchWithTimeout(DELTA_URL, { credentials: 'same-origin' }, FETCH_TIMEOUT_MS)
            .then(function (r) {
                if (!r.ok) throw new Error('delta HTTP ' + r.status);
                return r.json();
            })
            .then(function (d) {
                if (d && Array.isArray(d.produtos) && d.produtos.length) {
                    savePacoteFromServer(d);
                    return d;
                }
                /* Fallback catálogo wizard */
                return fetchWithTimeout(
                    AGRO_BUSCA_CATALOGO.api + '?wizard=1&wizard_catalog=1',
                    { credentials: 'same-origin' },
                    FETCH_TIMEOUT_MS
                ).then(function (r2) {
                    if (!r2.ok) throw new Error('catalog HTTP ' + r2.status);
                    return r2.json();
                }).then(function (d2) {
                    if (d2 && Array.isArray(d2.produtos) && d2.produtos.length) {
                        savePacoteFromServer(d2);
                    }
                    return d2;
                });
            });
    }

    /**
     * Garante pacote em memória/LS.
     * Delta completo: no abrir / pacote fraco / no máx. a cada 5 min — NÃO a cada busca
     * (senão o worker trava e caixa/fechar venda demoram 1–2 s).
     */
    function ensurePacote(opts) {
        opts = opts || {};
        var cur = getPacote();
        var agora = Date.now();
        var temLista = !!(cur.produtos && cur.produtos.length);
        if (temLista && !opts.force) {
            notifyStatus();
            var deltaRecente =
                _lastDeltaAt > 0 && agora - _lastDeltaAt < DELTA_MIN_INTERVAL_MS;
            var precisaDelta = !!opts.force || !deltaRecente;
            if (precisaDelta && !_ensurePromise) {
                _lastDeltaAt = agora;
                _ensurePromise = fetchDeltaFull()
                    .catch(function () {
                        return null;
                    })
                    .finally(function () {
                        _ensurePromise = null;
                        notifyStatus();
                    });
            }
            return Promise.resolve(cur);
        }
        if (_ensurePromise) {
            return _ensurePromise.then(function () {
                return getPacote();
            });
        }
        _lastDeltaAt = agora;
        _ensurePromise = fetchDeltaFull()
            .catch(function () {
                return null;
            })
            .finally(function () {
                _ensurePromise = null;
                notifyStatus();
            });
        return _ensurePromise.then(function () {
            return getPacote();
        });
    }

    function patchPrecosNoPacote(serverRows) {
        if (!serverRows || !serverRows.length) return;
        var pac = getPacote();
        if (!pac.produtos || !pac.produtos.length) return;
        var map = {};
        pac.produtos.forEach(function (p) {
            if (p && p.id != null) map[String(p.id)] = p;
        });
        var changed = false;
        serverRows.forEach(function (s) {
            if (!s || s.id == null) return;
            var row = map[String(s.id)];
            if (!row) return;
            if (s.preco_venda != null && row.preco_venda !== s.preco_venda) {
                row.preco_venda = s.preco_venda;
                changed = true;
            }
            if (s.preco_custo != null && row.preco_custo !== s.preco_custo) {
                row.preco_custo = s.preco_custo;
                changed = true;
            }
            if (s.nome && !row.nome) {
                row.nome = s.nome;
                changed = true;
            }
        });
        if (changed) {
            savePacoteFromServer({
                produtos: pac.produtos,
                catalog_version: pac.catalog_version,
                catalog_updated_at: pac.catalog_updated_at,
            });
        }
    }

    /** Inclui produtos novos do servidor no pacote local (evita «lista local» incompleta). */
    function mergeProdutosNoPacote(serverRows) {
        if (!serverRows || !serverRows.length) return 0;
        var pac = getPacote();
        var map = {};
        var list = (pac.produtos || []).slice();
        list.forEach(function (p) {
            if (p && p.id != null) map[String(p.id)] = p;
        });
        var added = 0;
        serverRows.forEach(function (s) {
            if (!s || s.id == null) return;
            var id = String(s.id);
            if (map[id]) {
                if (s.preco_venda != null) map[id].preco_venda = s.preco_venda;
                if (s.preco_custo != null) map[id].preco_custo = s.preco_custo;
                if (s.nome) map[id].nome = s.nome;
                return;
            }
            list.push(s);
            map[id] = s;
            added++;
        });
        if (added || list.length) {
            savePacoteFromServer({
                produtos: list,
                catalog_version: pac.catalog_version,
                catalog_updated_at: pac.catalog_updated_at,
            });
        }
        return added;
    }

    function buildBuscaUrl(q, opts) {
        opts = opts || {};
        var params = new URLSearchParams();
        var termo = String(q || '').trim();
        if (termo) params.set('q', termo);
        if (opts.limit != null) params.set('limit', String(opts.limit));
        if (opts.contexto) params.set('contexto', String(opts.contexto));
        if (opts.compras) params.set('compras', '1');
        if (opts.wizard) params.set('wizard', '1');
        if (opts.incluir_saldo) params.set('incluir_saldo', '1');
        if (opts.ativo) params.set('ativo', '1');
        if (opts.inativos) params.set('inativos', '1');
        if (opts.entrada_nfe) params.set('entrada_nfe', '1');
        var extra = opts.extra || {};
        Object.keys(extra).forEach(function (k) {
            var v = extra[k];
            if (v == null || v === '') return;
            if (Array.isArray(v)) {
                v.forEach(function (item) {
                    if (item != null && item !== '') params.append(k, String(item));
                });
                return;
            }
            params.set(k, String(v));
        });
        return AGRO_BUSCA_CATALOGO.api + '?' + params.toString();
    }

    function fetchServidor(q, opts) {
        var url = buildBuscaUrl(q, opts);
        return fetchWithTimeout(
            url,
            { credentials: 'same-origin', signal: opts.signal || null },
            opts.timeoutMs || FETCH_TIMEOUT_MS
        ).then(function (r) {
            return r.json();
        });
    }

    /**
     * Ordem A na busca:
     * 1) pacote local (instantâneo)
     * 2) se servidor < waitMs → usa resposta do servidor (+ merge no pacote)
     * 3) se servidor lento → devolve local; quando o servidor chegar, atualiza pacote + callback
     * 4) sem local → espera servidor (com timeout)
     * Cadastro: preferServer / serverWaitMs maior (lista completa > velocidade).
     */
    w.fetchAgroBuscaCatalogo = function (q, opts) {
        opts = opts || {};
        var termo = String(q || '').trim();
        var limit = opts.limit != null ? opts.limit : MAX_LOCAL;
        var skipLocal = !!opts.skipLocal;
        var preferServer = !!opts.preferServer || String(opts.contexto || '') === 'cadastro';
        var waitMs = preferServer
            ? Math.max(SERVER_FAST_MS, Number(opts.serverWaitMs) || 12000)
            : SERVER_FAST_MS;

        ensurePacote({ force: false });

        var localRows = skipLocal ? [] : searchLocal(termo, limit);
        var st = getStatus();

        function applyServerRows(j) {
            var srv = j && Array.isArray(j.produtos) ? j.produtos : [];
            if (srv.length) {
                mergeProdutosNoPacote(srv);
            } else if (j && Array.isArray(j.produtos)) {
                patchPrecosNoPacote(j.produtos);
            }
            if (j && j.ok === undefined) j.ok = true;
            if (j) {
                j.pacote_fonte = st.fonte;
                j.pacote_level = getStatus().level;
                j.pacote_fallback = false;
            }
            return j;
        }

        function notifyLate(j) {
            try {
                if (typeof opts.onServerLate === 'function') opts.onServerLate(j);
            } catch (e1) {}
            try {
                w.dispatchEvent(
                    new CustomEvent('agro-busca-catalogo-tardio', {
                        detail: { q: termo, data: j, contexto: opts.contexto || '' },
                    })
                );
            } catch (e2) {}
        }

        var serverP = fetchServidor(termo, opts).then(applyServerRows);

        if (!termo) return serverP;

        if (!localRows.length) {
            return serverP.catch(function (err) {
                var msg = (err && err.message) || 'Falha na busca';
                var name = (err && err.name) || '';
                if (
                    name === 'AbortError' ||
                    /aborted|abort|timeout|timed?\s*out/i.test(String(msg))
                ) {
                    msg =
                        'Busca demorou demais (servidor ocupado). Feche abas extras e tente de novo.';
                }
                return {
                    ok: false,
                    produtos: [],
                    erro: msg,
                    pacote_fonte: st.fonte,
                    pacote_level: st.level,
                };
            });
        }

        return new Promise(function (resolve) {
            var settled = false;
            var timer = setTimeout(function () {
                if (settled) return;
                settled = true;
                resolve({
                    ok: true,
                    produtos: localRows,
                    exact_barcode_match: false,
                    motor: 'pacote_local',
                    pacote_fonte: st.fonte,
                    pacote_level: st.level,
                    pacote_fallback: true,
                });
            }, waitMs);

            serverP
                .then(function (j) {
                    var srv = j && Array.isArray(j.produtos) ? j.produtos : [];
                    if (settled) {
                        if (srv.length) notifyLate(j);
                        return;
                    }
                    settled = true;
                    clearTimeout(timer);
                    if (srv.length) {
                        resolve(j);
                        return;
                    }
                    resolve({
                        ok: true,
                        produtos: localRows,
                        exact_barcode_match: false,
                        motor: 'pacote_local',
                        pacote_fonte: st.fonte,
                        pacote_level: st.level,
                        pacote_fallback: true,
                    });
                })
                .catch(function () {
                    if (settled) return;
                    settled = true;
                    clearTimeout(timer);
                    resolve({
                        ok: true,
                        produtos: localRows,
                        exact_barcode_match: false,
                        motor: 'pacote_local',
                        pacote_fonte: st.fonte,
                        pacote_level: st.level,
                        pacote_fallback: true,
                    });
                });
        });
    };

    w.agroTermoBuscaCatalogoServidor = function (termoBruto, modo) {
        var bruto = String(termoBruto || '').trim();
        if (!bruto) return false;
        if (bruto.toLowerCase() === '#prova' || bruto.indexOf('#') === 0) return true;
        if (modo === 'scanner') return false;
        if (typeof w.normalizarBuscaLocal !== 'function') return bruto.length >= 2;
        var norm = w.normalizarBuscaLocal(bruto);
        if (norm.length >= 2) return true;
        if (typeof w.pareceCodigoGmEtiqueta === 'function' && w.pareceCodigoGmEtiqueta(norm)) return true;
        if (typeof w.pareceCodigoBarrasNumerico === 'function' && w.pareceCodigoBarrasNumerico(norm)) return true;
        return false;
    };

    w.agroStatusTextoBuscaCatalogo = function (data, qtdVisivel) {
        var prova = data && data.prova_unificada;
        if (prova && prova.ok) {
            return 'Prova OK · ' + (prova.mensagem || AGRO_BUSCA_CATALOGO.nome);
        }
        var n = qtdVisivel != null ? qtdVisivel : data && data.produtos ? data.produtos.length : 0;
        var base = AGRO_BUSCA_CATALOGO.sigla + ' · ' + n + ' produto(s)';
        if (data && data.pacote_fallback) return base + ' · lista local';
        if (data && data.pacote_level === 'yellow') return base + ' · lista antiga';
        if (data && data.pacote_level === 'green') return base + ' · lista hoje';
        return base;
    };

    w.agroProdutoIdProvaUnificada = function (id) {
        return String(id || '') === '__prova_unificada__';
    };

    w.AgroPacoteCatalogo = {
        LS_PACOTE: LS_PACOTE,
        getPacote: getPacote,
        getStatus: getStatus,
        ensure: ensurePacote,
        saveFromServer: savePacoteFromServer,
        searchLocal: searchLocal,
        onStatus: function (fn) {
            if (typeof fn === 'function') _statusListeners.push(fn);
        },
        refreshChip: notifyStatus,
    };

    /* Boot leve: se já tem last good, status amarelo; tenta hoje em background. */
    try {
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', function () {
                ensurePacote({ force: false });
                notifyStatus();
            });
        } else {
            ensurePacote({ force: false });
            notifyStatus();
        }
    } catch (eBoot) {}
})(window);
