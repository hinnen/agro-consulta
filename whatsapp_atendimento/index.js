/**
 * Ponte WhatsApp (QR no celular) → Django Agro Consulta.
 * Uso próprio, volume baixo, sem disparo. Deixe o processo ligado.
 */
import { Boom } from "@hapi/boom";
import makeWASocket, {
  DisconnectReason,
  downloadContentFromMessage,
  downloadMediaMessage,
  fetchLatestBaileysVersion,
  normalizeMessageContent,
  useMultiFileAuthState,
} from "@whiskeysockets/baileys";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import pino from "pino";
import QRCode from "qrcode";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
carregarEnv(path.join(__dirname, ".env"));
carregarEnv(path.join(__dirname, "..", ".env"));

const BASE = (process.env.AGRO_WA_DJANGO_URL || "http://127.0.0.1:8000").replace(/\/+$/, "");
const TOKEN = process.env.AGRO_WA_BRIDGE_TOKEN || "gm-agro-wa-ponte-local";
const AUTH = path.join(__dirname, "auth");
const AGENDA_FILE = path.join(__dirname, "contatos_agenda.json");
const LID_FILE = path.join(__dirname, "lid_map.json");
const LOCK_FILE = path.join(__dirname, ".ponte.lock");
const logger = pino({ level: "silent" });
let salvarAgendaTimer = 0;
let enviarAgendaTimer = 0;
let salvarLidTimer = 0;

function carregarEnv(fp) {
  try {
    if (!fs.existsSync(fp)) return;
    const txt = fs.readFileSync(fp, "utf8");
    for (const line of txt.split(/\r?\n/)) {
      const s = line.trim();
      if (!s || s.startsWith("#") || !s.includes("=")) continue;
      const i = s.indexOf("=");
      const k = s.slice(0, i).trim();
      let v = s.slice(i + 1).trim();
      if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) {
        v = v.slice(1, -1);
      }
      if (k && process.env[k] == null) process.env[k] = v;
    }
  } catch {
    /* ignore */
  }
}

function garantirUmaInstancia() {
  if (fs.existsSync(LOCK_FILE)) {
    try {
      const pid = parseInt(String(fs.readFileSync(LOCK_FILE, "utf8")).trim(), 10);
      if (pid > 0) process.kill(pid, 0);
      console.error("");
      console.error("ERRO: Ja existe outra ponte WhatsApp neste PC (PID " + pid + ").");
      console.error("Feche a outra janela do iniciar.bat e abra so uma.");
      console.error("");
      process.exit(1);
    } catch {
      try {
        fs.unlinkSync(LOCK_FILE);
      } catch {
        /* ignore */
      }
    }
  }
  fs.writeFileSync(LOCK_FILE, String(process.pid));
  const limpar = () => {
    try {
      fs.unlinkSync(LOCK_FILE);
    } catch {
      /* ignore */
    }
  };
  process.on("exit", limpar);
  process.on("SIGINT", () => {
    limpar();
    process.exit(0);
  });
  process.on("SIGTERM", () => {
    limpar();
    process.exit(0);
  });
}

function headersAuth() {
  return { "X-Agro-Wa-Token": TOKEN };
}

function headersJson() {
  return {
    "Content-Type": "application/json",
    "X-Agro-Wa-Token": TOKEN,
  };
}

async function post(url, body) {
  const r = await fetch(BASE + url, {
    method: "POST",
    headers: headersJson(),
    body: JSON.stringify(body || {}),
  });
  if (!r.ok) {
    const t = await r.text();
    throw new Error(`HTTP ${r.status} ${url} ${t.slice(0, 180)}`);
  }
  return r.json().catch(() => ({}));
}

async function get(url) {
  const r = await fetch(BASE + url, { headers: headersJson() });
  if (!r.ok) {
    const t = await r.text();
    throw new Error(`HTTP ${r.status} ${url} ${t.slice(0, 180)}`);
  }
  return r.json().catch(() => ({}));
}

function conteudoDe(msg) {
  try {
    return normalizeMessageContent(msg && msg.message) || (msg && msg.message) || null;
  } catch {
    return (msg && msg.message) || null;
  }
}

function textoDe(msg) {
  const m = conteudoDe(msg);
  if (!m) return "";
  if (m.conversation) return String(m.conversation);
  if (m.extendedTextMessage && m.extendedTextMessage.text) return String(m.extendedTextMessage.text);
  if (m.imageMessage) return String(m.imageMessage.caption || "[imagem]");
  if (m.videoMessage) return String(m.videoMessage.caption || "[vídeo]");
  if (m.audioMessage) return "[áudio]";
  if (m.documentMessage) return String(m.documentMessage.fileName || "[arquivo]");
  if (m.stickerMessage) return "[figurinha]";
  return "";
}

let sock = null;
let pollTimer = null;
let lastPairing = "";
let connId = 0;
let primeiraLigacao = true;
const agenda = new Map();
const lidParaJid = new Map();
const msgCache = new Map();
const HIST_MS = 7 * 24 * 60 * 60 * 1000;
const LIVE_MS = 20 * 60 * 1000;
/** Só aceita append antigo quando a loja pediu «Anteriores» neste chat. */
let histJid = "";
let histAte = 0;

function historicoPermitido(jid) {
  return histJid && String(jid) === histJid && Date.now() < histAte;
}

function tsMs(m) {
  const raw = m && (m.messageTimestamp || m.timestamp);
  const n = Number(raw || 0);
  if (!n) return Date.now();
  return n > 1e12 ? n : n * 1000;
}

function ehMensagemAoVivo(m, type) {
  if (type === "notify") return true;
  if (type !== "append") return false;
  const age = Date.now() - tsMs(m);
  return age >= 0 && age < LIVE_MS;
}

function guardarMsgCache(m) {
  const id = m && m.key && m.key.id;
  if (!id) return;
  msgCache.set(id, m);
  if (msgCache.size > 400) {
    const first = msgCache.keys().next().value;
    msgCache.delete(first);
  }
}

function ehChatPrivado(jid) {
  const j = String(jid || "").toLowerCase();
  if (j.includes("@g.us") || j.includes("@newsletter") || j.includes("@broadcast")) return false;
  const num = j.split("@")[0].replace(/\D/g, "");
  if (!num) return false;
  if (j.endsWith("@lid")) return num.length >= 6 && num.length <= 22;
  if (!j.endsWith("@s.whatsapp.net")) return false;
  if (num.startsWith("120") && num.length >= 15) return false;
  return num.length >= 10 && num.length <= 13;
}

function jidPhoneDeValor(v) {
  const s = String(v || "").trim();
  if (!s) return "";
  const low = s.toLowerCase();
  if (low.endsWith("@lid")) return "";
  if (low.endsWith("@s.whatsapp.net")) return ehChatPrivado(s) ? s : "";
  const d = s.replace(/\D+/g, "");
  if (d.length >= 10 && d.length <= 13) return d + "@s.whatsapp.net";
  return "";
}

function pnDeLid(lid) {
  const L = String(lid || "");
  if (!L.endsWith("@lid")) return "";
  if (lidParaJid.has(L)) return lidParaJid.get(L);
  try {
    const map = sock && sock.signalRepository && sock.signalRepository.lidMapping;
    if (map && typeof map.getPNForLID === "function") {
      const pn = map.getPNForLID(L);
      const j = jidPhoneDeValor(pn);
      if (j) {
        lidParaJid.set(L, j);
        salvarLidDebounced();
        return j;
      }
    }
  } catch {
    /* ignore */
  }
  return "";
}

function telefoneDeJid(jid) {
  return (jidPhoneDeValor(jid) || "").split("@")[0].replace(/\D/g, "");
}

function lidDeContato(c) {
  if (!c) return "";
  for (const x of [c.lid, c.id, c.jid]) {
    const s = String(x || "");
    if (s.endsWith("@lid")) return s;
  }
  return "";
}

function jidDeContato(c) {
  if (!c) return "";
  for (const x of [c.phoneNumber, c.pn, c.jid, c.id]) {
    const pn = jidPhoneDeValor(x);
    if (pn) return pn;
  }
  const lid = lidDeContato(c);
  const mapped = lid && pnDeLid(lid);
  if (mapped) return mapped;
  if (lid && ehChatPrivado(lid)) return lid;
  return "";
}

function enviarLidMap() {
  const lids = {};
  for (const [lid, phone] of lidParaJid.entries()) lids[lid] = phone;
  if (!Object.keys(lids).length) return;
  post("/api/atendimento-whatsapp/bridge/lids/", { lids }).catch(() => {});
}

function salvarLidDebounced() {
  if (salvarLidTimer) clearTimeout(salvarLidTimer);
  salvarLidTimer = setTimeout(() => {
    salvarLidTimer = 0;
    try {
      const obj = {};
      for (const [lid, phone] of lidParaJid.entries()) obj[lid] = phone;
      fs.writeFileSync(LID_FILE, JSON.stringify(obj));
    } catch {
      /* ignore */
    }
    enviarLidMap();
  }, 400);
}

function carregarLidMap() {
  try {
    if (!fs.existsSync(LID_FILE)) return;
    const data = JSON.parse(fs.readFileSync(LID_FILE, "utf8"));
    for (const [lid, phone] of Object.entries(data || {})) {
      if (String(lid).endsWith("@lid") && ehChatPrivado(phone)) lidParaJid.set(lid, phone);
    }
  } catch {
    /* ignore */
  }
}

function vincularLid(c) {
  const lid = lidDeContato(c);
  let phone = "";
  for (const x of [(c && c.phoneNumber) || "", (c && c.pn) || "", (c && c.jid) || "", (c && c.id) || ""]) {
    phone = jidPhoneDeValor(x);
    if (phone) break;
  }
  if (!phone && lid) phone = pnDeLid(lid);
  if (lid.endsWith("@lid") && phone && !phone.endsWith("@lid")) {
    lidParaJid.set(lid, phone);
    salvarLidDebounced();
  }
}

function telefoneDeKey(key) {
  if (!key) return "";
  const cands = [key.senderPn, key.participantPn, key.remoteJidAlt, key.participantAlt, key.peerRecipientPn];
  for (const c of cands) {
    const j = jidPhoneDeValor(c);
    if (j) return j;
  }
  return "";
}

function jidDaMensagem(m) {
  const key = m && m.key;
  if (!key) return "";
  let jid = String(key.remoteJid || "");
  const phone = telefoneDeKey(key) || pnDeLid(jid);
  if (jid.endsWith("@lid")) {
    if (phone) {
      lidParaJid.set(jid, phone);
      salvarLidDebounced();
      jid = phone;
    } else if (lidParaJid.has(jid)) {
      jid = lidParaJid.get(jid);
    }
  }
  if (phone) return phone;
  if (!ehChatPrivado(jid) && key.participant) {
    const p = String(key.participant);
    if (ehChatPrivado(p)) jid = p;
  }
  return jid;
}

function nomeDeContato(c, prev) {
  const salvo = String((c && c.name) || "").trim();
  const zap = String((c && c.notify) || (c && c.verifiedName) || "").trim();
  if (salvo) return salvo.slice(0, 120);
  if (zap) return zap.slice(0, 120);
  return (prev && prev.nome) || "";
}

function carregarAgenda() {
  try {
    if (!fs.existsSync(AGENDA_FILE)) return;
    const data = JSON.parse(fs.readFileSync(AGENDA_FILE, "utf8"));
    for (const [jid, v] of Object.entries(data || {})) {
      if (ehChatPrivado(jid) && v && typeof v === "object") {
        agenda.set(jid, {
          nome: String(v.nome || "").slice(0, 120),
          telefone: String(v.telefone || jid.split("@")[0]),
        });
      }
    }
  } catch {
    /* ignore */
  }
}

function salvarAgendaDebounced() {
  if (salvarAgendaTimer) clearTimeout(salvarAgendaTimer);
  salvarAgendaTimer = setTimeout(() => {
    salvarAgendaTimer = 0;
    try {
      const obj = {};
      for (const [jid, v] of agenda.entries()) obj[jid] = v;
      fs.writeFileSync(AGENDA_FILE, JSON.stringify(obj));
    } catch {
      /* ignore */
    }
  }, 800);
}

function agendarEnvioAgenda() {
  if (enviarAgendaTimer) clearTimeout(enviarAgendaTimer);
  enviarAgendaTimer = setTimeout(() => {
    enviarAgendaTimer = 0;
    enviarAgenda(0).catch(() => {});
  }, 4000);
}

function guardarContato(c) {
  if (!c) return;
  vincularLid(c);
  const jid = jidDeContato(c);
  if (!ehChatPrivado(jid)) return;
  if (agenda.size >= 2000 && !agenda.has(jid)) return;
  const prev = agenda.get(jid);
  const nome = nomeDeContato(c, prev);
  if (prev && prev.nome && !nome) return;
  agenda.set(jid, { nome: nome || (prev && prev.nome) || "", telefone: jid.split("@")[0] });
  salvarAgendaDebounced();
  agendarEnvioAgenda();
}

function tipoMidiaDe(msg) {
  const m = conteudoDe(msg);
  if (!m) return "";
  if (m.imageMessage) return "image";
  if (m.audioMessage) return "audio";
  if (m.stickerMessage) return "sticker";
  if (m.videoMessage) return "video";
  if (m.documentMessage) return "document";
  return "";
}

function mimeDe(msg) {
  const m = conteudoDe(msg);
  if (!m) return "";
  if (m.imageMessage) return String(m.imageMessage.mimetype || "");
  if (m.audioMessage) return String(m.audioMessage.mimetype || "");
  if (m.stickerMessage) return String(m.stickerMessage.mimetype || "image/webp");
  if (m.videoMessage) return String(m.videoMessage.mimetype || "");
  if (m.documentMessage) return String(m.documentMessage.mimetype || "");
  return "";
}

async function streamParaBuf(stream) {
  const chunks = [];
  for await (const c of stream) chunks.push(c);
  return Buffer.concat(chunks);
}

async function baixarMidia(m) {
  const tipo = tipoMidiaDe(m);
  if (!["image", "audio", "sticker"].includes(tipo)) {
    return { tipo, b64: "", mime: mimeDe(m) };
  }
  const inner = conteudoDe(m);
  const node =
    (inner && (inner.imageMessage || inner.audioMessage || inner.stickerMessage)) || null;
  const kind = tipo === "sticker" ? "sticker" : tipo;
  for (let i = 0; i < 4; i++) {
    try {
      let buf = null;
      if (node && (node.mediaKey || node.url || node.directPath)) {
        const stream = await downloadContentFromMessage(node, kind, {});
        buf = await streamParaBuf(stream);
      }
      if (!buf || !buf.length) {
        buf = await downloadMediaMessage(
          m,
          "buffer",
          {},
          { logger, reuploadRequest: sock.updateMediaMessage }
        );
      }
      if (buf && buf.length && buf.length <= 6000000) {
        return { tipo, b64: Buffer.from(buf).toString("base64"), mime: mimeDe(m) };
      }
    } catch (e) {
      console.error("midia:", e.message || e);
    }
    await new Promise((r) => setTimeout(r, 500 * (i + 1)));
  }
  return { tipo, b64: "", mime: mimeDe(m) };
}

async function baixarSaidaArquivo(id) {
  const r = await fetch(BASE + "/api/atendimento-whatsapp/bridge/midia/" + id + "/", {
    headers: headersAuth(),
  });
  if (!r.ok) return null;
  const buf = Buffer.from(await r.arrayBuffer());
  return buf.length ? buf : null;
}

async function enviarEntrada(m, extra) {
  if (!m || !(m.message || conteudoDe(m))) return;
  const key = m.key || {};
  const orig = String(key.remoteJid || "");
  const lid = orig.endsWith("@lid") ? orig : "";
  let jid = jidDaMensagem(m);
  if (jid.endsWith("@lid")) {
    const pn = pnDeLid(jid);
    if (pn) jid = pn;
  }
  if (!ehChatPrivado(jid)) return;
  const quando = tsMs(m);
  const historico = !!(extra && extra.historico);
  if (historico && Date.now() - quando > HIST_MS) return;
  const texto = textoDe(m);
  const tipo = tipoMidiaDe(m);
  if (!texto && !tipo) return;
  const nome = String(m.pushName || "").slice(0, 120);
  const waId = String((m.key && m.key.id) || "");
  const midia = await baixarMidia(m);
  const tel = telefoneDeJid(jid) || telefoneDeJid(telefoneDeKey(key));
  await post("/api/atendimento-whatsapp/bridge/entrada/", {
    jid,
    jid_lid: lid,
    telefone: tel,
    texto,
    nome,
    wa_id: waId,
    historico,
    de_mim: !!(m.key && m.key.fromMe),
    ts: Math.floor(quando / 1000),
    tipo_midia: midia.tipo || tipo,
    midia_b64: midia.b64 || "",
    mime: midia.mime || "",
    nome_arquivo: String((conteudoDe(m) && conteudoDe(m).documentMessage && conteudoDe(m).documentMessage.fileName) || ""),
  });
}

async function estado(payload) {
  try {
    await post("/api/atendimento-whatsapp/bridge/estado/", payload);
  } catch (e) {
    console.error("estado:", e.message || e);
  }
}

function fecharSockAntigo() {
  const s = sock;
  sock = null;
  if (!s) return;
  try {
    s.ev.removeAllListeners();
  } catch {
    /* ignore */
  }
  try {
    if (s.ws && typeof s.ws.close === "function") s.ws.close();
  } catch {
    /* ignore */
  }
}

async function ligar() {
  const myId = ++connId;
  fecharSockAntigo();
  if (pollTimer) {
    clearInterval(pollTimer);
    pollTimer = null;
  }
  fs.mkdirSync(AUTH, { recursive: true });
  if (primeiraLigacao) {
    await estado({ status: "desconectado", aviso: "Ligando WhatsApp…" });
  }
  const { state, saveCreds } = await useMultiFileAuthState(AUTH);
  const { version } = await fetchLatestBaileysVersion();
  sock = makeWASocket({
    version,
    auth: state,
    logger,
    browser: ["Agro Consulta", "Chrome", "20.33"],
    markOnlineOnConnect: false,
    syncFullHistory: false,
    shouldSyncHistoryMessage: () => false,
    getMessage: async (key) => {
      const id = key && key.id;
      const hit = id && msgCache.get(id);
      return (hit && hit.message) || undefined;
    },
  });
  sock.ev.on("creds.update", saveCreds);
  sock.ev.on("connection.update", async (u) => {
    if (myId !== connId) return;
    const { connection, lastDisconnect, qr } = u;
    if (qr) {
      const dataUrl = await QRCode.toDataURL(qr, { margin: 1, width: 320 });
      await estado({
        status: "qr",
        qr: dataUrl,
        pairing_code: lastPairing,
        aviso: lastPairing ? "Digite o código no celular (sem câmera)" : "Leia o QR ou use o código",
      });
      console.log("QR enviado ao Agro. Abra /atendimento-whatsapp/");
    }
    if (connection === "open") {
      primeiraLigacao = false;
      lastPairing = "";
      const me = (sock.user && (sock.user.id || sock.user.lid)) || "";
      const numero = String(me).split(":")[0].split("@")[0];
      await estado({ status: "conectado", numero, aviso: "" });
      console.log("WhatsApp conectado", numero);
      enviarLidMap();
      setTimeout(() => {
        varrerStore();
        enviarAgenda(0).catch(() => {});
      }, 4000);
    }
    if (connection === "close") {
      if (myId !== connId) return;
      const err = lastDisconnect && lastDisconnect.error;
      const code = err instanceof Boom ? err.output.statusCode : 0;
      const loggedOut = code === DisconnectReason.loggedOut;
      console.error("WhatsApp caiu", code || "", err && (err.message || err));
      await estado({
        status: "desconectado",
        aviso: loggedOut ? "Desconectou. Escaneie o QR de novo." : "Reconectando…",
      });
      fecharSockAntigo();
      if (loggedOut) {
        primeiraLigacao = true;
        try {
          fs.rmSync(AUTH, { recursive: true, force: true });
        } catch {
          /* ignore */
        }
      }
      setTimeout(() => {
        if (myId === connId) ligar();
      }, loggedOut ? 2000 : 5000);
    }
  });
  sock.ev.on("contacts.upsert", (lista) => {
    for (const c of lista || []) guardarContato(c);
  });
  sock.ev.on("contacts.update", (lista) => {
    for (const c of lista || []) guardarContato(c);
  });
  sock.ev.on("contacts.set", (dados) => {
    const lista = (dados && dados.contacts) || dados || [];
    for (const c of lista || []) guardarContato(c);
  });
  sock.ev.on("chats.upsert", (lista) => {
    for (const ch of lista || []) {
      guardarContato({ id: ch.id, jid: ch.jid || ch.id, name: ch.name, notify: ch.notify || ch.name });
    }
  });
  sock.ev.on("chats.phoneNumberShare", ({ lid, jid }) => {
    const jj = String(jid || "");
    const lj = String(lid || "");
    if (lj.endsWith("@lid") && ehChatPrivado(jj)) {
      lidParaJid.set(lj, jj);
      salvarLidDebounced();
    }
    if (!ehChatPrivado(jj)) return;
    const prev = agenda.get(lj) || agenda.get(jj);
    if (!prev) return;
    if (lj) agenda.delete(lj);
    agenda.set(jj, { nome: prev.nome || "", telefone: jj.split("@")[0] });
    salvarAgendaDebounced();
  });
  sock.ev.on("messaging-history.set", async ({ contacts, chats }) => {
    for (const c of contacts || []) guardarContato(c);
    for (const ch of chats || []) {
      guardarContato({ id: ch.id, jid: ch.jid || ch.id, name: ch.name, notify: ch.notify || ch.name });
    }
  });
  sock.ev.on("messages.upsert", async ({ messages, type }) => {
    if (type !== "notify" && type !== "append") return;
    for (const m of messages || []) {
      try {
        guardarMsgCache(m);
        const jid = jidDaMensagem(m);
        if (ehMensagemAoVivo(m, type)) {
          await enviarEntrada(m, { historico: false });
          continue;
        }
        if (!historicoPermitido(jid)) continue;
        const quando = tsMs(m);
        if (Date.now() - quando > HIST_MS) continue;
        await enviarEntrada(m, { historico: true });
      } catch (e) {
        console.error("entrada:", e.message || e);
      }
    }
  });
  pollTimer = setInterval(puxarSaida, 2500);
}

function varrerStore() {
  try {
    const st = sock && sock.store;
    if (!st) return;
    for (const c of Object.values(st.contacts || {})) guardarContato(c);
    const chats = st.chats;
    const lista = chats && typeof chats.all === "function" ? chats.all() : Object.values(chats || {});
    for (const ch of lista || []) {
      guardarContato({
        id: ch.id,
        jid: ch.jid || ch.id,
        name: ch.name,
        notify: ch.notify || ch.name,
      });
    }
  } catch {
    /* ignore */
  }
}

async function enviarAgenda(pedidoId) {
  const itens = [];
  for (const [jid, v] of agenda.entries()) {
    const lid = jid.endsWith("@lid") ? jid : "";
    const phone = jid.endsWith("@s.whatsapp.net") ? jid : pnDeLid(jid);
    const dest = phone || jid;
    if (!dest.endsWith("@s.whatsapp.net") && !dest.endsWith("@lid")) continue;
    itens.push({
      jid: dest,
      jid_lid: lid,
      nome: String((v && v.nome) || "").slice(0, 120),
      telefone: String((v && v.telefone) || (phone || "").split("@")[0] || ""),
    });
    if (itens.length >= 2000) break;
  }
  if (!itens.length && !pedidoId) return;
  await post("/api/atendimento-whatsapp/bridge/contatos/", { pedido_id: pedidoId || 0, itens });
}

async function executarPedido(p) {
  if (!sock || !p) return;
  const pid = p.id;
  try {
    if (p.tipo === "contatos") {
      await enviarAgenda(pid);
      return;
    }
    if (p.tipo === "pairing") {
      let tel = String(p.telefone || "").replace(/\D/g, "");
      if (tel.length === 10 || tel.length === 11) tel = "55" + tel;
      if (tel.length < 12 || tel.length > 13) {
        throw new Error("Número inválido");
      }
      if (sock.authState && sock.authState.creds && sock.authState.creds.registered) {
        throw new Error("Já está ligado neste PC.");
      }
      const raw = await sock.requestPairingCode(tel);
      const code = String(raw || "").replace(/[^A-Za-z0-9]/g, "").toUpperCase();
      lastPairing = code.replace(/(.{4})/g, "$1-").replace(/-$/, "");
      await estado({
        status: "qr",
        pairing_code: lastPairing,
        aviso: "No celular: Vincular com número de telefone",
      });
      console.log("Código de ligação:", lastPairing);
      await post("/api/atendimento-whatsapp/bridge/pedido-ok/", { pedido_id: pid });
      return;
    }
    if (p.tipo === "historico") {
      const count = Math.min(40, Math.max(5, Number(p.count) || 30));
      histJid = String(p.jid || "");
      histAte = Date.now() + 120000;
      await sock.fetchMessageHistory(
        count,
        {
          remoteJid: String(p.jid || ""),
          id: String(p.oldest_id || ""),
          fromMe: !!p.oldest_from_me,
        },
        Number(p.oldest_ts) || Date.now()
      );
      await post("/api/atendimento-whatsapp/bridge/pedido-ok/", { pedido_id: pid });
      return;
    }
  } catch (e) {
    console.error("pedido:", e.message || e);
    await post("/api/atendimento-whatsapp/bridge/pedido-ok/", {
      pedido_id: pid,
      erro: String(e.message || e).slice(0, 180),
    });
  }
}

function lidDePhone(phoneJid) {
  const j = jidPhoneDeValor(phoneJid) || String(phoneJid || "");
  if (!j || j.endsWith("@lid")) return "";
  for (const [lid, pn] of lidParaJid.entries()) {
    if (pn === j || jidPhoneDeValor(pn) === j) return lid;
  }
  try {
    const map = sock && sock.signalRepository && sock.signalRepository.lidMapping;
    if (map && typeof map.getLIDForPN === "function") {
      const lid = map.getLIDForPN(j);
      if (lid && String(lid).endsWith("@lid")) {
        lidParaJid.set(String(lid), j);
        salvarLidDebounced();
        return String(lid);
      }
    }
  } catch {
    /* ignore */
  }
  return "";
}

function jidParaEnvio(item) {
  const raw = String((item && item.jid) || "");
  const lidItem = String((item && item.jid_lid) || "");
  if (lidItem.endsWith("@lid")) return lidItem;
  if (raw.endsWith("@lid")) return raw;
  return lidDePhone(raw) || raw;
}

async function enviarComRetry(jid, content) {
  let last = null;
  for (let i = 0; i < 3; i++) {
    try {
      return await sock.sendMessage(jid, content);
    } catch (e) {
      last = e;
      await new Promise((r) => setTimeout(r, 400 * (i + 1)));
    }
  }
  throw last;
}

async function puxarSaida() {
  if (!sock) return;
  try {
    const j = await get("/api/atendimento-whatsapp/bridge/saida/");
    const lista = (j && j.saida) || [];
    for (const item of lista) {
      try {
        const tipo = String(item.tipo_midia || "");
        const b64 = String(item.midia_b64 || "");
        const txt = String(item.texto || "");
        const caption = txt === "[imagem]" || txt === "[áudio]" ? "" : txt;
        let content;
        if (tipo === "image" || tipo === "audio") {
          let buf = await baixarSaidaArquivo(item.id);
          if (!buf && b64) buf = Buffer.from(b64, "base64");
          if (!buf || !buf.length) continue;
          if (tipo === "image") {
            content = { image: buf, caption, mimetype: String(item.mime || "image/jpeg") };
          } else {
            content = {
              audio: buf,
              ptt: true,
              mimetype: String(item.mime || "audio/ogg; codecs=opus"),
            };
          }
        } else {
          if (!txt.trim()) continue;
          content = { text: txt };
        }
        await enviarComRetry(jidParaEnvio(item), content);
        await post("/api/atendimento-whatsapp/bridge/saida-ok/", { ids: [item.id] });
      } catch (e) {
        console.error("saida:", e.message || e);
        await post("/api/atendimento-whatsapp/bridge/saida-ok/", {
          ids: [item.id],
          erro: String(e.message || e).slice(0, 180),
        });
      }
    }
    for (const p of (j && j.pedidos) || []) {
      await executarPedido(p);
    }
  } catch (e) {
    console.error("poll:", e.message || e);
  }
}

console.log("Django:", BASE);
garantirUmaInstancia();
carregarLidMap();
carregarAgenda();
ligar().catch((e) => {
  console.error(e);
  process.exit(1);
});
