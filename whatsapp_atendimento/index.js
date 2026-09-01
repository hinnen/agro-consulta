/**
 * Ponte WhatsApp (QR no celular) → Django Agro Consulta.
 * Uso próprio, volume baixo, sem disparo. Deixe o processo ligado.
 */
import { Boom } from "@hapi/boom";
import makeWASocket, {
  DisconnectReason,
  downloadMediaMessage,
  fetchLatestBaileysVersion,
  proto,
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
const AGENDA_FILE = path.join(AUTH, "contatos_agenda.json");
const logger = pino({ level: "silent" });
const SYNC_NOMES = new Set([
  proto.HistorySync.HistorySyncType.INITIAL_BOOTSTRAP,
  proto.HistorySync.HistorySyncType.PUSH_NAME,
]);
let salvarAgendaTimer = 0;

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

function textoDe(msg) {
  const m = msg && msg.message;
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
const agenda = new Map();
const HIST_MS = 7 * 24 * 60 * 60 * 1000;
/** Só aceita append/histórico quando a loja pediu «Anteriores» neste chat. */
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

function jidDeContato(c) {
  const jid = String((c && c.jid) || "");
  const id = String((c && c.id) || "");
  if (jid.endsWith("@s.whatsapp.net")) return jid;
  if (id.endsWith("@s.whatsapp.net")) return id;
  return "";
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
      fs.mkdirSync(AUTH, { recursive: true });
      fs.writeFileSync(AGENDA_FILE, JSON.stringify(obj));
    } catch {
      /* ignore */
    }
  }, 800);
}

function guardarContato(c) {
  if (!c) return;
  const jid = jidDeContato(c);
  if (!ehChatPrivado(jid)) return;
  if (agenda.size >= 2000 && !agenda.has(jid)) return;
  const prev = agenda.get(jid);
  const nome = nomeDeContato(c, prev);
  if (prev && prev.nome && !nome) return;
  agenda.set(jid, { nome: nome || (prev && prev.nome) || "", telefone: jid.split("@")[0] });
  salvarAgendaDebounced();
}

function ehChatPrivado(jid) {
  const j = String(jid || "").toLowerCase();
  if (!j.endsWith("@s.whatsapp.net")) return false;
  if (j.includes("@g.us") || j.includes("@newsletter") || j.includes("@broadcast")) return false;
  const num = j.split("@")[0].replace(/\D/g, "");
  if (!num) return false;
  if (num.startsWith("120") && num.length >= 15) return false;
  if (num.length < 10 || num.length > 13) return false;
  return true;
}

function tipoMidiaDe(msg) {
  const m = msg && msg.message;
  if (!m) return "";
  if (m.imageMessage) return "image";
  if (m.audioMessage) return "audio";
  if (m.stickerMessage) return "sticker";
  if (m.videoMessage) return "video";
  if (m.documentMessage) return "document";
  return "";
}

function mimeDe(msg) {
  const m = msg && msg.message;
  if (!m) return "";
  if (m.imageMessage) return String(m.imageMessage.mimetype || "");
  if (m.audioMessage) return String(m.audioMessage.mimetype || "");
  if (m.stickerMessage) return String(m.stickerMessage.mimetype || "image/webp");
  if (m.videoMessage) return String(m.videoMessage.mimetype || "");
  if (m.documentMessage) return String(m.documentMessage.mimetype || "");
  return "";
}

async function baixarMidia(m) {
  const tipo = tipoMidiaDe(m);
  if (!["image", "audio", "sticker"].includes(tipo)) {
    return { tipo, b64: "", mime: mimeDe(m) };
  }
  try {
    const buf = await downloadMediaMessage(m, "buffer", {}, { logger, reuploadRequest: sock.updateMediaMessage });
    if (!buf || !buf.length || buf.length > 6000000) return { tipo, b64: "", mime: mimeDe(m) };
    return { tipo, b64: Buffer.from(buf).toString("base64"), mime: mimeDe(m) };
  } catch (e) {
    console.error("midia:", e.message || e);
    return { tipo, b64: "", mime: mimeDe(m) };
  }
}

async function enviarEntrada(m, extra) {
  if (!m || !m.message) return;
  const jid = String((m.key && m.key.remoteJid) || "");
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
  await post("/api/atendimento-whatsapp/bridge/entrada/", {
    jid,
    texto,
    nome,
    wa_id: waId,
    historico,
    de_mim: !!(m.key && m.key.fromMe),
    ts: Math.floor(quando / 1000),
    tipo_midia: midia.tipo || tipo,
    midia_b64: midia.b64 || "",
    mime: midia.mime || "",
    nome_arquivo: String((m.message.documentMessage && m.message.documentMessage.fileName) || ""),
  });
}

async function estado(payload) {
  try {
    await post("/api/atendimento-whatsapp/bridge/estado/", payload);
  } catch (e) {
    console.error("estado:", e.message || e);
  }
}

async function ligar() {
  fs.mkdirSync(AUTH, { recursive: true });
  await estado({ status: "desconectado", aviso: "Ligando WhatsApp…" });
  const { state, saveCreds } = await useMultiFileAuthState(AUTH);
  const { version } = await fetchLatestBaileysVersion();
  sock = makeWASocket({
    version,
    auth: state,
    logger,
    browser: ["Agro Consulta", "Chrome", "20.33"],
    markOnlineOnConnect: false,
    syncFullHistory: false,
    shouldSyncHistoryMessage: (msg) => SYNC_NOMES.has(msg && msg.syncType),
  });
  sock.ev.on("creds.update", saveCreds);
  sock.ev.on("connection.update", async (u) => {
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
      lastPairing = "";
      const me = (sock.user && (sock.user.id || sock.user.lid)) || "";
      const numero = String(me).split(":")[0].split("@")[0];
      await estado({ status: "conectado", numero, aviso: "" });
      console.log("WhatsApp conectado", numero);
      setTimeout(() => enviarAgenda(0), 2500);
      setTimeout(() => enviarAgenda(0), 12000);
    }
    if (connection === "close") {
      const err = lastDisconnect && lastDisconnect.error;
      const code = err instanceof Boom ? err.output.statusCode : 0;
      const loggedOut = code === DisconnectReason.loggedOut;
      await estado({
        status: "desconectado",
        aviso: loggedOut ? "Desconectou. Escaneie o QR de novo." : "Reconectando…",
      });
      if (pollTimer) {
        clearInterval(pollTimer);
        pollTimer = null;
      }
      if (loggedOut) {
        try {
          fs.rmSync(AUTH, { recursive: true, force: true });
        } catch {
          /* ignore */
        }
      }
      setTimeout(ligar, loggedOut ? 1500 : 4000);
    }
  });
  sock.ev.on("contacts.upsert", (lista) => {
    for (const c of lista || []) guardarContato(c);
  });
  sock.ev.on("contacts.update", (lista) => {
    for (const c of lista || []) guardarContato(c);
  });
  sock.ev.on("contacts.set", (data) => {
    const lista = Array.isArray(data) ? data : (data && data.contacts) || [];
    for (const c of lista) guardarContato(c);
  });
  sock.ev.on("chats.upsert", (lista) => {
    for (const ch of lista || []) {
      guardarContato({ id: ch.id, jid: ch.jid || ch.id, name: ch.name, notify: ch.notify || ch.name });
    }
  });
  sock.ev.on("chats.phoneNumberShare", ({ lid, jid }) => {
    const jj = String(jid || "");
    if (!ehChatPrivado(jj)) return;
    const lj = String(lid || "");
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
    setTimeout(() => enviarAgenda(0), 1500);
  });
  sock.ev.on("messages.upsert", async ({ messages, type }) => {
    if (type !== "notify" && type !== "append") return;
    for (const m of messages || []) {
      try {
        const jid = String((m.key && m.key.remoteJid) || "");
        if (type === "notify") {
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
  if (pollTimer) clearInterval(pollTimer);
  pollTimer = setInterval(puxarSaida, 2500);
}

async function enviarAgenda(pedidoId) {
  const itens = [];
  for (const [jid, v] of agenda.entries()) {
    if (!jid.endsWith("@s.whatsapp.net")) continue;
    itens.push({
      jid,
      nome: String((v && v.nome) || "").slice(0, 120),
      telefone: String((v && v.telefone) || jid.split("@")[0]),
    });
    if (itens.length >= 500) break;
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
      if (agenda.size < 30) {
        setTimeout(() => enviarAgenda(pid).catch(() => {}), 3000);
        setTimeout(() => enviarAgenda(pid).catch(() => {}), 9000);
      }
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

async function puxarSaida() {
  if (!sock) return;
  try {
    const j = await get("/api/atendimento-whatsapp/bridge/saida/");
    const lista = (j && j.saida) || [];
    for (const item of lista) {
      try {
        await sock.sendMessage(item.jid, { text: String(item.texto || "") });
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
carregarAgenda();
ligar().catch((e) => {
  console.error(e);
  process.exit(1);
});
