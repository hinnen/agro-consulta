/**
 * Ponte WhatsApp (QR no celular) → Django Agro Consulta.
 * Uso próprio, volume baixo, sem disparo. Deixe o processo ligado.
 */
import { Boom } from "@hapi/boom";
import makeWASocket, {
  Browsers,
  DisconnectReason,
  downloadContentFromMessage,
  downloadMediaMessage,
  fetchLatestBaileysVersion,
  normalizeMessageContent,
  useMultiFileAuthState,
} from "@whiskeysockets/baileys";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import pino from "pino";
import QRCode from "qrcode";
import { repacketizeOggOpusToCode3 } from "./opus_ptt.js";

const require = createRequire(import.meta.url);
const ffmpegStatic = require("ffmpeg-static");
if (ffmpegStatic) console.log("ffmpeg:", ffmpegStatic);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
carregarEnv(path.join(__dirname, ".env"));
carregarEnv(path.join(__dirname, "..", ".env"));

const BASE = (process.env.AGRO_WA_DJANGO_URL || "http://127.0.0.1:8000").replace(/\/+$/, "");
const TOKEN = process.env.AGRO_WA_BRIDGE_TOKEN || "gm-agro-wa-ponte-local";
const AUTH = path.join(__dirname, "auth");
const AGENDA_FILE = path.join(__dirname, "contatos_agenda.json");
const LID_FILE = path.join(__dirname, "lid_map.json");
const SYNC_FILE = path.join(__dirname, "last_agenda_foto_sync.txt");
const LOCK_FILE = path.join(__dirname, ".ponte.lock");
const logger = pino({ level: "silent" });
let ultimoQrEm = 0;
let salvarAgendaTimer = 0;
let enviarAgendaTimer = 0;
let salvarLidTimer = 0;
let pollSegAtual = 5;
let syncHoraCfg = "00:00";
let syncRodando = false;
let pollQuerFotos = false;
let syncCheckTimer = 0;

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
      if (!k) continue;
      if (process.env.AGRO_WA_ALVO === "local" && (k === "AGRO_WA_DJANGO_URL" || k === "AGRO_WA_BRIDGE_TOKEN")) {
        continue;
      }
      const cur = process.env[k];
      const padraoBat =
        (k === "AGRO_WA_DJANGO_URL" && cur === "http://127.0.0.1:8000") ||
        (k === "AGRO_WA_BRIDGE_TOKEN" && cur === "gm-agro-wa-ponte-local");
      if (cur == null || cur === "" || padraoBat) process.env[k] = v;
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
/** Evita mandar a mesma saída 2x se o poll cruzar (foto/áudio demora). */
const saidaEmVoo = new Set();
/** Evita postar a mesma entrada 2x (notify + append / retry). */
const entradaJaPostada = new Set();
const ENTRADA_DEDUP_MAX = 800;
let puxarSaidaRodando = false;
const HIST_MS = 7 * 24 * 60 * 60 * 1000;
/** Mensagem “ao vivo” (dispara bot). Notify antigo no reconnect NÃO conta. */
const LIVE_MS = 3 * 60 * 1000;
/** Append = sync; só fila offline bem recente. */
const APPEND_LIVE_MS = 90 * 1000;
/** Quarentena pós-conexão: notify nos primeiros segundos = msg velha reenviada. */
const CONN_QUARANTINE_MS = 15000;
let connOpenAt = 0;
/** Só aceita append/histórico antigo quando a loja pediu «Anteriores» neste chat. */
let histJids = new Set();
let histAte = 0;

function normJid(j) {
  return String(j || "")
    .split(":")[0]
    .trim()
    .toLowerCase();
}

function marcarHistPedido(...jids) {
  histJids = new Set();
  histAte = Date.now() + 180000;
  for (const raw of jids) {
    const j = String(raw || "");
    if (!j) continue;
    histJids.add(normJid(j));
    if (j.endsWith("@lid")) {
      const pn = pnDeLid(j);
      if (pn) histJids.add(normJid(pn));
    } else if (j.endsWith("@s.whatsapp.net")) {
      const lid = lidDePhone(j);
      if (lid) histJids.add(normJid(lid));
    }
  }
  console.log("Hist pedido jids:", [...histJids]);
}

function historicoPermitido(jid) {
  if (!histJids.size || Date.now() >= histAte) return false;
  const j = normJid(jid);
  if (!j) return false;
  if (histJids.has(j)) return true;
  if (j.endsWith("@lid")) {
    const pn = pnDeLid(j);
    if (pn && histJids.has(normJid(pn))) return true;
  } else if (j.endsWith("@s.whatsapp.net")) {
    const lid = lidDePhone(j);
    if (lid && histJids.has(normJid(lid))) return true;
  }
  return false;
}

function histJanelaAberta() {
  return histJids.size > 0 && Date.now() < histAte;
}

function tsMs(m) {
  const raw = m && (m.messageTimestamp || m.timestamp);
  const n = Number(raw || 0);
  if (!n) return 0;
  return n > 1e12 ? n : n * 1000;
}

function ehMensagemAoVivo(m, type) {
  if (type === "notify") return true;
  const quando = tsMs(m);
  if (!quando) return type === "append";
  const age = Date.now() - quando;
  if (age < 0 || age > LIVE_MS) return false;
  if (type === "append") return age < LIVE_MS;
  return false;
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

function ehStatusBroadcast(jid) {
  const j = String(jid || "").toLowerCase();
  return j === "status@broadcast" || j.includes("status@");
}

function ehStatusOuGrupo(jid) {
  const j = String(jid || "").toLowerCase();
  return (
    j === "status@broadcast" ||
    j.includes("@broadcast") ||
    j.includes("@g.us") ||
    j.includes("@newsletter") ||
    j.includes("@status")
  );
}

function ehChatPrivado(jid) {
  const j = String(jid || "").toLowerCase();
  if (ehStatusOuGrupo(j)) return false;
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
  if (ehStatusOuGrupo(jid)) return jid;
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
  /* WA-PONTE-LEVE: agenda sobe 1×/dia (sync madrugada), não a cada contato. */
}

function ymdLocal(d = new Date()) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function lerUltimoSyncYmd() {
  try {
    return fs.readFileSync(SYNC_FILE, "utf8").trim().slice(0, 10);
  } catch {
    return "";
  }
}

function gravarSyncYmd(ymd) {
  try {
    fs.writeFileSync(SYNC_FILE, String(ymd || "") + "\n", "utf8");
  } catch (e) {
    console.error("sync file:", e.message || e);
  }
}

function minutosAgora(d = new Date()) {
  return d.getHours() * 60 + d.getMinutes();
}

function parseHoraCfg(s) {
  const m = /^(\d{1,2}):(\d{2})$/.exec(String(s || "00:00").trim());
  if (!m) return 0;
  const hh = Math.min(23, Math.max(0, parseInt(m[1], 10)));
  const mm = Math.min(59, Math.max(0, parseInt(m[2], 10)));
  return hh * 60 + mm;
}

function precisaSyncAgendaFotos() {
  const hoje = ymdLocal();
  if (lerUltimoSyncYmd() === hoje) return false;
  return minutosAgora() >= parseHoraCfg(syncHoraCfg);
}

function ajustarPollSaida(seg) {
  const n = Math.max(2, Math.min(15, Number(seg) || 5));
  if (pollTimer && n === pollSegAtual) return;
  pollSegAtual = n;
  if (pollTimer) clearInterval(pollTimer);
  pollTimer = setInterval(() => {
    puxarSaida().catch(() => {});
  }, pollSegAtual * 1000);
  console.log("Poll saída:", pollSegAtual, "s");
}

async function rodarSyncAgendaFotos() {
  if (syncRodando || !sock) return;
  if (!precisaSyncAgendaFotos()) return;
  syncRodando = true;
  console.log("Sync diário agenda+fotos… hora cfg", syncHoraCfg);
  try {
    await enviarAgenda(0, "", { completo: true });
    pollQuerFotos = true;
    gravarSyncYmd(ymdLocal());
    setTimeout(() => {
      pollQuerFotos = false;
      console.log("Janela de fotos do sync diário encerrada.");
    }, 3 * 60 * 1000);
  } catch (e) {
    console.error("sync diário:", e.message || e);
  } finally {
    syncRodando = false;
  }
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

async function baixarMidiaCompleta(m, tipos) {
  const tipo = tipoMidiaDe(m);
  const permitidos = tipos || ["image", "audio", "sticker", "video"];
  if (!permitidos.includes(tipo)) {
    return { tipo, b64: "", mime: mimeDe(m) };
  }
  const inner = conteudoDe(m);
  const node =
    (inner &&
      (inner.imageMessage ||
        inner.audioMessage ||
        inner.stickerMessage ||
        inner.videoMessage)) ||
    null;
  const kind = tipo === "sticker" ? "sticker" : tipo === "video" ? "video" : tipo;
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

async function baixarMidia(m) {
  return baixarMidiaCompleta(m, ["image", "audio", "sticker"]);
}

function autorDeStatus(m) {
  const key = m && m.key;
  if (!key) return "";
  const cands = [
    key.participant,
    key.participantAlt,
    key.participantPn,
    key.senderPn,
    key.remoteJidAlt,
    telefoneDeKey(key),
  ];
  for (const c of cands) {
    let autor = String(c || "");
    if (!autor || ehStatusBroadcast(autor)) continue;
    if (autor.endsWith("@lid")) {
      const pn = pnDeLid(autor);
      if (pn) return pn;
      if (ehChatPrivado(autor)) return autor;
      continue;
    }
    const phone = jidPhoneDeValor(autor);
    if (phone) return phone;
    if (ehChatPrivado(autor)) return autor;
  }
  return "";
}

async function enviarStatus(m) {
  if (!m || !(m.message || conteudoDe(m))) return;
  const key = m.key || {};
  const orig = String(key.remoteJid || "");
  if (!ehStatusBroadcast(orig)) return;
  if (key.fromMe) return;
  let jid = autorDeStatus(m);
  const lid = String(key.participant || "").endsWith("@lid") ? String(key.participant) : "";
  if (jid.endsWith("@lid")) {
    const pn = pnDeLid(jid);
    if (pn) jid = pn;
  }
  if (!ehChatPrivado(jid)) return;
  const quando = tsMs(m) || Date.now();
  if (Date.now() - quando > 24 * 60 * 60 * 1000) return;
  const texto = textoDe(m);
  const tipo = tipoMidiaDe(m);
  if (!texto && !tipo) return;
  const nome = String(m.pushName || "").slice(0, 120);
  const waId = String((key && key.id) || "");
  const midia = await baixarMidiaCompleta(m, ["image", "video"]);
  const tel = telefoneDeJid(jid) || telefoneDeJid(telefoneDeKey(key));
  await post("/api/atendimento-whatsapp/bridge/status/", {
    jid,
    jid_lid: lid,
    telefone: tel,
    texto,
    nome,
    wa_id: waId,
    ts: Math.floor(quando / 1000),
    tipo_midia: midia.tipo || tipo,
    midia_b64: midia.b64 || "",
    mime: midia.mime || "",
    nome_arquivo: String(
      (conteudoDe(m) && conteudoDe(m).documentMessage && conteudoDe(m).documentMessage.fileName) || ""
    ),
  });
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
  const waId = String((key && key.id) || "");
  if (waId) {
    if (entradaJaPostada.has(waId)) return;
    entradaJaPostada.add(waId);
    if (entradaJaPostada.size > ENTRADA_DEDUP_MAX) {
      const first = entradaJaPostada.keys().next().value;
      entradaJaPostada.delete(first);
    }
  }
  const orig = String(key.remoteJid || "");
  if (ehStatusOuGrupo(orig)) return;
  const lid = orig.endsWith("@lid") ? orig : "";
  let jid = jidDaMensagem(m);
  if (jid.endsWith("@lid")) {
    const pn = pnDeLid(jid);
    if (pn) jid = pn;
  }
  if (!ehChatPrivado(jid)) return;
  const quando = tsMs(m) || Date.now();
  const historico = !!(extra && extra.historico);
  if (historico && Date.now() - quando > HIST_MS) return;
  const texto = textoDe(m);
  const tipo = tipoMidiaDe(m);
  if (!texto && !tipo) return;
  const nome = String(m.pushName || "").slice(0, 120);
  const midia = await baixarMidia(m);
  const tel = telefoneDeJid(jid) || telefoneDeJid(telefoneDeKey(key));
  try {
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
  } catch (e) {
    if (waId) entradaJaPostada.delete(waId);
    throw e;
  }
  if (!historico && !(m.key && m.key.fromMe)) {
    agendarFotoPerfil(jid, { telefone: tel, jid_lid: lid });
  }
}

const fotoTentada = new Map();
const FOTO_TTL_MS = 6 * 60 * 60 * 1000;
let fotoFila = Promise.resolve();

function agendarFotoPerfil(jid, extra) {
  const j = String(jid || "");
  if (!ehChatPrivado(j)) return;
  const last = fotoTentada.get(j) || 0;
  if (Date.now() - last < FOTO_TTL_MS) return;
  fotoTentada.set(j, Date.now());
  fotoFila = fotoFila
    .then(() => enviarFotoPerfil(j, extra || {}))
    .catch((e) => console.error("foto perfil:", e.message || e));
}

async function enviarFotoPerfil(jid, extra) {
  if (!sock || typeof sock.profilePictureUrl !== "function") return;
  let url = "";
  try {
    url = await sock.profilePictureUrl(jid, "image");
  } catch {
    return;
  }
  if (!url) return;
  let buf = null;
  try {
    const r = await fetch(url);
    if (!r.ok) return;
    buf = Buffer.from(await r.arrayBuffer());
  } catch {
    return;
  }
  if (!buf || !buf.length || buf.length > 2500000) return;
  const mime = String((url.split("?")[0] || "").endsWith(".png") ? "image/png" : "image/jpeg");
  await post("/api/atendimento-whatsapp/bridge/foto/", {
    jid,
    telefone: (extra && extra.telefone) || telefoneDeJid(jid),
    jid_lid: (extra && extra.jid_lid) || "",
    midia_b64: buf.toString("base64"),
    mime,
  });
}

function semearFotosPerfil() {
  /* fotos pendentes vêm no poll da saída */
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
    browser: Browsers.ubuntu("Chrome"),
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
      ultimoQrEm = Date.now();
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
      connOpenAt = Date.now();
      await estado({ status: "conectado", numero, aviso: "" });
      console.log("WhatsApp conectado", numero);
      enviarLidMap();
      setTimeout(() => {
        varrerStore();
        // WA-PONTE-LEVE: não despeja agenda/fotos no connect — só sync 1×/dia
        rodarSyncAgendaFotos().catch(() => {});
      }, 4000);
      if (!syncCheckTimer) {
        syncCheckTimer = setInterval(() => {
          rodarSyncAgendaFotos().catch(() => {});
        }, 60 * 1000);
      }
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
  sock.ev.on("chats.update", (lista) => {
    for (const ch of lista || []) {
      guardarContato({ id: ch.id, jid: ch.jid || ch.id, name: ch.name, notify: ch.notify || ch.name });
    }
  });
  sock.ev.on("chats.set", (dados) => {
    for (const ch of (dados && dados.chats) || []) {
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
  sock.ev.on("messaging-history.set", async (dados) => {
    const contacts = (dados && dados.contacts) || [];
    const chats = (dados && dados.chats) || [];
    const messages = (dados && dados.messages) || [];
    for (const c of contacts || []) guardarContato(c);
    for (const ch of chats || []) {
      guardarContato({ id: ch.id, jid: ch.jid || ch.id, name: ch.name, notify: ch.notify || ch.name });
    }
    if (!messages.length) return;
    console.log("Hist sync msgs:", messages.length, "janela:", histJanelaAberta());
    for (const m of messages) {
      try {
        guardarMsgCache(m);
        const raw = String((m && m.key && m.key.remoteJid) || "");
        if (ehStatusOuGrupo(raw)) continue;
        const jid = jidDaMensagem(m);
        if (!histJanelaAberta()) continue;
        if (!historicoPermitido(jid) && !historicoPermitido(raw)) continue;
        const quando = tsMs(m);
        if (Date.now() - quando > HIST_MS) continue;
        await enviarEntrada(m, { historico: true });
      } catch (e) {
        console.error("hist set:", e.message || e);
      }
    }
  });
  sock.ev.on("messages.upsert", async ({ messages, type }) => {
    if (type !== "notify" && type !== "append") return;
    const emQuarentena = connOpenAt && (Date.now() - connOpenAt < CONN_QUARANTINE_MS);
    for (const m of messages || []) {
      try {
        guardarMsgCache(m);
        const raw = String((m && m.key && m.key.remoteJid) || "");
        if (ehStatusBroadcast(raw)) {
          await enviarStatus(m);
          continue;
        }
        if (ehStatusOuGrupo(raw)) continue;
        const jid = jidDaMensagem(m);
        if (m.key && m.key.fromMe && !histJanelaAberta()) continue;
        // Ao vivo: só notify (append = sync; mandava a mesma msg de novo).
        if (type === "notify" && !(m.key && m.key.fromMe)) {
          const quando = tsMs(m);
          const idade = quando ? Date.now() - quando : 0;
          if (emQuarentena && idade > 60000) {
            console.log("Quarentena: descartada notify antiga de", jid, idade);
            continue;
          }
          console.log("Entrada ao vivo:", jid, textoDe(m).slice(0, 40));
          await enviarEntrada(m, { historico: false });
          continue;
        }
        if (!historicoPermitido(jid) && !historicoPermitido(raw)) continue;
        const quando = tsMs(m);
        if (Date.now() - quando > HIST_MS) continue;
        await enviarEntrada(m, { historico: true });
      } catch (e) {
        console.error("entrada:", e.message || e);
      }
    }
  });
  ajustarPollSaida(pollSegAtual);
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

function semAcento(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

async function enviarAgenda(pedidoId, filtro, opts) {
  const f = semAcento(filtro);
  const completo = !!(opts && opts.completo) && !f;
  const itens = [];
  const teto = completo ? 5000 : 400;
  for (const [jid, v] of agenda.entries()) {
    const lid = jid.endsWith("@lid") ? jid : "";
    const phone = jid.endsWith("@s.whatsapp.net") ? jid : pnDeLid(jid);
    const dest = phone || jid;
    if (!dest.endsWith("@s.whatsapp.net") && !dest.endsWith("@lid")) continue;
    const nome = String((v && v.nome) || "").slice(0, 120);
    const tel = String((v && v.telefone) || (phone || "").split("@")[0] || "");
    const dig = String(filtro || "").replace(/\D/g, "");
    if (f && !semAcento(nome).includes(f) && !(dig && tel.includes(dig))) continue;
    itens.push({ jid: dest, jid_lid: lid, nome, telefone: tel });
    if (itens.length >= teto) break;
  }
  console.log(
    "Agenda Zap:",
    agenda.size,
    "filtro:",
    f || "(todos)",
    "enviados:",
    itens.length,
    completo ? "(sync completo)" : ""
  );
  if (!itens.length && !pedidoId) return;
  const lote = 80;
  for (let i = 0; i < Math.max(itens.length, 1); i += lote) {
    const fatia = itens.slice(i, i + lote);
    await post("/api/atendimento-whatsapp/bridge/contatos/", {
      pedido_id: i === 0 ? pedidoId || 0 : 0,
      itens: fatia,
    });
    if (!fatia.length) break;
  }
}

async function executarPedido(p) {
  if (!sock || !p) return;
  const pid = p.id;
  try {
    if (p.tipo === "contatos") {
      await enviarAgenda(pid, String(p.q || p.termo || ""));
      return;
    }
    if (p.tipo === "pairing") {
      let tel = String(p.telefone || "").replace(/\D/g, "");
      if (tel.length === 10 || tel.length === 11) tel = "55" + tel;
      if (tel.length < 12 || tel.length > 13) {
        throw new Error("Número inválido");
      }
      if (sock.authState && sock.authState.creds && sock.authState.creds.registered) {
        throw new Error("Já está ligado neste PC. Use Trocar Zap antes.");
      }
      const ate = Date.now() + 20000;
      while (Date.now() < ate && ultimoQrEm === 0) {
        await new Promise((r) => setTimeout(r, 400));
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
    if (p.tipo === "logout") {
      lastPairing = "";
      primeiraLigacao = true;
      try {
        if (sock && typeof sock.logout === "function") {
          await sock.logout();
        }
      } catch (e) {
        console.error("logout zap:", e.message || e);
      }
      fecharSockAntigo();
      try {
        fs.rmSync(AUTH, { recursive: true, force: true });
      } catch {
        /* ignore */
      }
      await estado({
        status: "desconectado",
        aviso: "Zap desligado neste PC. Leia o QR ou gere um código.",
        numero: "",
        qr: "",
        pairing_code: "",
      });
      await post("/api/atendimento-whatsapp/bridge/pedido-ok/", { pedido_id: pid });
      setTimeout(() => ligar(), 800);
      return;
    }
    if (p.tipo === "historico") {
      const count = Math.min(40, Math.max(5, Number(p.count) || 30));
      const jidPed = String(p.jid || "");
      const jidPhone = String(p.jid_phone || "");
      const candidatos = [];
      for (const j of [jidPed, jidPhone, pnDeLid(jidPed), lidDePhone(jidPhone || jidPed)]) {
        const s = String(j || "");
        if (s && !candidatos.includes(s)) candidatos.push(s);
      }
      marcarHistPedido(...candidatos);
      const oldestKeyBase = {
        id: String(p.oldest_id || ""),
        fromMe: !!p.oldest_from_me,
      };
      const ts = Number(p.oldest_ts) || Date.now();
      let ok = false;
      let lastErr = null;
      for (const jid of candidatos) {
        try {
          console.log("fetchMessageHistory", count, jid, oldestKeyBase.id);
          await sock.fetchMessageHistory(count, { ...oldestKeyBase, remoteJid: jid }, ts);
          ok = true;
          break;
        } catch (e) {
          lastErr = e;
          console.error("fetchMessageHistory falhou", jid, e.message || e);
        }
      }
      if (!ok && lastErr) throw lastErr;
      await post("/api/atendimento-whatsapp/bridge/pedido-ok/", { pedido_id: pid });
      return;
    }
    if (p.tipo === "apagar") {
      const waId = String(p.wa_id || "").trim();
      if (!waId) throw new Error("Sem ID da mensagem");
      const jidPed = String(p.jid || "");
      const jidPhone = String(p.jid_phone || "");
      const jidLid = String(p.jid_lid || "");
      const candidatos = [];
      for (const j of [jidLid, jidPed, jidPhone, pnDeLid(jidPed), lidDePhone(jidPhone || jidPed)]) {
        const s = String(j || "");
        if (s && !candidatos.includes(s)) candidatos.push(s);
      }
      if (!candidatos.length) throw new Error("Sem destino para apagar");
      let ok = false;
      let lastErr = null;
      for (const jid of candidatos) {
        try {
          console.log("apagar msg", waId, "->", jid);
          await sock.sendMessage(jid, {
            delete: { remoteJid: jid, fromMe: true, id: waId },
          });
          ok = true;
          break;
        } catch (e) {
          lastErr = e;
          console.error("apagar falhou", jid, e.message || e);
        }
      }
      if (!ok && lastErr) throw lastErr;
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

async function audioParaZap(buf, mime) {
  const m = String(mime || "").toLowerCase();
  const bin = typeof ffmpegStatic === "string" && ffmpegStatic ? ffmpegStatic : "ffmpeg";
  const stamp = Date.now();
  const inn = path.join(os.tmpdir(), "agro-wa-" + stamp + ".webm");
  const out = path.join(os.tmpdir(), "agro-wa-" + stamp + ".ogg");

  const finalizar = (oggBuf, seconds) => {
    let finalBuf = oggBuf;
    try {
      const remade = repacketizeOggOpusToCode3(oggBuf);
      if (remade && remade.length && remade !== oggBuf) {
        finalBuf = remade;
        console.log("audio ptt code3:", oggBuf.length, "->", remade.length);
      }
    } catch (e) {
      console.error("audio code3:", e.message || e);
    }
    return { ok: true, buf: finalBuf, mime: "audio/ogg; codecs=opus", ptt: true, seconds: seconds || 0 };
  };

  if (m.includes("ogg") && !m.includes("webm")) {
    return finalizar(buf, 0);
  }

  try {
    fs.writeFileSync(inn, buf);
    let seconds = 0;
    await new Promise((resolve, reject) => {
      const args = [
        "-y",
        "-i",
        inn,
        "-vn",
        "-ac",
        "1",
        "-ar",
        "48000",
        "-c:a",
        "libopus",
        "-b:a",
        "32k",
        "-application",
        "voip",
        "-frame_duration",
        "20",
        "-avoid_negative_ts",
        "make_zero",
        "-map_metadata",
        "-1",
        "-f",
        "ogg",
        out,
      ];
      const p = spawn(bin, args, { windowsHide: true });
      let errTxt = "";
      p.stderr.on("data", (d) => {
        errTxt += String(d || "");
      });
      p.on("error", reject);
      p.on("close", (c) => {
        const hit = /Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)/i.exec(errTxt);
        if (hit) {
          seconds = Math.max(1, Math.round(Number(hit[1]) * 3600 + Number(hit[2]) * 60 + Number(hit[3])));
        }
        c === 0 ? resolve() : reject(new Error("ffmpeg " + c + " " + errTxt.slice(-180)));
      });
    });
    const converted = fs.readFileSync(out);
    if (!converted || !converted.length) throw new Error("ffmpeg vazio");
    console.log("audio ok:", buf.length, "->", converted.length, "s=", seconds);
    return finalizar(converted, seconds);
  } catch (e) {
    console.error("audio ffmpeg:", e.message || e);
    return { ok: false, buf, mime: m || "audio/webm", ptt: false, seconds: 0, erro: String(e.message || e) };
  } finally {
    try {
      fs.unlinkSync(inn);
    } catch {
      /* ignore */
    }
    try {
      fs.unlinkSync(out);
    } catch {
      /* ignore */
    }
  }
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

async function enviarAudioZap(dest, aud) {
  const base = {
    audio: aud.buf,
    mimetype: "audio/ogg; codecs=opus",
  };
  if (aud.seconds > 0) base.seconds = aud.seconds;
  try {
    return await enviarComRetry(dest, { ...base, ptt: true });
  } catch (e1) {
    console.error("audio ptt:", e1.message || e1);
  }
  try {
    return await enviarComRetry(dest, { ...base, ptt: false });
  } catch (e2) {
    console.error("audio file:", e2.message || e2);
  }
  return await enviarComRetry(dest, {
    document: aud.buf,
    mimetype: "audio/ogg",
    fileName: "audio.ogg",
  });
}

async function puxarSaida() {
  if (!sock || puxarSaidaRodando) return;
  puxarSaidaRodando = true;
  try {
    const qs = pollQuerFotos ? "?fotos=1" : "";
    const j = await get("/api/atendimento-whatsapp/bridge/saida/" + qs);
    if (j && j.poll_seg != null) ajustarPollSaida(j.poll_seg);
    if (j && j.sync_agenda_fotos_hora) syncHoraCfg = String(j.sync_agenda_fotos_hora);
    const lista = (j && j.saida) || [];
    for (const item of lista) {
      const idItem = Number(item && item.id) || 0;
      if (!idItem || saidaEmVoo.has(idItem)) continue;
      saidaEmVoo.add(idItem);
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
            const aud = await audioParaZap(buf, item.mime);
            if (!aud.ok) {
              throw new Error("Conversão de áudio falhou: " + (aud.erro || "ffmpeg"));
            }
            const dest = jidParaEnvio(item);
            console.log("enviando audio ->", dest, "bytes", aud.buf.length, "s", aud.seconds);
            const sent = await enviarAudioZap(dest, aud);
            const waId = sent && sent.key && sent.key.id;
            await post("/api/atendimento-whatsapp/bridge/saida-ok/", {
              ids: [item.id],
              wa_id: waId || "",
            });
            continue;
          }
        } else if (tipo === "pix_copy") {
          /* legado: botão cta_copy quebrava no celular — manda texto limpo */
          if (!txt.trim()) continue;
          const sep = "|||PIX|||";
          let intro = txt;
          let chave = "";
          const ix = txt.indexOf(sep);
          if (ix >= 0) {
            intro = txt.slice(0, ix).trim();
            chave = txt.slice(ix + sep.length).trim();
          } else {
            chave = txt.trim();
            intro = "Chave Pix";
          }
          const dest = jidParaEnvio(item);
          const corpo =
            (intro || "Chave Pix") +
            (chave ? "\n\n" + chave : "");
          const sent = await enviarComRetry(dest, { text: corpo });
          const waId = sent && sent.key && sent.key.id;
          await post("/api/atendimento-whatsapp/bridge/saida-ok/", {
            ids: [item.id],
            wa_id: waId || "",
          });
          continue;
        } else {
          if (!txt.trim()) continue;
          content = { text: txt };
        }
        const sent = await enviarComRetry(jidParaEnvio(item), content);
        const waId = sent && sent.key && sent.key.id;
        await post("/api/atendimento-whatsapp/bridge/saida-ok/", {
          ids: [item.id],
          wa_id: waId || "",
        });
      } catch (e) {
        console.error("saida:", e.message || e);
        await post("/api/atendimento-whatsapp/bridge/saida-ok/", {
          ids: [item.id],
          erro: String(e.message || e).slice(0, 180),
        });
      } finally {
        saidaEmVoo.delete(idItem);
      }
    }
    for (const p of (j && j.pedidos) || []) {
      await executarPedido(p);
    }
    for (const f of (j && j.fotos) || []) {
      if (!f || !f.jid) continue;
      agendarFotoPerfil(f.jid, { telefone: f.telefone || "", jid_lid: f.jid_lid || "" });
    }
  } catch (e) {
    console.error("poll:", e.message || e);
  } finally {
    puxarSaidaRodando = false;
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
