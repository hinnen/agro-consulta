/**
 * Ponte WhatsApp (QR no celular) → Django Agro Consulta.
 * Uso próprio, volume baixo, sem disparo. Deixe o processo ligado.
 */
import { Boom } from "@hapi/boom";
import makeWASocket, {
  DisconnectReason,
  fetchLatestBaileysVersion,
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
const logger = pino({ level: "silent" });

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
  });
  sock.ev.on("creds.update", saveCreds);
  sock.ev.on("connection.update", async (u) => {
    const { connection, lastDisconnect, qr } = u;
    if (qr) {
      const dataUrl = await QRCode.toDataURL(qr, { margin: 1, width: 320 });
      await estado({ status: "qr", qr: dataUrl, aviso: "Leia o QR no celular da loja" });
      console.log("QR enviado ao Agro. Abra /atendimento-whatsapp/");
    }
    if (connection === "open") {
      const me = (sock.user && (sock.user.id || sock.user.lid)) || "";
      const numero = String(me).split(":")[0].split("@")[0];
      await estado({ status: "conectado", numero, aviso: "" });
      console.log("WhatsApp conectado", numero);
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
  sock.ev.on("messages.upsert", async ({ messages }) => {
    for (const m of messages || []) {
      try {
        if (!m.message || m.key.fromMe) continue;
        const jid = String(m.key.remoteJid || "");
        if (!jid || jid.endsWith("@g.us") || jid === "status@broadcast") continue;
        const texto = textoDe(m);
        if (!texto) continue;
        const nome = (m.pushName || "").slice(0, 120);
        const waId = String(m.key.id || "");
        await post("/api/atendimento-whatsapp/bridge/entrada/", {
          jid,
          texto,
          nome,
          wa_id: waId,
        });
      } catch (e) {
        console.error("entrada:", e.message || e);
      }
    }
  });
  if (pollTimer) clearInterval(pollTimer);
  pollTimer = setInterval(puxarSaida, 2500);
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
  } catch (e) {
    console.error("poll:", e.message || e);
  }
}

console.log("Django:", BASE);
ligar().catch((e) => {
  console.error(e);
  process.exit(1);
});
