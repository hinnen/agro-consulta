/**
 * Agro Etiqueta Print — ponte local (só este PC).
 * Chrome do SisVale fala com http://127.0.0.1:PORT e imprime sem diálogo.
 */
'use strict';

const http = require('http');
const path = require('path');
const fs = require('fs');
const os = require('os');
const { app, BrowserWindow, Tray, Menu, nativeImage, dialog } = require('electron');

const DEFAULT_PORT = 19192;
const PORT = Math.min(
  65535,
  Math.max(1024, parseInt(process.env.AGRO_PRINT_BRIDGE_PORT || String(DEFAULT_PORT), 10) || DEFAULT_PORT)
);
const HOST = '127.0.0.1';

let tray = null;
let server = null;
let statusText = 'iniciando…';
let printersCacheWin = null;

function setStatus(msg) {
  statusText = String(msg || '');
  try {
    if (tray && !tray.isDestroyed()) tray.setToolTip('Agro Etiqueta Print · ' + statusText);
  } catch (_) {}
}

function cors(res, origin) {
  const o = String(origin || '').trim();
  const ok =
    !o ||
    o === 'null' ||
    /^https?:\/\/(localhost|127\.0\.0\.1)(:\d+)?$/i.test(o) ||
    /^https?:\/\/([a-z0-9-]+\.)*sistvale\.com\.br$/i.test(o) ||
    /^https?:\/\/([a-z0-9-]+\.)*onrender\.com$/i.test(o);
  res.setHeader('Access-Control-Allow-Origin', ok ? o || '*' : 'null');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Agro-Print');
  res.setHeader('Access-Control-Max-Age', '86400');
  if (ok && o) res.setHeader('Vary', 'Origin');
}

function sendJson(res, code, obj, origin) {
  cors(res, origin);
  const body = JSON.stringify(obj);
  res.writeHead(code, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': Buffer.byteLength(body),
  });
  res.end(body);
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (c) => {
      size += c.length;
      if (size > 8 * 1024 * 1024) {
        reject(new Error('payload_too_large'));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => {
      const raw = Buffer.concat(chunks).toString('utf8');
      if (!raw) return resolve({});
      try {
        resolve(JSON.parse(raw));
      } catch (e) {
        reject(e);
      }
    });
    req.on('error', reject);
  });
}

async function ensurePrintersWindow() {
  if (printersCacheWin && !printersCacheWin.isDestroyed()) return printersCacheWin;
  printersCacheWin = new BrowserWindow({
    show: false,
    width: 100,
    height: 100,
    webPreferences: { contextIsolation: true, nodeIntegration: false },
  });
  await printersCacheWin.loadURL('data:text/html,<html><body>agro-print</body></html>');
  return printersCacheWin;
}

async function listPrinters() {
  const win = await ensurePrintersWindow();
  const printers = await win.webContents.getPrintersAsync();
  return (printers || []).map((p) => ({
    name: p.name,
    isDefault: Boolean(p.isDefault),
    status: p.status,
  }));
}

function silentPrint(payload) {
  const html = String(payload?.html || '');
  const deviceName = String(payload?.deviceName || '').trim();
  const waitMs = Math.min(Math.max(Number(payload?.waitMs) || 900, 200), 12000);
  const pageW = Number(payload?.pageWidthMicrons) || 40000;
  const pageH = Number(payload?.pageHeightMicrons) || 40000;
  if (!html) return Promise.resolve({ ok: false, reason: 'empty_html' });

  let tmpFile = '';
  try {
    tmpFile = path.join(os.tmpdir(), `agro-etq-bridge-${Date.now()}-${Math.random().toString(36).slice(2)}.html`);
    fs.writeFileSync(tmpFile, html, 'utf8');
  } catch (e) {
    return Promise.resolve({ ok: false, reason: String(e && e.message) });
  }

  return new Promise((resolve) => {
    const printWin = new BrowserWindow({
      show: false,
      webPreferences: { contextIsolation: true, nodeIntegration: false },
    });

    const finish = (ok, reason) => {
      try {
        if (tmpFile && fs.existsSync(tmpFile)) fs.unlinkSync(tmpFile);
      } catch (_) {}
      try {
        if (!printWin.isDestroyed()) printWin.destroy();
      } catch (_) {}
      resolve({ ok: !!ok, reason: reason || null, silent: true, bridge: true });
    };

    printWin.webContents.on('did-fail-load', (_e, code, desc) => {
      finish(false, `${code}: ${desc}`);
    });

    const doPrint = () => {
      const opts = {
        silent: true,
        printBackground: true,
        deviceName: deviceName || undefined,
        margins: { marginType: 'none' },
        pageSize: { width: pageW, height: pageH },
      };
      printWin.webContents.print(opts, (success, failureReason) => {
        finish(success, failureReason);
      });
    };

    printWin.webContents.on('did-finish-load', () => {
      setTimeout(doPrint, waitMs);
    });

    printWin.loadFile(tmpFile).catch((e) => finish(false, String(e && e.message)));
  });
}

function startHttpServer() {
  if (server) return;
  server = http.createServer(async (req, res) => {
    const origin = req.headers.origin || '';
    const url = String(req.url || '/').split('?')[0];

    if (req.method === 'OPTIONS') {
      cors(res, origin);
      res.writeHead(204);
      res.end();
      return;
    }

    try {
      if (req.method === 'GET' && (url === '/' || url === '/health')) {
        sendJson(
          res,
          200,
          {
            ok: true,
            service: 'agro-print-bridge',
            version: '1.0.0',
            port: PORT,
            host: HOST,
          },
          origin
        );
        return;
      }

      if (req.method === 'GET' && url === '/printers') {
        const printers = await listPrinters();
        sendJson(res, 200, { ok: true, printers }, origin);
        return;
      }

      if (req.method === 'POST' && url === '/print') {
        const body = await readBody(req);
        const result = await silentPrint(body);
        sendJson(res, result.ok ? 200 : 500, result, origin);
        return;
      }

      sendJson(res, 404, { ok: false, reason: 'not_found' }, origin);
    } catch (e) {
      sendJson(res, 500, { ok: false, reason: String(e && e.message) }, origin);
    }
  });

  server.on('error', (err) => {
    setStatus('erro: ' + (err && err.message));
    dialog.showErrorBox(
      'Agro Etiqueta Print',
      'Não deu para abrir a porta ' + PORT + '.\n\n' + String(err && err.message) +
        '\n\nFeche outro Agro Etiqueta Print ou mude AGRO_PRINT_BRIDGE_PORT.'
    );
  });

  server.listen(PORT, HOST, () => {
    setStatus('ligada · ' + HOST + ':' + PORT);
  });
}

function iconPath() {
  const candidates = [
    path.join(__dirname, 'assets', 'icon.png'),
    path.join(__dirname, '..', 'build', 'icon.png'),
  ];
  for (const p of candidates) {
    if (fs.existsSync(p)) return p;
  }
  return '';
}

function buildTray() {
  let image = nativeImage.createEmpty();
  const ip = iconPath();
  if (ip) {
    try {
      image = nativeImage.createFromPath(ip);
      if (image.isEmpty()) image = nativeImage.createEmpty();
      else image = image.resize({ width: 16, height: 16 });
    } catch (_) {
      image = nativeImage.createEmpty();
    }
  }
  tray = new Tray(image.isEmpty() ? nativeImage.createFromDataURL(
    'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAKElEQVQ4T2NkYGD4z0ABYBzVMKoBBgYGBgYGBgYGBgYGBgYGBgYGBgD6JwQB1nYVnQAAAABJRU5ErkJggg=='
  ) : image);
  tray.setToolTip('Agro Etiqueta Print');
  let openAtLogin = false;
  try {
    openAtLogin = Boolean(app.getLoginItemSettings().openAtLogin);
  } catch (_) {}
  const menu = Menu.buildFromTemplate([
    {
      label: 'Status: ' + statusText,
      enabled: false,
    },
    { type: 'separator' },
    {
      label: 'Abrir com o Windows',
      type: 'checkbox',
      checked: openAtLogin,
      click: (item) => {
        try {
          app.setLoginItemSettings({
            openAtLogin: Boolean(item.checked),
            openAsHidden: true,
            path: process.execPath,
            args: [path.resolve(__dirname)],
          });
        } catch (e) {
          dialog.showErrorBox('Inicio automatico', String(e && e.message));
        }
      },
    },
    {
      label: 'Testar listar impressoras',
      click: async () => {
        try {
          const printers = await listPrinters();
          dialog.showMessageBox({
            type: 'info',
            title: 'Impressoras',
            message: printers.length
              ? printers.map((p) => (p.isDefault ? '★ ' : '') + p.name).join('\n')
              : 'Nenhuma impressora encontrada.',
          });
        } catch (e) {
          dialog.showErrorBox('Impressoras', String(e && e.message));
        }
      },
    },
    {
      label: 'Abrir pasta do app',
      click: () => {
        const { shell } = require('electron');
        shell.openPath(__dirname);
      },
    },
    { type: 'separator' },
    {
      label: 'Sair',
      click: () => {
        app.quit();
      },
    },
  ]);
  tray.setContextMenu(menu);
  tray.on('click', () => tray.popUpContextMenu());
}

app.whenReady().then(() => {
  if (process.platform === 'win32') {
    app.setAppUserModelId('br.com.sistvale.agro-print-bridge');
  }
  /* Preferir o atalho em Startup (Instalar-inicio-Windows.bat).
     LoginItem do Electron é reforço se o usuário marcar no menu. */
  buildTray();
  startHttpServer();
});

app.on('window-all-closed', (e) => {
  e.preventDefault();
});

app.on('before-quit', () => {
  try {
    if (server) server.close();
  } catch (_) {}
  try {
    if (printersCacheWin && !printersCacheWin.isDestroyed()) printersCacheWin.destroy();
  } catch (_) {}
});
