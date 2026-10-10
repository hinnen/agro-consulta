const path = require('path');
const fs = require('fs');
const os = require('os');

const { app, BrowserWindow, shell, ipcMain } = require('electron');

const START_URL =
  process.env.AGRO_PDV_URL ||
  'http://127.0.0.1:8000/'; // durante dev, aponta pro Django local

let _agroAppOrigin = '';
try {
  _agroAppOrigin = new URL(START_URL).origin;
} catch (_) {}

function createWindow() {
  const win = new BrowserWindow({
    width: 1366,
    height: 768,
    minWidth: 1024,
    minHeight: 600,
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true,
      preload: path.join(__dirname, 'preload.js'),
    },
  });

  win.removeMenu();
  win.webContents.setWindowOpenHandler((details) => {
    const url = String(details.url || '').trim();
    if (!url) return { action: 'deny' };
    let absUrl = url;
    try {
      if (_agroAppOrigin && url.startsWith('/')) {
        absUrl = new URL(url, _agroAppOrigin).href;
      }
    } catch (_) {}
    try {
      if (_agroAppOrigin && absUrl.startsWith(_agroAppOrigin)) {
        return { action: 'allow' };
      }
    } catch (_) {}
    if (/^https?:\/\//i.test(absUrl) || /^whatsapp:/i.test(absUrl)) {
      void shell.openExternal(absUrl);
      return { action: 'deny' };
    }
    return { action: 'deny' };
  });
  win.loadURL(START_URL);
}

ipcMain.handle('agro-open-external', async (_event, url) => {
  const u = String(url || '').trim();
  if (!/^https?:\/\//i.test(u) && !/^whatsapp:/i.test(u)) {
    return { ok: false, reason: 'invalid_url' };
  }
  try {
    await shell.openExternal(u);
    return { ok: true };
  } catch (e) {
    return { ok: false, reason: String(e && e.message) };
  }
});

ipcMain.handle('agro-list-printers', async (event) => {
  try {
    const win = BrowserWindow.fromWebContents(event.sender);
    if (!win) return { ok: false, printers: [] };
    const printers = await win.webContents.getPrintersAsync();
    return {
      ok: true,
      printers: printers.map((p) => ({
        name: p.name,
        isDefault: Boolean(p.isDefault),
        status: p.status,
      })),
    };
  } catch (e) {
    return { ok: false, printers: [], reason: String(e && e.message) };
  }
});

ipcMain.handle('agro-silent-print', async (_event, payload) => {
  const html = String(payload?.html || '');
  const deviceName = String(payload?.deviceName || '').trim();
  const waitMs = Math.min(Math.max(Number(payload?.waitMs) || 900, 200), 12000);
  const pageW = Number(payload?.pageWidthMicrons) || 40000;
  const pageH = Number(payload?.pageHeightMicrons) || 40000;
  if (!html) return { ok: false, reason: 'empty_html' };

  let tmpFile = '';
  try {
    tmpFile = path.join(os.tmpdir(), `agro-etq-${Date.now()}.html`);
    fs.writeFileSync(tmpFile, html, 'utf8');
  } catch (e) {
    return { ok: false, reason: String(e && e.message) };
  }

  return new Promise((resolve) => {
    const printWin = new BrowserWindow({
      show: false,
      webPreferences: {
        contextIsolation: true,
        nodeIntegration: false,
      },
    });

    const finish = (ok, reason) => {
      try {
        if (tmpFile && fs.existsSync(tmpFile)) fs.unlinkSync(tmpFile);
      } catch (_) {}
      if (!printWin.isDestroyed()) printWin.destroy();
      resolve({ ok, reason: reason || null });
    };

    printWin.webContents.on('did-fail-load', (_e, code, desc) => {
      finish(false, `${code}: ${desc}`);
    });

    const doPrint = () => {
      printWin.webContents.print(
        {
          silent: true,
          printBackground: true,
          deviceName: deviceName || undefined,
          margins: { marginType: 'none' },
          pageSize: { width: pageW, height: pageH },
        },
        (success, failureReason) => {
          finish(success, failureReason);
        }
      );
    };

    printWin.webContents.on('did-finish-load', () => {
      setTimeout(doPrint, waitMs);
    });

    printWin.loadFile(tmpFile).catch((e) => finish(false, String(e && e.message)));
  });
});

app.whenReady().then(() => {
  createWindow();

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      createWindow();
    }
  });
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    app.quit();
  }
});

