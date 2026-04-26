const { app, BrowserWindow, shell, ipcMain } = require('electron');

const path = require('path');

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

