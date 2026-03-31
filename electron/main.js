/**
 * Shell Electron para o Agro Consulta (PDV web).
 * Defina a URL: AGRO_ELECTRON_URL ou AGRO_APP_URL ou padrão Render.
 *
 * disable-http-cache: cada recurso (HTML/JS/CSS) vem da rede — evita ficar preso em
 * versão antiga após deploy. (localStorage do PDV não é apagado por isso.)
 */
const { app, BrowserWindow, session, globalShortcut } = require('electron');

// Deve rodar antes de ready — desliga cache HTTP do Chromium neste app
app.commandLine.appendSwitch('disable-http-cache');

const DEFAULT_URL =
  process.env.AGRO_ELECTRON_URL ||
  process.env.AGRO_APP_URL ||
  'https://agro-consulta.onrender.com';

/** Evita query duplicada */
function urlComBust(u) {
  try {
    const x = new URL(u);
    x.searchParams.set('agro_electron', String(Date.now()));
    return x.toString();
  } catch {
    return u;
  }
}

function installNoCacheForOrigin() {
  let host = '';
  try {
    host = new URL(DEFAULT_URL).host;
  } catch {
    return;
  }
  if (!host) return;

  const filter = { urls: [`*://${host}/*`] };

  session.defaultSession.webRequest.onBeforeSendHeaders(filter, (details, callback) => {
    const headers = { ...details.requestHeaders };
    headers['Cache-Control'] = 'no-cache';
    headers.Pragma = 'no-cache';
    callback({ requestHeaders: headers });
  });
}

async function createWindow() {
  try {
    await session.defaultSession.clearCache();
  } catch (e) {
    console.error('Agro Electron: clearCache falhou (seguindo mesmo assim)', e);
  }

  const win = new BrowserWindow({
    width: 1440,
    height: 900,
    minWidth: 1024,
    minHeight: 700,
    show: false,
    autoHideMenuBar: true,
    webPreferences: {
      contextIsolation: true,
      nodeIntegration: false,
    },
  });

  win.once('ready-to-show', () => win.show());

  const target = urlComBust(DEFAULT_URL);
  try {
    await win.loadURL(target, {
      extraHeaders: ['Cache-Control: no-cache', 'Pragma: no-cache'].join('\n'),
    });
  } catch (err) {
    console.error('Falha ao carregar URL:', target, err);
  }

  win.webContents.setWindowOpenHandler(() => ({ action: 'deny' }));
}

function registerReloadShortcut() {
  try {
    const ok = globalShortcut.register('CommandOrControl+Shift+R', () => {
      const w = BrowserWindow.getFocusedWindow();
      if (w && !w.isDestroyed()) {
        w.webContents.reloadIgnoringCache();
      }
    });
    if (!ok) {
      console.warn('Agro Electron: atalho Ctrl+Shift+R não registrado (outro app pode ter tomado)');
    }
  } catch (e) {
    console.warn('Agro Electron: registro de atalho', e);
  }
}

app.whenReady().then(async () => {
  installNoCacheForOrigin();
  registerReloadShortcut();
  await createWindow();
});

app.on('will-quit', () => {
  globalShortcut.unregisterAll();
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit();
});

app.on('activate', () => {
  if (BrowserWindow.getAllWindows().length === 0) {
    void createWindow();
  }
});
