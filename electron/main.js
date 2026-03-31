/**
 * Shell Electron para o Agro Consulta (PDV web).
 * Defina a URL do Django: variável de ambiente AGRO_ELECTRON_URL ou arquivo .env na pasta do .exe (ver README em electron/).
 */
const { app, BrowserWindow } = require('electron');
const path = require('path');

const DEFAULT_URL =
  process.env.AGRO_ELECTRON_URL ||
  process.env.AGRO_APP_URL ||
  'https://agro-consulta.onrender.com';

function createWindow() {
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
  win.loadURL(DEFAULT_URL).catch((err) => {
    console.error('Falha ao carregar URL:', DEFAULT_URL, err);
  });
  win.webContents.setWindowOpenHandler(() => ({ action: 'deny' }));
}

app.whenReady().then(createWindow);

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit();
});

app.on('activate', () => {
  if (BrowserWindow.getAllWindows().length === 0) createWindow();
});
