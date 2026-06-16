const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('agroShell', {
  /** Abre URL no navegador/app do SO (WhatsApp, Maps, etc.) — necessário no Electron. */
  openExternal: (url) => ipcRenderer.invoke('agro-open-external', url),
  listPrinters: () => ipcRenderer.invoke('agro-list-printers'),
  silentPrint: (payload) => ipcRenderer.invoke('agro-silent-print', payload),
  isElectron: true,
});
