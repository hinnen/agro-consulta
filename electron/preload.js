const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('agroShell', {
  /** Abre URL no navegador/app do SO (WhatsApp, Maps, etc.) — necessário no Electron. */
  openExternal: (url) => ipcRenderer.invoke('agro-open-external', url),
  /** Lista impressoras disponíveis (Electron). */
  listPrinters: () => ipcRenderer.invoke('agro-list-printers'),
  /** Impressão silenciosa (sem diálogo do Windows). */
  silentPrint: (payload) => ipcRenderer.invoke('agro-silent-print', payload),
});
