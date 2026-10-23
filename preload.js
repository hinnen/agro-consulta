const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('agroShell', {
  /** http(s) e whatsapp: → shell.openExternal no processo principal. */
  openExternal: (url) => ipcRenderer.invoke('agro-open-external', url),
  listPrinters: () => ipcRenderer.invoke('agro-list-printers'),
  silentPrint: (payload) => ipcRenderer.invoke('agro-silent-print', payload),
  isElectron: true,
});

