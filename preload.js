const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('agroShell', {
  /** Abre http(s) no navegador do SO — necessário no Electron (window.open para wa.me/Maps falha). */
  openExternal: (url) => ipcRenderer.invoke('agro-open-external', url),
});

