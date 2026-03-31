// Preload simples: por enquanto só reserva espaço pra futuras integrações locais.
const { contextBridge } = require('electron');

contextBridge.exposeInMainWorld('agroShell', {
  // Exemplo futuro:
  // getConfig: () => { ... },
});

