/**
 * Path ETQ-PRINT-DIRETO — ponte Windows + modo impressão configurável.
 * node scripts/verify_etq_print_direto_path.js
 */
'use strict';

const fs = require('fs');
const path = require('path');
const vm = require('vm');

const root = path.join(__dirname, '..');
let n = 0;
let fails = 0;

function ok(cond, msg) {
  n += 1;
  if (cond) console.log('OK:', msg);
  else {
    fails += 1;
    console.error('FAIL:', msg);
  }
}

function read(rel) {
  return fs.readFileSync(path.join(root, rel), 'utf8');
}

const bridgeMain = read('agro-print-bridge/main.js');
const bridgePkg = read('agro-print-bridge/package.json');
const bridgeBat = read('agro-print-bridge/Iniciar-ponte-etiquetas.bat');
const bridgeClient = read('produtos/static/produtos/js/agro_print_bridge.js');
const coreSrc = read('produtos/static/produtos/js/produtos_etiquetas_core.js');
const pageJs = read('produtos/static/produtos/js/produtos_etiquetas.js');
const pageHtml = read('produtos/templates/produtos/produtos_etiquetas.html');
const loteHtml = read('produtos/templates/produtos/produtos_etiquetas_lote.html');
const nfeHtml = read('produtos/templates/produtos/entrada_nota.html');
const cadHtml = read('produtos/templates/produtos/produtos_cadastro_erp.html');
const pdvHtml = read('produtos/templates/produtos/pdv_wizard.html');

ok(bridgeMain.includes("HOST = '127.0.0.1'"), 'ponte só localhost');
ok(bridgeMain.includes('19192'), 'porta padrão 19192');
ok(bridgeMain.includes("url === '/print'"), 'endpoint /print');
ok(bridgeMain.includes("url === '/printers'"), 'endpoint /printers');
ok(bridgeMain.includes('silent: true'), 'print silencioso');
ok(bridgeMain.includes('pageSize'), 'pageSize microns');
ok(bridgePkg.includes('"agro-print-bridge"'), 'package bridge');
ok(fs.existsSync(path.join(root, 'agro-print-bridge/Instalar-inicio-Windows.bat')), 'bat inicio Windows');
ok(fs.existsSync(path.join(root, 'agro-print-bridge/iniciar-silencioso.vbs')), 'vbs silencioso');
ok(fs.existsSync(path.join(root, 'agro-print-bridge/Remover-inicio-Windows.bat')), 'bat remover inicio');
ok(bridgeMain.includes('Abrir com o Windows'), 'menu tray inicio automatico');
ok(bridgeBat.toLowerCase().includes('ensure-electron'), 'bat prepara electron');
ok(fs.existsSync(path.join(root, 'agro-print-bridge/ensure-electron.js')), 'ensure-electron.js');
ok(bridgeBat.includes('ELECTRON_OVERRIDE_DIST_PATH'), 'bat usa LocalAppData');
ok(bridgeBat.toLowerCase().includes('npm start'), 'bat inicia electron');
ok(bridgeClient.includes('agroPrintBridge'), 'cliente JS');
ok(bridgeClient.includes('/health'), 'cliente health');
ok(coreSrc.includes('normalizarPrintModo'), 'core print_modo');
ok(coreSrc.includes('getPrintShell'), 'core getPrintShell');
ok(coreSrc.includes('deveUsarSilent'), 'core deveUsarSilent');
ok(coreSrc.includes('ponte_offline'), 'core aviso ponte offline');
ok(coreSrc.includes("print_modo: 'auto'"), 'default print_modo auto');
ok(pageJs.includes('etq-preset-print-modo'), 'UI salva print_modo');
ok(pageJs.includes('testarBridgeUmaEtiqueta'), 'botão teste ponte');
ok(pageJs.includes('atualizarBridgeUi'), 'status ponte na tela');
ok(pageHtml.includes('etq-bridge-card'), 'card impressão direta');
ok(pageHtml.includes('etq-preset-print-modo'), 'select modo no preset');
ok(pageHtml.includes("agro_print_bridge.js' %}?v=1"), 'etiquetas puxa bridge');
ok(pageHtml.includes("produtos_etiquetas_core.js' %}?v=33"), 'etiquetas core v=33');
ok(loteHtml.includes('agro_print_bridge.js'), 'lote puxa bridge');
ok(loteHtml.includes('?v=33'), 'lote core v=33');
ok(nfeHtml.includes('agro_print_bridge.js'), 'NF puxa bridge');
ok(cadHtml.includes('agro_print_bridge.js'), 'cadastro puxa bridge');
ok(pdvHtml.includes('agro_print_bridge.js'), 'PDV puxa bridge');

const s = {
  document: { cookie: '' },
  localStorage: {
    _d: {},
    getItem(k) {
      return Object.prototype.hasOwnProperty.call(this._d, k) ? this._d[k] : null;
    },
    setItem(k, v) {
      this._d[k] = String(v);
    },
  },
};
s.window = s;
vm.createContext(s);
vm.runInContext(coreSrc, s);
const Core = s.AgroEtiquetasCore;
ok(!!Core, 'Core carregou');

const pAuto = Core.normalizarPreset({ id: 'x', estilo: 'termica', print_modo: 'auto' });
ok(pAuto.print_modo === 'auto', 'normaliza auto');
const pDir = Core.normalizarPreset({ id: 'x', estilo: 'termica', print_modo: 'direto' });
ok(pDir.print_modo === 'direto', 'normaliza direto');
const pDlg = Core.normalizarPreset({ id: 'x', estilo: 'termica', print_modo: 'DIALOG' });
ok(pDlg.print_modo === 'auto', 'modo inválido vira auto');

ok(Core.deveUsarSilent({ print_modo: 'auto' }).use === false, 'auto sem ponte = diálogo');
ok(Core.deveUsarSilent({ print_modo: 'dialogo' }).use === false, 'dialogo nunca silent');
ok(Core.deveUsarSilent({ print_modo: 'direto' }).reason === 'ponte_offline', 'direto sem ponte falha');

s.agroPrintBridge = {
  isReady: function () {
    return true;
  },
  silentPrint: function () {
    return Promise.resolve({ ok: true });
  },
};
ok(Core.podeSilentPrint() === true, 'podeSilent com bridge');
ok(Core.deveUsarSilent({ print_modo: 'auto' }).use === true, 'auto com ponte = direto');
ok(Core.deveUsarSilent({ print_modo: 'direto' }).use === true, 'direto com ponte = ok');
ok(Core.deveUsarSilent({ print_modo: 'dialogo' }).use === false, 'dialogo ignora ponte');

if (fails) {
  console.error('VERIFY_FAIL', fails + '/' + n);
  process.exit(1);
}
console.log('VERIFY_OK', n + '/' + n);
console.log('OK: path ETQ-PRINT-DIRETO verificado.');
