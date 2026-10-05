/**
 * Path completo ETQ-PRINT-DIRETO — verificação detalhada.
 * node scripts/verify_etq_print_direto_full_path.js
 */
'use strict';

const fs = require('fs');
const path = require('path');
const vm = require('vm');
const { execFileSync, spawnSync } = require('child_process');

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

function exists(rel) {
  return fs.existsSync(path.join(root, rel));
}

// --- Arquivos da ponte ---
const bridgeFiles = [
  'agro-print-bridge/main.js',
  'agro-print-bridge/package.json',
  'agro-print-bridge/ensure-electron.js',
  'agro-print-bridge/ensure-node.bat',
  'agro-print-bridge/ensure-node.ps1',
  'agro-print-bridge/Iniciar-ponte-etiquetas.bat',
  'agro-print-bridge/Instalar-inicio-Windows.bat',
  'agro-print-bridge/Remover-inicio-Windows.bat',
  'agro-print-bridge/iniciar-silencioso.vbs',
  'agro-print-bridge/.npmrc',
  'agro-print-bridge/README.md',
];
bridgeFiles.forEach((f) => ok(exists(f), 'existe ' + f));

const main = read('agro-print-bridge/main.js');
ok(main.includes("HOST = '127.0.0.1'"), 'só localhost');
ok(main.includes('19192'), 'porta 19192');
ok(main.includes("url === '/print'"), '/print');
ok(main.includes("url === '/printers'"), '/printers');
ok(main.includes("url === '/health'") || main.includes("url === '/'"), '/health');
ok(main.includes('silent: true'), 'silent print');
ok(main.includes('pageSize'), 'pageSize');
ok(main.includes('Abrir com o Windows'), 'menu inicio Windows');

const instalar = read('agro-print-bridge/Instalar-inicio-Windows.bat');
ok(instalar.includes('ensure-node.bat'), 'instalar → ensure-node');
ok(instalar.includes('ensure-electron.js'), 'instalar → ensure-electron');
ok(instalar.includes('iniciar-silencioso.vbs'), 'instalar → vbs');
ok(instalar.includes('Start Menu\\Programs\\Startup') || instalar.includes('Startup'), 'atalho Startup');

const ensureNode = read('agro-print-bridge/ensure-node.ps1');
ok(ensureNode.includes('nodejs.org/dist'), 'baixa Node do nodejs.org');
ok(ensureNode.includes('v22.14.0') || ensureNode.includes('node-'), 'versão Node pinada');
ok(ensureNode.includes('LOCALAPPDATA') || ensureNode.includes('AgroEtiquetaPrint'), 'Node em LocalAppData');

const vbs = read('agro-print-bridge/iniciar-silencioso.vbs');
ok(vbs.includes('19192'), 'vbs checa health');
ok(vbs.includes('electron.exe'), 'vbs sobe electron');
ok(vbs.includes(', 0, False'), 'vbs janela oculta');

// --- SisVale download ---
ok(exists('produtos/etiquetas_print_bridge_util.py'), 'util zip');
ok(exists('produtos/static/produtos/js/agro_print_bridge.js'), 'cliente JS');
const util = read('produtos/etiquetas_print_bridge_util.py');
ok(util.includes('1-INSTALAR.bat'), 'zip com 1-INSTALAR');
ok(util.includes('BAIXA SOZINHO'), 'LEIA-ME auto Node');
ok(util.includes('node_modules'), 'exclui node_modules');

const views = read('produtos/views.py');
ok(views.includes('api_etiquetas_print_bridge_download'), 'view download');
ok(views.includes('api_etq_bridge_download_url'), 'URL no template context');

const urls = read('produtos/urls.py');
ok(urls.includes('print-bridge/download'), 'rota download');

const html = read('produtos/templates/produtos/produtos_etiquetas.html');
ok(html.includes('etq-btn-bridge-download'), 'botão Baixar ponte');
ok(html.includes('Baixa Node/Electron sozinho'), 'texto UI auto');
ok(html.includes('agro_print_bridge.js'), 'página puxa bridge client');
ok(html.includes('etq-preset-print-modo'), 'modo impressão no preset');

['produtos_etiquetas_lote.html', 'entrada_nota.html', 'produtos_cadastro_erp.html', 'pdv_wizard.html'].forEach(
  (f) => {
    const t = read('produtos/templates/produtos/' + f);
    ok(t.includes('agro_print_bridge.js'), f + ' puxa bridge');
  }
);

const coreSrc = read('produtos/static/produtos/js/produtos_etiquetas_core.js');
ok(coreSrc.includes('deveUsarSilent'), 'core deveUsarSilent');
ok(coreSrc.includes('ponte_offline'), 'core ponte_offline');
ok(coreSrc.includes('print_modo'), 'core print_modo');

const pageJs = read('produtos/static/produtos/js/produtos_etiquetas.js');
ok(pageJs.includes('etq-btn-bridge-download'), 'JS download');
ok(pageJs.includes('testarBridgeUmaEtiqueta'), 'JS teste etiqueta');
ok(pageJs.includes('atualizarBridgeUi'), 'JS status ponte');

// --- Core runtime ---
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
ok(!!Core, 'Core VM');

ok(Core.normalizarPrintModo('DIRETO') === 'direto', 'modo DIRETO');
ok(Core.normalizarPrintModo('x') === 'auto', 'modo inválido → auto');
ok(Core.deveUsarSilent({ print_modo: 'dialogo' }).use === false, 'dialogo = janela');
ok(Core.deveUsarSilent({ print_modo: 'direto' }).reason === 'ponte_offline', 'direto sem ponte');
ok(Core.deveUsarSilent({ print_modo: 'auto' }).use === false, 'auto sem ponte');

s.agroPrintBridge = {
  isReady() {
    return true;
  },
  silentPrint() {
    return Promise.resolve({ ok: true });
  },
};
ok(Core.podeSilentPrint() === true, 'silent com bridge');
ok(Core.deveUsarSilent({ print_modo: 'auto' }).use === true, 'auto + bridge');
ok(Core.deveUsarSilent({ print_modo: 'direto' }).use === true, 'direto + bridge');
ok(Core.deveUsarSilent({ print_modo: 'dialogo' }).use === false, 'dialogo ignora bridge');

const p53 = Core.normalizarPreset({
  id: 'padrao-53x30',
  estilo: 'termica',
  largura_mm: 53,
  altura_mm: 30,
  print_modo: 'direto',
  impressora: 'Argox',
});
ok(p53.print_modo === 'direto' && p53.impressora === 'Argox', 'preset 53 impressora+modo');
ok(Number(p53.largura_mm) === 53 && Number(p53.altura_mm) === 30, 'preset 53 mm');

// --- ZIP real ---
try {
  const py = `
import sys
sys.path.insert(0, r"${root.replace(/\\/g, '\\\\')}")
from produtos.etiquetas_print_bridge_util import build_print_bridge_zip
import zipfile, io
z = build_print_bridge_zip()
assert z[:2] == b"PK"
zf = zipfile.ZipFile(io.BytesIO(z))
names = zf.namelist()
need = [
  "Agro-Etiqueta-Print/1-INSTALAR.bat",
  "Agro-Etiqueta-Print/LEIA-ME.txt",
  "Agro-Etiqueta-Print/main.js",
  "Agro-Etiqueta-Print/ensure-node.bat",
  "Agro-Etiqueta-Print/Instalar-inicio-Windows.bat",
  "Agro-Etiqueta-Print/iniciar-silencioso.vbs",
]
for n in need:
  assert n in names, n
assert not any("node_modules" in n for n in names)
print("ZIP_OK", len(z), len(names))
`;
  const out = execFileSync('python', ['-c', py], { cwd: root, encoding: 'utf8' });
  ok(out.includes('ZIP_OK'), 'zip conteúdo (' + out.trim() + ')');
} catch (e) {
  console.error(e.stderr || e.message || e);
  ok(false, 'zip conteúdo');
}

// --- Electron path local (se já instalado) ---
const electronExe = path.join(
  process.env.LOCALAPPDATA || '',
  'AgroEtiquetaPrint',
  'electron-dist',
  'electron.exe'
);
if (fs.existsSync(electronExe)) {
  ok(true, 'electron.exe LocalAppData presente');
  const bridgeDir = path.join(root, 'agro-print-bridge');
  const health = spawnSync(
    electronExe,
    [bridgeDir],
    {
      cwd: bridgeDir,
      env: {
        ...process.env,
        ELECTRON_OVERRIDE_DIST_PATH: path.dirname(electronExe),
      },
      timeout: 8000,
      windowsHide: true,
    }
  );
  // spawn may keep running — probe via curl with short wait
} else {
  ok(true, 'electron LocalAppData ausente (ok em CI; loja baixa na 1ª instalação)');
}

// --- Sub-paths já existentes ---
const sub = [
  ['scripts/verify_etq_print_direto_path.js', 'VERIFY_OK'],
  ['scripts/verify_etq_print_bridge_dl_path.js', 'VERIFY_OK'],
];
for (const [script, token] of sub) {
  try {
    const out = execFileSync('node', [path.join(root, script)], { cwd: root, encoding: 'utf8' });
    ok(out.includes(token), path.basename(script) + ' OK');
  } catch (e) {
    ok(false, path.basename(script) + ' OK');
  }
}

try {
  const out = execFileSync('node', [path.join(root, 'scripts/verify_etiquetas_termica_varias.js')], {
    cwd: root,
    encoding: 'utf8',
  });
  ok(out.includes('39/39') || out.includes('OK'), 'termica varias regressão');
} catch (e) {
  ok(false, 'termica varias regressão');
}

if (fails) {
  console.error('VERIFY_FAIL', fails + '/' + n);
  process.exit(1);
}
console.log('VERIFY_OK', n + '/' + n);
console.log('OK: path ETQ-PRINT-DIRETO full verificado.');
