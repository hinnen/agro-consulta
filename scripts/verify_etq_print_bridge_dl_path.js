/**
 * Path ETQ-PRINT-BRIDGE-DL — botão Baixar ponte no SisVale.
 * node scripts/verify_etq_print_bridge_dl_path.js
 */
'use strict';

const fs = require('fs');
const path = require('path');
const { execFileSync } = require('child_process');

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

const util = read('produtos/etiquetas_print_bridge_util.py');
const views = read('produtos/views.py');
const urls = read('produtos/urls.py');
const html = read('produtos/templates/produtos/produtos_etiquetas.html');
const js = read('produtos/static/produtos/js/produtos_etiquetas.js');

ok(util.includes('build_print_bridge_zip'), 'util zip');
ok(util.includes('1-INSTALAR.bat'), 'util inclui 1-INSTALAR');
ok(util.includes('LEIA-ME.txt'), 'util inclui LEIA-ME');
ok(util.includes('node_modules'), 'util exclui node_modules');
ok(views.includes('api_etiquetas_print_bridge_download'), 'view download');
ok(views.includes('Agro-Etiqueta-Print-Windows.zip'), 'nome zip');
ok(views.includes('api_etq_bridge_download_url'), 'view passa URL template');
ok(urls.includes('print-bridge/download'), 'url download');
ok(urls.includes('api_etiquetas_print_bridge_download'), 'url name');
ok(html.includes('etq-btn-bridge-download'), 'botão Baixar ponte');
ok(html.includes('Baixar ponte'), 'rótulo botão');
ok(html.includes('1-INSTALAR.bat'), 'dica 1-INSTALAR');
ok(html.includes('bridgeDownloadUrl'), 'cfg bridgeDownloadUrl');
ok(js.includes('etq-btn-bridge-download'), 'JS hook download');
ok(js.includes('1-INSTALAR.bat'), 'JS avisa instalar');

try {
  const py = `
import sys
sys.path.insert(0, r"${root.replace(/\\/g, '\\\\')}")
from produtos.etiquetas_print_bridge_util import build_print_bridge_zip
z = build_print_bridge_zip()
assert z[:2] == b"PK", "nao e zip"
assert len(z) > 1000, "zip muito pequeno"
print("ZIP_OK", len(z))
`;
  const out = execFileSync('python', ['-c', py], { cwd: root, encoding: 'utf8' });
  ok(out.includes('ZIP_OK'), 'zip gera PK (' + out.trim() + ')');
} catch (e) {
  console.error(e && e.stderr ? e.stderr : e);
  ok(false, 'zip gera PK');
}

if (fails) {
  console.error('VERIFY_FAIL', fails + '/' + n);
  process.exit(1);
}
console.log('VERIFY_OK', n + '/' + n);
