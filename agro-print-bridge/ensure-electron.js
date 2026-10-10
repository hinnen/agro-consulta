/**
 * Garante o Electron fora do OneDrive (LocalAppData).
 * OneDrive costuma quebrar o extract do zip dentro da pasta do Git.
 */
'use strict';

const fs = require('fs');
const path = require('path');
const os = require('os');
const { downloadArtifact } = require('@electron/get');
const extract = require('extract-zip');

const electronPkgDir = path.join(__dirname, 'node_modules', 'electron');
const { version } = require(path.join(electronPkgDir, 'package.json'));
const distDir = path.join(
  process.env.LOCALAPPDATA || path.join(os.homedir(), 'AppData', 'Local'),
  'AgroEtiquetaPrint',
  'electron-dist'
);
const exeName = process.platform === 'win32' ? 'electron.exe' : 'electron';
const exePath = path.join(distDir, exeName);
const pathTxt = path.join(electronPkgDir, 'path.txt');

async function main() {
  if (!fs.existsSync(electronPkgDir)) {
    console.error('Falta node_modules/electron. Rode npm install nesta pasta.');
    process.exit(1);
  }

  fs.mkdirSync(distDir, { recursive: true });

  if (!fs.existsSync(exePath)) {
    console.log('Baixando Electron ' + version + ' (pode demorar)...');
    const zipPath = await downloadArtifact({
      version,
      artifactName: 'electron',
      force: process.env.force_no_cache === 'true',
      platform: process.platform,
      arch: process.arch,
    });
    console.log('Extraindo para', distDir);
    await extract(zipPath, { dir: distDir });
  }

  if (!fs.existsSync(exePath)) {
    console.error('Ainda nao achou', exePath);
    process.exit(1);
  }

  fs.writeFileSync(pathTxt, exeName, 'utf8');
  console.log('OK Electron em', exePath);
  console.log('OVERRIDE=' + distDir);
}

main().catch((e) => {
  console.error(e && e.stack ? e.stack : e);
  process.exit(1);
});
