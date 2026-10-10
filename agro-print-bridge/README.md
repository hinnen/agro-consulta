# Agro Etiqueta Print (ponte local SisVale)

## Loja (Windows)

1. No SisVale → **Etiquetas** → **Baixar ponte**.
2. Extraia o ZIP.
3. Dois cliques **somente** em **`CLIQUE-AQUI-INSTALAR.bat`** (o resto fica na pasta `app`).
4. Espere “Pronto”. O card das etiquetas deve ficar verde.
5. A ponte sobe sozinha ao ligar o PC.

Node portatil ja vem dentro do ZIP (`app/vendor`). Electron ainda baixa na 1a vez (internet).

## Dev

```bat
cd agro-print-bridge
npm install
node ensure-electron.js
npm start
```
