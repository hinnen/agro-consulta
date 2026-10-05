# Agro Etiqueta Print (ponte Windows)

Imprime etiquetas do SisVale **direto** na impressora, sem ficar escolhendo tamanho 40×40 / 53×30 na janela do Windows.

## Na loja (uma vez por PC)

1. SisVale → Etiquetas → **Baixar ponte** (ou use os arquivos desta pasta).
2. Extraia o ZIP → dois cliques em **`1-INSTALAR.bat`** / **`Instalar-inicio-Windows.bat`**.
3. Na 1ª vez baixa **Node** e **Electron** sozinho (internet). Sem instalar nada na mão.
4. Ponte em segundo plano; ao ligar o PC sobe sozinha.

| Arquivo | Quando usar |
| ------- | ----------- |
| `Instalar-inicio-Windows.bat` | **1× por PC** — instala + início automático |
| `ensure-node.bat` / `ensure-node.ps1` | Baixa Node portátil se faltar (automático) |
| `Iniciar-ponte-etiquetas.bat` | Manual (teste / se caiu) |
| `Remover-inicio-Windows.bat` | Tirar do início automático |
| `iniciar-silencioso.vbs` | Usado pelo Windows na partida |

## Cenários

| Situação | Como configurar |
| -------- | --------------- |
| **1 impressora**, troca a bobina (40×40 ↔ 53×30) | Vários presets, **mesma impressora**, tamanhos diferentes. |
| **2 ou 3 impressoras** | Um preset por bobina; cada um com **sua impressora**. |
| Sem a ponte ligada | Continua abrindo a janela do Windows (modo Automático). |

## Porta

Padrão: `127.0.0.1:19192`. Só neste PC.

## Build instalador (opcional)

```bat
cd agro-print-bridge
npm install
npm run build
```

Gera instalador em `agro-print-bridge/dist`.
