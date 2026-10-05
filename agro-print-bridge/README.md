# Agro Etiqueta Print (ponte Windows)

Imprime etiquetas do SisVale **direto** na impressora, sem ficar escolhendo tamanho 40×40 / 53×30 na janela do Windows.

## Como usar (loja)

1. No PC da etiqueta, abra a pasta `agro-print-bridge`.
2. Dê dois cliques em **`Iniciar-ponte-etiquetas.bat`** (primeira vez instala; demora um pouco).
3. Deixe o ícone na bandeja do Windows (canto).
4. No Chrome, abra **Etiquetas** → veja o cartão **Impressão direta** ficar **verde**.
5. Em cada **preset**, escolha:
   - **Impressora** (qual máquina física)
   - **Modo**: Automático / Direto / Janela Windows
6. Salve o preset. Imprimir.

## Cenários

| Situação | Como configurar |
| -------- | --------------- |
| **1 impressora**, troca a bobina (40×40 ↔ 53×30) | Vários presets, **mesma impressora**, tamanhos diferentes. Troca a bobina e escolhe o preset certo. |
| **2 ou 3 impressoras** (cada uma com bobina fixa) | Um preset por bobina; cada um com **sua impressora**. |
| Sem a ponte ligada | Continua abrindo a janela do Windows (modo Automático). |

## Porta

Padrão: `127.0.0.1:19192`. Só neste PC. No SisVale dá para mudar a porta no cartão (se precisar).

## Build instalador (opcional)

```bat
cd agro-print-bridge
npm install
npm run build
```

Gera instalador em `agro-print-bridge/dist`.
