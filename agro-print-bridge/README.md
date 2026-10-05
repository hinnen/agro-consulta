# Agro Etiqueta Print (ponte Windows)

Imprime etiquetas do SisVale **direto** na impressora, sem ficar escolhendo tamanho 40×40 / 53×30 na janela do Windows.

## Na loja (uma vez por PC)

1. Abra a pasta `agro-print-bridge`.
2. Dê dois cliques em **`Instalar-inicio-Windows.bat`**.
3. Espere “Pronto”. A ponte sobe em **segundo plano** (ícone na bandeja).
4. Chrome → **Etiquetas** → card **Impressão direta** verde.
5. Em cada preset: impressora + modo → **Salvar**.

**Depois disso:** ao **ligar o computador**, a ponte **abre sozinha**. Não precisa rodar script todo dia.

| Arquivo | Quando usar |
| ------- | ----------- |
| `Instalar-inicio-Windows.bat` | **1× por PC** — instala + início automático |
| `Iniciar-ponte-etiquetas.bat` | Manual (teste / se caiu) |
| `Remover-inicio-Windows.bat` | Tirar do início automático |
| `iniciar-silencioso.vbs` | Usado pelo Windows na partida (não precisa abrir) |

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
