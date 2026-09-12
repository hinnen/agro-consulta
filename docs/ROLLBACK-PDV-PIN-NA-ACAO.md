# Rollback — PDV PIN na ação (loja alvo **v20.22**)

Volta a loja para o tip **antes** deste SOLO (`PDV-PIN-NA-ACAO`).

## Quando usar

Só se, após o deploy, a loja não conseguir confirmar venda / Pedir / chat por causa do PIN, ou comportamento inaceitável no balcão.

**Exige** frase explícita + senha `99738595` na **mesma** mensagem (produção).

## Pontos de restauração

| Item | Valor |
| ---- | ----- |
| Tag | `rollback/pre-pin-na-acao-v20.21` |
| Commit | `26cb4f9` (Live **v20.21** Chat pisca) |
| Branch backup | `producao-backup-pre-v2022-pin-na-acao-20260831` (se criada no deploy) |

## O que este pacote muda (para saber o que some no rollback)

- Consulta/carrinho sem PIN ao abrir o PDV
- Confirmar venda / Pedir loja / chat exigem PIN fresco (~45 s; mouse não renova)
- Descanso ~3 min continua (trava de tela)

## Como reverter (assistente)

1. Confirmar frase + senha do Renan.
2. `git checkout producao` · reset/hard para a tag `rollback/pre-pin-na-acao-v20.21` **ou** cherry reverso do commit do PREP.
3. `git push origin producao` (com autorização).
4. Render redeploy · Renan **Ctrl+F5** nos PDVs · smoke: 1 venda.

## Sem migrate

Não há migration neste pacote — rollback só de código.
