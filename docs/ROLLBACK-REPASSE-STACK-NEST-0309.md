# Rollback — Repasse sem vidro (`REPASSE-STACK-NEST` · loja alvo **v21.87**)

Ponto **antes** deste hotfix = loja **Live v21.86** (`9adc305` · WhatsApp anti-duplicata).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `9adc305` (Live v21.86) |
| **Tag (criada no PREP)** | `rollback/pre-repasse-stack-nest-0309-v21.86` |
| **Branch backup (criada no PREP)** | `producao-backup-pre-v2187-repasse-stack-nest-20260903` |
| **Branch PREP** | `deploy/prep-repasse-stack-nest-0309` · tip **v21.87** |
| **O quê sobe** | só `agro_overlay_stack.js` (+ provas) — popup filho do Repasse sem vidro |
| **O quê NÃO sobe** | resto do `teste` (WhatsApp UI extra, Excel cadastro, etc.) |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-repasse-stack-nest-0309-v21.86
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v21.86**.

## Risco operacional

| Pacote | Afeta finalizar venda? | Nota |
| ------ | ---------------------- | ---- |
| `REPASSE-STACK-NEST` | **Não** | Só overlay de Repasse (Confirmar / 3 OKs). Carrinho e F7 iguais. |

Rotina: **Ctrl+F5** · Repasse → Confirmar → clicar Confirmar / OKs.

## Deploy

**Não** resetar `producao` para o branch `teste`.

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-repasse-stack-nest-0309
git push origin producao --force-with-lease
```
