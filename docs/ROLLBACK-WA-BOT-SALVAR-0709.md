# Rollback — WA-BOT-SALVAR (loja alvo **v23.25**)

Ponto **antes** deste pacote = loja **Live v23.24** (`82d2ac2`).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `82d2ac2` (Live v23.24) |
| **Tag** | `rollback/pre-wa-bot-salvar-v23.24` |
| **Branch backup** | `producao-backup-pre-v2325-wa-bot-salvar-20260907` |
| **Branch PREP** | `deploy/prep-wa-bot-salvar-0709` · tip **v23.25** |
| **O quê sobe** | `WA-BOT-SALVAR` — Bot Salvar não travava (poll&lt;3 + HTML5) · `novalidate` · `type=button` · clamp poll |
| **O quê NÃO sobe** | merge do `teste` · instrumentação de debug |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-wa-bot-salvar-v23.24
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v23.24**.

## Risco operacional

| Pacote | Afeta finalizar venda? | Nota |
| ------ | ---------------------- | ---- |
| `WA-BOT-SALVAR` | **Não** | Só tela Bot WhatsApp |

## Depois do deploy (você)

1. **Ctrl+F5** no Bot · badge **v23.25**
2. Bot → **Salvar** → deve aparecer «Salvo»
3. (lentidão) Tempo → Checar saída **5** → Salvar
