# Rollback — WA-HORARIO-DIA (loja alvo **v23.24**)

Ponto **antes** deste pacote = loja **Live v23.23** (`1b25379`).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `1b25379` (Live v23.23) |
| **Tag** | `rollback/pre-wa-horario-dia-v23.23` |
| **Branch backup** | `producao-backup-pre-v2324-wa-horario-20260906` |
| **Branch PREP** | `deploy/prep-wa-horario-dia-0609` · tip **v23.24** |
| **O quê sobe** | `WA-HORARIO-DIA` — Bot Horário com 7 dias (Abre/Fecha) · `horario_por_dia` no JSON · `fora_do_horario` por dia |
| **O quê NÃO sobe** | merge do `teste` · resto do tip |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-wa-horario-dia-v23.23
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v23.23**.

## Risco operacional

| Pacote | Afeta finalizar venda? | Nota |
| ------ | ---------------------- | ---- |
| `WA-HORARIO-DIA` | **Não** | Só Bot WhatsApp (`/atendimento-whatsapp/bot/` + regra fora do horário) |

## Depois do deploy (você)

1. **Ctrl+F5** no Bot WhatsApp · badge **v23.24**
2. Bot → Horário → 7 linhas (Dom…Sáb) · ajuste Sáb se quiser · **Salvar**
3. (opcional lentidão) Bot → Tempo → Checar saída **5** → Salvar
