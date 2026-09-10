# Rollback — MP-POINT-POLL-RETRY (loja alvo **v23.73**)

Ponto **antes** deste pacote = loja **Live v23.70** (`71a169a` / checklist 09/09).

| Item | Valor |
| ---- | ----- |
| **Commit loja antes** | `71a169a` (Live v23.70) |
| **Tag** | `rollback/pre-mp-point-poll-retry-v23.70` |
| **Branch backup** | `producao-backup-pre-v2373-mp-point-poll-retry-20260909` |
| **O quê sobe** | `MP-POINT-POLL-RETRY` (poll Point não desiste no 502) |
| **O quê NÃO sobe** | merge do `teste` · outros |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-mp-point-poll-retry-v23.70
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v23.70**.
