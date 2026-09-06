# Rollback — lote checklist 3108b (loja alvo **v20.49**)

Ponto **antes** deste lote = loja **Live v20.45** (`18fc7d1`).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `18fc7d1` (Live v20.45) |
| **Tag (criada no PREP)** | `rollback/pre-lote-checklist-3108b-v20.45` |
| **Branch backup (criada no PREP)** | `producao-backup-pre-v2049-lote-checklist-20260831` |
| **Branch PREP** | `deploy/prep-checklist-3108b` · tip **v20.49** |
| **O quê sobe** | `PDV-WA-COR` · `REPASSE-ARREDONDA-COFRE` |
| **O quê NÃO sobe** | `WA-ATEND-QR` · `BI-META-C-VILA-RAMP` |
| **O quê NÃO reverte** | lote v20.45 (topbar/Mais/fundo troco) e anteriores |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-lote-checklist-3108b-v20.45
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v20.45**.

## Risco operacional (lojas abertas)

| Pacote | Afeta venda PDV? | Nota |
| ------ | ---------------- | ---- |
| `PDV-WA-COR` | **Não** | Só CSS do ícone Zap (#25D366) — ainda «Em breve» |
| `REPASSE-ARREDONDA-COFRE` | **Não** (venda) | Só Repasse — arredondar 3 campos |

Rotina no deploy: pausar vendas ~2–3 min · Zap · Ctrl+F5 · smoke: Zap verde · Repasse 50/100.

## Deploy (próximo chat + senha)

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-checklist-3108b
git push origin producao --force-with-lease
```

Render sobe · conferir healthz / badge **v20.49**.
