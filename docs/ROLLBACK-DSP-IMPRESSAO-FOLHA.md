# Rollback — Dispenser folha A4 (`DSP-IMPRESSAO-FOLHA`)

Ponto **antes** deste pacote = loja **Live v25.75** (`b934ae31`).

| Item | Valor |
| ---- | ----- |
| **Commit loja antes** | `b934ae31` |
| **Tag** | `rollback/pre-dsp-impressao-folha-v25.75` |
| **Branch backup** | `producao-backup-pre-v2575-dsp-impressao-20260930` |
| **O quê sobe** | só a impressão do Dispenser (4 A6 na A4 em pé · 2 A5 em pé na A4 deitada) |
| **O quê NÃO sobe** | merge do `teste` |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-dsp-impressao-folha-v25.75
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v25.75**. O PDV, o caixa e o financeiro não entram neste pacote.
