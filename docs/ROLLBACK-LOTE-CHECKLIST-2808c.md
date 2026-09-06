# Rollback — lote checklist 2808c (loja alvo **v18.72**)

Ponto **antes** deste lote = loja **Live v18.64** (`5e6e44a`).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje** | `5e6e44a` (Live v18.64) |
| **Tag (no deploy)** | `rollback/pre-lote-checklist-2808c-v18.64` |
| **Branch backup (no deploy)** | `producao-backup-pre-v1872-lote-checklist-20260828` |
| **Branch PREP** | `deploy/prep-checklist-2808c` @ **`ae126d9`** / **v18.72** (Live) |
| **O quê sobe** | `NS-ESCOLHA-EMP` · `REPASSE-PDV-OVERLAY-POPUP` · `CP-EMP-PG-FALLBACK` |
| **O quê NÃO reverte** | lote v18.64 (modo por forma, cofrinho acumulado, CP busca, overlay limpo base) |
| **Migrate** | **NÃO** — zero migration nova |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-lote-checklist-2808c-v18.64
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v18.64**. Badge da loja volta a **v18.64**.

## Risco operacional (lojas abertas)

| Pacote | Afeta venda PDV? | Nota |
| ------ | ---------------- | ---- |
| `NS-ESCOLHA-EMP` | **Não** | Só modal Nova saída (CP/BI) |
| `REPASSE-PDV-OVERLAY-POPUP` | **Não** (venda) | Só tela Repasse; quem/PIN em popup; forma = Dinheiro |
| `CP-EMP-PG-FALLBACK` | **Não** | Só gravação de empréstimo se Mongo off |

Rotina: pausar finalizações ~2 min · Zap · Ctrl+F5 nos PDVs após Live.
