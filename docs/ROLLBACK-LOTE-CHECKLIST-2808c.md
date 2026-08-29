# Rollback â€” lote checklist 2808c (loja alvo **v18.72**)

Ponto **antes** deste lote = loja **Live v18.64** (`5e6e44a`).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje** | `5e6e44a` (Live v18.64) |
| **Tag (no deploy)** | `rollback/pre-lote-checklist-2808c-v18.64` |
| **Branch backup (no deploy)** | `producao-backup-pre-v1872-lote-checklist-20260828` |
| **Branch PREP** | `deploy/prep-checklist-2808c` @ tip **v18.72** |
| **O quÃª sobe** | `NS-ESCOLHA-EMP` Â· `REPASSE-PDV-OVERLAY-POPUP` Â· `CP-EMP-PG-FALLBACK` |
| **O quÃª NÃƒO reverte** | lote v18.64 (modo por forma, cofrinho acumulado, CP busca, overlay limpo base) |
| **Migrate** | **NÃƒO** â€” zero migration nova |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-lote-checklist-2808c-v18.64
git push origin producao --force-with-lease
```

**SÃ³** com frase + senha do Renan. Volta para Live **v18.64**. Badge da loja volta a **v18.64**.

## Risco operacional (lojas abertas)

| Pacote | Afeta venda PDV? | Nota |
| ------ | ---------------- | ---- |
| `NS-ESCOLHA-EMP` | **NÃ£o** | SÃ³ modal Nova saÃ­da (CP/BI) |
| `REPASSE-PDV-OVERLAY-POPUP` | **NÃ£o** (venda) | SÃ³ tela Repasse; quem/PIN em popup; forma = Dinheiro |
| `CP-EMP-PG-FALLBACK` | **NÃ£o** | SÃ³ gravaÃ§Ã£o de emprÃ©stimo se Mongo off |

Rotina: pausar finalizaÃ§Ãµes ~2 min Â· Zap Â· Ctrl+F5 nos PDVs apÃ³s Live.
