# Rollback — Checklist 07/09 (loja alvo **v23.38**)

Ponto **antes** deste lote = loja **Live v23.34** (`95b10c3` / tip banana WA-PC-PWA-FIX).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `95b10c3` (Live v23.34) |
| **Tag** | `rollback/pre-checklist-0709-v23.34` |
| **Branch backup** | `producao-backup-pre-v2338-checklist-20260907` |
| **Branch PREP** | `deploy/prep-checklist-0709` · tip **v23.38** |
| **O quê sobe** | `WA-PC-ICON` + `EXTRAVIO-CONFERENCIA-AUTO` + `DRE-NO-DASH-OVERLAY` |
| **O quê NÃO sobe** | merge do `teste` · outros WIP |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-checklist-0709-v23.34
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v23.34**.

## Risco operacional (lojas abertas)

| Pacote | Afeta finalizar venda / PDV? | Nota |
| ------ | ---------------------------- | ---- |
| `WA-PC-ICON` | **Não** | Só PNGs do app Zap |
| `EXTRAVIO-CONFERENCIA-AUTO` | **Não** | CP baixa (formas) + Mini DRE; **não** mexe checkout |
| `DRE-NO-DASH-OVERLAY` | **Não** | Só Gestão / abas do BI |

## Depois do deploy (você)

1. **Ctrl+F5** · badge **v23.38**
2. Zap: reinstalar app se ícone antigo
3. CP: baixa → só **BANCO / DINHEIRO**
4. Gestão: F8 / card Lucro → **1 aba** DRE (Dashboard fica Dashboard)
5. Resumo: Extravio = depósito − BANCO (tooltip)
