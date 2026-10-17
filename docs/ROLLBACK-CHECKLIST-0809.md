# Rollback — Checklist 08/09 (loja alvo **v23.58**)

Ponto **antes** deste lote = loja **Live v23.45** (`1b942c4` / checklist 07/09b).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `1b942c4` (Live v23.45) |
| **Tag (criar no deploy)** | `rollback/pre-checklist-0809-v23.45` |
| **Branch backup (criar no deploy)** | `producao-backup-pre-v2358-checklist-20260908` |
| **Branch PREP** | `deploy/prep-checklist-0809` · tip **v23.58** |
| **O quê sobe** | `PIN-ALERT-TECLADO` + `REL-QUEM-COMPROU` + `ETQ-COLAR-MV` |
| **O quê NÃO sobe** | merge do `teste` · WhatsApp extra · Excel cadastro · outros WIP |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-checklist-0809-v23.45
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v23.45**.

## Risco operacional (lojas abertas)

| Pacote | Afeta finalizar venda / PDV? | Nota |
| ------ | ---------------------------- | ---- |
| `PIN-ALERT-TECLADO` | **Baixo** (melhora) | Alert «modo descanso» → teclado PIN; não muda regra de venda |
| `REL-QUEM-COMPROU` | **Não** | Relatório novo em `/relatorios/quem-comprou/` |
| `ETQ-COLAR-MV` | **Não** | Só `/produtos/etiquetas/` (colar GM + ranking) |

## Provas (pré-envio)

| Pacote | Prova |
| ------ | ----- |
| `PIN-ALERT-TECLADO` | `verify_pin_alert_teclado_path.py` **117/117** · LANC **70/70** · PIN 9973=Renan |
| `REL-QUEM-COMPROU` | `verify_rel_quem_comprou_path.py` **67/67** |
| `ETQ-COLAR-MV` | `verify_etq_colar_mv_path.py` **82/82** |
| Django | `manage.py check` OK |

## Depois do deploy (você)

1. **Ctrl+F5** · badge **v23.58**
2. PIN velho em gestão/PDV/lançamentos → **teclado** (sem alert preto)
3. Relatórios → **Quem já comprou** · produto · Zap
4. Etiquetas → **Colar códigos** ou **Carregar ranking** → Adicionar todos
