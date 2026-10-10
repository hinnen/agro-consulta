# Rollback — Checklist 14/09 (loja alvo **v25.23**)

Ponto **antes** deste lote = loja **Live v25.22** (`d747d63` / REL-MV-LOJA).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `d747d63` (Live v25.22) |
| **Tag** | `rollback/pre-checklist-1409-v25.22` |
| **Branch backup** | `producao-backup-pre-v2523-checklist-20260914` |
| **O quê sobe** | bugs **#18–#22, #24–#26** · Entregas modal · Fechar CTA · Orçamento Imprimir |
| **O quê NÃO sobe** | merge do `teste` · bug **#23** (já Live) |
| **Migrate** | **NÃO** |
| **Mantém** | REL-MV-LOJA (v25.22) — não reverte se usar esta tag |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-checklist-1409-v25.22
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v25.22**.

## Provas pré-envio

Path lote OK · pin **79/79** · fechar **60/60** · PIN **9973** · sem migrate.

## Depois do deploy

1. **Ctrl+F5** · badge **v25.23**
2. Smoke: 1 venda Dinheiro · próxima pede PIN · Entregas no Pagamento · Trocar c/ quitado
