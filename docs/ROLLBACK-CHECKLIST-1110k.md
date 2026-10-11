# Rollback — CHECKLIST 11/10k (restante) · v27.03

**Voltar para:** Live **v27.01** @ tip `producao` antes do push (tag `rollback/pre-checklist-1110k-restante-v27.01`)  
**Backup branch:** `producao-backup-pre-v2703-checklist-1110k-restante`

## Quando

Só com frase + senha do Renan (mesmo ritual do cutover).

## Passos (resumo)

1. Pausar vendas (Zap).
2. `git push origin rollback/pre-checklist-1110k-restante-v27.01:producao`
3. Render redeploy · **sem** rollback de migration (este pacote não leva migrate).

## O que some

- Nome PWA **SistApp** / card instalar atualizado · hub **Ajuste** · botão **Atualizar** no painel Vendas.
- Texto Zap enxuto nas metas (volta formato anterior do v27.01).
