# Rollback — CHECKLIST 11/10k · v27.03

**Voltar para:** Live **v27.00** @ `14f5cad7`  
**Tag:** `rollback/pre-checklist-1110k-v27.00`  
**Backup branch:** `producao-backup-pre-v2703-checklist-1110k`

## Quando

Só com frase + senha do Renan (mesmo ritual do cutover).

## Passos (resumo)

1. Pausar vendas (Zap).
2. `git push origin rollback/pre-checklist-1110k-v27.00:producao` (ou reset na máquina de deploy conforme ritual habitual).
3. Render redeploy · **não** reverter migration 0144 automaticamente (faixas `deposito` ficam no PG; código antigo ignora coluna extra — se precisar rollback DB, avaliar com calma).

## O que some

- Ajuste Mobile no hub SistApp sem PIN extra.
- META por loja + texto Zap enxuto (volta meta única do v27.00).
