# Deploy rápido — Checklist 05/10 (PREP v26.08)

**Só** com frase explícita + senha `99738595` na **mesma** mensagem.

## Já pronto (não repetir na hora)

| Item | Valor |
| ---- | ----- |
| Branch | `deploy/prep-checklist-0510` @ `8b636b67` |
| Base Live | v26.07 @ `93189ebb` |
| Tag rollback | `rollback/pre-checklist-0510-v26.07` |
| Alvo | **v26.08** |
| Migrate | `produtos.0138` (META) |
| Pacotes | META · LAB-ACESSO · XLSX · PEDIR-PARCIAL-PRINT |

## Na autorização (ordem curta)

1. Aviso Zap: pausar finalizar venda ~2–3 min.
2. Backup opcional: `git branch producao-backup-pre-v2608-checklist-0510 origin/producao`
3. Atualizar loja:
   ```bash
   git fetch origin
   git checkout producao
   git reset --hard origin/deploy/prep-checklist-0510
   git push origin producao --force-with-lease
   ```
4. Acompanhar Render **SistVale** até Live (migrate `0138` no build).
5. Ctrl+F5 nos PDVs · smoke: Pedir loja · Menu META · Análise de crédito (autorizado).
6. No banana: badges → ✅ enviado / Live v26.08 · limpar «aguarda senha».

## Se precisar voltar

```bash
git checkout producao
git reset --hard rollback/pre-checklist-0510-v26.07
git push origin producao --force-with-lease
```

**Só** com frase + senha de novo.
