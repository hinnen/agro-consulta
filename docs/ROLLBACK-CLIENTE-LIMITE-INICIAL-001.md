# Rollback — Cliente novo: limite fiado inicial 0,01 (`CLIENTE-NOVO-LIMITE-001`)

No lote 03/10b, ponto **antes** = loja **Live v25.99**. Rollback do lote: `docs/ROLLBACK-CHECKLIST-0310b.md`.

| Item | Valor |
| ---- | ----- |
| **O quê sobe** | Default `ClienteAgro.limite_fiado_local` = **0,01**; form/PDV rápido usam `fiado_limite_inicial_novo_cliente()`; migrate **0136** (só altera default do campo, não reescreve fichas antigas). |
| **O quê NÃO muda** | Clientes já cadastrados com **0** no limite local continuam com comportamento **padrão R$ 5.000** no PDV. |
| **Migrate rollback** | Reverter código e rodar migrate anterior **ou** redeploy sem 0136 — em produção, após rollback de código, o default volta a **0** no model antigo. |

```bash
git fetch origin tag rollback/pre-checklist-0310b-v25.99
git checkout producao
git reset --hard rollback/pre-checklist-0310b-v25.99
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan.

## Prova

`python scripts/verify_cliente_fonte_unica_path.py` — mensagens **cliente novo nasce com limite 0,01** e **PDV enxerga 0,01 (não 5000)**.
