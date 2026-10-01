# Rollback — Cliente novo: limite fiado inicial 0,01 (`CLIENTE-NOVO-LIMITE-001`)

Ponto **antes** deste pacote = loja **Live v25.86** (commit de etiquetas laser; ver `banana-roteiro.md` §44).

| Item | Valor |
| ---- | ----- |
| **O quê sobe** | Default `ClienteAgro.limite_fiado_local` = **0,01**; form/PDV rápido usam `fiado_limite_inicial_novo_cliente()`; migrate **0136** (só altera default do campo, não reescreve fichas antigas). |
| **O quê NÃO muda** | Clientes já cadastrados com **0** no limite local continuam com comportamento **padrão R$ 5.000** no PDV. |
| **Migrate rollback** | Reverter código e rodar migrate anterior **ou** redeploy sem 0136 — em produção, após rollback de código, o default volta a **0** no model antigo. |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-cliente-limite-inicial-v25.86
git push origin producao --force-with-lease
```

(Criar tag `rollback/pre-cliente-limite-inicial-v25.86` no commit Live **antes** do deploy deste pacote.)

**Só** com frase + senha do Renan.

## Prova

`python scripts/verify_cliente_fonte_unica_path.py` — mensagens **cliente novo nasce com limite 0,01** e **PDV enxerga 0,01 (não 5000)**.
