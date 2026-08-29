# Rollback — lote checklist 28/08c (v18.72)

**Só com frase explícita + senha `99738595` na mesma mensagem.**

## O que este lote sobe

| # | Pacote | Risco PDV venda |
| - | ------ | --------------- |
| 1 | `NS-ESCOLHA-EMP` | **Baixo** — só Nova saída / BI (não Finalizar venda) |
| 2 | `REPASSE-PDV-OVERLAY-POPUP` | **Baixo** — só overlay Repasse |
| 3 | `CP-EMP-PG-FALLBACK` | **Baixo** — só Registrar empréstimo (melhora se Mongo off) |

**Migrate:** **NÃO**

## Antes do deploy (loja aberta)

1. Zap: *«Atualização ~2 min — não finalize venda agora.»*
2. Janela mais calma se possível.
3. No chat: frase + senha.

## Rollback (emergência)

```bash
git push origin 5e6e44a:producao
# ou
git push origin rollback/pre-lote-checklist-2808c-v18.64:producao
```

- Tag: `rollback/pre-lote-checklist-2808c-v18.64` @ `5e6e44a`
- Branch backup: `producao-backup-pre-v1872-lote-checklist-20260828`
- PREP: `deploy/prep-checklist-2808c`

## Smoke pós-deploy

1. Ctrl+F5 · badge **v18.72**
2. Venda R$ 0,01
3. BI Nova saída → escolha
4. Empréstimo Interno → Registrar
5. PDV Repasse → popup quem/PIN

## Provas

NS+EMP 30/30 · CP 61/61 · CP-NE 18/18 · overlay 93/93 · check OK
