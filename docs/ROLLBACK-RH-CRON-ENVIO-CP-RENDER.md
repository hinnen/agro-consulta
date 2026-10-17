# Rollback — cron RH envio CP sem exit 1 falso (`RH-CRON-ENVIO-RENDER`)

Ponto **antes** deste pacote = loja **Live v25.82** (`d342e2a5`). Cron **`agro-rh-envio-cp-automatico`** continua existindo; só muda o tratamento de erro.

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `d342e2a5` (Live v25.82) |
| **Tag** | `rollback/pre-rh-cron-envio-cp-render-v25.82` |
| **Branch backup** | `producao-backup-pre-v2582-rh-cron-envio-20261001` |
| **O quê sobe** | Cron `rh_envio_cp_automatico`: salário R$ 0 / sem faixa → `pulados_salario_zero` + log; **não** exit 1 no Render. Erros reais ainda aparecem no log. |
| **O quê NÃO sobe** | merge do `teste` · PDV · caixa · venda |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-rh-cron-envio-cp-render-v25.82
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v25.82**.

## Prova (antes do envio)

`python scripts/verify_rh_envio_cp_automatico_path.py` — **20/20** (VERIFY_OK).
