# Rollback — VL-PREV-MES (loja alvo **v23.06**)

Ponto **antes** deste pacote = loja **Live v21.93** (`8884c9c` · lote checklist 0509h).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `8884c9c` (Live v21.93) |
| **Tag** | `rollback/pre-vl-prev-mes-v21.93` |
| **Branch backup** | `producao-backup-pre-v2306-vl-prev-mes-20260906` |
| **Branch PREP** | `deploy/prep-vl-prev-mes-0609` · tip **v23.06** |
| **O quê sobe** | `VL-PREV-MES` (previsão mês + aviso + fonte + async extras) |
| **O quê NÃO sobe** | merge do `teste` · `WA-PONTE-LEVE` · resto do `teste` |
| **O quê NÃO reverte** | Live v21.93 e anteriores |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-vl-prev-mes-v21.93
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v21.93**.

## Risco operacional (lojas abertas)

| Pacote | Afeta finalizar venda? | Nota |
| ------ | ---------------------- | ---- |
| `VL-PREV-MES` | **Não** | Só `/vendas/lojas/` + API extras; PDV/caixa/NFC-e **intocados** |

Rotina no deploy: **Ctrl+F5** · smoke abaixo.

## Provas

| Prova | Resultado |
| ----- | --------- |
| `scripts/verify_vendas_lojas_resumo_path.py` | **179/179** |
| PIN **9973** · HTTP página + extras | **200** |
| Soma Centro+Vila = total · ritmo | OK |

## Smoke pós-deploy

- [ ] `/healthz` ok
- [ ] badge home **v23.06**
- [ ] `/vendas/lojas/` · totais na hora · previsão completa em seguida
- [ ] Ctrl+F5
