# Rollback — MP-POINT-POLL-RETRY (loja alvo **v23.73**)

Ponto **antes** deste pacote = loja **Live v23.70** (`71a169a` / checklist 09/09).

| Item | Valor |
| ---- | ----- |
| **Commit loja antes** | `71a169a` (Live v23.70) |
| **Tag** | `rollback/pre-mp-point-poll-retry-v23.70` |
| **Branch backup** | `producao-backup-pre-v2373-mp-point-poll-retry-20260909` |
| **O quê sobe** | `MP-POINT-POLL-RETRY` (poll Point não desiste no 502) |
| **O quê NÃO sobe** | merge do `teste` · repasse hotfix · WhatsApp · Excel · outros |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-mp-point-poll-retry-v23.70
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v23.70**.

## Risco operacional

| Pacote | Afeta finalizar venda / PDV? | Nota |
| ------ | ---------------------------- | ---- |
| `MP-POINT-POLL-RETRY` | **Médio** (só espera Point) | 502/rede continua aguardando; cancel/recusa iguais |

## Provas (pré-envio)

| Item | Resultado |
| ---- | --------- |
| `verify_mp_point_poll_retry_path.py` | **13/13** |
| `verify_mp_point_final_pin_path.py` | **41/41** · PIN **9973**=Renan |
| `tests_mp_point_pin_forcar` | **16/16 OK** |
| Lógica 502/403/pago | **10/10** |
| `manage.py check` | OK |
| Static local `pdv_wizard.js` | código novo servido |

## Depois do deploy (você)

1. **Ctrl+F5** PDV · badge **v23.73**
2. Point automático: se a rede oscilar, a tela deve **continuar** «aguardando» (não some o pagamento)
