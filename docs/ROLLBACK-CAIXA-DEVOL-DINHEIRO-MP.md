# Rollback — devolução em dinheiro × MP (`CAIXA-DEVOL-DINHEIRO-MP` · v17.84)

Ponto **antes** deste pacote = loja **Live v17.83** (overlay Pesar limpo + script driver da balança).

| Item | Valor |
| ---- | ----- |
| **Commit** | `8bb72875` (Live v17.83 + `scripts/balanca-windows`) |
| **Tag** | `rollback/pre-caixa-devol-dinheiro-mp-v17.83` |
| **Branch backup** | `producao-backup-pre-v1784-caixa-devol-dinheiro-mp-20260823` |
| **O quê reverte** | Fechar caixa: esperado do Point/cartão/Pix com devolução em dinheiro · auto copia esperado · aviso amarelo da gaveta |
| **O quê NÃO reverte** | overlay Pesar v17.83 · parser kg ao vivo · NFC-e dest CNPJ |
| **Migrate** | **NÃO** — reverter código é seguro (sem coluna nova) |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-caixa-devol-dinheiro-mp-v17.83
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v17.83**. Badge da loja volta a **v17.83**.
