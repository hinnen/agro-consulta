# Rollback — lote vendas + BI (`prep-lote-vendas-bi-0109d` · alvo **v20.85**)

Ponto **antes** deste lote = loja **Live v20.58** (`751c0d4`).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `751c0d4` (Live v20.58) |
| **Tag rollback** | `rollback/pre-lote-vendas-bi-0109d-v20.58` |
| **Branch backup** | `producao-backup-pre-v2059-vendas-bi-0109d` |
| **Branch PREP** | `deploy/prep-lote-vendas-bi-0109d` |
| **O quê sobe** | `BI-DEVOL-CARD` · `BI-DEVOL-MEIO` · `VL-FIADO-TAGS` · `VL-CAL-INTERVALO` |
| **O quê NÃO sobe** | WhatsApp (`WA-ATEND-QR` e derivados) · resto do `teste` |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-lote-vendas-bi-0109d-v20.58
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v20.58**.

## Risco operacional (lojas abertas)

| Pacote | Afeta venda PDV? | Nota |
| ------ | ---------------- | ---- |
| `BI-DEVOL-CARD` | **Não** | Só números do BI `/` |
| `BI-DEVOL-MEIO` | **Não** | Atalhos + ranking BI |
| `VL-FIADO-TAGS` | **Não** | Só `/vendas/lojas/` (leitura) |
| `VL-CAL-INTERVALO` | **Não** | Só calendário da mesma tela |

Carrinho, caixa, NFC-e, entrega **inalterados**.

## Deploy (próximo chat + senha)

**Não** resetar `producao` para `teste` inteiro (lá tem WhatsApp).

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-lote-vendas-bi-0109d
git push origin producao --force-with-lease
```

Render sobe · healthz · badge **v20.85** · Ctrl+F5.

## Smoke pós-deploy

1. `/` — card hoje bate com `/vendas/lojas/`
2. `/vendas/lojas/` — tag fiado · calendário 2 toques
3. `/atalhos/` — vendas hoje
4. PDV — F7 uma venda (nada quebrou)
