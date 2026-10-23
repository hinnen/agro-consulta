# Rollback — lote checklist 0309 (loja alvo **v21.83**)

Ponto **antes** deste lote = loja **Live v21.82** (`527be62` · WhatsApp menu/gestão).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `527be62` (Live v21.82) |
| **Tag (criada no PREP)** | `rollback/pre-lote-checklist-0309-v21.82` |
| **Branch backup (criada no PREP)** | `producao-backup-pre-v2183-lote-checklist-20260903` |
| **Branch PREP** | `deploy/prep-checklist-0309` · tip **v21.83** |
| **O quê sobe** | `PIN-VENDA-10S` · `FIADO-VER-RECIBOS` · `PDV-OVERLAY-STACK` · `VENDAS-LISTA-UX` · `F8-HIST-VENDAS` · `CAIXA-FIADO-CONF` |
| **O quê NÃO sobe** | WhatsApp extra do `teste` · `CLI-FORM-PDV-LAYOUT` · `CAD-FALLBACK-HIST` · resto do `teste` |
| **O quê NÃO reverte** | WhatsApp Live v21.82 (PDV continua **Em breve**) e lotes anteriores |
| **Migrate** | **SIM** `produtos.0123` (campos nullable — conferência fiado no Fechar caixa) |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-lote-checklist-0309-v21.82
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v21.82**.  
A migration `0123` já aplicada **não some sozinha** — os campos ficam ociosos; preferir deixar no banco.

## Risco operacional (lojas abertas)

| Pacote | Afeta finalizar venda? | Nota |
| ------ | ---------------------- | ---- |
| `PIN-VENDA-10S` | **Janela PIN** | Depois do PIN, fechar venda em **10s**. Pedir loja / chat continuam **45s**. Preço, estoque, forma e NFC-e **iguais**. |
| `PDV-OVERLAY-STACK` | **Esc / Fechar** | 2ª camada: Esc/Fechar da de baixo não fecha tudo. Carrinho e F7 iguais. |
| `FIADO-VER-RECIBOS` | **Não** | Só tela `/fiado/` (Ver pedido em cima, recibos). |
| `VENDAS-LISTA-UX` | **Não** | Só `/vendas/` (lista + busca). |
| `F8-HIST-VENDAS` | **Não** | Só aba Histórico do F8 (sem cards). |
| `CAIXA-FIADO-CONF` | **Não** (venda) | Fechar caixa: Confirmar na caixinha fiado **grava** e não pede de novo. |

Rotina no deploy: **pausar vendas ~2 min** · aviso Zap · Ctrl+F5 · smoke abaixo.

## Deploy (próximo chat + senha)

**Não** resetar `producao` para o branch `teste` (lá tem WhatsApp extra e cadastro Excel **fora** deste lote).

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-checklist-0309
git push origin producao --force-with-lease
```

Render sobe · conferir healthz / badge **v21.83** · migrate `0123`.

**Smoke:** Ctrl+F5 · badge **v21.83** · PIN + F7 uma venda (10s) · F8 Histórico · overlay Vendas (Esc volta 1 nível) · Fiado Ver · Fechar caixa fiado Confirmar.
