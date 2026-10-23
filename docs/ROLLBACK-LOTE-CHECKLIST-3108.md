# Rollback — lote checklist 3108 (loja alvo **v20.45**)

Ponto **antes** deste lote = loja **Live v20.22** (`75779df`).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `75779df` (Live v20.22 · PIN na ação) |
| **Tag (criada no PREP)** | `rollback/pre-lote-checklist-3108-v20.22` |
| **Branch backup (criada no PREP)** | `producao-backup-pre-v2045-lote-checklist-20260831` |
| **Branch PREP** | `deploy/prep-checklist-3108` · tip **v20.45** |
| **O quê sobe** | 5 itens do CHECKLIST ÚNICO 31/08 (ver banana) |
| **O quê NÃO sobe** | `WA-ATEND-QR` · `BI-META-C-VILA-RAMP` |
| **O quê NÃO reverte** | PIN na ação v20.22 e lotes anteriores |
| **Migrate** | **SIM** — `0106` (fundo troco) · `0107` (cliques topbar) · `0110` (layout topbar). **Nota PREP:** `0110` depende de `0107` (pula `0108`/`0109` WhatsApp). Reverter código não exige unmigrate. |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-lote-checklist-3108-v20.22
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v20.22**.

## Risco operacional (lojas abertas)

| Pacote | Afeta venda PDV? | Nota |
| ------ | ---------------- | ---- |
| `PDV-TOPBAR-LAYOUT` | **Não** (finalizar) | Topbar Organizar · migrate `0110` · **Ctrl+F5** |
| `PDV-TOPBAR-MAIS` | **Não** | Botão Mais ⋯ · migrate `0107` |
| `PDV-WA-TOPBAR-BREVE` | **Não** | Só «Em breve…» — **não** abre atendimento WA |
| `PDV-PIN-CHAT-TEMPEDIDO` | **Não** | Só evita popup «tem pedido» ao renovar PIN no chat |
| `REPASSE-FUNDO-TROCO` | **Não** (venda) | Só Repasse Vila · migrate `0106` · aviso R$ 500 |

Rotina no deploy: pausar vendas ~2–3 min · Zap · Ctrl+F5 nos PDVs · smoke: venda · Mais/Organizar · WhatsApp «Em breve» · Repasse · chat+PIN.

## Deploy (próximo chat + senha)

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-checklist-3108
git push origin producao --force-with-lease
```

Render sobe · migrate `0106`+`0107`+`0110` · conferir healthz / badge **v20.45**.
