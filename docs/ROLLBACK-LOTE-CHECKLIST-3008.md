# Rollback — lote checklist 3008 (loja alvo **v19.83**)

Ponto **antes** deste lote = loja **Live v19.63** (`71eea32` · PDV-CHAT-OPEN).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `71eea32` (Live v19.63) |
| **Tag (criada no PREP)** | `rollback/pre-lote-checklist-3008-v19.63` |
| **Branch backup (criada no PREP)** | `producao-backup-pre-v1983-lote-checklist-20260830` |
| **Branch PREP** | `deploy/prep-checklist-3008` · tip **v19.83** |
| **O quê sobe** | Só os 4 do CHECKLIST ÚNICO tip v19.83 (abaixo) |
| **O quê NÃO sobe** | `BI-META-C-VILA-RAMP` (continua SOLO / fora deste PREP) · outros WIP do `teste` |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-lote-checklist-3008-v19.63
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v19.63**.

## Pacotes neste PREP

| # | Pacote | Afeta venda PDV? | Nota |
| - | ------ | ---------------- | ---- |
| 1 | `PDV-TRANSF-FORCADA` | **Não** (finalizar) | Só Pedir loja → escolha / forçada |
| 2 | `PDV-ENTER-SEM-IMP` | **Sim (Enter)** | Enter = sempre **sem** cupom · F9 = com |
| 3 | `CAIXA-DEVOL-MP-MESMA` | **Não** (venda) | Só Fechar caixa / conferência |
| 4 | `NFCE-REEMIT-TIMEOUT` | **Não** (balcão) | Só Vendas → Reemitir NFC-e |

## Provas (PREP / tip)

| Pacote | Prova |
| ------ | ----- |
| `PDV-TRANSF-FORCADA` | **88/88** + Pedir loja **68/68** + Logística forçada OK |
| `PDV-ENTER-SEM-IMP` | **41/41** |
| `CAIXA-DEVOL-MP-MESMA` | **171/171** |
| `NFCE-REEMIT-TIMEOUT` | **38/38** (+ DESC **57/57**) |

## Rotina no deploy (lojas abertas)

1. Pedir pausa de vendas ~2–3 min (Zap).
2. Deploy PREP → Render.
3. **Ctrl+F5** nos PDVs (Centro + Vila).
4. Smoke rápido:
   - Venda **Dinheiro → Enter** = **sem** cupom · **F9** = com
   - Pedir loja → Pedir · Forçada (direção Enter) sem abrir Gestão
   - Fechar caixa: devolução Pix MP não cai nas manuais (se der testar)
   - Vendas → Reemitir: loading ≤~30s ou aviso claro
5. Badge home **v19.83**.

## Deploy (próximo chat + senha)

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-checklist-3008
git push origin producao --force-with-lease
```

Render sobe · **sem migrate** · conferir healthz / badge **v19.83**.
