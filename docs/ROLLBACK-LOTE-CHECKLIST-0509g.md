# Rollback — lote checklist 0509g (loja alvo **v21.91**)

Ponto **antes** deste lote = loja **Live v21.90** (`aaff41d` · RH-PIN-GESTAO).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `aaff41d` (Live v21.90) |
| **Tag (criada no PREP)** | `rollback/pre-lote-checklist-0509g-v21.90` |
| **Branch backup (criada no PREP)** | `producao-backup-pre-v2191-lote-checklist-20260905` |
| **Branch PREP** | `deploy/prep-checklist-0509g` · tip **v21.91** |
| **O quê sobe** | `#12` entrega tabela · `REPASSE-ZERO-OK` · `#15` vale live · `#11` Point PIN · `#16` vale usado · `#14` orçamento lista · `WA-LISTA-SEM-PISCA` · `WA-FACHONA-PRETA` · `WA-PIN-COMPOSER` · `WA-SAUDACAO-RICH`/`WA-ARQUIVO` |
| **O quê NÃO sobe** | merge do `teste` · Excel cadastro · `WA-PONTE-LEVE` · resto do `teste` |
| **O quê NÃO reverte** | Live v21.90 (RH-PIN) e anteriores |
| **Migrate** | **SIM** `produtos.0126` (arquivar conversa Zap — campos novos, default false) |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-lote-checklist-0509g-v21.90
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v21.90**.  
A migration `0126` já aplicada **não some sozinha** — preferir deixar no banco.

## Risco operacional (lojas abertas)

| Pacote | Afeta finalizar venda? | Nota |
| ------ | ---------------------- | ---- |
| `PDV-ENTREGA-TABELA-FORMA` | **Entrega** | Só etapa Entrega aplica tabela da forma. Balcão F7 **igual**. |
| `REPASSE-ZERO-OK` | **Não** (venda) | Só overlay Repasse: 0,00 ok. |
| `PDV-VALE-SALDO-LIVE` | **Contador** | Número Vale crédito atualiza na hora. F7 igual. |
| `MP-POINT-FINAL-PIN` | **Point** | Depois de cobrar, grava mesmo com PIN morto + retry. Sem Point = igual. |
| `PDV-VALE-USADO` | **Pagar com vale** | Saldo desce de verdade. Sem vale = igual. |
| `PDV-ORC-LISTA-LIVE` | **Não** (cobrança) | Só card ORÇAMENTOS após salvar. |
| `WA-*` | **Não** (cobrança) | Ponte Zap está **off** de propósito. Código fica pronto; não liga a ponte. |

Rotina no deploy: **pausar vendas ~2 min** · aviso Zap · Ctrl+F5 · smoke abaixo.

## Deploy (próximo chat + senha)

**Não** resetar `producao` para o branch `teste`.

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-checklist-0509g
git push origin producao --force-with-lease
```

Render sobe · conferir healthz / badge **v21.91** · migrate `0126`.

**Smoke:** Ctrl+F5 · badge **v21.91** · PDV F7 · entrega+tabela se usar · Point se usar · vale se usar · Repasse 0,00 · orçamento aparece no card. **Não** ligar o `.bat` do Zap neste deploy (lento).
