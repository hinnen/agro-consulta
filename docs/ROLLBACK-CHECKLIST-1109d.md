# Rollback — Checklist 11/09d (loja alvo **v23.94**)

Ponto **antes** deste lote = loja **Live v23.93** (`fc34325` / checklist 11/09b pct-zero).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `fc34325` (Live v23.93) |
| **Tag** | `rollback/pre-checklist-1109d-v23.93` |
| **Branch backup** | `producao-backup-pre-v2394-checklist-20260911d` |
| **Branch PREP** | `deploy/prep-checklist-1109d` · tip **v23.94** |
| **O quê sobe** | `NF-LOTE-XML` · `REPASSE-HIST-OVERLAY` · `REPASSE-STATUS-FLASH` |
| **O quê NÃO sobe** | merge do `teste` · WhatsApp · Excel · DRE WIP · outros |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-checklist-1109d-v23.93
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v23.93**.

## Risco operacional (lojas abertas)

| Pacote | Afeta finalizar venda / PDV? | Nota |
| ------ | ---------------------------- | ---- |
| `NF-LOTE-XML` | **Não** (venda) | Só Entrada NF etapa 4 (lote/validade do XML). Sem migrate. |
| `REPASSE-HIST-OVERLAY` | **Não** (venda) | Só overlay Repasse · botão Histórico + API `envios/`. |
| `REPASSE-STATUS-FLASH` | **Não** (venda) | Só UI status TRANSFERINDO / avisos no Repasse. |

**Piora venda/caixa?** Não esperado — paths isolados (Entrada NF + Repasse Vila). Envelope / % lucro Live **v23.93** permanece.

## Cherry (11 commits de código · ordem)

1. `19100f5` `a02c4ff` — NF-LOTE-XML  
2. `2e17a59` … `7aabe52` (7) — REPASSE-HIST-OVERLAY  
3. `86213f6` `8b7ec87` — REPASSE-STATUS-FLASH  

## Provas (pré-envio · 11/09d)

| Pacote | Prova |
| ------ | ----- |
| `NF-LOTE-XML` | path **48/48** · PIN **9973** |
| `REPASSE-HIST-OVERLAY` | hist **19/19** · vila **280** |
| `REPASSE-STATUS-FLASH` | status-flash **43/43** · PIN **9973** |
| Regressão | pct-zero **20/20** · stack nest **34+** · `check` OK · node OK |

Arquivos no PREP vs `teste` (código dos 3 pacotes): **SAME** (hash) em util/JS/overlay/views/urls/entrada.

## Depois do deploy (você)

1. **Ctrl+F5** · badge **v23.94**
2. Entrada NF → Ler XML → etapa 4 mostra lote/validade
3. PDV → Repasse → **Histórico** (3 botões) · Imprimir
4. Confirmar repasse → **TRANSFERINDO…** no centro da tela
5. Venda normal / caixa: sem mudança esperada
