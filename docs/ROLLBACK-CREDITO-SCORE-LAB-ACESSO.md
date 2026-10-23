# Rollback — CREDITO-SCORE-LAB-ACESSO (botão + fix PDV · v26.12)

Volta a loja ao estado **antes** deste pacote (Live **v26.07** do laboratório shadow).

| Item | Valor |
| ---- | ----- |
| **Antes (loja)** | Live **v26.07** · laboratório no código · flag env já ligada |
| **Tag segurança** | `rollback/pre-credito-score-lab-acesso-v26.07` *(criar no deploy)* |
| **Branch PREP** | a criar no deploy · cherry só destes commits |
| **Migrate** | **NÃO** |
| **Merge `teste`?** | **NÃO** — `teste` tem META e outros pacotes |

## Pacote

| # | Pacote | Risco loja aberta |
| - | ------ | ----------------- |
| 1 | Botão **Análise de crédito** no menu Gestão | Baixo — só aparece com flag+allowlist/superuser |
| 2 | Fix: URL do lab **não** é puxada de volta ao PDV | Baixo — JS dual-window + shell |

**Não mexe:** limite, venda, baixa, caixa, estoque, PDV operacional.

## Provas (PIN 9973)

| Prova | Resultado |
| ----- | --------- |
| CREDITO-SCORE-LAB-ACESSO | **34/34** |
| shadow path (regressão) | **93/93** |
| PDV fiado refresh / card | **39/39** |
| PREP_FAILS | **0** |
