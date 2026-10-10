# Rollback — PDV-FIADO-LIMITE-REFRESH (limite fiado sem reabrir PDV · v26.05)

Volta a loja ao estado **antes** deste pacote (Live **v26.02**).

| Item | Valor |
| ---- | ----- |
| **Antes (loja)** | `producao` @ `f7ef7844` · VERSION **26.02** |
| **Tag segurança** | `rollback/pre-pdv-fiado-limite-refresh-v26.02` |
| **Branch backup** | `producao-backup-pre-v2605-pdv-fiado-limite-refresh-20261003` |
| **Branch PREP** | `deploy/prep-pdv-fiado-limite-refresh` · alvo loja **v26.05** |
| **Migrate** | **NÃO** |
| **Merge `teste`?** | **NÃO** — só cherry deste PREP |

## Pacote

| # | Pacote | Risco loja aberta |
| - | ------ | ----------------- |
| 1 | **PDV-FIADO-LIMITE-REFRESH** | Baixo — só JS do PDV (`pdv_wizard.js`): refresh do crédito fiado. **Não** mexe caixa, Point, NFC-e, estoque, financeiro, migrate. |

**O quê muda:** ao mudar o limite do cliente, o PDV consulta de novo o crédito (escolher Fiado / lançar / confirmar / voltar o foco) — sem fechar e abrir a tela.

## Como voltar (só frase + senha)

```bash
git fetch origin tag rollback/pre-pdv-fiado-limite-refresh-v26.02
git checkout producao
git reset --hard rollback/pre-pdv-fiado-limite-refresh-v26.02
git push origin producao --force-with-lease
```

## Provas (PREP · PIN 9973)

| Prova | Resultado |
| ----- | --------- |
| PDV-FIADO-LIMITE-REFRESH path | **39/39** |
| Regressão card limite fiado | **39/39** |
| PREP_FAILS | **0** |
