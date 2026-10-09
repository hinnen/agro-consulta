# PREP deploy — PDV-VALOR-RS-MODAL · alvo **v26.77**

**Status:** 🟢 **pronto para envio à produção**  
**Não sobe sozinho.** Só com frase explícita + senha `99738595` na mesma mensagem.

## Conteúdo

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **PDV-VALOR-RS-MODAL** | path **46/46** · PIN 9973 · só UNIDADE=KG | **NÃO** |

**O quê:** após Enter/clique em produto **UNIDADE=KG** → «Valor em R$?» · demais unidades sem pergunta · Enter vazio/Esc/Pular = qty normal.

**Tip PREP:** `deploy/prep-pdv-valor-rs-modal` · base Live **v26.74**  
**Não inclui** ETQ-EAN-LOJA-DV (esse fica no `teste` / checklist 09/10d separado).

## Na senha

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-pdv-valor-rs-modal
git push origin producao
```

Smoke: Ctrl+F5 `/pdv/checkout/` · badge **v26.77** · produto KG Enter → `10` ou Enter vazio · produto UN não pergunta.

## Voltar (só frase + senha)

Ver `docs/ROLLBACK-PDV-VALOR-RS-MODAL.md`.
