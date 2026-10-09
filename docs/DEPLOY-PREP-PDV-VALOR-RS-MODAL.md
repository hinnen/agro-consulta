# PREP deploy — PDV-VALOR-RS-MODAL · alvo **v26.76**

**Status:** 🟢 **pronto para envio à produção**  
**Não sobe sozinho.** Só com frase explícita + senha `99738595` na mesma mensagem.

## Conteúdo

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **PDV-VALOR-RS-MODAL** | path **39/39** · PIN 9973 · qty 1,064 | **NÃO** |

**O quê:** após Enter/clique no produto no wizard → «Valor em R$?» · Enter vazio/Esc/Pular = qty normal · valor = qty÷preço.

**Tip PREP:** `deploy/prep-pdv-valor-rs-modal` · base Live **v26.74**  
**Não inclui** ETQ-EAN-LOJA-DV (esse fica no `teste` / checklist 09/10d separado).

## Na senha

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-pdv-valor-rs-modal
git push origin producao
```

Smoke: Ctrl+F5 `/pdv/checkout/` · badge **v26.76** · busca produto Enter → `10` ou Enter vazio.

## Voltar (só frase + senha)

Ver `docs/ROLLBACK-PDV-VALOR-RS-MODAL.md`.
