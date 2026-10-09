# PREP deploy — PDV-VALOR-RS-KG · alvo **v26.77**

**Status:** 🟢 **pronto para envio à produção**  
**Não sobe sozinho.** Só com frase explícita + senha `99738595` na mesma mensagem.

## Conteúdo

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **PDV-VALOR-RS-MODAL** (só **UNIDADE=KG**) | path **46/46** · PIN 9973 | **NÃO** |

**O quê:** modal «Valor em R$?» após Enter **somente** se unidade = KG. UN/PC/etc. lançam direto.

**Tip PREP:** `deploy/prep-pdv-valor-rs-modal` · base Live **v26.76** @ `52da69c2`

## Na senha

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-pdv-valor-rs-modal
git push origin producao
```

Smoke: Ctrl+F5 · **v26.77** · produto KG Enter → pergunta · produto UN Enter → sem pergunta.

## Voltar (só frase + senha)

Ver `docs/ROLLBACK-PDV-VALOR-RS-MODAL.md` (ajustar tag para tip Live v26.76 se necessário).
