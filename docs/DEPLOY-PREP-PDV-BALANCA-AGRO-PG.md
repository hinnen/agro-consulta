# PREP deploy — PDV-BALANCA-AGRO-PG (`deploy/prep-pdv-balanca-agro-pg` · alvo **v26.71**)

**Não sobe sozinho.** Só com frase explícita + senha `99738595` na mesma mensagem.

## Conteúdo (1 pacote)

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **PDV-BALANCA-AGRO-PG** | path **49/49** · etq **39/39** · unit **9/9** | **NÃO** |

**O quê:** sob `agro_pg`, bip/colar EAN `2001000004812` (PLU `0010` · R$ 4,81) resolve produto + preço da etiqueta.  
**Base Live:** **v26.70** @ `f290c371`  
**Branch PREP:** `deploy/prep-pdv-balanca-agro-pg`  
**Rollback:** tag `rollback/pre-pdv-balanca-agro-pg-v26.70` · `docs/ROLLBACK-PDV-BALANCA-AGRO-PG.md`

**Não mexe:** caixa, finalizar venda, Point, NFC-e, financeiro, migrate. Só busca API/motor/overlay + provas/docs.

**Pausa loja:** ~1–2 min (Gunicorn restart, **sem** migrate).

## Na senha (comando único)

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-pdv-balanca-agro-pg
git push origin producao
```

Acompanhar Render até Live → lojas: **Ctrl+F5** · badge **v26.71** · colar `2001000004812` + Enter → carrinho **R$ 4,81**.

## Voltar (só frase + senha)

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-pdv-balanca-agro-pg-v26.70
git push origin producao
```

Ver também `docs/ROLLBACK-PDV-BALANCA-AGRO-PG.md`.
