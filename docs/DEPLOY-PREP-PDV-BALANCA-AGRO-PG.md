# PREP deploy — PDV-BALANCA-QTY-VALOR · alvo **v26.73**

**Não sobe sozinho.** Só com frase explícita + senha `99738595` na mesma mensagem.

## Conteúdo

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **PDV-BALANCA-QTY-VALOR** (etiqueta + digitar R$) | path agro_pg · unit balança | **NÃO** |

**O quê:**
1. **Etiqueta balança** (formato preço no EAN mantido): bip `2001000004812` → produto com **preço unitário** do cadastro/overlay (ex. R$ 9,40) + **quantidade** = total da etiqueta ÷ unitário (ex. 4,81 ÷ 9,40 ≈ **0,512**). Total no carrinho ≈ R$ 4,81; estoque baixa a qty correta.
2. **Sem balança:** na busca digitar produto + total — `R$10`, `$10`, `=10` ou `10$` — o PDV calcula a quantidade sozinho (mesmo cálculo).

**Base Live atual:** **v26.71** @ `2eab76c1` (bip OK; preço etiqueta ainda v26.72 PREP)  
**Branch feature:** `cursor/balanca-qty-por-valor-ca8a`  
**Rollback hotfix:** `producao` @ tip Live anterior · **só** frase+senha  

**Pausa loja:** ~1–2 min · **sem** migrate · Ctrl+F5 obrigatório (JS).

## Na senha

```bash
git fetch origin
git checkout producao
git reset --hard origin/cursor/balanca-qty-por-valor-ca8a
git push origin producao
```

(Ou tip PREP atualizado no momento do deploy.)

Smoke: Ctrl+F5 · badge **v26.73** · colar `2001000004812` → qty ≈ **0,512** × unitário · total **R$ 4,81** · digitar `produto R$10` Enter → qty = 10÷preço.
