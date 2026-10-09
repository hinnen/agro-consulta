# PREP deploy — PDV-BALANCA-AGRO-PG (+ preço etiqueta) · alvo **v26.72**

**Não sobe sozinho.** Só com frase explícita + senha `99738595` na mesma mensagem.

## Conteúdo

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **PDV-BALANCA-AGRO-PG** + **preço etiqueta** | path **52/52** · unit **10/10** | **NÃO** |

**O quê:** bip acha GM0010-1 **e** usa o total da etiqueta (ex. `2001000004812` → **R$ 4,81**), não o preço de cadastro (R$ 9,40). Overlay não sobrescreve.  
**Base Live atual:** **v26.71** @ `2eab76c1`  
**Branch PREP:** `deploy/prep-pdv-balanca-agro-pg`  
**Rollback hotfix:** `producao` @ `2eab76c1` (v26.71) · tag `rollback/pre-pdv-balanca-preco-v26.71`  
**Rollback pacote inteiro:** tag `rollback/pre-pdv-balanca-agro-pg-v26.70`

**Pausa loja:** ~1–2 min · **sem** migrate · Ctrl+F5 obrigatório (JS).

## Na senha

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-pdv-balanca-agro-pg
git push origin producao
```

Smoke: Ctrl+F5 · badge **v26.72** · colar `2001000004812` → carrinho **R$ 4,81** (não 9,40).
