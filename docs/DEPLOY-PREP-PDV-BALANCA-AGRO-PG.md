# PREP deploy — PDV-BALANCA-QTY-VALOR · alvo **v26.74**

**Status:** 🟢 **pronto para envio à produção**  
**Não sobe sozinho.** Só com frase explícita + senha `99738595` na mesma mensagem.

## Conteúdo

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **PDV-BALANCA-QTY-VALOR** (wizard `/pdv/checkout/`) | qty-valor **63/63** · agro **56/56** · etq **41/41** · unit **11/11** | **NÃO** |

**O quê (tela da loja = wizard):**
1. **Etiqueta balança** — bip EAN → preço unitário + qty = total÷unitário (estoque certo; total ≈ etiqueta).
2. **Sem balança** — busca `produto R$10` / `$10` / `=10` / `10$` → qty automática.

**Branch:** `cursor/balanca-qty-por-valor-ca8a` · tip PREP `deploy/prep-pdv-balanca-agro-pg`  
**Base Live:** **v26.71** @ `2eab76c1`  
**Pausa loja:** ~1–2 min · sem migrate · Ctrl+F5 no PDV.

## Na senha

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-pdv-balanca-agro-pg
git push origin producao
```

Smoke: Ctrl+F5 · **v26.74** · bip `2001000004812` → qty≈0,512 · `produto R$10` Enter.
