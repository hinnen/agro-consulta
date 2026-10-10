# Rollback — BUG #34 devolução frete

Volta a loja ao estado **antes** deste pacote (Live **v26.53**).

| Item | Valor |
| ---- | ----- |
| **Antes (loja)** | `producao` @ `32198975` · VERSION **26.53** |
| **Tag segurança** | `rollback/pre-bug34-devol-frete-v26.53` |
| **Branch PREP** | `deploy/prep-bug34-devol-frete` · alvo **v26.55** |
| **Migrate** | **NÃO** |
| **Merge `teste`?** | **NÃO** |

```bash
git fetch origin tag rollback/pre-bug34-devol-frete-v26.53
git checkout producao
git reset --hard rollback/pre-bug34-devol-frete-v26.53
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan.
