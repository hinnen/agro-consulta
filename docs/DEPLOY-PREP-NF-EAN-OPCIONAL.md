# PREP deploy — NF-EAN-OPCIONAL · alvo **v26.80**

**Status:** 🟢 **armado / pronto para envio** (CHECKLIST **10/10b**)  
**Não sobe sozinho.** Só com frase explícita + senha `99738595` na mesma mensagem.

Canônico do cutover: **`docs/DEPLOY-PREP-CHECKLIST-1010b.md`**.

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **NF-EAN-OPCIONAL** | path **26/26** · Django **20/20** | **NÃO** |

**Tip PREP:** `deploy/prep-nf-ean-opcional` @ `362fec4c` · base Live **v26.79** @ `735698c7`  
**Rollback:** `rollback/pre-checklist-1010b-v26.79`

## Na senha

```bash
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='99738595' \
  ./scripts/cutover_loja_checklist_1010b.sh --exec
```

Smoke: Entrada NF · EAN só opcional → casa · principal ganha · duplicidade sem vínculo.
