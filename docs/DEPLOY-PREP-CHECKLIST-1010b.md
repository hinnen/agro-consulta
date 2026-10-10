# PREP — CHECKLIST 10/10b · alvo **v26.80**

**Status:** 🟢 **armado / pronto para envio** — **não** sobe sozinho.  
Só com frase explícita + senha `99738595` na mesma mensagem.

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **NF-EAN-OPCIONAL** | path **26/26** · Django **20/20** | **NÃO** |

**O quê:** Entrada NF casa EAN do XML em `codigos_barras_opcionais` · principal prioridade · duplicidade não vincula.

| Campo | Valor |
| ----- | ----- |
| **Tip PREP** | `deploy/prep-nf-ean-opcional` @ `362fec4c` |
| **Base Live** | **v26.79** @ `735698c7` |
| **Rollback tag** | `rollback/pre-checklist-1010b-v26.79` → `735698c7` |
| **Backup branch** | `producao-backup-pre-v2680-checklist-1010b` |
| **Cutover** | `scripts/cutover_loja_checklist_1010b.sh` |

## Na senha (rápido)

```bash
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='99738595' \
  ./scripts/cutover_loja_checklist_1010b.sh --exec
```

Equivale a: `git push origin origin/deploy/prep-nf-ean-opcional:producao` após gates.

## Smoke (lojas)

1. Ctrl+F5 · badge **v26.80**.
2. Entrada NF · EAN só nos opcionais → casa (`ean_overlay_opcional`).
3. Mesmo EAN no principal → principal ganha.
4. EAN opcional em 2 produtos → sem vínculo automático.

## Voltar (só frase + senha)

```bash
git fetch origin
git push origin rollback/pre-checklist-1010b-v26.79:producao --force-with-lease
```

Ou: `docs/ROLLBACK-CHECKLIST-1010b.md` / `docs/ROLLBACK-NF-EAN-OPCIONAL.md`.
