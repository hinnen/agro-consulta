# PREP — CHECKLIST 10/10e · alvo **v26.83**

**Status:** 🟢 **PREP pronto** — **não** subiu · lojas abertas · **só** frase + senha no próximo chat.

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **CB-230-GM4045-HOTFIX** | gerador **26/26** · bip **10/10** · etq **74/74** · Django **29/29** | **NÃO** |

**O quê:** corrige gerador 230 «faixa esgotada» (max_seq lixo / Mongo na alocação). **Não** altera sozinho o código legado do GM4045.

**Pós-deploy (obrigatório GM4045):**

```bash
python manage.py reatribuir_cb_loja_exclusivo --codigo-gm GM4045 --esperado-atual 2300000001479 --aplicar --confirmar GM4045
```

Ou na tela: botão **230** → **Salvar** → **nova etiqueta** → bip.

| Campo | Valor |
| ----- | ----- |
| **Tip PREP** | `deploy/prep-checklist-1010e` |
| **Base Live** | **v26.82** @ `b7bdde7b` |
| **Rollback tag** | `rollback/pre-checklist-1010e-v26.82` → `b7bdde7b` |
| **Backup branch** | `producao-backup-pre-v2683-checklist-1010e` |
| **Cutover** | `scripts/cutover_loja_checklist_1010e.sh` |

## Dry-run

```bash
./scripts/cutover_loja_checklist_1010e.sh
```

## Na senha

```bash
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='99738595' \
  ./scripts/cutover_loja_checklist_1010e.sh --exec
```

## Smoke

1. Ctrl+F5 · badge **v26.83**.
2. Cadastro produto novo → **230** → não «faixa esgotada».
3. GM4045: reatribuir ou **230** + etiqueta nova → bip acha GM4045.

## Voltar

`docs/ROLLBACK-CHECKLIST-1010e.md`
