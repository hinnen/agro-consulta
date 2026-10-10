# PREP — CHECKLIST 10/10l · **v26.90**

Corrige PDV que abria **GM4241** no bip `2300000001556`: cache local (`index_codigos` velho) + bip 12 dígitos `200000001556`.

| Prova | Migrate |
| ----- | ------- |
| legado 15/15 · bip 10/10 · PDV **13/13** | **NÃO** |

**Rollback:** `rollback/pre-checklist-1010l-v26.89` (commit live v26.89)  
**Cutover:** `scripts/cutover_loja_checklist_1010l.sh`

**Loja após deploy:** **Ctrl+F5** no PDV (obrigatório — cache localStorage v4).
