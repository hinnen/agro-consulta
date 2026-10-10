# Rollback — PDV-BALANCA-AGRO-PG

Bip/colar EAN-13 de balança sob `agro_pg` (PLU 4 dígitos + preço etiqueta). Sem migrate.

| Item | Valor |
| ---- | ----- |
| **Antes** | Live **v26.70** · tag `rollback/pre-pdv-balanca-agro-pg-v26.70` @ `f290c371` |
| **O quê reverte** | Overlay PLU 4–7 · motor PLU exact+Mongo · API balança sem exigir Mongo · sem cache BCA no EAN flag 2 · casa PLU sem short `10` |
| **Migrate** | nenhuma |
| **Na senha** | `git reset --hard rollback/pre-pdv-balanca-agro-pg-v26.70` → `git push origin producao` |

Arquivos: `produtos/views.py` · `motor_busca_unificado_util.py` · `cadastro_busca_codigo_util.py` · `catalogo_agro.py` · `busca_filtro_pdv_util.py` · provas.
