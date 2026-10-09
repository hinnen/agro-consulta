# Rollback — PDV-BALANCA-AGRO-PG

Bip/colar EAN-13 de balança sob `agro_pg` (PLU 4 dígitos + preço etiqueta). Sem migrate.

| Item | Valor |
| ---- | ----- |
| **O quê reverte** | Overlay PLU 4–7 · motor complementar Mongo no PLU · API balança sem exigir Mongo · sem cache BCA no EAN flag 2 |
| **Migrate** | nenhuma |
| **Como** | voltar `producao` ao tip **antes** do pacote (tag/commit no banana) · **só** frase + senha |

Arquivos: `produtos/views.py` · `produtos/motor_busca_unificado_util.py` · `produtos/cadastro_busca_codigo_util.py` · `produtos/catalogo_agro.py` · `produtos/busca_filtro_pdv_util.py` · provas `scripts/verify_pdv_balanca_agro_pg_path.py` / `verify_pdv_balanca_etq_plu_path.py`.
