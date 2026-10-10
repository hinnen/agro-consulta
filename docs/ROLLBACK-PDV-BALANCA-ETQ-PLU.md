# Rollback — PDV-BALANCA-ETQ-PLU

Bip de etiqueta EAN-13 de balança (PLU 4 dígitos + preço total + overlay). Sem migrate.

| Item | Valor |
| ---- | ----- |
| **O quê reverte** | Fallback overlay/catálogo local + preferência PLU `0010` antes de `10` |
| **Migrate** | nenhuma |
| **Como** | voltar `producao` ao tip **antes** do pacote (tag/commit no banana) · **só** frase + senha |

Arquivos: `produtos/views.py` · `produtos/static/produtos/js/consulta_produtos.js` · prova `scripts/verify_pdv_balanca_etq_plu_path.py`.
