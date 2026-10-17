# Rollback — Checklist 10/09 (loja alvo **v23.91**)

Ponto **antes** deste lote = loja **Live v23.76** (`056e9a7` / hotfixes repasse).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `056e9a7` (Live v23.76) |
| **Tag (criar no deploy)** | `rollback/pre-checklist-1009-v23.76` |
| **Branch backup (criar no deploy)** | `producao-backup-pre-v2391-checklist-20260910` |
| **Branch PREP** | `deploy/prep-checklist-1009` · tip **v23.91** |
| **O quê sobe** | `CLI-DUP-TEL-Z` · `PIN-NS-BI` · `FOTOS-PRODUTO-MOBILE` · `PIN-SSPIN-GLOBAL` · `NF-FIN-NAO-TEM` · `NF-AGUARDA-PRODUTO` |
| **O quê NÃO sobe** | merge do `teste` · WhatsApp · Excel cadastro · DRE WIP · outros |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-checklist-1009-v23.76
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v23.76**.

## Risco operacional (lojas abertas)

| Pacote | Afeta finalizar venda / PDV? | Nota |
| ------ | ---------------------------- | ---- |
| `PIN-SSPIN-GLOBAL` | **Médio** (PIN em várias telas) | Teclado no lugar do alert preto; se PIN já logado, venda segue. Ctrl+F5 após Live. |
| `PIN-NS-BI` | **Não** (PDV) | Só Novo lançamento no BI |
| `CLI-DUP-TEL-Z` | **Baixo** | Só popup telefone duplicado no EDITAR cadastro (z 250) |
| `FOTOS-PRODUTO-MOBILE` | **Não** | Só hub `/vendas/lojas/` → Fotos |
| `NF-FIN-NAO-TEM` | **Não** (PDV) | Só Entrada NF · etapa financeiro |
| `NF-AGUARDA-PRODUTO` | **Não** (PDV) | Só lista Entrada NF · lembrete «Deve produto» |

## Provas (pré-envio · refeitas 10/09)

| Pacote | Prova |
| ------ | ----- |
| `CLI-DUP-TEL-Z` | path **60/60** · deep **24/24** |
| `PIN-NS-BI` / teclado | `verify_pin_alert_teclado_path.py` **130/130** · PIN 9973=Renan |
| `PIN-SSPIN-GLOBAL` | `verify_sspin_cobertura_path.py` **197/197** |
| `FOTOS-PRODUTO-MOBILE` | `verify_fotos_produto_mobile_path.py` **59/59** · PIN 9973 |
| `NF-FIN-NAO-TEM` | `verify_nf_fin_nao_tem_path.py` **15/15** · religa **6/6** |
| `NF-AGUARDA-PRODUTO` | `verify_nf_aguarda_produto_path.py` **6/6** |
| Django | `manage.py check` OK |

## Depois do deploy (você)

1. **Ctrl+F5** · badge **v23.91**
2. Qualquer tela de loja → ação com PIN → **teclado** (sem caixa preta)
3. PDV EDITAR cliente → telefone duplicado por cima do modal
4. Hub → **Fotos** (celular) · Entrada NF → **Deve produto** / nota «não tem»
