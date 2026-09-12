# Rollback — ETQ-A6-COLS (loja alvo **v23.97**)

Ponto **antes** deste pacote = loja **Live v23.96** (`80350dc` / checklist 12/09c).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `80350dc` (Live v23.96) |
| **Tag (criar no deploy)** | `rollback/pre-etq-a6-cols-v23.96` |
| **Branch backup (criar no deploy)** | `producao-backup-pre-v2397-etq-a6-cols-20260912` |
| **Branch PREP** | `deploy/prep-etq-a6-cols` · tip **v23.97** |
| **O quê sobe** | `ETQ-A6-COLS` — A6 1/2/3 colunas conforme largura |
| **O quê NÃO sobe** | merge do `teste` · Excel · DRE · PDV · outros |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-etq-a6-cols-v23.96
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v23.96**.

## Risco operacional (lojas abertas)

| Pacote | Afeta finalizar venda / PDV? | Nota |
| ------ | ---------------------------- | ---- |
| `ETQ-A6-COLS` | **Não** | Só `/produtos/etiquetas/` (JS grade + HTML). Bônus A6 100×45 continua 1×3. A4 e térmica intactos. |

**Piora venda/caixa?** Não esperado.

## Provas (pré-envio · 12/09)

| Item | Resultado |
| ---- | --------- |
| `verify_etiquetas_a6_path.js` | **86/86** |
| `verify_etiquetas_gondola_grade.js` | OK |
| Django `tests_etiquetas_presets` | **3/3** |
| Página local `/produtos/etiquetas/` | **200** · Folha A6 1–3 col · `?v=19` |
| API presets | **200** · `bonus-a6` 100×45 cols=1 |
| `manage.py check` | OK |

## Depois do deploy (você)

1. **Ctrl+F5** · badge **v23.97**
2. Etiquetas → Folha **A6** · Bônus 100×45 = 1 col
3. Largura **50** → 2 col · **33** → 3 col
4. Venda / caixa: sem mudança esperada
