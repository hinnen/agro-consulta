# Rollback — Checklist 12/09b (loja alvo **v23.95**)

Ponto **antes** deste lote = loja **Live v23.94** (`c3e0b1c` / checklist 11/09d).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `c3e0b1c` (Live v23.94) |
| **Tag (criar no deploy)** | `rollback/pre-checklist-1209b-v23.94` |
| **Branch backup (criar no deploy)** | `producao-backup-pre-v2395-checklist-20260912b` |
| **Branch PREP** | `deploy/prep-checklist-1209b` · tip **v23.95** |
| **O quê sobe** | `WA-PONTE-ULTRA-LEVE` · `WA-APP-SEM-PDV` · `PDV-RACOES-MARCA-VAZIA` · `PDV-CHAT-ENTREGA-DOCK` · `PDV-ENT-HORARIO-OPCOES` · `PDV-ENT-TROCO-ENTER` · `PDV-ENT-OVERLAY-SPLIT` · `ETQ-LOTE-FILA` · `PDV-IMP-SEP-OFF` · `PDV-IMP-PIN-ANTES` |
| **O quê NÃO sobe** | merge do `teste` · Excel cadastro · DRE WIP · outros |
| **Migrate** | **SIM** `produtos.0130` (`pdv_lista_concluida`) |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-checklist-1209b-v23.94
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v23.94**.  
**Atenção migrate:** ao voltar, o campo `pdv_lista_concluida` some do código; no Postgres a coluna pode permanecer (inofensiva) até migrate reverse manual se quiser limpar.

## Risco operacional (lojas abertas)

| Pacote | Afeta finalizar venda / PDV? | Nota |
| ------ | ---------------------------- | ---- |
| `WA-PONTE-ULTRA-LEVE` | **Não** (venda) | Ponte Zap mais leve. **Fechar `.bat` antes do deploy**; religar depois. |
| `WA-APP-SEM-PDV` | **Não** | Só UI do Zap app (sem Voltar PDV). |
| `PDV-RACOES-MARCA-VAZIA` | Baixo | Só fluxo Rações — marca sem peso some. |
| `PDV-CHAT-ENTREGA-DOCK` | Baixo | Aba Chat não cobre F7 na Entrega. |
| `PDV-ENT-HORARIO-OPCOES` | Baixo | Horário 9–17h obrigatório no frete. |
| `PDV-ENT-TROCO-ENTER` | Baixo | Enter vazio = total (sem troco). |
| `PDV-ENT-OVERLAY-SPLIT` | Médio | Overlay Entregas + migrate **0130** · Concluir. |
| `ETQ-LOTE-FILA` | **Não** (venda) | Só etiquetas Lote A4. |
| `PDV-IMP-SEP-OFF` | Baixo | Separação desmarcada no modal. |
| `PDV-IMP-PIN-ANTES` | Baixo | PIN antes de imprimir vias; sem 2ª impressão no retry. |

**Piora venda/caixa?** Não esperado — paths de Entrega/Zap/Etiquetas/Rações. Venda normal / Point / caixa Live **v23.94** permanece.

## Cherry (código · ordem)

1. `86196da` (+ prova `379cbb7`) — WA-PONTE-ULTRA-LEVE  
2. `5b3db53` (+ prova `c8ad2f3`) — WA-APP-SEM-PDV  
3. `ca05bd4` — PDV-RACOES-MARCA-VAZIA  
4. `5fcebdc` — PDV-CHAT-ENTREGA-DOCK  
5. `dda9b3b` — PDV-ENT-HORARIO-OPCOES  
6. `3194350` — PDV-ENT-TROCO-ENTER  
7. `ce604fa` (+ prova `1361f3c`) — PDV-ENT-OVERLAY-SPLIT + **0130**  
8. `7273b19` (+ prova `c30b77d`) — ETQ-LOTE-FILA  
9. `303ef8c` — PDV-IMP-SEP-OFF  
10. `08b5e24` · `a0e395e` — PDV-IMP-PIN-ANTES  

## Provas (pré-envio · 12/09b · tip `teste`)

| Pacote | Prova |
| ------ | ----- |
| `WA-PONTE-ULTRA-LEVE` | **47/47** · PIN **9973** |
| `WA-APP-SEM-PDV` | **53/53** · PIN **9973** |
| `PDV-RACOES-MARCA-VAZIA` | **57/57** · PIN **9973** |
| `PDV-CHAT-ENTREGA-DOCK` | **7/7** |
| `PDV-ENT-HORARIO-OPCOES` | **16/16** |
| `PDV-ENT-TROCO-ENTER` | **6/6** |
| `PDV-ENT-OVERLAY-SPLIT` | split **16/16** · lote **61/61** · PIN **9973** |
| `ETQ-LOTE-FILA` | **78/78** · PIN **9973** |
| `PDV-IMP-SEP-OFF` | UX · `chkSep.checked = false` · HTML sem checked |
| `PDV-IMP-PIN-ANTES` | PIN antes · retry sem 2ª via · TTL 120s pós-vias |
| Regressão | `manage.py check` OK · node `--check` wizard/consulta/etq OK |

Arquivos críticos vs tip `teste` (wizard/consulta/ponte/etq/0130): **SAME** (hash).

## Depois do deploy (você)

1. **Ctrl+F5** · badge **v23.95**
2. Religar `.bat` Zap · Bot → Tempo → poll **10** → Salvar
3. PDV Entrega: horário 9–17 · Enter no troco · overlay A pagar\|Pagas · Concluir · Separação desmarcada · PIN antes das vias
4. Etiquetas → Lote A4 pela fila
5. Venda normal / caixa: sem mudança esperada
