# Deploy rápido — Checklist 05/10b (PREP v26.24)

**Só** com frase explícita + senha `99738595` na **mesma** mensagem.

Lojas abertas: na autorização, pausar finalizar venda ~2–3 min (Zap) antes do push `producao`.

## Já pronto (não repetir na hora)

| Item | Valor |
| ---- | ----- |
| Branch | `deploy/prep-checklist-0510b` @ `6c1fab4b` |
| Base Live | **v26.08** @ `8b636b67` |
| Tag rollback | `rollback/pre-checklist-0510b-v26.08` @ `8b636b67` |
| Alvo | **v26.24** |
| Migrate | **NÃO** |
| Pacotes | **PDV-PEDIR-PRONTO-TRANSF** · **META-MODO-AGORA** · **CREDITO-SCORE-XLSX-COLS** |

### O que sobe (resumo loja)

| Pacote | Risco venda | O quê |
| ------ | ----------- | ----- |
| Pedir loja | **Médio** (PDV) | Transferir no Aceito (Pronto opcional) · Transferir selecionados · bip 30 min multi-PC |
| META Até agora | Baixo | Interruptor Meta do mês × Até agora (ritmo) |
| Crédito Excel cols | Baixo | Excel ↓ cols + badge Revisar dados (lab) |

### Provas (no tip `teste` / mesmo código)

| Pacote | Prova |
| ------ | ----- |
| Pedir | parcial **37/37** · Pedir **80/80** · PodeAgir/Util **10/10** |
| META | **125/125** |
| Crédito | **86/86** · shadow **93/93** · **PREP_FAILS=0** |

**Não** é merge do `teste` — cherry-pick / arquivos alinhados sobre Live.

## Na autorização (ordem curta)

1. Aviso Zap: pausar finalizar venda ~2–3 min.
2. Backup opcional: `git branch producao-backup-pre-v2624-checklist-0510b origin/producao`
3. Atualizar loja:
   ```bash
   git fetch origin
   git checkout producao
   git reset --hard origin/deploy/prep-checklist-0510b
   git push origin producao --force-with-lease
   ```
4. Acompanhar Render **SistVale** até Live (**sem** migrate).
5. Ctrl+F5 nos PDVs · smoke: Pedir loja (Aceitar→Transferir) · META Até agora · Lab crédito Excel (autorizado).
6. No banana: badges → ✅ enviado / Live v26.24 · limpar «aguarda senha».

## Se precisar voltar

```bash
git checkout producao
git reset --hard rollback/pre-checklist-0510b-v26.08
git push origin producao --force-with-lease
```

**Só** com frase + senha de novo.

## Diff vs Live (arquivos)

`credito_score_*` · `meta_vendas_*` · `pdv_pedir_loja.js` · `pedir_loja_overlay` · `pdv_transf_loja_util` · verifies · `VERSION` 26.08→26.24.
