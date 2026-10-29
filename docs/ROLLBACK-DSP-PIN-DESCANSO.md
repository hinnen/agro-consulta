# Rollback — PIN do descanso no Dispenser (`DSP-PIN-DESCANSO`)

Ponto **antes** deste pacote = loja **Live v25.77** (`6eff0f70`). A impressão da folha **permanece**.

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `6eff0f70` (Live v25.77) |
| **Tag** | `rollback/pre-dsp-pin-descanso-v25.77` |
| **Branch backup** | `producao-backup-pre-v2577-dsp-pin-20260930` |
| **Branch PREP** | `deploy/prep-dsp-pin-descanso` · alvo **v25.78** |
| **O quê sobe** | só o cartão do PIN na tela `/interno/dispenser-a6/` |
| **O quê NÃO sobe** | merge do `teste` · PDV · caixa · venda · fiado · nota · financeiro |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-dsp-pin-descanso-v25.77
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v25.77**. O PDV, o caixa e a venda não entram neste pacote.

## Prova no PC (30/09, antes do envio)

`scripts/verify_dsp_impressao_folha_path.py` — **19/19**.
