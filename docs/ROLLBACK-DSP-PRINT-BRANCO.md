# Rollback — fundo branco na impressão do Dispenser (`DSP-PRINT-BRANCO`)

Ponto **antes** deste pacote = loja **Live v25.78** (`4e4a1244`). O cartão do PIN **permanece**.

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `4e4a1244` (Live v25.78) |
| **Tag** | `rollback/pre-dsp-print-branco-v25.78` |
| **Branch backup** | `producao-backup-pre-v2578-dsp-branco-20260930` |
| **O quê sobe** | só o fundo das fotos e do logo na impressão do Dispenser |
| **O quê NÃO sobe** | merge do `teste` · PDV · caixa · venda · fiado · nota · financeiro |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-dsp-print-branco-v25.78
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v25.78**.

## Prova no PC (30/09, antes do envio)

`scripts/verify_dsp_impressao_folha_path.py` — **20/20**.  
No Chrome: PNG transparente sobre cartão branco sai branco no canto; sobre fundo verde sai verde. O miolo da imagem continua.
