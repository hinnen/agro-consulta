# Rollback — anti-duplicata WhatsApp (`WA-DEDUP-MSG` · 03/09/2026)

## Antes deste push

| | |
| --- | --- |
| **Tag** | `rollback/pre-wa-dedup-0309-v21.85` |
| **Branch backup** | `producao-backup-pre-wa-dedup-0309-v21.85` |
| **Commit** | `10b2821` · VERSION **21.85** |

## O que este lote muda

- 1 mensagem no Zap = 1 no SisVale (para eco notify×append / corrida)
- Unique `wa_id` — migrate **`0124`**
- Ponte: só `notify` ao vivo · trava saída 90s
- UI: não re-appenda bolha já na tela

## Reverter a loja

```bash
git checkout producao
git reset --hard rollback/pre-wa-dedup-0309-v21.85
git push --force-with-lease origin producao
```

No Render: deploy do commit da tag.

**Migrate:** o `0124` só **adiciona** índice único (e zera `wa_id` duplicado antigo). Voltar o código **não** remove o índice sozinho — na prática **deixar o 0124** no banco é seguro mesmo após rollback do código. Se precisar desfazer o constraint:

```bash
# só se alguém pedir explicitamente
python manage.py migrate produtos 0123
```

## Ponte

Fecha e abre **uma** janela `iniciar.bat` depois do Render verde.
