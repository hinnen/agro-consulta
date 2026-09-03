# Rollback — fix WhatsApp Limpar / entrada / foto (03/09/2026)

## Antes deste push

| | |
| --- | --- |
| **Tag** | `rollback/pre-wa-fix-0309-v21.84` |
| **Branch backup** | `producao-backup-pre-wa-fix-0309-v21.84` |
| **Commit** | `c165db2` · VERSION **21.84** |

## O que este lote muda

- Botão **Limpar** (só SisVale)
- Mensagem nova após limpar cria chat de novo
- Foto/áudio não duplicam (trava saída em voo)
- `iniciar-local.bat` (PC local) · `iniciar.bat` lê `.env` da loja
- **Sem** migrate nova

## Reverter a loja

```bash
git checkout producao
git reset --hard rollback/pre-wa-fix-0309-v21.84
git push --force-with-lease origin producao
```

No Render: deploy do commit da tag. **Não** precisa `migrate` para voltar este lote.

## Ponte neste PC

- Loja: `whatsapp_atendimento/iniciar.bat` + `.env` com `https://sistvale.com.br` e o token do Render
- Local: `iniciar-local.bat`
