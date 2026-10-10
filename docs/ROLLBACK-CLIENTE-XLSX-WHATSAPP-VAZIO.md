# Rollback — Excel clientes: WhatsApp vazio apaga (`CLIENTE-XLSX-WHATSAPP-VAZIO`)

Ponto **antes** deste pacote = loja **Live v25.84** (`c1034c00`).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `c1034c00` (Live v25.84) |
| **Tag** | `rollback/pre-cliente-xlsx-whatsapp-vazio-v25.84` |
| **Branch backup** | `producao-backup-pre-v2584-xlsx-whatsapp-vazio-20261001` |
| **O quê sobe** | Na importação Excel de `/clientes/`, **célula vazia na coluna WhatsApp** grava cadastro **sem número** (antes: vazio = não alterava). Demais colunas amarelas: vazio continua = não muda. Aba «Como usar» item 7 atualizado. |
| **O quê NÃO sobe** | merge do `teste` · cron RH · PDV · caixa · limite fiado · saldo fiado |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-cliente-xlsx-whatsapp-vazio-v25.84
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v25.84**.

## Prova

`python scripts/verify_cliente_planilha_path.py` — blocos **Patch WhatsApp**, **Import WhatsApp vazio (mock)**; contratos `help_whatsapp_vazio_apaga` · **57/57** OK (mock cobre gravação se DB local incompleto).
