# Rollback — hotfix Chat abre (`PDV-CHAT-OPEN` · loja alvo **v19.63**)

Ponto **antes** deste hotfix = loja **Live v19.60** (`460e1c7`).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes)** | `460e1c7` (Live v19.60 · lote 2908g) |
| **Tag (criar no PREP)** | `rollback/pre-pdv-chat-open-v19.60` |
| **Branch backup (criar no PREP)** | `producao-backup-pre-chat-open-v1960-20260829` |
| **O quê sobe** | Só abrir Chat (dock → `body` + z-index 220) |
| **Arquivos** | `pdv_chat_loja.js` · `chat_loja_overlay.html` (+ VERSION / verify) |
| **Migrate** | **NÃO** |
| **O quê NÃO sobe** | `BI-META-C-VILA-RAMP` · demais WIP do `teste` |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-pdv-chat-open-v19.60
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v19.60**.

## Risco operacional (lojas abertas)

| Área | Afeta? | Nota |
| ---- | ------ | ---- |
| Venda / pagar / NFC-e | **Não** | Zero mudança |
| Caixa / fiado / Pedir loja | **Não** | Zero mudança |
| Chat | **Sim** | Só UI do chat — Ctrl+F5 nos PDVs |

**Prova:** `python scripts/verify_pdv_chat_loja.py` → **VERIFY_OK 62/62**.
