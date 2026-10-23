# Rollback — Excel clientes grava todas as linhas de limite (`CLIENTE-XLSX-LIMITE-TODOS`)

Ponto **antes** deste pacote = loja **Live v25.82** (`d342e2a5`). A média fiado/mês (**CLIENTE-MEDIA-FIADO-MES**) **permanece**.

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `d342e2a5` (Live v25.82) |
| **Tag** | `rollback/pre-cliente-xlsx-limite-todos-v25.82` |
| **Branch backup** | `producao-backup-pre-v2582-xlsx-limite-todos-20261001` |
| **O quê sobe** | Excel ↑ de `/clientes/` grava **todas** as alterações (antes parava em **400**). Aba «Como usar» explica 0 vs 0,01. **Fiado em aberto** continua só leitura. |
| **O quê NÃO sobe** | merge do `teste` · cron RH · PDV · caixa · saldo fiado |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-cliente-xlsx-limite-todos-v25.82
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v25.82**.

## Prova

`python scripts/verify_cliente_planilha_path.py` — bloco **Import >400**: prévia conta **401**, grava **401** limites `0,01`, coluna fiado em aberto fora das chaves editáveis.
