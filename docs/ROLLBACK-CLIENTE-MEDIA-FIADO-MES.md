# Rollback — Excel clientes: média fiado por mês (`CLIENTE-MEDIA-FIADO-MES`)

Ponto **antes** deste pacote = loja **Live v25.81** (`c03890f2`). O Excel de clientes (**CLIENTE-XLSX-FIADO**) e a etiqueta NF GM (**NF-ETQ-CODIGO-GM**) **permanecem**.

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `c03890f2` (Live v25.81) |
| **Tag** | `rollback/pre-cliente-media-fiado-mes-v25.81` |
| **Branch backup** | `producao-backup-pre-v2581-cliente-media-fiado-20261001` |
| **O quê sobe** | Coluna **Média fiado/mês (3 meses)** = total fiado nos 3 meses calendário (atual + 2 anteriores) ÷ 3 (antes era média **por compra**) |
| **O quê NÃO sobe** | merge do `teste` · PDV · caixa · venda · fiado (só leitura na planilha) |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-cliente-media-fiado-mes-v25.81
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v25.81**.

## Prova (antes do envio)

`python scripts/verify_cliente_planilha_path.py` — contratos + média mensal (rodar no PC com banco migrado).
