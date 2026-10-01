# Rollback — etiqueta da nota só com o nome do cadastro (`NF-ETQ-NOME-CADASTRO`)

Ponto **antes** deste pacote = loja **Live v25.79** (`072ff56e`). O fundo branco do Dispenser **permanece**.

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `072ff56e` (Live v25.79) |
| **Tag** | `rollback/pre-nf-etq-nome-cadastro-v25.79` |
| **Branch backup** | `producao-backup-pre-v2579-nf-etq-20261001` |
| **O quê sobe** | só o nome da etiqueta na etapa 6 da entrada de nota |
| **O quê NÃO sobe** | merge do `teste` · PDV · caixa · venda · fiado · financeiro |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-nf-etq-nome-cadastro-v25.79
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v25.79**.

## Prova no PC (01/10, antes do envio)

`node scripts/verify_nf_etq_nome_cadastro.js` — **14/14**.  
`node scripts/verify_etiquetas_termica_varias.js` — **39/39**.
