# Rollback — etiqueta da nota com código GM (`NF-ETQ-CODIGO-GM`)

Ponto **antes** deste pacote = loja **Live v25.80** (`780015dd`). O nome limpo na etiqueta (**NF-ETQ-NOME-CADASTRO**) **permanece**.

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `780015dd` (Live v25.80) |
| **Tag** | `rollback/pre-nf-etq-codigo-gm-v25.80` |
| **Branch backup** | `producao-backup-pre-v2580-nf-etq-gm-20261001` |
| **O quê sobe** | etapa 6 da entrada de nota: código impresso = **GM do catálogo** (`codigo_nfe` / `produto_id`), não `cProd` do XML |
| **O quê NÃO sobe** | merge do `teste` · PDV · caixa · venda · fiado · financeiro |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-nf-etq-codigo-gm-v25.80
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v25.80**.

## Prova no PC (antes do envio)

`node scripts/verify_nf_etq_nome_cadastro.js` — **21/21**.  
`node scripts/verify_etiquetas_termica_varias.js` — **39/39** (ou **30/31** se Chrome não estiver no PATH; o único fail é PDF de prova).
