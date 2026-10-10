# Código 230 legado (DV errado) vs etiqueta Zebra

## Sintoma (ex.: GM0024-P)

- Cadastro com **2300000001558** (EAN inválido — DV «8» no lugar de «6»).
- Etiqueta / leitor bipa **2300000001556** (EAN válido).
- Outro produto pode ter **1556** literal → busca abre o produto errado.

Mesmo padrão já visto em **GM4045** (1479 no cadastro vs 1471 no bip), com regra de busca que **não** troca legado por EAN de outro produto.

## Correção em massa (loja não precisa abrir item por item)

Após **v26.86**, no Shell do servidor (uma vez):

```bash
python manage.py migrar_cb_loja_legado --liberar-intruso --dry-run
python manage.py migrar_cb_loja_legado --liberar-intruso
```

- **`--liberar-intruso`:** agrupa cadastros que **bipam o mesmo EAN** (ex. vários legados 1550–1559 → etiqueta 1556), gera **230 novo** nos demais e corrige o vencedor (preferência código **GM**).
- Se ainda aparecer colisão: lista curta no log — ajuste pontual.

## Se aparecer colisão (ex. GM0024-P)

1. Reatribuir **230** no produto que já está com o código que a etiqueta bipa.
2. Rodar a varredura de novo **ou** salvar o GM afetado na gestão.

## Comportamento do sistema (v26.84+)

- Ao **salvar overlay** com 230 legado inválido, o backend tenta gravar o **EAN bipável** e mover o legado para `codigos_barras_opcionais`.
- `migrar_cb_loja_legado` valida colisão antes de gravar.
