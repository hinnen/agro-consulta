# Código 230 legado (DV errado) vs etiqueta Zebra

## Sintoma (ex.: GM0024-P)

- Cadastro com **2300000001558** (EAN inválido — DV «8» no lugar de «6»).
- Etiqueta / leitor bipa **2300000001556** (EAN válido).
- Outro produto pode ter **1556** literal → busca abre o produto errado.

Mesmo padrão já visto em **GM4045** (1479 no cadastro vs 1471 no bip), com regra de busca que **não** troca legado por EAN de outro produto.

## Correção na loja (ordem)

1. **Achar** quem está com o EAN bipável literal (ex. `2300000001556`) — gestão / cadastro / busca por barras.
2. **Reatribuir 230** nesse produto (botão **230** na gestão ou `python manage.py reatribuir_cb_loja_exclusivo …`) para liberar o **1556**.
3. No **GM0024-P** (ou produto com legado **1558**):
   - Salvar de novo na gestão (promove **1558 → 1556** e guarda legado em opcionais), **ou**
   - `python manage.py migrar_cb_loja_legado --pid=<produto_externo_id>` (sem `--dry-run` após conferir).

Se a migração ou o salvar retornar colisão, o **1556** ainda está em outro produto — volte ao passo 2.

## Comportamento do sistema (v26.84+)

- Ao **salvar overlay** com 230 legado inválido, o backend tenta gravar o **EAN bipável** e mover o legado para `codigos_barras_opcionais`.
- `migrar_cb_loja_legado` valida colisão antes de gravar.
