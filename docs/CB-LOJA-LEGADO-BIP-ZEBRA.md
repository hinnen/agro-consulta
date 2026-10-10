# Código 230 legado (DV errado) vs etiqueta Zebra

## Sintoma (ex.: GM0024-P)

- Cadastro com **2300000001558** (EAN inválido — DV «8» no lugar de «6»).
- Etiqueta / leitor bipa **2300000001556** (EAN válido).
- Outro produto pode ter **1556** literal → busca abre o produto errado.

Mesmo padrão já visto em **GM4045** (1479 no cadastro vs 1471 no bip), com regra de busca que **não** troca legado por EAN de outro produto.

## Correção em massa (loja não precisa abrir item por item)

Após subir a versão com este pacote, roda-se **uma vez** a varredura no servidor (equipe técnica / cutover):

- Corrige sozinha **todos** os 230 legados que derem (cadastro + gestão).
- No final aparece só a **lista de colisões** (poucos GM) — **só esses** pedem ajuste manual (reatribuir 230 no “intruso”, rodar de novo).

## Se aparecer colisão (ex. GM0024-P)

1. Reatribuir **230** no produto que já está com o código que a etiqueta bipa.
2. Rodar a varredura de novo **ou** salvar o GM afetado na gestão.

## Comportamento do sistema (v26.84+)

- Ao **salvar overlay** com 230 legado inválido, o backend tenta gravar o **EAN bipável** e mover o legado para `codigos_barras_opcionais`.
- `migrar_cb_loja_legado` valida colisão antes de gravar.
