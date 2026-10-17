# Rollback — ETQ-PONTE-1CLIQUE

Volta o ZIP da ponte para o layout antigo (sem `CLIQUE-AQUI` / sem Node em `app/vendor`).

**Não** rodar sem frase explícita + senha de produção do Renan.

## O que reverter

- `produtos/etiquetas_print_bridge_util.py` (layout raiz + vendor Node)
- Textos UI/JS `CLIQUE-AQUI-INSTALAR.bat`
- `ensure-node.ps1` (uso do vendor do pacote)

## O que NÃO mexer

- Impressão térmica / mapa Elgin / PDV
