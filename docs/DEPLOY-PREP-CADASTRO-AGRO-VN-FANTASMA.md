# PREP — **CADASTRO-AGRO-VN-FANTASMA** (CHECKLIST **12/10o**)

## O quê

Cadastro ERP: remove duplicata **agro:vn:** (validade sem código) quando já existe produto **GM** com o mesmo nome.

- Funde overlay/lotes no produto real
- Catálogo Postgres e busca não listam fantasma
- Excel ↑ remapeia ID `agro:vn:` pelo GM/nome
- Comando pós-deploy: `python manage.py limpar_overlays_agro_vn_fantasma`

## Migrate

**NÃO**

## Prova

```bash
python scripts/verify_cadastro_agro_vn_path.py
```

## Cutover produção

Frase + senha loja · branch `teste` → `producao` · rodar comando de limpeza uma vez no shell Render (agro-db).
