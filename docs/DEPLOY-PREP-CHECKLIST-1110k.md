# PREP — CHECKLIST 11/10k (restante) · **v27.03**

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **SISTAPP-PWA-AJUSTE-ATUALIZAR** | PWA **25/25** · resumo estático **164/164** | **NÃO** |
| 2 | **META-ZAP-ENXUTO** | `verify_meta_mostruario_path` core | **NÃO** |

**META-POR-LOJA** já **Live v27.01** (`d77cb17e`) — **não** repetir migrate 0144.

**Live antes deste envio:** **v27.01**  
**PREP:** `deploy/prep-checklist-1110k` (rebase em `producao` antes do cutover)  
**Rollback:** tag `rollback/pre-checklist-1110k-restante-v27.01` @ tip `producao` pré-push  
**Cutover (dry-run):** `./scripts/cutover_loja_checklist_1110k.sh`

**Não inclui** merge de `teste` inteiro — delta só GM Lojas / SistApp / Zap. **PDV, caixa e busca 230 intocados.**

## Antes de pausar vendas (~1 min)

```bash
git fetch origin producao deploy/prep-checklist-1110k
./scripts/cutover_loja_checklist_1110k.sh
```

Tudo `OK` → pausar vendas no Zap.

## Na senha (janela curta ~2 min)

```bash
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='99738595' \
  ./scripts/cutover_loja_checklist_1110k.sh --exec
```

Render Shell (opcional, rápido):

```bash
./scripts/post_deploy_checklist_1110k.sh
```

## Conferir na loja

- Badge **v27.03** · manifest **SistApp**  
- Hub → **Ajuste** verde · Vendas → **Atualizar** (reload)  
- `/meta/` Zap com linhas enxutas (`* R$ … bônus … ✅`)
