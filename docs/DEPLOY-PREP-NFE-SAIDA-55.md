# PREP — NFE-SAIDA-55

Pacote: tela NF-e saída modelo 55 (`/nfe/saida/`).

- **Migrate:** não
- **Prova:** `python scripts/verify_nfe_saida_path.py` (27/27)
- **Homolog:** `NFE_DIST_DFE_TP_AMB=2` + cert `NFE_DIST_DFE_*` → emitir simula autorização
- **Produção:** emissão real ainda retorna erro até fase 2 SOAP

Na senha loja: cherry ou merge do trecho em `teste` → `producao` conforme checklist §88 `banana-roteiro.md`.
