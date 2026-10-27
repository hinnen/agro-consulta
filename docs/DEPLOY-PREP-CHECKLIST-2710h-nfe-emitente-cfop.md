# PREP 27/10h — NF-e 55 emitente Centro/Vila + CFOP × UF

**Pacote:** v27.26.39 · **NFE-EMITENTE-CFOP**  
**Migrate:** não  
**NFC-e / PDV:** intocados  

## Checkpoint (antes da loja)

```bash
./scripts/prep_checkpoint_checklist_2710h_nfe_emitente_cfop.sh
```

- Rollback: `rollback/pre-v272639-nfe-emitente-cfop` = `97d4b22c` (v27.26.38)
- Backup: `producao-backup-pre-v272639-nfe-emitente-cfop`
- PREP: `deploy/prep-checklist-2710h-nfe-emitente-cfop`

## Subir (só com frase + senha no mesmo chat)

```bash
AGRO_LOJA_SENHA=… AGRO_PIN_FRASE='pode ir para produção' \
  ./scripts/cutover_loja_checklist_2710h_nfe_emitente_cfop.sh --exec
```

Pausa da loja: só o deploy Render (sem migrate).

## Reverter

```bash
git push origin rollback/pre-v272639-nfe-emitente-cfop:producao
```

## Smoke (não transmitir NF-e)

1. Ctrl+F5 `/nfe/saida/`
2. Nova avulsa: sem emitente até clicar Centro ou Vila
3. Destinatário SP + CFOP 5102: checklist verde
4. Destinatário fora de SP + 5102: vermelho com UF origem→destino
5. PIN operador **9973** só se for emitir à mão
