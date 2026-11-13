# PREP 13/11 — câmera travada + PIN do Zap · v27.26.59

Ainda **não** foi para a loja.

| | |
| - | - |
| 1 | **SISTAPP-CAMERA-TRAS-FIXA** — tira **Trocar câmera**. Pede a câmera de trás, imagem maior e foco juntos |
| 2 | **WA-PIN-PC** — o Zap pede PIN. O botão do PDV continua abrindo o Zap separado e manda o PIN daquele PC |
| Migrate | Não |
| Base | `producao` @ `ee330433` (v27.26.58) |
| Volta | `git push origin ee3304332d77f0d91f6565bb84cfcfd1e81263b0:producao` |

Não entra o resto da branch do Zap. PDV, caixa e venda não mudam.

Na senha, com as vendas pausadas:

```bash
./scripts/prep_checkpoint_checklist_1311.sh
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='…' \
  ./scripts/cutover_loja_checklist_1311.sh --exec
```

Se o Zap roda em outro serviço, a mesma `AGRO_WA_PIN_HANDOFF_KEY` precisa existir lá. Sem a chave igual, o operador digita o PIN na tela do Zap.

Prova: câmera **VERIFY_OK 21**. O botão do PDV ainda tem `window.open`. Bip da câmera **18/19** (falta a chave desta máquina).
