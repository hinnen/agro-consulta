# PREP 11/11c — troca de câmera no SistApp · v27.26.57

Ainda **não** foi para a loja. Lojas abertas: o envio espera pausa + frase + senha no mesmo pedido.

| | |
| - | - |
| Entra | Ajuste, Fotos e Etiquetas: câmera de trás mais nítida e botão **Trocar câmera** |
| Não entra | PDV, caixa, venda, nota, financeiro |
| Migrate | Não |
| Base | `producao` @ `c8256c26` (v27.26.56) |
| Código | `teste` @ `77de8913` |
| Volta | Na hora da senha nasce `rollback/pre-v272657-1111c-sistapp-troca-camera`. Revert: `git push origin rollback/pre-v272657-1111c-sistapp-troca-camera:producao` |

Antes da senha:

```bash
./scripts/prep_checkpoint_checklist_1111c_sistapp_troca_camera.sh
./scripts/cutover_loja_checklist_1111c_sistapp_troca_camera.sh
```

Na senha, com as vendas pausadas:

```bash
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='…' \
  ./scripts/cutover_loja_checklist_1111c_sistapp_troca_camera.sh --exec
```

Depois, no Shell do Render: `./scripts/post_deploy_checklist_1111c_sistapp_troca_camera.sh`

Prova neste preparo: patch aplica em `c8256c26`. JS ok. Path da troca **VERIFY_OK 10**. Path antigo do bip **20/21** (falta só o banco desta máquina).
