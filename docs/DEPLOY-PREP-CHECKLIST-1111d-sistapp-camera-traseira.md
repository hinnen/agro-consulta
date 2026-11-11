# PREP 11/11d — câmera de trás no SistApp · v27.26.58

| | |
| - | - |
| Entra | Ajuste, Fotos e Etiquetas abrem na câmera de trás. Trocar câmera muda a lente. |
| Não entra | PDV, caixa, venda, nota, financeiro |
| Migrate | Não |
| Base | `producao` @ `007e9bfc` (v27.26.57) |
| Código | `teste` @ `87de7db5` |
| Volta | `git push origin 007e9bfc:producao` |

Na senha:

```bash
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='…' \
  ./scripts/cutover_loja_checklist_1111d_sistapp_camera_traseira.sh --exec
```

Depois: `./scripts/post_deploy_checklist_1111d_sistapp_camera_traseira.sh`
