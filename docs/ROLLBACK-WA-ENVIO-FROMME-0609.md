# Rollback — WA-ENVIO-FROMME (loja alvo **v23.23**)

Ponto **antes** deste pacote = loja **Live v23.20** (`1e88a32`).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `1e88a32` (Live v23.20) |
| **Tag** | `rollback/pre-wa-envio-fromme-v23.20` |
| **Branch backup** | `producao-backup-pre-v2323-wa-envio-20260906` |
| **Branch PREP** | `deploy/prep-wa-envio-fromme-0609` · tip **v23.23** |
| **O quê sobe** | `WA-ENVIO-FROMME` — eco celular · seta sem travar · envio lid+telefone · poll mín. 3s · status mais leve |
| **O quê NÃO sobe** | merge do `teste` · Excel / resto |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-wa-envio-fromme-v23.20
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v23.20**.

**Ponte local:** o `whatsapp_atendimento/index.js` da pasta do PC também muda — se der ruim, volte o arquivo pelo Git **e** feche/abra o `iniciar.bat`.

## Risco operacional

| Pacote | Afeta finalizar venda? | Nota |
| ------ | ---------------------- | ---- |
| `WA-ENVIO-FROMME` | **Não** | Só Zap (`/atendimento-whatsapp/` + ponte Node no PC) |

## Depois do deploy (você)

1. **Ctrl+F5** no Zap · badge **v23.23**
2. Fechar/abrir `iniciar.bat`
3. Abrir chat → digitar → seta · preta: `Saida pendente` / `Enviado ok` / `Eco celular`
