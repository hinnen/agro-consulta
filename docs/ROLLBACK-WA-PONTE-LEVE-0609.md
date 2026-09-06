# Rollback — WA-PONTE-LEVE (loja alvo **v23.07**)

Ponto **antes** deste pacote = loja **Live v23.06** (`c39d7a2` · tip docs pós VL-PREV-MES / código útil `d0732d0`).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `c39d7a2` (Live v23.06) |
| **Tag** | `rollback/pre-wa-ponte-leve-v23.06` |
| **Branch backup** | `producao-backup-pre-v2307-wa-ponte-leve-20260906` |
| **Branch PREP** | `deploy/prep-wa-ponte-leve-0609` · tip **v23.07** |
| **O quê sobe** | `WA-PONTE-LEVE` — agenda/fotos **1×/dia** (Bot → Tempo, padrão 00:00) · poll saída 2–15s · `bridge/saida` só devolve fotos com `?fotos=1` · ponte Node sem despejo no connect |
| **O quê NÃO sobe** | merge do `teste` · Excel / resto |
| **O quê NÃO reverte** | Live v23.06 e anteriores (`VL-PREV-MES` permanece se voltar só este pacote… **não**: reset duro volta para v23.06 completo) |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-wa-ponte-leve-v23.06
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v23.06**.

## Risco operacional

| Pacote | Afeta finalizar venda? | Nota |
| ------ | ---------------------- | ---- |
| `WA-PONTE-LEVE` | **Não** (cobrança) | Alivia carga no Render (fotos não vêm a cada poll). Ponte no PC precisa **fechar/abrir** `iniciar.bat` para pegar o `index.js` novo |

## Depois do deploy (você)

1. **Ctrl+F5** · badge **v23.07**
2. **Fecha e abre** o `iniciar.bat` (ponte)
3. Bot → **Tempo** → conferir **Checar saída** + **Atualizar agenda/fotos** (00:00) → **Salvar**
4. Smoke: enviar 1 msg no Zap · PDV F7 continua leve
