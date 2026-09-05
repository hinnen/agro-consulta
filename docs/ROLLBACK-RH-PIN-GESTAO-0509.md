# Rollback — RH-PIN-GESTAO (loja alvo **v21.90**)

Ponto **antes** deste pacote = loja **Live v21.89** (`4910c79` · lote checklist 0409).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `4910c79` (Live v21.89) |
| **Tag** | `rollback/pre-rh-pin-gestao-v21.89` |
| **Branch backup** | `producao-backup-pre-v2190-rh-pin-gestao-20260905` |
| **Branch PREP** | `deploy/prep-rh-pin-gestao-0509` · tip **v21.90** |
| **O quê sobe** | `RH-PIN-GESTAO` — tela `/rh/operadores/` (buscar RH · cadastrar · vincular · remover/reativar · reset 1234) · migrate `base.0011` |
| **O quê NÃO sobe** | merge do `teste` · vale crédito · Point · repasse 0,00 · tabela entrega · resto checklist 05/09 |
| **Migrate** | **SIM** `base.0011` (`PerfilUsuario.ativo` + vínculo `funcionario`) |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-rh-pin-gestao-v21.89
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v21.89**.  
A migration `0011` já aplicada **não some sozinha** — campos novos ficam ociosos; preferir deixar no banco.

## Risco operacional

| Item | Nota |
| ---- | ---- |
| PIN existentes | Continuam (default `ativo=True`) |
| PDV / venda | Sem mudança de regra de venda · só bounce curto do Render |
| 1234 | Só em **novo** / **reativar** / **reset** |

## Smoke

Ctrl+F5 · badge **v21.90** · `/rh/operadores/` · listar operadores · PIN 9973 ainda entra no PDV.
