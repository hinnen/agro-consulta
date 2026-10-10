# Rollback — PDV balança USE-P2 (v17.82)

Hotfix do overlay **Pesar granel** (F10). Sem migrate.

## Produção — peso ao vivo `kg` (**v17.82**) — ponto **antes** deste hotfix (Live v17.81)

| Item | Valor |
| ---- | ----- |
| **Commit anterior** | `fef6815` (Live v17.81 · NFC-e dest CNPJ + parser balança v17.80) |
| **O quê reverte** | parser: `kg` antes de `ESC N 1` 0,00 no mesmo ecrã |
| **Migrate** | **nenhuma** |
| **NFC-e** | permanece (pacote v17.81) se voltar só este hotfix |

Se a loja falhar **depois do v17.82** e o visor antigo (0,00 no prato cheio) for aceitável:

```bash
git fetch origin
git checkout producao
git reset --hard fef6815ee313ea014703aa59faf6beb7ba863289
git push origin producao --force-with-lease
```

Volta para Live **v17.81** (NFC-e CNPJ intacto; parser da balança = v17.80).

## Produção — USE-P2 ESC N 1 (**v17.80**) — ponto **antes** do pacote (Live v17.79)

| Item | Valor |
| ---- | ----- |
| **Commit anterior** | `589aa20` (Live v17.79 · hash banana) |
| **Tag** | `rollback/pre-pdv-balanca-use-p2-v17.79` |
| **Tag do pacote verificado v17.80** | `checkpoint-use-p2-pronto-99738595` @ `12b59342` |
| **Branch backup** | `producao-backup-pre-v1780-balanca-use-p2-20260823` |
| **O quê reverte** | parser USE-P2 (ESC N 1 / dump COM4) · serial 8N2 · SEM PORTA · auto-add **e** o NFC-e dest CNPJ (v17.81) se este reset for usado hoje |
| **Migrate** | **nenhuma** neste pacote; NFC-e v17.81 tem migrate `0099` |

Se a loja falhar e precisar do PDV **antes** da balança USE-P2 (isso também tira o NFC-e CNPJ):

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-pdv-balanca-use-p2-v17.79
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan (`99738595`). Volta para Live **v17.79**.
