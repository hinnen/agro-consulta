# Rollback — WA-CHAT-SNAP + ponte 5s (loja alvo **v21.92**)

Ponto **antes** deste pacote = loja **Live v21.91** (`319404f` · lote checklist 0509g).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `319404f` (Live v21.91) |
| **Tag** | `rollback/pre-wa-chat-snap-v21.91` |
| **Branch backup** | `producao-backup-pre-v2192-wa-chat-snap-20260905` |
| **Branch PREP** | `deploy/prep-wa-chat-snap-0509` · tip **v21.92** |
| **O quê sobe** | `WA-CHAT-SNAP` (troca de chat sem reload lista/estado) · poll UI lista/estado **5s** · ponte `puxarSaida` **5s** |
| **O quê NÃO sobe** | merge do `teste` · `WA-TOPBAR-OVERLAY` · `WA-TROCAR-FEED` · Excel / resto |
| **O quê NÃO reverte** | Live v21.91 e anteriores |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-wa-chat-snap-v21.91
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v21.91**.

## Risco operacional

| Pacote | Afeta finalizar venda? | Nota |
| ------ | ---------------------- | ---- |
| `WA-CHAT-SNAP` / ponte 5s | **Não** (cobrança) | Só tela Zap + ritmo da ponte no PC |

## Depois do deploy (você)

1. **Ctrl+F5** no Zap / loja · badge **v21.92**
2. **Fecha e abre** o `iniciar.bat` (ponte pegar 5s do arquivo novo — se o `.bat` apontar para esta pasta do Git)
3. Smoke: trocar 3 chats · PDV F7 · enviar 1 msg
