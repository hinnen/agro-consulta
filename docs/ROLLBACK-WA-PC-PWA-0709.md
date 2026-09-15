# Rollback — WA-PC-PWA (loja alvo **v23.33**)

Ponto **antes** deste pacote = loja **Live v23.32** (`2f5206f`).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `2f5206f` (Live v23.32) |
| **Tag** | `rollback/pre-wa-pc-pwa-v23.32` |
| **Branch backup** | `producao-backup-pre-v2333-wa-pc-pwa-20260907` |
| **Branch PREP** | `deploy/prep-wa-pc-pwa-0709` · tip **v23.33** |
| **O quê sobe** | `WA-PC-PWA` — Zap web instalável no Chrome (manifest + SW + botão Instalar no PC) |
| **O quê NÃO sobe** | merge do `teste` · resto do tip |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-wa-pc-pwa-v23.32
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v23.32**.

## Risco operacional

| Pacote | Afeta finalizar venda? | Nota |
| ------ | ---------------------- | ---- |
| `WA-PC-PWA` | **Não** | Só tela WhatsApp PC · celular PWA intacto |

## Depois do deploy (você)

1. **Ctrl+F5** no Zap web · badge **v23.33**
2. Chrome ⋮ → **Instalar WhatsApp lojas…** (ou botão **Instalar no PC**)
3. Abrir o app e conferir lista/chat
