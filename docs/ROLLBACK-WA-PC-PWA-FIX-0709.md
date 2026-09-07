# Rollback — WA-PC-PWA-FIX (loja alvo **v23.34**)

Ponto **antes** deste pacote = loja **Live v23.33** (`c4e931c` / código `aed0349`).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `c4e931c` (Live v23.33) |
| **Tag** | `rollback/pre-wa-pc-pwa-fix-v23.33` |
| **Branch backup** | `producao-backup-pre-v2334-wa-pc-pwa-fix-20260907` |
| **Branch PREP** | `deploy/prep-wa-pc-pwa-fix-0709` · tip **v23.34** |
| **O quê sobe** | `WA-PC-PWA-FIX` — Zap PC abre fora da Gestão (janela `SistValeZap`) |
| **O quê NÃO sobe** | merge do `teste` · CP/Extravio extra do tip |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-wa-pc-pwa-fix-v23.33
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v23.33**.

## Risco operacional

| Pacote | Afeta finalizar venda? | Nota |
| ------ | ---------------------- | ---- |
| `WA-PC-PWA-FIX` | **Não** | Só roteamento Zap ↔ Gestão |

## Depois do deploy (você)

1. **Ctrl+F5** Gestão · badge **v23.34**
2. Abrir **WhatsApp computador** → **outra janela** (não aba)
3. Nessa janela: Instalar app
