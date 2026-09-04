# Rollback — lote checklist 0409 (loja alvo **v21.89**)

Ponto **antes** deste lote = loja **Live v21.88** (`329f9b5` · NF-FIN-MANUAL-RELIGA).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `329f9b5` (Live v21.88) |
| **Tag (criada no PREP)** | `rollback/pre-lote-checklist-0409-v21.88` |
| **Branch backup (criada no PREP)** | `producao-backup-pre-v2189-lote-checklist-20260904` |
| **Branch PREP** | `deploy/prep-checklist-0409` · tip **v21.89** |
| **O quê sobe** | `LOGIN-BI-FECHADO` + `LOGIN-UI-AGRO` · `NF-LISTA-ANDAMENTO` · `ETQ-A6-BONUS` · `FIADO-LIMITE-LINHA` · `PDV-CHAT-POLL-10S` · `WA-XFER-PIX-ORC` (inclui recursos Zap **desligados** + migrate `0125`) |
| **O quê NÃO sobe** | merge do `teste` · Excel cadastro · WhatsApp UI extra além deste lote · `CLI-FORM-PDV-LAYOUT` · `CAD-FALLBACK-HIST` |
| **O quê NÃO reverte** | Live v21.88 (religa CP nota) e anteriores |
| **Migrate** | **SIM** `produtos.0125` (JSON `extras` na conversa Zap — campo novo, default `{}`) |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-lote-checklist-0409-v21.88
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v21.88**.  
A migration `0125` já aplicada **não some sozinha** — a coluna `extras` fica ociosa; preferir deixar no banco.

## Risco operacional (lojas abertas)

| Pacote | Afeta finalizar venda? | Nota |
| ------ | ---------------------- | ---- |
| `LOGIN-BI-FECHADO` | **Não** no PDV `/consulta/` | Fecha **BI `/`** e **atalhos** sem usuário/senha. PDV consulta **continua sem** login Django. Depois do deploy: quem abrir o painel `/` precisa entrar uma vez. |
| `LOGIN-UI-AGRO` | **Não** | Só tela `/entrar/` (marca). Admin feio redireciona. |
| `NF-LISTA-ANDAMENTO` | **Não** | Só lista Entrada NF «Em andamento». |
| `ETQ-A6-BONUS` | **Não** | Só etiquetas gôndola A6. |
| `FIADO-LIMITE-LINHA` | **Não** | Só tela `/fiado/` (clique no limite). |
| `PDV-CHAT-POLL-10S` | **Não** (cobrança) | Chat interno: 10s fechado / 2,5s aberto. Carrinho/F7 iguais. |
| `WA-XFER-PIX-ORC` | **Orçamento** | PDV: botões Celular \| Loja no orçamento Zap. Cobrança F7 **igual**. Recursos Zap **desligados** até ligar no Bot. Ponte: reiniciar o `.bat` depois do deploy. |

Rotina no deploy: **pausar vendas ~2 min** · aviso Zap · Ctrl+F5 · smoke abaixo.

## Deploy (próximo chat + senha)

**Não** resetar `producao` para o branch `teste`.

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-checklist-0409
git push origin producao --force-with-lease
```

Render sobe · conferir healthz / badge **v21.89** · migrate `0125`.

**Smoke:** Ctrl+F5 · badge **v21.89** · PDV consulta **abre** (sem login Django) · F7 uma venda · BI `/` pede `/entrar/` (janela anônima) · Fiado clique no limite · Entrada NF Em andamento · etiquetas A6 se for usar · Zap: passar loja (modal).
