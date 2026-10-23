# Rollback — orçamento por cliente (`PDV-ORC-POR-CLIENTE` · alvo **v21.08**)

Ponto **antes** deste hotfix = loja **Live v21.07** (`0f5bd5d`).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `0f5bd5d` (Live v21.07) |
| **Tag rollback** | `rollback/pre-orc-cliente-0209-v21.07` |
| **Branch backup** | `producao-backup-pre-v2108-orc-cliente-20260902` |
| **Branch PREP** | `deploy/prep-orc-cliente-0209` |
| **O quê sobe** | só `PDV-ORC-POR-CLIENTE` |
| **O quê NÃO sobe** | WhatsApp (`WA-*`) · resto do `teste` |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-orc-cliente-0209-v21.07
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v21.07**.

## O que este lote muda

- Orçamento continua gravando no Postgres na **pasta do cliente da tela**
- F6 / card mostram **só** esse cliente (inclui consumidor não identificado)
- Sync online em todos os PCs com o mesmo cliente
- Modal inicial (`unset`) usa pasta **consumidor** (não mistura a loja)

**Não** mexe Finalizar / F7 / caixa / NFC-e / estoque / fiado / PIN / WhatsApp.

## Risco operacional (lojas abertas)

| Área | Afeta venda F7 / caixa / NFC-e? | Nota |
| ---- | ------------------------------- | ---- |
| Lista orçamento por `cliente_key` | **Não** | Só F6 / card / salvar orçamento |
| Sync GET | **Não** | Leitura; falha some em silêncio |

## Deploy (próximo chat + senha)

**Não** resetar `producao` para `teste` inteiro.

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-orc-cliente-0209
git push origin producao --force-with-lease
```

Render · healthz · badge **v21.08** · **Ctrl+F5** nos PCs.

## Smoke pós-deploy

1. Badge **v21.08**
2. PDV — F7 **uma venda** (nada quebrou)
3. Cliente Renan · salvar orç. · outro PC com Renan · F6 só dele
4. Consumidor · F6 não mistura orçamento de Renan
