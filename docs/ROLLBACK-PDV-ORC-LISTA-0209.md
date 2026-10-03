# Rollback — lista orçamento outro PC (`PDV-ORC-LISTA-PC` · alvo **v21.07**)

Ponto **antes** deste hotfix = loja **Live v21.06** (`a08dfed`).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `a08dfed` (Live v21.06) |
| **Tag rollback** | `rollback/pre-orc-lista-0209-v21.06` |
| **Branch backup** | `producao-backup-pre-v2107-orc-lista-20260902` |
| **Branch PREP** | `deploy/prep-orc-lista-0209` |
| **O quê sobe** | só `PDV-ORC-LISTA-PC` (bug #14) |
| **O quê NÃO sobe** | WhatsApp (`WA-*`) · resto do `teste` |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-orc-lista-0209-v21.06
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v21.06**.

## O que este lote muda

- `/pdv/` baixa orçamentos recentes da loja **ao abrir** (não espera escolher cliente)
- F6 / card lateral mostram a lista da loja (todos os PCs)
- CSRF meta no HTML do wizard (POST orçamento)

**Não** mexe Finalizar / F7 / caixa / NFC-e / estoque / fiado / PIN / WhatsApp.

## Risco operacional (lojas abertas)

| Área | Afeta venda F7 / caixa / NFC-e? | Nota |
| ---- | ------------------------------- | ---- |
| Lista orçamento GET `recentes` | **Não** | Leitura em background; falha some em silêncio |
| F6 / card | **Não** | Só lista de orçamento |
| CSRF meta | **Não** | Igual cookie que o PDV já seta |

## Deploy (próximo chat + senha)

**Não** resetar `producao` para `teste` inteiro.

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-orc-lista-0209
git push origin producao --force-with-lease
```

Render · healthz · badge **v21.07** · **Ctrl+F5** nos PCs.

## Smoke pós-deploy

1. Badge **v21.07**
2. PDV — F7 **uma venda** (nada quebrou)
3. Caixa Centro `/pdv/?agro_dual=1` — F6 vê orçamento gravado noutro PC
4. Ctrl+F5 se a lista antiga ficou no cache
