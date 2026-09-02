# Rollback — PIN teclado obrigatório (`PIN-TECLADO-OBRIG` · alvo loja **v21.02**)

Ponto **antes** deste SOLO = loja **Live v20.86** (`798caaa`).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `798caaa` (Live v20.86) |
| **Tag rollback** | `rollback/pre-pin-teclado-obrig-v20.86` |
| **Branch backup** | `producao-backup-pre-v2102-pin-teclado-20260902` |
| **Branch PREP** | `deploy/prep-pin-teclado-obrig` |
| **O quê sobe** | `PIN-TECLADO-OBRIG` |
| **O quê NÃO sobe** | WhatsApp (`WA-*`) · resto do `teste` |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-pin-teclado-obrig-v20.86
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v20.86**.

## O que este pacote muda

- Entrada NF / Gestão / Cadastro ERP passam a ter a tela do PIN (teclado)
- Toast «Identifique-se com o PIN…» abre o teclado (não só o aviso vermelho)
- Registrar estoque na NF pede PIN antes; 403 reabre o teclado
- PDV: mesmo fallback no toast; descanso ~3 min inalterado
- Watchdog se travar sem teclado visível

## Risco operacional (lojas abertas)

| Área | Afeta venda PDV? | Nota |
| ---- | ---------------- | ---- |
| Entrada NF / Gestão / Cadastro | **Não** (venda) | Só melhora pedir PIN com teclado |
| PDV toast → PIN | **Baixo** | Se API pedir PIN, agora **abre** o teclado (antes travava) |
| Descanso ~3 min | Igual | Já existia na loja |

Carrinho, caixa, NFC-e, estoque ledger, financeiro **sem** mudança de regra — só UI do PIN.

## Deploy (próximo chat + senha)

**Não** resetar `producao` para `teste` inteiro (lá tem WhatsApp).

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-pin-teclado-obrig
git push origin producao --force-with-lease
```

Render sobe · healthz · badge **v21.02** · **Ctrl+F5** nos PCs.

## Smoke pós-deploy

1. Badge home **v21.02**
2. `/entrada-nota/` — teclado no HTML · etapa 5 Registrar → PIN ou teclado
3. `/produtos/gestao/` · `/produtos/cadastro-erp/` — `#sspin-root` presente
4. PDV — F7 uma venda (nada quebrou) · se pedir PIN, teclado aparece
