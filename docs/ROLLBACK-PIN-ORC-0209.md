# Rollback — PIN + orçamento (`PIN-TECLADO-OBRIG` + `PIN-ET5-CAMPO` + `PDV-ORC-SAVE` · alvo **v21.06**)

Ponto **antes** deste lote = loja **Live v20.86** (`798caaa`).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `798caaa` (Live v20.86) |
| **Tag rollback** | `rollback/pre-pin-orc-0209-v20.86` |
| **Branch backup** | `producao-backup-pre-v2106-pin-orc-20260902` |
| **Branch PREP** | `deploy/prep-pin-orc-0209` |
| **O quê sobe** | `PIN-TECLADO-OBRIG` · `PIN-ET5-CAMPO` · `PDV-ORC-SAVE` |
| **O quê NÃO sobe** | WhatsApp (`WA-*`) · resto do `teste` |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-pin-orc-0209-v20.86
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v20.86**.

## O que este lote muda

- Entrada NF / Gestão / Cadastro: teclado PIN; etapa 5 tem **campo PIN** acima do botão azul
- Toast «Identifique-se com o PIN…» abre teclado (PDV/gestão)
- Orçamento do PDV grava no Postgres **sem login Chrome**; Zap espera gravar; CSRF continua obrigatório

## Risco operacional (lojas abertas)

| Área | Afeta venda F7 / caixa / NFC-e? | Nota |
| ---- | ------------------------------- | ---- |
| PIN teclado + campo NF | **Não** (venda) | Só quando a ação **já** pedia PIN |
| Orçamento salvar/Zap | **Não** | Só botão orçamento / F2 — carrinho e Finalizar iguais |
| Lista orçamentos GET | Leitura | Sem login, igual buscar do PDV |

**Não** mexe regra de estoque, caixa, NFC-e, fiado, financeiro.

## Deploy (próximo chat + senha)

**Não** resetar `producao` para `teste` inteiro.

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-pin-orc-0209
git push origin producao --force-with-lease
```

Render · healthz · badge **v21.06** · **Ctrl+F5** nos PCs.

## Smoke pós-deploy

1. Badge **v21.06**
2. PDV — F7 **uma venda** (nada quebrou)
3. PDV — 1 item · Salvar orç. · F2 lista
4. `/entrada-nota/` etapa 5 — campo PIN visível
5. Gestão / Cadastro ERP — teclado PIN no HTML
