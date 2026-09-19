# Rollback — lote checklist 0509h (loja alvo **v21.93**)

Ponto **antes** deste lote = loja **Live v21.92** (`041e1b5` · WA-CHAT-SNAP).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `041e1b5` (Live v21.92) |
| **Tag (criada no PREP)** | `rollback/pre-lote-checklist-0509h-v21.92` |
| **Branch backup (criada no PREP)** | `producao-backup-pre-v2193-lote-checklist-20260905` |
| **Branch PREP** | `deploy/prep-checklist-0509h` · tip **v21.93** |
| **O quê sobe** | `LANC-PIN-TECLADO` · `WA-TROCAR-FEED` · `WA-TOPBAR-OVERLAY` |
| **O quê NÃO sobe** | merge do `teste` · Excel cadastro · foto/agenda ponte (`WA-PONTE-LEVE` resto) · resto do `teste` |
| **O quê NÃO reverte** | Live v21.92 (CHAT-SNAP) e anteriores |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-lote-checklist-0509h-v21.92
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v21.92**.

## Risco operacional (lojas abertas)

| Pacote | Afeta finalizar venda? | Nota |
| ------ | ---------------------- | ---- |
| `LANC-PIN-TECLADO` | **Não** (venda) | Só Lançamentos / CP / CR / Novo lançamento — PIN vencido abre teclado |
| `WA-TROCAR-FEED` | **Não** (venda) | Só botão Trocar Zap (CSS + aviso) |
| `WA-TOPBAR-OVERLAY` | **Não** (cobrança F7) | Só chrome do Zap no overlay do balcão; `pdv_wizard` / caixa **intocados** |

Rotina no deploy: **pausar vendas ~2 min** · Ctrl+F5 · smoke abaixo.

## Provas no PREP

| Pacote | Prova |
| ------ | ----- |
| `LANC-PIN-TECLADO` | `scripts/verify_lanc_pin_teclado_path.py` **VERIFY_OK 70/70** (PIN 9973) |
| `WA-TROCAR` + `WA-TOPBAR` + CHAT-SNAP preservado | contrato estático **10/10** no PREP |
| Paths venda | `pdv_wizard.js` · `caixa_util.py` · `views_pdv.py` · `index.js` ponte = **iguais** à loja v21.92 |

## Deploy (próximo chat + senha)

**Não** resetar `producao` para o branch `teste`.

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-checklist-0509h
git push origin producao --force-with-lease
```

Render sobe · conferir healthz / badge **v21.93**.

**Smoke:** Ctrl+F5 · badge **v21.93** · PDV F7 · Novo lançamento com PIN velho → teclado · Zap no balcão: **1 barra** com status/Trocar/Bot · se Off, Trocar some.
