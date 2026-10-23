# Rollback — ETQ-PRESET-SYNC (presets multi-PC v26.04)

Volta a loja ao estado **antes** deste pacote (Live **v26.02**).

| Item | Valor |
| ---- | ----- |
| **Antes (loja)** | `producao` @ `f7ef7844` · VERSION **26.02** |
| **Tag segurança** | `rollback/pre-etq-preset-sync-v26.02` |
| **Branch backup** | `producao-backup-pre-v2604-etq-preset-sync-20261003` |
| **Branch PREP** | `deploy/prep-etq-preset-sync` · alvo loja **v26.04** |
| **Migrate** | **NÃO** |
| **Merge `teste`?** | **NÃO** — só cherry deste PREP |

## Pacote

| # | Pacote | Risco loja aberta |
| - | ------ | ----------------- |
| 1 | **ETQ-PRESET-SYNC** | Baixo — só `/produtos/etiquetas/` (JS sync Postgres + cache-bust). **Não** mexe venda, caixa, Point, NFC-e, financeiro. |

**O quê muda:** arrastar layout / reset / salvar preset → grava na loja; outros PCs puxam do Postgres. Melhora sync; não muda impressão nem fila.

## Como voltar (só frase + senha)

```bash
git fetch origin tag rollback/pre-etq-preset-sync-v26.02
git checkout producao
git reset --hard rollback/pre-etq-preset-sync-v26.02
git push origin producao --force-with-lease
```

## Provas (PREP · PIN 9973)

| Prova | Resultado |
| ----- | --------- |
| ETQ-PRESET-SYNC path | **25/25** |
| Smoke API multi-PC | **23/23** |
| ETQ-53-QUOTA | **34/34** |
| ETQ-53-UX | **32/32** |
| Térmica várias | **39/39** |
| Smoke UX | **22/22** |
