# Rollback — Hotfixes Repasse 09/09 noite (loja alvo **v23.76**)

Ponto **antes** deste hotfix = loja tip atual (MP-POINT-POLL-RETRY · **v23.73**).

| Item | Valor |
| ---- | ----- |
| **Commit loja antes** | `c7e6fd2` (Live v23.73) |
| **Tag (criar no deploy)** | `rollback/pre-repasse-hotfix-0909-v23.73` |
| **Branch backup** | `producao-backup-pre-v2376-repasse-hotfix-20260909` |
| **Branch PREP** | `deploy/prep-repasse-hotfix-0909` · tip **v23.76** |
| **O quê sobe** | `REPASSE-COFRE-ESTORNO-MOTIVO` · `REPASSE-FUNDO-FECHADO` |
| **O quê NÃO sobe** | merge do `teste` · outros WIP |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-repasse-hotfix-0909-v23.73
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v23.73**.

## Pacotes

| Pacote | O quê |
| ------ | ----- |
| `REPASSE-COFRE-ESTORNO-MOTIVO` | Estornar abre modal de motivo (não usa «Detalhe opcional») |
| `REPASSE-FUNDO-FECHADO` | Fundo troco / prioridade cofres também com caixa fechado (último fechamento) |

## Provas pré-envio

| Prova | Resultado |
| ----- | --------- |
| `verify_repasse_fundo_troco.py` | **61/61** |
| `verify_repasse_cofre_plano_path.py` | **65/65** · PIN 9973=Renan |
| `verify_repasse_gestao_simples_path.py` | **64/64** |
| `verify_repasse_vila_path.py` | **262** OK |
| deep PIN/API (calc + gestão modal + 200/500→Levar 0) | OK |
| `manage.py check` | OK |

## Risco

Baixo — só sugestão/UI do Repasse e modal de estorno. Confirmar transferência continua exigindo caixa aberto.
