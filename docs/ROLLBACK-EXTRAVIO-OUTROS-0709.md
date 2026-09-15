# Rollback — EXTRAVIO-APOS-DEPOSITO + BI-ROTULO-OUTROS (loja alvo **v23.32**)

Ponto **antes** deste pacote = loja **Live v23.25** (`849f9e0` / código `677d22e`).

| Item | Valor |
| ---- | ----- |
| **Commit loja antes do deploy** | tip `origin/producao` @ **v23.25** |
| **Tag** | `rollback/pre-extravio-outros-v23.25` |
| **Branch backup** | `producao-backup-pre-v2332-extravio-20260907` |
| **Branch PREP** | `deploy/prep-extravio-0709` · tip **v23.32** |
| **O quê sobe** | `BI-ROTULO-OUTROS` + `EXTRAVIO-APOS-DEPOSITO` (plano · Mini DRE · checkbox Dinheiro baixa CP · saída Extravio sem 2ª retirada) |
| **O quê NÃO sobe** | merge do `teste` · resto WhatsApp/PDV |
| **Migrate** | **SIM** `produtos.0127` (seed plano Extravio) |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-extravio-outros-v23.25
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v23.25**.

## Risco operacional

| Pacote | Afeta finalizar venda? | Nota |
| ------ | ---------------------- | ---- |
| `BI-ROTULO-OUTROS` | **Não** | Só rótulo no Resumo |
| `EXTRAVIO-APOS-DEPOSITO` | **Não** | Financeiro/caixa saída · não corta lucro operacional |

## Depois do deploy (você)

1. **Ctrl+F5** · badge **v23.32**
2. Resumo → donut **OUTROS** · Mini DRE linha **Extravio após Depósito**
3. Caixa → plano Extravio (sem retirar gaveta de novo)
4. CP → baixa Dinheiro → checkbox retirada PDV **desligado** por padrão
