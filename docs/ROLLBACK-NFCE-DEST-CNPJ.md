# Rollback — NFC-e destinatário CNPJ (`NFCE-DEST-CNPJ` · v17.81)

Ponto **antes** deste pacote = loja **Live v17.80** (PDV-BALANCA-USE-P2).

| Item | Valor |
| ---- | ----- |
| **Commit** | `12b59342` (Live v17.80) |
| **Tag** | `rollback/pre-nfce-dest-cnpj-v17.80` |
| **Branch backup** | `producao-backup-pre-v1781-nfce-dest-cnpj-20260823` |
| **O quê reverte** | dest CNPJ no cupom fiscal (modal PDV, XML `dest/CNPJ`, cadastro, reemissão) |
| **O quê NÃO reverte** | balança USE-P2 (continua) · cupons já autorizados |
| **Migrate** | `0099` só aumenta `dest_cpf` 11→14. Reverter **código** sem desfazer o migrate é **seguro** (coluna 14 ainda cabe CPF de 11). |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-nfce-dest-cnpj-v17.80
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v17.80**.
