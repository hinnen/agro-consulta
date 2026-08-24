# Rollback — Excel ↓ últimos 3 fornecedores (`CAD-XLSX-ULT-FORN` · v18.02)

Ponto **antes** deste pacote = loja **Live v17.84** (CAIXA-DEVOL-DINHEIRO-MP).

| Item | Valor |
| ---- | ----- |
| **Commit base** | `da7c1cb` (Live v17.84) |
| **Tag** | `rollback/pre-cad-xlsx-ult-forn-v17.84` |
| **Branch backup** | `producao-backup-pre-v1802-cad-xlsx-ult-forn-20260824` |
| **O quê reverte** | Colunas opcionais Últ. / 2º / 3º fornecedor no Excel ↓ do cadastro |
| **O quê NÃO reverte** | Devolução dinheiro × MP (v17.84) · balança · NFC-e dest CNPJ |
| **Migrate** | **NÃO** — reverter código é seguro |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-cad-xlsx-ult-forn-v17.84
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v17.84**. Badge da loja volta a **v17.84**.
