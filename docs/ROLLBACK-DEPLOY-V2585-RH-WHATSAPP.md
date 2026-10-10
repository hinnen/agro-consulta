# Rollback — lote v25.85 (RH cron + Excel WhatsApp vazio)

Volta a loja **Live v25.84** (estado **antes** deste deploy).

| Item | Valor |
| ---- | ----- |
| **Commit loja (antes)** | `c1034c00` |
| **Tag** | `rollback/pre-deploy-v2584-live-20261001` |
| **Branch backup** | `producao-backup-pre-v2585-rh-whatsapp-20261001` |
| **Pacotes** | `RH-CRON-ENVIO-RENDER` · `CLIENTE-XLSX-WHATSAPP-VAZIO` |
| **Migrate** | **NÃO** |

```bash
git fetch origin tag rollback/pre-deploy-v2584-live-20261001
git checkout producao
git reset --hard rollback/pre-deploy-v2584-live-20261001
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan.
