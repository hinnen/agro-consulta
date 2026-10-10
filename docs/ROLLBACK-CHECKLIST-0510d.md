# Rollback — Checklist 05/10d (CREDITO-SCORE-TRAVAS + ETQ-PONTE-1CLIQUE)

Volta a loja ao estado **antes** deste lote (Live **v26.40**).

**Não** rodar sem frase explícita + senha de produção do Renan.

## Comandos

```bash
git fetch origin tag rollback/pre-checklist-0510d-v26.40
git checkout producao
git reset --hard rollback/pre-checklist-0510d-v26.40
git push origin producao --force-with-lease
```

Alternativa: `git reset --hard origin/producao-backup-pre-v2646-checklist-0510d` (mesmo tip).

## O que volta

- Score shadow sem travas `shadow_v1_1` (volta lógica anterior do lab)
- ZIP da ponte sem layout CLIQUE-AQUI / Node embutido

## O que NÃO mexe

- PDV venda · caixa · Point · NFC-e · financeiro
- Mapa Elgin (já Live v26.40 — permanece)
