# PREP deploy — NF-EAN-OPCIONAL · alvo **v26.80**

**Status:** 🟢 **pronto para envio à produção**  
**Não sobe sozinho.** Só com frase explícita + senha `99738595` na mesma mensagem.

## Conteúdo

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **NF-EAN-OPCIONAL** | path + Django **20/20** | **NÃO** |

**O quê:** Entrada NF casa EAN do XML com `codigos_barras_opcionais`. EAN principal (PG/overlay) tem prioridade. Duplicidade não vincula.

**Tip PREP:** `deploy/prep-nf-ean-opcional` · base Live **v26.79**

## Na senha

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-nf-ean-opcional
git push origin producao
```

Smoke: Entrada NF · item cujo EAN está só nos opcionais → vínculo `ean_overlay_opcional` · EAN principal continua ganhando · EAN em 2 produtos → sem vínculo automático.

## Voltar (só frase + senha)

Ver `docs/ROLLBACK-NF-EAN-OPCIONAL.md`.
