# Estoque: fonte da verdade no Agro

## Regra de produto

- **Operação no chão (PDV, transferência, conferência):** usa-se **somente o saldo exibido no Agro**.
- **ERP → Agro:** todos os movimentos de estoque que o ERP registra devem **chegar ao Mongo** (espelho lido pelo Agro). O Agro **não** envia movimentos de estoque de volta ao ERP.
- **Camada Agro:** ajustes locais (`AjusteRapidoEstoque`) somam-se ao espelho do Mongo com a mesma fórmula já usada no PDV (referência ERP na hora do ajuste + saldo informado).

## Onde está no código

- Mongo: `DtoEstoqueDepositoProduto` / `DtoProduto` — cliente `VendaERPMongoClient` (`integracoes/venda_erp_mongo.py`).
- Saldos finais por produto: `_mapa_saldos_finais_por_produtos` em `produtos/views.py`.
- Ajustes: modelo `AjusteRapidoEstoque` com `origem` (`OrigemAjusteEstoque`) e opcionalmente `usuario`.
- Saúde da leitura: modelo `EstoqueSyncHealth` (singleton) + `estoque/sync_health.py`.

## Painel e APIs

- Página: `/estoque/sincronizacao/` (login).
- `GET /api/estoque/sync-health/` — último ping, versão do catálogo, alertas.
- `GET /api/estoque/divergencia-ajustes/` — último ajuste por produto/depósito (auditoria da camada Agro).
- **Automático (recomendado):** no Render, o Blueprint inclui um **Cron Job** `agro-estoque-mongo-ping` (a cada 10 min) que roda `python manage.py estoque_mongo_ping` — só um `find_one` no Mongo e grava sucesso/erro em `EstoqueSyncHealth`; **não** invalida cache do PDV. Anexe ao cron o **mesmo Environment Group** do serviço web.
- Alternativa HTTP: `GET /api/cron/estoque-mongo-ping/` — mesmo token que `api/cron/enviar-alerta-vendas-dia` (`ALERTA_VENDAS_CRON_TOKEN`), útil se o agendador externo não tiver o repositório Django.

## Reconciliação

- Comando: `python manage.py reconciliar_estoque_agro` — invalida cache do catálogo PDV e força rebuild na próxima leitura.

## Tipos de movimento ERP (referência)

Lista lógica em `estoque/erp_movimentos_registry.py` (`TIPOS_MOVIMENTO_ERP_ESPERADOS`). O nome interno no ERP pode variar; o importante é que **cada movimento** que altera estoque no ERP **reproduza** o efeito no Mongo.
