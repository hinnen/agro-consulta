# Contexto da sessão — Clientes Agro, PDV e deploy

**Branch:** `refactor/fundacao-multiempresa`  
**Estado do Git (última verificação):** working tree clean (alterações já commitadas).

## O que foi implementado

1. **Sincronização Mongo + API ERP → `ClienteAgro`**
   - Serviço: `produtos/services_clientes_sync.py` (`sincronizar_clientes_fontes_para_agro`).
   - Botão POST na tela **Clientes** (`/clientes/`): **Sincronizar ERP / Mongo**.
   - Comando: `python manage.py sincronizar_clientes_agro`.
   - **Não grava no ERP** (só leitura nas fontes).
   - Registros com **`editado_local=True`** não são sobrescritos na sync.

2. **Modelo `ClienteAgro`**
   - `externo_id`, `origem_import`, `editado_local`.
   - Endereço estruturado: `cep`, `uf`, `cidade`, `bairro`, `logradouro`, `numero`, `complemento` + `endereco` (resumo automático).
   - Migrações: `0005_clienteagro_externo_sync`, `0006_clienteagro_endereco_estruturado`.

3. **PDV — lista/busca de clientes só no Agro**
   - `GET /api/listar-clientes/` e `GET /api/buscar-clientes/` usam apenas **`ClienteAgro`** (não Mongo/ERP em tempo real).
   - IDs: origem `mongo` → `local:{pk}` no JSON (evita mandar ObjectId ao ERP); `erp_api` usa `externo_id`.

4. **Mapeamento de dados na importação**
   - CPF/CNPJ: mais chaves + subdocumentos (`_documento_pessoa`).
   - Endereço: projeção Mongo ampliada; ERP usa a mesma lógica de partes; **número** aceita int BSON e separação “Rua X, 123” do logradouro.

5. **Formulário de cliente**
   - `produtos/forms.py` + `cliente_form.html` em grade (CEP, UF, cidade, bairro, logradouro, número, complemento).

## Arquivos centrais

| Área | Arquivos |
|------|----------|
| Sync | `produtos/services_clientes_sync.py`, `produtos/management/commands/sincronizar_clientes_agro.py` |
| Views / APIs | `produtos/views.py` (`clientes_*`, `api_list_customers`, `api_buscar_clientes`, helpers de pessoa/endereço) |
| Model | `produtos/models.py` (`ClienteAgro`, `compor_endereco_resumo_cliente`) |
| URLs | `produtos/urls.py` (`clientes_sincronizar`, etc.) |
| UI | `produtos/templates/produtos/clientes_lista.html`, `cliente_form.html`, trechos em `consulta_produtos.html` |
| Admin | `produtos/admin.py` |

## Deploy (lembrar)

- `python manage.py migrate` (inclui `0005` e `0006`).
- Uso **somente consulta produto/preço/saldo**: não exige usar clientes; APIs de cliente podem retornar lista vazia.
- Produção: `DEBUG=False`, `SECRET_KEY` forte, HTTPS/cookies conforme `manage.py check --deploy`.

## Possíveis próximos passos (se quiser depois)

- Dedupe cliente Mongo vs ERP por CPF.
- Testes automatizados para sync ou APIs de cliente.
- Ajustar nomes de campos se algum JSON real ainda não mapear.

---

*Gerado para não perder o fio da meada ao retomar o trabalho; o código fonte continua sendo a fonte da verdade.*
