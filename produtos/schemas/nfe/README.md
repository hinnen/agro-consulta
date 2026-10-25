# Schemas NF-e — Pacote PL 010f (layout 4.00)

## Vigência

- **Pacote:** PL **010f** (espelho **PL_010_V1.30**, publicação alinhada a **31/08/2026** no [Portal Nacional da NF-e](https://www.nfe.fazenda.gov.br/portal/listaConteudo.aspx?tipoConteudo=BMPFMBoln3w=)).
- **Diretório:** `produtos/schemas/nfe/PL_010f/` (212 arquivos `.xsd` com dependências `include`/`import`).

## Origem e atualização

Os arquivos foram obtidos do pacote oficial de esquemas (layout PL 010), via espelho versionado `nfephp-org/sped-nfe` → `schemes/PL_010_V1.30`, equivalente ao conjunto distribuído pelo Portal (inclui `DFeTiposBasicos_v1.00.xsd` e grupos da Reforma Tributária no leiaute).

Para republicar a partir do Portal:

1. Baixar o ZIP **PL_010** vigente no Portal Nacional.
2. Extrair para `produtos/schemas/nfe/PL_010f/` substituindo os `.xsd`.
3. Rodar `python manage.py test produtos.tests_nfe_saida_util.NfeSaidaXsdValidationTests`.

Script auxiliar: `scripts/sync_nfe_xsd_pl010f.sh` (download do espelho GitHub quando o Portal estiver indisponível).

## Validação no emissor

- Entrada: `PL_010f/nfe_v4.00.xsd`
- Utilitário: `produtos/nfe_saida_xml_validate_util.py`
- **Antes da assinatura:** XML montado + `Signature` estrutural mínima (só para XSD).
- **Depois da assinatura:** XML assinado real (sem stub).

## Reforma Tributária (IBS/CBS)

O leiaute PL 010f **define** grupos/campos de IBS/CBS. O emissor NF-e 55 do Agro Consulta, em **2026**, **não preenche** IBS/CBS automaticamente — depende de parametrização contábil, cronograma SN (obrigatoriedade a partir de **01/01/2027** na NT 2025.002) e feature flag futura. Ver `docs/NFE_SAIDA_REFORMA_TRIBUTARIA_2026.md`.
