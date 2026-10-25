# NF-e saída 55 — Reforma Tributária (visão 2026, Simples Nacional)

## Situação do emissor (branch `teste`, out/2026)

- XML gerado: **NF-e 4.00**, CRT **1** (Simples Nacional), grupos **ICMS SN**, **PIS/COFINS** conforme cadastro.
- Pacote XSD: **PL 010f** (campos de IBS/CBS presentes no leiaute, **opcionais** até a vigência aplicável ao emitente).
- O motor **não** calcula nem envia IBS/CBS, `cClassTrib`, alíquotas ou créditos da reforma sem parametrização explícita.

## Dependências de decisão contábil / fiscal

| Tema | Decisão necessária | Variável / implementação futura |
|------|-------------------|--------------------------------|
| Obrigatoriedade IBS/CBS para SN | Data e anexo da NT 2025.002 por estabelecimento | `NFE_SAIDA_RT_HABILITAR` (não definida) |
| Classificação tributária (`cClassTrib`) | Tabela oficial por operação/NCM | Cadastro produto + motor `nfe_saida_tributacao_util` |
| Alíquotas IBS/CBS | Não inventar — tabelas federais/UF/município | Integração tabela + vigência |
| `cMunFGIBS` | Município do fato gerador | Endereço emitente/entrega conforme operação |
| Convivência com PIS/COFINS/ICMS | Período de transição | Regra por `dhEmi` e regime |

## Compatibilidade atual

- Validação XSD **PL 010f** garante que o XML **atual** (sem grupos RT) continua **estruturalmente válido** enquanto os campos RT forem opcionais no leiaute.
- Quando o contador exigir RT: estender `apurar_nfe_saida` e `_montar_xml_nfe_saida` com regras versionadas por data — **sem** alterar notas já autorizadas.

## Referências

- NT 2025.002 e atualizações (Receita Federal / Comitê Gestor IBS).
- Portal Nacional NF-e — Notas Técnicas e pacote de schemas PL 010.
