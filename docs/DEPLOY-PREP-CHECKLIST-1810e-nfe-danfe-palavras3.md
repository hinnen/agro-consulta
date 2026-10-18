# PREP — **18/10e** · **v27.26.5**

| Pacote | Migrate | Prova |
| ------ | ------- | ----- |
| **NFE-SAIDA-55** (PG **0146** + botões DANFE/XML + layout A4 padrão) | **SIM** (se ainda não rodou **0146**) | `verify_nfe_saida_path.py` **47/47** |
| **CADASTRO-PALAVRAS-PDV-3** | **NÃO** | `verify_cadastro_busca_med_path.py` **53/53** loja |

**Smoke NF-e:** `/nfe/saida/` · Nova · autorizar (PIN) · **Imprimir DANFE** (canhoto + código barras) · **Baixar XML**.  
**Smoke cadastro:** PDV **`2 tempo`** / **`oleo`** após `backfill_busca_palavras_overlay` (uma vez).

**Base git loja:** `producao` @ **`f82f5be1`** · **v27.26**.
