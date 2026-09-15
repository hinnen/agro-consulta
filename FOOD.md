# FOOD — SisVale delivery (instância em branco)

**FOOD** = instância **delivery** do SisVale (repo `hinnen/food`, espelho do código GM). Chat: **`@FOOD`** + este arquivo.

| Nome | O quê |
| ---- | ----- |
| **SISTVALE** | Produto |
| **BANANA** | GM Agro (loja Jacupiranga) — `banana.md` |
| **FOOD** | Delivery em branco |

**GM Agro:** deploy/dados **inalterados** — ver CHECKPOINT «PRODUTO — FOOD» no `banana.md`.

---

## UI — popups / modais (herda SisVale · 08/07)

Mesma regra do produto — ver **`SISTVALE.md`** (seção popups).

| Item | FOOD |
| ---- | ---- |
| **Padrão** | `<div>` + Tailwind + JS (`hidden`) |
| **`<dialog>` nativo** | **Não** por padrão — só se a tela FOOD for **nova do zero** e padronizar tudo em `<dialog>` |
| **Espelho GM** | Ao portar tela da GM → **manter** o padrão `div` da tela de origem |

**Referência GM:** `banana.md` §4.14.
