# SISTVALE — produto (multi-cliente)

Mapa do **produto SisVale** — vale para **GM Agro** (`banana.md`), **FOOD** (`FOOD.md`) e futuras instâncias.

| Instância | Contexto |
| --------- | -------- |
| **BANANA** / GM Agro | Loja Jacupiranga — `banana.md` |
| **FOOD** | Delivery em branco — `FOOD.md` |

**Stack comum:** Django MPA + Tailwind (CDN) + JavaScript puro + Postgres + Mongo (espelho ERP onde couber).

---

## UI — popups / modais (decisão 08/07)

| Item | Padrão |
| ---- | ------ |
| **Tecnologia** | **`<div>`** no template + **Tailwind** + **JS vanilla** |
| **Abrir/fechar** | `classList` em `hidden` (+ `modal-open` no `body` se travar scroll) |
| **Acessibilidade** | `role="dialog"` · `aria-modal` · foco/Esc no JS da tela |
| **Proibido por padrão** | Bootstrap Modal, SweetAlert, React modal, etc. |
| **`<dialog>` nativo** | **Não** obrigatório em popup novo — avaliado 08/07: sem ganho visível nem de CPU/memória |
| **Regra assistente** | Popup em tela existente → **copiar padrão da tela**; tela **nova do zero** → `<dialog>` só se padronizar a tela inteira |

**Exceção legada:** algumas telas antigas usam CSS próprio (ex. `.sv-modal` em Lançamentos) — mesma lógica (div + JS).

**Referência:** `banana.md` §4.14 · `AGENTS.md` §5 (UX loja).
