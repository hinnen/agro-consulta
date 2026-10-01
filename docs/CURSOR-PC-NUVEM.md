# Cursor no PC ↔ Agente na nuvem — não se perder

Dois lugares podem editar o mesmo repo: **Cursor no seu computador** e **Cloud Agent** (agente na nuvem do Cursor). O código e o contexto só ficam alinhados se todo mundo seguir a mesma fila.

## Fonte da verdade (ordem)

1. **GitHub, branch `teste`** — código entregue (backup se o PC estragar).
2. **`banana.md` → seção CHECKPOINT** — o que mudou, versão, o que está só no teste vs loja, WIP.
3. **Chat do Cursor** — **não** é memória; cada chat novo começa do zero.

Regras duras de produção, push e senha continuam no **topo do `banana.md`**.

## Você (PC) — antes de abrir um chat no Cursor

1. Feche entregas pela metade ou faça **commit** / **stash** do que estiver solto.
2. Alinhe com o remoto:

   **PowerShell (Windows):**

   ```powershell
   cd C:\Users\RenanHinnen\OneDrive\Documentos\GitHub\agro-consulta
   .\scripts\alinhar_cursor_teste.ps1
   ```

   **Linux / Mac / nuvem:**

   ```bash
   ./scripts/alinhar_cursor_teste.sh
   ```

3. No chat: descreva a tarefa ou `@banana-roteiro`. Se retomar trabalho da nuvem, diga: *«continua o pacote X do CHECKPOINT»*.
4. **Ctrl+F5** no Chrome depois de `git pull` se for testar telas.

## Assistente (qualquer Cursor — PC ou nuvem)

| Momento | O quê |
| -------- | ----- |
| **Início do chat** | Ler `banana-roteiro.md` → CHECKPOINT (grep no `banana.md`) → **perguntar** se Renan subiu algo fora do Git desde o último CHECKPOINT |
| **Durante** | Trabalhar na branch combinada; nuvem usa `cursor/…-9476` + PR → `teste` quando for entrega fechada |
| **Fechar entrega** | Código na **`teste`** (merge do PR ou push direto) + **`git push origin teste`** + linha no **CHECKPOINT** (o quê, commits, `VERSION`) |
| **Produção** | **Nunca** sem frase + senha no **mesmo** pedido do Renan (topo do `banana.md`) |

**Proibido assumir:** «último commit deste chat» = estado do PC do Renan ou = produção.

## Cenários comuns

### Nuvem entregou; você vai continuar no PC

1. `alinhar_cursor_teste` (pull).
2. Ler o pacote novo no CHECKPOINT.
3. Testar local (`docs/TESTE-LOCAL.md`).

### Você editou no PC; nuvem rodou ao mesmo tempo

1. No PC: commit na `teste` e push **ou** stash → pull → reaplicar.
2. Se o Git pedir merge, resolver conflitos; **priorizar** CHECKPOINT + intenção do pacote mais recente.
3. No próximo chat (nuvem ou PC): mencionar *«resolvi conflito em …»*.

### Só conversa / dúvida, sem código

Não precisa pull; se for retomar **código** de outro chat, pull + CHECKPOINT.

## O que **não** sincroniza sozinho

- Abas abertas, rascunho do PDV, `localStorage` — ficam no browser daquele PC.
- `.env` — **não** vai pro Git; cada máquina tem o seu.
- Branch `producao` — loja; só muda com deploy autorizado.

## Referência rápida

| Arquivo | Papel |
| -------- | ----- |
| `banana-roteiro.md` | O que ler em cada tarefa |
| `banana.md` CHECKPOINT | Memória viva entre chats |
| `.cursor/rules/agro-consulta.mdc` | Regras auto-carregadas no Cursor |
| `docs/TESTE-LOCAL.md` | Provar no PC antes de produção |
