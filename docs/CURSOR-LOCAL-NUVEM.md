# Cursor no PC × agente na nuvem — não perder nem desincronizar

O código **único** que importa para os dois lados é o **GitHub**, branch **`teste`**.  
Cursor local, Cursor na nuvem (Cloud Agent) e o site no PC (`127.0.0.1:8000`) só ficam alinhados se o **`teste` remoto** e o **`teste` na pasta do repo** forem a mesma história.

## Regra de ouro

| Quem | Onde grava | O que você faz depois |
|------|------------|------------------------|
| **Você (PC)** | commit + push em **`teste`** | Nada, se já deu push |
| **Agente na nuvem** | branch **`cursor/...`** + PR → **`teste`**, ou push direto em **`teste`** | **Merge do PR** (se existir) + script abaixo |
| **Produção** | só branch **`producao`** | Fora deste doc — ver `docs/DEPLOY-AMBIENTES.md` |

**Nunca** force push (`git push --force`). **Não** use `main`/`principal` para o dia a dia.

## Antes de abrir o Cursor no PC (ou antes de codar)

Na pasta do repo (PowerShell):

```powershell
.\scripts\alinhar-teste.ps1
```

O script:

1. `git fetch origin teste`
2. Mostra se você está **atrás** ou **à frente** do GitHub
3. Se estiver limpo e só atrás → `git pull --rebase origin teste`
4. Mostra **`VERSION`** local vs último commit remoto (referência rápida)

Se tiver alteração local não commitada, o script **não** puxa — commit ou `git stash` primeiro.

## Depois que um agente na nuvem terminou

1. GitHub → **Pull requests** abertos para base **`teste`**
2. Revise e **merge** (ou peça ao agente para integrar em `teste`)
3. No PC: `.\scripts\alinhar-teste.ps1`
4. Reinicie o `runserver` se estiver rodando; **Ctrl+F5** no Chrome

Conferência rápida: arquivo **`VERSION`** no repo = número que o site mostra nos assets (cada commit em `teste`/`producao` sobe a versão pelo hook).

## Evitar conflito clássico

- **OneDrive** na pasta do repo: antes de `pull`, pare o Django; evite dois Cursors editando o mesmo arquivo ao mesmo tempo.
- **Dois chats ao mesmo tempo** (nuvem + PC) no mesmo arquivo → um vai ganhar no Git; prefira **terminar/merge** de um lado antes de editar no outro.
- Trabalho **só seu**, sem agente: ainda assim rode `alinhar-teste.ps1` no início do dia — outro dispositivo ou agente pode ter pushado.

## O que o repositório já faz por você

- **`.cursor/rules/sync-local-nuvem.mdc`** — instruções para **qualquer** Cursor (PC ou nuvem): puxar `teste` antes de editar; integrar trabalho em `origin/teste`.
- **`docs/TESTE-LOCAL.md`** — subir o site no PC; fluxo com assistente (§5).
- **Hook `.githooks`** — bump de `VERSION` em commit em `teste`/`producao` (ativar uma vez: `git config core.hooksPath .githooks`).

## Frases úteis no chat

- *«Alinhei o teste no PC, pode continuar»* — agente pode assumir que você está em cima do remoto.
- *«Estou com WIP local em X, não mexe em X»* — evita colisão.
- *«Integra em teste e avisa»* — agente na nuvem deve deixar **`teste`** atualizado, não só branch solta.
