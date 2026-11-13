# PREP 13/11e — SistApp fala com o Zap online · v27.26.63

Ainda **não** foi para a loja.

A versão .58 que aparece no aplicativo do Zap é outro sistema. A loja estava chamando um endereço desligado. Agora chama `https://zap.zapsistvale.online`.

O Zap .58 também precisa da portinha nova (`480715c5`). Sem isso a lista continua vazia.

Base `eb34cf2a`. Sem migrate. Volta: `git push origin eb34cf2aeef01b228483a1e97b5e455eaeed656d:producao`.
