"""Tela «sem acesso» — Vendas / Tarefas (mesmo PIN do hub, checagem só ao entrar)."""

from __future__ import annotations

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.urls import reverse

from produtos.vendas_lojas_hub_acesso_util import mensagem_sem_acesso_modulo
from produtos.vendas_lojas_hub_sessao_util import operador_da_sessao


def resposta_sem_acesso_html(request: HttpRequest, modulo: str) -> HttpResponse:
    modulo = (modulo or "vendas").strip().lower()
    if modulo not in ("vendas", "tarefas"):
        modulo = "vendas"
    label = "Vendas" if modulo == "vendas" else "Tarefas"
    return render(
        request,
        "produtos/vendas_lojas_sem_acesso.html",
        {
            "modulo": modulo,
            "modulo_label": label,
            "operador": operador_da_sessao(request),
            "mensagem": mensagem_sem_acesso_modulo(modulo),
            "hub_url": reverse("vendas_lojas_hub"),
        },
    )
