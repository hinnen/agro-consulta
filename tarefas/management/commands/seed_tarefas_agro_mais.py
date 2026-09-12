"""Carga inicial — plano de ação Agro Mais (idempotente por seed_key)."""

from django.core.management.base import BaseCommand
from django.db import transaction

from tarefas.models import TarefaAgro, TarefaEventoAgro
from tarefas.services import STATUS_LABEL

SEED = [
    {
        "seed_key": "equipe-centro-vila",
        "titulo": "Equipe — Centro e Vila Elias",
        "descricao": "Debater sábados, horários e salários dos funcionários novos.",
        "status": TarefaAgro.Status.DECIDIR,
        "loja": TarefaAgro.Loja.AMBAS,
        "ordem": 10,
    },
    {
        "seed_key": "logistica-vila-descarregamento",
        "titulo": "Logística — Vila Elias",
        "descricao": "Garantir estratégia para descarregamento das mercadorias diretamente na Vila Elias.",
        "status": TarefaAgro.Status.DECIDIR,
        "loja": TarefaAgro.Loja.VILA,
        "ordem": 20,
    },
    {
        "seed_key": "racoes-rp-robustus",
        "titulo": "Rações / fornecedores",
        "descricao": "Debater com o Pai sobre a ração RP Robustus.",
        "status": TarefaAgro.Status.DECIDIR,
        "loja": TarefaAgro.Loja.GERAL,
        "ordem": 30,
    },
    {
        "seed_key": "linha-produtos-vila",
        "titulo": "Linha de produtos — Vila Elias",
        "descricao": (
            "Fazer diferente do Centro: buscar linhas completas de produtos, começando aos poucos. "
            "Exemplo: não ter só algumas conexões de encanamento — ir completando até as principais medidas/modelos."
        ),
        "status": TarefaAgro.Status.DECIDIR,
        "loja": TarefaAgro.Loja.VILA,
        "ordem": 40,
    },
    {
        "seed_key": "parafusos-miudezas",
        "titulo": "Parafusos e miudezas",
        "descricao": (
            "Voltar com as embalagens de parafusos e miudezas. "
            "Se sobrar tempo para o Vitor na Vila, ele faz lá; senão, no Centro. "
            "Arrumar um balcão no quartinho do Centro exclusivamente para esse trabalho (na Vila já tem)."
        ),
        "status": TarefaAgro.Status.DECIDIR,
        "loja": TarefaAgro.Loja.AMBAS,
        "ordem": 50,
        "responsavel": "Vitor",
    },
    {
        "seed_key": "delivery-catalogo-fotos",
        "titulo": "Delivery — catálogo",
        "descricao": "Terminar o catálogo de pedidos/delivery. Só falta fotos.",
        "status": TarefaAgro.Status.EM_ANDAMENTO,
        "loja": TarefaAgro.Loja.GERAL,
        "ordem": 60,
    },
    {
        "seed_key": "billy-dog-fornecedor",
        "titulo": "Billy Dog",
        "descricao": "Aguardando fornecedor — vira loja terça ou quarta.",
        "status": TarefaAgro.Status.AGUARDANDO,
        "loja": TarefaAgro.Loja.GERAL,
        "ordem": 70,
    },
    {
        "seed_key": "guabi-precos",
        "titulo": "Guabi",
        "descricao": "Aguardando fornecedor se resolver com a questão de preços.",
        "status": TarefaAgro.Status.AGUARDANDO,
        "loja": TarefaAgro.Loja.GERAL,
        "ordem": 80,
    },
]


class Command(BaseCommand):
    help = "Cria as pendências iniciais do plano Agro Mais (não duplica seed_key)."

    def handle(self, *args, **options):
        criadas = 0
        existentes = 0
        with transaction.atomic():
            for item in SEED:
                key = item["seed_key"]
                if TarefaAgro.objects.filter(seed_key=key).exists():
                    existentes += 1
                    continue
                t = TarefaAgro.objects.create(
                    seed_key=key,
                    titulo=item["titulo"],
                    descricao=item.get("descricao") or "",
                    status=item["status"],
                    loja=item.get("loja") or TarefaAgro.Loja.GERAL,
                    responsavel=item.get("responsavel") or "",
                    ordem=int(item.get("ordem") or 0),
                    criado_por_nome="Sistema",
                    atualizado_por_nome="Sistema",
                )
                TarefaEventoAgro.objects.create(
                    tarefa=t,
                    tipo=TarefaEventoAgro.Tipo.CRIADA,
                    autor_nome="Sistema",
                    detalhe=f"Carga inicial — {STATUS_LABEL.get(t.status, t.status)}",
                )
                criadas += 1
        self.stdout.write(
            self.style.SUCCESS(f"Seed OK: {criadas} criadas, {existentes} já existiam.")
        )
