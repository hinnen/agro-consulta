from django.db import migrations


def seed_forward(apps, schema_editor):
    TarefaAgro = apps.get_model("tarefas", "TarefaAgro")
    TarefaEventoAgro = apps.get_model("tarefas", "TarefaEventoAgro")
    seed = [
        ("equipe-centro-vila", "Equipe — Centro e Vila Elias", "Debater sábados, horários e salários dos funcionários novos.", "decidir", "ambas", 10, ""),
        ("logistica-vila-descarregamento", "Logística — Vila Elias", "Garantir estratégia para descarregamento das mercadorias diretamente na Vila Elias.", "decidir", "vila", 20, ""),
        ("racoes-rp-robustus", "Rações / fornecedores", "Debater com o Pai sobre a ração RP Robustus.", "decidir", "geral", 30, ""),
        ("linha-produtos-vila", "Linha de produtos — Vila Elias", "Fazer diferente do Centro: buscar linhas completas de produtos, começando aos poucos. Exemplo: não ter só algumas conexões de encanamento — ir completando até as principais medidas/modelos.", "decidir", "vila", 40, ""),
        ("parafusos-miudezas", "Parafusos e miudezas", "Voltar com as embalagens de parafusos e miudezas. Se sobrar tempo para o Vitor na Vila, ele faz lá; senão, no Centro. Arrumar um balcão no quartinho do Centro exclusivamente para esse trabalho (na Vila já tem).", "decidir", "ambas", 50, "Vitor"),
        ("delivery-catalogo-fotos", "Delivery — catálogo", "Terminar o catálogo de pedidos/delivery. Só falta fotos.", "em_andamento", "geral", 60, ""),
        ("billy-dog-fornecedor", "Billy Dog", "Aguardando fornecedor — vira loja terça ou quarta.", "aguardando", "geral", 70, ""),
        ("guabi-precos", "Guabi", "Aguardando fornecedor se resolver com a questão de preços.", "aguardando", "geral", 80, ""),
    ]
    labels = {
        "decidir": "Decidir",
        "em_andamento": "Já sendo feito",
        "aguardando": "Aguardando terceiros",
    }
    for key, titulo, desc, status, loja, ordem, resp in seed:
        if TarefaAgro.objects.filter(seed_key=key).exists():
            continue
        t = TarefaAgro.objects.create(
            seed_key=key,
            titulo=titulo,
            descricao=desc,
            status=status,
            loja=loja,
            responsavel=resp,
            ordem=ordem,
            criado_por_nome="Sistema",
            atualizado_por_nome="Sistema",
        )
        TarefaEventoAgro.objects.create(
            tarefa=t,
            tipo="criada",
            autor_nome="Sistema",
            detalhe=f"Carga inicial — {labels.get(status, status)}",
        )


def seed_backward(apps, schema_editor):
    TarefaAgro = apps.get_model("tarefas", "TarefaAgro")
    TarefaAgro.objects.filter(
        seed_key__in=[
            "equipe-centro-vila",
            "logistica-vila-descarregamento",
            "racoes-rp-robustus",
            "linha-produtos-vila",
            "parafusos-miudezas",
            "delivery-catalogo-fotos",
            "billy-dog-fornecedor",
            "guabi-precos",
        ]
    ).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("tarefas", "0001_initial"),
    ]

    operations = [
        migrations.RunPython(seed_forward, seed_backward),
    ]
