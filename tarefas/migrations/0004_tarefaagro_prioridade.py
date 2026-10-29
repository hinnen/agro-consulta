from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("tarefas", "0003_status_adiado_perm_cancelado"),
    ]

    operations = [
        migrations.AddField(
            model_name="tarefaagro",
            name="prioridade",
            field=models.CharField(
                choices=[("alta", "Alta"), ("media", "Média"), ("baixa", "Baixa")],
                db_index=True,
                default="media",
                max_length=8,
            ),
        ),
    ]
