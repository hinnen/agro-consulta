# Decisão de revisão de limite no laboratório. CreateModel only.

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0140_contabancarialojaagro"),
    ]

    operations = [
        migrations.CreateModel(
            name="CreditoLimiteRevisaoDecisaoAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("acao", models.CharField(choices=[("APROVADO", "Aprovado"), ("IGNORADO", "Ignorado")], db_index=True, max_length=16)),
                ("limite_anterior", models.DecimalField(decimal_places=2, default=0, max_digits=12)),
                ("limite_aplicado", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("usuario", models.CharField(blank=True, default="", max_length=150)),
                ("criado_em", models.DateTimeField(auto_now_add=True, db_index=True)),
                (
                    "analise",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="decisoes_limite",
                        to="produtos.clienteanalisecreditoagro",
                    ),
                ),
                (
                    "cliente",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="revisoes_limite_credito",
                        to="produtos.clienteagro",
                    ),
                ),
            ],
            options={
                "verbose_name": "Revisão de limite (crédito)",
            },
        ),
        migrations.AddConstraint(
            model_name="creditolimiterevisaodecisaoagro",
            constraint=models.UniqueConstraint(fields=("analise",), name="credito_limite_revisao_analise_uniq"),
        ),
    ]
