import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("base", "0009_limpar_plano_pedido_erp_sisvale"),
        ("rh", "0007_ajustemanualfolha"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="DecimoTerceiroFuncionario",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("ano", models.PositiveSmallIntegerField()),
                (
                    "parcela",
                    models.CharField(
                        choices=[
                            ("PRIMEIRA", "1ª parcela"),
                            ("SEGUNDA", "2ª parcela"),
                            ("UNICA", "Parcela única"),
                        ],
                        max_length=20,
                    ),
                ),
                ("valor", models.DecimalField(decimal_places=2, max_digits=12)),
                ("data_vencimento", models.DateField()),
                ("observacao", models.TextField(blank=True)),
                ("mongo_lancamento_id", models.CharField(blank=True, max_length=32)),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
                (
                    "criado_por",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="decimos_terceiros_criados",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
                (
                    "empresa",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="decimos_terceiros",
                        to="base.empresa",
                    ),
                ),
                (
                    "funcionario",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="decimos_terceiros",
                        to="rh.funcionario",
                    ),
                ),
            ],
            options={
                "verbose_name": "Décimo terceiro",
                "verbose_name_plural": "Décimos terceiros",
                "ordering": ["-ano", "parcela", "-id"],
            },
        ),
        migrations.AddConstraint(
            model_name="decimoterceirofuncionario",
            constraint=models.UniqueConstraint(
                fields=("funcionario", "ano", "parcela"),
                name="rh_decimo_unico_parcela_ano",
            ),
        ),
    ]
