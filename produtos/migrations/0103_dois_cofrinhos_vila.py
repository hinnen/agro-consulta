# Dois cofrinhos Vila: Salário + Vila Elias

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0102_alter_cofrinho_origem_saldo_inicial"),
    ]

    operations = [
        migrations.AddField(
            model_name="repassevilaconfigagro",
            name="saldo_cofre_vila_elias",
            field=models.DecimalField(
                decimal_places=2,
                default=0,
                help_text="Saldo do Cofre Vila Elias (fatia do lucro que não vai ao Centro).",
                max_digits=12,
            ),
        ),
        migrations.AlterField(
            model_name="repassevilaconfigagro",
            name="saldo_reserva_vila",
            field=models.DecimalField(
                decimal_places=2,
                default=0,
                help_text="Saldo do Cofrinho Salário funcionário (reserva diária configurável).",
                max_digits=12,
            ),
        ),
        migrations.AlterField(
            model_name="repassevilacentroagro",
            name="reserva_aplicada",
            field=models.DecimalField(
                decimal_places=2,
                default=0,
                help_text="Cofrinho Salário (config) reservado neste envio — sai do lado que fica na Vila.",
                max_digits=12,
            ),
        ),
        migrations.AlterField(
            model_name="repassevilacentroagro",
            name="lucro_penultimo_dia",
            field=models.DecimalField(
                decimal_places=2,
                default=0,
                help_text="Base do lucro ao Centro após cofres (legado: antes era lucro−reserva).",
                max_digits=12,
            ),
        ),
        migrations.AddField(
            model_name="repassevilareservamovimentoagro",
            name="cofre",
            field=models.CharField(
                choices=[
                    ("salario", "Cofrinho Salário funcionário"),
                    ("vila_elias", "Cofre Vila Elias"),
                ],
                db_index=True,
                default="salario",
                help_text="Qual cofrinho físico este movimento afeta.",
                max_length=16,
            ),
        ),
    ]
