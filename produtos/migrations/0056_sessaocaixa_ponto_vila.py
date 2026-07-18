# Generated manually — ponto_caixa Vila Elias

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0055_vendaagro_deposito"),
    ]

    operations = [
        migrations.AlterField(
            model_name="sessaocaixa",
            name="ponto_caixa",
            field=models.CharField(
                choices=[
                    ("gaveta", "Caixa Gaveta (Centro)"),
                    ("vila", "Caixa Vila Elias"),
                    ("notebook", "Caixa Notebook"),
                    ("teste", "Caixa Teste"),
                ],
                db_index=True,
                default="gaveta",
                help_text="Ponto físico do turno: gaveta Centro, Vila Elias, notebook (satélite) ou teste.",
                max_length=16,
            ),
        ),
    ]
