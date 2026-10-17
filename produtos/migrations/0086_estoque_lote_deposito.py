from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0085_plano_conta_exibir_pdv"),
    ]

    operations = [
        migrations.AddField(
            model_name="estoquelote",
            name="deposito",
            field=models.CharField(
                blank=True,
                db_index=True,
                default="",
                help_text="centro | vila — loja onde a entrada NF lançou o estoque; vazio = não definido.",
                max_length=16,
                verbose_name="Depósito / loja",
            ),
        ),
    ]
