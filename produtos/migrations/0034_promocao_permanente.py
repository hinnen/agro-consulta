from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0033_promocao_agro"),
    ]

    operations = [
        migrations.AddField(
            model_name="promocaoagro",
            name="permanente",
            field=models.BooleanField(
                default=False,
                help_text="Sem data de encerramento; vale a partir do início enquanto estiver ativa.",
                verbose_name="Válida permanentemente",
            ),
        ),
        migrations.AlterField(
            model_name="promocaoagro",
            name="data_fim",
            field=models.DateField(blank=True, null=True, verbose_name="Fim"),
        ),
    ]
