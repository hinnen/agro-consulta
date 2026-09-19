# Conferência da caixinha fiado no Fechar caixa (Postgres).

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0122_whatsapp_apagar_msg"),
    ]

    operations = [
        migrations.AddField(
            model_name="vendaagro",
            name="fiado_nota_caixa_conferida_em",
            field=models.DateTimeField(
                blank=True,
                db_index=True,
                help_text="Fechar caixa: nota fiado conferida na caixinha (não pede de novo neste turno).",
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="fiadobaixaagro",
            name="fiado_nota_caixa_conferida_em",
            field=models.DateTimeField(
                blank=True,
                db_index=True,
                help_text="Fechar caixa: nota paga conferida (retirada da caixinha).",
                null=True,
            ),
        ),
    ]
