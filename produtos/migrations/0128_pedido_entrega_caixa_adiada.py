from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("produtos", "0127_plano_extravio_apos_deposito"),
    ]

    operations = [
        migrations.AddField(
            model_name="pedidoentrega",
            name="caixa_adiada_para",
            field=models.DateField(
                blank=True,
                db_index=True,
                help_text="Até esta data (exclusive) não trava o caixa; no dia, trava de novo.",
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="pedidoentrega",
            name="caixa_adiada_em",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="pedidoentrega",
            name="caixa_adiada_por",
            field=models.CharField(blank=True, default="", max_length=120),
        ),
    ]
