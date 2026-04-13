# Generated manually for HistoricoTransferencia

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("estoque", "0012_pedidotransferencia_lote_status"),
    ]

    operations = [
        migrations.CreateModel(
            name="HistoricoTransferencia",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("tipo", models.CharField(db_index=True, max_length=32)),
                ("criado_em", models.DateTimeField(auto_now_add=True, db_index=True)),
                ("lote_uuid", models.UUIDField(blank=True, db_index=True, null=True)),
                ("produto_externo_id", models.CharField(blank=True, db_index=True, max_length=100)),
                ("quantidade", models.DecimalField(blank=True, decimal_places=3, max_digits=10, null=True)),
                ("usuario_label", models.CharField(blank=True, max_length=200)),
                ("observacao", models.TextField(blank=True)),
            ],
            options={
                "verbose_name": "Histórico de transferência",
                "verbose_name_plural": "Históricos de transferências",
                "ordering": ["-criado_em", "-id"],
            },
        ),
    ]
