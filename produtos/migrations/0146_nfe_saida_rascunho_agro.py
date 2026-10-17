from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0145_clienteagro_desconto_pdv_percentual"),
    ]

    operations = [
        migrations.CreateModel(
            name="NfeSaidaRascunhoAgro",
            fields=[
                (
                    "doc_id",
                    models.CharField(max_length=24, primary_key=True, serialize=False),
                ),
                (
                    "status",
                    models.CharField(db_index=True, default="rascunho", max_length=40),
                ),
                (
                    "venda_agro_id",
                    models.IntegerField(blank=True, db_index=True, null=True),
                ),
                ("usuario", models.CharField(blank=True, default="", max_length=200)),
                ("corpo", models.JSONField(blank=True, default=dict)),
                ("criado_em", models.DateTimeField(db_index=True)),
                ("atualizado_em", models.DateTimeField(db_index=True)),
            ],
            options={
                "verbose_name": "Rascunho NF-e saída Agro",
                "verbose_name_plural": "Rascunhos NF-e saída Agro",
                "ordering": ["-atualizado_em"],
                "indexes": [
                    models.Index(
                        fields=["status", "-atualizado_em"],
                        name="produtos_nf_status__a1b2c3_idx",
                    ),
                    models.Index(
                        fields=["venda_agro_id", "status", "-atualizado_em"],
                        name="produtos_nf_venda_a_d4e5f6_idx",
                    ),
                ],
            },
        ),
    ]
