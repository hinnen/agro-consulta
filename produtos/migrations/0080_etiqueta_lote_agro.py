# Generated manually for EtiquetaLoteAgro (lote A4 gôndola provisório)

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0079_etiqueta_preset_agro"),
    ]

    operations = [
        migrations.CreateModel(
            name="EtiquetaLoteAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("nome", models.CharField(blank=True, default="", max_length=160)),
                ("loja", models.CharField(blank=True, default="vila", max_length=16)),
                ("filtros_json", models.JSONField(blank=True, default=dict)),
                ("preset_id", models.CharField(blank=True, default="gondola", max_length=64)),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("aberto", "Aberto"),
                            ("concluido", "Concluído"),
                            ("cancelado", "Cancelado"),
                        ],
                        db_index=True,
                        default="aberto",
                        max_length=16,
                    ),
                ),
                ("itens_json", models.JSONField(blank=True, default=list)),
                ("cursor", models.PositiveIntegerField(default=0)),
                ("ultima_folha_qtd", models.PositiveSmallIntegerField(default=0)),
                ("usuario", models.CharField(blank=True, default="", max_length=150)),
                ("criado_em", models.DateTimeField(auto_now_add=True, db_index=True)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name": "Lote etiquetas A4",
                "verbose_name_plural": "Lotes etiquetas A4",
                "ordering": ["-criado_em"],
            },
        ),
        migrations.AddIndex(
            model_name="etiquetaloteagro",
            index=models.Index(fields=["status", "-criado_em"], name="etq_lote_status_criado_idx"),
        ),
    ]
