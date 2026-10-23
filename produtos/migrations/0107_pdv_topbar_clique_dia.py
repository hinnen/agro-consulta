# Contagem diária de cliques da topbar PDV (quente / frio)

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0106_fundo_troco_vila"),
    ]

    operations = [
        migrations.CreateModel(
            name="PdvTopbarCliqueDiaAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("botao", models.CharField(db_index=True, max_length=40)),
                ("deposito", models.CharField(blank=True, db_index=True, default="", max_length=16)),
                ("data", models.DateField(db_index=True)),
                ("cliques", models.PositiveIntegerField(default=0)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name": "Clique topbar PDV (dia)",
                "verbose_name_plural": "Cliques topbar PDV (dia)",
            },
        ),
        migrations.AddConstraint(
            model_name="pdvtopbarcliquediaagro",
            constraint=models.UniqueConstraint(
                fields=("botao", "deposito", "data"),
                name="pdv_topbar_clique_botao_dep_dia_uq",
            ),
        ),
        migrations.AddIndex(
            model_name="pdvtopbarcliquediaagro",
            index=models.Index(fields=["data", "botao"], name="pdv_topbar_clique_data_btn_idx"),
        ),
    ]
