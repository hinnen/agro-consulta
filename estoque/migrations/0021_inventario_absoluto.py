from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("estoque", "0020_solicitacao_item_quantidade_pedida"),
    ]

    operations = [
        migrations.AddField(
            model_name="contagemciclicasessao",
            name="modo",
            field=models.CharField(
                db_index=True,
                default="ciclica",
                help_text="ciclica = bate divergência e recontagem. inventario = conta absoluta, sem passagem 2.",
                max_length=20,
            ),
        ),
        migrations.CreateModel(
            name="ContagemCiclicaAtribuicao",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("pin", models.CharField(db_index=True, max_length=32)),
                ("operador_rotulo", models.CharField(blank=True, default="", max_length=120)),
                ("entrar_direto", models.BooleanField(default=True)),
                (
                    "sessao",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="atribuicoes",
                        to="estoque.contagemciclicasessao",
                    ),
                ),
            ],
            options={
                "constraints": [
                    models.UniqueConstraint(fields=("sessao", "pin"), name="uniq_ciclica_sessao_pin")
                ],
            },
        ),
        migrations.CreateModel(
            name="ContagemCiclicaBloco",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("operador_rotulo", models.CharField(blank=True, default="", max_length=120)),
                ("fechado_em", models.DateTimeField(auto_now_add=True)),
                ("encerrou", models.BooleanField(default=False)),
                ("itens_gravados", models.PositiveIntegerField(default=0)),
                ("amostra_json", models.TextField(blank=True, default="[]")),
                (
                    "sessao",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="blocos",
                        to="estoque.contagemciclicasessao",
                    ),
                ),
            ],
        ),
    ]
