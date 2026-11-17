from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0155_pdv_operacao_log_agro"),
    ]

    operations = [
        migrations.CreateModel(
            name="ClienteCartaoAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("codigo", models.CharField(db_index=True, max_length=13, unique=True)),
                ("token", models.CharField(db_index=True, max_length=64, unique=True)),
                ("ativo", models.BooleanField(db_index=True, default=True)),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                ("vinculado_em", models.DateTimeField(blank=True, null=True)),
                ("codigo_whatsapp_hash", models.CharField(blank=True, default="", max_length=64)),
                ("codigo_whatsapp_expira", models.DateTimeField(blank=True, null=True)),
                (
                    "cliente",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="cartoes",
                        to="produtos.clienteagro",
                    ),
                ),
            ],
            options={
                "verbose_name": "Cartão do cliente",
                "verbose_name_plural": "Cartões do cliente",
                "ordering": ["-criado_em"],
            },
        ),
    ]
