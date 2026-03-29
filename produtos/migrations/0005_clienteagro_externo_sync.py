from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0004_cliente_caixa_venda"),
    ]

    operations = [
        migrations.AddField(
            model_name="clienteagro",
            name="externo_id",
            field=models.CharField(
                blank=True,
                db_index=True,
                default="",
                help_text="Chave da fonte; vazio = cadastro manual só no Agro.",
                max_length=80,
                verbose_name="ID externo (Mongo/ERP)",
            ),
        ),
        migrations.AddField(
            model_name="clienteagro",
            name="origem_import",
            field=models.CharField(
                blank=True,
                default="",
                help_text="mongo, erp_api ou vazio (manual).",
                max_length=20,
                verbose_name="Origem da importação",
            ),
        ),
        migrations.AddField(
            model_name="clienteagro",
            name="editado_local",
            field=models.BooleanField(
                default=False,
                help_text="Se verdadeiro, sincronização não sobrescreve nome/CPF/WhatsApp.",
                verbose_name="Editado no Agro",
            ),
        ),
        migrations.AddConstraint(
            model_name="clienteagro",
            constraint=models.UniqueConstraint(
                condition=models.Q(externo_id__gt=""),
                fields=("externo_id",),
                name="unique_clienteagro_externo_id_quando_preenchido",
            ),
        ),
    ]
