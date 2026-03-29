from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0005_clienteagro_externo_sync"),
    ]

    operations = [
        migrations.AddField(
            model_name="clienteagro",
            name="bairro",
            field=models.CharField(blank=True, default="", max_length=120, verbose_name="Bairro"),
        ),
        migrations.AddField(
            model_name="clienteagro",
            name="cep",
            field=models.CharField(blank=True, default="", max_length=12, verbose_name="CEP"),
        ),
        migrations.AddField(
            model_name="clienteagro",
            name="cidade",
            field=models.CharField(blank=True, default="", max_length=120, verbose_name="Cidade"),
        ),
        migrations.AddField(
            model_name="clienteagro",
            name="complemento",
            field=models.CharField(blank=True, default="", max_length=200, verbose_name="Complemento"),
        ),
        migrations.AddField(
            model_name="clienteagro",
            name="logradouro",
            field=models.CharField(blank=True, default="", max_length=300, verbose_name="Logradouro"),
        ),
        migrations.AddField(
            model_name="clienteagro",
            name="numero",
            field=models.CharField(blank=True, default="", max_length=30, verbose_name="Número"),
        ),
        migrations.AddField(
            model_name="clienteagro",
            name="uf",
            field=models.CharField(blank=True, default="", max_length=2, verbose_name="UF"),
        ),
        migrations.AlterField(
            model_name="clienteagro",
            name="endereco",
            field=models.CharField(
                blank=True,
                default="",
                help_text="Preenchido automaticamente a partir dos campos abaixo quando existirem.",
                max_length=500,
                verbose_name="Endereço (resumo)",
            ),
        ),
        migrations.AlterField(
            model_name="clienteagro",
            name="editado_local",
            field=models.BooleanField(
                default=False,
                help_text="Se verdadeiro, sincronização não sobrescreve dados do cliente (incl. endereço).",
                verbose_name="Editado no Agro",
            ),
        ),
    ]
