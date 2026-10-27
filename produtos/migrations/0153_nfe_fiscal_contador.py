from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0152_nfe_fiscal_config"),
    ]

    operations = [
        migrations.AddField(
            model_name="nfealiquotacreditosn",
            name="contador_aprovado_em",
            field=models.DateField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="nfealiquotacreditosn",
            name="contador_crc",
            field=models.CharField(blank=True, default="", max_length=40),
        ),
        migrations.AddField(
            model_name="nfealiquotacreditosn",
            name="contador_nome",
            field=models.CharField(blank=True, default="", max_length=200),
        ),
        migrations.AddField(
            model_name="nfeestabelecimentofiscal",
            name="contador_aprovado_em",
            field=models.DateField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="nfeestabelecimentofiscal",
            name="contador_crc",
            field=models.CharField(blank=True, default="", max_length=40),
        ),
        migrations.AddField(
            model_name="nfeestabelecimentofiscal",
            name="contador_nome",
            field=models.CharField(blank=True, default="", max_length=200),
        ),
        migrations.AddField(
            model_name="nfeexcecaoicmsst",
            name="contador_aprovado_em",
            field=models.DateField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="nfeexcecaoicmsst",
            name="contador_crc",
            field=models.CharField(blank=True, default="", max_length=40),
        ),
        migrations.AddField(
            model_name="nfeexcecaoicmsst",
            name="contador_nome",
            field=models.CharField(blank=True, default="", max_length=200),
        ),
        migrations.AddField(
            model_name="nfeparametroibpt",
            name="contador_aprovado_em",
            field=models.DateField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="nfeparametroibpt",
            name="contador_crc",
            field=models.CharField(blank=True, default="", max_length=40),
        ),
        migrations.AddField(
            model_name="nfeparametroibpt",
            name="contador_nome",
            field=models.CharField(blank=True, default="", max_length=200),
        ),
        migrations.AddField(
            model_name="nfeparametropiscofins",
            name="contador_aprovado_em",
            field=models.DateField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="nfeparametropiscofins",
            name="contador_crc",
            field=models.CharField(blank=True, default="", max_length=40),
        ),
        migrations.AddField(
            model_name="nfeparametropiscofins",
            name="contador_nome",
            field=models.CharField(blank=True, default="", max_length=200),
        ),
        migrations.AddField(
            model_name="nfequestionarioresposta",
            name="contador_aprovado_em",
            field=models.DateField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="nfequestionarioresposta",
            name="contador_crc",
            field=models.CharField(blank=True, default="", max_length=40),
        ),
        migrations.AddField(
            model_name="nfequestionarioresposta",
            name="contador_nome",
            field=models.CharField(blank=True, default="", max_length=200),
        ),
        migrations.AddField(
            model_name="nferegraoperacao",
            name="contador_aprovado_em",
            field=models.DateField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="nferegraoperacao",
            name="contador_crc",
            field=models.CharField(blank=True, default="", max_length=40),
        ),
        migrations.AddField(
            model_name="nferegraoperacao",
            name="contador_nome",
            field=models.CharField(blank=True, default="", max_length=200),
        ),
    ]
