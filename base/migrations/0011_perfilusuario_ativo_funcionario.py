from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("base", "0010_alter_integracaoerp_pedido_plano_conta"),
        ("rh", "0006_funcionario_dias_cp_auto"),
    ]

    operations = [
        migrations.AddField(
            model_name="perfilusuario",
            name="ativo",
            field=models.BooleanField(
                default=True,
                help_text="False = saiu / não aparece no PIN nem valida no caixa.",
                verbose_name="Ativo no caixa",
            ),
        ),
        migrations.AddField(
            model_name="perfilusuario",
            name="funcionario",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="perfis_pin",
                to="rh.funcionario",
                verbose_name="Funcionário RH",
            ),
        ),
        migrations.AlterModelOptions(
            name="perfilusuario",
            options={
                "verbose_name": "Perfil de operador (PIN)",
                "verbose_name_plural": "Perfis de operador (PIN)",
            },
        ),
    ]
