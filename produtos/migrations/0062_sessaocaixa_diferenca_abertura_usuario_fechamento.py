# Diferença na abertura + usuário que fechou

import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("produtos", "0061_pedido_entrega_loja"),
    ]

    operations = [
        migrations.AddField(
            model_name="sessaocaixa",
            name="usuario_fechamento",
            field=models.ForeignKey(
                blank=True,
                help_text="Login Django de quem fechou o turno.",
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="sessoes_caixa_fechadas",
                to=settings.AUTH_USER_MODEL,
            ),
        ),
        migrations.AddField(
            model_name="sessaocaixa",
            name="valor_abertura_sugerido",
            field=models.DecimalField(
                blank=True,
                decimal_places=2,
                help_text="Dinheiro contado no último fechamento do mesmo ponto (sugestão na abertura).",
                max_digits=12,
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="sessaocaixa",
            name="diferenca_abertura",
            field=models.DecimalField(
                blank=True,
                decimal_places=2,
                help_text="valor_abertura − valor_abertura_sugerido (só quando havia sugestão).",
                max_digits=12,
                null=True,
            ),
        ),
        migrations.AlterField(
            model_name="sessaocaixa",
            name="usuario",
            field=models.ForeignKey(
                blank=True,
                help_text="Login Django de quem abriu o turno.",
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="sessoes_caixa",
                to=settings.AUTH_USER_MODEL,
            ),
        ),
    ]
