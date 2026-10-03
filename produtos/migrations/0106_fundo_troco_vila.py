from decimal import Decimal

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0105_chat_loja_mensagem"),
    ]

    operations = [
        migrations.AddField(
            model_name="repassevilaconfigagro",
            name="fundo_troco_vila",
            field=models.DecimalField(
                decimal_places=2,
                default=Decimal("500.00"),
                help_text=(
                    "Alvo de dinheiro que deve ficar na gaveta da Vila após o repasse (troco). "
                    "Só aviso — não bloqueia. Prioridade: Salário → Vila Elias → Centro."
                ),
                max_digits=12,
            ),
        ),
    ]
