# Merge das folhas 0050 (frete) + 0051 (modelo) — evita Conflicting migrations no Render.

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0050_vendaagro_frete"),
        ("produtos", "0051_produto_modelo"),
    ]

    operations = []
