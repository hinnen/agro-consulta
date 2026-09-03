# Unique wa_id em mensagens WhatsApp (anti-duplicata notify×append / 2 pontes).

from django.db import migrations, models


def _limpar_wa_id_duplicado(apps, schema_editor):
    Msg = apps.get_model("produtos", "WhatsAppMensagemAgro")
    vistos = set()
    # Mais antigas ficam; duplicatas perdem o wa_id (mantém a linha, só quebra o índice).
    for m in Msg.objects.exclude(wa_id="").order_by("id").only("id", "wa_id"):
        wid = (m.wa_id or "").strip()
        if not wid:
            continue
        if wid in vistos:
            Msg.objects.filter(pk=m.pk).update(wa_id="")
        else:
            vistos.add(wid)


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0123_fiado_nota_caixa_conferida"),
    ]

    operations = [
        migrations.RunPython(_limpar_wa_id_duplicado, migrations.RunPython.noop),
        migrations.AddConstraint(
            model_name="whatsappmensagemagro",
            constraint=models.UniqueConstraint(
                condition=models.Q(("wa_id", ""), _negated=True),
                fields=("wa_id",),
                name="wa_msg_wa_id_uniq",
            ),
        ),
    ]
