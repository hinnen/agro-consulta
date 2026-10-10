# Lista: verde (nova) · laranja (leu, sem resposta) · neutro (respondeu / ✓).

from django.db import migrations, models


def _backfill(apps, schema_editor):
    Conversa = apps.get_model("produtos", "WhatsAppConversaAgro")
    Msg = apps.get_model("produtos", "WhatsAppMensagemAgro")
    for conv in Conversa.objects.all().iterator():
        ult = (
            Msg.objects.filter(conversa_id=conv.pk)
            .order_by("-criado_em", "-id")
            .values_list("direcao", flat=True)
            .first()
        )
        if ult == "in":
            Conversa.objects.filter(pk=conv.pk).update(aguardando_loja=True)


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0117_whatsapp_aviso_fora_em"),
    ]

    operations = [
        migrations.AddField(
            model_name="whatsappconversaagro",
            name="aguardando_loja",
            field=models.BooleanField(
                default=False,
                help_text="Cliente falou por último e a loja ainda não respondeu / não concluiu.",
            ),
        ),
        migrations.RunPython(_backfill, migrations.RunPython.noop),
    ]
