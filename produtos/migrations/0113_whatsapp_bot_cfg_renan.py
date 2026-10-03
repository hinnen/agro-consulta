# Bot WhatsApp — horário, pausas, boas-vindas e ausência (Renan 01/09)

from django.db import migrations

PATCH = {
    "horario_ativo": True,
    "horario_ini": "08:00",
    "horario_fim": "18:00",
    "horario_dias": [1, 2, 3, 4, 5, 6],
    "msg_fora_horario": (
        "Olá! Agora estamos *fora do horário* (seg–sáb 8h–18h).\n"
        "Deixe sua mensagem que a loja responde no próximo expediente."
    ),
    "ainda_atende_fora": False,
    "atraso_resposta_seg": 2,
    "atraso_entre_msgs_seg": 2,
    "enviar_boas_vindas": True,
    "ausencia_ligada": True,
}


def aplicar_cfg_bot(apps, schema_editor):
    Model = apps.get_model("produtos", "WhatsAppBotConfigAgro")
    for obj in Model.objects.filter(chave="default"):
        dados = dict(obj.dados or {})
        dados.update(PATCH)
        obj.dados = dados
        obj.save(update_fields=["dados"])


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0112_whatsapp_chamar_historico"),
    ]

    operations = [
        migrations.RunPython(aplicar_cfg_bot, migrations.RunPython.noop),
    ]
