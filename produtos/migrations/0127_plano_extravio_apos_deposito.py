from django.db import migrations


def forwards(apps, schema_editor):
    PlanoContaAgro = apps.get_model("produtos", "PlanoContaAgro")
    obj, created = PlanoContaAgro.objects.get_or_create(
        nome="Extravio após Depósito",
        defaults={
            "tipo": "outra",
            "grupo": "Sócio",
            "observacao": (
                "Dinheiro saiu no depósito e não foi para boleto/banco — "
                "não reduz lucro operacional; mesma regra da retirada de sócio."
            ),
            "ativo": True,
            "exibir_pdv": True,
        },
    )
    if not created:
        PlanoContaAgro.objects.filter(pk=obj.pk).update(
            tipo="outra",
            grupo="Sócio",
            ativo=True,
            exibir_pdv=True,
        )


def backwards(apps, schema_editor):
    PlanoContaAgro = apps.get_model("produtos", "PlanoContaAgro")
    PlanoContaAgro.objects.filter(nome="Extravio após Depósito").delete()


class Migration(migrations.Migration):
    dependencies = [
        ("produtos", "0126_whatsapp_conversa_arquivada"),
    ]

    operations = [
        migrations.RunPython(forwards, backwards),
    ]
