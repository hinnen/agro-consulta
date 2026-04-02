# Generated manually: alinha nome_fantasia ao texto de Empresa no Mongo/ERP (DtoLancamento).

from django.db import migrations


def _only_digits(cnpj: str) -> str:
    return "".join(c for c in (cnpj or "") if c.isdigit())


# CNPJs conforme cadastro ERP (Gm Agropecuaria — lojas Centro e Vila Elias).
CNPJ_PARA_NOME_FANTASIA = {
    "48900774000103": "Agro Mais Centro",
    "03230457000180": "Agro Mais Vila Elias",
}


def forwards(apps, schema_editor):
    Empresa = apps.get_model("base", "Empresa")
    for emp in Empresa.objects.all():
        d = _only_digits(emp.cnpj)
        novo = CNPJ_PARA_NOME_FANTASIA.get(d)
        if novo and emp.nome_fantasia != novo:
            emp.nome_fantasia = novo
            emp.save(update_fields=["nome_fantasia"])

    # Fallback: empresa pk=1 ainda com rótulo genérico (caso CNPJ não estivesse preenchido no Django).
    Empresa.objects.filter(
        pk=1,
        nome_fantasia__iexact="Gm Agro Mais",
    ).update(nome_fantasia="Agro Mais Centro")


def backwards(apps, schema_editor):
    # Não reverte nomes: não há como saber o valor anterior com segurança.
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("base", "0003_integracao_pedido_labels"),
    ]

    operations = [
        migrations.RunPython(forwards, backwards),
    ]
