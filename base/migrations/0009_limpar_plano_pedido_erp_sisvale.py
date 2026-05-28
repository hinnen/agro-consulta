"""Remove plano SisVale/ID legado da integração ERP — passa a resolver Vendas Pdv no Mongo."""

from django.db import migrations


def limpar_plano_pedido_erp_legado(apps, schema_editor):
    IntegracaoERP = apps.get_model("base", "IntegracaoERP")
    qs = IntegracaoERP.objects.filter(tipo_erp="venda_erp")
    for row in qs.iterator():
        pc = (row.pedido_plano_conta or "").strip()
        pid = (row.pedido_plano_conta_id or "").strip()
        upd = {}
        if pc and ("sisvale" in pc.lower() or pc.startswith("1.1.3")):
            upd["pedido_plano_conta"] = ""
        if pid == "69d2e2d35c5d14cb68c6acef":
            upd["pedido_plano_conta_id"] = ""
        if upd:
            IntegracaoERP.objects.filter(pk=row.pk).update(**upd)


class Migration(migrations.Migration):

    dependencies = [
        ("base", "0008_alter_integracaoerp_pedido_plano_conta"),
    ]

    operations = [
        migrations.RunPython(limpar_plano_pedido_erp_legado, migrations.RunPython.noop),
    ]
