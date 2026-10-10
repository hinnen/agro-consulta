# Campo «mostrar no PDV» + pré-marca os planos que já estavam na saída do caixa.

from django.db import migrations, models


# Nomes oficiais (PlanoContaAgro) que já aparecem em saida_caixa_planos.py
NOMES_PDV_ATUAL = (
    "Adiantamento de Salário (Vale)",
    "Salários",
    "Alimentação",
    "Brindes e ações festivas",
    "Combustível Strada",
    "Combustível Demais Carros",
    "Compra Mercadoria SN",
    "Embalagens",
    "Material de Limpeza e Conservação",
    "Matérias de Escritório",
    "Matérias de Informática",
    "Retiradas Geraldinho",
    "Retiradas Geraldo",
    "Outros (verificar)",
)


def seed_exibir_pdv(apps, schema_editor):
    Plano = apps.get_model("produtos", "PlanoContaAgro")
    Plano.objects.filter(nome__in=NOMES_PDV_ATUAL).update(exibir_pdv=True)


def unseed_exibir_pdv(apps, schema_editor):
    Plano = apps.get_model("produtos", "PlanoContaAgro")
    Plano.objects.filter(nome__in=NOMES_PDV_ATUAL).update(exibir_pdv=False)


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0083_dfe_manifestacao_ciencia"),
    ]

    operations = [
        migrations.AddField(
            model_name="planocontaagro",
            name="exibir_pdv",
            field=models.BooleanField(
                db_index=True,
                default=False,
                help_text="Se marcado, aparece no select de plano da saída/retirada do caixa.",
                verbose_name="Mostrar no PDV",
            ),
        ),
        migrations.RunPython(seed_exibir_pdv, unseed_exibir_pdv),
    ]
