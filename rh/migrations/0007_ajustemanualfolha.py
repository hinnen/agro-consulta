from decimal import Decimal

from django.db import migrations, models
import django.db.models.deletion


def copiar_totais_para_linhas(apps, schema_editor):
    Fechamento = apps.get_model("rh", "FechamentoFolhaSimplificado")
    Ajuste = apps.get_model("rh", "AjusteManualFolha")
    zero = Decimal("0")
    for f in Fechamento.objects.all().iterator():
        if Ajuste.objects.filter(fechamento_id=f.id).exists():
            if not f.ajustes_manuais_inicializados:
                f.ajustes_manuais_inicializados = True
                f.save(update_fields=["ajustes_manuais_inicializados"])
            continue
        ordem = 0
        prov = f.outros_proventos if f.outros_proventos is not None else zero
        desc = f.outros_descontos if f.outros_descontos is not None else zero
        pares = []
        if prov > zero:
            pares.append(("ACRESCIMO", "Outros proventos", prov))
        elif prov < zero:
            pares.append(("DESCONTO", "Outros proventos", abs(prov)))
        if desc > zero:
            pares.append(("DESCONTO", "Outros descontos", desc))
        elif desc < zero:
            pares.append(("ACRESCIMO", "Outros descontos", abs(desc)))
        for tipo, descricao, valor in pares:
            Ajuste.objects.create(
                fechamento_id=f.id,
                tipo=tipo,
                descricao=descricao,
                valor=valor,
                ordem=ordem,
            )
            ordem += 1
        f.ajustes_manuais_inicializados = True
        f.save(update_fields=["ajustes_manuais_inicializados"])


class Migration(migrations.Migration):

    dependencies = [
        ("rh", "0006_funcionario_dias_cp_auto"),
    ]

    operations = [
        migrations.CreateModel(
            name="AjusteManualFolha",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("tipo", models.CharField(choices=[("DESCONTO", "Desconto"), ("ACRESCIMO", "Acréscimo")], max_length=20)),
                ("descricao", models.CharField(max_length=200, verbose_name="Nome na folha")),
                ("valor", models.DecimalField(decimal_places=2, max_digits=12, verbose_name="Valor")),
                ("ordem", models.PositiveSmallIntegerField(default=0)),
                (
                    "fechamento",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="ajustes_manuais",
                        to="rh.fechamentofolhasimplificado",
                    ),
                ),
            ],
            options={
                "verbose_name": "Ajuste manual da folha",
                "verbose_name_plural": "Ajustes manuais da folha",
                "ordering": ["ordem", "id"],
            },
        ),
        migrations.AddField(
            model_name="fechamentofolhasimplificado",
            name="ajustes_manuais_inicializados",
            field=models.BooleanField(
                default=False,
                help_text="Depois da cópia dos totais antigos para linhas, o recálculo soma só as linhas. Enquanto for falso, total sem linha não é zerado.",
            ),
        ),
        migrations.RunPython(copiar_totais_para_linhas, migrations.RunPython.noop),
    ]
