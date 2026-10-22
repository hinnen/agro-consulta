from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0149_fornecedor_entrada_nfe_alias_agro"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="PdvRapidoSlotAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("chave", models.CharField(db_index=True, default="default", max_length=32)),
                ("slot", models.PositiveSmallIntegerField()),
                ("produto_id", models.CharField(blank=True, default="", max_length=64)),
                ("apelido", models.CharField(blank=True, default="", max_length=40)),
                ("foto", models.BinaryField(blank=True, null=True)),
                ("foto_mime", models.CharField(blank=True, default="", max_length=40)),
                ("foto_versao", models.PositiveIntegerField(default=0)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
                (
                    "atualizado_por",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="pdv_rapido_slots",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "verbose_name": "Slot rápido PDV",
                "verbose_name_plural": "Slots rápidos PDV",
            },
        ),
        migrations.AddConstraint(
            model_name="pdvrapidoslotagro",
            constraint=models.UniqueConstraint(
                fields=("chave", "slot"),
                name="pdv_rapido_slot_chave_slot_uq",
            ),
        ),
        migrations.AddIndex(
            model_name="pdvrapidoslotagro",
            index=models.Index(fields=["chave", "slot"], name="pdv_rapido_chave_slot_idx"),
        ),
    ]
