# Repasse Vila — saldo compartilhado e razão do cofrinho/reserva física

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("produtos", "0099_nfce_dest_cpf_cnpj"),
    ]

    operations = [
        migrations.AddField(
            model_name="repassevilaconfigagro",
            name="saldo_reserva_vila",
            field=models.DecimalField(decimal_places=2, default=0, help_text="Saldo acumulado do cofrinho/reserva física da Vila Elias.", max_digits=12),
        ),
        migrations.CreateModel(
            name="RepasseVilaReservaMovimentoAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("tipo", models.CharField(choices=[("separacao", "Separação"), ("retirada", "Retirada / uso"), ("ajuste", "Ajuste"), ("estorno", "Estorno")], db_index=True, max_length=16)),
                ("origem", models.CharField(choices=[("fechamento_caixa", "Fechamento de caixa"), ("repasse", "Repasse Vila → Centro"), ("lancamento_separado", "Lançamento separado"), ("ajuste_manual", "Ajuste manual"), ("estorno", "Estorno")], db_index=True, max_length=24)),
                ("criado_em", models.DateTimeField(auto_now_add=True, db_index=True)),
                ("data_ref", models.DateField(db_index=True)),
                ("valor", models.DecimalField(decimal_places=2, help_text="Variação assinada: entrada positiva; retirada negativa.", max_digits=12)),
                ("saldo_anterior", models.DecimalField(decimal_places=2, default=0, max_digits=12)),
                ("saldo_posterior", models.DecimalField(decimal_places=2, default=0, max_digits=12)),
                ("operador", models.CharField(max_length=120)),
                ("observacao", models.CharField(blank=True, default="", max_length=500)),
                ("idempotencia_chave", models.CharField(max_length=160, unique=True)),
                ("detalhe", models.JSONField(blank=True, default=dict)),
                ("estornado_de", models.OneToOneField(blank=True, null=True, on_delete=django.db.models.deletion.PROTECT, related_name="estorno_movimento", to="produtos.repassevilareservamovimentoagro")),
                ("movimento_caixa", models.OneToOneField(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="movimento_reserva_vila", to="produtos.movimentocaixa")),
                ("repasse", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="movimentos_cofrinho", to="produtos.repassevilacentroagro")),
                ("sessao_caixa", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="movimentos_reserva_vila", to="produtos.sessaocaixa")),
                ("usuario", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="movimentos_reserva_vila", to=settings.AUTH_USER_MODEL)),
            ],
            options={"verbose_name": "Repasse Vila · movimento do cofrinho", "verbose_name_plural": "Repasse Vila · movimentos do cofrinho", "ordering": ["-criado_em", "-pk"]},
        ),
        migrations.AddIndex(
            model_name="repassevilareservamovimentoagro",
            index=models.Index(fields=["data_ref", "tipo"], name="rv_res_data_tipo_idx"),
        ),
    ]
