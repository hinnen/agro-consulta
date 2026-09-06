# Bug report + dispositivo loja

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("produtos", "0073_uso_loja_retirada"),
    ]

    operations = [
        migrations.CreateModel(
            name="DispositivoLojaAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("device_id", models.CharField(db_index=True, max_length=64, unique=True)),
                ("nome", models.CharField(blank=True, default="", max_length=80)),
                ("ponto_caixa_ultimo", models.CharField(blank=True, default="", max_length=32)),
                ("user_agent", models.CharField(blank=True, default="", max_length=400)),
                ("tela", models.CharField(blank=True, default="", max_length=40)),
                ("ultimo_visto_em", models.DateTimeField(auto_now=True)),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
            ],
            options={
                "verbose_name": "Dispositivo loja",
                "verbose_name_plural": "Dispositivos loja",
                "ordering": ["-ultimo_visto_em"],
            },
        ),
        migrations.CreateModel(
            name="BugReportAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("o_que_aconteceu", models.TextField()),
                ("o_que_esperava", models.TextField(blank=True, default="")),
                ("usuario_nome", models.CharField(blank=True, default="", max_length=120)),
                ("device_id", models.CharField(blank=True, db_index=True, default="", max_length=64)),
                ("dispositivo_nome", models.CharField(blank=True, default="", max_length=80)),
                ("ponto_caixa", models.CharField(blank=True, default="", max_length=32)),
                ("url_pagina", models.CharField(blank=True, default="", max_length=500)),
                ("versao_app", models.CharField(blank=True, default="", max_length=32)),
                ("user_agent", models.CharField(blank=True, default="", max_length=400)),
                ("tela", models.CharField(blank=True, default="", max_length=40)),
                ("print_base64", models.TextField(blank=True, default="")),
                ("print_mime", models.CharField(blank=True, default="image/jpeg", max_length=40)),
                (
                    "status",
                    models.CharField(
                        choices=[("novo", "Novo"), ("visto", "Visto"), ("feito", "Feito")],
                        db_index=True,
                        default="novo",
                        max_length=16,
                    ),
                ),
                ("notificado_whatsapp", models.BooleanField(default=False)),
                ("notificado_email", models.BooleanField(default=False)),
                ("criado_em", models.DateTimeField(auto_now_add=True, db_index=True)),
                (
                    "usuario",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="bug_reports_agro",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "verbose_name": "Bug report",
                "verbose_name_plural": "Bug reports",
                "ordering": ["-criado_em"],
            },
        ),
    ]
