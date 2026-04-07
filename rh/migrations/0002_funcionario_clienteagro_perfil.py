# Manual migration: perfil RH vinculado a ClienteAgro (pessoa base).

import django.db.models.deletion
from django.db import migrations, models
from django.db.models import Q


def forwards_link_clientes(apps, schema_editor):
    Funcionario = apps.get_model("rh", "Funcionario")
    ClienteAgro = apps.get_model("produtos", "ClienteAgro")
    for row in Funcionario.objects.all().iterator():
        nome_velho = (getattr(row, "nome", "") or "").strip()
        nome_cache_val = (getattr(row, "nome_cache", "") or nome_velho or "Pessoa (legado RH)")[:200]
        ca = None
        if nome_velho:
            ca = ClienteAgro.objects.filter(nome__iexact=nome_velho, ativo=True).order_by("pk").first()
        if ca is None:
            ca = ClienteAgro.objects.create(
                nome=nome_cache_val[:200],
                ativo=True,
                origem_import="rh_legacy",
            )
        row.nome_cache = nome_cache_val
        row.cliente_agro_id = ca.pk
        row.save(update_fields=["nome_cache", "cliente_agro_id"])


def backwards_noop(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("rh", "0001_initial"),
        ("produtos", "0012_venda_estoque_baixa_agro"),
    ]

    operations = [
        migrations.AddField(
            model_name="funcionario",
            name="cliente_agro",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="perfis_rh",
                to="produtos.clienteagro",
                verbose_name="Pessoa base (ClienteAgro)",
            ),
        ),
        migrations.AddField(
            model_name="funcionario",
            name="nome_cache",
            field=models.CharField(default="", max_length=200),
            preserve_default=False,
        ),
        migrations.RenameField(
            model_name="funcionario",
            old_name="apelido",
            new_name="apelido_interno",
        ),
        migrations.RemoveIndex(
            model_name="funcionario",
            name="rh_funciona_empresa_d603e8_idx",
        ),
        migrations.RunPython(forwards_link_clientes, backwards_noop),
        migrations.RemoveField(
            model_name="funcionario",
            name="nome",
        ),
        migrations.RemoveField(
            model_name="funcionario",
            name="cpf",
        ),
        migrations.RemoveField(
            model_name="funcionario",
            name="telefone",
        ),
        migrations.AlterField(
            model_name="funcionario",
            name="cliente_agro",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.PROTECT,
                related_name="perfis_rh",
                to="produtos.clienteagro",
                verbose_name="Pessoa base (ClienteAgro)",
            ),
        ),
        migrations.AlterField(
            model_name="funcionario",
            name="nome_cache",
            field=models.CharField(
                help_text="Cópia para listagens e correspondência com descrições legadas (ex.: caixa). Atualize se o nome no ERP mudar.",
                max_length=200,
            ),
        ),
        migrations.AddIndex(
            model_name="funcionario",
            index=models.Index(fields=["empresa", "ativo", "nome_cache"], name="rh_funciona_empresa_nomec_idx"),
        ),
        migrations.AddConstraint(
            model_name="funcionario",
            constraint=models.UniqueConstraint(
                condition=Q(ativo=True),
                fields=("empresa", "cliente_agro"),
                name="rh_funcionario_unique_cliente_empresa_ativo",
            ),
        ),
        migrations.AlterModelOptions(
            name="fechamentofolhasimplificado",
            options={
                "ordering": ["-competencia", "funcionario__nome_cache"],
                "verbose_name": "Fechamento de folha (simplificado)",
                "verbose_name_plural": "Fechamentos de folha",
            },
        ),
        migrations.AlterModelOptions(
            name="funcionario",
            options={
                "ordering": ["empresa_id", "nome_cache"],
                "verbose_name": "Funcionário (perfil RH)",
                "verbose_name_plural": "Funcionários (perfis RH)",
            },
        ),
    ]
