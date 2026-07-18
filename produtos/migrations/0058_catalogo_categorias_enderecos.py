# Generated manually — catálogo delivery: categorias + 2 endereços

from django.db import migrations, models
import django.db.models.deletion


def seed_categorias(apps, schema_editor):
    Cat = apps.get_model("produtos", "CatalogoDeliveryCategoria")
    if Cat.objects.exists():
        return
    # Padrão apps de ração: espécie → faixa etária / uso
    raiz = [
        ("caes", "Cães", 10),
        ("gatos", "Gatos", 20),
        ("aves", "Aves", 30),
        ("peixes", "Peixes", 40),
        ("outros", "Outros", 90),
    ]
    subs = {
        "caes": [("adulto", "Adulto", 1), ("filhote", "Filhote", 2), ("racas-pequenas", "Raças pequenas", 3)],
        "gatos": [("adulto", "Adulto", 1), ("filhote", "Filhote", 2)],
        "aves": [("poedeiras", "Poedeiras", 1), ("corte", "Corte", 2), ("pet", "Pet", 3)],
    }
    for slug, nome, ordem in raiz:
        pai = Cat.objects.create(nome=nome, slug=slug, ordem=ordem, ativo=True, parent=None)
        for sslug, snome, sordem in subs.get(slug, []):
            Cat.objects.create(
                nome=snome,
                slug=f"{slug}-{sslug}",
                ordem=sordem,
                ativo=True,
                parent=pai,
            )


def migrate_endereco_legado(apps, schema_editor):
    Cfg = apps.get_model("produtos", "CatalogoDeliveryConfig")
    for cfg in Cfg.objects.all():
        legado = (getattr(cfg, "endereco_loja", None) or "").strip()
        if legado and not (cfg.endereco_loja_1 or "").strip():
            cfg.endereco_loja_1 = legado
            if not (cfg.rotulo_loja_1 or "").strip():
                cfg.rotulo_loja_1 = "Centro"
            cfg.save(update_fields=["endereco_loja_1", "rotulo_loja_1"])


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0057_catalogo_delivery"),
    ]

    operations = [
        migrations.AddField(
            model_name="catalogodeliveryconfig",
            name="rotulo_loja_1",
            field=models.CharField(blank=True, default="Centro", max_length=80),
        ),
        migrations.AddField(
            model_name="catalogodeliveryconfig",
            name="endereco_loja_1",
            field=models.CharField(blank=True, default="", max_length=320),
        ),
        migrations.AddField(
            model_name="catalogodeliveryconfig",
            name="rotulo_loja_2",
            field=models.CharField(blank=True, default="Vila Elias", max_length=80),
        ),
        migrations.AddField(
            model_name="catalogodeliveryconfig",
            name="endereco_loja_2",
            field=models.CharField(blank=True, default="", max_length=320),
        ),
        migrations.AlterField(
            model_name="catalogodeliveryconfig",
            name="endereco_loja",
            field=models.CharField(
                blank=True,
                default="",
                help_text="Legado — preferir endereço 1 / 2 abaixo.",
                max_length=320,
            ),
        ),
        migrations.CreateModel(
            name="CatalogoDeliveryCategoria",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("nome", models.CharField(max_length=80)),
                ("slug", models.SlugField(max_length=90, unique=True)),
                ("ordem", models.PositiveIntegerField(default=0)),
                ("ativo", models.BooleanField(default=True)),
                (
                    "parent",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="filhos",
                        to="produtos.catalogodeliverycategoria",
                        verbose_name="Categoria pai (se for subcategoria)",
                    ),
                ),
            ],
            options={
                "verbose_name": "Categoria catálogo delivery",
                "verbose_name_plural": "Categorias catálogo delivery",
                "ordering": ["ordem", "nome"],
            },
        ),
        migrations.RunPython(migrate_endereco_legado, migrations.RunPython.noop),
        migrations.RunPython(seed_categorias, migrations.RunPython.noop),
    ]
