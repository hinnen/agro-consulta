# Generated manually — cadastro oficial planos CP + seed CSV

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


def seed_planos(apps, schema_editor):
    # Usar modelo histórico (sem exibir_pdv etc.) — util atual quebra migrate em banco novo.
    import csv
    from pathlib import Path

    Plano = apps.get_model("produtos", "PlanoContaAgro")
    Alias = apps.get_model("produtos", "PlanoContaAliasAgro")
    base = Path(__file__).resolve().parents[2] / "dados"
    niveis = base / "planos_conta_niveis.csv"
    if not niveis.is_file():
        niveis = Path(__file__).resolve().parents[1] / "dados" / "planos_conta_niveis.csv"
    # Fallback: util se CSV existir no path canônico
    try:
        from produtos.plano_conta_agro_util import _csv_niveis_path, _csv_mapa_path, _tipo_from_csv

        niveis = _csv_niveis_path()
        mapa = _csv_mapa_path()
    except Exception:
        mapa = None
        def _tipo_from_csv(v):
            t = (v or "").strip().lower()
            if t.startswith("fix"):
                return "fixa"
            if t.startswith("var"):
                return "variavel"
            return "outra"

    if niveis.is_file():
        with niveis.open(encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f, delimiter=";"):
                nome = (row.get("Plano oficial") or row.get("plano oficial") or "").strip()
                if not nome:
                    continue
                tipo = _tipo_from_csv(row.get("Tipo") or row.get("tipo") or "")
                grupo = (row.get("Grupo") or row.get("grupo") or "").strip()[:120]
                obs = (
                    row.get("Observação")
                    or row.get("Observacao")
                    or row.get("observação")
                    or ""
                ).strip()[:400]
                Plano.objects.get_or_create(
                    nome=nome,
                    defaults={"tipo": tipo, "grupo": grupo, "observacao": obs, "ativo": True},
                )

    if mapa is not None and getattr(mapa, "is_file", lambda: False)():
        with mapa.open(encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f, delimiter=";")
            cols = {(c or "").strip().lower(): c for c in (reader.fieldnames or [])}
            k_ant = cols.get("nome antigo (como está no cp)") or cols.get("nome antigo")
            k_ofi = cols.get("nome oficial")
            if k_ant and k_ofi:
                for row in reader:
                    ant = (row.get(k_ant) or "").strip()
                    ofi = (row.get(k_ofi) or "").strip()
                    if not ant or not ofi:
                        continue
                    plano = Plano.objects.filter(nome=ofi).first()
                    if not plano:
                        continue
                    Alias.objects.get_or_create(grafia=ant[:200], defaults={"plano": plano})


def noop_reverse(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("produtos", "0064_dispenser_a6_biblioteca_compartilhada"),
    ]

    operations = [
        migrations.CreateModel(
            name="PlanoContaAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("nome", models.CharField(db_index=True, max_length=200, unique=True)),
                (
                    "tipo",
                    models.CharField(
                        blank=True,
                        choices=[("fixa", "Fixa"), ("variavel", "Variável"), ("outra", "Outra")],
                        default="outra",
                        max_length=16,
                    ),
                ),
                ("grupo", models.CharField(blank=True, default="", max_length=120)),
                ("observacao", models.CharField(blank=True, default="", max_length=400)),
                ("ativo", models.BooleanField(db_index=True, default=True)),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name": "Plano de conta Agro",
                "verbose_name_plural": "Planos de conta Agro",
                "ordering": ["nome"],
            },
        ),
        migrations.CreateModel(
            name="PlanoContaAliasAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("grafia", models.CharField(db_index=True, max_length=200, unique=True)),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                (
                    "criado_por",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="plano_conta_aliases",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
                (
                    "plano",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="aliases",
                        to="produtos.planocontaagro",
                    ),
                ),
            ],
            options={
                "verbose_name": "Alias plano de conta",
                "verbose_name_plural": "Aliases plano de conta",
                "ordering": ["grafia"],
            },
        ),
        migrations.RunPython(seed_planos, noop_reverse),
    ]
