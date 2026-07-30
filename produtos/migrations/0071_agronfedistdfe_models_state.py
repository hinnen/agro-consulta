# Tabelas já existem no SQLite/Postgres local (migrate antigo).
# Esta migração só registra os models no estado Django.

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0069_entrada_nfe_vinculo_agro"),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            state_operations=[
                migrations.CreateModel(
                    name="AgroNfeDistDfeCursor",
                    fields=[
                        ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                        ("cnpj", models.CharField(db_index=True, max_length=14, unique=True)),
                        ("ult_nsu", models.CharField(default="000000000000000", max_length=15)),
                        ("atualizado_em", models.DateTimeField(auto_now=True)),
                    ],
                    options={
                        "verbose_name": "Cursor Dist DF-e",
                        "verbose_name_plural": "Cursores Dist DF-e",
                    },
                ),
                migrations.CreateModel(
                    name="AgroNfeDistDfeDocumento",
                    fields=[
                        ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                        ("cnpj", models.CharField(db_index=True, max_length=14)),
                        ("chave", models.CharField(db_index=True, max_length=44)),
                        ("nsu", models.CharField(blank=True, db_index=True, default="", max_length=15)),
                        ("schema", models.CharField(choices=[("nfe", "NF-e completa"), ("resumo", "Resumo"), ("outro", "Outro")], db_index=True, default="nfe", max_length=16)),
                        ("xml", models.TextField(blank=True, default="")),
                        ("emit_nome", models.CharField(blank=True, default="", max_length=300)),
                        ("numero", models.CharField(blank=True, default="", max_length=20)),
                        ("valor_total", models.DecimalField(decimal_places=2, default=0, max_digits=14)),
                        ("dh_emi", models.CharField(blank=True, default="", max_length=40)),
                        ("status", models.CharField(choices=[("pendente", "Pendente"), ("carregada", "Carregada na grade"), ("processada", "Entrada concluída"), ("ignorada", "Ignorada")], db_index=True, default="pendente", max_length=16)),
                        ("rascunho_id", models.CharField(blank=True, default="", max_length=64)),
                        ("criado_em", models.DateTimeField(auto_now_add=True, db_index=True)),
                        ("atualizado_em", models.DateTimeField(auto_now=True)),
                    ],
                    options={
                        "verbose_name": "Documento Dist DF-e",
                        "verbose_name_plural": "Documentos Dist DF-e",
                    },
                ),
                migrations.AddConstraint(
                    model_name="agronfedistdfedocumento",
                    constraint=models.UniqueConstraint(fields=("cnpj", "chave"), name="uniq_dfe_doc_cnpj_chave"),
                ),
                migrations.AddIndex(
                    model_name="agronfedistdfedocumento",
                    index=models.Index(fields=["cnpj", "status", "-criado_em"], name="produtos_ag_cnpj_dfe_idx"),
                ),
            ],
            database_operations=[],
        ),
    ]
