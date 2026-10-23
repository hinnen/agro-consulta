# Generated manually for etiqueta print history

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('produtos', '0035_produto_catalogo_agro_pg'),
    ]

    operations = [
        migrations.CreateModel(
            name='EtiquetaImpressaoHistoricoAgro',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('criado_em', models.DateTimeField(auto_now_add=True, db_index=True)),
                ('usuario', models.CharField(blank=True, default='', max_length=150)),
                ('origem', models.CharField(blank=True, default='fila', max_length=32)),
                ('preset_id', models.CharField(blank=True, default='', max_length=64)),
                ('preset_nome', models.CharField(blank=True, default='', max_length=120)),
                ('texto_rodape', models.CharField(blank=True, default='', max_length=120)),
                ('total_etiquetas', models.PositiveIntegerField(default=0)),
                ('qtd_linhas', models.PositiveSmallIntegerField(default=0)),
                ('resumo_nomes', models.CharField(blank=True, default='', max_length=400)),
                ('itens_json', models.JSONField(default=list)),
            ],
            options={
                'verbose_name': 'Histórico impressão etiqueta',
                'verbose_name_plural': 'Históricos impressão etiquetas',
                'ordering': ['-criado_em'],
                'indexes': [
                    models.Index(fields=['-criado_em'], name='etq_hist_criado_idx'),
                ],
            },
        ),
    ]
