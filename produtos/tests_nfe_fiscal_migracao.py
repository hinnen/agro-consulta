"""Etapa 2 — migração 0152 em Postgres descartável e diagnóstico da 0039. Sem produção."""
from __future__ import annotations

import os
import subprocess
import textwrap
import uuid
from pathlib import Path

from django.test import SimpleTestCase

REPO = Path(__file__).resolve().parents[1]

_APPLY_0152 = textwrap.dedent(
    """
    import os
    import django
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    django.setup()
    from django.core.management import call_command
    from django.db import connection
    from django.db.migrations.executor import MigrationExecutor

    call_command("migrate", "contenttypes", verbosity=0, skip_checks=True)
    call_command("migrate", "auth", verbosity=0, skip_checks=True)

    executor = MigrationExecutor(connection)
    state = executor.loader.project_state()
    for name in ("0152_nfe_fiscal_config", "0153_nfe_fiscal_contador"):
        mig = executor.loader.disk_migrations[("produtos", name)]
        with connection.schema_editor() as editor:
            state = mig.apply(state, editor)
        executor.recorder.record_applied("produtos", name)
    print("OK")
    """
).strip()


class NfeFiscalMigracao0039DiagnosticoTests(SimpleTestCase):
    def test_0039_sqlite_usa_get_model_fora_do_state(self):
        src = (REPO / "produtos" / "migrations" / "0039_nfce_documento_agro.py").read_text(encoding="utf-8")
        self.assertIn("SeparateDatabaseAndState", src)
        self.assertIn('apps.get_model("produtos", "NfceNumeracaoAgro")', src)
        self.assertIn('vendor == "postgresql"', src)
        self.assertIn("# SQLite / dev local", src)
        # RunPython vê o from_state; CreateModel está só em state_operations → LookupError no SQLite.
        # Postgres não chama get_model (SQL cru). Não reescrever 0039 nesta revisão.
        # A cadeia completa até 0152 também esbarra em seeds históricos (ex.: 0065 PlanoConta).


class NfeFiscalMigracao0152PostgresTests(SimpleTestCase):
    def test_0152_cria_tabelas_em_postgres_descartavel(self):
        db = f"nfe_e2_{uuid.uuid4().hex[:10]}"
        created = subprocess.run(
            ["createdb", db],
            capture_output=True,
            text=True,
        )
        if created.returncode != 0:
            self.skipTest("PostgreSQL local indisponível: " + (created.stderr or created.stdout))
        env = os.environ.copy()
        env["DATABASE_URL"] = f"postgres://ubuntu@/{db}?host=/var/run/postgresql"
        env["DJANGO_SETTINGS_MODULE"] = "config.settings"
        env.setdefault("SECRET_KEY", "nfe-fiscal-mig-test")
        py = "/workspace/.venv/bin/python"
        if not Path(py).exists():
            py = "python3"
        try:
            applied = subprocess.run(
                [py, "-c", _APPLY_0152],
                cwd=str(REPO),
                env=env,
                capture_output=True,
                text=True,
            )
            self.assertEqual(applied.returncode, 0, applied.stdout + "\n" + applied.stderr)
            self.assertIn("OK", applied.stdout)
            check = subprocess.run(
                [
                    "psql", "-d", db, "-Atc",
                    "SELECT tablename FROM pg_tables WHERE schemaname='public' "
                    "AND tablename LIKE 'produtos_nfe%' ORDER BY 1;",
                ],
                capture_output=True,
                text=True,
            )
            tabelas = {ln.strip() for ln in check.stdout.splitlines() if ln.strip()}
            for nome in (
                "produtos_nfeestabelecimentofiscal",
                "produtos_nfealiquotacreditosn",
                "produtos_nfeparametropiscofins",
                "produtos_nfeparametroibpt",
                "produtos_nferegraoperacao",
                "produtos_nfeexcecaoicmsst",
                "produtos_nfequestionarioresposta",
                "produtos_nfealteracaofiscallog",
            ):
                self.assertIn(nome, tabelas)
            cols = subprocess.run(
                [
                    "psql", "-d", db, "-Atc",
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name='produtos_nfeestabelecimentofiscal' "
                    "AND column_name LIKE 'contador%';",
                ],
                capture_output=True,
                text=True,
            )
            colset = {ln.strip() for ln in cols.stdout.splitlines() if ln.strip()}
            self.assertIn("contador_nome", colset)
            self.assertIn("contador_crc", colset)
            self.assertIn("contador_aprovado_em", colset)
        finally:
            subprocess.run(["dropdb", "--if-exists", db], check=False)
