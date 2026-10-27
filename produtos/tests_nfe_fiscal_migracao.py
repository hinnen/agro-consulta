"""Etapa 2 — migração 0152 em Postgres descartável e diagnóstico da 0039. Sem produção."""
from __future__ import annotations

import os
import subprocess
import uuid
from pathlib import Path

from django.test import SimpleTestCase

REPO = Path(__file__).resolve().parents[1]


class NfeFiscalMigracao0039DiagnosticoTests(SimpleTestCase):
    def test_0039_sqlite_usa_get_model_fora_do_state(self):
        src = (REPO / "produtos" / "migrations" / "0039_nfce_documento_agro.py").read_text(encoding="utf-8")
        self.assertIn("SeparateDatabaseAndState", src)
        self.assertIn('apps.get_model("produtos", "NfceNumeracaoAgro")', src)
        self.assertIn('vendor == "postgresql"', src)
        self.assertIn("# SQLite / dev local", src)
        # RunPython vê o from_state; CreateModel está só em state_operations → LookupError no SQLite.
        # Postgres não chama get_model (SQL cru). Não reescrever 0039 nesta revisão.


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
        py = "/workspace/.venv/bin/python"
        if not Path(py).exists():
            py = "python3"
        env.setdefault("SECRET_KEY", "nfe-fiscal-mig-test")
        env.setdefault("DJANGO_DEBUG", "0")
        try:
            mig152 = subprocess.run(
                [py, "manage.py", "migrate", "produtos", "0152", "--skip-checks", "--verbosity", "1"],
                cwd=str(REPO),
                env=env,
                capture_output=True,
                text=True,
            )
            self.assertEqual(mig152.returncode, 0, mig152.stdout + "\n" + mig152.stderr)
            mig = subprocess.run(
                [py, "manage.py", "migrate", "produtos", "0153", "--skip-checks", "--verbosity", "1"],
                cwd=str(REPO),
                env=env,
                capture_output=True,
                text=True,
            )
            self.assertEqual(mig.returncode, 0, mig.stdout + "\n" + mig.stderr)
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
        finally:
            subprocess.run(["dropdb", "--if-exists", db], check=False)
