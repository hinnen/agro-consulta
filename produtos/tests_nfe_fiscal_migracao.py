"""Etapa 2 — 0152/0153 no Postgres descartável; diagnóstico 0039 e 0065. Sem produção."""
from __future__ import annotations

import os
import subprocess
import textwrap
import uuid
from pathlib import Path

from django.test import SimpleTestCase

REPO = Path(__file__).resolve().parents[1]

_APPLY_0152_ISOLADO = textwrap.dedent(
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

_REPRO_0065 = textwrap.dedent(
    """
    import os
    import django
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    django.setup()
    from django.core.management import call_command
    call_command("migrate", "produtos", "0064", verbosity=0, skip_checks=True)
    try:
        call_command("migrate", "produtos", "0065", verbosity=1, skip_checks=True)
    except Exception as exc:
        print("FALHA_0065:" + type(exc).__name__ + ":" + str(exc).replace("\\n", " ")[:800])
    else:
        print("INESPERADO_0065_OK")
    """
).strip()

_STAGING_EQUIV = textwrap.dedent(
    """
    import os
    import django
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    django.setup()
    from django.core.management import call_command
    from django.db import connection
    from django.db.migrations.recorder import MigrationRecorder
    import produtos.plano_conta_agro_util as plano_util

    def _noop(*, force=False):
        return {"planos": 0, "aliases": 0, "ja_existiam": 0}

    plano_util.seed_planos_conta_agro = _noop
    call_command("migrate", "produtos", "0151", verbosity=1, skip_checks=True)
    rec = MigrationRecorder(connection)
    applied = rec.applied_migrations()
    if ("produtos", "0151_pdv_rapido_layout") not in applied:
        raise SystemExit("0151 nao aplicada")
    if ("produtos", "0152_nfe_fiscal_config") in applied:
        raise SystemExit("0152 ja estava aplicada antes do migrate normal")
    call_command("migrate", "produtos", "0153", verbosity=1, skip_checks=True)
    applied = rec.applied_migrations()
    if ("produtos", "0152_nfe_fiscal_config") not in applied:
        raise SystemExit("0152 nao aplicada pelo migrate")
    if ("produtos", "0153_nfe_fiscal_contador") not in applied:
        raise SystemExit("0153 nao aplicada pelo migrate")
    print("STAGING_EQUIV_OK")
    """
).strip()


def _pg_env(db: str) -> dict:
    env = os.environ.copy()
    env["DATABASE_URL"] = f"postgres://ubuntu@/{db}?host=/var/run/postgresql"
    env["DJANGO_SETTINGS_MODULE"] = "config.settings"
    env.setdefault("SECRET_KEY", "nfe-fiscal-mig-test")
    return env


def _py() -> str:
    p = "/workspace/.venv/bin/python"
    return p if Path(p).exists() else "python3"


def _createdb() -> str | None:
    db = f"nfe_e2_{uuid.uuid4().hex[:10]}"
    created = subprocess.run(["createdb", db], capture_output=True, text=True)
    if created.returncode != 0:
        return None
    return db


class NfeFiscalMigracao0039DiagnosticoTests(SimpleTestCase):
    def test_0039_sqlite_usa_get_model_fora_do_state(self):
        src = (REPO / "produtos" / "migrations" / "0039_nfce_documento_agro.py").read_text(encoding="utf-8")
        self.assertIn("SeparateDatabaseAndState", src)
        self.assertIn('apps.get_model("produtos", "NfceNumeracaoAgro")', src)
        self.assertIn('vendor == "postgresql"', src)


class NfeFiscalMigracao0065DiagnosticoTests(SimpleTestCase):
    def test_0065_seed_usa_modelo_vivo_sem_exibir_pdv(self):
        src65 = (REPO / "produtos" / "migrations" / "0065_plano_conta_agro_cadastro.py").read_text(encoding="utf-8")
        src85 = (REPO / "produtos" / "migrations" / "0085_plano_conta_exibir_pdv.py").read_text(encoding="utf-8")
        src_seed = (REPO / "produtos" / "plano_conta_agro_util.py").read_text(encoding="utf-8")
        self.assertIn("seed_planos_conta_agro", src65)
        self.assertNotIn("exibir_pdv", src65)
        self.assertIn("exibir_pdv", src85)
        self.assertIn("PlanoContaAgro.objects.get_or_create", src_seed)
        self.assertIn("apps.get_model", (REPO / "produtos" / "migrations" / "0127_plano_extravio_apos_deposito.py").read_text())

    def test_0065_falha_em_postgres_vazio_por_exibir_pdv(self):
        db = _createdb()
        if not db:
            self.skipTest("PostgreSQL local indisponível")
        try:
            run = subprocess.run(
                [_py(), "-c", _REPRO_0065],
                cwd=str(REPO),
                env=_pg_env(db),
                capture_output=True,
                text=True,
            )
            out = run.stdout + "\n" + run.stderr
            self.assertIn("FALHA_0065:", out, out)
            self.assertIn("exibir_pdv", out)
            self.assertNotIn("INESPERADO_0065_OK", out)
        finally:
            subprocess.run(["dropdb", "--if-exists", db], check=False)


class NfeFiscalMigracao0152PostgresTests(SimpleTestCase):
    def test_0152_cria_tabelas_em_postgres_descartavel(self):
        db = _createdb()
        if not db:
            self.skipTest("PostgreSQL local indisponível")
        try:
            applied = subprocess.run(
                [_py(), "-c", _APPLY_0152_ISOLADO],
                cwd=str(REPO),
                env=_pg_env(db),
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
        finally:
            subprocess.run(["dropdb", "--if-exists", db], check=False)

    def test_0152_0153_via_migrate_apos_historico_ate_0151(self):
        """Equivalente a staging: 0065 já teria sido aplicada no passado; aqui o seed vivo é no-op só no teste."""
        db = _createdb()
        if not db:
            self.skipTest("PostgreSQL local indisponível")
        try:
            run = subprocess.run(
                [_py(), "-c", _STAGING_EQUIV],
                cwd=str(REPO),
                env=_pg_env(db),
                capture_output=True,
                text=True,
                timeout=180,
            )
            out = run.stdout + "\n" + run.stderr
            self.assertEqual(run.returncode, 0, out)
            self.assertIn("STAGING_EQUIV_OK", out)
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
            django_migs = subprocess.run(
                [
                    "psql", "-d", db, "-Atc",
                    "SELECT name FROM django_migrations WHERE app='produtos' "
                    "AND name IN ('0152_nfe_fiscal_config','0153_nfe_fiscal_contador') ORDER BY 1;",
                ],
                capture_output=True,
                text=True,
            )
            names = [ln.strip() for ln in django_migs.stdout.splitlines() if ln.strip()]
            self.assertEqual(names, ["0152_nfe_fiscal_config", "0153_nfe_fiscal_contador"])
        finally:
            subprocess.run(["dropdb", "--if-exists", db], check=False)
