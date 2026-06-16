#!/usr/bin/env python
"""Registra commit do deploy em config/deploy_manifest.json (Render buildCommand).

Sem Django: evita conectar ao Postgres só para gravar o manifest no build.
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from config.app_build_util import record_deploy_build

if __name__ == "__main__":
    out = record_deploy_build()
    if out.get("ok"):
        if out.get("skipped"):
            print(f"deploy_manifest: já registrado v{out.get('version')} @ {out.get('commit')}")
        else:
            print(f"deploy_manifest: v{out.get('version')} @ {out.get('commit')}")
    else:
        print(f"deploy_manifest: aviso — {out.get('erro', 'falha')}", file=sys.stderr)
