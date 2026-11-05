"""
Prova sync de arquivamento Zap ↔ Agro (path).

  python scripts/verify_wa_arquivo_sync_path.py
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def main() -> int:
    print("VERIFY WA-ARQUIVO-SYNC — estático")
    models = _read("produtos/models.py")
    util = _read("produtos/atendimento_whatsapp_util.py")
    views = _read("produtos/views_atendimento_whatsapp.py")
    urls = _read("produtos/urls.py")
    node = _read("whatsapp_atendimento/index.js")
    tests = _read("produtos/tests_atendimento_whatsapp.py")
    mig = ROOT / "produtos/migrations/0152_whatsapp_pedido_arquivar.py"

    check("model_tipo_arquivar", 'TIPO_ARQUIVAR = "arquivar"' in models)
    check("mig_0152", mig.is_file())
    check("util_pedir", "def pedir_arquivar_zap" in util)
    check("util_aplicar", "def aplicar_arquivo_zap" in util)
    check("util_sync_flag", "sync_zap: bool = True" in util)
    check("util_msg_cliente_sem_pedido", "Não enfileira pedido na ponte" in util)
    check("view_bridge", "api_atendimento_whatsapp_bridge_arquivo" in views)
    check("url_bridge", "bridge/arquivo/" in urls)
    check("node_chatmodify", "chatModify" in node and 'tipo === "arquivar"' in node)
    check("node_avisar", "avisarArquivoZap" in node and "bridge/arquivo/" in node)
    check("node_echo", "arquivoLocalAte" in node and "marcarArquivoLocal" in node)
    check("tests_pedido", "test_arquivar_enfileira_pedido_ponte" in tests)
    check("tests_bridge", "test_bridge_arquivo_celular_vai_para_resolvidas" in tests)

    print()
    if fails:
        print(f"FALHOU {len(fails)}: {', '.join(fails)}")
        return 1
    print(f"OK {len(oks)} checks")
    return 0


if __name__ == "__main__":
    sys.exit(main())
