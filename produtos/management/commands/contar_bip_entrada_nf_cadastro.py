"""
Conta (dry-run) códigos bipados nas Entradas NF antigas que ainda podem ir ao cadastro.

Só lê — não grava. Fonte: ``EntradaNotaRascunhoAgro.linhas[].bip_similar_codigos``.

Uso:
  python manage.py contar_bip_entrada_nf_cadastro
  python manage.py contar_bip_entrada_nf_cadastro --amostra 15
"""

from __future__ import annotations

from typing import Any

from django.core.management.base import BaseCommand

from produtos.agro_codigo_barras_loja_util import eh_codigo_barras_loja
from produtos.mongo_index_codigos import (
    codigos_barras_opcionais_de_cadastro_extras,
    normalizar_codigos_barras_opcionais,
)
from produtos.models import EntradaNotaRascunhoAgro, Produto, ProdutoGestaoOverlayAgro


def _digits(s: Any) -> str:
    return "".join(ch for ch in str(s or "") if ch.isdigit())


def _pid_ok(pid: str) -> bool:
    s = str(pid or "").strip()
    if not s or s.lower().startswith("local:"):
        return False
    if s.startswith('"') or " " in s:
        return False
    if len(s) == 24 and all(c in "0123456789abcdefABCDEF" for c in s):
        return True
    if s.upper().startswith("AGRO") and len(s) >= 8:
        return True
    # ObjectId-like / hex curto legado
    if 16 <= len(s) <= 64 and all(c.isalnum() for c in s):
        return True
    return False


def _codigos_ja_no_cadastro(pid: str) -> set[str]:
    out: set[str] = set()
    ov = (
        ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid[:64])
        .only("codigo_barras", "codigo_nfe", "cadastro_extras")
        .first()
    )
    if ov:
        for z in (ov.codigo_barras, ov.codigo_nfe):
            d = _digits(z)
            if len(d) >= 8:
                out.add(d)
        ce = ov.cadastro_extras if isinstance(ov.cadastro_extras, dict) else {}
        for d in codigos_barras_opcionais_de_cadastro_extras(ce):
            out.add(d)
        for k in ("entrada_nfe_ean_embalagem", "ean_embalagem_nf"):
            d = _digits(ce.get(k))
            if len(d) >= 8:
                out.add(d)
    p = (
        Produto.objects.filter(produto_externo_id=pid[:64])
        .only("codigo_barras", "codigo_interno", "codigo_nfe")
        .first()
    )
    if p:
        for z in (p.codigo_barras, p.codigo_interno, p.codigo_nfe):
            d = _digits(z)
            if len(d) >= 8:
                out.add(d)
    return out


def _principal_atual(pid: str) -> str:
    ov = (
        ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid[:64])
        .only("codigo_barras")
        .first()
    )
    if ov and (ov.codigo_barras or "").strip():
        return _digits(ov.codigo_barras)
    p = Produto.objects.filter(produto_externo_id=pid[:64]).only("codigo_barras").first()
    if p and (p.codigo_barras or "").strip():
        return _digits(p.codigo_barras)
    return ""


class Command(BaseCommand):
    help = "Conta bips antigos da Entrada NF ainda não no cadastro (só leitura)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--amostra",
            type=int,
            default=12,
            help="Quantos pares recuperáveis listar no final (padrão 12).",
        )

    def handle(self, *args, **options):
        amostra_n = max(0, int(options.get("amostra") or 0))
        notas_com_bip = 0
        linhas_com_bip = 0
        pares_total = 0
        pares_ja = 0
        pares_faltam = 0
        promove_est = 0
        so_opc_est = 0
        vistos: set[tuple[str, str]] = set()
        amostra: list[dict[str, str]] = []

        qs = EntradaNotaRascunhoAgro.objects.only(
            "rascunho_id", "linhas", "cabecalho", "status"
        ).iterator(chunk_size=200)

        for row in qs:
            linhas = row.linhas if isinstance(row.linhas, list) else []
            teve = False
            cab = row.cabecalho if isinstance(row.cabecalho, dict) else {}
            nf = str(cab.get("numero") or "").strip() or "—"
            for ln in linhas:
                if not isinstance(ln, dict):
                    continue
                pid = str(ln.get("produto_id") or "").strip()
                if not _pid_ok(pid):
                    continue
                sim = ln.get("bip_similar_codigos")
                if not isinstance(sim, list) or not sim:
                    continue
                codigos = normalizar_codigos_barras_opcionais(sim)
                if not codigos:
                    continue
                teve = True
                linhas_com_bip += 1
                ja = _codigos_ja_no_cadastro(pid)
                princ = _principal_atual(pid)
                for dig in codigos:
                    key = (pid, dig)
                    if key in vistos:
                        continue
                    vistos.add(key)
                    pares_total += 1
                    if dig in ja:
                        pares_ja += 1
                        continue
                    pares_faltam += 1
                    if princ and eh_codigo_barras_loja(princ):
                        promove_est += 1
                        acao = "promove"
                    else:
                        so_opc_est += 1
                        acao = "opcional"
                    if len(amostra) < amostra_n:
                        amostra.append(
                            {
                                "nf": nf,
                                "pid": pid,
                                "bip": dig,
                                "principal_hoje": princ or "—",
                                "acao": acao,
                                "status_nota": str(row.status or ""),
                            }
                        )
            if teve:
                notas_com_bip += 1

        self.stdout.write("")
        self.stdout.write("=== Contagem bip Entrada NF -> cadastro (dry-run) ===")
        self.stdout.write(f"Notas com bip_similar gravado:     {notas_com_bip}")
        self.stdout.write(f"Linhas com bip_similar:            {linhas_com_bip}")
        self.stdout.write(f"Pares unicos produto+codigo:       {pares_total}")
        self.stdout.write(f"  ja no cadastro (pula):           {pares_ja}")
        self.stdout.write(f"  ainda faltam gravar:             {pares_faltam}")
        self.stdout.write(f"    estim. promove (230...->opc):  {promove_est}")
        self.stdout.write(f"    estim. so opcional:            {so_opc_est}")
        self.stdout.write("")
        self.stdout.write(
            "Obs.: so entra o que ficou em bip_similar_codigos (muito do Sim./Opc.). "
            "Ok puro sem lista nao recupera EAN unitario diferente do da NF."
        )
        if amostra:
            self.stdout.write("")
            self.stdout.write(f"Amostra ({len(amostra)}):")
            for a in amostra:
                self.stdout.write(
                    f"  NF {a['nf']} · prod {a['pid'][:20]} · bip {a['bip']} · "
                    f"hoje {a['principal_hoje']} -> {a['acao']} · nota {a['status_nota']}"
                )
        self.stdout.write("")
