"""
Conta e (opcional) aplica códigos bipados das Entradas NF antigas no cadastro.

Fonte: ``EntradaNotaRascunhoAgro.linhas[].bip_similar_codigos``.
Padrão = só conta. Com ``--aplicar`` grava (regra B: 230… promove).

Uso:
  python manage.py contar_bip_entrada_nf_cadastro
  python manage.py contar_bip_entrada_nf_cadastro --aplicar
"""

from __future__ import annotations

from typing import Any

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from produtos.agro_codigo_barras_loja_util import eh_codigo_barras_loja
from produtos.mongo_index_codigos import (
    aplicar_bip_entrada_nf_troca_inteligente,
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


def _bip_conflito_outro_produto(pid: str, dig: str) -> bool:
    return (
        ProdutoGestaoOverlayAgro.objects.filter(codigo_barras=dig)
        .exclude(produto_externo_id=pid[:64])
        .exists()
        or Produto.objects.filter(codigo_barras=dig)
        .exclude(produto_externo_id=pid[:64])
        .exists()
    )


def _aplicar_um(pid: str, dig: str) -> str:
    """Grava 1 bip. Retorna acao efetiva: promove|opcional|noop|erro."""
    ov, _ = ProdutoGestaoOverlayAgro.objects.get_or_create(
        produto_externo_id=pid[:64],
        defaults={},
    )
    ex = dict(ov.cadastro_extras) if isinstance(ov.cadastro_extras, dict) else {}
    promover = not _bip_conflito_outro_produto(pid, dig)
    res = aplicar_bip_entrada_nf_troca_inteligente(
        codigo_barras_atual=(ov.codigo_barras or "").strip(),
        cadastro_extras=ex,
        bip=dig,
        promover_se_loja=promover,
    )
    acao = str(res.get("acao") or "noop")
    if acao == "noop":
        return "noop"
    with transaction.atomic():
        if acao == "promove" and res.get("codigo_barras"):
            ov.codigo_barras = str(res["codigo_barras"])[:80]
        lista = res.get("codigos_barras_opcionais") or []
        if lista:
            ex["codigos_barras_opcionais"] = lista
            ex.pop("codigos_barras_alternativos", None)
        ov.cadastro_extras = ex
        ov.atualizado_em = timezone.now()
        ov.save()
        if acao == "promove" and res.get("codigo_barras"):
            Produto.objects.filter(produto_externo_id=pid[:64]).update(
                codigo_barras=str(res["codigo_barras"])[:80]
            )
    return acao


def _coletar_faltantes() -> list[dict[str, str]]:
    vistos: set[tuple[str, str]] = set()
    out: list[dict[str, str]] = []
    qs = EntradaNotaRascunhoAgro.objects.only(
        "rascunho_id", "linhas", "cabecalho", "status"
    ).iterator(chunk_size=200)
    for row in qs:
        linhas = row.linhas if isinstance(row.linhas, list) else []
        cab = row.cabecalho if isinstance(row.cabecalho, dict) else {}
        nf = str(cab.get("numero") or "").strip() or "-"
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
            ja = _codigos_ja_no_cadastro(pid)
            princ = _principal_atual(pid)
            for dig in codigos:
                key = (pid, dig)
                if key in vistos:
                    continue
                vistos.add(key)
                if dig in ja:
                    continue
                if princ and eh_codigo_barras_loja(princ):
                    acao_est = "promove"
                else:
                    acao_est = "opcional"
                out.append(
                    {
                        "nf": nf,
                        "pid": pid,
                        "bip": dig,
                        "principal_hoje": princ or "-",
                        "acao_est": acao_est,
                        "status_nota": str(row.status or ""),
                    }
                )
    return out


class Command(BaseCommand):
    help = "Conta / aplica bips antigos da Entrada NF no cadastro."

    def add_arguments(self, parser):
        parser.add_argument(
            "--amostra",
            type=int,
            default=12,
            help="Quantos pares listar no dry-run (padrao 12).",
        )
        parser.add_argument(
            "--aplicar",
            action="store_true",
            help="Grava no cadastro. Sem isto = so conta.",
        )

    def handle(self, *args, **options):
        aplicar = bool(options.get("aplicar"))
        amostra_n = max(0, int(options.get("amostra") or 0))

        faltam = _coletar_faltantes()
        promove_est = sum(1 for x in faltam if x["acao_est"] == "promove")
        so_opc = len(faltam) - promove_est

        notas_com_bip = 0
        linhas_com_bip = 0
        pares_total = 0
        pares_ja = 0
        vistos_all: set[tuple[str, str]] = set()
        for row in EntradaNotaRascunhoAgro.objects.only("linhas").iterator(chunk_size=200):
            linhas = row.linhas if isinstance(row.linhas, list) else []
            teve = False
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
                for dig in codigos:
                    key = (pid, dig)
                    if key in vistos_all:
                        continue
                    vistos_all.add(key)
                    pares_total += 1
                    if dig in ja:
                        pares_ja += 1
            if teve:
                notas_com_bip += 1

        self.stdout.write("")
        self.stdout.write("=== Contagem bip Entrada NF -> cadastro ===")
        self.stdout.write(f"Notas com bip_similar gravado:     {notas_com_bip}")
        self.stdout.write(f"Linhas com bip_similar:            {linhas_com_bip}")
        self.stdout.write(f"Pares unicos produto+codigo:       {pares_total}")
        self.stdout.write(f"  ja no cadastro (pula):           {pares_ja}")
        self.stdout.write(f"  ainda faltam gravar:             {len(faltam)}")
        self.stdout.write(f"    estim. promove (230...->opc):  {promove_est}")
        self.stdout.write(f"    estim. so opcional:            {so_opc}")
        self.stdout.write("")
        self.stdout.write(
            "Obs.: so entra bip_similar_codigos (Sim./Opc.). "
            "Centenas de notas com Ok puro nao tem bip separado gravado."
        )

        if not aplicar:
            if faltam and amostra_n:
                self.stdout.write("")
                self.stdout.write(f"Amostra ({min(amostra_n, len(faltam))}):")
                for a in faltam[:amostra_n]:
                    self.stdout.write(
                        f"  NF {a['nf']} · prod {a['pid'][:20]} · bip {a['bip']} · "
                        f"hoje {a['principal_hoje']} -> {a['acao_est']} · nota {a['status_nota']}"
                    )
            self.stdout.write("")
            self.stdout.write("Dry-run. Para gravar: --aplicar")
            self.stdout.write("")
            return

        ok_p = ok_o = noop = err = 0
        for item in faltam:
            try:
                acao = _aplicar_um(item["pid"], item["bip"])
            except Exception as exc:
                err += 1
                self.stdout.write(
                    f"ERRO prod {item['pid'][:24]} bip {item['bip']}: {exc}"
                )
                continue
            if acao == "promove":
                ok_p += 1
            elif acao == "opcional":
                ok_o += 1
            else:
                noop += 1

        try:
            from django.core.cache import cache

            cache.delete("pdv_catalogo_cache_v1")
            hoje = timezone.localdate().isoformat()
            for v in ("v5", "v4", "v3", "v2"):
                cache.delete(f"pdv_catalogo_slim_{v}:{hoje}")
        except Exception:
            pass

        self.stdout.write("")
        self.stdout.write("=== Aplicado ===")
        self.stdout.write(f"  promove:  {ok_p}")
        self.stdout.write(f"  opcional: {ok_o}")
        self.stdout.write(f"  noop:     {noop}")
        self.stdout.write(f"  erro:     {err}")
        self.stdout.write("")
