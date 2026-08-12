"""
Conta e (opcional) aplica códigos das Entradas NF antigas no cadastro.

Fontes:
  - bip  = linhas[].bip_similar_codigos (Sim./Opc.)
  - ean  = linhas[].ean da NF (+ variante sem 1º dígito se GTIN-14 1…/0…)
  - ambas = as duas

Uso:
  python manage.py contar_bip_entrada_nf_cadastro --fonte=ean
  python manage.py contar_bip_entrada_nf_cadastro --fonte=ean --aplicar
"""

from __future__ import annotations

from typing import Any, Iterable

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


def _ean_candidatos(raw: Any) -> list[str]:
    """EAN da NF + variante unitária se vier GTIN-14 com prefixo 0/1."""
    dig = _digits(raw)
    if len(dig) < 8 or len(dig) > 20:
        return []
    if set(dig) <= {"0"}:
        return []
    out: list[str] = []
    seen: set[str] = set()

    def add(x: str) -> None:
        if 8 <= len(x) <= 20 and x not in seen and not (set(x) <= {"0"}):
            seen.add(x)
            out.append(x)

    add(dig)
    # Embalagem/caixa: 14 dígitos começando com 0 ou 1 → unitário sem o 1º dígito
    if len(dig) == 14 and dig[0] in ("0", "1"):
        add(dig[1:])
    return out


def _codigos_fonte_linha(ln: dict, fonte: str) -> list[str]:
    raw: list[str] = []
    if fonte in ("bip", "ambas"):
        sim = ln.get("bip_similar_codigos")
        if isinstance(sim, list):
            raw.extend(str(x) for x in sim)
    if fonte in ("ean", "ambas"):
        raw.extend(_ean_candidatos(ln.get("ean")))
    return normalizar_codigos_barras_opcionais(raw)


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
        # EAN da NF em 14 dígitos (embalagem) — campo dedicado, se ainda vazio.
        if len(dig) == 14 and dig[0] in ("0", "1"):
            if not str(ex.get("entrada_nfe_ean_embalagem") or "").strip():
                ex["entrada_nfe_ean_embalagem"] = dig
        ov.cadastro_extras = ex
        ov.save()
        if acao == "promove" and res.get("codigo_barras"):
            Produto.objects.filter(produto_externo_id=pid[:64]).update(
                codigo_barras=str(res["codigo_barras"])[:80]
            )
    return acao


def _scan(fonte: str) -> tuple[dict[str, int], list[dict[str, str]]]:
    notas_com = 0
    linhas_com = 0
    pares_total = 0
    pares_ja = 0
    vistos: set[tuple[str, str]] = set()
    faltam: list[dict[str, str]] = []

    for row in EntradaNotaRascunhoAgro.objects.only(
        "rascunho_id", "linhas", "cabecalho", "status"
    ).iterator(chunk_size=200):
        linhas = row.linhas if isinstance(row.linhas, list) else []
        cab = row.cabecalho if isinstance(row.cabecalho, dict) else {}
        nf = str(cab.get("numero") or "").strip() or "-"
        teve = False
        for ln in linhas:
            if not isinstance(ln, dict):
                continue
            pid = str(ln.get("produto_id") or "").strip()
            if not _pid_ok(pid):
                continue
            codigos = _codigos_fonte_linha(ln, fonte)
            if not codigos:
                continue
            teve = True
            linhas_com += 1
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
                acao_est = (
                    "promove"
                    if princ and eh_codigo_barras_loja(princ)
                    else "opcional"
                )
                faltam.append(
                    {
                        "nf": nf,
                        "pid": pid,
                        "bip": dig,
                        "principal_hoje": princ or "-",
                        "acao_est": acao_est,
                        "status_nota": str(row.status or ""),
                    }
                )
        if teve:
            notas_com += 1

    stats = {
        "notas_com": notas_com,
        "linhas_com": linhas_com,
        "pares_total": pares_total,
        "pares_ja": pares_ja,
        "faltam": len(faltam),
        "promove": sum(1 for x in faltam if x["acao_est"] == "promove"),
        "opcional": sum(1 for x in faltam if x["acao_est"] == "opcional"),
    }
    return stats, faltam


class Command(BaseCommand):
    help = "Conta / aplica EAN ou bip das Entradas NF no cadastro."

    def add_arguments(self, parser):
        parser.add_argument(
            "--fonte",
            choices=("bip", "ean", "ambas"),
            default="bip",
            help="bip=similar antigo; ean=EAN da linha NF; ambas=os dois.",
        )
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
        fonte = str(options.get("fonte") or "bip")
        aplicar = bool(options.get("aplicar"))
        amostra_n = max(0, int(options.get("amostra") or 0))

        stats, faltam = _scan(fonte)

        self.stdout.write("")
        self.stdout.write(f"=== Contagem fonte={fonte} Entrada NF -> cadastro ===")
        self.stdout.write(f"Notas com codigo na fonte:         {stats['notas_com']}")
        self.stdout.write(f"Linhas com codigo na fonte:        {stats['linhas_com']}")
        self.stdout.write(f"Pares unicos produto+codigo:       {stats['pares_total']}")
        self.stdout.write(f"  ja no cadastro (pula):           {stats['pares_ja']}")
        self.stdout.write(f"  ainda faltam gravar:             {stats['faltam']}")
        self.stdout.write(f"    estim. promove (230...->opc):  {stats['promove']}")
        self.stdout.write(f"    estim. so opcional:            {stats['opcional']}")
        self.stdout.write("")
        if fonte == "ean":
            self.stdout.write(
                "Fonte ean: campo ean da linha + variante sem 1o digito se GTIN-14 (0/1…)."
            )
        elif fonte == "bip":
            self.stdout.write("Fonte bip: so bip_similar_codigos (Sim./Opc.).")
        else:
            self.stdout.write("Fonte ambas: bip_similar + ean da linha (+ variante GTIN-14).")

        if not aplicar:
            if faltam and amostra_n:
                self.stdout.write("")
                self.stdout.write(f"Amostra ({min(amostra_n, len(faltam))}):")
                for a in faltam[:amostra_n]:
                    self.stdout.write(
                        f"  NF {a['nf']} · prod {a['pid'][:20]} · cod {a['bip']} · "
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
                    f"ERRO prod {item['pid'][:24]} cod {item['bip']}: {exc}"
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
