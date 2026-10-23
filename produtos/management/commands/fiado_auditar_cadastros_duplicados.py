"""Audita fiado em aberto: títulos do mesmo cliente em cadastros ClienteAgro diferentes."""

from __future__ import annotations

import json
from collections import defaultdict
from decimal import Decimal

from django.core.management.base import BaseCommand

from produtos.fiado_import_util import _norm_nome_fiado_match
from produtos.models import ClienteAgro, FiadoTituloAgro


def _dec(val) -> Decimal:
    try:
        return Decimal(str(val or 0)).quantize(Decimal("0.01"))
    except Exception:
        return Decimal("0")


class Command(BaseCommand):
    help = (
        "Relatório: clientes com fiado em aberto em mais de um ClienteAgro (mesmo nome) "
        "ou com títulos sem cadastro + com cadastro — risco de visão parcial no PDV (pré v5.58)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--limite",
            type=int,
            default=40,
            help="Máximo de linhas no detalhe (0 = todas).",
        )
        parser.add_argument(
            "--json",
            action="store_true",
            help="Imprime só JSON (para log/cópia).",
        )

    def handle(self, *args, **options):
        limite = int(options.get("limite") or 40)
        so_json = bool(options.get("json"))

        abertos = FiadoTituloAgro.objects.exclude(
            situacao__in=(
                FiadoTituloAgro.Situacao.QUITADO,
                FiadoTituloAgro.Situacao.CANCELADO,
            )
        ).only(
            "pk",
            "cliente_agro_id",
            "cliente_nome",
            "cliente_codigo",
            "numero_documento",
            "valor_bruto",
            "valor_pago",
            "origem",
        )

        grupos: dict[str, dict] = {}

        for t in abertos.iterator(chunk_size=500):
            saldo = _dec(t.valor_bruto) - _dec(t.valor_pago)
            if saldo <= 0:
                continue
            key = _norm_nome_fiado_match(t.cliente_nome or "")
            if not key:
                key = f"sem-nome:{t.pk}"
            g = grupos.get(key)
            if not g:
                g = {
                    "nome_chave": key,
                    "nomes": set(),
                    "pks": set(),
                    "titulos": 0,
                    "saldo": Decimal("0"),
                    "sem_cadastro": 0,
                    "importacao": 0,
                    "pdv": 0,
                    "por_pk": defaultdict(lambda: {"titulos": 0, "saldo": Decimal("0")}),
                }
                grupos[key] = g
            g["nomes"].add((t.cliente_nome or "").strip())
            g["titulos"] += 1
            g["saldo"] += saldo
            if t.origem == FiadoTituloAgro.Origem.IMPORTACAO:
                g["importacao"] += 1
            elif t.origem == FiadoTituloAgro.Origem.PDV:
                g["pdv"] += 1
            if t.cliente_agro_id:
                g["pks"].add(int(t.cliente_agro_id))
                g["por_pk"][int(t.cliente_agro_id)]["titulos"] += 1
                g["por_pk"][int(t.cliente_agro_id)]["saldo"] += saldo
            else:
                g["sem_cadastro"] += 1

        afetados: list[dict] = []
        ok_nome = 0

        for g in grupos.values():
            multi_pk = len(g["pks"]) > 1
            misto = bool(g["pks"]) and g["sem_cadastro"] > 0
            if not multi_pk and not misto:
                ok_nome += 1
                continue
            nome_exib = max(g["nomes"], key=lambda x: (len(x), x)) if g["nomes"] else g["nome_chave"]
            pk_principal = None
            saldo_pk_principal = Decimal("0")
            titulos_pk_principal = 0
            if g["pks"]:
                pk_principal = max(
                    g["pks"],
                    key=lambda pk: (g["por_pk"][pk]["titulos"], float(g["por_pk"][pk]["saldo"])),
                )
                titulos_pk_principal = g["por_pk"][pk_principal]["titulos"]
                saldo_pk_principal = g["por_pk"][pk_principal]["saldo"]
            ocultos = g["titulos"] - titulos_pk_principal
            saldo_oculto = (g["saldo"] - saldo_pk_principal).quantize(Decimal("0.01"))
            pks_nomes = []
            for pk in sorted(g["pks"]):
                cli = ClienteAgro.objects.filter(pk=pk).only("nome", "externo_id").first()
                pks_nomes.append(
                    {
                        "pk": pk,
                        "nome_cadastro": (cli.nome if cli else "?"),
                        "externo_id": (cli.externo_id if cli else "") or "",
                        "titulos": g["por_pk"][pk]["titulos"],
                        "saldo": float(g["por_pk"][pk]["saldo"].quantize(Decimal("0.01"))),
                    }
                )
            afetados.append(
                {
                    "nome": nome_exib,
                    "titulos_total": g["titulos"],
                    "saldo_total": float(g["saldo"].quantize(Decimal("0.01"))),
                    "cadastros_distintos": len(g["pks"]),
                    "titulos_sem_cadastro": g["sem_cadastro"],
                    "importacao": g["importacao"],
                    "pdv": g["pdv"],
                    "pk_mais_titulos": pk_principal,
                    "titulos_só_esse_pk": titulos_pk_principal,
                    "saldo_só_esse_pk": float(saldo_pk_principal.quantize(Decimal("0.01"))),
                    "titulos_ocultos_pdv": ocultos,
                    "saldo_oculto_pdv": float(saldo_oculto),
                    "cadastros": pks_nomes,
                    "motivo": (
                        "vários cadastros"
                        if multi_pk
                        else "títulos importados sem cadastro + com cadastro"
                    ),
                }
            )

        afetados.sort(key=lambda x: (-x["saldo_oculto_pdv"], -x["titulos_ocultos_pdv"], x["nome"]))

        resumo = {
            "clientes_com_saldo_por_nome": len(grupos),
            "clientes_ok_um_cadastro": ok_nome,
            "clientes_afetados": len(afetados),
            "titulos_ocultos_total": sum(x["titulos_ocultos_pdv"] for x in afetados),
            "saldo_oculto_total": float(
                sum(Decimal(str(x["saldo_oculto_pdv"])) for x in afetados).quantize(Decimal("0.01"))
            ),
        }

        if so_json:
            self.stdout.write(
                json.dumps(
                    {"ok": True, "resumo": resumo, "afetados": afetados},
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return

        self.stdout.write(self.style.MIGRATE_HEADING("Fiado — auditoria cadastros duplicados"))
        self.stdout.write("")
        self.stdout.write(
            "Clientes com saldo (agrupado por nome): {}".format(resumo["clientes_com_saldo_por_nome"])
        )
        self.stdout.write("Sem problema (1 cadastro / tudo no mesmo): {}".format(resumo["clientes_ok_um_cadastro"]))
        self.stdout.write(self.style.WARNING("Afetados (visão PDV antiga incompleta): {}".format(resumo["clientes_afetados"])))
        self.stdout.write(
            "Títulos que o PDV podia não mostrar (total): {} · R$ {:,.2f}".format(
                resumo["titulos_ocultos_total"],
                resumo["saldo_oculto_total"],
            ).replace(",", "X").replace(".", ",").replace("X", ".")
        )
        self.stdout.write("")
        self.stdout.write(
            "Nota: importação de fiado (planilha ERP) + sync de clientes + vendas PDV "
            "podem gravar o mesmo nome em cadastros diferentes. "
            "Não é o import de histórico de vendas F8 (FL-042)."
        )
        self.stdout.write("")

        if not afetados:
            self.stdout.write(self.style.SUCCESS("Nenhum cliente afetado."))
            return

        mostrar = afetados if limite <= 0 else afetados[:limite]
        for i, row in enumerate(mostrar, start=1):
            self.stdout.write(
                self.style.WARNING(
                    "{:02d}. {} — {} título(s) · R$ {:.2f} · {} cadastro(s) · "
                    "PDV via 1 pk via só {} (R$ {:.2f}) · ocultos {} (R$ {:.2f}) · {}".format(
                        i,
                        row["nome"][:50],
                        row["titulos_total"],
                        row["saldo_total"],
                        row["cadastros_distintos"],
                        row["titulos_só_esse_pk"],
                        row["saldo_só_esse_pk"],
                        row["titulos_ocultos_pdv"],
                        row["saldo_oculto_pdv"],
                        row["motivo"],
                    )
                )
            )
            for c in row["cadastros"]:
                self.stdout.write(
                    "    · pk {} · {} · ext {} · {} tít. · R$ {:.2f}".format(
                        c["pk"],
                        (c["nome_cadastro"] or "")[:40],
                        c["externo_id"] or "—",
                        c["titulos"],
                        c["saldo"],
                    )
                )
            if row["titulos_sem_cadastro"]:
                self.stdout.write(
                    "    · (sem ClienteAgro): {} título(s)".format(row["titulos_sem_cadastro"])
                )

        if limite > 0 and len(afetados) > limite:
            self.stdout.write("")
            self.stdout.write("… +{} cliente(s). Use --limite 0 para listar todos.".format(len(afetados) - limite))

        self.stdout.write("")
        self.stdout.write("JSON completo: python manage.py fiado_auditar_cadastros_duplicados --json")
