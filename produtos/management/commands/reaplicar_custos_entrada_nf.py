"""
Reaplica custo da Entrada NF (após PIN) no Cadastro SisVale — overlay + Produto.

Uso (dry-run):
  python manage.py reaplicar_custos_entrada_nf --nf=77846

Gravar:
  python manage.py reaplicar_custos_entrada_nf --nf=77846 --aplicar

Ou por id do rascunho:
  python manage.py reaplicar_custos_entrada_nf --id=<objectid> --aplicar
"""
from __future__ import annotations

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Reaplica custos da Entrada NF aprovada no catálogo SisVale (sem mexer em estoque)."

    def add_arguments(self, parser):
        parser.add_argument("--nf", type=str, default="", help="Número da NF (ex.: 77846).")
        parser.add_argument("--id", type=str, default="", help="ID do rascunho aprovado.")
        parser.add_argument(
            "--aplicar",
            action="store_true",
            help="Grava custo. Sem isto, só mostra o que faria.",
        )

    def handle(self, *args, **options):
        from produtos.nfe_entrada_util import _entrada_nota_rascunho_store, _object_id_rascunho
        from produtos.views import (
            _entrada_nfe_aplicar_custos_catalogo_pos_aprovacao,
            _entrada_nfe_custo_prev_sisvale,
            _entrada_nfe_decimal_v_un,
            obter_conexao_mongo,
        )

        nf = str(options.get("nf") or "").strip()
        rid = str(options.get("id") or "").strip()
        aplicar = bool(options.get("aplicar"))
        if not nf and not rid:
            self.stderr.write("Informe --nf=77846 ou --id=<rascunho>.")
            return

        _, db = obter_conexao_mongo()
        col = _entrada_nota_rascunho_store(db)
        if col is None:
            self.stderr.write("Armazenamento de rascunho indisponível.")
            return

        doc = None
        if rid:
            oid = _object_id_rascunho(rid)
            if oid is None:
                self.stderr.write("ID inválido.")
                return
            doc = col.find_one({"_id": oid})
        else:
            # Aprovada + número da NF (vários formatos de cabeçalho).
            candidatos = list(
                col.find(
                    {
                        "extra.aprovacao_wizard_em": {"$exists": True, "$nin": [None, ""]},
                        "$or": [
                            {"cabecalho.nNF": nf},
                            {"cabecalho.numero": nf},
                            {"cabecalho.numero_nf": nf},
                            {"cabecalho.nnf": nf},
                        ],
                    }
                ).sort("atualizado_em", -1).limit(5)
            )
            if not candidatos:
                # Match textual frouxo (NF pode estar int).
                try:
                    nfi = int(nf)
                except ValueError:
                    nfi = None
                q_or = [
                    {"cabecalho.nNF": nf},
                    {"cabecalho.numero": nf},
                ]
                if nfi is not None:
                    q_or.extend(
                        [
                            {"cabecalho.nNF": nfi},
                            {"cabecalho.numero": nfi},
                            {"cabecalho.numero_nf": nfi},
                        ]
                    )
                candidatos = list(
                    col.find(
                        {
                            "extra.aprovacao_wizard_em": {"$exists": True, "$nin": [None, ""]},
                            "$or": q_or,
                        }
                    )
                    .sort("atualizado_em", -1)
                    .limit(5)
                )
            if not candidatos:
                self.stderr.write(f"Nenhuma NF {nf} aprovada (PIN) encontrada.")
                return
            doc = candidatos[0]
            if len(candidatos) > 1:
                self.stdout.write(
                    f"Achei {len(candidatos)} — usando a mais recente "
                    f"id={doc.get('_id')}"
                )

        if not doc:
            self.stderr.write("Rascunho não encontrado.")
            return

        oid = str(doc.get("_id") or "")
        cab = doc.get("cabecalho") if isinstance(doc.get("cabecalho"), dict) else {}
        linhas = doc.get("linhas") if isinstance(doc.get("linhas"), list) else []
        extra = doc.get("extra") if isinstance(doc.get("extra"), dict) else {}
        self.stdout.write(
            f"Nota id={oid} nNF={cab.get('nNF') or cab.get('numero') or '?'} "
            f"linhas={len(linhas)} aprovada={str(extra.get('aprovacao_wizard_em') or '')[:19]}"
        )

        for ln in linhas:
            if not isinstance(ln, dict):
                continue
            pid = str(ln.get("produto_id") or "").strip()
            if not pid or pid.lower().startswith("local:"):
                continue
            v_nf = _entrada_nfe_decimal_v_un(ln)
            atual = _entrada_nfe_custo_prev_sisvale(pid)
            nome = str(ln.get("x_prod") or "")[:40]
            self.stdout.write(
                f"  {pid[:12]} {nome:40} custo_atual={atual} v_un_nf={v_nf}"
            )

        if not aplicar:
            self.stdout.write("Dry-run. Para gravar: acrescente --aplicar")
            return

        client, db2 = obter_conexao_mongo()
        out = _entrada_nfe_aplicar_custos_catalogo_pos_aprovacao(
            db=db2,
            client_m=client,
            linhas=linhas,
            cab=cab,
            extra=extra,
            user_pk=None,
            excluir_rascunho_id=oid,
        )
        self.stdout.write(
            f"OK sisvale={out.get('atualizados_sisvale')} mongo={out.get('atualizados_mongo')} "
            f"erros={len(out.get('erros') or [])}"
        )
        for err in out.get("erros") or []:
            self.stderr.write(str(err))
