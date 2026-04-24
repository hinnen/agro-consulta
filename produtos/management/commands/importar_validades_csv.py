import csv
import hashlib
import os
import unicodedata
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from django.core.management.base import BaseCommand

from produtos.models import (
    EstoqueLote,
    ProdutoGestaoOverlayAgro,
    sync_overlay_validade_resumo_de_lotes,
)

_MARCADOR_GERACAO = "Geração do Relatório"
_MARCADOR_GERACAO_ASC = "Geracao do Relatorio"

_MSG_FECHE_EXCEL = (
    "ERRO: o ficheiro está em uso. Feche o Excel (ou o programa que o abriu) e volte a "
    "executar. Dica: guarde como lista.csv ou lista.xlsx na pasta do projeto e use um "
    "caminho curto, sem carateres especiais."
)


def _as_str_cel(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, float) and x == int(x):
        return str(int(x))
    return str(x).strip()


def _erro_ficheiro_bloqueado(err: BaseException) -> bool:
    if isinstance(err, PermissionError):
        return True
    if isinstance(err, OSError) and err.errno in (11, 13, 32, 16):
        return True
    t = str(err)
    return "Permission denied" in t or "being used by another" in t


def _analisar_validade(val: Any) -> tuple[str, bool, str]:
    """
    Devolve (string a gravar em cadastro_extras['validade'], tem_erro, mensagem se erro).
    """
    if val is None or val == "":
        return "", True, "Data vazia no relatório."
    if isinstance(val, datetime):
        d = val.date()
        raw = d.isoformat()[:10]
    elif isinstance(val, date) and not isinstance(val, datetime):
        d = val
        raw = d.isoformat()[:10]
    else:
        raw0 = str(val).split()[0].strip()[:32]
        if not raw0 or raw0 in ("None", "nan") or "1899" in raw0:
            return (
                raw0[:16] if raw0 else "",
                True,
                "Data inválida (1899) detectada no relatório original.",
            )
        try:
            d = datetime.strptime(raw0[:10], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            return (
                raw0[:16] if raw0 else "",
                True,
                "Formato de data não reconhecido.",
            )
        raw = d.isoformat()[:10]

    if "1899" in raw or raw == "1899-12-31":
        return raw, True, "Data inválida (1899) detectada no relatório original."
    if d.year < 2000:
        return raw, True, f"Data muito antiga: {raw}"
    return raw, False, ""


def _id_sintetico_por_nome(nome: str) -> str:
    """ID único ≤64 para overlay sem código ERP, estável para o mesmo nome."""
    n = nome.strip()[:200]
    h = hashlib.blake2s(n.encode("utf-8"), digest_size=10).hexdigest()
    return f"agro:vn:{h}"


def _indice_cabecalho_csv(linhas: list[str]) -> int:
    for i, raw in enumerate(linhas):
        s = raw.lstrip("\ufeff").lstrip()
        if s.startswith("Código") or s.startswith("Codigo"):
            return i
        if "Produto" in s and (
            "Código" in s
            or "Codigo" in s
            or "Lote" in s
            or "Validade" in s
        ):
            return i
    h0 = linhas[0] if linhas else ""
    if _MARCADOR_GERACAO in h0 or _MARCADOR_GERACAO_ASC in h0:
        return 1
    for i, raw in enumerate(linhas):
        if "Produto" in raw or "Código" in raw or "Codigo" in raw:
            return i
    return 0


def _ler_csv(caminho: str) -> list[dict[str, Any]]:
    with open(caminho, "r", encoding="utf-8-sig", newline="") as f:
        lines = f.readlines()
    if not lines:
        return []
    start = _indice_cabecalho_csv(lines)
    return list(csv.DictReader(lines[start:]))


def _linha_parece_cabecalho_xlsx(row: Any) -> bool:
    if not row:
        return False
    for c in row:
        if c is None:
            continue
        s = str(c)
        if "Produto" in s or "Código" in s or "Codigo" in s or "Codi" in s:
            return True
    return False


def _ler_xlsx(caminho: str) -> list[dict[str, Any]]:
    """
    Lê a folha ativa com read_only + data_only; procura a linha de cabeçalho
    (Código / Produto) em qualquer posição, sem exigir meta na 1.ª linha.
    """
    import openpyxl

    out: list[dict[str, Any]] = []
    wb = openpyxl.load_workbook(caminho, data_only=True, read_only=True)
    try:
        ws = wb.active
        rows = ws.iter_rows(values_only=True)
        headers: list[str] | None = None
        for row in rows:
            if not row:
                continue
            if _linha_parece_cabecalho_xlsx(row):
                headers = [(_as_str_cel(c) or f"_col_{i}") for i, c in enumerate(row)]
                break
        if not headers:
            return []
        for row_cells in rows:
            if not row_cells:
                continue
            c_prod = None
            c_cod = None
            for j, h in enumerate(headers):
                c = row_cells[j] if j < len(row_cells) else None
                if h in ("Código", "Codigo", "codigo") or h.startswith("Códi"):
                    c_cod = c
                if h in ("Produto", "Product", "Nome"):
                    c_prod = c
            if c_cod is None and headers:
                c_cod = row_cells[0] if len(row_cells) else None
            if c_prod is None:
                for j, h in enumerate(headers):
                    if h == "Produto" or h == "Product":
                        c_prod = row_cells[j] if j < len(row_cells) else None
                        break
            has_code = c_cod is not None and str(c_cod).strip() not in ("", "None")
            has_name = c_prod is not None and str(c_prod).strip() not in ("", "None")
            if not has_code and not has_name and all(
                (v is None or str(v).strip() in ("", "None")) for v in row_cells
            ):
                continue
            if not has_code and not has_name:
                continue
            d = {
                headers[i]: (row_cells[i] if i < len(row_cells) else None)
                for i in range(len(headers))
            }
            out.append(d)
    finally:
        wb.close()
    return out


def _qtd_saldo_decimal_de_valor(v: Any) -> Decimal | None:
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return Decimal(str(v)).quantize(Decimal("0.01"))
    s = _as_str_cel(v).strip()
    if not s or s in ("None", "nan"):
        return None
    s = s.replace(" ", "")
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    try:
        return Decimal(s).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _qtd_saldo_da_linha(row: dict[str, Any]) -> Decimal:
    """Lê quantidade a partir de colunas comuns (incl. C+V de relatórios); default 0."""
    chaves_diretas = (
        "Saldo",
        "Qtd",
        "Quantidade",
        "Qtde",
        "Estoque",
        "Qtd.",
        "C+V",
        "C + V",
        "C+ V",
        "C +V",
        "SALDO",
        "CV",
        "Estoque Disponivel",
        "Estoque disponível",
    )
    for k in chaves_diretas:
        if k not in row:
            continue
        d = _qtd_saldo_decimal_de_valor(row.get(k))
        if d is not None:
            return d

    for k, v in row.items():
        if v in (None, ""):
            continue
        if k is None:
            continue
        norm = (
            "".join(
                c
                for c in unicodedata.normalize("NFKD", str(k))
                if not unicodedata.combining(c)
            )
            .lower()
            .replace(" ", "")
        )
        if norm in (
            "lote",
            "codigo",
            "produto",
            "product",
            "validade",
            "venc",
            "vencimento",
            "descricao",
            "id",
            "marca",
        ):
            continue
        if not any(
            x in norm
            for x in (
                "c+v",
                "saldo",
                "estoque",
                "qtd",
                "qtde",
                "quant",
                "dispon",
                "fisico",
            )
        ):
            continue
        d = _qtd_saldo_decimal_de_valor(v)
        if d is not None:
            return d
    return Decimal("0.00")


def _row_codigo_nome(row: dict[str, Any]) -> tuple[str, str]:
    cv = _as_str_cel(
        (row.get("Código") or row.get("Codigo") or row.get("codigo") or "")
    )[:64]
    nome = _as_str_cel(
        (row.get("Produto") or row.get("Product") or row.get("nome") or "")
    )[:300]
    return cv, nome


def _resolver_overlay(
    codigo: str, nome_produto: str
) -> ProdutoGestaoOverlayAgro | None:
    codigo = (codigo or "").strip()[:64]
    nome_produto = (nome_produto or "").strip()[:300]
    code_ok = bool(codigo) and codigo.lower() != "none"

    if code_ok:
        o = ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=codigo).first()
        if o:
            if nome_produto and not (o.nome or "").strip():
                o.nome = nome_produto
                o.save(update_fields=["nome"])
            return o

    if nome_produto:
        o = ProdutoGestaoOverlayAgro.objects.filter(nome__iexact=nome_produto).first()
        if o:
            return o

    if code_ok:
        o = ProdutoGestaoOverlayAgro(produto_externo_id=codigo, nome=nome_produto)
        o.save()
        return o

    if nome_produto:
        sid = _id_sintetico_por_nome(nome_produto)
        o, created = ProdutoGestaoOverlayAgro.objects.get_or_create(
            produto_externo_id=sid,
            defaults={"nome": nome_produto},
        )
        if not created and not (o.nome or "").strip():
            o.nome = nome_produto
            o.save(update_fields=["nome"])
        return o

    return None


class Command(BaseCommand):
    help = (
        "Importa validades por Código ou Nome (CSV / Excel .xlsx). "
        "Feche o ficheiro no Excel antes de correr."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "arquivo",
            type=str,
            help="Caminho do ficheiro .csv ou .xlsx (caminho curto ajuda, ex. lista.xlsx).",
        )

    def handle(self, *args, **options):
        caminho = (options.get("arquivo") or "").strip()
        if not caminho:
            self.stdout.write(self.style.ERROR("Indique o caminho do ficheiro."))
            return
        if not os.path.exists(caminho):
            self.stdout.write(self.style.ERROR(f"Ficheiro não encontrado: {caminho}"))
            return
        if not os.path.isfile(caminho):
            self.stdout.write(self.style.ERROR(f"Não é um ficheiro: {caminho}"))
            return

        ext = os.path.splitext(caminho)[1].lower()
        try:
            if ext == ".xlsx":
                dados = _ler_xlsx(caminho)
            elif ext in (".csv", ".txt"):
                dados = _ler_csv(caminho)
            else:
                self.stdout.write(
                    self.style.ERROR(
                        f"Extensão não suportada: {ext!r}. Use .csv, .txt ou .xlsx."
                    )
                )
                return
        except (PermissionError, OSError) as e:
            if _erro_ficheiro_bloqueado(e):
                self.stdout.write(self.style.ERROR(_MSG_FECHE_EXCEL))
            else:
                self.stdout.write(self.style.ERROR(f"Erro ao aceder ao ficheiro: {e}"))
            return
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Erro ao ler ficheiro: {e}"))
            return

        sucesso = 0
        alertas = 0
        skipped = 0

        for row in dados:
            cv, nome_p = _row_codigo_nome(row)
            if not cv and not nome_p:
                skipped += 1
                continue

            ov = _resolver_overlay(cv, nome_p)
            if ov is None:
                skipped += 1
                continue

            lote_val = row.get("Lote")
            lote = _as_str_cel(lote_val)[:80] if lote_val is not None else ""

            validade_raw, tem_erro, msg = _analisar_validade(row.get("Validade"))
            if tem_erro:
                ex = (
                    dict(ov.cadastro_extras) if isinstance(ov.cadastro_extras, dict) else {}
                )
                ex["validade"] = (validade_raw or "")[:16] if validade_raw else ex.get(
                    "validade", ""
                )
                ex["lote"] = lote[:100] if lote else ex.get("lote", "")
                ex["validade_alerta"] = True
                ex["validade_msg"] = (msg or "Revisar data (origem do relatório).")[:300]
                ov.cadastro_extras = ex
                ov.save(update_fields=["cadastro_extras", "atualizado_em"])
                alertas += 1
            else:
                try:
                    d = datetime.strptime(
                        (validade_raw or "")[:10], "%Y-%m-%d"
                    ).date()
                except (ValueError, TypeError):
                    alertas += 1
                    ex = (
                        dict(ov.cadastro_extras)
                        if isinstance(ov.cadastro_extras, dict)
                        else {}
                    )
                    ex["validade_alerta"] = True
                    ex["validade_msg"] = "Data inválida após análise."
                    ov.cadastro_extras = ex
                    ov.save(update_fields=["cadastro_extras", "atualizado_em"])
                else:
                    lote_c = lote.strip()[:100] or "—"
                    qtd = _qtd_saldo_da_linha(row)
                    EstoqueLote.objects.update_or_create(
                        overlay=ov,
                        lote_codigo=lote_c,
                        defaults={"data_validade": d, "quantidade_atual": qtd},
                    )
                    sync_overlay_validade_resumo_de_lotes(ov)
            sucesso += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"Agro Mais: {sucesso} produtos atualizados | {alertas} alertas de revisão"
                + (f" | {skipped} linhas vazias ignoradas." if skipped else ".")
            )
        )
