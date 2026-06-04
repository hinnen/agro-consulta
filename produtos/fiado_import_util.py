"""Importação de fiados pendentes (planilha ERP) → FiadoTituloAgro."""

from __future__ import annotations

import csv
import re
import unicodedata
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from django.db import transaction

from produtos.cliente_saldos_import_util import _norm_header, _norm_nome, _norm_nome_match_key
from produtos.fiado_gestao_util import _dec, registrar_evento_fiado, titulo_snapshot
from produtos.models import ClienteAgro, FiadoEventoAgro, FiadoTituloAgro


def _parse_data_br(val) -> date | None:
    if isinstance(val, date):
        return val
    if isinstance(val, datetime):
        return val.date()
    s = str(val or "").strip()
    if not s:
        return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except ValueError:
            continue
    return None


def _map_col(headers: list[str]) -> dict[str, str | None]:
    norm = {_norm_header(h): h for h in headers}
    out: dict[str, str | None] = {}

    def pick(keys: tuple[str, ...]) -> str | None:
        for k in keys:
            if k in norm:
                return norm[k]
        return None

    out["codigo"] = pick(("codigo", "código", "id", "id cliente"))
    out["cliente"] = pick(("cliente", "nome", "nome cliente"))
    out["documento"] = pick(
        ("numero documento", "número documento", "documento", "num documento", "pedido")
    )
    out["vencimento"] = pick(("vencimento", "data vencimento", "dt vencimento"))
    out["situacao"] = pick(("situacao", "situação", "status"))
    out["valor"] = pick(("valor", "valor bruto", "vlr"))
    out["valor_pago"] = pick(("valor pago", "pago", "vlr pago"))
    out["descricao"] = pick(("descricao", "descrição", "observacao", "observação"))
    out["forma"] = pick(("forma de pagamento", "forma pagamento", "forma"))
    return out


def _ler_linhas(path: Path) -> tuple[list[str], list[dict[str, Any]]]:
    suf = path.suffix.lower()
    if suf == ".csv":
        for enc in ("utf-8-sig", "latin-1", "cp1252"):
            try:
                with path.open("r", encoding=enc, newline="") as f:
                    reader = csv.DictReader(f, delimiter=";")
                    if reader.fieldnames and len(reader.fieldnames) == 1:
                        f.seek(0)
                        reader = csv.DictReader(f, delimiter=",")
                    headers = list(reader.fieldnames or [])
                    rows = [dict(r) for r in reader]
                    return headers, rows
            except UnicodeDecodeError:
                continue
        raise ValueError("Não foi possível ler o CSV (encoding).")
    if suf in (".xlsx", ".xls"):
        try:
            import openpyxl
        except ImportError as exc:
            raise ValueError("Instale openpyxl para importar XLSX.") from exc
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        ws = wb.active
        it = ws.iter_rows(values_only=True)
        headers = [str(c or "").strip() for c in next(it, [])]
        rows = []
        for row in it:
            if not any(row):
                continue
            d = {}
            for i, h in enumerate(headers):
                if h:
                    d[h] = row[i] if i < len(row) else None
            rows.append(d)
        wb.close()
        return headers, rows
    raise ValueError("Formato não suportado. Use .csv ou .xlsx.")


def _norm_nome_fiado_match(s: str) -> str:
    """Chave de casamento: ignora prefixo numérico, parênteses, emoji e sufixo « a » solto."""
    t = _norm_nome_match_key(s)
    t = re.sub(r"^[^A-Z0-9ÁÉÍÓÚÃÕÇ\s]+", "", t)
    t = re.sub(r"\([^)]*\)", " ", t)
    t = re.sub(r"\s+[A-ZÁÉÍÓÚÃÕÇ]$", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _codigo_planilha_str(val) -> str:
    if val is None:
        return ""
    if isinstance(val, float) and val == int(val):
        return str(int(val))
    s = str(val).strip()
    if s.endswith(".0") and s[:-2].isdigit():
        return s[:-2]
    return s


def _resolver_cliente(codigo: str, nome: str) -> ClienteAgro | None:
    cod = _codigo_planilha_str(codigo)
    if cod:
        cli = ClienteAgro.objects.filter(externo_id=cod).first()
        if cli:
            return cli
        if cod.isdigit():
            cli = ClienteAgro.objects.filter(pk=int(cod)).first()
            if cli:
                return cli
    nm = (nome or "").strip()
    if not nm:
        return None
    key = _norm_nome_fiado_match(nm)
    candidatos = [
        c
        for c in ClienteAgro.objects.filter(ativo=True).only("pk", "nome", "externo_id")
        if _norm_nome_fiado_match(c.nome) == key
    ]
    if len(candidatos) == 1:
        return candidatos[0]
    if len(candidatos) > 1:
        ativos = [c for c in candidatos if c.ativo]
        if len(ativos) == 1:
            return ativos[0]
    parts = key.split()
    if len(parts) >= 2:
        qs = ClienteAgro.objects.filter(ativo=True, nome__icontains=parts[0]).only(
            "pk", "nome", "externo_id", "ativo"
        )
        cands = [c for c in qs if parts[1] in _norm_nome(c.nome)]
        if len(cands) == 1:
            return cands[0]
    return None


def _forma_e_fiado(val) -> bool:
    fn = _norm_header(str(val or ""))
    if not fn:
        return True
    return "credito loja" in fn or fn == "fiado" or "crédito loja" in fn


def _situacao_de_planilha(sit_txt: str, valor_bruto: Decimal, valor_pago: Decimal) -> str:
    if valor_pago >= valor_bruto and valor_bruto > 0:
        return FiadoTituloAgro.Situacao.QUITADO
    s = _norm_header(sit_txt)
    if "parcial" in s:
        return FiadoTituloAgro.Situacao.PARCIAL
    if "pago" in s and "nao" not in s and "não" not in s:
        return FiadoTituloAgro.Situacao.QUITADO
    if valor_pago > 0:
        return FiadoTituloAgro.Situacao.PARCIAL
    return FiadoTituloAgro.Situacao.ABERTO


def _parcela_de_descricao(desc: str) -> tuple[int, int]:
    m = re.search(r"parcela\s+(\d+)\s+de\s+(\d+)", str(desc or ""), re.I)
    if m:
        return int(m.group(1)), int(m.group(2))
    return 1, 1


def aplicar_importacao_fiados(
    path: Path,
    *,
    dry_run: bool = False,
    usuario: str = "",
) -> dict[str, Any]:
    headers, rows = _ler_linhas(path)
    cols = _map_col(headers)
    if not cols.get("cliente") and not cols.get("codigo"):
        raise ValueError("Planilha precisa de coluna Cliente ou Código.")

    criados = 0
    atualizados = 0
    ignorados = 0
    erros: list[str] = []
    sem_cliente: list[str] = []

    with transaction.atomic():
        for i, row in enumerate(rows, start=2):
            try:
                if cols.get("forma") and not _forma_e_fiado(row.get(cols["forma"])):
                    ignorados += 1
                    continue
                codigo = (
                    _codigo_planilha_str(row.get(cols["codigo"]))
                    if cols.get("codigo")
                    else ""
                )
                nome = str(row.get(cols["cliente"] or "") or "").strip() if cols.get("cliente") else ""
                doc = str(row.get(cols["documento"] or "") or "").strip() if cols.get("documento") else ""
                venc_raw = row.get(cols["vencimento"]) if cols.get("vencimento") else None
                venc = _parse_data_br(venc_raw)
                if not venc:
                    ignorados += 1
                    continue
                valor_bruto = _dec(row.get(cols["valor"])) if cols.get("valor") else Decimal("0")
                if valor_bruto <= 0:
                    ignorados += 1
                    continue
                valor_pago = _dec(row.get(cols["valor_pago"])) if cols.get("valor_pago") else Decimal("0")
                if valor_pago >= valor_bruto:
                    ignorados += 1
                    continue
                desc = str(row.get(cols["descricao"] or "") or "").strip() if cols.get("descricao") else ""
                sit_txt = str(row.get(cols["situacao"] or "") or "") if cols.get("situacao") else ""
                p_num, p_tot = _parcela_de_descricao(desc)
                cli = _resolver_cliente(codigo, nome)
                if not cli and nome:
                    sem_cliente.append(nome[:80])
                chave = f"import:{codigo or nome}:{doc}:{p_num}:{venc.isoformat()}"[:120]
                situacao = _situacao_de_planilha(sit_txt, valor_bruto, valor_pago)
                payload_titulo = {
                    "chave_unica": chave,
                    "cliente_nome": nome or (cli.nome if cli else ""),
                    "cliente_codigo": codigo or (cli.externo_id if cli else ""),
                    "numero_documento": doc,
                    "parcela_num": p_num,
                    "parcela_total": p_tot,
                    "vencimento": venc.isoformat(),
                    "valor_bruto": float(valor_bruto),
                    "valor_pago": float(valor_pago),
                    "situacao": situacao,
                    "descricao": desc[:500],
                }
                if dry_run:
                    criados += 1
                    continue
                existente = FiadoTituloAgro.objects.filter(chave_unica=chave).first()
                if existente:
                    existente.valor_pago = valor_pago
                    existente.situacao = situacao
                    existente.descricao = desc[:500]
                    existente.save(update_fields=["valor_pago", "situacao", "descricao", "atualizado_em"])
                    atualizados += 1
                    continue
                titulo = FiadoTituloAgro.objects.create(
                    chave_unica=chave,
                    cliente_agro=cli,
                    venda_agro=None,
                    cliente_nome=nome or (cli.nome if cli else "Cliente"),
                    cliente_codigo=codigo or (cli.externo_id if cli else ""),
                    numero_documento=doc,
                    parcela_num=p_num,
                    parcela_total=p_tot,
                    vencimento=venc,
                    valor_bruto=valor_bruto,
                    valor_pago=valor_pago,
                    situacao=situacao,
                    origem=FiadoTituloAgro.Origem.IMPORTACAO,
                    descricao=desc[:500],
                    dados_snapshot_json={"linha_planilha": i, "row": {k: str(v)[:200] for k, v in row.items()}},
                )
                registrar_evento_fiado(
                    FiadoEventoAgro.Tipo.IMPORT,
                    cliente_agro=cli,
                    titulo=titulo,
                    payload={"titulo": titulo_snapshot(titulo), "arquivo": str(path.name)},
                    usuario=usuario,
                )
                criados += 1
            except Exception as exc:
                erros.append(f"Linha {i}: {exc}")

        if dry_run:
            transaction.set_rollback(True)

    return {
        "ok": True,
        "criados": criados,
        "atualizados": atualizados,
        "ignorados": ignorados,
        "erros": erros[:20],
        "sem_cliente": len(sem_cliente),
        "sem_cliente_amostra": sem_cliente[:10],
    }
