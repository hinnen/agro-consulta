import json
import re
from decimal import Decimal

from django.shortcuts import render
from django.http import JsonResponse
from django.core.cache import cache
from django.views.decorators.http import require_GET, require_POST
from django.utils import timezone

from base.models import Empresa, PerfilUsuario, IntegracaoERP
from estoque.models import AjusteRapidoEstoque
from integracoes.texto import normalizar, expandir_tokens
from integracoes.venda_erp_mongo import VendaERPMongoClient
from integracoes.venda_erp_api import VendaERPAPIClient


# --- CONEXÃO MONGO ---
_cached_mongo_client = None


def obter_conexao_mongo():
    global _cached_mongo_client
    try:
        if _cached_mongo_client is None:
            _cached_mongo_client = VendaERPMongoClient()
        db = _cached_mongo_client.db if _cached_mongo_client else None
        return _cached_mongo_client, db
    except Exception:
        _cached_mongo_client = None
        return None, None


# --- AUXILIARES GERAIS ---
def _somente_alnum(txt):
    return re.sub(r"[^a-zA-Z0-9]", "", str(txt or ""))


def _regex_exato_ci(valor):
    return re.compile(rf"^{re.escape(str(valor))}$", re.IGNORECASE)


def _regex_inicio_ci(valor):
    return re.compile(rf"^{re.escape(str(valor))}", re.IGNORECASE)


def _regex_contem_ci(valor):
    return re.compile(re.escape(str(valor)), re.IGNORECASE)


def _termo_parece_codigo(termo_original):
    termo = str(termo_original or "").strip()
    termo_limpo = _somente_alnum(termo)

    if not termo_limpo:
        return False

    # EAN / barras
    if termo_limpo.isdigit() and len(termo_limpo) >= 6:
        return True

    # Código misto tipo GM123 / 123ABC
    tem_letra = any(c.isalpha() for c in termo_limpo)
    tem_numero = any(c.isdigit() for c in termo_limpo)
    if tem_letra and tem_numero and len(termo_limpo) >= 3 and " " not in termo:
        return True

    # Prefixos comuns digitados
    if termo_limpo.lower().startswith("gm") and len(termo_limpo) >= 2:
        return True

    return False


def _codigo_exact_conditions(termo_limpo):
    conds = [
        {"Codigo": {"$regex": _regex_exato_ci(termo_limpo)}},
        {"CodigoNFe": {"$regex": _regex_exato_ci(termo_limpo)}},
        {"CodigoBarras": {"$regex": _regex_exato_ci(termo_limpo)}},
        {"EAN_NFe": {"$regex": _regex_exato_ci(termo_limpo)}},
    ]

    if termo_limpo.isdigit():
        try:
            numero = int(termo_limpo)
            conds.extend([
                {"Codigo": numero},
                {"CodigoNFe": numero},
                {"CodigoBarras": numero},
                {"EAN_NFe": numero},
            ])
        except Exception:
            pass

    return conds


def _codigo_prefix_conditions(termo_limpo):
    return [
        {"Codigo": {"$regex": _regex_inicio_ci(termo_limpo)}},
        {"CodigoNFe": {"$regex": _regex_inicio_ci(termo_limpo)}},
        {"CodigoBarras": {"$regex": _regex_inicio_ci(termo_limpo)}},
        {"EAN_NFe": {"$regex": _regex_inicio_ci(termo_limpo)}},
    ]


def _extrair_codigo_barras(p):
    return (
        p.get("CodigoBarras")
        or p.get("EAN_NFe")
        or p.get("EAN")
        or p.get("CodigoDeBarras")
        or ""
    )


def _mapear_estoques_por_produto(estoques, client):
    mapa = {}

    for e in estoques:
        pid = str(e.get("ProdutoID") or "")
        dep = str(e.get("DepositoID") or "")
        saldo = float(e.get("Saldo", 0) or 0)

        if pid not in mapa:
            mapa[pid] = {"centro": 0.0, "vila": 0.0}

        if dep == client.DEPOSITO_CENTRO:
            mapa[pid]["centro"] += saldo
        elif dep == client.DEPOSITO_VILA_ELIAS:
            mapa[pid]["vila"] += saldo

    return mapa


# --- AUXILIARES DE IMAGEM ---
def _formatar_url_imagem(img_str):
    img_str = str(img_str or "").strip()
    if not img_str or img_str == "None":
        return ""
    if img_str.startswith("data:image"):
        return img_str
    if len(img_str) > 1000 and not img_str.startswith("http"):
        return "data:image/jpeg;base64," + img_str

    base_url = "https://cw.vendaerp.com.br"
    try:
        integ = IntegracaoERP.objects.filter(ativo=True).first()
        if integ and integ.url_base:
            base_url = integ.url_base.rstrip("/")
    except Exception:
        pass

    if img_str.startswith("Uploads/"):
        return base_url + "/" + img_str
    elif img_str.startswith("/Uploads/"):
        return base_url + img_str
    elif not img_str.startswith("http"):
        return base_url + "/Uploads/Produtos/" + img_str.lstrip("/")

    return img_str


def _extrair_imagem_produto(p, mapa_imagens, pid):
    if mapa_imagens.get(pid):
        return mapa_imagens.get(pid)
    if p.get("Codigo") and mapa_imagens.get(str(p.get("Codigo"))):
        return mapa_imagens.get(str(p.get("Codigo")))
    if p.get("CodigoNFe") and mapa_imagens.get(str(p.get("CodigoNFe"))):
        return mapa_imagens.get(str(p.get("CodigoNFe")))

    for c in [
        "UrlImagem",
        "Imagem",
        "CaminhoImagem",
        "Foto",
        "Url",
        "UrlImagemPrincipal",
        "ImagemPrincipal",
        "ImagemBase64",
        "FotoBase64",
    ]:
        val = p.get(c)
        if val and isinstance(val, str) and len(val.strip()) > 2:
            return val

    for c in ["Imagens", "Fotos", "ImagemProduto", "ProdutoImagem"]:
        arr = p.get(c)
        if isinstance(arr, list) and len(arr) > 0:
            i = arr[0]
            if isinstance(i, dict):
                for sub_c in [
                    "Url",
                    "UrlImagem",
                    "Caminho",
                    "Imagem",
                    "Path",
                    "ImagemBase64",
                    "Base64",
                ]:
                    val = i.get(sub_c)
                    if val and isinstance(val, str) and len(val.strip()) > 2:
                        return val
            elif isinstance(i, str):
                return i

    return ""


# --- VIEWS DE PÁGINA ---
def consulta_produtos(request):
    return render(request, "produtos/consulta_produtos.html")


def historico_ajustes(request):
    ajustes = AjusteRapidoEstoque.objects.all().order_by("-criado_em")
    return render(request, "produtos/historico_ajustes.html", {"ajustes": ajustes})


def sugestao_transferencia(request):
    return render(request, "produtos/transferencias.html")


def compras_view(request):
    return render(request, "produtos/compras.html")


def ajuste_mobile_view(request):
    if not request.session.get("mobile_auth"):
        return render(request, "produtos/ajuste_mobile_login.html")
    return render(request, "produtos/mobile_ajuste.html")


# --- MOTOR DE BUSCA ÚNICO ---
def motor_de_busca_agro(termo_original, db, client, limit=20):
    termo_original = str(termo_original or "").strip()
    if not termo_original:
        return []

    termo_limpo = _somente_alnum(termo_original)
    palavras = [p for p in termo_original.split() if p]
    base_filter = {"CadastroInativo": {"$ne": True}}

    candidatos = []
    vistos = set()

    def adicionar(lista):
        for item in lista:
            pid = str(item.get("Id") or item.get("_id"))
            if pid not in vistos:
                vistos.add(pid)
                candidatos.append(item)

    # 1) Código / barras exato
    if termo_limpo and _termo_parece_codigo(termo_original):
        query_cod_exato = {
            **base_filter,
            "$or": [
                {"Codigo": _regex_exato_ci(termo_limpo)},
                {"CodigoNFe": _regex_exato_ci(termo_limpo)},
                {"CodigoBarras": _regex_exato_ci(termo_limpo)},
                {"EAN_NFe": _regex_exato_ci(termo_limpo)},
            ]
        }

        if termo_limpo.isdigit():
            try:
                numero = int(termo_limpo)
                query_cod_exato["$or"].extend([
                    {"Codigo": numero},
                    {"CodigoNFe": numero},
                    {"CodigoBarras": numero},
                    {"EAN_NFe": numero},
                ])
            except Exception:
                pass

        exatos = list(db[client.col_p].find(query_cod_exato).limit(max(limit, 10)))
        if exatos:
            return exatos[:limit]

        # 1.1) Prefixo de código
        query_cod_prefixo = {
            **base_filter,
            "$or": [
                {"Codigo": _regex_inicio_ci(termo_limpo)},
                {"CodigoNFe": _regex_inicio_ci(termo_limpo)},
                {"CodigoBarras": _regex_inicio_ci(termo_limpo)},
                {"EAN_NFe": _regex_inicio_ci(termo_limpo)},
            ]
        }
        adicionar(list(db[client.col_p].find(query_cod_prefixo).limit(30)))

    # 2) Busca por palavras com expansão
    condicoes_and = []
    for p in palavras:
        tokens = expandir_tokens(p)
        p_norm = normalizar(p)
        if p_norm and p_norm not in tokens:
            tokens.append(p_norm)

        regex_tokens = []
        for t in tokens:
            if not t:
                continue
            regex_tokens.append(_regex_contem_ci(t))

        if regex_tokens:
            condicoes_and.append({
                "$or": [
                    {"BuscaTexto": {"$in": regex_tokens}},
                    {"Nome": {"$in": regex_tokens}},
                    {"Marca": {"$in": regex_tokens}},
                    {"Codigo": {"$in": regex_tokens}},
                    {"CodigoNFe": {"$in": regex_tokens}},
                    {"CodigoBarras": {"$in": regex_tokens}},
                    {"EAN_NFe": {"$in": regex_tokens}},
                ]
            })

    if condicoes_and:
        adicionar(list(db[client.col_p].find({
            **base_filter,
            "$and": condicoes_and
        }).limit(80)))

    # 3) Fallback por frase inteira
    if len(candidatos) < limit:
        termo_regex = _regex_contem_ci(termo_original)
        adicionar(list(db[client.col_p].find({
            **base_filter,
            "$or": [
                {"Nome": termo_regex},
                {"Marca": termo_regex},
                {"Codigo": termo_regex},
                {"CodigoNFe": termo_regex},
                {"CodigoBarras": termo_regex},
                {"EAN_NFe": termo_regex},
            ]
        }).limit(80)))

    # 4) Ordenação de relevância
    termo_norm = normalizar(termo_original)
    termo_limpo_lower = termo_limpo.lower()

    def score(p):
        nome = str(p.get("Nome") or "")
        marca = str(p.get("Marca") or "")
        codigo = str(p.get("Codigo") or "")
        codigo_nfe = str(p.get("CodigoNFe") or "")
        codigo_barras = str(_extrair_codigo_barras(p) or "")

        nome_norm = normalizar(nome)
        marca_norm = normalizar(marca)
        codigo_alnum = _somente_alnum(codigo).lower()
        codigo_nfe_alnum = _somente_alnum(codigo_nfe).lower()
        barras_alnum = _somente_alnum(codigo_barras).lower()

        s = 0

        if termo_limpo_lower:
            if codigo_alnum == termo_limpo_lower:
                s += 5000
            if codigo_nfe_alnum == termo_limpo_lower:
                s += 4900
            if barras_alnum == termo_limpo_lower:
                s += 5200

            if codigo_alnum.startswith(termo_limpo_lower):
                s += 1800
            if codigo_nfe_alnum.startswith(termo_limpo_lower):
                s += 1700
            if barras_alnum.startswith(termo_limpo_lower):
                s += 1900

        if termo_norm:
            if nome_norm == termo_norm:
                s += 1600
            elif nome_norm.startswith(termo_norm):
                s += 1200
            elif termo_norm in nome_norm:
                s += 700

            if marca_norm.startswith(termo_norm):
                s += 200

        if palavras:
            presentes = 0
            for p_txt in palavras:
                p_norm = normalizar(p_txt)
                if p_norm and p_norm in nome_norm:
                    presentes += 1
            s += presentes * 120
            if presentes == len(palavras):
                s += 300

        s -= len(nome_norm.split())
        return s

    candidatos.sort(key=lambda p: (-score(p), str(p.get("Nome") or "").lower()))
    return candidatos[:limit]


# --- APIs DE BUSCA ---
@require_GET
def api_buscar_produtos(request):
    q = request.GET.get("q", "").strip()
    client, db = obter_conexao_mongo()
    if not db or not q:
        return JsonResponse({"produtos": []})

    try:
        prods = motor_de_busca_agro(q, db, client, limit=20)
        p_ids = [str(p.get("Id") or p["_id"]) for p in prods]

        estoques = list(db[client.col_e].find({"ProdutoID": {"$in": p_ids}}))
        estoque_map = _mapear_estoques_por_produto(estoques, client)

        ajustes_bd = AjusteRapidoEstoque.objects.filter(produto_externo_id__in=p_ids)
        ajustes_map = {(aj.produto_externo_id, aj.deposito): aj for aj in ajustes_bd}

        res = []
        for p in prods:
            pid = str(p.get("Id") or p["_id"])

            saldo_centro_erp = float(estoque_map.get(pid, {}).get("centro", 0.0))
            saldo_vila_erp = float(estoque_map.get(pid, {}).get("vila", 0.0))

            ac = ajustes_map.get((pid, "centro"))
            av = ajustes_map.get((pid, "vila"))

            saldo_centro = (
                float(ac.saldo_informado) + (saldo_centro_erp - float(ac.saldo_erp_referencia))
                if ac else saldo_centro_erp
            )
            saldo_vila = (
                float(av.saldo_informado) + (saldo_vila_erp - float(av.saldo_erp_referencia))
                if av else saldo_vila_erp
            )

            codigo = p.get("Codigo") or ""
            codigo_nfe = p.get("CodigoNFe") or codigo or ""
            codigo_barras = _extrair_codigo_barras(p)

            res.append({
                "id": pid,
                "nome": p.get("Nome"),
                "marca": p.get("Marca") or "",
                "codigo": codigo,
                "codigo_nfe": codigo_nfe,
                "codigo_barras": codigo_barras,
                "preco_venda": float(p.get("ValorVenda") or p.get("PrecoVenda") or 0),
                "imagem": _formatar_url_imagem(_extrair_imagem_produto(p, {}, pid)),
                "saldo_centro": round(saldo_centro, 2),
                "saldo_vila": round(saldo_vila, 2),
                "saldo_centro_erp": round(saldo_centro_erp, 2),
                "saldo_vila_erp": round(saldo_vila_erp, 2),
                "saldo_erp_centro": round(saldo_centro_erp, 2),  # compatibilidade com mobile atual
                "saldo_erp_vila": round(saldo_vila_erp, 2),
            })

        return JsonResponse({"produtos": res})
    except Exception as e:
        return JsonResponse({"erro": str(e)}, status=500)


@require_GET
def api_buscar_compras(request):
    q = request.GET.get("q", "").strip()
    client, db = obter_conexao_mongo()
    if not db or not q:
        return JsonResponse({"produtos": []})

    try:
        prods = motor_de_busca_agro(q, db, client, limit=50)
        p_ids = [str(p.get("Id") or p["_id"]) for p in prods]

        estoques = list(db[client.col_e].find({"ProdutoID": {"$in": p_ids}}))
        estoque_map = _mapear_estoques_por_produto(estoques, client)

        ajustes_bd = AjusteRapidoEstoque.objects.filter(produto_externo_id__in=p_ids)
        ajustes_map = {(aj.produto_externo_id, aj.deposito): aj for aj in ajustes_bd}

        res = []
        for p in prods:
            pid = str(p.get("Id") or p["_id"])

            saldo_centro_erp = float(estoque_map.get(pid, {}).get("centro", 0.0))
            saldo_vila_erp = float(estoque_map.get(pid, {}).get("vila", 0.0))

            ac = ajustes_map.get((pid, "centro"))
            av = ajustes_map.get((pid, "vila"))

            saldo_centro = (
                float(ac.saldo_informado) + (saldo_centro_erp - float(ac.saldo_erp_referencia))
                if ac else saldo_centro_erp
            )
            saldo_vila = (
                float(av.saldo_informado) + (saldo_vila_erp - float(av.saldo_erp_referencia))
                if av else saldo_vila_erp
            )

            custo = float(str(p.get("PrecoCusto") or p.get("ValorCusto") or 0).replace(",", "."))
            preco_venda = float(p.get("ValorVenda") or p.get("PrecoVenda") or 0)
            codigo = p.get("Codigo") or ""
            codigo_nfe = p.get("CodigoNFe") or codigo or ""
            codigo_barras = _extrair_codigo_barras(p)

            res.append({
                "id": pid,
                "nome": p.get("Nome"),
                "marca": p.get("Marca") or "",
                "codigo": codigo,
                "codigo_nfe": codigo_nfe,
                "codigo_barras": codigo_barras,
                "preco_custo": custo,
                "preco_custo_acrescimo": custo,
                "preco_venda": preco_venda,
                "imagem": _formatar_url_imagem(_extrair_imagem_produto(p, {}, pid)),
                "saldo_centro": round(saldo_centro, 2),
                "saldo_vila": round(saldo_vila, 2),
                "saldo_centro_erp": round(saldo_centro_erp, 2),
                "saldo_vila_erp": round(saldo_vila_erp, 2),
            })

        return JsonResponse({"produtos": res})
    except Exception as e:
        return JsonResponse({"erro": str(e)}, status=500)


# --- APIs DE ESTOQUE E PEDIDO ---
@require_POST
def api_login_mobile(request):
    if PerfilUsuario.objects.filter(senha_rapida=request.POST.get("pin")).exists():
        request.session["mobile_auth"] = True
        return JsonResponse({"ok": True})
    return JsonResponse({"ok": False}, status=403)


@require_POST
def api_ajustar_estoque(request):
    pin = request.POST.get("pin")
    if (pin == "SESSAO" and request.session.get("mobile_auth")) or PerfilUsuario.objects.filter(
        senha_rapida=pin
    ).exists():
        try:
            empresa = Empresa.objects.filter(nome_fantasia="Agro Mais").first()
            AjusteRapidoEstoque.objects.create(
                empresa=empresa,
                produto_externo_id=request.POST.get("produto_id"),
                deposito=request.POST.get("deposito", "centro"),
                nome_produto=request.POST.get("nome_produto"),
                saldo_erp_referencia=Decimal(request.POST.get("saldo_atual", "0")),
                saldo_informado=Decimal(request.POST.get("novo_saldo", "0")),
            )
            cache.clear()
            return JsonResponse({"ok": True})
        except Exception as e:
            return JsonResponse({"ok": False, "erro": str(e)})
    return JsonResponse({"ok": False, "erro": "PIN INCORRETO"}, status=403)


@require_POST
def api_enviar_pedido_erp(request):
    try:
        data = json.loads(request.body)
        client_m, db = obter_conexao_mongo()

        dep_id = ""
        emp_id = ""

        if db is not None:
            est = db[client_m.col_e].find_one({"Deposito": {"$regex": "centro", "$options": "i"}})
            if est:
                dep_id = str(est.get("DepositoID"))
                emp_id = str(est.get("EmpresaID"))

        payload = {
            "statusSistema": "Orçamento",
            "cliente": data.get("cliente", "Consumidor Final"),
            "data": timezone.now().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "origemVenda": "Venda Direta",
            "empresa": "Agro Mais Centro",
            "deposito": "Deposito Centro",
            "vendedor": "Gm Agro Mais",
            "items": [],
        }

        if dep_id:
            payload["depositoID"] = dep_id
        if emp_id:
            payload["empresaID"] = emp_id

        for i in data.get("itens", []):
            payload["items"].append({
                "produtoID": i.get("id"),
                "codigo": i.get("id"),
                "unidade": "UN",
                "descricao": i.get("nome"),
                "quantidade": float(i.get("qtd")),
                "valorUnitario": float(i.get("preco")),
                "valorTotal": round(float(i.get("qtd")) * float(i.get("preco")), 2),
            })

        ok, status, res = VendaERPAPIClient().salvar_operacao_pdv(payload)
        return JsonResponse({"ok": ok, "mensagem": res})
    except Exception as e:
        return JsonResponse({"ok": False, "erro": str(e)})


# --- CARGA INICIAL E APIs AUXILIARES ---
@require_GET
def api_todos_produtos_local(request):
    client, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"erro": "Erro conexao"}, status=500)

    try:
        produtos = list(db[client.col_p].find({"CadastroInativo": {"$ne": True}}))
        p_ids = [str(p.get("Id") or p["_id"]) for p in produtos]

        estoques = list(db[client.col_e].find({"ProdutoID": {"$in": p_ids}}))
        ajustes_bd = AjusteRapidoEstoque.objects.filter(produto_externo_id__in=p_ids)
        ajustes_map = {(aj.produto_externo_id, aj.deposito): aj for aj in ajustes_bd}

        res = []

        for p in produtos:
            pid = str(p.get("Id") or p["_id"])

            sc = sum(
                float(e.get("Saldo", 0))
                for e in estoques
                if str(e.get("ProdutoID")) == pid and str(e.get("DepositoID")) == client.DEPOSITO_CENTRO
            )

            sv = sum(
                float(e.get("Saldo", 0))
                for e in estoques
                if str(e.get("ProdutoID")) == pid and str(e.get("DepositoID")) == client.DEPOSITO_VILA_ELIAS
            )

            ac = ajustes_map.get((pid, "centro"))
            av = ajustes_map.get((pid, "vila"))

            saldo_centro = round(
                float(ac.saldo_informado) + (sc - float(ac.saldo_erp_referencia)) if ac else sc,
                2
            )

            saldo_vila = round(
                float(av.saldo_informado) + (sv - float(av.saldo_erp_referencia)) if av else sv,
                2
            )

            preco_venda = float(p.get("ValorVenda") or p.get("PrecoVenda") or 0)

            try:
                preco_custo = float(str(p.get("PrecoCusto") or p.get("ValorCusto") or 0).replace(",", "."))
            except Exception:
                preco_custo = 0.0

            res.append({
                "id": pid,
                "nome": p.get("Nome"),
                "marca": p.get("Marca") or "",
                "codigo": p.get("Codigo") or "",
                "codigo_nfe": p.get("CodigoNFe") or p.get("Codigo") or "",
                "codigo_barras": p.get("CodigoBarras") or p.get("EAN_NFe") or "",
                "preco_venda": preco_venda,
                "preco_custo": preco_custo,
                "preco_custo_acrescimo": preco_custo,
                "saldo_centro": saldo_centro,
                "saldo_vila": saldo_vila,
                "saldo_centro_erp": round(sc, 2),
                "saldo_vila_erp": round(sv, 2),
                "saldo_erp_centro": round(sc, 2),
                "saldo_erp_vila": round(sv, 2),
                "busca_texto": normalizar(
                    f"{p.get('Nome')} {p.get('Marca')} {p.get('Codigo')} "
                    f"{p.get('CodigoNFe') or ''} {p.get('CodigoBarras') or p.get('EAN_NFe') or ''}"
                )
            })

        return JsonResponse({"produtos": res})

    except Exception as e:
        return JsonResponse({"erro": str(e)}, status=500)

def api_list_customers(request):
    client, db = obter_conexao_mongo()
    if not db:
        return JsonResponse({"clientes": []})

    try:
        clis = list(
            db[client.col_c].find({"CadastroInativo": {"$ne": True}}, {"Nome": 1, "Id": 1}).limit(1000)
        )
        res = [{"id": str(i.get("Id") or i.get("_id")), "nome": i.get("Nome").strip()} for i in clis]
        res.sort(key=lambda x: x["nome"])
        return JsonResponse({"clientes": res})
    except Exception:
        return JsonResponse({"clientes": []})


def api_buscar_clientes(request):
    client, db = obter_conexao_mongo()
    termo = request.GET.get("q", "")
    if not db:
        return JsonResponse({"clientes": []})

    try:
        clis = list(
            db[client.col_c].find({"Nome": {"$regex": termo, "$options": "i"}}, {"Nome": 1, "CpfCnpj": 1}).limit(10)
        )
        res = [{"nome": i.get("Nome"), "documento": i.get("CpfCnpj") or "Sem Doc"} for i in clis]
        return JsonResponse({"clientes": res})
    except Exception:
        return JsonResponse({"clientes": []})


def api_autocomplete_produtos(request):
    client, db = obter_conexao_mongo()
    termo = request.GET.get("q", "")
    if not db or len(termo) < 2:
        return JsonResponse({"sugestoes": []})

    try:
        ps = motor_de_busca_agro(termo, db, client, limit=8)
        res = [{"id": str(i.get("Id") or i.get("_id")), "nome": i.get("Nome")} for i in ps]
        return JsonResponse({"sugestoes": res})
    except Exception:
        return JsonResponse({"sugestoes": []})


def api_buscar_produto_id(request, id):
    client, db = obter_conexao_mongo()
    if not db:
        return JsonResponse({"erro": "DB Offline"}, status=500)

    p = db[client.col_p].find_one({"Id": id})
    if not p:
        return JsonResponse({"erro": "Nao encontrado"}, status=404)

    return JsonResponse({"id": id, "nome": p.get("Nome")})