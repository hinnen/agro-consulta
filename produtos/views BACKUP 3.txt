import json
import re
import unicodedata
from decimal import Decimal

from bson.objectid import ObjectId
from django.core.cache import cache
from django.http import JsonResponse
from django.shortcuts import render
from django.utils import timezone
from django.views.decorators.http import require_GET, require_POST

from base.models import Empresa, IntegracaoERP, PerfilUsuario
from estoque.models import AjusteRapidoEstoque
from integracoes.texto import expandir_tokens, normalizar
from integracoes.venda_erp_api import VendaERPAPIClient
from integracoes.venda_erp_mongo import VendaERPMongoClient


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


def _tokens_busca(txt):
    base = normalizar(txt)
    if not base:
        return []
    return [t for t in base.split() if t]


def _termo_parece_codigo(termo_original):
    """
    Evita sequestrar buscas como '15', '20', '25kg', etc.
    Só trata como código quando realmente parece código.
    """
    termo = str(termo_original or "").strip()
    termo_limpo = _somente_alnum(termo)

    if not termo_limpo:
        return False

    # Código de barras / EAN
    if termo_limpo.isdigit() and len(termo_limpo) >= 6:
        return True

    # Mistura de letras e números sem espaços: ex GM123, 123ABC
    tem_letra = any(c.isalpha() for c in termo_limpo)
    tem_numero = any(c.isdigit() for c in termo_limpo)
    if tem_letra and tem_numero and " " not in termo and len(termo_limpo) >= 4:
        return True

    # Códigos curtos puramente numéricos NÃO entram aqui
    return False


def _regex_inicio(valor):
    return re.compile(rf"^{re.escape(valor)}", re.IGNORECASE)


def _regex_contem(valor):
    return re.compile(re.escape(valor), re.IGNORECASE)


def _texto_produto_normalizado(produto):
    partes = [
        produto.get("Nome", ""),
        produto.get("Marca", ""),
        produto.get("Codigo", ""),
        produto.get("CodigoNFe", ""),
        produto.get("Categoria", ""),
        produto.get("Subcategoria", ""),
        produto.get("BuscaTexto", ""),
    ]
    return normalizar(" ".join(str(p or "") for p in partes))


def _eh_granel_produto(produto):
    texto = _texto_produto_normalizado(produto)
    return "granel" in texto


def _pontuar_produto(produto, termo_original, tokens_expandidos_por_token):
    """
    Ranking simples e rápido, priorizando:
    - código exato / prefixo
    - nome iniciando com a busca
    - frase completa no nome
    - todos os tokens presentes
    - marca
    - penalidade leve para granel
    """
    nome = str(produto.get("Nome") or "")
    marca = str(produto.get("Marca") or "")
    codigo = str(produto.get("Codigo") or "")
    codigo_nfe = str(produto.get("CodigoNFe") or "")
    codigo_barras = str(produto.get("CodigoBarras") or produto.get("EAN_NFe") or "")

    nome_norm = normalizar(nome)
    marca_norm = normalizar(marca)
    codigo_norm = normalizar(codigo)
    codigo_nfe_norm = normalizar(codigo_nfe)
    codigo_barras_norm = normalizar(codigo_barras)

    termo_norm = normalizar(termo_original)
    termo_alnum = _somente_alnum(termo_original).lower()

    score = 0

    # Código
    if termo_alnum:
        if codigo_norm == termo_alnum:
            score += 2000
        elif codigo_nfe_norm == termo_alnum:
            score += 1950
        elif codigo_barras_norm == termo_alnum:
            score += 2100
        elif codigo_norm.startswith(termo_alnum):
            score += 900
        elif codigo_nfe_norm.startswith(termo_alnum):
            score += 850
        elif codigo_barras_norm.startswith(termo_alnum):
            score += 950

    # Nome / frase
    if termo_norm:
        if nome_norm == termo_norm:
            score += 1600
        if nome_norm.startswith(termo_norm):
            score += 1300
        elif f" {termo_norm}" in f" {nome_norm}":
            score += 700

        if marca_norm == termo_norm:
            score += 500
        elif marca_norm.startswith(termo_norm):
            score += 250

    # Tokens
    todos_presentes = True
    qtd_tokens_fortes = 0

    for grupo in tokens_expandidos_por_token:
        presente_no_nome = False
        presente_na_marca = False
        prefixo_no_nome = False

        for tk in grupo:
            tk = normalizar(tk)
            if not tk:
                continue

            if tk in nome_norm:
                presente_no_nome = True
            if tk in marca_norm:
                presente_na_marca = True

            for palavra_nome in nome_norm.split():
                if palavra_nome.startswith(tk):
                    prefixo_no_nome = True
                    break

        if presente_no_nome:
            score += 140
            qtd_tokens_fortes += 1
        elif prefixo_no_nome:
            score += 95
            qtd_tokens_fortes += 1
        elif presente_na_marca:
            score += 60
        else:
            todos_presentes = False
            score -= 120

    if todos_presentes and tokens_expandidos_por_token:
        score += 450

    # Bônus para menos ruído
    score -= max(0, len(nome_norm.split()) - qtd_tokens_fortes) * 3

    # Deixa granel mais para baixo quando houver concorrência melhor
    if _eh_granel_produto(produto):
        score -= 80

    return score


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


# --- MOTOR DE BUSCA ÚNICO (REVISADO) ---
def motor_de_busca_agro(termo_original, db, client, limit=20):
    termo_original = str(termo_original or "").strip()
    if not termo_original:
        return []

    termo_norm = normalizar(termo_original)
    termo_limpo = _somente_alnum(termo_original)

    base_filter = {"CadastroInativo": {"$ne": True}}

    candidatos = {}
    ordem_insercao = 0

    def adicionar(lista):
        nonlocal ordem_insercao
        for item in lista:
            pid = str(item.get("Id") or item.get("_id"))
            if pid not in candidatos:
                candidatos[pid] = {"doc": item, "ordem": ordem_insercao}
                ordem_insercao += 1

    # 1) Código exato / barras exato
    if _termo_parece_codigo(termo_original):
        query_cod_exato = {
            **base_filter,
            "$or": [
                {"Codigo": termo_limpo},
                {"CodigoNFe": termo_limpo},
                {"CodigoBarras": termo_limpo},
                {"EAN_NFe": termo_limpo},
            ],
        }
        exatos = list(db[client.col_p].find(query_cod_exato).limit(max(limit, 10)))
        if exatos:
            return exatos[:limit]

        # prefixo de código só se realmente parecer código
        query_cod_prefixo = {
            **base_filter,
            "$or": [
                {"Codigo": {"$regex": _regex_inicio(termo_limpo)}},
                {"CodigoNFe": {"$regex": _regex_inicio(termo_limpo)}},
                {"CodigoBarras": {"$regex": _regex_inicio(termo_limpo)}},
                {"EAN_NFe": {"$regex": _regex_inicio(termo_limpo)}},
            ],
        }
        adicionar(list(db[client.col_p].find(query_cod_prefixo).limit(30)))

    # 2) Preparação de tokens
    tokens_base = _tokens_busca(termo_original)
    if not tokens_base and termo_norm:
        tokens_base = [termo_norm]

    tokens_expandidos_por_token = []
    for tk in tokens_base:
        grupo = expandir_tokens(tk)
        grupo_norm = []
        vistos = set()
        for item in [tk] + list(grupo):
            item_norm = normalizar(item)
            if item_norm and item_norm not in vistos:
                vistos.add(item_norm)
                grupo_norm.append(item_norm)
        if grupo_norm:
            tokens_expandidos_por_token.append(grupo_norm)

    # 3) Frase inteira no nome / marca
    if termo_original:
        query_frase = {
            **base_filter,
            "$or": [
                {"Nome": {"$regex": _regex_contem(termo_original)}},
                {"Marca": {"$regex": _regex_contem(termo_original)}},
            ],
        }
        adicionar(list(db[client.col_p].find(query_frase).limit(35)))

    # 4) Todos os tokens precisam aparecer em algum campo
    condicoes_and = []
    for grupo in tokens_expandidos_por_token:
        regexes = [_regex_contem(tk) for tk in grupo if tk]
        if not regexes:
            continue

        condicoes_and.append(
            {
                "$or": [
                    {"BuscaTexto": {"$in": regexes}},
                    {"Nome": {"$in": regexes}},
                    {"Marca": {"$in": regexes}},
                    {"Codigo": {"$in": regexes}},
                    {"CodigoNFe": {"$in": regexes}},
                ]
            }
        )

    if condicoes_and:
        query_and = {
            **base_filter,
            "$and": condicoes_and,
        }
        adicionar(list(db[client.col_p].find(query_and).limit(60)))

    # 5) Fallback mais aberto para digitação ruim
    if len(candidatos) < limit:
        regexes_or = []
        for grupo in tokens_expandidos_por_token:
            for tk in grupo:
                if not tk:
                    continue
                regexes_or.append(_regex_inicio(tk))
                if len(tk) >= 4:
                    regexes_or.append(_regex_contem(tk))

        if regexes_or:
            query_fallback = {
                **base_filter,
                "$or": [
                    {"BuscaTexto": {"$in": regexes_or}},
                    {"Nome": {"$in": regexes_or}},
                    {"Marca": {"$in": regexes_or}},
                ],
            }
            adicionar(list(db[client.col_p].find(query_fallback).limit(80)))

    docs = [v["doc"] for v in candidatos.values()]
    if not docs:
        return []

    docs.sort(
        key=lambda p: (
            -_pontuar_produto(p, termo_original, tokens_expandidos_por_token),
            len(str(p.get("Nome") or "")),
            str(p.get("Nome") or "").lower(),
        )
    )

    return docs[:limit]


def _mapear_estoques_por_produto(estoques, client):
    mapa = {}
    for e in estoques:
        pid = str(e.get("ProdutoID"))
        dep = str(e.get("DepositoID"))
        saldo = float(e.get("Saldo", 0) or 0)

        if pid not in mapa:
            mapa[pid] = {"centro": 0.0, "vila": 0.0}

        if dep == client.DEPOSITO_CENTRO:
            mapa[pid]["centro"] += saldo
        elif dep == client.DEPOSITO_VILA_ELIAS:
            mapa[pid]["vila"] += saldo

    return mapa


# --- APIs DE BUSCA ---
@require_GET
def api_buscar_produtos(request):
    q = request.GET.get("q", "").strip()
    client, db = obter_conexao_mongo()
    if not db or not q:
        return JsonResponse({"produtos": []})

    try:
        prods = motor_de_busca_agro(q, db, client)
        p_ids = [str(p.get("Id") or p["_id"]) for p in prods]

        estoques = list(db[client.col_e].find({"ProdutoID": {"$in": p_ids}}))
        estoque_map = _mapear_estoques_por_produto(estoques, client)

        ajustes_bd = AjusteRapidoEstoque.objects.filter(produto_externo_id__in=p_ids)
        ajustes_map = {(aj.produto_externo_id, aj.deposito): aj for aj in ajustes_bd}

        res = []
        for p in prods:
            pid = str(p.get("Id") or p["_id"])
            saldo_base = estoque_map.get(pid, {"centro": 0.0, "vila": 0.0})

            ac = ajustes_map.get((pid, "centro"))
            av = ajustes_map.get((pid, "vila"))

            sc = saldo_base["centro"]
            sv = saldo_base["vila"]

            saldo_centro = float(ac.saldo_informado) + (sc - float(ac.saldo_erp_referencia)) if ac else sc
            saldo_vila = float(av.saldo_informado) + (sv - float(av.saldo_erp_referencia)) if av else sv

            res.append(
                {
                    "id": pid,
                    "nome": p.get("Nome"),
                    "marca": p.get("Marca") or "",
                    "codigo_nfe": p.get("CodigoNFe") or p.get("Codigo") or "",
                    "preco_venda": float(p.get("ValorVenda") or p.get("PrecoVenda") or 0),
                    "imagem": _formatar_url_imagem(_extrair_imagem_produto(p, {}, pid)),
                    "saldo_centro": round(saldo_centro, 2),
                    "saldo_vila": round(saldo_vila, 2),
                }
            )

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
            saldo_base = estoque_map.get(pid, {"centro": 0.0, "vila": 0.0})

            ac = ajustes_map.get((pid, "centro"))
            av = ajustes_map.get((pid, "vila"))

            sc = saldo_base["centro"]
            sv = saldo_base["vila"]

            saldo_centro = float(ac.saldo_informado) + (sc - float(ac.saldo_erp_referencia)) if ac else sc
            saldo_vila = float(av.saldo_informado) + (sv - float(av.saldo_erp_referencia)) if av else sv

            custo = float(str(p.get("PrecoCusto") or p.get("ValorCusto") or 0).replace(",", "."))

            res.append(
                {
                    "id": pid,
                    "nome": p.get("Nome"),
                    "marca": p.get("Marca") or "",
                    "preco_custo": custo,
                    "saldo_centro": round(saldo_centro, 2),
                    "saldo_vila": round(saldo_vila, 2),
                }
            )

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
            payload["items"].append(
                {
                    "produtoID": i.get("id"),
                    "codigo": i.get("id"),
                    "unidade": "UN",
                    "descricao": i.get("nome"),
                    "quantidade": float(i.get("qtd")),
                    "valorUnitario": float(i.get("preco")),
                    "valorTotal": round(float(i.get("qtd")) * float(i.get("preco")), 2),
                }
            )

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
        res = []
        for p in produtos:
            pid = str(p.get("Id") or p["_id"])
            res.append(
                {
                    "id": pid,
                    "nome": p.get("Nome"),
                    "marca": p.get("Marca") or "",
                    "busca_texto": normalizar(f"{p.get('Nome')} {p.get('Marca')} {p.get('Codigo')}"),
                }
            )
        return JsonResponse({"produtos": res})
    except Exception:
        return JsonResponse({"produtos": []})


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
            db[client.col_c]
            .find({"Nome": {"$regex": termo, "$options": "i"}}, {"Nome": 1, "CpfCnpj": 1})
            .limit(10)
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