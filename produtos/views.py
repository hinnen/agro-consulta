import json
import re
import unicodedata
from collections import defaultdict
from decimal import Decimal

from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET, require_POST

from base.models import Empresa, PerfilUsuario
from estoque.models import AjusteRapidoEstoque
from integracoes.venda_erp_mongo import VendaERPMongoClient


import traceback

def obter_conexao_mongo():
    try:
        client = VendaERPMongoClient()
        client.client.admin.command("ping")
        return client, client.db
    except Exception as e:
        print("\n=== ERRO REAL DO MONGO ===")
        print(repr(e))
        traceback.print_exc()
        print("=== FIM ERRO MONGO ===\n")
        return None, NoneS


def normalizar_termo(txt):
    if not txt:
        return ""
    txt = str(txt).strip().lower()
    return "".join(
        c for c in unicodedata.normalize("NFD", txt)
        if unicodedata.category(c) != "Mn"
    )


def escape_regex(txt):
    return re.escape(txt or "")


def termo_sem_espacos(txt):
    return re.sub(r"\s+", "", txt or "")


def eh_granel(produto):
    campos = [
        produto.get("Categoria") or "",
        produto.get("SubCategoria") or "",
        produto.get("Grupo") or "",
        produto.get("Nome") or "",
        produto.get("BuscaTexto") or "",
    ]
    texto = normalizar_termo(" ".join(map(str, campos)))
    return "granel" in texto


def score_produto(produto, termo_original, termo_norm, termo_limpo, palavras):
    nome = normalizar_termo(produto.get("Nome") or "")
    marca = normalizar_termo(produto.get("Marca") or "")
    busca = normalizar_termo(produto.get("BuscaTexto") or "")
    codigo_nfe = normalizar_termo(str(produto.get("CodigoNFe") or ""))
    codigo_barras = normalizar_termo(str(produto.get("CodigoBarras") or ""))
    ean_nfe = normalizar_termo(str(produto.get("EAN_NFe") or ""))
    codigo_produto = normalizar_termo(str(produto.get("CodigoProduto") or ""))

    texto_total = f"{nome} {marca} {busca}".strip()

    score = 0

    if termo_limpo:
        if codigo_nfe == termo_limpo:
            score += 1000
        if codigo_barras == termo_limpo:
            score += 1000
        if ean_nfe == termo_limpo:
            score += 1000
        if codigo_produto == termo_limpo:
            score += 1000

    if nome == termo_norm:
        score += 500
    if nome.startswith(termo_norm):
        score += 300
    if termo_norm and termo_norm in nome:
        score += 200

    if busca.startswith(termo_norm):
        score += 180
    if termo_norm and termo_norm in busca:
        score += 120

    if marca.startswith(termo_norm):
        score += 80
    if termo_norm and termo_norm in marca:
        score += 50

    score += sum(25 for p in palavras if p and p in texto_total)

    if eh_granel(produto):
        score -= 120

    return score


def consulta_produtos(request):
    return render(request, "produtos/consulta_produtos.html")


def historico_ajustes(request):
    ajustes = AjusteRapidoEstoque.objects.all().order_by("-criado_em")
    return render(request, "produtos/historico_ajustes.html", {"ajustes": ajustes})


def sugestao_transferencia(request):
    return render(request, "produtos/transferencias.html")


@require_GET
def api_buscar_produtos(request):
    termo_original = request.GET.get("q", "").strip()

    if len(termo_original) < 2:
        return JsonResponse({"produtos": []})

    client, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse(
            {"erro": "Erro de conexão com o banco Mongo."},
            status=500
        )

    termo_norm = normalizar_termo(termo_original)
    termo_limpo = termo_sem_espacos(termo_original)
    palavras = [p for p in termo_norm.split() if p]

    try:
        regex_parts = [f"(?=.*{escape_regex(p)})" for p in palavras]
        regex_final = "".join(regex_parts) + ".*" if regex_parts else escape_regex(termo_norm)

        query = {
            "$and": [
                {"CadastroInativo": {"$ne": True}},
                {
                    "$or": [
                        {"BuscaTexto": {"$regex": regex_final, "$options": "i"}},
                        {"Nome": {"$regex": regex_final, "$options": "i"}},
                        {"CodigoNFe": termo_limpo},
                        {"CodigoBarras": termo_limpo},
                        {"EAN_NFe": termo_limpo},
                        {"CodigoProduto": termo_limpo},
                    ]
                }
            ]
        }

        col_p = db[client.col_p]
        produtos_mongo = list(
            col_p.find(
                query,
                {
                    "Nome": 1,
                    "Marca": 1,
                    "CodigoNFe": 1,
                    "CodigoBarras": 1,
                    "EAN_NFe": 1,
                    "CodigoProduto": 1,
                    "BuscaTexto": 1,
                    "Categoria": 1,
                    "SubCategoria": 1,
                    "Grupo": 1,
                    "ValorVenda": 1,
                    "PrecoVenda": 1,
                    "Id": 1,
                }
            ).limit(40)
        )

        if not produtos_mongo:
            return JsonResponse({"produtos": []})

        produto_ids_busca = set()
        produto_ids_principais = []

        for p in produtos_mongo:
            mongo_id = str(p["_id"])
            produto_ids_principais.append(mongo_id)
            produto_ids_busca.add(mongo_id)

            id_externo = p.get("Id")
            if id_externo is not None:
                produto_ids_busca.add(str(id_externo))

        estoque_list = list(
            db[client.col_e].find(
                {"ProdutoID": {"$in": list(produto_ids_busca)}},
                {
                    "ProdutoID": 1,
                    "DepositoID": 1,
                    "Saldo": 1,
                    "Produto": 1,
                    "Deposito": 1,
                }
            )
        )

        estoques_por_produto = defaultdict(list)
        for est in estoque_list:
            pid = str(est.get("ProdutoID") or "")
            if pid:
                estoques_por_produto[pid].append(est)

        ajustes = AjusteRapidoEstoque.objects.filter(
            produto_externo_id__in=produto_ids_principais
        ).order_by("-criado_em")

        ajustes_map = {}
        for aj in ajustes:
            chave = (str(aj.produto_externo_id), aj.deposito)
            if chave not in ajustes_map:
                ajustes_map[chave] = aj

        resultados = []
        for p in produtos_mongo:
            pid_mongo = str(p["_id"])
            pid_externo = str(p.get("Id")) if p.get("Id") is not None else None

            estoques_produto = []
            estoques_produto.extend(estoques_por_produto.get(pid_mongo, []))
            if pid_externo and pid_externo != pid_mongo:
                estoques_produto.extend(estoques_por_produto.get(pid_externo, []))

            saldo_centro = 0.0
            saldo_vila = 0.0

            for est in estoques_produto:
                deposito_id = str(est.get("DepositoID") or "")
                saldo = float(est.get("Saldo") or 0)

                if deposito_id == client.DEPOSITO_CENTRO:
                    saldo_centro = saldo
                elif deposito_id == client.DEPOSITO_VILA_ELIAS:
                    saldo_vila = saldo

            aj_c = ajustes_map.get((pid_mongo, "centro"))
            aj_v = ajustes_map.get((pid_mongo, "vila"))

            saldo_final_centro = (
                float(aj_c.saldo_informado) + (saldo_centro - float(aj_c.saldo_erp_referencia))
                if aj_c else saldo_centro
            )
            saldo_final_vila = (
                float(aj_v.saldo_informado) + (saldo_vila - float(aj_v.saldo_erp_referencia))
                if aj_v else saldo_vila
            )

            score = score_produto(
                produto=p,
                termo_original=termo_original,
                termo_norm=termo_norm,
                termo_limpo=termo_limpo,
                palavras=palavras,
            )

            resultados.append({
                "id": pid_mongo,
                "nome": p.get("Nome") or "",
                "marca": p.get("Marca") or "",
                "codigo_nfe": p.get("CodigoNFe") or "",
                "preco_venda": float(p.get("ValorVenda") or p.get("PrecoVenda") or 0),
                "saldo_centro": round(saldo_final_centro, 2),
                "saldo_vila": round(saldo_final_vila, 2),
                "_score": score,
                "_granel": eh_granel(p),
            })

        resultados.sort(
            key=lambda x: (
                x["_granel"],
                -x["_score"],
                x["nome"].lower(),
            )
        )

        resultados = resultados[:15]

        for item in resultados:
            item.pop("_score", None)
            item.pop("_granel", None)

        return JsonResponse({"produtos": resultados})

    except Exception as e:
        return JsonResponse({"erro": str(e)}, status=500)


@require_GET
def api_autocomplete_produtos(request):
    termo = request.GET.get("q", "").strip()

    if len(termo) < 2:
        return JsonResponse({"sugestoes": []})

    client, db = obter_conexao_mongo()
    if db is None:
        return JsonResponse({"sugestoes": []})

    termo_norm = normalizar_termo(termo)

    try:
        query = {
            "$and": [
                {"CadastroInativo": {"$ne": True}},
                {
                    "$or": [
                        {"BuscaTexto": {"$regex": f"^{escape_regex(termo_norm)}", "$options": "i"}},
                        {"Nome": {"$regex": f"^{escape_regex(termo_norm)}", "$options": "i"}},
                    ]
                }
            ]
        }

        sugestoes = list(
            db[client.col_p].find(
                query,
                {
                    "Nome": 1,
                    "Marca": 1,
                    "ValorVenda": 1,
                    "PrecoVenda": 1,
                    "Id": 1,
                }
            ).limit(8)
        )

        res = [{
            "id": str(s["_id"]),
            "nome": s.get("Nome") or "",
            "marca": s.get("Marca") or "",
            "preco_venda": float(s.get("PrecoVenda") or s.get("ValorVenda") or 0),
        } for s in sugestoes]

        return JsonResponse({"sugestoes": res})

    except Exception:
        return JsonResponse({"sugestoes": []})


@require_POST
def api_ajustar_estoque(request):
    pin = request.POST.get("pin")
    perfil = PerfilUsuario.objects.filter(senha_rapida=pin).first()

    if not perfil:
        return JsonResponse({"ok": False, "erro": "PIN INCORRETO"}, status=403)

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

        return JsonResponse({"ok": True})

    except Exception as e:
        return JsonResponse({"ok": False, "erro": str(e)}, status=500)


@require_GET
def api_buscar_clientes(request):
    termo = request.GET.get("q", "").strip()
    client, db = obter_conexao_mongo()
    
    if not termo or db is None: 
        return JsonResponse({"clientes": []})
    try:
        # CORREÇÃO: Usando os campos corretos 'NomeFantasia', 'RazaoSocial' e 'CNPJ_CPF'
        query = {
            "$or": [
                {"NomeFantasia": {"$regex": termo, "$options": "i"}},
                {"RazaoSocial": {"$regex": termo, "$options": "i"}},
                {"CNPJ_CPF": {"$regex": termo, "$options": "i"}}
            ]
        }
        clientes = list(db[client.col_c].find(query).limit(10))
        
        # CORREÇÃO: Retornando os campos corretos
        res = [{
            "nome": c.get("NomeFantasia") or c.get("RazaoSocial"), 
            "documento": c.get("CNPJ_CPF") or "Sem Doc"
        } for c in clientes]
        return JsonResponse({"clientes": res})
    except Exception as e:
        return JsonResponse({"erro": str(e)}, status=500)