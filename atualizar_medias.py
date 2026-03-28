import os
import django
from datetime import datetime, timedelta
from decimal import Decimal

def calcular():
    from estoque.models import ConfiguracaoTransferencia
    from integracoes.venda_erp_mongo import VendaERPMongoClient

    print("="*60)
    print("🚀 INICIANDO CÁLCULO DE VENDA MÉDIA (30 DIAS)")
    print("="*60)

    # 1. Pegamos todas as regras cadastradas no Cockpit
    regras = ConfiguracaoTransferencia.objects.all()
    if not regras.exists():
        print("⚠️ Nenhuma regra de transferência cadastrada. Saindo...")
        return

    mapa_regras = {r.produto_externo_id: r for r in regras}
    produto_ids = list(mapa_regras.keys())
    print(f"📊 Monitorando {len(produto_ids)} produto(s) configurado(s).")

    client = VendaERPMongoClient()
    db = client.db
    
    # Define a data de 30 dias atrás
    data_limite = datetime.now() - timedelta(days=30)
    
    print(f"🔍 Buscando vendas faturadas desde: {data_limite.strftime('%d/%m/%Y')}...")
    
    # 2. Buscar as Vendas dos últimos 30 dias (ignorando canceladas)
    vendas = list(db["DtoVenda"].find({
        "Data": {"$gte": data_limite},
        "Cancelada": {"$ne": True},
        "Status": {"$nin": ["Cancelado", "Cancelada", "Orcamento"]}
    }, {"_id": 1, "Id": 1}))
    
    from bson import ObjectId
    venda_ids_obj = []
    venda_ids_str = []
    
    for v in vendas:
        vid = str(v.get("Id") or v.get("_id"))
        venda_ids_str.append(vid)
        if len(vid) == 24:
            try: venda_ids_obj.append(ObjectId(vid))
            except: pass

    print(f"✅ Encontradas {len(vendas)} vendas válidas no período.")

    vendas_por_produto = {pid: Decimal('0') for pid in produto_ids}
    
    # 3. Somar as quantidades vendidas (Apenas dos produtos da regra)
    if vendas:
        query_itens = {"$or": [
            {"VendaID": {"$in": venda_ids_obj}},
            {"VendaID": {"$in": venda_ids_str}}
        ]}
        
        print("📦 Analisando itens vendidos...")
        itens = db["DtoVendaProduto"].find(query_itens)
        
        for item in itens:
            pid = str(item.get("ProdutoID"))
            if pid in vendas_por_produto:
                qtd = Decimal(str(item.get("Quantidade", 0) or 0))
                vendas_por_produto[pid] += qtd

    # 4. Salvar as médias no painel
    print("💾 Salvando médias no sistema...")
    atualizados = 0
    
    for pid, total_vendido in vendas_por_produto.items():
        # A Mágica: Divide o total vendido por 30 dias
        media_diaria = total_vendido / Decimal('30')
        regra = mapa_regras[pid]
        
        # Só salva se o número mudou (para não gastar processamento à toa)
        if round(regra.venda_media_diaria, 3) != round(media_diaria, 3):
            regra.venda_media_diaria = round(media_diaria, 3)
            regra.save()
            atualizados += 1
            print(f"   -> {regra.nome_produto[:30]}: {total_vendido} vendidos no mês = Média {regra.venda_media_diaria}/dia")

    print("="*60)
    print(f"🏁 FINALIZADO! {atualizados} produtos tiveram suas médias atualizadas.")
    print("="*60)

    try:
        from django.core.cache import cache

        cache.delete("pdv_mapa_medias_venda_diaria_30d_entry_v2")
        cache.delete("pdv_mapa_medias_venda_diaria_30d_v1")
        print("Cache PDV de médias de venda (30d) invalidado.")
    except Exception as exc:
        print(f"Aviso: não foi possível limpar cache de médias: {exc}")

if __name__ == '__main__':
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
    django.setup()
    calcular()