class ClassificacaoABCService:
    def classificar(self, produtos_metricas):
        """
        produtos_metricas: lista de dicts com produto_id e valor_vendido (Decimal).
        """
        total = sum(item["valor_vendido"] for item in produtos_metricas) or 0
        if total <= 0:
            for item in produtos_metricas:
                item["classe_abc"] = "C"
            return produtos_metricas

        ordenados = sorted(
            produtos_metricas, key=lambda x: x["valor_vendido"], reverse=True
        )

        acumulado = 0
        for item in ordenados:
            acumulado += item["valor_vendido"]
            pct_acumulado = acumulado / total

            if pct_acumulado <= 0.80:
                item["classe_abc"] = "A"
            elif pct_acumulado <= 0.95:
                item["classe_abc"] = "B"
            else:
                item["classe_abc"] = "C"

        return ordenados
