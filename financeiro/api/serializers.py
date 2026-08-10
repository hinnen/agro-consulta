from rest_framework import serializers


class ResumoOperacionalQuerySerializer(serializers.Serializer):
    empresa_id = serializers.IntegerField(required=False)
    grupo_id = serializers.IntegerField(required=False)
    loja = serializers.ChoiceField(
        choices=["todas", "centro", "vila"], required=False
    )
    data_inicio = serializers.DateField(required=True)
    data_fim = serializers.DateField(required=True)
    modo = serializers.ChoiceField(
        choices=["empresa", "grupo", "lojas"], required=False
    )
    dias_periodo = serializers.IntegerField(required=False, default=30, min_value=1)
    fonte = serializers.ChoiceField(
        choices=["postgres"],
        default="postgres",
        required=False,
    )
    por = serializers.ChoiceField(
        choices=["competencia", "vencimento", "pagamento"],
        default="competencia",
        required=False,
    )
    valor = serializers.ChoiceField(
        choices=["bruto", "realizado"],
        default="bruto",
        required=False,
    )
    contas = serializers.CharField(required=False, allow_blank=True, default="")
    incluir_linhas = serializers.BooleanField(required=False, default=False)
    incluir_visual = serializers.BooleanField(required=False, default=False)

    def validate(self, attrs):
        loja = attrs.get("loja")
        modo = attrs.get("modo")
        if loja:
            attrs["modo"] = "lojas"
            attrs["loja"] = loja
            return attrs
        if not modo:
            raise serializers.ValidationError("Informe loja ou modo")
        if modo == "lojas":
            attrs["loja"] = attrs.get("loja") or "todas"
            return attrs
        if modo == "empresa" and not attrs.get("empresa_id"):
            raise serializers.ValidationError(
                "empresa_id é obrigatório quando modo=empresa"
            )
        if modo == "grupo" and not attrs.get("grupo_id"):
            raise serializers.ValidationError(
                "grupo_id é obrigatório quando modo=grupo"
            )
        return attrs


class DebugMongoResumoQuerySerializer(serializers.Serializer):
    """Query params para ``/api/financeiro/debug-mongo-resumo/`` (somente staff)."""

    empresa_id = serializers.IntegerField(required=True, min_value=1)
    data_inicio = serializers.DateField(required=True)
    data_fim = serializers.DateField(required=True)
    por = serializers.ChoiceField(
        choices=["competencia", "vencimento", "pagamento"],
        default="competencia",
        required=False,
    )
    contas = serializers.CharField(required=False, allow_blank=True, default="")
