from rest_framework import serializers


class ResumoOperacionalQuerySerializer(serializers.Serializer):
    empresa_id = serializers.IntegerField(required=False)
    grupo_id = serializers.IntegerField(required=False)
    data_inicio = serializers.DateField(required=True)
    data_fim = serializers.DateField(required=True)
    modo = serializers.ChoiceField(choices=["empresa", "grupo"], required=True)
    dias_periodo = serializers.IntegerField(required=False, default=30, min_value=1)

    def validate(self, attrs):
        modo = attrs["modo"]
        if modo == "empresa" and not attrs.get("empresa_id"):
            raise serializers.ValidationError(
                "empresa_id é obrigatório quando modo=empresa"
            )
        if modo == "grupo" and not attrs.get("grupo_id"):
            raise serializers.ValidationError(
                "grupo_id é obrigatório quando modo=grupo"
            )
        return attrs
