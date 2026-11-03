from decimal import Decimal

from django import forms
from django.core.exceptions import ValidationError
from django.forms.models import BaseModelFormSet, modelformset_factory
from base.models import Loja

from produtos.models import ClienteAgro

from .models import (
    AjusteManualFolha,
    FechamentoFolhaSimplificado,
    Funcionario,
    HistoricoSalarial,
    ValeFuncionario,
)


class FuncionarioForm(forms.ModelForm):
    salario_inicial = forms.DecimalField(
        label="Salário base inicial",
        max_digits=12,
        decimal_places=2,
        required=False,
        help_text="Opcional no cadastro; pode definir depois na ficha.",
    )

    class Meta:
        model = Funcionario
        fields = (
            "cliente_agro",
            "empresa",
            "loja",
            "nome_cache",
            "apelido_interno",
            "cargo",
            "data_admissao",
            "ativo",
            "dia_envio_cp_auto",
            "dia_vencimento_salario",
            "observacoes",
        )
        widgets = {
            "observacoes": forms.Textarea(attrs={"rows": 3}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        emp_id = None
        if self.data.get("empresa"):
            try:
                emp_id = int(self.data.get("empresa"))
            except (TypeError, ValueError):
                pass
        elif self.instance.pk:
            emp_id = self.instance.empresa_id
        if emp_id:
            self.fields["loja"].queryset = Loja.objects.filter(empresa_id=emp_id, ativa=True)
        else:
            self.fields["loja"].queryset = Loja.objects.none()

        base_qs = ClienteAgro.objects.filter(ativo=True).order_by("nome")
        if emp_id:
            q_emp = Funcionario.objects.filter(empresa_id=emp_id, ativo=True)
            if self.instance.pk:
                q_emp = q_emp.exclude(pk=self.instance.pk)
            usados = list(q_emp.values_list("cliente_agro_id", flat=True))
            self.fields["cliente_agro"].queryset = base_qs.exclude(pk__in=usados)
        else:
            self.fields["cliente_agro"].queryset = base_qs

        self.fields["cliente_agro"].label = "Pessoa base (ClienteAgro / ERP)"
        self.fields["nome_cache"].label = "Nome (cache para RH e caixa)"
        self.fields["nome_cache"].help_text = (
            "Por padrão segue o nome da pessoa base; ajuste se precisar bater com textos antigos."
        )
        self.fields["dia_envio_cp_auto"].label = "Dia envio automático → CP"
        self.fields["dia_envio_cp_auto"].help_text = (
            "1–28: nesse dia do mês o sistema lança/atualiza o título no Contas a pagar. "
            "Com vencimento 1–14 e envio 28 = folha do mês do envio, vence no mês seguinte. "
            "0 = não envia sozinho (só manual no passo 2)."
        )
        self.fields["dia_vencimento_salario"].label = "Dia vencimento salário (CP)"
        self.fields["dia_vencimento_salario"].help_text = (
            "1–28: dia do vencimento (ex.: 1, 7 ou 14). Conta/banco fica em branco "
            "(ADICIONAR CONTA) até o pagamento."
        )

    def clean(self):
        data = super().clean()
        emp = data.get("empresa")
        ca = data.get("cliente_agro")
        if emp and ca and data.get("ativo", True):
            qs = Funcionario.objects.filter(empresa=emp, cliente_agro=ca, ativo=True)
            if self.instance.pk:
                qs = qs.exclude(pk=self.instance.pk)
            if qs.exists():
                raise ValidationError(
                    "Já existe um perfil RH ativo para esta pessoa nesta empresa."
                )
        envio = data.get("dia_envio_cp_auto")
        if envio is None:
            envio = 0
        try:
            envio = int(envio)
        except (TypeError, ValueError):
            raise ValidationError({"dia_envio_cp_auto": "Informe um número (0 ou 1–28)."})
        if envio < 0 or envio > 28:
            raise ValidationError({"dia_envio_cp_auto": "Use 0 (desligado) ou dia 1–28."})
        data["dia_envio_cp_auto"] = envio
        venc = data.get("dia_vencimento_salario")
        if venc is None:
            venc = 5
        try:
            venc = int(venc)
        except (TypeError, ValueError):
            raise ValidationError({"dia_vencimento_salario": "Informe um dia 1–28."})
        if venc < 1 or venc > 28:
            raise ValidationError({"dia_vencimento_salario": "Use dia 1–28."})
        data["dia_vencimento_salario"] = venc
        return data


_HIST_SAL_CTL = "mt-1 w-full min-h-[48px] rounded-xl border-2 border-slate-200 px-3 py-2 text-base font-semibold tabular-nums focus:border-emerald-500 outline-none"


class HistoricoSalarialForm(forms.ModelForm):
    class Meta:
        model = HistoricoSalarial
        fields = ("salario_base", "data_inicio_vigencia", "motivo_alteracao")
        widgets = {
            "salario_base": forms.NumberInput(attrs={"class": _HIST_SAL_CTL, "step": "0.01"}),
            "data_inicio_vigencia": forms.DateInput(attrs={"type": "date", "class": _HIST_SAL_CTL}),
            "motivo_alteracao": forms.TextInput(attrs={"class": _HIST_SAL_CTL, "placeholder": "Opcional"}),
        }


class ValeManualForm(forms.ModelForm):
    class Meta:
        model = ValeFuncionario
        fields = ("data", "valor", "observacao")


class ValeManualRHForm(forms.Form):
    """Vale na ficha: só folha ou folha + título Mongo (forma/banco como na saída de caixa)."""

    _ctl = "mt-1 w-full rounded-lg border-2 border-slate-200 px-2 py-2 text-sm font-semibold"

    data = forms.DateField(
        label="Data do vale",
        input_formats=["%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y"],
        widget=forms.DateInput(
            format="%Y-%m-%d",
            attrs={
                "type": "date",
                "class": _ctl,
                "title": "Pode ser data passada (ex.: ontem); vale entra na competência do mês desta data.",
            },
        ),
    )
    valor = forms.DecimalField(
        label="Valor (R$)",
        max_digits=12,
        decimal_places=2,
        min_value=Decimal("0.01"),
    )
    observacao = forms.CharField(
        required=False,
        label="Observação",
        widget=forms.Textarea(attrs={"rows": 2, "class": _ctl}),
    )
    registrar_no_financeiro = forms.BooleanField(
        required=False,
        initial=True,
        label="Registrar no financeiro (Mongo) como pagamento parcial do salário",
        help_text="Exige título de salário gerado no fechamento do mês; aplica baixa parcial no mesmo DtoLancamento (sem nova despesa de vale).",
    )
    forma_baixa = forms.ChoiceField(
        required=False,
        label="Forma de pagamento",
        choices=[("", "— Selecione —")],
    )
    banco_baixa = forms.ChoiceField(
        required=False,
        label="Conta / banco",
        choices=[("", "— Selecione —")],
    )

    def __init__(self, *args, formas_choices=None, bancos_choices=None, **kwargs):
        super().__init__(*args, **kwargs)
        fc = list(formas_choices or [])
        bc = list(bancos_choices or [])
        self.fields["forma_baixa"].choices = [("", "— Selecione —")] + fc
        self.fields["banco_baixa"].choices = [("", "— Selecione —")] + bc
        self.fields["data"].widget.attrs.setdefault("class", self._ctl)
        self.fields["valor"].widget.attrs.setdefault("class", self._ctl)
        self.fields["forma_baixa"].widget.attrs.setdefault("class", self._ctl)
        self.fields["banco_baixa"].widget.attrs.setdefault("class", self._ctl)
        self.fields["registrar_no_financeiro"].widget.attrs.setdefault(
            "class", "h-5 w-5 rounded border-slate-300 text-orange-600 align-middle"
        )

    def clean(self):
        data = super().clean()
        if data.get("registrar_no_financeiro"):
            if not (data.get("forma_baixa") or "").strip():
                raise ValidationError("Para lançar no financeiro, selecione a forma de pagamento.")
            if not (data.get("banco_baixa") or "").strip():
                raise ValidationError("Para lançar no financeiro, selecione a conta / banco.")
        return data


_FECH_INP = "mt-1 w-full min-h-[48px] rounded-xl border-2 border-slate-200 px-3 py-2.5 text-base font-semibold tabular-nums focus:border-emerald-500 outline-none"


class FechamentoTituloFinanceiroForm(forms.Form):
    """Título único de salário no Mongo (vencimento + cabeçalho forma/banco)."""

    _ctl = "mt-1 w-full min-h-[48px] rounded-xl border-2 border-slate-200 px-3 py-2.5 text-base font-semibold focus:border-emerald-500 outline-none"

    data_vencimento = forms.DateField(
        label="Dia do vencimento",
        widget=forms.DateInput(attrs={"type": "date", "class": _ctl}),
    )
    forma_financeiro = forms.ChoiceField(
        required=False,
        label="Forma de pagamento (opcional até quitar)",
        choices=[("", "— Em branco —")],
    )
    banco_financeiro = forms.ChoiceField(
        required=False,
        label="Conta / banco (obrigatório para gerar o título)",
        choices=[("", "— Selecione —")],
    )

    def __init__(self, *args, formas_choices=None, bancos_choices=None, **kwargs):
        super().__init__(*args, **kwargs)
        fc = list(formas_choices or [])
        bc = list(bancos_choices or [])
        self.fields["forma_financeiro"].choices = [("", "— Em branco —")] + fc
        self.fields["banco_financeiro"].choices = [
            ("", "— Em branco (ADICIONAR CONTA no pagamento) —")
        ] + bc

    def clean(self):
        data = super().clean()
        # Banco opcional: vazio → placeholder no service ao publicar.
        return data


class FechamentoEditForm(forms.ModelForm):
    class Meta:
        model = FechamentoFolhaSimplificado
        fields = ("observacoes", "valor_pago", "status")
        labels = {
            "observacoes": "Observações",
            "valor_pago": "Valor pago",
            "status": "Status",
        }
        widgets = {
            "valor_pago": forms.NumberInput(attrs={"class": _FECH_INP, "step": "0.01"}),
            "observacoes": forms.Textarea(
                attrs={
                    "rows": 4,
                    "class": "mt-1 w-full rounded-xl border-2 border-slate-200 px-3 py-2.5 text-base font-semibold focus:border-emerald-500 outline-none",
                }
            ),
            "status": forms.Select(attrs={"class": _FECH_INP + " font-bold"}),
        }


class AjusteManualFolhaForm(forms.ModelForm):
    class Meta:
        model = AjusteManualFolha
        fields = ("tipo", "descricao", "valor")
        labels = {
            "tipo": "Tipo",
            "descricao": "Nome na folha",
            "valor": "Valor",
        }
        widgets = {
            "tipo": forms.Select(attrs={"class": _FECH_INP + " font-bold"}),
            "descricao": forms.TextInput(
                attrs={
                    "class": _FECH_INP,
                    "placeholder": "Ex.: INSS, bônus da meta, gratificação",
                    "maxlength": "200",
                }
            ),
            "valor": forms.NumberInput(
                attrs={"class": _FECH_INP, "step": "0.01", "min": "0.01", "inputmode": "decimal"}
            ),
        }

    def has_changed(self):
        if self.instance.pk:
            return super().has_changed()
        desc = (self.data.get(self.add_prefix("descricao")) or "").strip() if self.data else ""
        valor = (self.data.get(self.add_prefix("valor")) or "").strip() if self.data else ""
        if not desc and not valor:
            return False
        return super().has_changed()

    def clean_valor(self):
        valor = self.cleaned_data.get("valor")
        if valor is not None and valor < 0:
            raise ValidationError("O valor fica sempre positivo. Escolha Desconto ou Acréscimo.")
        return valor

    def clean(self):
        data = super().clean()
        if self.errors or data.get("DELETE"):
            return data
        desc = (data.get("descricao") or "").strip()
        valor = data.get("valor")
        tem_valor = valor is not None and valor != Decimal("0")
        if not desc and not tem_valor:
            data["descricao"] = ""
            return data
        if tem_valor and not desc:
            self.add_error("descricao", "Dê um nome (ex.: INSS, bônus da meta, gratificação).")
        if desc and (valor is None or valor <= 0):
            self.add_error("valor", "Informe um valor maior que zero.")
        data["descricao"] = desc
        return data


class BaseAjusteManualFormSet(BaseModelFormSet):
    def __init__(self, *args, fechamento=None, **kwargs):
        self.fechamento = fechamento
        super().__init__(*args, **kwargs)

    def save(self, commit=True):
        instances = super().save(commit=False)
        if not commit:
            return instances
        for obj in self.deleted_objects:
            if obj.pk:
                obj.delete()
        for obj in instances:
            obj.fechamento = self.fechamento
            obj.save()
        ordem = 0
        for form in self.forms:
            cd = getattr(form, "cleaned_data", None) or {}
            inst = form.instance
            if not cd or cd.get("DELETE") or not inst.pk:
                continue
            if not (inst.descricao or "").strip() or not inst.valor:
                inst.delete()
                continue
            if inst.ordem != ordem:
                inst.ordem = ordem
                inst.save(update_fields=["ordem"])
            ordem += 1
        return instances


AjusteManualFormSet = modelformset_factory(
    AjusteManualFolha,
    form=AjusteManualFolhaForm,
    formset=BaseAjusteManualFormSet,
    extra=1,
    can_delete=True,
)


class ImportarValesForm(forms.Form):
    data_de = forms.DateField(label="De")
    data_ate = forms.DateField(label="Até")


class ReconciliarClienteAgroForm(forms.Form):
    """Troca a pessoa base (ClienteAgro) de um perfil RH existente."""

    cliente_agro = forms.ModelChoiceField(
        queryset=ClienteAgro.objects.none(),
        label="Nova pessoa base (ClienteAgro)",
    )
    atualizar_nome_cache = forms.BooleanField(
        required=False,
        initial=True,
        label="Atualizar nome_cache a partir do nome do ClienteAgro",
    )

    def __init__(self, *args, funcionario: Funcionario | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self._funcionario = funcionario
        if funcionario:
            q_ocupados = Funcionario.objects.filter(
                empresa_id=funcionario.empresa_id,
                ativo=True,
            ).exclude(pk=funcionario.pk)
            usados = list(q_ocupados.values_list("cliente_agro_id", flat=True))
            qs = ClienteAgro.objects.filter(ativo=True).exclude(pk__in=usados).order_by("nome")
            self.fields["cliente_agro"].queryset = qs

    def clean(self):
        data = super().clean()
        ca = data.get("cliente_agro")
        f = self._funcionario
        if f and ca:
            if (
                Funcionario.objects.filter(empresa=f.empresa, cliente_agro=ca, ativo=True)
                .exclude(pk=f.pk)
                .exists()
            ):
                raise ValidationError(
                    "Já existe outro perfil RH ativo para esta pessoa nesta empresa."
                )
        return data
