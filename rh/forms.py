from decimal import Decimal

from django import forms
from django.core.exceptions import ValidationError
from base.models import Loja

from produtos.models import ClienteAgro

from .models import FechamentoFolhaSimplificado, Funcionario, HistoricoSalarial, ValeFuncionario


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

    data = forms.DateField(label="Data")
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
        self.fields["banco_financeiro"].choices = [("", "— Selecione —")] + bc

    def clean(self):
        data = super().clean()
        acao = (self.data.get("titulo_acao") or "").strip()
        if acao == "publicar":
            if not (data.get("banco_financeiro") or "").strip():
                raise ValidationError("Escolha a conta / banco.")
        return data


class FechamentoEditForm(forms.ModelForm):
    class Meta:
        model = FechamentoFolhaSimplificado
        fields = ("outros_descontos", "outros_proventos", "observacoes", "valor_pago", "status")
        widgets = {
            "outros_descontos": forms.NumberInput(attrs={"class": _FECH_INP, "step": "0.01"}),
            "outros_proventos": forms.NumberInput(attrs={"class": _FECH_INP, "step": "0.01"}),
            "valor_pago": forms.NumberInput(attrs={"class": _FECH_INP, "step": "0.01"}),
            "observacoes": forms.Textarea(
                attrs={
                    "rows": 4,
                    "class": "mt-1 w-full rounded-xl border-2 border-slate-200 px-3 py-2.5 text-base font-semibold focus:border-emerald-500 outline-none",
                }
            ),
            "status": forms.Select(attrs={"class": _FECH_INP + " font-bold"}),
        }


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
