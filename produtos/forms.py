from django import forms

from .models import ClienteAgro


class ClienteAgroForm(forms.ModelForm):
    class Meta:
        model = ClienteAgro
        fields = (
            "nome",
            "whatsapp",
            "cpf",
            "cep",
            "uf",
            "cidade",
            "bairro",
            "logradouro",
            "numero",
            "complemento",
            "plus_code",
            "referencia_rural",
            "maps_url_manual",
            "ativo",
        )
        widgets = {
            "nome": forms.TextInput(
                attrs={"class": "w-full rounded-xl border-2 border-slate-200 px-4 py-3 font-bold text-slate-800"}
            ),
            "whatsapp": forms.TextInput(
                attrs={
                    "class": "w-full rounded-xl border-2 border-slate-200 px-4 py-3 font-bold text-slate-800",
                    "placeholder": "DDD + número (só dígitos ou formatado)",
                }
            ),
            "cpf": forms.TextInput(
                attrs={
                    "class": "w-full rounded-xl border-2 border-slate-200 px-4 py-3 font-bold text-slate-800",
                    "placeholder": "CPF",
                }
            ),
            "cep": forms.TextInput(
                attrs={
                    "class": "w-full rounded-xl border-2 border-slate-200 px-4 py-3 font-bold text-slate-800",
                    "placeholder": "00000-000",
                    "inputmode": "numeric",
                }
            ),
            "uf": forms.TextInput(
                attrs={
                    "class": "w-full rounded-xl border-2 border-slate-200 px-4 py-3 font-bold text-slate-800 uppercase",
                    "placeholder": "SP",
                    "maxlength": "2",
                }
            ),
            "cidade": forms.TextInput(
                attrs={
                    "class": "w-full rounded-xl border-2 border-slate-200 px-4 py-3 font-bold text-slate-800",
                    "placeholder": "Nome da cidade",
                }
            ),
            "bairro": forms.TextInput(
                attrs={
                    "class": "w-full rounded-xl border-2 border-slate-200 px-4 py-3 font-bold text-slate-800",
                    "placeholder": "Nome do bairro",
                }
            ),
            "logradouro": forms.TextInput(
                attrs={
                    "class": "w-full rounded-xl border-2 border-slate-200 px-4 py-3 font-bold text-slate-800",
                    "placeholder": "Rua, avenida…",
                }
            ),
            "numero": forms.TextInput(
                attrs={
                    "class": "w-full rounded-xl border-2 border-slate-200 px-4 py-3 font-bold text-slate-800",
                    "placeholder": "123",
                }
            ),
            "complemento": forms.TextInput(
                attrs={
                    "class": "w-full rounded-xl border-2 border-slate-200 px-4 py-3 font-bold text-slate-800",
                    "placeholder": "Apartamento, sala, etc.",
                }
            ),
            "plus_code": forms.TextInput(
                attrs={
                    "class": "w-full rounded-xl border-2 border-slate-200 px-4 py-3 font-bold text-slate-800",
                    "placeholder": "Plus Code rural (ex.: 8X5R+7M9 Jacupiranga)",
                }
            ),
            "referencia_rural": forms.Textarea(
                attrs={
                    "class": "w-full rounded-xl border-2 border-slate-200 px-4 py-3 font-bold text-slate-800",
                    "rows": 2,
                    "placeholder": "Porteira, km, referência no local…",
                }
            ),
            "maps_url_manual": forms.TextInput(
                attrs={
                    "class": "w-full rounded-xl border-2 border-slate-200 px-4 py-3 font-bold text-slate-800",
                    "placeholder": "https://maps.google.com/…",
                }
            ),
            "ativo": forms.CheckboxInput(attrs={"class": "rounded border-slate-300 w-5 h-5"}),
        }

