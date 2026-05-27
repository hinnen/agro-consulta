from django import template

from produtos.caixa_util import format_moeda_br, format_quantidade_br

register = template.Library()


@register.filter(name="moeda_br")
def moeda_br_filter(value):
    return format_moeda_br(value)


@register.filter(name="qtd_br")
def qtd_br_filter(value):
    return format_quantidade_br(value)
