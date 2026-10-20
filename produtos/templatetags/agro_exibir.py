from django import template

from produtos.caixa_util import format_moeda_br, format_quantidade_br

register = template.Library()


@register.filter(name="get_item")
def get_item(mapping, key):
    if mapping is None:
        return None
    try:
        return mapping.get(key)
    except AttributeError:
        return None


@register.filter(name="moeda_br")
def moeda_br_filter(value):
    return format_moeda_br(value)


@register.filter(name="qtd_br")
def qtd_br_filter(value):
    return format_quantidade_br(value)
