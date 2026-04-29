"""Variáveis de template partilhadas entre apps."""


def home_launcher_nav(request):
    from produtos.views import home_launcher_nav_items

    return {"home_launcher_nav_items": home_launcher_nav_items()}

