from django.urls import path

from .views import pdv_home


urlpatterns = [
    path("", pdv_home, name="pdv_home"),
]
