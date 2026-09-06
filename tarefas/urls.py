from django.urls import path

from . import views

urlpatterns = [
    path("pin/", views.tarefas_pin, name="tarefas_pin"),
    path("sair/", views.tarefas_logout, name="tarefas_logout"),
    path("", views.tarefas_lista, name="tarefas_lista"),
    path("nova/", views.tarefas_nova, name="tarefas_nova"),
    path("<int:pk>/", views.tarefas_detalhe, name="tarefas_detalhe"),
    path("api/criar/", views.api_tarefa_criar, name="api_tarefa_criar"),
    path("api/<int:pk>/atualizar/", views.api_tarefa_atualizar, name="api_tarefa_atualizar"),
    path("api/<int:pk>/status/", views.api_tarefa_status, name="api_tarefa_status"),
    path("api/<int:pk>/comentar/", views.api_tarefa_comentar, name="api_tarefa_comentar"),
]
