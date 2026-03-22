from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    # O include já vai tratar a rota vazia se ela estiver no produtos/urls.py
    path('', include('produtos.urls')), 
    path('admin/', admin.site.urls),
]