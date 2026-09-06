from django.db import models
from django.utils import timezone


class TarefaAgro(models.Model):
    class Status(models.TextChoices):
        DECIDIR = "decidir", "Decidir"
        EM_ANDAMENTO = "em_andamento", "Já sendo feito"
        AGUARDANDO = "aguardando", "Aguardando terceiros"
        CONCLUIDO = "concluido", "Concluídos"
        ADIADO = "adiado", "Adiados"
        ADIADO_PERM = "adiado_permanente", "Adiado permanente"
        CANCELADO = "cancelado", "Cancelados"

    class Loja(models.TextChoices):
        GERAL = "geral", "Geral"
        CENTRO = "centro", "Centro"
        VILA = "vila", "Vila Elias"
        AMBAS = "ambas", "Centro e Vila"

    titulo = models.CharField(max_length=200)
    descricao = models.TextField(blank=True, default="")
    status = models.CharField(
        max_length=24,
        choices=Status.choices,
        default=Status.DECIDIR,
        db_index=True,
    )
    loja = models.CharField(
        max_length=16,
        choices=Loja.choices,
        default=Loja.GERAL,
    )
    responsavel = models.CharField(max_length=120, blank=True, default="")
    ordem = models.PositiveIntegerField(default=0)
    seed_key = models.CharField(max_length=64, blank=True, default="", db_index=True)
    criado_por_nome = models.CharField(max_length=150, blank=True, default="")
    atualizado_por_nome = models.CharField(max_length=150, blank=True, default="")
    criado_em = models.DateTimeField(auto_now_add=True)
    atualizado_em = models.DateTimeField(auto_now=True)
    concluido_em = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["ordem", "-atualizado_em", "pk"]
        verbose_name = "Tarefa / pendência"
        verbose_name_plural = "Tarefas / pendências"

    def __str__(self) -> str:
        return self.titulo

    def marcar_concluida(self, *, quem: str) -> None:
        self.status = self.Status.CONCLUIDO
        self.atualizado_por_nome = (quem or "")[:150]
        self.concluido_em = timezone.now()


class TarefaComentarioAgro(models.Model):
    tarefa = models.ForeignKey(
        TarefaAgro,
        on_delete=models.CASCADE,
        related_name="comentarios",
    )
    texto = models.TextField()
    autor_nome = models.CharField(max_length=150)
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["criado_em"]
        verbose_name = "Comentário de tarefa"
        verbose_name_plural = "Comentários de tarefa"

    def __str__(self) -> str:
        return f"{self.autor_nome}: {self.texto[:40]}"


class TarefaEventoAgro(models.Model):
    class Tipo(models.TextChoices):
        CRIADA = "criada", "Criada"
        EDITADA = "editada", "Editada"
        STATUS = "status", "Mudança de status"
        COMENTARIO = "comentario", "Comentário"
        CONCLUIDA = "concluida", "Concluída"

    tarefa = models.ForeignKey(
        TarefaAgro,
        on_delete=models.CASCADE,
        related_name="eventos",
    )
    tipo = models.CharField(max_length=20, choices=Tipo.choices)
    autor_nome = models.CharField(max_length=150)
    detalhe = models.TextField(blank=True, default="")
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-criado_em", "-pk"]
        verbose_name = "Evento de tarefa"
        verbose_name_plural = "Eventos de tarefa"

    def __str__(self) -> str:
        return f"{self.tipo} por {self.autor_nome}"
