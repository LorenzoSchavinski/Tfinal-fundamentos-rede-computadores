"""Fila de mensagens por maquina (FIFO, ate 10 itens).

Cada item guarda o apelido de destino e a mensagem. A maquina so transmite
quando esta de posse do token, retirando o item da cabeca da fila.
"""
from __future__ import annotations


class QueueItem:
    """Uma mensagem enfileirada aguardando envio."""

    def __init__(self, destino: str, message_str: str) -> None:
        self.destino = destino
        self.message_str = message_str  # forma textual, para exibicao
        self.message_bytes = message_str.encode("utf-8")  # forma enviada na rede
        # Flags de acompanhamento ao longo da volta no anel:
        self.no_error = False  # destino confirmou recebimento sem erro (ACK)
        self.retransmit_used = False  # ja houve uma retransmissao deste item

    def __repr__(self) -> str:
        return "QueueItem(destino={!r}, message_str={!r}, no_error={}, retransmit_used={})".format(
            self.destino, self.message_str, self.no_error, self.retransmit_used
        )


class MessageQueue:
    """Fila FIFO limitada a ``MAX`` mensagens."""

    MAX = 10

    def __init__(self) -> None:
        self._items: list = []

    def enqueue(self, destino: str, message_str: str) -> bool:
        """Adiciona uma mensagem ao fim da fila.

        Retorna ``False`` se a fila ja estiver cheia (``MAX`` itens); caso
        contrario cria o ``QueueItem``, adiciona e retorna ``True``.
        """
        if self.is_full():
            return False
        self._items.append(QueueItem(destino, message_str))
        return True

    def peek(self):
        """Retorna o item da cabeca sem remove-lo, ou ``None`` se vazia."""
        if not self._items:
            return None
        return self._items[0]

    def pop(self):
        """Remove e retorna o item da cabeca, ou ``None`` se vazia."""
        if not self._items:
            return None
        return self._items.pop(0)

    def is_empty(self) -> bool:
        return not self._items

    def is_full(self) -> bool:
        return len(self._items) >= self.MAX

    def __len__(self) -> int:
        return len(self._items)

    def items(self) -> list:
        """Copia da lista de itens, para exibicao de status."""
        return list(self._items)


if __name__ == "__main__":
    # Auto-teste da fila.
    q = MessageQueue()
    assert q.is_empty()

    # Preenche ate 10.
    for i in range(MessageQueue.MAX):
        assert q.enqueue("A", "msg{}".format(i)) is True
    assert len(q) == 10
    assert q.is_full()

    # 11o enqueue deve falhar.
    assert q.enqueue("A", "demais") is False
    assert len(q) == 10

    # Flags nascem False.
    cabeca = q.peek()
    assert cabeca is not None
    assert cabeca.no_error is False
    assert cabeca.retransmit_used is False
    assert cabeca.message_bytes == b"msg0"

    # peek nao remove; pop remove e devolve o mesmo item.
    assert q.peek() is cabeca
    assert len(q) == 10
    saido = q.pop()
    assert saido is cabeca
    assert len(q) == 9
    # Apos o pop, a nova cabeca eh a proxima mensagem.
    assert q.peek().message_str == "msg1"

    print("message_queue.py: auto-teste OK")
