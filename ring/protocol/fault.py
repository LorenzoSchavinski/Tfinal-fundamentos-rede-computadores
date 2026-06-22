"""Modulo de insercao de falhas.

Simula erros de transmissao corrompendo, com certa probabilidade, um byte da
mensagem antes de envia-la. O receptor detecta a divergencia via CRC e responde
com NAK, exercitando o mecanismo de retransmissao do anel.
"""

from __future__ import annotations

import random


def maybe_corrupt(message: bytes, prob_percent: float, skip: bool = False) -> tuple:
    """Possivelmente corrompe um byte de ``message``.

    Retorna ``(mensagem_resultante, corrompida)``:
      - Se ``skip`` for True, ou a mensagem for vazia, ou ``prob_percent`` <= 0,
        nada acontece: devolve ``(message, False)``.
      - Com probabilidade ``prob_percent`` por cento, inverte UM byte aleatorio
        (XOR com valor 1..255, garantindo que o byte realmente mude e mantendo o
        tamanho): devolve ``(corrompida, True)``.
      - Caso contrario, devolve ``(message, False)``.
    """
    if skip or not message or prob_percent <= 0:
        return (message, False)

    # Sorteio uniforme em [0, 100); abaixo do limiar significa "deu falha".
    if random.uniform(0, 100) < prob_percent:
        corrompida = bytearray(message)
        pos = random.randrange(len(corrompida))
        # XOR com 1..255 nunca eh zero, entao o byte com certeza fica diferente.
        corrompida[pos] ^= random.randint(1, 255)
        return (bytes(corrompida), True)

    return (message, False)


if __name__ == "__main__":
    # Auto-teste: com 100% sempre corrompe e preserva o tamanho.
    base = b"Oi pessoal!"
    for _ in range(1000):
        out, flag = maybe_corrupt(base, 100)
        assert flag is True
        assert out != base
        assert len(out) == len(base)

    # Com skip=True nunca corrompe, mesmo a 100%.
    for _ in range(1000):
        out, flag = maybe_corrupt(base, 100, skip=True)
        assert flag is False
        assert out == base

    # prob 0 e mensagem vazia nao corrompem.
    assert maybe_corrupt(base, 0) == (base, False)
    assert maybe_corrupt(b"", 100) == (b"", False)

    print("fault.py: auto-teste OK")
