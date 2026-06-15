"""CRC-32 padrao IEEE 802.3 via zlib.

O algoritmo do zlib eh o mesmo usado por java.util.zip.CRC32, portanto o valor
produzido aqui eh byte-a-byte identico ao de outros grupos que usem Java/zlib.
Isso garante a interoperabilidade do campo de CRC trafegado na rede.
"""
from __future__ import annotations

import zlib


def crc32(data: bytes) -> int:
    """Retorna o CRC-32 de ``data`` como inteiro sem sinal de 32 bits."""
    # zlib.crc32 ja retorna sem sinal no Python 3, mas a mascara deixa explicito
    # o contrato e protege contra qualquer ambiente que devolva valor com sinal.
    return zlib.crc32(data) & 0xFFFFFFFF


def crc_field(data: bytes) -> str:
    """Representacao do CRC que vai na rede: o inteiro como string decimal."""
    return str(crc32(data))


def matches(data: bytes, field) -> bool:
    """Recalcula o CRC de ``data`` e compara com ``field``.

    ``field`` pode ser ``str`` ou ``bytes`` contendo o numero decimal recebido.
    Qualquer erro de conversao significa campo invalido, entao retorna ``False``.
    """
    try:
        if isinstance(field, bytes):
            field = field.decode("ascii")
        return crc32(data) == int(field)
    except (ValueError, UnicodeDecodeError, TypeError):
        return False


if __name__ == "__main__":
    # Auto-teste: valor de referencia combinado com os demais grupos.
    print(crc_field(b"Oi pessoal!"))
    assert crc_field(b"Oi pessoal!") == "1751094473"
    assert matches(b"Oi pessoal!", "1751094473")
    assert matches(b"Oi pessoal!", b"1751094473")
    assert not matches(b"Oi pessoal!", "0")
    assert not matches(b"Oi pessoal!", "nao-numero")
    print("crc.py: auto-teste OK")
