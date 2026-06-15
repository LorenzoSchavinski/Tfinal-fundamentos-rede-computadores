"""CONTRATO DE INTEROPERABILIDADE DA REDE (formato dos datagramas).

Todo trafego do anel passa por aqui. Este arquivo define exatamente como cada
datagrama eh montado e desmontado, porque maquinas de grupos diferentes precisam
falar o mesmo "idioma" no fio. Por isso o codigo eh comentado em detalhe.

Regras gerais do formato:
  - Um datagrama eh uma sequencia de bytes.
  - Os campos do cabecalho sao texto ASCII separados por ':' (dois-pontos).
  - A mensagem (payload de DATA) permanece em bytes crus e NAO eh "escapada":
    ela pode conter ':' a vontade. Por isso o parser limita o numero de
    divisoes (maxsplit) para nunca quebrar a mensagem.

Tipos de pacote (prefixo numerico antes do primeiro ':'):
  10   -> DISCOVER  (anuncio de entrada na rede)
  20   -> HELLO     (resposta de presenca)
  1000 -> TOKEN     (o bastao que circula no anel; nao tem ':')
  2000 -> DATA      (mensagem de dados com origem, destino, controle e CRC)
"""
from __future__ import annotations

# --- Prefixos de tipo (string, pois vao em ASCII no fio) ---------------------
P_DISCOVER = "10"
P_HELLO = "20"
P_TOKEN = "1000"
P_DATA = "2000"

# Destino especial usado em anuncios.
BROADCAST = "BROADCAST"

# --- Valores do campo de controle de DATA ------------------------------------
# CTRL_NONE: estado inicial; significa "ainda nao se sabe se a maquina destino
# existe" (literalmente "maquina inexistente" ate prova em contrario).
CTRL_NONE = "maquinainexistente"
CTRL_ACK = "ACK"  # destino recebeu sem erro de CRC
CTRL_NAK = "NAK"  # destino recebeu com erro de CRC (pede retransmissao)

# Separador de campos do cabecalho.
_SEP = b":"


# =============================================================================
# Construtores (sempre retornam bytes prontos para o socket)
# =============================================================================
def build_discover(apelido: str, ip: str) -> bytes:
    """Monta ``b"10:<apelido>:<ip>"``."""
    return P_DISCOVER.encode("ascii") + _SEP + apelido.encode("ascii") + _SEP + ip.encode("ascii")


def build_hello(apelido: str, ip: str) -> bytes:
    """Monta ``b"20:<apelido>:<ip>"``."""
    return P_HELLO.encode("ascii") + _SEP + apelido.encode("ascii") + _SEP + ip.encode("ascii")


def build_token() -> bytes:
    """Monta o token: apenas ``b"1000"`` (sem campos)."""
    return P_TOKEN.encode("ascii")


def build_data(origem: str, destino: str, controle: str, crc: str, message: bytes) -> bytes:
    """Monta ``b"2000:<origem>:<destino>:<controle>:<crc>:" + message``.

    A ``message`` (bytes) eh anexada literalmente, sem nenhuma transformacao,
    para que qualquer ':' interno seja preservado no fio.
    """
    cabecalho = (
        P_DATA.encode("ascii")
        + _SEP
        + origem.encode("ascii")
        + _SEP
        + destino.encode("ascii")
        + _SEP
        + controle.encode("ascii")
        + _SEP
        + crc.encode("ascii")
        + _SEP
    )
    return cabecalho + message


def set_controle(raw: bytes, new_controle: str) -> bytes:
    """Troca SOMENTE o campo de controle de um datagrama DATA completo.

    O CRC e os bytes da mensagem sao mantidos exatamente como estavam. A divisao
    usa maxsplit=5, de modo que ``partes[5]`` ainda contem a mensagem crua (com
    eventuais ':') intacta; trocamos ``partes[3]`` (controle) e rejuntamos.
    """
    partes = raw.split(_SEP, 5)
    # Sem 6 partes nao eh um DATA valido: devolve o datagrama inalterado.
    if len(partes) < 6:
        return raw
    partes[3] = new_controle.encode("ascii")
    return _SEP.join(partes)


# =============================================================================
# Parser (desmonta o datagrama recebido)
# =============================================================================
def parse(datagram: bytes) -> dict:
    """Classifica e desmonta um datagrama recebido.

    Passos:
      1) Remove exatamente UM ``b"\\x00"`` final, se houver. O cliente C++ de
         referencia envia ``strlen+1``, incluindo o NUL terminador, entao esse
         byte extra precisa ser descartado antes de interpretar o pacote.
      2) Classifica pelo prefixo antes do primeiro ':' (ou pelo payload inteiro
         igual a ``b"1000"`` no caso do token).

    Retorna sempre um dict com a chave ``'type'`` (DISCOVER/HELLO/TOKEN/DATA/
    UNKNOWN) e ``'raw'`` (o datagrama ja sem o NUL).
    """
    # (1) Descarta um unico NUL terminador, se presente.
    if datagram.endswith(b"\x00"):
        datagram = datagram[:-1]

    # (2a) Token: o payload inteiro eh "1000", sem separadores.
    if datagram == P_TOKEN.encode("ascii"):
        return {"type": "TOKEN", "raw": datagram}

    # Prefixo = tudo antes do primeiro ':'.
    prefixo, sep, _ = datagram.partition(_SEP)
    if not sep:
        # Nao ha ':' e nao eh token -> desconhecido.
        return {"type": "UNKNOWN", "raw": datagram}
    prefixo = prefixo.decode("ascii", errors="replace")

    if prefixo == P_DISCOVER or prefixo == P_HELLO:
        # "10:<apelido>:<ip>" -> 3 partes.
        partes = datagram.split(_SEP, 2)
        if len(partes) < 3:
            return {"type": "UNKNOWN", "raw": datagram}
        tipo = "DISCOVER" if prefixo == P_DISCOVER else "HELLO"
        return {
            "type": tipo,
            "apelido": partes[1].decode("ascii", errors="replace"),
            "ip": partes[2].decode("ascii", errors="replace"),
            "raw": datagram,
        }

    if prefixo == P_DATA:
        # "2000:<origem>:<destino>:<controle>:<crc>:<message>" -> 6 partes.
        # maxsplit=5 mantem a mensagem (com qualquer ':') inteira em partes[5].
        partes = datagram.split(_SEP, 5)
        if len(partes) < 6:
            return {"type": "UNKNOWN", "raw": datagram}
        return {
            "type": "DATA",
            "origem": partes[1].decode("ascii", errors="replace"),
            "destino": partes[2].decode("ascii", errors="replace"),
            "controle": partes[3].decode("ascii", errors="replace"),
            "crc": partes[4].decode("ascii", errors="replace"),
            "message": partes[5],  # permanece em bytes crus
            "raw": datagram,
        }

    return {"type": "UNKNOWN", "raw": datagram}


if __name__ == "__main__":
    # --- Auto-teste do contrato de fio ---
    # Round-trip de DATA: o ':' dentro da mensagem deve sobreviver.
    d = build_data("B", "A", CTRL_NONE, "19385749", b"Oi: pessoal!")
    p = parse(d)
    assert p["type"] == "DATA"
    assert p["origem"] == "B"
    assert p["destino"] == "A"
    assert p["controle"] == "maquinainexistente"
    assert p["crc"] == "19385749"
    assert p["message"] == b"Oi: pessoal!"

    # set_controle troca so o controle, preservando crc e mensagem.
    r = set_controle(d, CTRL_ACK)
    pr = parse(r)
    assert pr["controle"] == "ACK"
    assert pr["message"] == b"Oi: pessoal!"
    assert pr["crc"] == "19385749"

    # NUL terminador (cliente C++) deve ser descartado sem afetar a mensagem.
    assert parse(d + b"\x00")["message"] == b"Oi: pessoal!"

    # Token e Discover.
    assert parse(build_token())["type"] == "TOKEN"
    pd = parse(build_discover("A", "1.2.3.4"))
    assert pd["type"] == "DISCOVER"
    assert pd["apelido"] == "A"
    assert pd["ip"] == "1.2.3.4"

    print("packets.py: auto-teste OK")
