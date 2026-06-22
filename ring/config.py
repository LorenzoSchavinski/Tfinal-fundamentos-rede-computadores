"""Leitura do arquivo de configuracao de 5 linhas definido pela especificacao.

Layout exato (uma informacao por linha):
  linha 0: apelido               (string, ex.: "B")
  linha 1: tempo_token_e_dados   (segundos)
  linha 2: probabilidade_erro    (percentual)
  linha 3: timeout_token         (segundos)
  linha 4: tempo_minimo_entre_tokens (segundos)

Os numeros podem usar virgula decimal (ex.: "2,5" = 2.5).
"""

from __future__ import annotations

import math


class Config:
    """Parametros de operacao de uma maquina do anel."""

    def __init__(
        self,
        apelido: str,
        token_time: float,
        error_prob: float,
        token_timeout: float,
        min_token_interval: float,
    ) -> None:
        self.apelido = apelido
        # Campo "tempo_token_e_dados" do arquivo; controla o ritmo de passagem
        # do token quando a maquina nao esta transmitindo.
        self.token_time = token_time
        self.error_prob = error_prob  # probabilidade de erro (percentual)
        self.token_timeout = token_timeout  # tempo ate considerar token perdido
        self.min_token_interval = min_token_interval  # intervalo minimo entre tokens

    def __repr__(self) -> str:
        return (
            "Config(apelido={!r}, token_time={}, error_prob={}, "
            "token_timeout={}, min_token_interval={})".format(
                self.apelido,
                self.token_time,
                self.error_prob,
                self.token_timeout,
                self.min_token_interval,
            )
        )

    def __str__(self) -> str:
        return (
            "Config: apelido={} | token_time={}s | error_prob={}% | "
            "token_timeout={}s | min_token_interval={}s".format(
                self.apelido,
                self.token_time,
                self.error_prob,
                self.token_timeout,
                self.min_token_interval,
            )
        )


def _to_float(s: str) -> float:
    """Converte texto com virgula decimal em float (ex.: " 2,5 " -> 2.5)."""
    return float(s.strip().replace(",", "."))


def _from_lines(linhas: list) -> Config:
    """Monta o Config a partir de uma lista de linhas ja lidas do arquivo."""
    # Mantem apenas as 5 primeiras linhas nao vazias (apos strip).
    uteis = [ln.strip() for ln in linhas if ln.strip()]
    if len(uteis) < 5:
        raise ValueError(
            "Arquivo de configuracao invalido: esperadas 5 linhas nao vazias, "
            "encontradas {}.".format(len(uteis))
        )
    apelido = uteis[0].strip().upper()
    token_time = _to_float(uteis[1])
    error_prob = _to_float(uteis[2])
    token_timeout = _to_float(uteis[3])
    min_token_interval = _to_float(uteis[4])

    if not apelido or ":" in apelido:
        raise ValueError("Apelido invalido: use texto nao vazio e sem ':'.")
    try:
        apelido.encode("ascii")
    except UnicodeEncodeError as exc:
        raise ValueError("Apelido invalido: use apenas caracteres ASCII.") from exc
    valores = (token_time, error_prob, token_timeout, min_token_interval)
    if not all(math.isfinite(valor) for valor in valores):
        raise ValueError("Os valores numericos devem ser finitos.")
    if token_time <= 0:
        raise ValueError("tempo_token_e_dados deve ser maior que zero.")
    if not 0 <= error_prob <= 100:
        raise ValueError("probabilidade de erro deve estar entre 0 e 100.")
    if token_timeout <= 0:
        raise ValueError("timeout do token deve ser maior que zero.")
    if min_token_interval < 0:
        raise ValueError("tempo minimo entre tokens nao pode ser negativo.")

    return Config(
        apelido=apelido,
        token_time=token_time,
        error_prob=error_prob,
        token_timeout=token_timeout,
        min_token_interval=min_token_interval,
    )


def load(path: str) -> Config:
    """Le o arquivo em ``path`` e devolve um ``Config``.

    Usa as 5 primeiras linhas nao vazias e valida apelido, faixas e tempos antes
    de abrir qualquer socket.
    """
    with open(path, "r", encoding="utf-8") as f:
        return _from_lines(f.readlines())


if __name__ == "__main__":

    def deve_falhar(linhas) -> None:
        try:
            _from_lines(linhas)
        except ValueError:
            return
        raise AssertionError("configuracao invalida foi aceita: {!r}".format(linhas))

    # Auto-teste com exemplo em memoria "B/2/20/2,5/2" (uma linha por campo).
    exemplo = ["B", "2", "20", "2,5", "2"]
    cfg = _from_lines(exemplo)
    assert cfg.apelido == "B"
    assert cfg.token_timeout == 2.5
    assert cfg.token_time == 2.0
    assert cfg.error_prob == 20.0
    assert cfg.min_token_interval == 2.0

    # _to_float aceita virgula e espacos.
    assert _to_float(" 2,5 ") == 2.5

    # Menos de 5 linhas deve falhar com ValueError.
    try:
        _from_lines(["B", "2"])
        raise AssertionError("deveria ter levantado ValueError")
    except ValueError:
        pass

    # Faixas e apelidos invalidos devem falhar antes de abrir a rede.
    invalidos = [
        ["", "1", "0", "10", "1"],
        ["A:B", "1", "0", "10", "1"],
        ["A", "0", "0", "10", "1"],
        ["A", "1", "101", "10", "1"],
        ["A", "1", "0", "-1", "1"],
        ["A", "1", "0", "10", "-1"],
    ]
    for invalido in invalidos:
        deve_falhar(invalido)

    print(cfg)
    print("config.py: auto-teste OK")
