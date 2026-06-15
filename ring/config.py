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
        self.token_time = token_time  # tempo segurando token / envio de dados
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
    return Config(
        apelido=uteis[0],
        token_time=_to_float(uteis[1]),
        error_prob=_to_float(uteis[2]),
        token_timeout=_to_float(uteis[3]),
        min_token_interval=_to_float(uteis[4]),
    )


def load(path: str) -> Config:
    """Le o arquivo em ``path`` e devolve um ``Config``.

    Usa as 5 primeiras linhas nao vazias; levanta ``ValueError`` se houver menos
    de 5. A linha 0 eh o apelido; as linhas 1 a 4 sao floats via ``_to_float``.
    """
    with open(path, "r", encoding="utf-8") as f:
        return _from_lines(f.readlines())


if __name__ == "__main__":
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

    print(cfg)
    print("config.py: auto-teste OK")
