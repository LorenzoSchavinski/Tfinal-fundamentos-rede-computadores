"""Ponto de entrada da simulacao do anel de tokens sobre UDP.

Executado a partir do diretorio base como:

    python main.py <config> [--port N] [--peers arq] [--ip IP] [--discovery S]

Dois modos:
  - lan   (padrao): maquinas reais; porta fixa 6000; IP anunciado detectado
                    automaticamente (ou forcado por --ip).
  - local (--peers): varias maquinas no mesmo host; cada uma escuta em --port e
                    conhece as demais por um arquivo de peers.
"""

from __future__ import annotations

import argparse

from ring import config
from ring.core.node import Node
from ring.network.transport import detect_ip
from ring.ui.console import Console, log


def _parse_peers(path: str) -> dict:
    """Le o arquivo de peers: linhas "<apelido> <ip> <porta>".

    Aceita comentarios iniciados por '#' e ignora linhas em branco. O proprio no
    deve constar no arquivo (o mapa inclui a si mesmo).
    """
    peers = {}
    with open(path, "r", encoding="utf-8") as f:
        for numero, linha in enumerate(f, start=1):
            linha = linha.strip()
            if not linha or linha.startswith("#"):
                continue
            partes = linha.split()
            if len(partes) != 3:
                raise ValueError(
                    "Linha {} do arquivo de peers deve ter: apelido ip porta.".format(
                        numero
                    )
                )
            apelido, ip, porta = partes[0], partes[1], partes[2]
            apelido = apelido.upper()
            if not apelido or ":" in apelido:
                raise ValueError("Apelido invalido na linha {}.".format(numero))
            try:
                apelido.encode("ascii")
            except UnicodeEncodeError as exc:
                raise ValueError(
                    "Apelido invalido na linha {}: use apenas ASCII.".format(numero)
                ) from exc
            if apelido in peers:
                raise ValueError(
                    "Apelido duplicado no arquivo de peers: {}.".format(apelido)
                )
            porta = int(porta)
            if not 1 <= porta <= 65535:
                raise ValueError("Porta invalida na linha {}.".format(numero))
            peers[apelido] = (ip, porta)
    return peers


def main() -> None:
    parser = argparse.ArgumentParser(description="No do anel de tokens sobre UDP")
    parser.add_argument("config_path", help="arquivo de configuracao de 5 linhas")
    parser.add_argument(
        "--port",
        type=int,
        default=6000,
        help="porta de escuta (so faz sentido com --peers / modo local)",
    )
    parser.add_argument(
        "--peers",
        default=None,
        help="arquivo de peers; sua presenca ativa o modo local",
    )
    parser.add_argument(
        "--ip",
        default=None,
        help="IP anunciado; sem ele, lan auto-detecta e local usa 127.0.0.1",
    )
    parser.add_argument(
        "--discovery",
        type=float,
        default=6.0,
        help="janela de descoberta (s) antes de avaliar o token inicial",
    )
    args = parser.parse_args()

    cfg = config.load(args.config_path)

    if args.peers:
        mode = "local"
        peers = _parse_peers(args.peers)
        if cfg.apelido not in peers:
            raise ValueError(
                "O proprio apelido {} deve constar no arquivo de peers.".format(
                    cfg.apelido
                )
            )
        bind_ip = "0.0.0.0"
        bind_port = args.port
        advertise_ip = args.ip or "127.0.0.1"
    else:
        mode = "lan"
        peers = None
        bind_ip = "0.0.0.0"
        bind_port = 6000
        advertise_ip = args.ip or detect_ip()

    node = Node(cfg, mode, bind_ip, bind_port, advertise_ip, peers)
    node.discovery_window = args.discovery

    log("=== Anel de Tokens (UDP) ===")
    log(
        "apelido: {} | modo: {} | endereco anunciado: {}:{}".format(
            cfg.apelido, mode, advertise_ip, bind_port
        )
    )
    log(str(cfg))
    if mode == "local":
        log(
            "peers: {}".format(
                {ap: "{}:{}".format(ip, pt) for ap, (ip, pt) in peers.items()}
            )
        )

    node.start()
    try:
        Console(cfg.apelido, node.post).run()
    finally:
        node.close()


if __name__ == "__main__":
    main()
