"""Transporte UDP do anel.

Encapsula um unico socket UDP e uma thread receptora. Dois modos de operacao:

  - "lan":   maquinas reais em rede; broadcast usa o endereco de difusao
             255.255.255.255 na porta fixa do anel.
  - "local": varias maquinas no mesmo host; o "broadcast" eh simulado enviando
             uma copia para cada alvo conhecido (lista de (ip, porta)).

Nada aqui interpreta o conteudo dos datagramas: isso eh papel da camada de
protocolo. Aqui so trafegam bytes crus.
"""
from __future__ import annotations

import socket
import threading

# WinError 10054: no Windows, enviar UDP para uma porta fechada faz o sistema
# devolver um ICMP port-unreachable que se manifesta como WinError 10054 no
# proximo recvfrom() do mesmo socket. Eh seguro ignorar e continuar o laco.
_WINERROR_CONNECTION_RESET = 10054


def detect_ip() -> str:
    """Descobre o IP primario da maquina (o usado para sair para a internet).

    Truque classico: "conecta" um socket UDP a um destino externo (8.8.8.8) e
    le o endereco local escolhido pela tabela de rotas. Nenhum pacote eh de fato
    enviado. Em falha (sem rede), cai para o loopback.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()


class Transport:
    """Wrapper de socket UDP com thread receptora e broadcast por modo."""

    def __init__(self, bind_ip, bind_port, mode, broadcast_targets, on_datagram) -> None:
        # mode: "lan" ou "local". on_datagram: callback(data: bytes, addr: (ip, porta)).
        # broadcast_targets so eh usado no modo "local" para simular difusao.
        self.bind_ip = bind_ip
        self.bind_port = bind_port
        self.mode = mode
        self.broadcast_targets = list(broadcast_targets or [])
        self.on_datagram = on_datagram

        self._stop = False
        self._thread = None

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Reuso do endereco: permite religar rapidamente e, no modo local, varias
        # instancias coexistirem em portas distintas sem reclamar de TIME_WAIT.
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if mode == "lan":
            # Necessario para poder enviar a 255.255.255.255.
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock.bind((bind_ip, bind_port))

    def start(self) -> None:
        """Sobe a thread daemon que recebe datagramas e chama on_datagram."""
        self._thread = threading.Thread(target=self._recv_loop, name="rx", daemon=True)
        self._thread.start()

    def _recv_loop(self) -> None:
        # Laco de recepcao. recvfrom bloqueia ate chegar pacote; quando close()
        # fecha o socket, recvfrom levanta OSError e o laco encerra limpo.
        # WinError 10054 (ICMP port-unreachable refletido de envio a porta
        # inexistente) eh ignorado: o laco continua sem encerrar o receptor.
        while not self._stop:
            try:
                data, addr = self.sock.recvfrom(2048)
            except OSError as exc:
                winerr = getattr(exc, "winerror", None)
                if winerr == _WINERROR_CONNECTION_RESET:
                    continue  # ICMP port-unreachable no Windows: ignorar
                break
            if data:
                self.on_datagram(data, addr)

    def broadcast(self, data: bytes) -> None:
        """Difunde ``data`` a todos: difusao real (lan) ou copia por alvo (local)."""
        if self.mode == "lan":
            self.sock.sendto(data, ("255.255.255.255", self.bind_port))
        else:
            for t in self.broadcast_targets:
                self.sock.sendto(data, t)

    def send_addr(self, ip, port, data: bytes) -> None:
        """Envia ``data`` para um endereco unico (ip, porta)."""
        self.sock.sendto(data, (ip, port))

    def local_addr(self):
        """Endereco (ip, porta) efetivamente vinculado ao socket."""
        return self.sock.getsockname()

    def close(self) -> None:
        """Sinaliza parada e fecha o socket (desbloqueia o recvfrom)."""
        self._stop = True
        try:
            self.sock.close()
        except OSError:
            pass
