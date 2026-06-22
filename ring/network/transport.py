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
import traceback

from ring.ui.console import log

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

    def __init__(
        self, bind_ip, bind_port, mode, broadcast_targets, on_datagram
    ) -> None:
        # mode: "lan" ou "local". on_datagram: callback(data: bytes, addr: (ip, porta)).
        # broadcast_targets so eh usado no modo "local" para simular difusao.
        self.bind_ip = bind_ip
        self.bind_port = bind_port
        self.mode = mode
        self.broadcast_targets = list(broadcast_targets or [])
        self.on_datagram = on_datagram

        self._stop = threading.Event()
        self._thread = None

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Permite reiniciar rapidamente a aplicacao na mesma porta UDP.
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
        while not self._stop.is_set():
            try:
                # Buffer para um datagrama UDP completo. Evita truncar mensagens
                # maiores e transforma-las artificialmente em erro de CRC.
                data, addr = self.sock.recvfrom(65535)
            except OSError as exc:
                winerr = getattr(exc, "winerror", None)
                if winerr == _WINERROR_CONNECTION_RESET:
                    continue  # ICMP port-unreachable no Windows: ignorar
                break
            if data:
                # Um pacote malformado nunca deve derrubar o receptor: isola o handler.
                try:
                    self.on_datagram(data, addr)
                except Exception:
                    log(
                        "[transport] erro ao tratar datagrama de {}: {}".format(
                            addr, traceback.format_exc()
                        )
                    )

    def _sendto(self, data: bytes, addr) -> bool:
        """Envia ``data`` a ``addr`` tolerando falha: loga e retorna False em erro."""
        try:
            self.sock.sendto(data, addr)
            return True
        except OSError as exc:
            log("[transport] falha ao enviar para {}: {}".format(addr, exc))
            return False

    def broadcast(self, data: bytes) -> bool:
        """Difunde ``data`` a todos: difusao real (lan) ou copia por alvo (local)."""
        if self.mode == "lan":
            return self._sendto(data, ("255.255.255.255", self.bind_port))
        ok = True
        for t in self.broadcast_targets:
            ok = self._sendto(data, t) and ok
        return ok

    def send_addr(self, ip, port, data: bytes) -> bool:
        """Envia ``data`` para um endereco unico (ip, porta)."""
        # Evidencia de fio: registra apenas pacotes DATA (prefixo "2000") enviados.
        if data[:4] == b"2000":
            log(
                "[wire TX -> {}:{}] {}".format(
                    ip, port, data.decode("utf-8", errors="replace")
                )
            )
        return self._sendto(data, (ip, port))

    def close(self) -> None:
        """Sinaliza parada e fecha o socket (desbloqueia o recvfrom)."""
        self._stop.set()
        try:
            self.sock.close()
        except OSError:
            pass
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=1)
