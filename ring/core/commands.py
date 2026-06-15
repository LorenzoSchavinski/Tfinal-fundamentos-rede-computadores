"""Tratadores dos comandos do usuario (mixin do Node).

Estes metodos rodam SEMPRE na thread unica do motor (sao despachados pelo
barramento), entao mexem no estado do no sem locks, igual aos tratadores de
protocolo. Ficam separados de ``node.py`` apenas para manter cada arquivo focado
e dentro do limite de tamanho; o ``Node`` herda este mixin, logo continua sendo
um unico objeto dono de todo o estado.
"""
from __future__ import annotations

from ring.protocol.packets import build_discover
from ring.ui.console import log


class CommandsMixin:
    """Handlers de CMD_* postados pelo console. Assume atributos do Node."""

    def _on_cmd_send(self, destino, message) -> None:
        if self.queue.is_full():
            log("[{}] fila cheia (max 10)".format(self.apelido))
            return
        self.queue.enqueue(destino, message)
        log("[{}] msg para {} enfileirada ({}/10)".format(self.apelido, destino, len(self.queue)))

    def _on_cmd_add_token(self) -> None:
        # Insere um token na rede (gera e envia ao sucessor).
        self._generate_token()

    def _on_cmd_remove_token(self) -> None:
        # Retira o token somente se ele esta aqui. Bumpar epoch sem ter o token
        # invalidaria timers de encaminhamento legitimamente pendentes em outros nos.
        if self.has_token:
            self.has_token = False
            self.epoch += 1
            self._refresh_monitor_pause()
            log("[{}] token retirado da rede".format(self.apelido))
        else:
            log("[{}] nao estou com o token no momento (nada a retirar)".format(self.apelido))

    def _on_cmd_status(self) -> None:
        ordem = [ap for ap, _ip, _port in self.ring.order()]
        suc_ap, _ = self._successor()
        log(
            "[{ap}] STATUS\n"
            "  endereco: {ip}:{port}  modo: {modo}\n"
            "  controladora: {ctrl}  com_token: {tok}  aguardando_retorno: {wait}\n"
            "  anel: {anel}\n"
            "  sucessor: {suc}  fila: {flen}  primeiro_token_gerado: {ftg}".format(
                ap=self.apelido, ip=self.advertise_ip, port=self.port, modo=self.mode,
                ctrl=self.is_controller, tok=self.has_token, wait=self.waiting_for_data_return,
                anel=ordem, suc=suc_ap, flen=len(self.queue), ftg=self.first_token_generated,
            )
        )

    def _on_cmd_queue(self) -> None:
        if self.queue.is_empty():
            log("[{}] fila vazia".format(self.apelido))
            return
        log("[{}] fila ({}/10):".format(self.apelido, len(self.queue)))
        for i, it in enumerate(self.queue.items()):
            log("  {}. -> {} : \"{}\" (no_error={}, retransmit_used={})".format(
                i, it.destino, it.message_str, it.no_error, it.retransmit_used))

    def _on_cmd_join(self) -> None:
        # Reenvia DISCOVER para reanunciar presenca e atualizar a topologia.
        self.transport.broadcast(build_discover(self.apelido, self.advertise_ip))
        log("[{}] DISCOVER reenviado (atualizando topologia)".format(self.apelido))
