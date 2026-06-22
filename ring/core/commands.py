"""Tratadores dos comandos do usuario (mixin do Node).

Estes metodos rodam SEMPRE na thread unica do motor (sao despachados pelo
barramento), entao mexem no estado do no sem locks, igual aos tratadores de
protocolo. Ficam separados de ``node.py`` apenas para manter cada arquivo focado
em uma responsabilidade; o ``Node`` herda este mixin, logo continua sendo um
unico objeto dono de todo o estado.
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
        destino = (
            destino.strip().upper()
        )  # normaliza apelido alvo; mensagem fica intacta
        if not destino or ":" in destino:
            log(
                "[{}] destino invalido: use apelido sem ':' ou BROADCAST".format(
                    self.apelido
                )
            )
            return
        try:
            destino.encode("ascii")
        except UnicodeEncodeError:
            log(
                "[{}] destino invalido: use apenas caracteres ASCII".format(
                    self.apelido
                )
            )
            return
        self.queue.enqueue(destino, message)
        log(
            "[{}] msg para {} enfileirada ({}/10)".format(
                self.apelido, destino, len(self.queue)
            )
        )

    def _on_cmd_add_token(self) -> None:
        # Injecao manual eh diferente de criacao inicial/recuperacao: deve criar
        # uma copia adicional sem abandonar o token que talvez ja esteja aqui.
        self._inject_token()

    def _on_cmd_remove_token(self) -> None:
        if self.has_token and not self.waiting_for_data_return:
            self.has_token = False
            self.epoch += 1
            self.active_data_attempt = None
            self.active_data_fingerprint = None
            self.remove_token_pending = False
            self._refresh_monitor_pause()
            log("[{}] token retirado da rede".format(self.apelido))
        elif self.has_token and self.waiting_for_data_return:
            self.remove_token_pending = True
            log(
                "[{}] retirada do token agendada para apos o retorno dos dados".format(
                    self.apelido
                )
            )
        else:
            self.remove_token_pending = True
            log(
                "[{}] retirada agendada: o proximo token recebido sera removido".format(
                    self.apelido
                )
            )

    def _on_cmd_status(self) -> None:
        ordem = [ap for ap, _ip, _port in self.ring.order()]
        suc_ap, _ = self._successor()
        sozinho = (
            "  (sozinho: segurando o token)"
            if (self.has_token and self._is_alone())
            else ""
        )
        log(
            "[{ap}] STATUS\n"
            "  endereco: {ip}:{port}  modo: {modo}\n"
            "  controladora: {ctrl}  com_token: {tok}{sozinho}  aguardando_retorno: {wait}\n"
            "  retirada_token_pendente: {remove_pending}\n"
            "  anel: {anel}\n"
            "  sucessor: {suc}  fila: {flen}  primeiro_token_gerado: {ftg}\n"
            "  tokens_perdidos: {perd}  tokens_duplicados: {dup}".format(
                ap=self.apelido,
                ip=self.advertise_ip,
                port=self.port,
                modo=self.mode,
                ctrl=self.is_controller,
                tok=self.has_token,
                sozinho=sozinho,
                wait=self.waiting_for_data_return,
                remove_pending=self.remove_token_pending,
                anel=ordem,
                suc=suc_ap,
                flen=len(self.queue),
                ftg=self.first_token_generated,
                perd=self.tokens_perdidos,
                dup=self.tokens_duplicados,
            )
        )

    def _on_cmd_queue(self) -> None:
        if self.queue.is_empty():
            log("[{}] fila vazia".format(self.apelido))
            return
        log("[{}] fila ({}/10):".format(self.apelido, len(self.queue)))
        for i, it in enumerate(self.queue.items()):
            log(
                '  {}. -> {} : "{}" '
                "(skip_fault_injection={}, retransmit_used={})".format(
                    i,
                    it.destino,
                    it.message_str,
                    it.skip_fault_injection,
                    it.retransmit_used,
                )
            )

    def _on_cmd_join(self) -> None:
        # Reenvia DISCOVER para reanunciar presenca e atualizar a topologia.
        self.transport.broadcast(build_discover(self.apelido, self.advertise_ip))
        log("[{}] DISCOVER reenviado (atualizando topologia)".format(self.apelido))
