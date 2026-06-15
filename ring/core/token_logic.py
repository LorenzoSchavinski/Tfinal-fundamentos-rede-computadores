"""Ciclo de vida do token e emissao de dados (mixin do Node).

Reune o que acontece quando o token chega, eh segurado, encaminhado ou gerado,
e o envio de um pacote de DADOS quando ha algo na fila. Tudo roda na thread
unica do motor, entao manipula o estado do no diretamente, sem locks.

Separado de ``node.py`` para manter o arquivo do motor dentro do limite e cada
modulo com uma responsabilidade clara. O ``Node`` herda este mixin e continua
sendo o unico objeto dono do estado.
"""
from __future__ import annotations

import threading
import time

from ring.protocol import crc as crc_mod
from ring.protocol.packets import BROADCAST, CTRL_NONE, build_data, build_token
from ring.protocol.fault import maybe_corrupt
from ring.ui.console import log


class TokenLogicMixin:
    """Recepcao, retencao, encaminhamento e geracao do token. Assume atributos do Node."""

    def _on_rx_token(self) -> None:
        self.observed_activity = True
        self.monitor.note_activity()
        now = time.monotonic()

        if self.is_controller:
            if self.expect_token_return:
                # Primeiro retorno apos (re)gerar o token: nao eh duplicata.
                self.expect_token_return = False
                self.last_token_rx = now
            elif self.last_token_rx is not None and (now - self.last_token_rx) < self.config.min_token_interval:
                # Intervalo curto demais => ha token duplicado no anel: consome este.
                # NAO incrementa epoch aqui: o token real (que ja esta aqui) possui um
                # TIMER_FORWARD_TOKEN pendente com a epoca atual; bumpar epoch cancelaria
                # esse timer e mataria o anel. Consumir silenciosamente o token excedente
                # (retornar sem encaminhar) eh suficiente.
                log("[{}] TOKEN DUPLICADO detectado (intervalo < minimo) -> removido da rede".format(self.apelido))
                self.last_token_rx = now  # atualiza janela para o proximo token genuino
                return
            else:
                self.last_token_rx = now

        self.has_token = True
        self._refresh_monitor_pause()  # token esta aqui: pausa o monitor enquanto o seguramos
        log("[{}] recebeu o token".format(self.apelido))

        if not self.queue.is_empty():
            # Segura o token e transmite; nao encaminha agora.
            self._send_data_packet(self.queue.peek())
        else:
            # Segura o token por token_time e depois encaminha (timer guardado por epoca).
            ep = self.epoch
            threading.Timer(
                self.config.token_time,
                lambda: self.post("TIMER_FORWARD_TOKEN", ep=ep),
            ).start()

    def _on_timer_forward_token(self, ep) -> None:
        # So encaminha se o timer ainda eh valido e nada mudou (sem dados pendentes).
        if ep == self.epoch and self.has_token and self.queue.is_empty():
            self._forward_token()

    def _send_data_packet(self, item) -> None:
        crc = crc_mod.crc_field(item.message_bytes)
        msg = item.message_bytes
        is_bcast = (item.destino == BROADCAST)
        # Broadcast e itens marcados no_error nunca sofrem injecao de falha.
        msg_out, corrupted = maybe_corrupt(
            msg, self.config.error_prob, skip=(item.no_error or is_bcast)
        )
        data = build_data(self.apelido, item.destino, CTRL_NONE, crc, msg_out)
        self.inflight = item
        self.waiting_for_data_return = True
        self._refresh_monitor_pause()  # aguardando retorno dos dados: pausa monitor
        log("[{}] enviando DADOS para {} (corrompido={}): \"{}\"".format(
            self.apelido, item.destino, corrupted, item.message_str))
        self._send_to_successor(data)
        self.monitor.note_activity()

    def _forward_token(self) -> None:
        self.has_token = False
        self._refresh_monitor_pause()  # token saiu daqui: retoma vigilancia
        ap, _ = self._successor()
        log("[{}] enviando token para {}".format(self.apelido, ap))
        self._send_to_successor(build_token())
        self.monitor.note_activity()

    def _generate_token(self) -> None:
        self.epoch += 1
        if self.is_controller:
            self.expect_token_return = True
        self.first_token_generated = True
        self.monitor.set_enabled(self.is_controller)
        self.has_token = False
        self._refresh_monitor_pause()  # token enviado: monitor pode observar a circulacao
        log("[{}] gerou/inseriu um token na rede".format(self.apelido))
        self._send_to_successor(build_token())
        self.monitor.note_activity()

    # ----------------------------------------------------- handlers do monitor
    def _on_mon_token_timeout(self) -> None:
        # Token perdido (timeout na controladora): so regenera se ha mais de um
        # membro e nao estamos no meio de uma transmissao de dados.
        if len(self.ring.members()) > 1 and not self.waiting_for_data_return:
            log("[{}] TOKEN PERDIDO detectado (timeout) -> gerando novo token".format(self.apelido))
            self._generate_token()

    def _on_eval_first_token(self) -> None:
        # Apos a janela de descoberta: a primeira maquina cria o token inicial,
        # mas somente se nenhuma atividade (token/dados) ja tiver sido observada.
        membros = len(self.ring.members())
        if self.is_controller and not self.first_token_generated and not self.observed_activity and membros >= 1:
            log("[{}] sou a primeira maquina (menor apelido) e nenhum token observado -> gerando token inicial".format(self.apelido))
            self._generate_token()
        elif self.is_controller:
            # Token ja existe ou existira; habilita a vigilancia.
            self.monitor.set_enabled(True)
