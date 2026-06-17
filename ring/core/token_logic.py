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
                self.tokens_duplicados += 1
                log("[{}] TOKEN DUPLICADO detectado (intervalo < minimo) -> removido da rede".format(self.apelido))
                self.last_token_rx = now  # atualiza janela para o proximo token genuino
                return
            else:
                self.last_token_rx = now

        self.has_token = True
        self._refresh_monitor_pause()  # token esta aqui: pausa o monitor enquanto o seguramos
        log("[{}] recebeu o token".format(self.apelido))

        if self._is_alone():
            # Caso degenerado: somos a unica maquina no anel. Nao enviamos o token a nos
            # mesmos; seguramos quieto ate outra maquina entrar (DISCOVER/HELLO o retoma).
            if not self._segurando_sozinho:
                log("[{}] sou a unica maquina no anel; segurando o token ate outra entrar".format(self.apelido))
                self._segurando_sozinho = True
            return

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
        # Corrige a corrida: se uma mensagem foi enfileirada durante a janela
        # de espera do token, envia os dados; senao, repassa o token.
        if ep == self.epoch and self.has_token and not self.waiting_for_data_return and not self._is_alone():
            if self.queue.is_empty():
                self._forward_token()
            else:
                self._send_data_packet(self.queue.peek())

    def _on_timer_data_return_timeout(self, ep) -> None:
        # O DATA enviado nesta epoca nao voltou a tempo. Se ainda aguardamos o
        # retorno (e a epoca nao mudou), desistimos: descartamos a cabeca da fila,
        # limpamos o estado de espera e liberamos o token. Caso contrario (retorno
        # ja ocorreu ou token foi regenerado) este timer eh inerte.
        if ep != self.epoch or not self.waiting_for_data_return:
            return
        log("[{}] timeout aguardando retorno dos dados, liberando token".format(self.apelido))
        self._drop_head()
        self.waiting_for_data_return = False
        self._refresh_monitor_pause()
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
        self.waiting_for_data_return = True
        self._refresh_monitor_pause()  # aguardando retorno dos dados: pausa monitor
        # Rede de seguranca: se o DATA nunca voltar (par dropou, destino saiu), o no
        # seguraria o token para sempre. Um timer guardado por epoca libera o token.
        ep = self.epoch
        threading.Timer(
            self.config.token_timeout,
            lambda: self.post("TIMER_DATA_RETURN_TIMEOUT", ep=ep),
        ).start()
        log("[{}] enviando DADOS para {} (corrompido={}): \"{}\"".format(
            self.apelido, item.destino, corrupted, item.message_str))
        self._send_to_successor(data)
        self.monitor.note_activity()

    def _forward_token(self) -> None:
        if self._is_alone():
            # Defensivo: nunca enviar o token a nos mesmos; mantem has_token inalterado.
            return
        # Libera o token ANTES de enviar: se o envio falhar, has_token ja esta False
        # e o monitor volta a vigiar, permitindo regeneracao. Nunca ficamos travados
        # segurando o token apos uma falha de envio (essencial na controladora).
        self.has_token = False
        self._refresh_monitor_pause()  # token saiu daqui: retoma vigilancia
        ap, _ = self._successor()
        log("[{}] enviando token para {}".format(self.apelido, ap))
        if not self._send_to_successor(build_token()):
            log("[{}] falha ao repassar token, recuperacao via monitor".format(self.apelido))
        self.monitor.note_activity()

    def _generate_token(self) -> None:
        self.epoch += 1
        if self.is_controller:
            self.expect_token_return = True
        # Baseline fresco para o detector de duplicata: evita que um timestamp
        # antigo julgue mal a chegada do proximo token apos a (re)geracao.
        self.last_token_rx = time.monotonic()
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
            self.tokens_perdidos += 1
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
