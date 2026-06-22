"""Ciclo de vida do token e emissao de dados (mixin do Node).

Reune o que acontece quando o token chega, eh segurado, encaminhado ou gerado,
e o envio de um pacote de DADOS quando ha algo na fila. Tudo roda na thread
unica do motor, entao manipula o estado do no diretamente, sem locks.

Separado de ``node.py`` para deixar o ciclo do token em um modulo focado. O
``Node`` herda este mixin e continua sendo o unico objeto dono do estado.
"""

from __future__ import annotations

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

        # Pedido feito "a qualquer momento": o proximo token recebido eh
        # consumido antes de alterar posse, fila ou timers.
        if self.remove_token_pending:
            self.remove_token_pending = False
            log(
                "[{}] token recebido e retirado da rede (pedido pendente)".format(
                    self.apelido
                )
            )
            return

        # Se ja seguramos um token, este novo 1000 eh necessariamente excedente.
        # A controladora o contabiliza e remove; as demais apenas o repassam para
        # que a controladora oficial possa detectar e registrar a duplicidade.
        if self.has_token:
            if self.is_controller:
                self.tokens_duplicados += 1
                log(
                    "[{}] TOKEN DUPLICADO detectado (ja possuia token) -> removido da rede".format(
                        self.apelido
                    )
                )
            else:
                log(
                    "[{}] token extra recebido enquanto ja possuia um; repassando para a controladora".format(
                        self.apelido
                    )
                )
                self._send_to_successor(build_token())
            return

        if self.is_controller:
            if self.expect_token_return:
                # Primeiro retorno apos (re)gerar o token: nao eh duplicata.
                self.expect_token_return = False
                self.last_token_rx = now
            elif (
                self.last_token_rx is not None
                and (now - self.last_token_rx) < self.config.min_token_interval
            ):
                # Intervalo curto demais => ha token duplicado no anel: consome este.
                # Nao altera epoch: pode existir um timer legitimo de encaminhamento
                # usando a epoca atual. Basta consumir o pacote excedente.
                self.tokens_duplicados += 1
                log(
                    "[{}] TOKEN DUPLICADO detectado (intervalo < minimo) -> removido da rede".format(
                        self.apelido
                    )
                )
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
                log(
                    "[{}] sou a unica maquina no anel; segurando o token ate outra entrar".format(
                        self.apelido
                    )
                )
                self._segurando_sozinho = True
            return

        if not self.queue.is_empty():
            # Segura o token e transmite; nao encaminha agora.
            self._send_data_packet(self.queue.peek())
        else:
            # Segura o token por token_time e depois encaminha (timer guardado por epoca).
            ep = self.epoch
            self._start_timer(
                self.config.token_time,
                "TIMER_FORWARD_TOKEN",
                ep=ep,
            )

    def _on_timer_forward_token(self, ep) -> None:
        # Corrige a corrida: se uma mensagem foi enfileirada durante a janela
        # de espera do token, envia os dados; senao, repassa o token.
        if (
            ep == self.epoch
            and self.has_token
            and not self.waiting_for_data_return
            and not self._is_alone()
        ):
            if self.queue.is_empty():
                self._forward_token()
            else:
                self._send_data_packet(self.queue.peek())

    def _on_timer_data_return_timeout(self, attempt) -> None:
        # O DATA desta tentativa nao voltou a tempo. O identificador local impede
        # que um timer antigo descarte a cabeca de uma transmissao mais nova.
        if attempt != self.active_data_attempt or not self.waiting_for_data_return:
            return
        log(
            "[{}] timeout aguardando retorno dos dados, liberando token".format(
                self.apelido
            )
        )
        self._drop_head()
        self._complete_data_round()

    def _send_data_packet(self, item) -> None:
        crc = crc_mod.crc_field(item.message_bytes)
        msg = item.message_bytes
        is_bcast = item.destino == BROADCAST
        # Broadcast e retransmissoes corretas nao sofrem injecao de falha.
        msg_out, corrupted = maybe_corrupt(
            msg,
            self.config.error_prob,
            skip=(item.skip_fault_injection or is_bcast),
        )
        data = build_data(self.apelido, item.destino, CTRL_NONE, crc, msg_out)
        self.waiting_for_data_return = True
        self.data_attempt_seq += 1
        self.active_data_attempt = self.data_attempt_seq
        self.active_data_fingerprint = (item.destino, crc, msg_out)
        self.last_data_activity = time.monotonic()
        self._refresh_monitor_pause()  # aguardando retorno dos dados: pausa monitor
        # Rede de seguranca: se o DATA nunca voltar (par dropou, destino saiu), o no
        # seguraria o token para sempre. Um timer identificado libera o token.
        attempt = self.active_data_attempt
        self._start_timer(
            self.config.token_timeout,
            "TIMER_DATA_RETURN_TIMEOUT",
            attempt=attempt,
        )
        log(
            '[{}] enviando DADOS para {} (corrompido={}): "{}"'.format(
                self.apelido, item.destino, corrupted, item.message_str
            )
        )
        self._send_to_successor(data)
        self.monitor.note_activity()

    def _complete_data_round(self) -> None:
        """Finaliza a tentativa atual e encaminha ou remove o token."""
        self.waiting_for_data_return = False
        self.active_data_attempt = None
        self.active_data_fingerprint = None
        self._refresh_monitor_pause()
        if self.remove_token_pending:
            self.remove_token_pending = False
            self.has_token = False
            self.epoch += 1
            self._refresh_monitor_pause()
            log(
                "[{}] token retirado da rede apos concluir os dados".format(
                    self.apelido
                )
            )
            return
        self._forward_token()

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
            log(
                "[{}] falha ao repassar token, recuperacao via monitor".format(
                    self.apelido
                )
            )
        self.monitor.note_activity()

    def _generate_token(self) -> None:
        self.epoch += 1
        if self.is_controller:
            self.expect_token_return = True
        # O primeiro retorno da nova geracao cria a baseline do detector.
        self.last_token_rx = None
        self.first_token_generated = True
        self.monitor.set_enabled(self.is_controller)
        self.has_token = False
        self._refresh_monitor_pause()  # token enviado: monitor pode observar a circulacao
        log("[{}] gerou/inseriu um token na rede".format(self.apelido))
        self._send_to_successor(build_token())
        self.monitor.note_activity()

    def _inject_token(self) -> None:
        """Insere exatamente mais um pacote 1000 sem substituir o token local."""
        log("[{}] inseriu um token adicional na rede".format(self.apelido))
        self._send_to_successor(build_token())
        self.monitor.note_activity()

    # ----------------------------------------------------- handlers do monitor
    def _on_mon_token_timeout(self) -> None:
        # Token perdido (timeout na controladora): so regenera se ha mais de um
        # membro e nao estamos no meio de uma transmissao de dados. O papel eh
        # conferido novamente porque o evento pode ter aguardado no barramento
        # enquanto uma mudanca de topologia escolhia outra controladora.
        if (
            self.is_controller
            and len(self.ring.members()) > 1
            and not self.has_token
            and not self.waiting_for_data_return
        ):
            self.tokens_perdidos += 1
            log(
                "[{}] TOKEN PERDIDO detectado (timeout) -> gerando novo token".format(
                    self.apelido
                )
            )
            self._generate_token()

    def _on_eval_first_token(self) -> None:
        # Apos a janela de descoberta: a primeira maquina cria o token inicial,
        # mas somente se nenhuma atividade (token/dados) ja tiver sido observada.
        if (
            self.is_controller
            and not self.first_token_generated
            and not self.observed_activity
        ):
            log(
                "[{}] sou a primeira maquina (menor apelido) e nenhum token observado -> gerando token inicial".format(
                    self.apelido
                )
            )
            self._generate_token()
        elif self.is_controller:
            # Token ja existe ou existira; habilita a vigilancia.
            self.monitor.set_enabled(True)
