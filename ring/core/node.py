"""Motor do no do anel: maquina de estados orientada a eventos.

O estado do protocolo pertence a uma unica thread (o motor), que consome eventos
de um ``queue.Queue``. Receptor UDP, console e timers apenas postam eventos. O
``TokenMonitor`` mantem somente seu proprio relogio, protegido internamente por
lock, e avisa o motor quando detecta timeout.

Token e dados sempre vao para o endereco do SUCESSOR. Intermediarios repassam os
bytes do DATA VERBATIM; somente o destino enderecado edita o campo de controle
(via set_controle, preservando crc e mensagem).
"""

from __future__ import annotations

import queue
import threading
import time
import traceback

from ring.protocol import crc as crc_mod
from ring.protocol.packets import (
    BROADCAST,
    CTRL_ACK,
    CTRL_NAK,
    CTRL_NONE,
    build_discover,
    build_hello,
    parse,
    set_controle,
)
from ring.core.commands import CommandsMixin
from ring.core.message_queue import MessageQueue
from ring.core.token_control import TokenMonitor
from ring.core.token_logic import TokenLogicMixin
from ring.network.discovery import Ring
from ring.network.transport import Transport
from ring.ui.console import log


class Node(TokenLogicMixin, CommandsMixin):
    """No do anel: estado + maquina de estados dirigida pelo barramento.

    Herda de TokenLogicMixin (ciclo do token e emissao de dados) e CommandsMixin
    (tratadores de CMD_*). Todos os tratadores rodam na thread unica do motor,
    entao compartilham o estado do no sem locks.
    """

    def __init__(self, config, mode, bind_ip, bind_port, advertise_ip, peers) -> None:
        # peers: dict apelido -> (ip, porta) no modo local (inclui a si mesmo);
        #        None no modo lan (enderecos vem dos pacotes na porta fixa 6000).
        self.config = config
        self.mode = mode
        self.peers = peers
        self.apelido = config.apelido
        self.advertise_ip = advertise_ip
        self.port = bind_port

        self.bus = queue.Queue()
        self.queue = MessageQueue()
        self.ring = Ring()

        alvos = []
        if mode == "local" and peers:
            alvos = [addr for ap, addr in peers.items() if ap != self.apelido]
        self.transport = Transport(bind_ip, bind_port, mode, alvos, self.on_datagram)
        self.monitor = TokenMonitor(
            config.token_timeout, on_timeout=lambda: self.post("MON_TOKEN_TIMEOUT")
        )

        # Estado do token e da controladora (alterado apenas pelo motor).
        self.is_controller = False
        self.has_token = False
        self.first_token_generated = False
        self.observed_activity = False
        self.epoch = 0
        self.last_token_rx = None  # monotonic do ultimo token aceito (controladora)
        self.expect_token_return = False
        self.tokens_perdidos = 0  # contador de tokens perdidos detectados (PDF)
        self.tokens_duplicados = 0  # contador de tokens duplicados detectados (PDF)
        self.remove_token_pending = False

        # Estado da unica transmissao DATA que pode estar ativa neste no.
        self.waiting_for_data_return = False
        self.data_attempt_seq = 0
        self.active_data_attempt = None
        self.active_data_fingerprint = None

        # Presenca dos membros: DISCOVER/HELLO atualizam o instante last_seen.
        self.member_last_seen = {}
        self.last_data_activity = 0.0
        self._segurando_sozinho = False

        # Ciclo de vida das threads e timers recorrentes.
        self._engine = None
        self._first_token_timer = None
        self._discovery_timer = None
        self._discovery_deferred = False
        self._stopping = False

    # ------------------------------------------------------------------ bus
    def post(self, etype, **kw) -> None:
        """Coloca um evento (tipo, payload) no barramento do no."""
        if self._stopping:
            return
        self.bus.put((etype, kw))

    def _start_timer(self, delay, etype, **kw):
        """Agenda um evento daemon; o callback nunca altera o estado diretamente."""
        timer = threading.Timer(delay, lambda: self.post(etype, **kw))
        timer.daemon = True
        timer.start()
        return timer

    # -------------------------------------------------------- enderecamento
    def _addr_for(self, apelido, payload_ip):
        """Resolve o endereco de um membro conforme o modo.

        lan:   usa o ip anunciado no pacote e a porta fixa do anel (6000).
        local: ignora o ip do pacote e usa o mapa estatico de peers; ignora
               membros que nao estejam no mapa.
        """
        if self.mode == "lan":
            return (payload_ip, 6000)
        return self.peers.get(apelido) if self.peers else None

    def on_datagram(self, data, addr) -> None:
        """Callback da thread receptora: classifica e posta evento de RX."""
        p = parse(data)
        tipo = p.get("type")
        if tipo == "DISCOVER" or tipo == "HELLO":
            ap = p["apelido"]
            # Supressao de eco proprio: ignora o nosso anuncio que volta no broadcast.
            if ap == self.apelido:
                return
            end = self._addr_for(ap, p["ip"])
            if end is None:
                return
            if tipo == "DISCOVER":
                self.post("RX_DISCOVER", apelido=ap, ip=end[0], port=end[1])
            else:
                self.post("RX_HELLO", apelido=ap, ip=end[0], port=end[1])
        elif tipo == "TOKEN":
            # Reseta o relogio de atividade do monitor imediatamente (thread receptora),
            # antes mesmo de o motor processar o evento. Garante que token circulando
            # entre outros nos (controladora como intermediaria) nao dispare falso timeout.
            self.monitor.note_activity()
            self.post("RX_TOKEN")
        elif tipo == "DATA":
            # Idem para pacotes DATA: qualquer atividade no anel prova que o token existe.
            log(
                "[wire RX <- {}:{}] {}".format(
                    addr[0], addr[1], p["raw"].decode("utf-8", errors="replace")
                )
            )
            self.monitor.note_activity()
            self.post("RX_DATA", parsed=p, raw=p["raw"])
        else:
            # UNKNOWN/nao parseavel: registra cru para disputa de interop e ignora.
            log(
                "[wire RX <- {}:{}] BAD/unknown: {}".format(
                    addr[0], addr[1], p["raw"].decode("utf-8", errors="replace")
                )
            )

    # ----------------------------------------------------------- ciclo de vida
    def start(self) -> None:
        """Sobe transporte, registra a si mesmo, anuncia-se e inicia o motor."""
        self.transport.start()
        self.ring.update(self.apelido, self.advertise_ip, self.port)
        self.member_last_seen[self.apelido] = time.monotonic()
        self._recompute_role()
        self._engine = threading.Thread(
            target=self._engine_loop, name="engine", daemon=True
        )
        self._engine.start()
        # DISCOVER repetido ao longo da janela: em partidas escalonadas o primeiro
        # broadcast pode se perder (socket do par ainda nao escutando, UDP sem
        # retransmissao). Reenviar varias vezes deixa os nos se descobrirem mesmo
        # com perdas iniciais. DISCOVER eh pacote da spec e o par so responde HELLO
        # (idempotente, dedup por apelido), entao permanece interop-safe.
        self._broadcast_discover_repeated()
        # Janela de descoberta: depois dela, avalia se deve criar o token inicial.
        # O timer eh (re)agendado a cada novo membro (_schedule_first_token_eval), de
        # modo que a avaliacao so dispara apos a membership ficar estavel.
        self._schedule_first_token_eval()
        # Depois da formacao inicial, DISCOVER/HELLO passam a servir tambem como
        # prova periodica de presenca, sem criar nenhum tipo novo de pacote.
        self._schedule_discovery_tick(self.discovery_window)
        # Monitor sobe desabilitado; so liga quando for controladora com token.
        self.monitor.start()

    discovery_window = 6.0  # sobrescrito por main antes de start(), se desejado
    discover_interval = 0.5  # intervalo entre reenvios de DISCOVER na janela
    presence_interval = 2.0
    member_timeout = 6.0
    data_quiet_period = 0.75

    def _broadcast_discover_repeated(self) -> None:
        """Reemite DISCOVER a cada ``discover_interval`` ate fechar a janela.

        O primeiro envio eh imediato; os demais sao agendados por timers que se
        encadeiam ate o tempo decorrido alcancar ``discovery_window``.
        """
        pacote = build_discover(self.apelido, self.advertise_ip)

        def envia(restante: float) -> None:
            if self._stopping:
                return
            self.transport.broadcast(pacote)
            if restante > self.discover_interval:
                timer = threading.Timer(
                    self.discover_interval,
                    envia,
                    args=(restante - self.discover_interval,),
                )
                timer.daemon = True
                timer.start()

        envia(self.discovery_window)

    def _schedule_first_token_eval(self) -> None:
        """(Re)agenda a avaliacao do token inicial para daqui a ``discovery_window``.

        Substitui qualquer timer pendente, adiando a decisao enquanto a membership
        ainda muda. So o ultimo timer agendado dispara EVAL_FIRST_TOKEN.
        """
        if self._first_token_timer is not None:
            self._first_token_timer.cancel()
        self._first_token_timer = self._start_timer(
            self.discovery_window, "EVAL_FIRST_TOKEN"
        )

    def _schedule_discovery_tick(self, delay=None) -> None:
        """Agenda a proxima rodada periodica de DISCOVER/expiracao."""
        if self._stopping:
            return
        if self._discovery_timer is not None:
            self._discovery_timer.cancel()
        self._discovery_timer = self._start_timer(
            self.presence_interval if delay is None else delay,
            "DISCOVERY_TICK",
        )

    def _engine_loop(self) -> None:
        # Thread unica dona do estado: consome o barramento e despacha.
        while True:
            etype, kw = self.bus.get()
            if etype == "CMD_QUIT":
                self._stopping = True
                break
            handler = self._handlers.get(etype)
            if handler is not None:
                try:
                    handler(self, **kw)
                except Exception:
                    log(
                        "[{}] ERRO inesperado no handler {}: {}".format(
                            self.apelido, etype, traceback.format_exc()
                        )
                    )
        self._shutdown()

    def _shutdown(self) -> None:
        # Encerramento limpo. NAO emitimos o pacote "30"/LEAVE: ele nao faz parte da
        # spec (10/20/1000/2000) e nao deve ir num anel compartilhado. A ausencia
        # passa a ser percebida pelas rodadas periodicas dos pacotes oficiais 10/20.
        if self._first_token_timer is not None:
            self._first_token_timer.cancel()
        if self._discovery_timer is not None:
            self._discovery_timer.cancel()
        self.monitor.stop()
        self.transport.close()

    def close(self) -> None:
        """Solicita encerramento do motor e aguarda o termino (inclusive _shutdown)."""
        self.post("CMD_QUIT")
        if self._engine is not None and self._engine.is_alive():
            self._engine.join(timeout=5)

    # --------------------------------------------------------------- helpers
    def _recompute_role(self) -> None:
        """Recalcula se este no eh controladora e ajusta a vigilancia do token.

        Nunca gera token aqui: a criacao do token inicial eh decidida so em
        EVAL_FIRST_TOKEN, protegida por observed_activity.
        """
        era_controladora = self.is_controller
        self.is_controller = self.ring.is_controller(self.apelido)
        if self.is_controller and not era_controladora:
            # Ao assumir o controle, comeca uma janela limpa. Isso evita usar um
            # timestamp antigo e da uma volta completa antes de declarar perda.
            self.last_token_rx = None
            self.expect_token_return = False
            self.monitor.note_activity()
        if self.is_controller:
            # Uma controladora que entrou tarde tem first_token_generated False, mas
            # se ja observou atividade (token/dados existem) tambem deve vigiar.
            self.monitor.set_enabled(
                self.first_token_generated or self.observed_activity
            )
        else:
            self.monitor.set_enabled(False)
        # O latch evita repetir o mesmo log a cada avaliacao.
        if not self._is_alone() and not self.has_token:
            self._segurando_sozinho = False

    def _send(self, target_apelido, target_addr, data) -> bool:
        """Envia ``data`` ao endereco do alvo (tipicamente o sucessor).

        Retorna True se o envio saiu, False se nao havia endereco ou o socket falhou.
        """
        if target_addr is None:
            log(
                "[{}] sem endereco para '{}', pacote descartado".format(
                    self.apelido, target_apelido
                )
            )
            return False
        return self.transport.send_addr(target_addr[0], target_addr[1], data)

    def _successor(self):
        """(apelido, (ip, porta)) do sucessor, ou (None, None) se indefinido."""
        suc = self.ring.successor(self.apelido)
        if suc is None:
            return (None, None)
        ap, ip, port = suc
        return (ap, (ip, port))

    def _is_alone(self) -> bool:
        """True se este no eh a unica maquina do anel (sucessor eh ele mesmo ou indefinido)."""
        suc = self.ring.successor(self.apelido)
        return suc is None or suc[0] == self.apelido

    def _send_to_successor(self, data) -> bool:
        ap, addr = self._successor()
        return self._send(ap, addr, data)

    def _refresh_monitor_pause(self) -> None:
        """Pausa o monitor sempre que o token estiver comprovadamente aqui ou aguardando retorno.

        No-op em nos que nao sao controladora (monitor desabilitado). Deve ser
        chamado logo apos qualquer alteracao de has_token ou waiting_for_data_return.
        """
        self.monitor.set_paused(self.has_token or self.waiting_for_data_return)

    def _drop_head(self) -> None:
        """Remove o item da cabeca da fila (concluido)."""
        self.queue.pop()

    def _remove_member(self, apelido, motivo) -> bool:
        """Remove um membro conhecido e recompõe papel/sucessor localmente."""
        if apelido == self.apelido or apelido not in self.ring.members():
            return False
        self.ring.remove(apelido)
        self.member_last_seen.pop(apelido, None)
        log("[{}] removendo {} do anel ({})".format(self.apelido, apelido, motivo))
        self._recompute_role()
        if self.has_token and self._is_alone() and not self._segurando_sozinho:
            self._segurando_sozinho = True
            log(
                "[{}] sou a unica maquina no anel; segurando o token ate outra entrar".format(
                    self.apelido
                )
            )
        return True

    # ----------------------------------------------------------- handlers RX
    def _update_member(self, apelido, ip, port) -> str:
        """Atualiza o anel, avisa em caso de troca de endereco e recalcula papel."""
        self.member_last_seen[apelido] = time.monotonic()
        res = self.ring.update(apelido, ip, port)
        if res == "changed":
            log(
                "[{}] AVISO: '{}' mudou de endereco (rejuncao com novo IP ou colisao de apelido)".format(
                    self.apelido, apelido
                )
            )
        # Membro novo durante a fase de descoberta: adia a avaliacao do token
        # inicial por mais uma janela, ate a membership ficar quieta. So entao o
        # menor apelido conhecido equivale ao menor global e apenas ele gera.
        # Se um token ja existe (gerado ou observado), nao adia nada: late-join
        # nunca cria um segundo token (o guard de _on_eval_first_token continua).
        if (
            res == "new"
            and not self.first_token_generated
            and not self.observed_activity
        ):
            self._schedule_first_token_eval()
        self._recompute_role()
        # Estavamos sozinhos segurando o token e outra maquina acabou de entrar: retoma a circulacao.
        if (
            self.has_token
            and not self.waiting_for_data_return
            and not self._is_alone()
            and self._segurando_sozinho
        ):
            self._segurando_sozinho = False
            log(
                "[{}] nova maquina entrou; retomando a circulacao do token".format(
                    self.apelido
                )
            )
            if not self.queue.is_empty():
                self._send_data_packet(self.queue.peek())
            else:
                self._forward_token()
        return res

    def _on_rx_discover(self, apelido, ip, port) -> None:
        res = self._update_member(apelido, ip, port)
        # Responde com HELLO para se identificar a quem entrou.
        self.transport.broadcast(build_hello(self.apelido, self.advertise_ip))
        # DISCOVER periodico de membro conhecido eh idempotente e nao inunda o log.
        if res == "new":
            log(
                "[{}] DISCOVER de {} (entrou no anel), respondendo HELLO".format(
                    self.apelido, apelido
                )
            )
        elif res == "changed":
            log(
                "[{}] DISCOVER de {} com endereco atualizado, respondendo HELLO".format(
                    self.apelido, apelido
                )
            )

    def _on_rx_hello(self, apelido, ip, port) -> None:
        if self._update_member(apelido, ip, port) == "new":
            log("[{}] HELLO de {} (entrou no anel)".format(self.apelido, apelido))

    def _on_discovery_tick(self) -> None:
        """Revalida membros usando apenas DISCOVER/HELLO oficiais."""
        now = time.monotonic()
        self.member_last_seen[self.apelido] = now

        # O PDF permite alteracao topologica somente sem DATA circulando. Como
        # nao existe consulta global no protocolo, evitamos iniciar uma rodada
        # enquanto este no ainda ve uma transmissao recente.
        data_recente = (now - self.last_data_activity) < self.data_quiet_period
        if self.waiting_for_data_return or data_recente:
            self._discovery_deferred = True
            self._schedule_discovery_tick(self.presence_interval)
            return

        self.transport.broadcast(build_discover(self.apelido, self.advertise_ip))

        # Se rodadas foram adiadas por DATA, esta primeira rodada serve apenas
        # para dar aos membros vivos a oportunidade de atualizar last_seen.
        if self._discovery_deferred:
            self._discovery_deferred = False
            self._schedule_discovery_tick(self.presence_interval)
            return

        expirados = []
        for apelido in self.ring.members():
            if apelido == self.apelido:
                continue
            visto = self.member_last_seen.get(apelido, now)
            if (now - visto) >= self.member_timeout:
                expirados.append(apelido)

        for apelido in expirados:
            self._remove_member(
                apelido, "sem DISCOVER/HELLO ha {:.1f}s".format(self.member_timeout)
            )

        self._schedule_discovery_tick(self.presence_interval)

    def _on_rx_data(self, parsed, raw) -> None:
        o = parsed["origem"]
        d = parsed["destino"]
        ctrl = parsed["controle"]
        m = parsed["message"]
        self.observed_activity = True
        self.last_data_activity = time.monotonic()
        self.monitor.note_activity()

        if o == self.apelido:
            # Datagrama deu a volta e voltou a origem (eu). Trata BROADCAST antes.
            # Toda saida conhecida libera o token; _drop_head remove o item da fila.
            if not self.waiting_for_data_return:
                # Retorno tardio: o timeout ja desistiu deste DATA e liberou o token.
                # Nao mexe na fila nem repassa outro token (evitaria token duplicado).
                log("[{}] retorno tardio de dados ignorado".format(self.apelido))
                return
            retorno = (d, parsed["crc"], m)
            if retorno != self.active_data_fingerprint:
                # Um ACK/NAK antigo pode chegar durante uma tentativa posterior.
                # Sem alterar o protocolo, comparamos os campos que efetivamente
                # foram enviados para nao concluir a mensagem errada.
                log(
                    "[{}] retorno de dados antigo/diferente ignorado".format(
                        self.apelido
                    )
                )
                return
            if d == BROADCAST:
                log(
                    "[{}] BROADCAST concluido (deu a volta no anel)".format(
                        self.apelido
                    )
                )
                self._drop_head()
            elif ctrl == CTRL_ACK:
                log("[{}] mensagem entregue com sucesso (ACK)".format(self.apelido))
                self._drop_head()
            elif ctrl == CTRL_NONE:
                log(
                    "[{}] destino {} ausente/desligado (maquinainexistente)".format(
                        self.apelido, d
                    )
                )
                self._drop_head()
                # Sinal complementar de ausencia: se o DATA voltou sem ACK/NAK,
                # remove o destino sem esperar a expiracao periodica.
                if d != self.apelido and d != BROADCAST and d in self.ring.members():
                    self._remove_member(d, "maquinainexistente")
            elif ctrl == CTRL_NAK:
                head = self.queue.peek()
                if head is None:
                    # Cabeca ausente (fila inconsistente): libera o token sem travar.
                    log(
                        "[{}] NAK recebido mas fila vazia -> liberando token".format(
                            self.apelido
                        )
                    )
                elif not head.retransmit_used:
                    # Spec: retransmite exatamente uma vez, agora sem injetar falha.
                    head.retransmit_used = True
                    head.skip_fault_injection = True
                    log(
                        "[{}] RETRANSMISSAO: NAK recebido de {}, reenviando msg correta na proxima passagem do token".format(
                            self.apelido, d
                        )
                    )
                else:
                    log(
                        "[{}] NAK novamente apos retransmissao unica -> descartando msg (spec: retransmite apenas uma vez)".format(
                            self.apelido
                        )
                    )
                    self._drop_head()
            else:
                # Controle inesperado de volta a origem: loga, descarta e libera o token.
                log(
                    "[{}] controle inesperado '{}' de volta a origem -> descartando msg e liberando token".format(
                        self.apelido, ctrl
                    )
                )
                self._drop_head()
            self._complete_data_round()
            return

        # Nao sou a origem.
        if d == BROADCAST:
            log(
                '[{}] BROADCAST de {}: "{}"'.format(
                    self.apelido, o, m.decode("utf-8", errors="replace")
                )
            )
            self._send_to_successor(raw)  # repassa verbatim
            return

        if d == self.apelido:
            # Unicast enderecado a mim: confere CRC e marca ACK/NAK.
            ok = crc_mod.matches(m, parsed["crc"])
            new = CTRL_ACK if ok else CTRL_NAK
            if not ok:
                log(
                    "[{}] CRC NAO confere de {} (recebido={} calculado={}) -> NAK".format(
                        self.apelido, o, parsed["crc"], crc_mod.crc32(m)
                    )
                )
            log(
                '[{}] DADOS de {}: "{}" -> {}'.format(
                    self.apelido, o, m.decode("utf-8", errors="replace"), new
                )
            )
            self._send_to_successor(set_controle(raw, new))
            return

        # Intermediario: nem para mim nem broadcast -> repassa verbatim.
        log("[{}] repassando DADOS de {} para {}".format(self.apelido, o, d))
        self._send_to_successor(raw)


# Registro dos handlers fora do corpo da classe para manter o __init__ enxuto.
Node._handlers = {
    "RX_DISCOVER": Node._on_rx_discover,
    "RX_HELLO": Node._on_rx_hello,
    "RX_TOKEN": Node._on_rx_token,
    "RX_DATA": Node._on_rx_data,
    "TIMER_FORWARD_TOKEN": Node._on_timer_forward_token,
    "TIMER_DATA_RETURN_TIMEOUT": Node._on_timer_data_return_timeout,
    "CMD_SEND": Node._on_cmd_send,
    "CMD_ADD_TOKEN": Node._on_cmd_add_token,
    "CMD_REMOVE_TOKEN": Node._on_cmd_remove_token,
    "CMD_STATUS": Node._on_cmd_status,
    "CMD_QUEUE": Node._on_cmd_queue,
    "CMD_JOIN": Node._on_cmd_join,
    "MON_TOKEN_TIMEOUT": Node._on_mon_token_timeout,
    "EVAL_FIRST_TOKEN": Node._on_eval_first_token,
    "DISCOVERY_TICK": Node._on_discovery_tick,
}
