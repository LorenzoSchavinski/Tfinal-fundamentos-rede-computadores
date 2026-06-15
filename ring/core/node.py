"""Motor do no do anel: maquina de estados orientada a eventos.

O ``Node`` detem TODO o estado mutavel e eh dirigido por UMA unica thread (o
motor) que consome eventos de um ``queue.Queue`` (barramento). Threads externas
(receptora UDP, vigia do token, console, timers) apenas POSTAM eventos; nunca
mexem no estado. Como o barramento serializa tudo, nao ha locks no estado.

Token e dados sempre vao para o endereco do SUCESSOR. Intermediarios repassam os
bytes do DATA VERBATIM; somente o destino enderecado edita o campo de controle
(via set_controle, preservando crc e mensagem).
"""
from __future__ import annotations

import queue
import threading

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

        # --- Estado da maquina (so o motor escreve) ---
        self.is_controller = False
        self.has_token = False
        self.waiting_for_data_return = False
        self.first_token_generated = False
        self.observed_activity = False
        self.epoch = 0
        self.last_token_rx = None      # monotonic do ultimo token aceito (controladora)
        self.expect_token_return = False
        self.inflight = None           # QueueItem circulando no momento

        self._engine = None

    # ------------------------------------------------------------------ bus
    def post(self, etype, **kw) -> None:
        """Coloca um evento (tipo, payload) no barramento do no."""
        self.bus.put((etype, kw))

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
            self.monitor.note_activity()
            self.post("RX_DATA", parsed=p, raw=p["raw"])
        # UNKNOWN: ignorado.

    # ----------------------------------------------------------- ciclo de vida
    def start(self) -> None:
        """Sobe transporte, registra a si mesmo, anuncia-se e inicia o motor."""
        self.transport.start()
        self.ring.update(self.apelido, self.advertise_ip, self.port)
        self._recompute_role()
        self.transport.broadcast(build_discover(self.apelido, self.advertise_ip))
        self._engine = threading.Thread(target=self._engine_loop, name="engine", daemon=True)
        self._engine.start()
        # Janela de descoberta: depois dela, avalia se deve criar o token inicial.
        threading.Timer(self.discovery_window, lambda: self.post("EVAL_FIRST_TOKEN")).start()
        # Monitor sobe desabilitado; so liga quando for controladora com token.
        self.monitor.start()

    discovery_window = 3.0  # sobrescrito por main antes de start(), se desejado

    def _engine_loop(self) -> None:
        # Thread unica dona do estado: consome o barramento e despacha.
        while True:
            etype, kw = self.bus.get()
            if etype == "CMD_QUIT":
                break
            handler = self._handlers.get(etype)
            if handler is not None:
                handler(self, **kw)
        self._shutdown()

    def _shutdown(self) -> None:
        self.monitor.stop()
        self.transport.close()

    def close(self) -> None:
        """Solicita encerramento do motor a partir de outra thread."""
        self.post("CMD_QUIT")

    # --------------------------------------------------------------- helpers
    def _recompute_role(self) -> None:
        """Recalcula se este no eh controladora e ajusta a vigilancia do token.

        Nunca gera token aqui: a criacao do token inicial eh decidida so em
        EVAL_FIRST_TOKEN, protegida por observed_activity.
        """
        self.is_controller = self.ring.is_controller(self.apelido)
        if self.is_controller and self.first_token_generated:
            self.monitor.set_enabled(True)
        elif not self.is_controller:
            self.monitor.set_enabled(False)

    def _send(self, target_apelido, target_addr, data) -> None:
        """Envia ``data`` ao endereco do alvo (tipicamente o sucessor)."""
        if target_addr is None:
            log("[{}] sem endereco para '{}', pacote descartado".format(self.apelido, target_apelido))
            return
        self.transport.send_addr(target_addr[0], target_addr[1], data)

    def _successor(self):
        """(apelido, (ip, porta)) do sucessor, ou (None, None) se indefinido."""
        suc = self.ring.successor(self.apelido)
        if suc is None:
            return (None, None)
        ap, ip, port = suc
        return (ap, (ip, port))

    def _send_to_successor(self, data) -> None:
        ap, addr = self._successor()
        self._send(ap, addr, data)

    def _refresh_monitor_pause(self) -> None:
        """Pausa o monitor sempre que o token estiver comprovadamente aqui ou aguardando retorno.

        No-op em nos que nao sao controladora (monitor desabilitado). Deve ser
        chamado logo apos qualquer alteracao de has_token ou waiting_for_data_return.
        """
        self.monitor.set_paused(self.has_token or self.waiting_for_data_return)

    def _drop_head(self) -> None:
        """Remove o item da cabeca da fila (concluido) e limpa o inflight."""
        self.queue.pop()
        self.inflight = None

    # ----------------------------------------------------------- handlers RX
    def _update_member(self, apelido, ip, port) -> str:
        """Atualiza o anel, avisa em caso de troca de endereco e recalcula papel."""
        res = self.ring.update(apelido, ip, port)
        if res == "changed":
            log("[{}] AVISO: '{}' mudou de endereco (rejuncao com novo IP ou colisao de apelido)".format(self.apelido, apelido))
        self._recompute_role()
        return res

    def _on_rx_discover(self, apelido, ip, port) -> None:
        self._update_member(apelido, ip, port)
        # Responde com HELLO para se identificar a quem entrou.
        self.transport.broadcast(build_hello(self.apelido, self.advertise_ip))
        log("[{}] DISCOVER de {}, respondendo HELLO".format(self.apelido, apelido))

    def _on_rx_hello(self, apelido, ip, port) -> None:
        if self._update_member(apelido, ip, port) == "new":
            log("[{}] HELLO de {} (entrou no anel)".format(self.apelido, apelido))

    def _on_rx_data(self, parsed, raw) -> None:
        o = parsed["origem"]
        d = parsed["destino"]
        ctrl = parsed["controle"]
        m = parsed["message"]
        self.observed_activity = True
        self.monitor.note_activity()

        if o == self.apelido:
            # Datagrama deu a volta e voltou a origem (eu). Trata BROADCAST antes.
            # Toda saida conhecida libera o token; _drop_head remove o item da fila.
            release = True
            if d == BROADCAST:
                log("[{}] BROADCAST concluido (deu a volta no anel)".format(self.apelido))
                self._drop_head()
            elif ctrl == CTRL_ACK:
                log("[{}] mensagem entregue com sucesso (ACK)".format(self.apelido))
                self._drop_head()
            elif ctrl == CTRL_NONE:
                log("[{}] destino {} ausente/desligado (maquinainexistente)".format(self.apelido, d))
                self._drop_head()
            elif ctrl == CTRL_NAK:
                head = self.queue.peek()
                if head and not head.retransmit_used:
                    # Spec: retransmite exatamente uma vez, agora sem injetar falha.
                    head.retransmit_used = True
                    head.no_error = True
                    log("[{}] RETRANSMISSAO: NAK recebido de {}, reenviando msg correta na proxima passagem do token".format(self.apelido, d))
                else:
                    log("[{}] NAK novamente apos retransmissao unica -> descartando msg (spec: retransmite apenas uma vez)".format(self.apelido))
                    self._drop_head()
            else:
                # Controle inesperado de volta a origem: nao libera (mantem estado).
                release = False
            if release:
                self.waiting_for_data_return = False
                self._refresh_monitor_pause()
                self._forward_token()
            return

        # Nao sou a origem.
        if d == BROADCAST:
            log("[{}] BROADCAST de {}: \"{}\"".format(self.apelido, o, m.decode("utf-8", errors="replace")))
            self._send_to_successor(raw)  # repassa verbatim
            return

        if d == self.apelido:
            # Unicast enderecado a mim: confere CRC e marca ACK/NAK.
            ok = crc_mod.matches(m, parsed["crc"])
            new = CTRL_ACK if ok else CTRL_NAK
            if not ok:
                log("[{}] CRC NAO confere de {} (recebido={} calculado={}) -> NAK".format(
                    self.apelido, o, parsed["crc"], crc_mod.crc32(m)))
            log("[{}] DADOS de {}: \"{}\" -> {}".format(
                self.apelido, o, m.decode("utf-8", errors="replace"), new))
            self._send_to_successor(set_controle(raw, new))
            return

        # Intermediario: nem para mim nem broadcast -> repassa verbatim.
        log("[{}] repassando DADOS de {} para {}".format(self.apelido, o, d))
        self._send_to_successor(raw)

    # Tabela de despacho: tipo de evento -> metodo (preenchida abaixo da classe).
    _handlers = {}


# Registro dos handlers fora do corpo da classe para manter o __init__ enxuto.
Node._handlers = {
    "RX_DISCOVER": Node._on_rx_discover,
    "RX_HELLO": Node._on_rx_hello,
    "RX_TOKEN": Node._on_rx_token,
    "RX_DATA": Node._on_rx_data,
    "TIMER_FORWARD_TOKEN": Node._on_timer_forward_token,
    "CMD_SEND": Node._on_cmd_send,
    "CMD_ADD_TOKEN": Node._on_cmd_add_token,
    "CMD_REMOVE_TOKEN": Node._on_cmd_remove_token,
    "CMD_STATUS": Node._on_cmd_status,
    "CMD_QUEUE": Node._on_cmd_queue,
    "CMD_JOIN": Node._on_cmd_join,
    "MON_TOKEN_TIMEOUT": Node._on_mon_token_timeout,
    "EVAL_FIRST_TOKEN": Node._on_eval_first_token,
}
