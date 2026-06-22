"""Vigia do token (apenas na controladora): deteccao de token PERDIDO.

Thread daemon que, enquanto este no for a controladora, monitora o tempo desde a
ultima atividade do anel. Se passar tempo demais sem ver token nem dados, conclui
que o token se perdeu e dispara um callback para o no gerar um novo.

A deteccao de token DUPLICADO (intervalo curto demais entre tokens) NAO eh feita
aqui: ela mora no no, no tratamento de RX_TOKEN, porque depende do papel atual e
do historico de chegada do token.
"""

from __future__ import annotations

import threading
import time


class TokenMonitor:
    """Detector de timeout do token, controlado pelo no dono do estado."""

    def __init__(self, token_timeout, on_timeout) -> None:
        # on_timeout: callback sem argumentos (o no posta MON_TOKEN_TIMEOUT).
        self.token_timeout = token_timeout
        self.on_timeout = on_timeout

        self._last_activity = time.monotonic()
        # A pausa cobre os periodos em que a controladora segura o token ou
        # aguarda o retorno de seu DATA; habilitado so vale para a controladora.
        self._paused = False
        self._enabled = False
        self._stop = False
        self._thread = None
        # RX, motor e esta thread acessam o relogio; o lock deixa essa pequena
        # excecao ao modelo de "estado no motor" explicita e portavel.
        self._lock = threading.Lock()

    def note_activity(self) -> None:
        """Marca atividade do anel (todo RX/envio/encaminhamento de token e dados)."""
        with self._lock:
            self._last_activity = time.monotonic()

    def set_paused(self, flag) -> None:
        """Pausa: enquanto a controladora espera seus dados voltarem, nao disparar."""
        with self._lock:
            self._paused = bool(flag)

    def set_enabled(self, flag) -> None:
        """Habilita a vigilancia (so quando um token ja existe e este no controla)."""
        with self._lock:
            self._enabled = bool(flag)

    def start(self) -> None:
        """Sobe a thread daemon de vigilancia."""
        self._thread = threading.Thread(
            target=self._loop, name="token-monitor", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        """Encerra a thread de vigilancia."""
        with self._lock:
            self._stop = True
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=0.3)

    def _loop(self) -> None:
        # Acorda periodicamente; so age quando habilitado, nao pausado e o silencio
        # ultrapassou o timeout. Apos disparar uma vez, reseta o relogio para
        # esperar outro intervalo completo antes de um novo disparo.
        while True:
            time.sleep(0.1)
            now = time.monotonic()
            with self._lock:
                if self._stop:
                    return
                deve_disparar = (
                    self._enabled
                    and not self._paused
                    and (now - self._last_activity) > self.token_timeout
                )
                if deve_disparar:
                    # Reinicia a janela antes do callback para nunca empilhar
                    # varios eventos de timeout no barramento.
                    self._last_activity = now
            if not deve_disparar:
                continue
            self.on_timeout()
