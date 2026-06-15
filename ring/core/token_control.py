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
        self._paused = False   # controladora segura o token aguardando o retorno dos dados
        self._enabled = False  # so vale apos existir token e enquanto for controladora
        self._stop = False
        self._thread = None

    def note_activity(self) -> None:
        """Marca atividade do anel (todo RX/envio/encaminhamento de token e dados)."""
        self._last_activity = time.monotonic()

    def set_paused(self, flag) -> None:
        """Pausa: enquanto a controladora espera seus dados voltarem, nao disparar."""
        self._paused = bool(flag)

    def set_enabled(self, flag) -> None:
        """Habilita a vigilancia (so quando um token ja existe e este no controla)."""
        self._enabled = bool(flag)

    def start(self) -> None:
        """Sobe a thread daemon de vigilancia."""
        self._thread = threading.Thread(target=self._loop, name="token-monitor", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Encerra a thread de vigilancia."""
        self._stop = True

    def _loop(self) -> None:
        # Acorda periodicamente; so age quando habilitado, nao pausado e o silencio
        # ultrapassou o timeout. Apos disparar uma vez, reseta o relogio para
        # esperar outro intervalo completo antes de um novo disparo.
        while not self._stop:
            time.sleep(0.1)
            if not self._enabled or self._paused:
                continue
            if (time.monotonic() - self._last_activity) > self.token_timeout:
                self.on_timeout()
                self.note_activity()
