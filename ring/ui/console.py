"""Log thread-safe e laco de comandos interativo.

Varias threads (receptora, vigia do token, motor) imprimem ao mesmo tempo.
``log`` serializa as escritas com um lock para que as linhas nao se misturem.

``Console`` roda na thread principal, le comandos do teclado e os converte em
eventos postados no barramento do no. O Console NUNCA toca no estado do no: so
posta eventos; quem decide eh o motor (thread unica) do no.
"""

from __future__ import annotations

import sys
import threading

_print_lock = threading.Lock()


def log(msg) -> None:
    """Imprime ``msg`` de forma atomica entre threads.

    Substitui caracteres que o codepage local nao suporta por '?', prevenindo
    UnicodeEncodeError causado por bytes corrompidos (ex.: U+FFFD em cp1252)
    que derrubaria a thread do motor.
    """
    with _print_lock:
        if not isinstance(msg, str):
            msg = str(msg)
        try:
            print(msg)
        except UnicodeEncodeError:
            enc = getattr(sys.stdout, "encoding", "utf-8") or "utf-8"
            print(msg.encode(enc, errors="replace").decode(enc, errors="replace"))


_HELP = (
    "Comandos:\n"
    "  send <destino> <mensagem...>  envia mensagem (destino = apelido ou BROADCAST)\n"
    "  gentoken | addtoken           insere um token adicional na rede\n"
    "  removetoken | rmtoken         retira agora ou na proxima passagem\n"
    "  status                        mostra o estado atual do no\n"
    "  queue | fila                  lista a fila de mensagens\n"
    "  join | discover               reenvia DISCOVER (atualiza topologia)\n"
    "  help | ?                      mostra esta ajuda\n"
    "  quit | exit | sair            encerra"
)


class Console:
    """Laco de leitura de comandos do usuario, na thread principal."""

    def __init__(self, apelido, post) -> None:
        # post(event_type, **payload) coloca um evento no barramento do no.
        self.apelido = apelido
        self.post = post

    def run(self) -> None:
        """Le linhas e posta eventos ate receber quit/EOF."""
        log("No '{}' pronto. Digite 'help' para os comandos.".format(self.apelido))
        log(_HELP)
        while True:
            try:
                linha = input()
            except (EOFError, KeyboardInterrupt):
                self.post("CMD_QUIT")
                return

            linha = linha.strip()
            if not linha:
                continue

            partes = linha.split(maxsplit=2)
            cmd = partes[0].lower()

            if cmd == "send":
                # send <destino> <mensagem...> : destino e mensagem sao obrigatorios.
                if len(partes) < 3:
                    log("uso: send <destino> <mensagem...>")
                    continue
                self.post("CMD_SEND", destino=partes[1], message=partes[2])
            elif cmd in ("gentoken", "addtoken"):
                self.post("CMD_ADD_TOKEN")
            elif cmd in ("removetoken", "rmtoken"):
                self.post("CMD_REMOVE_TOKEN")
            elif cmd == "status":
                self.post("CMD_STATUS")
            elif cmd in ("queue", "fila"):
                self.post("CMD_QUEUE")
            elif cmd in ("join", "discover"):
                self.post("CMD_JOIN")
            elif cmd in ("help", "?"):
                log(_HELP)
            elif cmd in ("quit", "exit", "sair"):
                self.post("CMD_QUIT")
                return
            else:
                log("comando desconhecido: {!r} (digite 'help')".format(cmd))
