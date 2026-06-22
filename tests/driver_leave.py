"""Driver: saida real de um sucessor e reconstrucao por DISCOVER/HELLO.

Cenario:
  1. Forma A->B->C->A.
  2. Encerra somente B, sem reiniciar ou alterar A/C.
  3. A e C deixam de receber DISCOVER/HELLO de B e o expiram.
  4. A controladora recupera o token perdido ja usando A->C->A.
  5. A envia uma mensagem para C e recebe ACK.

Este teste nao fabrica topologias divergentes: todos os sobreviventes partem da
mesma visao e convergem pela mesma regra de expiracao.
"""

from __future__ import annotations

import os
import subprocess
import sys
import threading
import time

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TESTS = os.path.join(BASE, "tests")
PYTHON = sys.executable

NODES = [
    ("A", "tests/config_leave_A.txt", 6001),
    ("B", "tests/config_leave_B.txt", 6002),
    ("C", "tests/config_leave_C.txt", 6003),
]

procs = {}
log_lines = {}
log_locks = {}


def _reader(name, proc, log_path):
    with open(log_path, "w", encoding="utf-8") as f:
        try:
            for line in proc.stdout:
                with log_locks[name]:
                    log_lines[name].append((time.monotonic(), line))
                f.write(line)
                f.flush()
        except Exception:
            pass


def _launch(name, cfg, port):
    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"
    cmd = [
        PYTHON,
        "-u",
        "main.py",
        cfg,
        "--peers",
        "tests/peers.txt",
        "--port",
        str(port),
        "--ip",
        "127.0.0.1",
        "--discovery",
        "6",
    ]
    p = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        cwd=BASE,
        env=env,
    )
    procs[name] = p
    t = threading.Thread(
        target=_reader,
        args=(name, p, os.path.join(TESTS, "log_leave_{}.txt".format(name))),
        daemon=True,
    )
    t.start()
    print("[driver] no {} iniciado pid={}".format(name, p.pid))


def _kill(name):
    p = procs.get(name)
    if p is None or p.poll() is not None:
        return
    p.terminate()
    try:
        p.wait(timeout=4)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait(timeout=2)


def send(name, cmd):
    p = procs.get(name)
    if p is None or p.poll() is not None:
        return
    try:
        p.stdin.write(cmd + "\n")
        p.stdin.flush()
        print("[driver] -> {}: {}".format(name, cmd))
    except BrokenPipeError:
        pass


def collect(name):
    with log_locks[name]:
        return list(log_lines[name])


def find_line_after(name, fragment, after_ts):
    for ts, line in collect(name):
        if ts >= after_ts and fragment in line:
            return line.rstrip()
    return None


def main():
    for name, _cfg, _port in NODES:
        log_lines[name] = []
        log_locks[name] = threading.Lock()

    try:
        print("[driver] formando anel A->B->C->A...")
        for name, cfg, port in NODES:
            _launch(name, cfg, port)
            time.sleep(0.5)
        time.sleep(10)

        t_leave = time.monotonic()
        print("[driver] encerrando somente B...")
        send("B", "quit")
        time.sleep(1)
        _kill("B")

        # member_timeout=6s e token_timeout=8s nestes configs.
        print("[driver] aguardando expiracao de B e recuperacao do token (12s)...")
        time.sleep(12)

        t_send = time.monotonic()
        send("A", "send C apos-saida")
        time.sleep(6)
        send("A", "status")
        send("C", "status")
        time.sleep(2)
    finally:
        send("A", "quit")
        send("C", "quit")
        time.sleep(1)
        for name, _cfg, _port in NODES:
            _kill(name)

    checks = []

    def chk(label, evidence):
        checks.append(("PASS" if evidence else "FAIL", label, evidence))

    chk("A expirou B", find_line_after("A", "removendo B do anel", t_leave))
    chk("C expirou B", find_line_after("C", "removendo B do anel", t_leave))
    chk("A convergiu para ['A','C']", find_line_after("A", "anel: ['A', 'C']", t_leave))
    chk("C convergiu para ['A','C']", find_line_after("C", "anel: ['A', 'C']", t_leave))
    chk("A detectou token perdido", find_line_after("A", "TOKEN PERDIDO", t_leave))
    chk(
        "C recebeu mensagem apos saida",
        find_line_after("C", 'DADOS de A: "apos-saida" -> ACK', t_send),
    )
    chk(
        "A recebeu ACK apos saida",
        find_line_after("A", "mensagem entregue com sucesso (ACK)", t_send),
    )

    traceback = None
    for name, _cfg, _port in NODES:
        for _ts, line in collect(name):
            if "Traceback" in line:
                traceback = "{}: {}".format(name, line.rstrip())
                break
    chk("sem tracebacks", "OK" if traceback is None else None)

    print("\n{:<6} {:<42} {}".format("STATUS", "VERIFICACAO", "EVIDENCIA"))
    print("-" * 110)
    all_pass = True
    for status, label, evidence in checks:
        if status == "FAIL":
            all_pass = False
        print(
            "{:<6} {:<42} {}".format(
                status, label, str(evidence or "(nao encontrado)")[:55]
            )
        )
    print("\n=== RESULTADO FINAL: {} ===".format("PASS" if all_pass else "FAIL"))


if __name__ == "__main__":
    main()
