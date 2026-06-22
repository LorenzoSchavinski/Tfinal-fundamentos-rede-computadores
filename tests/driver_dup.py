"""Driver deterministico para deteccao de TOKEN DUPLICADO.

Depois do aquecimento, espera A (controladora) receber e segurar o token. Nesse
instante manda C, predecessor direto de A, injetar um 1000 adicional. O pacote
chega a A enquanto ela ainda possui o token legitimo, eliminando a dependencia
de uma coincidencia de intervalos entre voltas.
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
    ("A", "tests/config_ctrl_A.txt", 6001),
    ("B", "tests/config_ctrl_B.txt", 6002),
    ("C", "tests/config_ctrl_C.txt", 6003),
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


def send(name, cmd):
    p = procs[name]
    p.stdin.write(cmd + "\n")
    p.stdin.flush()
    print("[driver] -> {}: {}".format(name, cmd))


def collect(name):
    with log_locks[name]:
        return list(log_lines[name])


def find_line_after(name, fragment, after_ts):
    for ts, line in collect(name):
        if ts >= after_ts and fragment in line:
            return (ts, line.rstrip())
    return None


def wait_line_after(name, fragment, after_ts, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        found = find_line_after(name, fragment, after_ts)
        if found:
            return found
        time.sleep(0.02)
    return None


def main():
    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"
    for name, _cfg, _port in NODES:
        log_lines[name] = []
        log_locks[name] = threading.Lock()

    try:
        for name, cfg, port in NODES:
            p = subprocess.Popen(
                [
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
                ],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                cwd=BASE,
                env=env,
            )
            procs[name] = p
            threading.Thread(
                target=_reader,
                args=(name, p, os.path.join(TESTS, "log_dup_{}.txt".format(name))),
                daemon=True,
            ).start()
            time.sleep(0.5)

        print("[driver] aguardando anel saudavel (9s)...")
        time.sleep(9)
        dup_before = find_line_after("A", "TOKEN DUPLICADO", 0)

        marker = time.monotonic()
        held = wait_line_after("A", "recebeu o token", marker, 6)
        if held:
            # C eh predecessor direto de A no anel A->B->C->A.
            send("C", "gentoken")
        t_inject = time.monotonic()
        duplicate = wait_line_after("A", "TOKEN DUPLICADO", t_inject - 0.2, 4)
        alive = None
        if duplicate:
            alive = wait_line_after("A", "recebeu o token", duplicate[0], 6)

        send("A", "status")
        time.sleep(1)
    finally:
        for name, _cfg, _port in NODES:
            p = procs.get(name)
            if p and p.poll() is None:
                try:
                    send(name, "quit")
                except (BrokenPipeError, OSError):
                    pass
        time.sleep(1)
        for p in procs.values():
            if p.poll() is None:
                p.terminate()

    checks = [
        ("sem falso positivo antes da injecao", dup_before is None, dup_before),
        ("A estava segurando o token", held is not None, held),
        ("A detectou a duplicata", duplicate is not None, duplicate),
        ("token legitimo continuou circulando", alive is not None, alive),
    ]
    traceback = None
    for name, _cfg, _port in NODES:
        for _ts, line in collect(name):
            if "Traceback" in line:
                traceback = "{}: {}".format(name, line.rstrip())
                break
    checks.append(("sem tracebacks", traceback is None, traceback or "OK"))

    print("\n{:<6} {:<42} {}".format("STATUS", "VERIFICACAO", "EVIDENCIA"))
    print("-" * 110)
    all_pass = True
    for label, ok, evidence in checks:
        status = "PASS" if ok else "FAIL"
        all_pass = all_pass and ok
        print(
            "{:<6} {:<42} {}".format(
                status, label, str(evidence or "(nao encontrado)")[:55]
            )
        )
    print("\n=== RESULTADO FINAL: {} ===".format("PASS" if all_pass else "FAIL"))


if __name__ == "__main__":
    main()
