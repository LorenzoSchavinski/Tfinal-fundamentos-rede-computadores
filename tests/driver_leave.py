"""Driver STEP 2: graceful-leave heal.

Timeline:
  1) Launch A(6001), B(6002), C(6003); sleep 10 -- ring A->B->C->A forms, token circulates.
  2) Send ONLY B the command 'quit'; keep A and C running; sleep 12.
  3) Send A 'status'; send C 'status'; sleep 2.
  4) Send A and C 'quit'; terminate leftovers.

PASS criteria:
  - A and/or C log "B saiu da rede -> recalculando anel".
  - After B left, ring heals: "[A] enviando token para C" AND "[C] enviando token para A"
    appear AFTER B's departure (proves successor recomputed, token keeps moving).
  - Final status of A and C shows anel ['A','C'] and correct successor.
  - No freeze, no traceback.
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
log_files = {}


def _reader(name, proc, log_path):
    f = open(log_path, "w", encoding="utf-8")
    log_files[name] = f
    try:
        for line in proc.stdout:
            with log_locks[name]:
                log_lines[name].append((time.monotonic(), line))
                f.write(line)
                f.flush()
    except Exception:
        pass
    finally:
        f.close()


def send(name, cmd):
    p = procs.get(name)
    if p is None or p.poll() is not None:
        print("[driver] aviso: processo {} nao disponivel".format(name))
        return
    try:
        p.stdin.write(cmd + "\n")
        p.stdin.flush()
        print("[driver] -> {}: {}".format(name, cmd))
    except BrokenPipeError:
        print("[driver] aviso: pipe quebrado para {}".format(name))


def collect(name):
    with log_locks[name]:
        return list(log_lines[name])


def find_line(name, fragment):
    for _ts, ln in collect(name):
        if fragment in ln:
            return ln.rstrip()
    return None


def find_line_after(name, fragment, after_ts):
    for ts, ln in collect(name):
        if ts >= after_ts and fragment in ln:
            return ln.rstrip()
    return None


def main():
    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"

    for name, _cfg, _port in NODES:
        log_lines[name] = []
        log_locks[name] = threading.Lock()

    # Phase 1: launch all 3 nodes with 0.5s stagger
    for name, cfg, port in NODES:
        cmd = [
            PYTHON, "-u", "main.py",
            cfg,
            "--peers", "tests/peers.txt",
            "--port", str(port),
            "--ip", "127.0.0.1",
            "--discovery", "6",
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
        log_path = os.path.join(TESTS, "log_leave_{}.txt".format(name))
        t = threading.Thread(target=_reader, args=(name, p, log_path), daemon=True)
        t.start()
        print("[driver] no {} iniciado pid={}".format(name, p.pid))
        time.sleep(0.5)

    print("[driver] aguardando formacao do anel (10s)...")
    time.sleep(10)

    # Phase 2: only B quits; record the timestamp of departure
    t_b_quit = time.monotonic()
    print("[driver] enviando quit para B; A e C continuam")
    send("B", "quit")
    print("[driver] aguardando 12s para o anel sarar...")
    time.sleep(12)

    # Phase 3: status on A and C
    send("A", "status")
    send("C", "status")
    time.sleep(2)

    # Phase 4: quit A and C; force-terminate leftovers
    send("A", "quit")
    send("C", "quit")
    time.sleep(2)
    for name, _, _ in NODES:
        p = procs[name]
        if p.poll() is None:
            p.terminate()
            try:
                p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p.kill()

    # --- Verification ---
    print("\n" + "=" * 60)
    print("VERIFICACAO")
    print("=" * 60)

    checks = []

    def chk(label, ok, evidence=None):
        status = "PASS" if ok else "FAIL"
        checks.append((status, label, evidence))

    # 1. A or C logged the LEAVE reception
    leave_on_A = find_line("A", "B saiu da rede -> recalculando anel")
    leave_on_C = find_line("C", "B saiu da rede -> recalculando anel")
    chk(
        "A ou C logow 'B saiu da rede'",
        leave_on_A is not None or leave_on_C is not None,
        leave_on_A or leave_on_C,
    )

    # 2. After B left: A sends token to C
    a_to_c = find_line_after("A", "enviando token para C", t_b_quit)
    chk(
        "A enviou token para C apos saida de B",
        a_to_c is not None,
        a_to_c,
    )

    # 3. After B left: C sends token to A
    c_to_a = find_line_after("C", "enviando token para A", t_b_quit)
    chk(
        "C enviou token para A apos saida de B",
        c_to_a is not None,
        c_to_a,
    )

    # 4. A's status shows ring as ['A','C']
    ring_in_a_status = find_line_after("A", "anel: ['A', 'C']", t_b_quit)
    chk(
        "status de A mostra anel ['A','C']",
        ring_in_a_status is not None,
        ring_in_a_status,
    )

    # 5. C's status shows ring as ['A','C']
    ring_in_c_status = find_line_after("C", "anel: ['A', 'C']", t_b_quit)
    chk(
        "status de C mostra anel ['A','C']",
        ring_in_c_status is not None,
        ring_in_c_status,
    )

    # 6. No freeze: at least N token events appeared on A after B's departure
    token_events_a_after = sum(
        1 for ts, ln in collect("A")
        if ts >= t_b_quit and ("recebeu o token" in ln or "enviando token" in ln)
    )
    chk(
        "sem freeze: token events em A apos saida de B (>=3)",
        token_events_a_after >= 3,
        "token_events_A_after={}".format(token_events_a_after),
    )

    # 7. No tracebacks
    tb_found = None
    for name, _, _ in NODES:
        tb = find_line(name, "Traceback")
        if tb:
            tb_found = "{}: {}".format(name, tb)
            break
    chk("sem tracebacks", tb_found is None, tb_found or "OK")

    print("\n{:<6} {:<52} {}".format("STATUS", "VERIFICACAO", "EVIDENCIA"))
    print("-" * 120)
    all_pass = True
    for status, label, evidence in checks:
        if status == "FAIL":
            all_pass = False
        ev = str(evidence or "(nao encontrado)")[:60]
        print("{:<6} {:<52} {}".format(status, label, ev))

    print("\n=== RESULTADO FINAL: {} ===".format("PASS" if all_pass else "FAIL"))


if __name__ == "__main__":
    main()
