"""Driver STEP 3: late-join controller monitor.

Validates the fix that makes a late-joining controller (with observed_activity=True)
enable its monitor so it can detect a lost token.

Timeline:
  1) Launch ONLY B(6002) and C(6003); sleep 10.
     B has smallest apelido among {B,C} -> controller -> generates first token.
     Ring B<->C circulates.
  2) Launch A(6001) late; sleep 8.
     A has smallest apelido -> becomes new controller. Ring A->B->C->A.
  3) Drop the only token: send 'removetoken' to A, then B, then C (0.5s apart); sleep 12.
     Timeout is 8s -> A's monitor should fire.
  4) Send A 'status'; sleep 2; quit all.

PASS criteria:
  - A becomes controller (status shows controladora: True for A).
  - A's monitor fires: A logs 'TOKEN PERDIDO detectado (timeout) -> gerando novo token'
    AFTER the token was removed. (KEY CHECK: late controller must monitor.)
  - After regeneration, the ring resumes circulating.
  - A status shows tokens_perdidos >= 1.
  - No traceback.
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

ALL_NODES = [
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
    log_path = os.path.join(TESTS, "log_latejoin_{}.txt".format(name))
    t = threading.Thread(target=_reader, args=(name, p, log_path), daemon=True)
    t.start()
    print("[driver] no {} iniciado pid={}".format(name, p.pid))


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
    for name, _cfg, _port in ALL_NODES:
        log_lines[name] = []
        log_locks[name] = threading.Lock()

    # Phase 1: launch B and C only
    print("[driver] lancando apenas B e C...")
    for name, cfg, port in ALL_NODES:
        if name in ("B", "C"):
            _launch(name, cfg, port)
            time.sleep(0.5)

    print("[driver] aguardando B+C formarem anel (10s)...")
    time.sleep(10)

    print("[driver] estado de B e C antes de A entrar:")
    for name in ("B", "C"):
        with log_locks[name]:
            ll = list(log_lines[name])
        for _ts, ln in ll[-5:]:
            print("  [{}] {}".format(name, ln), end="")

    # Phase 2: launch A late
    t_a_join = time.monotonic()
    print("[driver] lancando A (entrada tardia)...")
    _launch("A", "tests/config_leave_A.txt", 6001)
    print("[driver] aguardando A entrar no anel e tornar-se controladora (8s)...")
    time.sleep(8)

    print("[driver] estado apos A entrar:")
    for name in ("A", "B", "C"):
        with log_locks[name]:
            ll = list(log_lines[name])
        for ts, ln in ll:
            if ts >= t_a_join - 0.5:
                print("  [{}] {}".format(name, ln), end="")

    # Phase 3: drop the token everywhere
    t_remove = time.monotonic()
    print("[driver] removendo token em A, B, C (0.5s apart)...")
    send("A", "removetoken")
    time.sleep(0.5)
    send("B", "removetoken")
    time.sleep(0.5)
    send("C", "removetoken")
    print("[driver] aguardando 12s (timeout=8s -> monitor deve disparar em A)...")
    time.sleep(12)

    # Phase 4: status and quit
    send("A", "status")
    time.sleep(2)
    for name, _, _ in ALL_NODES:
        send(name, "quit")
    time.sleep(2)
    for name, _, _ in ALL_NODES:
        p = procs.get(name)
        if p and p.poll() is None:
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

    # 1. B was controller before A joined (B's apelido < C's)
    b_was_ctrl = find_line("B", "gerando token inicial") or find_line(
        "B", "gerou/inseriu"
    )
    chk(
        "B foi controladora inicial (gerou token)",
        b_was_ctrl is not None,
        b_was_ctrl,
    )

    # 2. A became controller after joining
    a_is_ctrl = find_line_after("A", "controladora: True", t_a_join)
    chk(
        "A tornou-se controladora apos entrar (status)",
        a_is_ctrl is not None,
        a_is_ctrl,
    )

    # 3. A's monitor fired after token removal (the KEY check)
    a_detected = find_line_after("A", "TOKEN PERDIDO detectado (timeout)", t_remove)
    chk(
        "A detectou TOKEN PERDIDO apos remocao (monitor ativo)",
        a_detected is not None,
        a_detected,
    )

    # 4. Ring resumed after regeneration (token events after the detection)
    t_regen = None
    for ts, ln in collect("A"):
        if ts >= t_remove and "TOKEN PERDIDO" in ln:
            t_regen = ts
            break
    ring_resumed = None
    if t_regen is not None:
        ring_resumed = (
            find_line_after("A", "recebeu o token", t_regen)
            or find_line_after("B", "recebeu o token", t_regen)
            or find_line_after("C", "recebeu o token", t_regen)
        )
    chk(
        "anel retomou circulacao apos regeneracao",
        ring_resumed is not None,
        ring_resumed,
    )

    # 5. A status shows tokens_perdidos >= 1
    perd_line = find_line_after("A", "tokens_perdidos:", t_remove)
    perd_ge1 = False
    if perd_line:
        try:
            val = int(perd_line.strip().split("tokens_perdidos:")[1].split()[0])
            perd_ge1 = val >= 1
        except Exception:
            pass
    chk(
        "A status tokens_perdidos >= 1",
        perd_ge1,
        perd_line,
    )

    # 6. No tracebacks
    tb_found = None
    for name, _, _ in ALL_NODES:
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
