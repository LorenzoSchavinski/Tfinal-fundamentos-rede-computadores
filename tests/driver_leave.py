"""Driver: reactive leave heal via maquinainexistente.

Topology trick that makes the reactive path work in UDP:
  Ring sorted alphabetically: A < C < D  ->  A->C->D->A
  A's successor = C (living).  D is C's successor (the node that will leave).

  When D dies and C is restarted fresh (C's ring heals to {A,C}):
    - A's ring still has {A, C, D} (D remains in A's members dict).
    - A sends DATA(destino=D) to C (A's successor = C, alive).
    - C's ring = {A, C}, C's successor = A.
    - C forwards the packet to A (d=D, not in C's path; C's next = A).
    - A receives the packet with ctrl still = maquinainexistente!
    - BOTH log lines fire: 'destino D ausente' AND 'removendo D do anel'.

Nodes:
  A (6001) -- controller, alive throughout
  C (6003) -- alive, killed and restarted fresh so its ring heals to {A,C}
  D (6004) -- the node that leaves; stays in A's ring.members after quit

Files: tests/peers_leave.txt (A,C,D), tests/config_leave_D.txt

Timeline:
  1. Launch A, C, D; wait 10s for ring {A,C,D} to form.
  2. Kill D; wait 3s for D to die.
  3. Kill C; wait 3s for port 6003 to free.
  4. Restart C fresh; C discovers only A (D dead); C's ring = {A,C}; wait 8s.
  5. A sends 'send D teste'; packet circuits A->C->A; maquinainexistente fires.
  6. Wait 5s; send status to A; quit all.

PASS criteria:
  - A logs 'destino D ausente/desligado (maquinainexistente)'.
  - A logs 'removendo D do anel (maquinainexistente)'.
  - A's status shows anel: ['A', 'C'].
  - Token events on A after heal (ring keeps circulating).
  - No tracebacks.
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

PEERS_FILE = "tests/peers_leave.txt"

NODES = [
    ("A", "tests/config_leave_A.txt", 6001),
    ("C", "tests/config_leave_C.txt", 6003),
    ("D", "tests/config_leave_D.txt", 6004),
]

procs = {}
log_lines = {}
log_locks = {}


def _reader(name, proc, log_path, mode="w"):
    with open(log_path, mode, encoding="utf-8") as f:
        try:
            for line in proc.stdout:
                with log_locks[name]:
                    log_lines[name].append((time.monotonic(), line))
                f.write(line)
                f.flush()
        except Exception:
            pass


def _launch(name, cfg, port, log_mode="w"):
    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"
    cmd = [
        PYTHON, "-u", "main.py",
        cfg,
        "--peers", PEERS_FILE,
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
    t = threading.Thread(
        target=_reader, args=(name, p, log_path, log_mode), daemon=True
    )
    t.start()
    print("[driver] no {} iniciado pid={}".format(name, p.pid))


def _kill(name):
    p = procs.get(name)
    if p is None:
        return
    if p.poll() is None:
        p.terminate()
        try:
            p.wait(timeout=4)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait(timeout=2)


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
    for name, _cfg, _port in NODES:
        log_lines[name] = []
        log_locks[name] = threading.Lock()

    # Phase 1: launch A, C, D
    print("[driver] lancando A, C, D (ring: A->C->D->A)...")
    for name, cfg, port in NODES:
        _launch(name, cfg, port, log_mode="w")
        time.sleep(0.5)

    print("[driver] aguardando formacao do anel A->C->D->A (10s)...")
    time.sleep(10)

    # Phase 2: kill D -- D stays in A's ring.members
    print("[driver] matando D (permanece nos members de A)...")
    send("D", "quit")
    time.sleep(1)
    _kill("D")
    print("[driver] D morto. aguardando 2s...")
    time.sleep(2)

    # Phase 3: kill C -- we need C to restart fresh so its ring heals to {A,C}
    print("[driver] matando C para reiniciar limpo...")
    send("C", "quit")
    time.sleep(1)
    _kill("C")
    print("[driver] C morto. aguardando 3s para porta 6003 liberar...")
    time.sleep(3)

    # Phase 4: restart C fresh
    # C sends DISCOVER; only A responds (D is dead); C's ring = {A, C}; C's successor = A
    print("[driver] reiniciando C (ring de C sera {A,C}, successor=A)...")
    _launch("C", "tests/config_leave_C.txt", 6003, log_mode="a")
    print("[driver] aguardando C redescobrir anel (8s)...")
    time.sleep(8)

    # Phase 5: A sends data to D
    # A's ring = {A, C, D}; A's successor = C (alive).
    # Data: A->C (A's successor). C receives, d=D, C is intermediate.
    # C's ring = {A, C}; C's successor = A. C forwards to A.
    # A gets packet back with ctrl = maquinainexistente. Both log lines fire.
    t_send = time.monotonic()
    print("[driver] A envia 'send D teste' (caminho reativo)...")
    send("A", "send D teste")
    print("[driver] aguardando circulacao e cura (8s)...")
    time.sleep(8)

    # Phase 6: status and quit
    send("A", "status")
    time.sleep(2)
    send("A", "quit")
    send("C", "quit")
    time.sleep(2)
    for name, _, _ in NODES:
        _kill(name)

    # --- Verification ---
    print("\n" + "=" * 60)
    print("VERIFICACAO")
    print("=" * 60)

    checks = []

    def chk(label, ok, evidence=None):
        status = "PASS" if ok else "FAIL"
        checks.append((status, label, evidence))

    # 1. A logs maquinainexistente detection for destino D
    absent_line = find_line_after("A", "destino D ausente/desligado (maquinainexistente)", t_send)
    chk(
        "A logou destino D ausente/desligado (maquinainexistente)",
        absent_line is not None,
        absent_line,
    )

    # 2. A logs removing D from ring
    remove_line = find_line_after("A", "removendo D do anel (maquinainexistente)", t_send)
    chk(
        "A logou removendo D do anel (maquinainexistente)",
        remove_line is not None,
        remove_line,
    )

    # 3. A's status shows ring as ['A', 'C'] after heal
    ring_line = find_line_after("A", "anel: ['A', 'C']", t_send)
    chk(
        "status de A mostra anel ['A','C'] apos cura",
        ring_line is not None,
        ring_line,
    )

    # 4. Ring keeps circulating after heal
    t_heal = t_send
    for ts, ln in collect("A"):
        if ts >= t_send and "removendo D do anel" in ln:
            t_heal = ts
            break
    token_events_after = sum(
        1 for ts, ln in collect("A")
        if ts >= t_heal and ("recebeu o token" in ln or "enviando token" in ln)
    )
    chk(
        "anel circula apos cura (token events em A >= 1)",
        token_events_after >= 1,
        "token_events_A_after_heal={}".format(token_events_after),
    )

    # 5. No tracebacks
    tb_found = None
    for name, _, _ in NODES:
        tb = find_line(name, "Traceback")
        if tb:
            tb_found = "{}: {}".format(name, tb)
            break
    chk("sem tracebacks", tb_found is None, tb_found or "OK")

    print("\n{:<6} {:<55} {}".format("STATUS", "VERIFICACAO", "EVIDENCIA"))
    print("-" * 120)
    all_pass = True
    for status, label, evidence in checks:
        if status == "FAIL":
            all_pass = False
        ev = str(evidence or "(nao encontrado)")[:55]
        print("{:<6} {:<55} {}".format(status, label, ev))

    print("\n=== RESULTADO FINAL: {} ===".format("PASS" if all_pass else "FAIL"))


if __name__ == "__main__":
    main()
