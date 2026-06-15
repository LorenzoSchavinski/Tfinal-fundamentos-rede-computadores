"""Demonstration driver: TOKEN DUPLICADO detection.

Timeline:
  - launch A(6001), B(6002), C(6003) with config_ctrl_X.txt (min_token_interval=2s)
  - wait ~10s warmup: ring forms, A generates token, it circulates
  - check no TOKEN DUPLICADO appeared yet (healthy single token)
  - inject gentoken on B -> two tokens circulate, gap at A is ~1.5s < 2s -> triggers detection
  - if not detected, retry with gentoken on C
  - send status, quit; report results
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
    p = procs[name]
    try:
        p.stdin.write(cmd + "\n")
        p.stdin.flush()
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


def inter_arrival_gaps(name, fragment):
    """Return list of (ts, gap_from_prev) for all matching lines."""
    times = [ts for ts, ln in collect(name) if fragment in ln]
    gaps = []
    for i, t in enumerate(times):
        gap = (t - times[i - 1]) if i > 0 else None
        gaps.append((t, gap))
    return gaps


def launch_nodes():
    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"

    for name, _cfg, _port in NODES:
        log_lines[name] = []
        log_locks[name] = threading.Lock()

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
        log_path = os.path.join(TESTS, "log_dup_{}.txt".format(name))
        t = threading.Thread(target=_reader, args=(name, p, log_path), daemon=True)
        t.start()
        print("[driver] no {} iniciado (pid={})".format(name, p.pid))
        time.sleep(0.5)


def shutdown_nodes():
    for name, _, _ in NODES:
        send(name, "quit")
    time.sleep(2)
    for name, _, _ in NODES:
        p = procs[name]
        if p.poll() is None:
            p.terminate()
            p.wait(timeout=3)


def attempt_injection(inject_on, label):
    """Inject a second token on `inject_on`, wait 10s, return (dup_line, ring_alive_after)."""
    t_inject = time.monotonic()
    print("[driver] injetando gentoken em {} ({})".format(inject_on, label))
    send(inject_on, "gentoken")
    time.sleep(10)
    dup_line = find_line_after("A", "TOKEN DUPLICADO", t_inject)
    ring_alive = (
        find_line_after("A", "recebeu o token", t_inject)
        or find_line_after("B", "recebeu o token", t_inject)
        or find_line_after("C", "recebeu o token", t_inject)
    )
    return dup_line, ring_alive, t_inject


def main():
    launch_nodes()

    # Phase 1: warmup - ring forms, single token circulates
    print("[driver] aguardando formacao do anel + circulacao saudavel (10s)...")
    t_warmup_start = time.monotonic()
    time.sleep(10)
    t_warmup_end = time.monotonic()

    # Check: no TOKEN DUPLICADO before injection
    dup_before = find_line_after("A", "TOKEN DUPLICADO", t_warmup_start)
    token_circulating = (
        find_line("A", "recebeu o token")
        or find_line("B", "recebeu o token")
        or find_line("C", "recebeu o token")
    )

    # Debug: show inter-arrival gaps observed at A so far
    gaps = inter_arrival_gaps("A", "recebeu o token")
    print("[driver] gaps de chegada do token em A durante warmup:")
    for ts, gap in gaps:
        if gap is not None:
            print("  t={:.2f}s  gap={:.3f}s".format(ts, gap))

    # Phase 2: inject second token on B (attempt 1)
    dup_line, ring_alive, t_inject1 = attempt_injection("B", "tentativa 1")

    # If not detected, retry on C (different phase offset)
    if not dup_line:
        print("[driver] TOKEN DUPLICADO nao detectado com B; aguardando 5s e tentando em C...")
        time.sleep(5)
        dup_line, ring_alive, _t2 = attempt_injection("C", "tentativa 2")

    # If still not detected, show inter-arrival timing for diagnosis
    if not dup_line:
        print("[driver] AVISO: TOKEN DUPLICADO nao disparou em nenhuma das tentativas.")
        print("[driver] inter-arrival gaps em A (todas as chegadas):")
        all_gaps = inter_arrival_gaps("A", "recebeu o token")
        for ts, gap in all_gaps:
            if gap is not None:
                print("  t={:.2f}s  gap={:.3f}s".format(ts, gap))

    send("A", "status")
    send("B", "status")
    send("C", "status")
    time.sleep(2)

    shutdown_nodes()

    # Print context around TOKEN DUPLICADO in A's log
    print("\n" + "=" * 60)
    print("CONTEXTO log_dup_A.txt em torno do TOKEN DUPLICADO")
    print("=" * 60)
    all_lines_A = collect("A")
    for i, (ts, ln) in enumerate(all_lines_A):
        if "TOKEN DUPLICADO" in ln:
            start = max(0, i - 7)
            end = min(len(all_lines_A), i + 8)
            for j in range(start, end):
                marker = ">>>" if j == i else "   "
                print("{} {}".format(marker, all_lines_A[j][1].rstrip()))
            break

    # Verification report
    print("\n" + "=" * 60)
    print("VERIFICACAO")
    print("=" * 60)

    checks = []

    def chk(label, ok, evidence=None):
        status = "PASS" if ok else "FAIL"
        checks.append((status, label, evidence))

    # 1. No TOKEN DUPLICADO before injection
    chk(
        "sem TOKEN DUPLICADO antes da injecao (sem falso positivo)",
        dup_before is None,
        "(nenhum)" if dup_before is None else dup_before,
    )

    # 2. Single token was circulating during warmup
    chk(
        "token circulando durante warmup",
        token_circulating is not None,
        token_circulating,
    )

    # 3. TOKEN DUPLICADO fired after injection
    chk(
        "TOKEN DUPLICADO detectado apos injecao",
        dup_line is not None,
        dup_line,
    )

    # 4. Ring survived (tokens still circulating after the event)
    chk(
        "anel sobrevive (tokens circulando apos evento)",
        ring_alive is not None,
        ring_alive,
    )

    # 5. No tracebacks anywhere
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
        ev = str(evidence or "(nao encontrado)")[:55]
        print("{:<6} {:<52} {}".format(status, label, ev))

    print("\n=== RESULTADO FINAL: {} ===".format("PASS" if all_pass else "FAIL"))


if __name__ == "__main__":
    main()
