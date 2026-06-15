"""Reproduces the freeze bug: token stops circulating after a second send
following a corrupted->NAK->retransmit->ACK cycle.

Captures stdout+stderr (stderr=STDOUT) so Python tracebacks from dying
threads are not lost. Logs go to tests/log_freeze_A/B/C.txt.

Run from the base directory:
    python -u tests/driver_freeze.py
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
    ("A", "tests/config_rep_A.txt", 6001),
    ("B", "tests/config_rep_B.txt", 6002),
    ("C", "tests/config_rep_C.txt", 6003),
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
        print("[driver] -> {}: {}".format(name, cmd))
    except BrokenPipeError:
        print("[driver] AVISO: pipe quebrado para {}".format(name))


def collect(name):
    with log_locks[name]:
        return list(log_lines[name])


def count_token_lines(name):
    """Count lines containing token circulation evidence."""
    fragments = ["recebeu o token", "enviando token"]
    n = 0
    for _ts, ln in collect(name):
        for frag in fragments:
            if frag in ln:
                n += 1
                break
    return n


def count_token_lines_after(name, after_ts):
    fragments = ["recebeu o token", "enviando token"]
    n = 0
    for ts, ln in collect(name):
        if ts >= after_ts:
            for frag in fragments:
                if frag in ln:
                    n += 1
                    break
    return n


def find_line(name, fragment):
    for _ts, ln in collect(name):
        if fragment in ln:
            return ln.rstrip()
    return None


def find_all_lines(name, fragment, after_ts=None):
    results = []
    for ts, ln in collect(name):
        if after_ts is not None and ts < after_ts:
            continue
        if fragment in ln:
            results.append((ts, ln.rstrip()))
    return results


def find_any_traceback():
    """Return (node, lines) for the first traceback found in any log."""
    tb_markers = ["Traceback (most recent call last)", "Exception ", "Error:", "raise "]
    for name, _, _ in NODES:
        lines = collect(name)
        for i, (ts, ln) in enumerate(lines):
            for marker in tb_markers:
                if marker in ln:
                    # grab up to 20 lines of context
                    start = max(0, i)
                    end = min(len(lines), i + 25)
                    block = [l.rstrip() for _, l in lines[start:end]]
                    return name, block
    return None, None


def tail_lines(name, n=25):
    lines = collect(name)
    tail = lines[-n:] if len(lines) >= n else lines
    return [ln.rstrip() for _, ln in tail]


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
        log_path = os.path.join(TESTS, "log_freeze_{}.txt".format(name))
        t = threading.Thread(target=_reader, args=(name, p, log_path), daemon=True)
        t.start()
        print("[driver] no {} iniciado pid={}".format(name, p.pid))
        time.sleep(0.5)


def shutdown_nodes():
    for name, _, _ in NODES:
        send(name, "quit")
    time.sleep(2)
    for name, _, _ in NODES:
        p = procs[name]
        if p.poll() is None:
            p.terminate()
            try:
                p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p.kill()


def run_once(run_index=1):
    print("\n" + "=" * 60)
    print("TENTATIVA {}".format(run_index))
    print("=" * 60)

    launch_nodes()

    # -- Phase 1: warmup -- ring forms, A generates token, circulation
    print("[driver] aguardando aquecimento (10s)...")
    time.sleep(10)

    token_before_msg1 = {n: count_token_lines(n) for n, _, _ in NODES}
    print("[driver] token lines antes msg1: {}".format(token_before_msg1))

    # -- Phase 2: msg1 -- A->B "abc", error_prob=100 => corrupted -> NAK -> retransmit -> ACK
    t_msg1 = time.monotonic()
    print("[driver] enviando msg1: A -> B 'abc' (esperado: corrupcao -> NAK -> RETRANSMISSAO -> ACK)")
    send("A", "send B abc")
    time.sleep(10)

    t_after_msg1 = time.monotonic()
    token_after_msg1 = {n: count_token_lines(n) for n, _, _ in NODES}
    print("[driver] token lines apos msg1: {}".format(token_after_msg1))

    # -- Phase 3: msg2 -- the one that froze for the user
    t_msg2 = time.monotonic()
    print("[driver] enviando msg2: A -> B '1234' (este e o que travou)")
    send("A", "send B 1234")
    time.sleep(10)

    t_after_msg2 = time.monotonic()
    token_after_msg2 = {n: count_token_lines(n) for n, _, _ in NODES}
    print("[driver] token lines apos msg2: {}".format(token_after_msg2))

    # Check if freeze already happened
    new_tokens_msg2 = {n: token_after_msg2[n] - token_after_msg1[n] for n, _, _ in NODES}
    print("[driver] novos token-events durante msg2: {}".format(new_tokens_msg2))

    # -- Phase 4: msg3 -- extra probe
    t_msg3 = time.monotonic()
    print("[driver] enviando msg3: A -> C 'hello'")
    send("A", "send C hello")
    time.sleep(8)

    t_after_msg3 = time.monotonic()
    token_after_msg3 = {n: count_token_lines(n) for n, _, _ in NODES}
    new_tokens_msg3 = {n: token_after_msg3[n] - token_after_msg2[n] for n, _, _ in NODES}
    print("[driver] novos token-events durante msg3: {}".format(new_tokens_msg3))

    # -- Phase 5: status
    send("A", "status")
    send("B", "status")
    send("C", "status")
    time.sleep(2)

    # -- Phase 6: shutdown
    shutdown_nodes()

    # ---- ANALYSIS ----
    print("\n" + "=" * 60)
    print("ANALISE - TENTATIVA {}".format(run_index))
    print("=" * 60)

    # Freeze detection: did tokens STOP after msg2?
    total_new_after_msg2 = sum(new_tokens_msg2.values())
    total_new_after_msg3 = sum(new_tokens_msg3.values())
    freeze_detected = (total_new_after_msg2 == 0 and total_new_after_msg3 == 0)
    print("\n[FREEZE] novos token-events apos msg2={}, apos msg3={}  =>  FREEZE={}".format(
        total_new_after_msg2, total_new_after_msg3, freeze_detected))

    # Traceback scan
    tb_node, tb_block = find_any_traceback()
    if tb_node:
        print("\n[TRACEBACK] encontrado no no {}:".format(tb_node))
        for ln in tb_block:
            print("  " + ln)
    else:
        print("\n[TRACEBACK] nenhum encontrado em nenhum no")

    # msg1 sequence: corrupted -> NAK -> RETRANSMISSAO -> ACK
    print("\n[MSG1] sequencia corrupcao/NAK/retransmissao:")
    for frag in ["corrompido", "corrupted", "NAK", "RETRANSMISSAO", "ACK", "CRC NAO confere"]:
        line = find_line("A", frag) or find_line("B", frag) or find_line("C", frag)
        if line:
            # find which node
            for n, _, _ in NODES:
                l2 = find_line(n, frag)
                if l2:
                    print("  [{}] {}".format(n, l2))
                    break

    # msg2 flow
    print("\n[MSG2] fluxo apos A enfileirar '1234':")
    for frag in ["1234", "enfileirou", "enviando DADOS", "DATA", "DADOS", "retornou", "ACK", "waiting"]:
        for n, _, _ in NODES:
            lines = find_all_lines(n, frag, after_ts=t_msg2)
            for ts, ln in lines[:3]:
                print("  [{}] {}".format(n, ln))

    # Status output
    print("\n[STATUS] ultimas respostas de status:")
    for n, _, _ in NODES:
        lines = find_all_lines(n, "status", after_ts=t_after_msg2)
        lines += find_all_lines(n, "has_token", after_ts=t_after_msg2)
        lines += find_all_lines(n, "fila", after_ts=t_after_msg2)
        lines += find_all_lines(n, "waiting", after_ts=t_after_msg2)
        lines += find_all_lines(n, "is_controller", after_ts=t_after_msg2)
        for ts, ln in lines[:5]:
            print("  [{}] {}".format(n, ln))

    # Tail logs
    print("\n[TAIL] ultimas 25 linhas de cada no:")
    for n, _, _ in NODES:
        lines = tail_lines(n, 25)
        print("\n  --- {} ({} linhas totais) ---".format(n, len(collect(n))))
        for ln in lines:
            print("  " + ln)

    return freeze_detected


def main():
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        # reset state between attempts
        procs.clear()
        log_lines.clear()
        log_locks.clear()
        log_files.clear()

        frozen = run_once(attempt)

        if frozen:
            print("\n=== FREEZE REPRODUZIDO na tentativa {} ===".format(attempt))
            break
        else:
            if attempt < max_attempts:
                print("\n[driver] freeze nao reproduzido na tentativa {}; aguardando 3s antes de nova tentativa...".format(attempt))
                time.sleep(3)
            else:
                print("\n=== FREEZE NAO REPRODUZIDO em {} tentativas ===".format(max_attempts))


if __name__ == "__main__":
    main()
