"""Harness de controle: valida os 4 cenarios de corretude do token-ring.

Cenario A - regressao: formacao, token, ACK, NAK+retransmissao, broadcast.
Cenario B - token duplicado: sincroniza a injecao com A segurando o token, de
            modo que a deteccao seja obrigatoria e nao dependa de corrida.
Cenario C - sem falso timeout durante circulacao quieta (janela de 12s).
Cenario D - token perdido: B remove o proximo token; controladora deve regenerar.
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
    ("A", "tests/config_A.txt", 6001),
    ("B", "tests/config_B.txt", 6002),
    ("C", "tests/config_C.txt", 6003),
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


def count_lines_between(name, fragment, t_start, t_end):
    n = 0
    for ts, ln in collect(name):
        if t_start <= ts <= t_end and fragment in ln:
            n += 1
    return n


def launch_nodes():
    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"

    for name, cfg, _port in NODES:
        log_lines[name] = []
        log_locks[name] = threading.Lock()

    for name, cfg, port in NODES:
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
        log_path = os.path.join(TESTS, "log_ctrl_{}.txt".format(name))
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


def main():
    launch_nodes()

    # ---------------------------------------------------------------- Cenario A
    print("[driver] aguardando formacao do anel + aquecimento (9s)...")
    time.sleep(9)

    print("[driver] A: B -> A 'Ola A'")
    send("B", "send A Ola A")
    time.sleep(6)

    print("[driver] A: A -> C 'Teste erro' (NAK + retransmissao)")
    send("A", "send C Teste erro")
    time.sleep(8)

    print("[driver] A: C -> BROADCAST 'Ola galera'")
    send("C", "send BROADCAST Ola galera")
    time.sleep(6)

    # ---------------------------------------------------------------- Cenario B: injecao deterministica de token duplicado
    # Espera A receber e segurar o token; C, predecessor direto de A, injeta um
    # 1000 adicional que chega enquanto A ainda possui o token legitimo.
    marker_dup = time.monotonic()
    held_by_a = None
    deadline_dup = time.monotonic() + 6
    while time.monotonic() < deadline_dup:
        held_by_a = find_line_after("A", "recebeu o token", marker_dup)
        if held_by_a:
            break
        time.sleep(0.02)
    t_before_dup = time.monotonic()
    print("[driver] C: injetando token extra enquanto A segura o token")
    send("C", "gentoken")
    time.sleep(6)

    # ---------------------------------------------------------------- Cenario C: circulacao quieta sem falso timeout
    t_quiet_start = time.monotonic()
    print("[driver] C: janela quieta de 12s (sem comandos)...")
    time.sleep(12)
    t_quiet_end = time.monotonic()

    # ---------------------------------------------------------------- Cenario D: token perdido e recuperacao
    print("[driver] D: B solicita retirada do proximo token...")
    send("B", "removetoken")
    t_remove_done = time.monotonic()
    # Aguarda até 14s: timeout=10 + margem
    time.sleep(14)

    send("A", "status")
    send("B", "status")
    send("C", "status")
    time.sleep(2)

    shutdown_nodes()

    # ---------------------------------------------------------------- Dump
    print("\n" + "=" * 60)
    print("DUMP DOS LOGS")
    print("=" * 60)
    for name, _, _ in NODES:
        lines = collect(name)
        print("\n--- log_ctrl_{}.txt ({} linhas) ---".format(name, len(lines)))
        for _ts, ln in lines:
            print(ln, end="")
        if lines and not lines[-1][1].endswith("\n"):
            print()

    # ---------------------------------------------------------------- Verificacao
    print("\n" + "=" * 60)
    print("VERIFICACAO")
    print("=" * 60)

    checks = []

    def chk(label, line, note=None):
        ok = line is not None
        checks.append(("PASS" if ok else "FAIL", label, line, note))

    # Cenario A - regressao
    chk(
        "(a1) DISCOVER/HELLO trocados",
        find_line("A", "DISCOVER de ") or find_line("A", "HELLO de "),
    )
    chk("(a2) anel com 3 membros em A", find_line("A", "anel: ['A', 'B', 'C']"))
    chk(
        "(b1) A gerou token inicial",
        find_line("A", "gerando token inicial") or find_line("A", "gerou/inseriu"),
    )
    chk(
        "(b2) algum no recebeu o token",
        find_line("A", "recebeu o token")
        or find_line("B", "recebeu o token")
        or find_line("C", "recebeu o token"),
    )
    chk(
        "(b3) token circulou",
        find_line("A", "enviando token para") or find_line("B", "enviando token para"),
    )
    chk("(c1) A recebeu DADOS de B", find_line("A", "DADOS de B"))
    chk(
        "(c2) B confirmou ACK",
        find_line("B", "entregue com sucesso (ACK)") or find_line("B", "ACK"),
    )
    chk(
        "(d1) C recebeu NAK / CRC errado",
        find_line("C", "NAK") or find_line("C", "CRC NAO confere"),
    )
    chk("(d2) A logou RETRANSMISSAO", find_line("A", "RETRANSMISSAO"))
    chk(
        "(d3) A confirmou ACK apos retransmissao",
        find_line("A", "entregue com sucesso (ACK)"),
    )
    chk("(e1) A recebeu BROADCAST de C", find_line("A", "BROADCAST de C"))
    chk("(e2) B recebeu BROADCAST de C", find_line("B", "BROADCAST de C"))
    chk(
        "(e3) C broadcast concluido",
        find_line("C", "broadcast concluido") or find_line("C", "BROADCAST concluido"),
    )

    # Cenario B - injecao de token extra
    # Verificacao primaria: anel NAO morre (tokens continuam circulando apos injecao).
    ring_alive_after_dup = (
        find_line_after("A", "recebeu o token", t_before_dup)
        or find_line_after("B", "recebeu o token", t_before_dup)
        or find_line_after("C", "recebeu o token", t_before_dup)
    )
    chk("(B1) anel sobrevive a injecao de token extra", ring_alive_after_dup)
    # Deteccao obrigatoria: a injecao foi sincronizada com a posse do token em A.
    dup_line = find_line_after("A", "TOKEN DUPLICADO", t_before_dup)
    chk("(B2) TOKEN DUPLICADO detectado por A", dup_line)

    # Cenario C - sem falso TOKEN PERDIDO na janela quieta
    false_lost = None
    for ts, ln in collect("A"):
        if t_quiet_start <= ts <= t_quiet_end and "TOKEN PERDIDO" in ln:
            false_lost = ln.rstrip()
            break
    chk(
        "(C1) sem TOKEN PERDIDO na janela quieta",
        "OK - sem falso timeout" if false_lost is None else None,
    )

    # Cenario D - token realmente perdido e recuperado
    # Procura TOKEN PERDIDO em A apos t_remove_done (com margem de 2s anterior para capturar)
    real_lost = find_line_after(
        "A", "TOKEN PERDIDO detectado (timeout)", t_remove_done - 2
    )
    chk("(D1) A detectou TOKEN PERDIDO apos removetoken", real_lost)
    ring_recovered = (
        find_line_after("A", "recebeu o token", t_remove_done)
        or find_line_after("B", "recebeu o token", t_remove_done)
        or find_line_after("C", "recebeu o token", t_remove_done)
    )
    chk("(D2) anel retomou circulacao apos recuperacao", ring_recovered)

    # Sem tracebacks
    tb_found = None
    for name, _, _ in NODES:
        tb = find_line(name, "Traceback")
        if tb:
            tb_found = "{}: {}".format(name, tb)
            break
    chk("(G) sem tracebacks", None if tb_found else "OK - sem tracebacks")

    print("\n{:<6} {:<45} {}".format("STATUS", "VERIFICACAO", "EVIDENCIA"))
    print("-" * 110)
    all_pass = True
    for status, label, evidence, note in checks:
        if status == "FAIL":
            all_pass = False
        ev = (evidence or "")[:50] if evidence else "(nao encontrado)"
        print("{:<6} {:<45} {}".format(status, label, ev))

    verdict = "PASS" if all_pass else "FAIL (veja tabela acima)"
    print("\n=== RESULTADO FINAL: {} ===".format(verdict))


if __name__ == "__main__":
    main()
