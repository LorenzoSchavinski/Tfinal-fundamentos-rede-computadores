"""Harness de controle: valida os 4 cenarios de corretude do token-ring.

Cenario A - regressao: formacao, token, ACK, NAK+retransmissao, broadcast.
Cenario B - token duplicado: injeta segundo token; se dois tokens chegarem ao
            controlador dentro de min_token_interval, ele loga TOKEN DUPLICADO.
            Com token_time=1s e 3 nos (lap ~3s), a janela de deteccao (0.5s) so
            dispara se os dois tokens chegarem a A quase simultaneamente -- o que
            depende de corrida de timing. O teste verifica o LOG quando ocorre, e
            sempre verifica que o anel NAO morre (criterio principal).
Cenario C - sem falso timeout durante circulacao quieta (janela de 12s).
Cenario D - token perdido: removetoken em todos; controladora deve regenerar.
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

    # ---------------------------------------------------------------- Cenario B: injecao de token duplicado
    # Injeta em B (nao-controladora). Dois resultados possiveis dependendo de timing:
    # (a) B tem o token quando gentoken chega: epoch sobe, token real e abandonado,
    #     novo token vai para C -- net=1 token, sem duplicata em A.
    # (b) B nao tem o token quando gentoken chega: dois tokens no anel. A pode ou nao
    #     logar TOKEN DUPLICADO dependendo do intervalo de chegada dos dois em A.
    # Em ambos os casos, o anel NAO deve morrer (criterio principal do cenario B).
    t_before_dup = time.monotonic()
    print("[driver] B: injetando token extra em B via gentoken")
    send("B", "gentoken")
    # Espera 6s: suficiente para que qualquer duplicata seja absorvida ou detectada
    # e o anel volte a estado estavel com 1 token.
    time.sleep(6)
    t_after_dup = time.monotonic()

    # Drena possiveis tokens extras: envia removetoken a todos para garantir que
    # apenas 1 token sobra antes do cenario D. Se o anel tiver 2 tokens, dois dos
    # tres removetokens irao pegar um cada; se tiver 1, apenas um sera pego.
    # Depois, aguarda ate 12s: se todos forem removidos, A regenerara o token.
    # Se sobrou 1, o anel continua e D sinaliza "nao tinha token".
    # Para garantir que D comeca com anel limpo (1 token), fazemos a limpeza aqui
    # e depois confirmamos que o anel continua antes de prosseguir.
    print("[driver] normalizando anel apos cenario B (possivel extra token)...")
    send("A", "removetoken")
    time.sleep(0.5)
    send("B", "removetoken")
    time.sleep(0.5)
    send("C", "removetoken")
    # Aguarda regeneracao se necessario (timeout=10s)
    time.sleep(12)
    t_normalized = time.monotonic()

    # ---------------------------------------------------------------- Cenario C: circulacao quieta sem falso timeout
    t_quiet_start = time.monotonic()
    print("[driver] C: janela quieta de 12s (sem comandos)...")
    time.sleep(12)
    t_quiet_end = time.monotonic()

    # ---------------------------------------------------------------- Cenario D: token perdido e recuperacao
    print("[driver] D: removendo token em todos os nos...")
    send("A", "removetoken")
    time.sleep(0.5)
    send("B", "removetoken")
    time.sleep(0.5)
    send("C", "removetoken")
    t_remove_done = time.monotonic()
    # Aguarda até 14s: timeout=10 + margem
    time.sleep(14)
    t_after_recovery = time.monotonic()

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
    chk("(a1) DISCOVER/HELLO trocados",
        find_line("A", "DISCOVER de ") or find_line("A", "HELLO de "))
    chk("(a2) anel com 3 membros em A",
        find_line("A", "anel: ['A', 'B', 'C']"))
    chk("(b1) A gerou token inicial",
        find_line("A", "gerando token inicial") or find_line("A", "gerou/inseriu"))
    chk("(b2) algum no recebeu o token",
        find_line("A", "recebeu o token") or find_line("B", "recebeu o token") or find_line("C", "recebeu o token"))
    chk("(b3) token circulou",
        find_line("A", "enviando token para") or find_line("B", "enviando token para"))
    chk("(c1) A recebeu DADOS de B",
        find_line("A", "DADOS de B"))
    chk("(c2) B confirmou ACK",
        find_line("B", "entregue com sucesso (ACK)") or find_line("B", "ACK"))
    chk("(d1) C recebeu NAK / CRC errado",
        find_line("C", "NAK") or find_line("C", "CRC NAO confere"))
    chk("(d2) A logou RETRANSMISSAO",
        find_line("A", "RETRANSMISSAO"))
    chk("(d3) A confirmou ACK apos retransmissao",
        find_line("A", "entregue com sucesso (ACK)"))
    chk("(e1) A recebeu BROADCAST de C",
        find_line("A", "BROADCAST de C"))
    chk("(e2) B recebeu BROADCAST de C",
        find_line("B", "BROADCAST de C"))
    chk("(e3) C broadcast concluido",
        find_line("C", "broadcast concluido") or find_line("C", "BROADCAST concluido"))

    # Cenario B - injecao de token extra
    # Verificacao primaria: anel NAO morre (tokens continuam circulando apos injecao).
    ring_alive_after_dup = (
        find_line_after("A", "recebeu o token", t_before_dup)
        or find_line_after("B", "recebeu o token", t_before_dup)
        or find_line_after("C", "recebeu o token", t_before_dup)
    )
    chk("(B1) anel sobrevive a injecao de token extra", ring_alive_after_dup)
    # Verificacao secundaria (timing-dependente): TOKEN DUPLICADO no log de A.
    dup_line = find_line_after("A", "TOKEN DUPLICADO", t_before_dup)
    chk("(B2) TOKEN DUPLICADO detectado por A (timing-dep.)", dup_line,
        note="ESPERADO APENAS se dois tokens chegaram a A com gap < 0.5s; do contrario FAIL eh aceitavel")

    # Cenario C - sem falso TOKEN PERDIDO na janela quieta
    false_lost = None
    for ts, ln in collect("A"):
        if t_quiet_start <= ts <= t_quiet_end and "TOKEN PERDIDO" in ln:
            false_lost = ln.rstrip()
            break
    chk("(C1) sem TOKEN PERDIDO na janela quieta",
        "OK - sem falso timeout" if false_lost is None else None)

    # Cenario D - token realmente perdido e recuperado
    # Procura TOKEN PERDIDO em A apos t_remove_done (com margem de 2s anterior para capturar)
    real_lost = find_line_after("A", "TOKEN PERDIDO detectado (timeout)", t_remove_done - 2)
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
    timing_dep_fail = False
    all_pass = True
    for status, label, evidence, note in checks:
        is_timing = note is not None
        if status == "FAIL":
            if is_timing:
                timing_dep_fail = True
            else:
                all_pass = False
        ev = (evidence or "")[:50] if evidence else "(nao encontrado)"
        suffix = " [timing-dep]" if is_timing and status == "FAIL" else ""
        print("{:<6} {:<45} {}{}".format(status, label, ev, suffix))

    if timing_dep_fail:
        print("\n  NOTA (B2): TOKEN DUPLICADO so e detectado se dois tokens chegam ao")
        print("  controlador com gap < min_token_interval (0.5s). Com token_time=1s e")
        print("  3 nos (lap ~3s), isso depende de corrida de timing e pode nao ocorrer.")
        print("  O criterio principal (B1: anel sobrevive) e o que deve passar.")

    verdict = "PASS" if all_pass else "FAIL (veja tabela acima)"
    if all_pass and timing_dep_fail:
        verdict = "PASS (com nota: B2 timing-dep falhou, B1 passou)"
    print("\n=== RESULTADO FINAL: {} ===".format(verdict))


if __name__ == "__main__":
    main()
