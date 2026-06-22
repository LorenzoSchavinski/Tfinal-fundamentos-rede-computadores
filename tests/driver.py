"""Harness de teste: sobe 3 nos do anel e valida o comportamento esperado."""

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
logs = {}
log_lines = {}
log_locks = {}
log_files = {}


def _reader(name, proc, log_path):
    """Thread daemon: le stdout linha a linha, salva no arquivo e na lista."""
    f = open(log_path, "w", encoding="utf-8")
    log_files[name] = f
    try:
        for line in proc.stdout:
            log_locks[name].acquire()
            try:
                log_lines[name].append(line)
                f.write(line)
                f.flush()
            finally:
                log_locks[name].release()
    except Exception:
        pass
    finally:
        f.close()


def send(name, cmd):
    """Envia um comando ao processo indicado."""
    p = procs[name]
    try:
        p.stdin.write(cmd + "\n")
        p.stdin.flush()
    except BrokenPipeError:
        print("[driver] aviso: pipe quebrado para {}".format(name))


def collect(name):
    """Retorna copia segura das linhas acumuladas."""
    log_locks[name].acquire()
    try:
        return list(log_lines[name])
    finally:
        log_locks[name].release()


def find_line(name, fragment):
    """Retorna a primeira linha que contem o fragmento, ou None."""
    for ln in collect(name):
        if fragment in ln:
            return ln.rstrip()
    return None


def main():
    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"

    # Inicializa estruturas por no
    for name, cfg, port in NODES:
        log_lines[name] = []
        log_locks[name] = threading.Lock()

    # Lanca processos com pequena defasagem para garantir que todos estao com
    # o socket aberto antes de o EVAL_FIRST_TOKEN disparar em qualquer no.
    # --discovery 6 da margem para os 3 processos subirem antes da eleicao.
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
        log_path = os.path.join(TESTS, "log_{}.txt".format(name))
        t = threading.Thread(target=_reader, args=(name, p, log_path), daemon=True)
        t.start()
        print("[driver] no {} iniciado (pid={})".format(name, p.pid))
        time.sleep(0.5)  # pequena defasagem entre lancamentos

    # 1) Janela de descoberta (6s) + formacao do anel + primeiras circulacoes
    print("[driver] aguardando formacao do anel (9s)...")
    time.sleep(9)

    # 2) Unicast limpo: B -> A
    print("[driver] enviando: B -> A 'Ola A'")
    send("B", "send A Ola A")
    time.sleep(6)

    # 3) Unicast com erro: A -> C (A tem error_prob=100, vai corromper e C manda NAK)
    # Necessita ~9s: 3s para A obter o token, 3s para retorno do NAK, 3s para retransmissao + ACK.
    print("[driver] enviando: A -> C 'Teste erro' (esperando NAK + retransmissao)")
    send("A", "send C Teste erro")
    time.sleep(12)

    # 4) Broadcast: C -> todos (aguarda 8s para o token chegar a C e o broadcast completar)
    print("[driver] enviando: C -> BROADCAST 'Ola galera'")
    send("C", "send BROADCAST Ola galera")
    time.sleep(8)

    # 5) B solicita a retirada do proximo token; A regenera apos o timeout.
    print("[driver] B solicitando retirada do proximo token...")
    send("B", "removetoken")
    time.sleep(12)

    # 6) Status em todos
    print("[driver] consultando status...")
    send("A", "status")
    send("B", "status")
    send("C", "status")
    time.sleep(2)

    # 7) Encerra
    print("[driver] encerrando nos...")
    send("A", "quit")
    send("B", "quit")
    send("C", "quit")
    time.sleep(2)

    # Forca encerramento de qualquer sobrevivente
    for name, _, _ in NODES:
        p = procs[name]
        if p.poll() is None:
            p.terminate()
            p.wait(timeout=3)

    # --- Dump dos logs ---
    print("\n" + "=" * 60)
    print("DUMP DOS LOGS")
    print("=" * 60)
    for name, _, _ in NODES:
        lines = collect(name)
        print("\n--- log_{}.txt ({} linhas) ---".format(name, len(lines)))
        for ln in lines:
            print(ln, end="")
        if lines and not lines[-1].endswith("\n"):
            print()

    # --- Verificacao dos marcadores esperados ---
    print("\n" + "=" * 60)
    print("VERIFICACAO")
    print("=" * 60)

    checks = []

    def chk(label, line):
        ok = line is not None
        status = "PASS" if ok else "FAIL"
        checks.append((status, label, line))

    # a) Formacao do anel
    chk(
        "(a1) DISCOVER/HELLO trocados",
        find_line("A", "DISCOVER de ") or find_line("A", "HELLO de "),
    )
    chk("(a2) anel com 3 membros em A", find_line("A", "anel: ['A', 'B', 'C']"))

    # b) Primeiro token
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
        "(b3) token circulou (enviando token para)",
        find_line("A", "enviando token para") or find_line("B", "enviando token para"),
    )

    # c) Unicast ACK: B->A
    chk("(c1) A recebeu DADOS de B", find_line("A", "DADOS de B"))
    chk(
        "(c2) B confirmou entrega com ACK",
        find_line("B", "entregue com sucesso (ACK)") or find_line("B", "ACK"),
    )

    # d) NAK + retransmissao: A->C
    chk(
        "(d1) C recebeu CRC errado / NAK",
        find_line("C", "NAK") or find_line("C", "CRC NAO confere"),
    )
    chk("(d2) A logou RETRANSMISSAO", find_line("A", "RETRANSMISSAO"))
    chk(
        "(d3) A confirmou ACK apos retransmissao",
        find_line("A", "entregue com sucesso (ACK)"),
    )

    # e) Broadcast
    chk("(e1) A recebeu BROADCAST de C", find_line("A", "BROADCAST de C"))
    chk("(e2) B recebeu BROADCAST de C", find_line("B", "BROADCAST de C"))
    chk(
        "(e3) C broadcast concluido",
        find_line("C", "broadcast concluido") or find_line("C", "BROADCAST concluido"),
    )

    # f) Recuperacao de token perdido
    chk("(f1) A detectou TOKEN PERDIDO e regenerou", find_line("A", "TOKEN PERDIDO"))

    # g) Sem tracebacks
    tb_found = None
    for name, _, _ in NODES:
        tb = find_line(name, "Traceback")
        if tb:
            tb_found = "{}: {}".format(name, tb)
            break
    chk("(g) sem tracebacks em nenhum log", None if tb_found else "OK - sem tracebacks")

    print("\n{:<6} {:<35} {}".format("STATUS", "VERIFICACAO", "EVIDENCIA"))
    print("-" * 90)
    all_pass = True
    for status, label, evidence in checks:
        if status == "FAIL":
            all_pass = False
        ev = (evidence or "")[:60] if evidence else "(nao encontrado)"
        print("{:<6} {:<35} {}".format(status, label, ev))

    print(
        "\n"
        + (
            "=== RESULTADO FINAL: PASS ==="
            if all_pass
            else "=== RESULTADO FINAL: FAIL (veja tabela acima) ==="
        )
    )


if __name__ == "__main__":
    main()
