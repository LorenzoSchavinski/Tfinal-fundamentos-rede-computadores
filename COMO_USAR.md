# Como usar o anel de token

Guia rapido pra rodar e testar hoje, sozinho ou com outras pessoas.

Nao precisa instalar nada. So Python 3.8 ou mais novo. Sem bibliotecas externas.

Pra conferir o Python:

```
python --version
```

---

## O arquivo de configuracao

Cada maquina precisa de um arquivo de config com **exatamente 5 linhas**, nesta ordem:

```
A          <- apelido desta maquina (uma letra: A, B, C...)
2          <- tempo do token e dados (segundos)
20         <- probabilidade de inserir erro nas mensagens (em %)
2,5        <- timeout do token (segundos)
2          <- tempo minimo entre tokens (segundos)
```

Pode usar virgula no decimal (`2,5`), igual no exemplo da materia. O apelido de cada maquina tem que ser **unico** no anel.

---

## Opcao 1: testar sozinho no seu PC (modo local)

Roda 3 nodes na mesma maquina, cada um numa janela de terminal.

### Passo 1: criar 3 arquivos de config

Crie `config_a.txt`, `config_b.txt`, `config_c.txt`. Conteudo de cada (so muda a primeira linha, o apelido):

`config_a.txt`
```
A
2
20
2,5
2
```

`config_b.txt` igual mas primeira linha `B`. `config_c.txt` igual mas primeira linha `C`.

### Passo 2: conferir o arquivo de peers

Ja existe `peers.example.txt`. Ele lista os 3 nodes com porta de cada:

```
A 127.0.0.1 6001
B 127.0.0.1 6002
C 127.0.0.1 6003
```

### Passo 3: abrir 3 terminais e rodar um comando em cada

Terminal 1:
```
python main.py config_a.txt --peers peers.example.txt --port 6001 --ip 127.0.0.1
```

Terminal 2:
```
python main.py config_b.txt --peers peers.example.txt --port 6002 --ip 127.0.0.1
```

Terminal 3:
```
python main.py config_c.txt --peers peers.example.txt --port 6003 --ip 127.0.0.1
```

Cada um precisa da **sua propria porta** (6001, 6002, 6003), batendo com a porta do peers. A maquina A (menor apelido) gera o primeiro token automaticamente.

---

## Opcao 2: testar com outras pessoas (modo LAN)

Varias maquinas reais na mesma rede. Use isso pra testar com os outros grupos.

### Em cada maquina

1. Crie um `config.txt` com um apelido **unico** (combine com o pessoal pra ninguem repetir letra).
2. Rode, sem `--peers` e sem `--port`:

```
python main.py config.txt
```

Pronto. Os nodes se descobrem sozinhos por broadcast.

### Importante pra funcionar

- Todos tem que estar **na mesma rede** (mesmo Wi-Fi / mesma LAN).
- A porta usada e a **6000 UDP**, fixa. Se nao conectar, libere a porta 6000 UDP no firewall do Windows.
- Comecem os programas **com poucos segundos de diferenca**, pra todo mundo se descobrir antes do primeiro token. Se precisar de mais tempo de descoberta: adicione `--discovery 5` no fim do comando (5 segundos).
- Apelidos **unicos** em todo o anel. Apelido repetido quebra a ordem.

---

## Comandos durante a execucao

Depois que o node ta rodando, digite no terminal e aperte Enter:

| Comando | O que faz |
|---|---|
| `send B oi pessoal` | envia a mensagem "oi pessoal" pra maquina B |
| `send BROADCAST oi` | envia pra todas as maquinas |
| `gentoken` | cria/insere um token no anel |
| `removetoken` | remove o token (so se este node estiver com ele) |
| `status` | mostra o estado deste node (apelido, sucessor, se e controlador, se ta com token, ordem do anel) |
| `queue` | mostra a fila de mensagens deste node |
| `join` | reenvia DISCOVER pra atualizar a topologia |
| `help` | lista os comandos |
| `quit` | encerra o node silenciosamente (nao avisa os outros) |

A fila guarda no maximo **10 mensagens** por node.

---

## O que dar pra ver na apresentacao

Tudo aparece no log da tela conforme acontece. Da pra acompanhar:

- token chegando: `recebeu o token`
- token sendo passado: `enviando token para X`
- dados sendo enviados / repassados: `enviando DADOS para...`, `repassando DADOS de...`
- entrega com `ACK` ou `NAK`
- retransmissao: `RETRANSMISSAO: NAK recebido...`
- token perdido ou duplicado: `TOKEN PERDIDO detectado` / `TOKEN DUPLICADO detectado`

Pra mostrar topologia e quem ta com o token, use `status` em cada maquina.

### Roteiro de demo sugerido

1. Suba 3 nodes (A, B, C). Mostre o token circulando.
2. `send` uma mensagem unicast (ex: A manda pra C). Mostre o ACK voltando.
3. `send BROADCAST` uma mensagem. Mostre chegando em todos.
4. Mande pra um apelido que nao existe. Mostre o `maquinainexistente`.
5. `removetoken` e mostre o controlador regenerando (token perdido).
6. `gentoken` num segundo node e mostre a deteccao de token duplicado.
7. Suba um node novo (D) com o anel rodando e mostre ele entrando.
8. Encerre um node com `quit` (ele sai sem avisar). Depois mande uma mensagem pra ele: o pacote volta com `maquinainexistente`, o remetente remove o no ausente do anel e imprime o aviso.

---

## Problemas comuns

- **Nao conecta na LAN**: firewall bloqueando a porta 6000 UDP, ou maquinas em redes diferentes.
- **Token nao aparece**: comecaram muito espacado, alguem nao foi descoberto. Use `--discovery 5` e subam mais juntos. Ou force com `gentoken` na maquina de menor apelido.
- **`--port` ignorado**: na LAN a porta e sempre 6000, `--port` so vale no modo local (com `--peers`).
- **Apelidos repetidos**: cada maquina tem que ter uma letra diferente.
