
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

## Opcao 2: testar com outras pessoas (modo LAN)

Varias maquinas reais na mesma rede. Use isso pra testar com os outros grupos.

### Em cada maquina

1. Crie um `config.txt` com um apelido **unico** (combine com o pessoal pra ninguem repetir letra).

```
python main.py config.txt
```

Pronto. Os nodes se descobrem sozinhos por broadcast.

no caso tu pode so uysar o config.example.txt e fodasse so troca a letrinha la 
entao 

python main.py config.example.txt

### Importante pra funcionar

- Todos tem que estar **na mesma rede** (mesmo Wi-Fi / mesma LAN). NAO FUNCIONA A DA PUC 

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
