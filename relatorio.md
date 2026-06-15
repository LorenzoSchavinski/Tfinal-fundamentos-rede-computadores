# Relatório — Simulação de Rede Local em Anel com Passagem de Token sobre UDP

Trabalho Final — Fundamentos de Redes de Computadores
Implementação em Python 3 (pacote `ring/` + `main.py`).

---

## 1. Introdução e objetivo

A aplicação simula o funcionamento de uma **rede local em anel** (token ring) na qual várias máquinas trocam mensagens usando **UDP** como transporte. Cada máquina é um processo independente; o conjunto das máquinas ativas forma um anel lógico orientado, e um pacote especial (o **token**) circula continuamente. Só a máquina que está de posse do token pode transmitir dados, e a transmissão sempre segue para o **sucessor** no anel.

Objetivos atendidos:

- formação automática do anel por descoberta em broadcast (DISCOVER/HELLO);
- circulação do token entre os nós, na ordem do anel;
- fila de mensagens por máquina (até 10), com destino por mensagem;
- entrega unicast com confirmação (ACK), detecção de erro por CRC-32 (NAK) e retransmissão; envio broadcast;
- módulo de inserção de falhas que corrompe mensagens com probabilidade configurável;
- controle do token: detecção de token perdido (timeout) e token duplicado (intervalo mínimo), com geração/retirada manual a qualquer momento;
- alteração topológica em execução (entrada de máquinas sem reinicialização);
- interface de console para operar e observar o anel.

A separação rígida entre cabeçalho ASCII e payload, mais o uso do CRC-32 padrão (zlib/IEEE 802.3), visa **interoperabilidade** com implementações de outros grupos, como exige o enunciado.

---

## 2. Visão geral da arquitetura

O código é organizado em camadas, cada uma um subpacote de `ring/`:

| Camada | Pacote | Responsabilidade |
|---|---|---|
| Protocolo | `ring/protocol/` | Formato dos datagramas (`packets.py`), CRC-32 (`crc.py`), inserção de falhas (`fault.py`). Define o "idioma de fio". |
| Núcleo | `ring/core/` | Estado da máquina e máquina de estados: `node.py` (motor), `token_logic.py`, `commands.py`, `message_queue.py`, `token_control.py`. |
| Rede | `ring/network/` | Transporte UDP (`transport.py`) e topologia/descoberta do anel (`discovery.py`). |
| Interface | `ring/ui/` | Log thread-safe e laço de comandos (`console.py`). |
| Entrada | `main.py` | Parsing de argumentos, leitura de config/peers, criação e arranque do `Node`. |

### Modelo central: uma thread de motor sobre um barramento de eventos

O ponto arquitetural mais importante é que **todo o estado mutável da máquina vive no objeto `Node` e é alterado por UMA única thread**, o *motor* (`Node._engine_loop`). As demais threads (receptora UDP, vigia do token, console, `threading.Timer`) **nunca tocam o estado**: elas apenas **postam eventos** num `queue.Queue` (o barramento, `Node.bus`) via `Node.post(...)`. O motor consome o barramento em laço e despacha cada evento para um handler através da tabela `Node._handlers`.

Consequência direta: como o barramento serializa todos os eventos, as mutações de estado acontecem em série, numa só thread, e **não é preciso lock algum sobre o estado**.

Fluxo textual de um datagrama recebido até virar ação:

```
                 +------------------+        post(evento)        +------------------+
  rede (UDP) --> | thread receptora | -------------------------> |    barramento    |
                 |  (Transport.rx)  |  on_datagram -> parse      |  Node.bus (Queue)|
                 +------------------+                            +---------+--------+
                                                                           |
  console (stdin) --post(CMD_*)--------------------------------------------+
  vigia do token  --post(MON_TOKEN_TIMEOUT)--------------------------------+
  threading.Timer --post(TIMER_FORWARD_TOKEN / EVAL_FIRST_TOKEN)-----------+
                                                                           v
                                                              +-------------------------+
                                                              |  thread MOTOR (engine)  |
                                                              |  _engine_loop:          |
                                                              |   etype,kw = bus.get()  |
                                                              |   _handlers[etype](...) |
                                                              +-----------+-------------+
                                                                          | (muta estado, envia)
                                                                          v
                                                              Transport.send_addr -> sucessor
```

---

## 3. Estruturas de dados

### `Config` (`ring/config.py`)
Parâmetros de operação lidos do arquivo de 5 linhas:

- `apelido` (str)
- `token_time` (s) — tempo segurando o token / ritmo de envio
- `error_prob` (%) — probabilidade de inserir erro
- `token_timeout` (s) — tempo até considerar o token perdido
- `min_token_interval` (s) — intervalo mínimo entre tokens (detecção de duplicata)

Números aceitam vírgula decimal (`_to_float` faz `replace(",", ".")`).

### `MessageQueue` + `QueueItem` (`ring/core/message_queue.py`)
Fila FIFO limitada a `MAX = 10`. Cada `QueueItem` guarda:

- `destino` (apelido de destino, **por mensagem**);
- `message_str` (forma textual, para exibição) e `message_bytes` (forma enviada, UTF-8);
- `no_error` — passou a `True` quando a mensagem deve ser reenviada já correta (após NAK);
- `retransmit_used` — `True` depois que houve a retransmissão única.

Operações: `enqueue` (retorna `False` se cheia), `peek`, `pop`, `is_empty`, `is_full`, `items` (cópia para status).

### `Ring` (`ring/network/discovery.py`)
Topologia do anel:

- `_members`: mapa `apelido -> (ip, porta)`;
- `order()`: lista `(apelido, ip, porta)` ordenada por `(apelido, ip)` — o apelido define o sentido do anel; o ip é só desempate determinístico;
- `successor(apelido)`: próximo membro, **circular** (último liga no primeiro; com um só membro, ele é seu próprio sucessor);
- `controller_apelido()` / `is_controller(apelido)`: a controladora é o **menor apelido** (primeiro da ordem) — eleição implícita, sem mensagens.

### Barramento `queue.Queue` (`Node.bus`)
Fila thread-safe de eventos `(tipo, payload)`. É o ponto de serialização de toda mutação de estado.

### Estado interno do `Node` (só o motor escreve)
- `has_token` — está de posse do token;
- `waiting_for_data_return` — enviou DADOS e aguarda o retorno à origem (segura o token);
- `epoch` (contador) — invalida `Timer`s obsoletos quando o token muda (regerado/retirado/topologia);
- `expect_token_return` — primeira volta após (re)gerar o token, para isentar a detecção de duplicata;
- `observed_activity` — já viu token ou dados (impede gerar token inicial à toa);
- `first_token_generated` — já houve geração do token inicial;
- `tokens_perdidos`, `tokens_duplicados` — contadores de tokens perdidos/duplicados detectados (expostos no `status`);
- `is_controller`, `last_token_rx` (instante monotônico do último token aceito como controladora).

---

## 4. Threads

| Thread | Onde nasce | Papel |
|---|---|---|
| **receptora (rx)** | `Transport.start` | Bloqueia em `recvfrom(2048)`; ao chegar datagrama, chama `on_datagram`, que faz `parse` e **posta** o evento de RX correspondente. Encerra quando o socket fecha. |
| **motor (engine)** | `Node.start` | Único dono do estado. Laço `bus.get()` -> `_handlers[etype]`. Toda lógica de protocolo, token e comandos roda aqui, em série. |
| **TokenMonitor (token-monitor)** | `Node.__init__`/`monitor.start` | Vigia do token, **só atua na controladora**. Acorda a cada 0,1 s; se o silêncio (sem token nem dados) passar de `token_timeout`, posta `MON_TOKEN_TIMEOUT`. |
| **`threading.Timer`** | em `token_logic.py` / `node.start` | Temporizadores pontuais: ritmo do token (`TIMER_FORWARD_TOKEN` após `token_time`) e fim da janela de descoberta (`EVAL_FIRST_TOKEN`). Cada disparo só posta evento. |
| **console** | thread principal, `Console.run` | Lê `stdin`, traduz comandos em `CMD_*` e posta. Nunca toca o estado. |

---

## 5. Classes

| Classe | Arquivo | Responsabilidade |
|---|---|---|
| `Config` | `ring/config.py` | Guardar os 5 parâmetros de operação. |
| `QueueItem` | `ring/core/message_queue.py` | Uma mensagem enfileirada (destino, bytes, flags `no_error`/`retransmit_used`). |
| `MessageQueue` | `ring/core/message_queue.py` | Fila FIFO de até 10 `QueueItem`. |
| `Ring` | `ring/network/discovery.py` | Membros ativos, cálculo de sucessor e da controladora. |
| `Transport` | `ring/network/transport.py` | Socket UDP único + thread receptora; `broadcast` e `send_addr`. |
| `TokenMonitor` | `ring/core/token_control.py` | Detector de **token perdido** (timeout), controlado pelo nó. |
| `Console` | `ring/ui/console.py` | Laço de comandos do usuário (posta `CMD_*`). |
| `Node` | `ring/core/node.py` | Estado + máquina de estados dirigida pelo barramento. Herda os mixins abaixo. |
| `TokenLogicMixin` | `ring/core/token_logic.py` | Recepção/retenção/encaminhamento/geração do token e emissão de DADOS. |
| `CommandsMixin` | `ring/core/commands.py` | Handlers dos comandos do console (`_on_cmd_*`). |

`Node` herda de `TokenLogicMixin` e `CommandsMixin` apenas para dividir o arquivo do motor mantendo **um único objeto dono do estado**; todos os handlers rodam na mesma thread.

---

## 6. Mecanismos de sincronização (seção exigida)

A sincronização **não** usa locks sobre o estado. Ela se apoia em três peças:

1. **Barramento thread-safe `queue.Queue`** (`Node.bus`). Todas as threads externas (rx, monitor, console, timers) apenas **postam** eventos; o motor é a **única** thread que lê o barramento e muta o estado. Isso **serializa todas as mutações** numa só thread, eliminando condições de corrida e a necessidade de locks. `queue.Queue` já é internamente sincronizada para o par produtor/consumidor.

2. **Contador `epoch`** — invalida `Timer`s obsoletos. Quando o token é regenerado (`_generate_token`), retirado (`_on_cmd_remove_token`) ou um duplicado é consumido (`_on_rx_token`), `epoch` é incrementado. Os timers do token carregam a época em que foram criados (`ep = self.epoch`); ao disparar, `_on_timer_forward_token` só age se `ep == self.epoch`. Assim, um timer agendado antes de uma mudança de topologia/token "morre" silenciosamente em vez de encaminhar um token que já não deveria existir.

3. **Lock de impressão** (`ring/ui/console._print_lock`). Como rx, monitor e motor podem imprimir simultaneamente, `log()` envolve o `print` num `threading.Lock`, garantindo linhas atômicas (sem entrelaçamento). É a única exclusão mútua explícita do sistema, e protege apenas a saída, não o estado.

### Robustez do motor

O laço do motor (`_engine_loop`) envolve cada despacho de handler em `try/except Exception`, registrando o `traceback` e seguindo para o próximo evento. Assim, um pacote malformado ou estranho (por exemplo, de outro grupo durante a interoperação) **não derruba a thread do motor nem congela o nó**. Complementando, o caminho de retorno à origem em `_on_rx_data` **sempre libera/encaminha o token**, inclusive quando o `controle` que volta é inesperado (ou um NAK chega com a fila vazia): o token nunca fica preso por um valor de controle desconhecido, o que importa para interoperar com implementações de outros grupos.

---

## 7. Protocolo e formato dos pacotes (interoperabilidade)

Todo o tráfego é montado/desmontado em `ring/protocol/packets.py`. O cabeçalho é texto ASCII com campos separados por `:`; a **mensagem** de DADOS permanece em **bytes crus** e pode conter `:`.

| Tipo | Prefixo | Formato | Campos |
|---|---|---|---|
| DISCOVER | `10` | `10:<apelido>:<ip>` | apelido e IP da origem |
| HELLO | `20` | `20:<apelido>:<ip>` | apelido e IP da origem |
| LEAVE | `30` | `30:<apelido>:<ip>` | apelido e IP de quem sai (extensão local) |
| TOKEN | `1000` | `1000` | nenhum (payload é só `1000`) |
| DADOS | `2000` | `2000:<origem>:<destino>:<controle>:<crc>:<mensagem>` | origem, destino, controle, CRC, mensagem |

Valores do campo `controle`: `maquinainexistente` (CTRL_NONE, estado inicial), `ACK`, `NAK`.

> **Honestidade sobre o LEAVE**: o pacote `30:<apelido>:<ip>` é uma **extensão local** desta implementação para a saída educada (`quit`). Ele **não faz parte do protocolo de fio do enunciado**: outros grupos não o enviam e, ao recebê-lo, o tratam como datagrama desconhecido (`UNKNOWN`) e o **ignoram com segurança**. Apenas os nossos próprios nós o honram (`_on_rx_leave`). Portanto o LEAVE **não afeta a interoperabilidade**.

Exemplo real de DADOS (formato do enunciado): `2000:B:A:maquinainexistente:19385749:Oi pessoal!`.

Detalhes de implementação relevantes para interoperar:

- **Construção em bytes**: cada construtor (`build_discover`, `build_hello`, `build_token`, `build_data`) retorna `bytes` prontos para o socket; a mensagem é anexada **literalmente** (`cabecalho + message`), sem escape.
- **Parsing com `maxsplit`**: `parse` usa `datagram.split(b":", 5)` para DADOS, de modo que `partes[5]` contém a mensagem inteira mesmo que ela tenha `:`. DISCOVER/HELLO usam `split(b":", 2)`.
- **`set_controle`**: troca **apenas** o campo de controle (`partes[3]`), preservando CRC e mensagem; usa `split(b":", 5)` e rejunta. É a única alteração que um nó faz num DATA que não é seu.
- **Tolerância a `\x00` final**: `parse` remove **um** byte `b"\x00"` ao final, se houver. O cliente C++ de referência envia `strlen+1` (inclui o NUL terminador); descartá-lo evita corromper a interpretação e mantém a interoperabilidade.
- **Classificação**: TOKEN é reconhecido quando o payload inteiro é `b"1000"`; os demais pelo prefixo antes do primeiro `:`. Datagramas fora do contrato viram `UNKNOWN` e são ignorados.

---

## 8. Formação do anel

1. Ao subir (`Node.start`), o nó registra a si mesmo no `Ring` e envia **DISCOVER em broadcast**. No modo LAN o broadcast vai para `255.255.255.255` na **porta 6000**; no modo local, é simulado enviando uma cópia a cada peer conhecido.
2. Quem recebe DISCOVER (`_on_rx_discover`) atualiza o anel e responde **HELLO** em broadcast, identificando-se.
3. Quem recebe HELLO (`_on_rx_hello`) registra o novo membro.
4. A ordem do anel é a **ordenação por `(apelido, ip)`** (`Ring.order`) — apelido como chave primária (ordem alfabética), ip apenas como desempate determinístico. Cada nó calcula o **sucessor** circular com `Ring.successor`.
5. A **controladora** é o **menor apelido** (`Ring.controller_apelido`), eleita implicitamente por todos verem o mesmo conjunto.

Supressão de eco: em `on_datagram`, um DISCOVER/HELLO cujo apelido é o próprio é descartado (o broadcast volta para quem enviou).

Trecho real (formação, `tests/log_C.txt`):

```
[C] HELLO de A (entrou no anel)
[C] HELLO de B (entrou no anel)
```

e a resposta de A a quem chega (`tests/log_A.txt`):

```
[A] DISCOVER de B, respondendo HELLO
[A] DISCOVER de C, respondendo HELLO
```

---

## 9. CRC e módulo de inserção de falhas (seção exigida)

### CRC (`ring/protocol/crc.py`)
- `crc32(data)` usa `zlib.crc32(data) & 0xFFFFFFFF` — **CRC-32 padrão IEEE 802.3**, o mesmo de `java.util.zip.CRC32` e das implementações CRC32 comuns em C++, garantindo valor byte-a-byte idêntico entre grupos.
- **Escopo**: o CRC é calculado **somente sobre os bytes do campo `<mensagem>`** (`item.message_bytes` no envio; `parsed["message"]` na verificação).
- **Representação**: vai na rede como **string decimal** (`crc_field` faz `str(crc32(...))`).
- `matches(data, field)` recalcula e compara, aceitando `field` em `str` ou `bytes`; qualquer erro de conversão é tratado como campo inválido (`False`).
- **Observação sobre o exemplo do enunciado**: o valor `19385749` que aparece no exemplo `...:19385749:Oi pessoal!` é **ilustrativo**. O CRC-32 real de `"Oi pessoal!"` é **`1751094473`**, verificado no auto-teste de `crc.py` (`assert crc_field(b"Oi pessoal!") == "1751094473"`).

### Módulo de falhas (`ring/protocol/fault.py`)
- `maybe_corrupt(message, prob_percent, skip)` corrompe **um** byte aleatório da mensagem com probabilidade `prob_percent` (sorteio `random.uniform(0,100) < prob`), via XOR com `1..255` (garante que o byte muda e **mantém o tamanho**). Retorna `(mensagem, corrompida?)`.
- Não corrompe quando `skip` é `True`, a mensagem é vazia ou `prob_percent <= 0`.
- **O CRC enviado é sempre o da mensagem original** (`_send_data_packet` calcula `crc` sobre `item.message_bytes` antes de corromper). Ao corromper só o payload, o destino recalcula um CRC diferente e responde **NAK**, exercitando a retransmissão.
- A injeção é **pulada** quando o item é reenvio já correto (`item.no_error`) e em **broadcast** (`is_bcast`): `skip=(item.no_error or is_bcast)`.

---

## 10. Fila de mensagens

Cada máquina tem uma `MessageQueue` (FIFO, até **10** itens). Cada item carrega o **apelido de destino** próprio (`QueueItem.destino`), atendendo "para cada mensagem adicionada, deve ser armazenado também o apelido da máquina destino". A máquina só transmite quando está com o token, retirando da cabeça (`peek`/`pop`).

---

## 11. Funcionamento do token e dos dados

### Ciclo do token (`TokenLogicMixin._on_rx_token`)
Ao receber o token, `has_token = True`. Então:

- **Fila vazia**: segura o token por `token_time` e agenda `TIMER_FORWARD_TOKEN` (com a época atual); ao disparar, `_on_timer_forward_token` reavalia a fila: se uma mensagem foi enfileirada **durante** a janela de retenção (intervalo de ritmo), os dados são transmitidos no próprio tique (`_send_data_packet(peek())`); caso contrário, `_forward_token` envia o token ao sucessor. Assim nenhuma mensagem é perdida e o token nunca fica preso por ter sido enfileirado um envio enquanto o nó o segurava.
- **Fila não vazia**: chama `_send_data_packet(head)` e **segura o token** até os dados voltarem (não encaminha agora).

### Emissão de DADOS (`_send_data_packet`)
Calcula o CRC da mensagem original, aplica `maybe_corrupt` (respeitando `skip`), monta o DATA com controle `maquinainexistente`, marca `waiting_for_data_return`, **pausa o monitor** se for controladora, e envia ao sucessor.

### Tratamento na origem (`_on_rx_data`, ramo `o == self.apelido`)
O datagrama deu a volta. Conforme o controle:

- **BROADCAST**: imprime "BROADCAST concluído", remove da fila (`_drop_head`) e libera o token;
- **ACK**: "mensagem entregue com sucesso", remove da fila e libera;
- **maquinainexistente** (CTRL_NONE): destino ausente/desligado, remove da fila e libera;
- **NAK**: se o item ainda não foi retransmitido, marca `retransmit_used=True` e `no_error=True` (reenvio **sem injeção** na próxima passagem do token) e libera; se já houve a retransmissão única, descarta a mensagem e libera.

Liberar significa `waiting_for_data_return = False`, despausar o monitor (se controladora) e `_forward_token()`.

### Tratamento no destino (`_on_rx_data`, ramo `d == self.apelido`)
Recalcula o CRC (`crc_mod.matches`), define `ACK`/`NAK`, **imprime origem + mensagem**, e repassa ao sucessor com `set_controle(raw, novo)` (só o campo de controle muda).

### Intermediários
Quem não é origem nem destino (e não é broadcast) **repassa verbatim** ao sucessor (`_send_to_successor(raw)`); o broadcast também é repassado verbatim, mas **todos imprimem**. Apenas o destino endereçado altera o controle.

Trecho real (origem segura token, envia, recebe ACK, libera — `tests/log_B.txt`):

```
[B] enviando DADOS para A (corrompido=False): "Ola A"
[B] mensagem entregue com sucesso (ACK)
[B] enviando token para C
```

---

## 12. Controle do token

### Token perdido (timeout)
O `TokenMonitor` roda **só na controladora**. O `set_enabled` é ligado quando o nó **é controladora E** (`first_token_generated` **OU** `observed_activity`) — ver `_recompute_role`. A condição `observed_activity` cobre um caso real: uma máquina que **se torna controladora ao entrar tarde** (apelido menor que os já presentes) tem `first_token_generated == False`, mas, como já viu token/dados circulando, **também passa a vigiar** o token perdido. Trecho real (`tests/log_latejoin_A.txt`), em que A entra depois, vira controladora e detecta o token sumido:

```
[A] TOKEN PERDIDO detectado (timeout) -> gerando novo token
```

O relógio de liveness é **resetado por qualquer atividade observada no anel**: a própria thread receptora chama `monitor.note_activity()` para **todo datagrama de token ou dados** que vê (em `on_datagram`, antes mesmo de o motor processar), e o motor também o faz a cada RX/envio/encaminhamento. Assim, um token saudável que está **parado/circulando em nós a jusante** mantém o relógio vivo e **não** é declarado perdido.

Além disso, o monitor é **pausado** sempre que o token está comprovadamente na controladora. O helper `_refresh_monitor_pause()` aplica `set_paused(self.has_token or self.waiting_for_data_return)` e é chamado logo após qualquer mudança de `has_token` ou `waiting_for_data_return`. Ou seja, a vigilância fica suspensa tanto enquanto a controladora **detém o token** (`has_token`) quanto durante o **round-trip dos seus próprios dados** (`waiting_for_data_return`), e só retoma quando o token de fato saiu daqui. Se o silêncio passar de `token_timeout`, o monitor posta `MON_TOKEN_TIMEOUT`; `_on_mon_token_timeout` regenera o token **somente se** há mais de um membro (`len(members) > 1`) e não há transmissão de dados em curso (`not waiting_for_data_return`).

Em conjunto, essas duas peças (relógio resetado por atividade + pausa enquanto o token está aqui) fazem a controladora regenerar um token **apenas quando ele está genuinamente perdido**, eliminando o falso "token perdido" durante a circulação normal ou com o token estacionado em outro nó.

### Token duplicado
Detectado **no nó**, em `_on_rx_token` (controladora). A detecção é **temporal (heurística)**: se o intervalo entre dois tokens consecutivos for menor que `min_token_interval`, conclui-se que há mais de um token circulando. O nó **consome o token excedente** (retorna sem encaminhar) e **atualiza `last_token_rx`** com o instante atual, para medir corretamente o intervalo até o próximo token genuíno. Esse caminho **não incrementa `epoch`**: o token real já está aqui e possui um `TIMER_FORWARD_TOKEN` pendente com a época atual; bumpar `epoch` cancelaria esse timer e mataria o anel. Consumir silenciosamente o excedente é suficiente e preserva a circulação. A **primeira volta** logo após uma (re)geração do token é isentada por `expect_token_return` (o primeiro retorno não conta como duplicata); além disso, `_generate_token` **reseta `last_token_rx`** no instante da geração, criando uma baseline fresca para que um timestamp antigo não julgue mal a chegada do próximo token. Como o método é heurístico, recomenda-se configurar `tempo_minimo_entre_tokens` entre **~metade** e **a volta inteira** do anel: assim um único token (que retorna a cada ~volta) nunca é flagrado, enquanto dois tokens co-circulando (que chegam ~meia-volta apart) são.

### Geração e retirada manuais
`gentoken`/`addtoken` -> `_on_cmd_add_token` -> `_generate_token` (incrementa época, marca `first_token_generated`, envia token ao sucessor) — funciona em **qualquer máquina**. `removetoken`/`rmtoken` -> `_on_cmd_remove_token`: **só remove quando o nó realmente detém o token** — nesse caso larga-o (`has_token = False`), incrementa `epoch` e atualiza a pausa do monitor; se o nó **não** está com o token, nada é feito (loga "nada a retirar") e `epoch` **não** é tocado, pois bumpar a época sem ter o token invalidaria timers de encaminhamento legítimos pendentes em outros nós.

### Handoff de controlador
Quando a topologia muda, `_recompute_role` reavalia `is_controller` (menor apelido) e liga/desliga o monitor de acordo, sem reinicialização.

Trechos reais (`tests/log_A.txt`): a controladora A larga o token com `removetoken` e, esgotado o `token_timeout` sem atividade, detecta o silêncio e regenera:

```
[A] token retirado da rede
[A] TOKEN PERDIDO detectado (timeout) -> gerando novo token
[A] gerou/inseriu um token na rede
```

(O monitor só atua em A porque A é a controladora — menor apelido do anel `['A','B','C']`. A tentativa de `removetoken` num nó que não está com o token apenas loga "nao estou com o token no momento (nada a retirar)", como em `tests/log_C.txt`.)

---

## 13. Unicast e broadcast

- **Unicast**: `send <apelido> <mensagem>` enfileira com destino específico. Ao circular, intermediários repassam verbatim; o destino confere CRC e marca ACK/NAK; a origem trata o retorno (Seção 11).
- **Broadcast**: `send BROADCAST <mensagem>`. Regras (conforme o enunciado e o código): o módulo de falhas **mantém** `maquinainexistente` (não injeta erro: `skip` por `is_bcast`); **todos imprimem** a mensagem (`[X] BROADCAST de O: "..."`); o pacote **retorna à origem**, que loga "BROADCAST concluído" e libera o token.

Trecho real (broadcast em todas as máquinas — `tests/log_A.txt`, `tests/log_B.txt`, `tests/log_C.txt`):

```
[A] BROADCAST de C: "Ola galera"
[B] BROADCAST de C: "Ola galera"
[C] enviando DADOS para BROADCAST (corrompido=False): "Ola galera"
[C] BROADCAST concluido (deu a volta no anel)
```

---

## 14. Alteração topológica do anel

Máquinas podem **entrar em execução**: ao subir (ou via comando `join`/`discover`), enviam DISCOVER; as demais respondem HELLO. `_update_member` chama `Ring.update` e `_recompute_role`, recomputando **sucessor** e **controladora** a partir do novo conjunto, **sem reinicialização**. Se um apelido reaparece com endereço diferente, `Ring.update` retorna `"changed"` e o nó avisa ("mudou de endereço").

Além da entrada, uma máquina que **sai limpa** (comando `quit`) agora difunde um **LEAVE** em broadcast (em `_shutdown`, com o socket ainda aberto). Quem recebe (`_on_rx_leave`) chama `Ring.remove`, recomputa sucessor/controladora (`_recompute_role`) e o anel **se cura sozinho** entre os nossos nós — por exemplo, `A->B->C->A` passa a `A->C->A`. Trecho real (`tests/log_leave_A.txt`), quando B sai e A reconstrói o anel:

```
[A] B saiu da rede -> recalculando anel
```

Limitação declarada com franqueza: um **CRASH abrupto** (queda sem enviar LEAVE) **não** é curado automaticamente. A expulsão unilateral por timeout foi **deliberadamente evitada**: num anel compartilhado com outros grupos, cada nó perceberia silêncios diferentes e removeria membros distintos, **divergindo a topologia** entre as máquinas; a recuperação de um crash é, portanto, **re-sincronização manual** (subir o nó de novo / `join`). Pela mesma razão de anel compartilhado, o "portão" de entrada do enunciado ("nova máquina só entra quando somente o token estiver circulando") é tratado como **disciplina do operador**, não imposto pelo código: a topologia é reavaliada de forma incremental a cada DISCOVER/HELLO.

**Caso degenerado (uma única máquina).** Se as demais máquinas saem e resta **apenas uma** no anel, ela **não** envia o token para si mesma: segura o token **em silêncio** (loga uma vez "sou a unica maquina no anel; segurando o token ate outra entrar") e volta a circulá-lo assim que outra máquina entra (DISCOVER/HELLO), logando "nova maquina entrou; retomando a circulacao do token". O enunciado exige no mínimo 3 máquinas, então é um caso degenerado fora do cenário pedido, tratado de forma limpa apenas para evitar um loop inútil do token contra o próprio endereço.

---

## 15. Arquivo de configuração

Cinco linhas, uma informação por linha (`ring/config.py`):

```
<apelido>
<tempo_token_e_dados>
<probabilidade_de_erro>
<timeout_do_token>
<tempo_minimo_entre_tokens>
```

Exemplo (`config.example.txt`):

```
A
2
20
2,5
2
```

Os números aceitam **vírgula decimal** (`2,5` = 2.5). São usadas as 5 primeiras linhas não vazias; menos que isso gera `ValueError`.

### Ajuste dos dois timers de controle

`timeout_do_token` (linha 4) e `tempo_minimo_entre_tokens` (linha 5) são **relógios locais de cada máquina**, não viajam no fio: cada grupo afina os seus. A volta do anel ("lap") dura aproximadamente `numero_de_maquinas x tempo_token_e_dados`.

- **`timeout_do_token`** deve ser **maior que a volta**, senão um token saudável estacionado/circulando em nós a jusante seria declarado perdido. Exemplo: 3 máquinas, `tempo_token_e_dados=1` -> volta ~3 s -> use `timeout >= ~8 a 10`. (É o valor das execuções de teste: `token_timeout=10`.)
- **`tempo_minimo_entre_tokens`** deve ficar **abaixo da volta** mas **acima de ~metade dela** para demonstrar a detecção de duplicata: um único token retorna a cada ~volta (intervalo acima do mínimo, seguro), enquanto dois tokens co-circulando chegam ~meia-volta apart (abaixo do mínimo, flagrados). Exemplo: 3 máquinas, `tempo_token_e_dados=1`, volta ~3 s -> use `min ~2`.

> O `config.example.txt` acima usa valores ilustrativos curtos; para os cenários de controle prefira `timeout_do_token=10` e (para exibir duplicata) `tempo_minimo_entre_tokens=2` com `tempo_token_e_dados=1` em 3 máquinas, exatamente como nos logs de `tests/`.

---

## 16. Interface e comandos do usuário

O `Console` (thread principal) traduz comandos em eventos:

| Comando | Evento | Efeito |
|---|---|---|
| `send <destino> <msg...>` | `CMD_SEND` | Enfileira mensagem (destino = apelido ou `BROADCAST`). |
| `gentoken` / `addtoken` | `CMD_ADD_TOKEN` | Gera/insere um token na rede. |
| `removetoken` / `rmtoken` | `CMD_REMOVE_TOKEN` | Retira o token da rede. |
| `status` | `CMD_STATUS` | Mostra endereço, papel, posse do token, anel, sucessor, fila, contadores `tokens_perdidos`/`tokens_duplicados`, etc. |
| `queue` / `fila` | `CMD_QUEUE` | Lista a fila (destino, mensagem, flags `no_error`/`retransmit_used`). |
| `join` / `discover` | `CMD_JOIN` | Reenvia DISCOVER (atualiza topologia). |
| `help` / `?` | — | Ajuda. |
| `quit` / `exit` / `sair` | `CMD_QUIT` | Encerra. |

Atendimento ao requisito "saber o que está acontecendo no anel": cada ação relevante é logada com o prefixo `[apelido]` — recepção/envio de token ("recebeu o token", "enviando token para X"), envio de dados (com flag `corrompido=...`), entrega (ACK), NAK/CRC, retransmissão, broadcast, detecção de perdido/duplicado. `status` mostra **onde está o token** (`com_token`) e se há dados em trânsito (`aguardando_retorno`), e `queue` mostra **onde estão os dados** pendentes. O `status` também reporta os contadores `tokens_perdidos` e `tokens_duplicados`, respondendo diretamente ao item do enunciado "saber se houve token perdido ou se há mais de um token".

Trecho real de `status` (`tests/log_A.txt`):

```
[A] STATUS
  endereco: 127.0.0.1:6001  modo: local
  controladora: True  com_token: False  aguardando_retorno: False
  anel: ['A', 'B', 'C']
  sucessor: B  fila: 0  primeiro_token_gerado: True
  tokens_perdidos: 0  tokens_duplicados: 0
```

---

## 17. Como executar

Forma geral (a partir do diretório base):

```
python main.py <config> [--port N] [--peers arq] [--ip IP] [--discovery S]
```

### Modo LAN (máquinas reais)
Sem `--peers`. Porta fixa **6000**, IP anunciado autodetectado (ou `--ip`). Em cada máquina:

```
python main.py config.example.txt
```

### Modo local de teste (várias instâncias no mesmo host)
Com `--peers` (ativa o modo local); cada instância escuta em `--port` e conhece as demais pelo arquivo de peers. Exemplo de três nós (igual ao `tests/driver.py`):

```
python main.py tests/config_A.txt --peers tests/peers.txt --port 6001 --ip 127.0.0.1 --discovery 6
python main.py tests/config_B.txt --peers tests/peers.txt --port 6002 --ip 127.0.0.1 --discovery 6
python main.py tests/config_C.txt --peers tests/peers.txt --port 6003 --ip 127.0.0.1 --discovery 6
```

`tests/peers.txt`:

```
A 127.0.0.1 6001
B 127.0.0.1 6002
C 127.0.0.1 6003
```

`--discovery` é a janela (s) antes de avaliar a criação do token inicial.

---

## 18. Exemplos de execução

Logs reais capturados pelo harness `tests/driver.py` (3 nós: A com `error_prob=100`, B e C com `0`).

### 18.1 Formação do anel
`tests/log_A.txt`:

```
[A] DISCOVER de B, respondendo HELLO
[A] DISCOVER de C, respondendo HELLO
[A] sou a primeira maquina (menor apelido) e nenhum token observado -> gerando token inicial
```

### 18.2 Geração e circulação do token
`tests/log_A.txt`:

```
[A] gerou/inseriu um token na rede
[A] recebeu o token
[A] enviando token para B
```

e o token seguindo o anel A -> B -> C -> A (`tests/log_B.txt`, `tests/log_C.txt`):

```
[B] recebeu o token
[B] enviando token para C
[C] recebeu o token
[C] enviando token para A
```

### 18.3 Entrega unicast com ACK (B -> A)
`tests/log_B.txt` (origem) e `tests/log_A.txt` (destino):

```
[B] enviando DADOS para A (corrompido=False): "Ola A"
[B] mensagem entregue com sucesso (ACK)
[A] DADOS de B: "Ola A" -> ACK
```

### 18.4 NAK + retransmissão + recuperação (A -> C)
A corrompe a mensagem ("Teste erro" -> "Tes?e erro"); C detecta CRC errado e responde NAK; A retransmite a mensagem correta na próxima passagem e obtém ACK.

`tests/log_C.txt`:

```
[C] CRC NAO confere de A (recebido=2097710523 calculado=2704260576) -> NAK
[C] DADOS de A: "Tes?e erro" -> NAK
...
[C] DADOS de A: "Teste erro" -> ACK
```

`tests/log_A.txt`:

```
[A] enviando DADOS para C (corrompido=True): "Teste erro"
[A] RETRANSMISSAO: NAK recebido de C, reenviando msg correta na proxima passagem do token
...
[A] enviando DADOS para C (corrompido=False): "Teste erro"
[A] mensagem entregue com sucesso (ACK)
```

### 18.5 Broadcast em todas as máquinas (C -> todos)
`tests/log_C.txt` (origem), `tests/log_A.txt` e `tests/log_B.txt` (recepção):

```
[C] enviando DADOS para BROADCAST (corrompido=False): "Ola galera"
[C] BROADCAST concluido (deu a volta no anel)
[A] BROADCAST de C: "Ola galera"
[B] BROADCAST de C: "Ola galera"
```

### 18.6 Detecção de token perdido + regeneração
A controladora A larga o token com `removetoken`; sem nenhuma atividade no anel, o `token_timeout` (10 s nesta execução) expira e A detecta o silêncio e regenera. Como o relógio do monitor é resetado por qualquer token/dados observado, isso só dispara porque o token foi de fato retirado.

`tests/log_A.txt`:

```
[A] token retirado da rede
[A] TOKEN PERDIDO detectado (timeout) -> gerando novo token
[A] gerou/inseriu um token na rede
```

### 18.7 Detecção de token duplicado (sem matar o anel)
Execução dedicada com `min_token_interval=2,0` (acima de meia-volta e abaixo da volta ~3 s; `tests/log_dup_*.txt`). Com um token saudável dando a volta a cada ~3 s o intervalo medido fica **acima** do mínimo e **nada** é sinalizado. Quando B (não-controladora) injeta um segundo token com `gentoken`, passam a co-circular dois tokens que chegam a A com ~1,5 s de diferença (~meia-volta): A flagra o excedente e o consome, sem incrementar época.

B injeta o token duplicado (`tests/log_dup_B.txt`):

```
[B] gerou/inseriu um token na rede
```

A (controladora) detecta e remove o excedente (`tests/log_dup_A.txt`):

```
[A] TOKEN DUPLICADO detectado (intervalo < minimo) -> removido da rede
```

E o anel **segue circulando** depois do evento — o próprio B continua recebendo e repassando o token (`tests/log_dup_B.txt`):

```
[B] recebeu o token
[B] enviando token para C
```

### 18.8 Saída educada e cura do anel (B sai, A->C)
B encerra com `quit` e difunde LEAVE; A o remove e recompõe o anel `['A','B','C']` para `['A','C']`, passando a encaminhar o token direto para C (`tests/log_leave_A.txt`):

```
[A] B saiu da rede -> recalculando anel
[A] enviando token para C
```

### 18.9 Controladora que entra tarde vigia o token perdido
A entra depois de B e C já circularem o token; por ter o menor apelido, torna-se controladora (`first_token_generated` ainda `False`, mas `observed_activity` `True`). Quando o token some, A detecta o timeout e regenera (`tests/log_latejoin_A.txt`):

```
[A] TOKEN PERDIDO detectado (timeout) -> gerando novo token
[A] gerou/inseriu um token na rede
```

---

## 19. Mapeamento requisito -> implementação

| Requisito (enunciado) | Onde é atendido |
|---|---|
| Rede em anel sobre UDP | `ring/network/transport.py` (UDP) + `ring/network/discovery.py` (anel) |
| Fila por máquina, 1 mensagem por vez | `MessageQueue`; `_on_rx_token` só envia o `peek()` |
| Token circulando | `TokenLogicMixin` (`_on_rx_token`, `_forward_token`) |
| Topologia por lista ordenada, sucessor circular | `Ring.order` / `Ring.successor` |
| 3 tipos de pacote (controle/token/dados) + DISCOVER/HELLO | `ring/protocol/packets.py` |
| Arquivo de config (5 linhas, vírgula decimal) | `ring/config.py` |
| DISCOVER broadcast porta 6000 + resposta HELLO | `Node.start`, `_on_rx_discover`; LAN usa 6000 |
| Anel em ordem alfabética; primeira máquina gera token | `Ring.order` (apelido); `_on_eval_first_token` (menor apelido) |
| Token vazio -> repassa; com fila -> envia dados e segura até voltar | `_on_rx_token`, `_send_data_packet`, `_on_rx_data` |
| `maquinainexistente`/ACK/NAK na origem | `_on_rx_data` (ramo origem) |
| Recalcular CRC no destino, imprimir origem+mensagem, marcar ACK/NAK | `_on_rx_data` (ramo destino) + `set_controle` |
| CRC32 como controle de erro | `ring/protocol/crc.py` (zlib) |
| Módulo de inserção de falhas com probabilidade do config | `ring/protocol/fault.py` + `_send_data_packet` |
| Fila até 10, com destino por mensagem | `MessageQueue.MAX`, `QueueItem.destino` |
| Unicast e Broadcast (broadcast mantém `maquinainexistente`) | `send`; `_send_data_packet` (`skip=is_bcast`), `_on_rx_data` |
| **Deve ser possível**: especificar mensagem a qualquer momento | `send` -> `CMD_SEND` -> `_on_cmd_send` |
| **Deve ser possível**: retirar token a qualquer momento | `removetoken` -> `_on_cmd_remove_token` |
| **Deve ser possível**: incluir token a qualquer momento | `gentoken` -> `_on_cmd_add_token` -> `_generate_token` |
| **Deve ser possível**: visualizar onde estão token e dados | `status` (`com_token`, `aguardando_retorno`), `queue`; logs `[ap] ...` |
| **Deve ser possível**: avisar retransmissões | log `RETRANSMISSAO` em `_on_rx_data` |
| **Deve ser possível**: saber o que cada máquina faz | `log()` por ação, prefixo `[apelido]` |
| **Deve ser possível**: detectar token perdido | `TokenMonitor` + `_on_mon_token_timeout` |
| **Deve ser possível**: detectar token duplicado | `_on_rx_token` (intervalo < `min_token_interval`) |
| **Deve ser possível**: detecção de falhas e recuperação | CRC no destino (NAK) + retransmissão única em `_on_rx_data` |
| **Deve ser possível**: inclusão de máquinas a qualquer momento | `_on_rx_discover`/`_on_rx_hello` + `_recompute_role`; `join` |
| Demonstração em >= 3 máquinas | `tests/driver.py` + logs A/B/C (anel `['A','B','C']`) |
| Interoperabilidade (formato fiel) | `ring/protocol/packets.py` (ASCII + `:`); CRC zlib |

---

## 20. Premissas e limitações de interoperabilidade

- **CRC**: convenção adotada = escopo **somente a mensagem** e representação **decimal** (string). Precisa ser confirmada com os outros grupos (o enunciado não fixa o escopo nem a base; o exemplo `19385749` é ilustrativo e não bate com o CRC-32 real de "Oi pessoal!", que é `1751094473`).
- **IPv4** apenas (`socket.AF_INET`); `detect_ip` usa o truque do `connect` a `8.8.8.8` para achar o IP de saída.
- **Apelidos únicos**: o anel é indexado por apelido; colisão é tratada como troca de endereço (`Ring.update -> "changed"`), não como dois nós distintos. Em particular, uma colisão de apelido entre grupos **sobrescreve** a entrada no mapa de endereços — o enunciado pressupõe apelidos únicos (`A`, `B`, `C`, ...).
- **Saída e crash**: a **saída educada** (`quit`) é tratada — o nó difunde LEAVE e os demais curam o anel (`Ring.remove` + `_recompute_role`). Já um **crash abrupto** (sem LEAVE) **não** é curado automaticamente: a expulsão unilateral por timeout foi evitada de propósito porque, num anel compartilhado com outros grupos, divergiria a topologia entre os nós; a recuperação de crash é **manual** (re-subir / `join`). O LEAVE (`30:apelido:ip`) é **extensão local** e não trafega para outros grupos como pacote conhecido (eles o ignoram).
- **Portão de entrada não imposto**: o "nova máquina só entra quando somente o token estiver circulando" é **disciplina do operador**, não verificado pelo código, pela mesma razão de anel compartilhado — a topologia é reavaliada incrementalmente a cada DISCOVER/HELLO.
- **Detecção de duplicata é heurística**: baseia-se em tempo (`intervalo < tempo_minimo_entre_tokens`), logo depende do dimensionamento correto desse parâmetro pela volta do anel (ver Seção 15).
- **Timers de controle são locais e devem ser dimensionados pela volta do anel** (ver Seção 15): `timeout_do_token` **maior** que a volta (~`numero_de_maquinas x tempo_token_e_dados`) para não declarar perdido um token saudável parado a jusante; `tempo_minimo_entre_tokens` **abaixo** da volta porém **acima de ~metade dela** para flagrar duplicata sem alarme falso no token único. Não viajam no fio, então cada grupo ajusta os seus.
- **Janela de descoberta**: o token inicial só é avaliado após `--discovery` segundos; subir as máquinas com defasagem grande demais pode adiar a formação completa antes da eleição.
- **Segundo NAK**: o enunciado diz "retransmite apenas uma vez". A implementação interpreta isso como: após a retransmissão única, um novo NAK faz a mensagem ser **descartada** (`retransmit_used` já `True`), liberando o token. Comportamento documentado no log ("NAK novamente apos retransmissao unica -> descartando msg").
- **Broadcast e modo local**: no modo local o broadcast é simulado por cópia para cada peer do arquivo; no modo LAN usa `255.255.255.255:6000` com `SO_BROADCAST`.
- **WinError 10054** (ICMP port-unreachable no Windows ao enviar para porta fechada) é ignorado no laço receptor, evitando derrubar a thread rx quando um peer ainda não subiu.

---

## 21. Conclusão

A solução implementa uma rede em anel com passagem de token sobre UDP cobrindo os requisitos do enunciado: formação automática do anel (DISCOVER/HELLO), circulação do token, fila de até 10 mensagens com destino por item, entrega unicast com ACK/NAK por CRC-32, retransmissão única, broadcast, controle de token perdido (timeout baseado em atividade) e duplicado (intervalo mínimo), geração/retirada manual, e alteração topológica em execução.

O ponto de projeto que sustenta a corretude e a simplicidade é o **modelo de thread única de motor sobre um barramento de eventos**: todas as threads externas apenas postam eventos, e o `Node` é o único a mutar o estado, serializado pelo `queue.Queue`. Isso elimina locks sobre o estado, deixando a sincronização restrita ao barramento, ao contador `epoch` (que invalida timers obsoletos) e ao lock de impressão. As camadas (`protocol`, `core`, `network`, `ui`) isolam responsabilidades, e a camada de protocolo concentra o contrato de fio necessário à interoperabilidade entre grupos. Os logs reais em `tests/log_A.txt`, `tests/log_B.txt` e `tests/log_C.txt` demonstram o funcionamento sobre três máquinas.

---

### Apêndice — discrepâncias entre a descrição de referência e o código

A implementação confere com a descrição pedida. Ajustes de precisão observados ao ler o código:

1. **Ordenação do anel**: `Ring.order` ordena por `(apelido, ip)`, não só por apelido. O apelido é a chave primária (ordem alfabética, como pede o enunciado); o ip é apenas desempate determinístico para apelidos coincidentes.
2. **Geração do token inicial**: o enunciado diz "a primeira máquina (A) gera o token". O código generaliza para o **menor apelido** (controladora), e a geração é decidida em `EVAL_FIRST_TOKEN`, protegida por `observed_activity` (só gera se nenhum token/dado foi visto na janela de descoberta).
3. **Regeneração por timeout**: além de `not waiting_for_data_return`, exige `len(members) > 1` (não regenera token num anel de um só nó).
4. **Token duplicado nos logs**: a detecção vive em `_on_rx_token` e está capturada em execução dedicada com `min_token_interval=2,0` (`tests/log_dup_*.txt`), onde B injeta um segundo token com `gentoken` e A o consome sem matar o anel. O caminho consome o token excedente **sem** incrementar `epoch` (ver Seção 12), justamente para não cancelar o `TIMER_FORWARD_TOKEN` do token genuíno.
