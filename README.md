# Anel de Tokens sobre UDP

Simulacao de uma rede em anel com passagem de token (token ring) sobre UDP, em Python puro.

## Requisitos

Python 3.8+. Somente biblioteca padrao, sem dependencias externas.

## Formato do arquivo de configuracao

O arquivo tem exatamente 5 linhas uteis (linhas em branco sao ignoradas; o parser usa as 5 primeiras linhas nao vazias). Uma informacao por linha, nesta ordem:

| Linha | Campo                      | Significado                                                  |
|-------|----------------------------|-------------------------------------------------------------|
| 1     | apelido                    | nome da maquina no anel (string, ex.: `A`)                  |
| 2     | tempo_token_e_dados        | tempo segurando o token / enviando dados, em segundos       |
| 3     | probabilidade_erro_percent | chance de injetar erro de CRC, em percentual (ex.: `20`)    |
| 4     | timeout_token              | tempo ate considerar o token perdido, em segundos           |
| 5     | tempo_minimo_entre_tokens  | intervalo minimo entre tokens, em segundos                  |

Os numeros podem usar virgula decimal (ex.: `2,5` equivale a `2.5`).

Exemplo (`config.example.txt`):

```
A
2
20
2,5
2
```

## Como executar na rede local (modo de apresentacao / LAN)

```
python main.py config.txt
```

Neste modo:

- escuta na porta fixa **6000 UDP**;
- descobre o proprio IP automaticamente (ou use `--ip <ip>` para forcar);
- envia `DISCOVER` em broadcast (`255.255.255.255:6000`) para montar a topologia.

Cada aluno roda uma instancia em sua maquina. A maquina de **menor apelido** (ordem alfabetica) e a controladora e gera o primeiro token. Inicie as maquinas com poucos segundos de diferenca para que todas se descubram antes da avaliacao do token inicial; ajuste a janela de descoberta com `--discovery <segundos>` se necessario (padrao 3,0 s).

## Como executar varias instancias na mesma maquina (modo local de teste)

Para testar sozinho, ative o modo local com `--peers` (tabela apelido/ip/porta) e dê uma porta distinta a cada instancia com `--port`. No modo local o broadcast e simulado enviando uma copia para cada par da tabela, entao todas as instancias devem constar no arquivo de peers (incluindo a propria).

Arquivo `peers.example.txt`:

```
A 127.0.0.1 6001
B 127.0.0.1 6002
C 127.0.0.1 6003
```

Abra 3 terminais, um por no, cada um com sua config (apelido `A`, `B`, `C`) e sua porta:

```
# terminal 1
python main.py config_a.txt --peers peers.example.txt --port 6001 --ip 127.0.0.1

# terminal 2
python main.py config_b.txt --peers peers.example.txt --port 6002 --ip 127.0.0.1

# terminal 3
python main.py config_c.txt --peers peers.example.txt --port 6003 --ip 127.0.0.1
```

Isso e apenas para teste local. Na apresentacao real todos usam a porta 6000 e o modo LAN (sem `--peers`).

## Comandos interativos

Apos iniciar, o no le comandos do teclado:

- `send <destino> <mensagem>` — envia mensagem; `<destino>` e um apelido ou `BROADCAST`.
- `gentoken` — gera/insere um token na rede.
- `removetoken` — retira o token da rede.
- `status` — mostra o estado atual do no.
- `queue` — lista a fila de mensagens.
- `join` — reenvia `DISCOVER` para atualizar a topologia.
- `help` — mostra a ajuda dos comandos.
- `quit` — encerra o no.

## Demonstrando o controle de token

Os dois timers do controle de token (`timeout_token` na linha 4 e `tempo_minimo_entre_tokens` na linha 5 do config) sao locais a cada maquina. Dimensione-os pela volta do anel, que dura ~`numero_de_maquinas x tempo_token_e_dados`. Para 3 maquinas com `tempo_token_e_dados=1` (volta ~3 s) os valores praticos sao: `timeout_token=10` (sempre maior que a volta, senao um token saudavel parado em outro no e declarado perdido) e, para exibir duplicata, `tempo_minimo_entre_tokens=2` (abaixo da volta e acima de ~metade dela).

**Token perdido (regeneracao).** Com o anel circulando, use `removetoken` na maquina que esta com o token (em geral a controladora, de menor apelido) para tirar o unico token da rede. Como nao ha mais atividade no anel, o relogio do monitor da controladora estoura ao fim de `timeout_token` e ela regenera:

```
[A] token retirado da rede
[A] TOKEN PERDIDO detectado (timeout) -> gerando novo token
[A] gerou/inseriu um token na rede
```

**Token duplicado (deteccao).** Use `tempo_minimo_entre_tokens=2` (com `tempo_token_e_dados=1`, 3 maquinas). Com um unico token a controladora nao acusa nada (ele volta a cada ~3 s). De `gentoken` numa maquina que **nao** e a controladora para injetar um segundo token: agora dois tokens co-circulam e chegam a controladora com ~1,5 s de diferenca, abaixo do minimo, e ela os marca como duplicados, consumindo o excedente sem matar o anel (que segue circulando):

```
[A] TOKEN DUPLICADO detectado (intervalo < minimo) -> removido da rede
```

**Saida de um no (cura reativa do anel).** `quit` encerra o no silenciosamente, sem avisar os outros. A remocao acontece de forma reativa: quando alguem envia um DATA para o no ausente, o pacote retorna marcado `maquinainexistente`; o remetente imprime um aviso, descarta a mensagem da fila e remove o no ausente do anel (ex.: `A->B->C->A` vira `A->C->A`). Para ver a remocao acontecer numa demo, envie uma mensagem para o no que acabou de sair.

Se sobrar so uma maquina no anel, ela segura o token quieto (sem enviar a si mesma) e retoma a circulacao quando outra entrar.

**Timeout de dados (pacote perdido).** Se o remetente nao recebe seu proprio pacote de volta dentro do `timeout_token`, ele descarta a mensagem da fila e passa o token adiante, evitando que um unico pacote perdido congele o anel.

**Wire log.** Todo pacote DATA enviado ou recebido, e qualquer pacote desconhecido/corrompido recebido, e impresso com os bytes crus no formato `[wire TX -> ip:porta] 2000:...` / `[wire RX <- ip:porta] ...`. Util para depuracao e para provar interoperabilidade na apresentacao. Nao requer nenhum comando; e automatico.

**Normalizacao de apelidos.** Os apelidos sao convertidos para MAIUSCULAS e com espacos removidos antes de qualquer comparacao, entao uma letra minuscula ou espaco extra nao quebra a entrega. O texto das mensagens e mantido exatamente como digitado. Para a apresentacao: combinem com os outros grupos usar apelidos de uma unica letra maiuscula (A, B, C...).

**Inicializacao robusta.** O DISCOVER e reemitido varias vezes durante a janela de descoberta, e o primeiro token so e gerado apos a lista de membros estabilizar. Isso evita que varios nos gerem tokens simultaneamente num start escalonado. O primeiro token pode levar alguns segundos a mais para aparecer; e normal.

O `status` tambem mostra os contadores `tokens_perdidos` e `tokens_duplicados`, respondendo se houve token perdido ou mais de um token na rede. Uma maquina que vira controladora ao entrar atrasada (menor apelido entrando depois que o anel ja roda) tambem passa a vigiar token perdido, pois o monitor liga via `observed_activity`; portanto e possivel adicionar a maquina de menor apelido por ultimo sem perder a deteccao de token perdido.

## Contrato de interoperabilidade

Para a apresentacao com outros grupos, todas as maquinas devem falar o mesmo formato no fio.

- **Porta:** 6000 UDP.
- **Cabecalho:** campos em texto ASCII separados por `:` (dois-pontos). O prefixo numerico antes do primeiro `:` define o tipo do pacote.

Formatos exatos:

| Tipo     | Prefixo | Formato                                                  | Exemplo                                          |
|----------|---------|----------------------------------------------------------|--------------------------------------------------|
| DISCOVER | `10`    | `10:<apelido>:<ip>`                                       | `10:A:192.168.0.10`                              |
| HELLO    | `20`    | `20:<apelido>:<ip>`                                       | `20:A:192.168.0.10`                              |
| TOKEN    | `1000`  | `1000` (sem campos)                                       | `1000`                                           |
| DADOS    | `2000`  | `2000:<origem>:<destino>:<controle>:<crc>:<mensagem>`    | `2000:B:A:maquinainexistente:1751094473:Oi pessoal!` |

- **Controle (campo `<controle>` de DADOS):** um de `maquinainexistente`, `ACK` ou `NAK`. O valor inicial e `maquinainexistente` (origem ainda nao sabe se o destino existe); o destino devolve `ACK` (recebido sem erro de CRC) ou `NAK` (erro de CRC, pede retransmissao).
- **Destino especial:** `BROADCAST` entrega a mensagem a todos os nos do anel.
- **Mensagem:** vai em bytes crus no fim do datagrama, sem escape, podendo conter `:` a vontade (o parser limita as divisoes para nunca quebrar a mensagem).
O protocolo usa **somente** esses quatro tipos. Nao existe pacote LEAVE.

### CRC

`<crc>` e o **CRC-32 padrao (IEEE 802.3)**, identico ao `zlib.crc32` do Python e ao `java.util.zip.CRC32` do Java. E calculado **sobre os bytes do campo `<mensagem>`** e representado em **decimal** (inteiro sem sinal de 32 bits).

Atencao: o valor `19385749` que aparece no enunciado e apenas ilustrativo. O CRC real de `Oi pessoal!` e `1751094473`. Confirme a convencao de CRC (escopo = somente a mensagem; representacao decimal) com os outros grupos **antes** da demonstracao, pois divergencias nesse ponto quebram a interoperabilidade.

## Estrutura de pastas

```
Tfinal Fundamentos/
  main.py            ponto de entrada (argparse, escolha de modo lan/local)
  ring/
    config.py        leitura do arquivo de configuracao de 5 linhas
    protocol/        contrato de fio: formato dos pacotes, CRC e injecao de falha
    core/            logica do no: maquina de estados, token e fila de mensagens
    network/         transporte UDP (socket, broadcast) e topologia do anel
    ui/              log thread-safe e laco de comandos interativo (console)
  tests/             driver e arquivos de apoio para testes
```
