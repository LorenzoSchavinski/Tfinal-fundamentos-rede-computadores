SIMULAÇÃO DE REDE EM ANEL SOBRE UDP

REQUISITOS

- Python 3.8 ou superior.
- Mesma rede local para todas as máquinas.
- Porta UDP 6000 liberada.
- Nenhuma biblioteca externa é necessária.

CONFIGURAÇÃO

Cada máquina deve possuir um arquivo de configuração com cinco linhas:

<apelido>
<tempo_token_e_dados>
<probabilidade_de_erro>
<timeout_do_token>
<tempo_minimo_entre_tokens>

Os apelidos devem ser únicos.

EXECUÇÃO

Na pasta do projeto, execute:

python main.py config.example.txt

Cada máquina deve executar uma instância com seu próprio arquivo de configuração e apelido.

COMANDOS

send B mensagem
    Enfileira uma mensagem para a máquina B.

send BROADCAST mensagem
    Enfileira uma mensagem para todas as máquinas.

gentoken
    Insere um token adicional na rede.

removetoken
    Retira o token atual ou o próximo recebido.

status
    Mostra a topologia e o estado da máquina.

queue
    Mostra a fila de mensagens.

join
    Reenvia DISCOVER.

help
    Exibe os comandos disponíveis.

quit
    Encerra a aplicação.

OBSERVAÇÕES

- A aplicação utiliza UDP na porta 6000.
- DISCOVER e HELLO são enviados em broadcast.
- Token e dados são enviados para a próxima máquina do anel.
- O anel é ordenado alfabeticamente pelos apelidos.