"""Topologia do anel: membros ativos, sucessor e controladora.

O anel eh definido de forma deterministica a partir do conjunto de membros
conhecidos. Cada membro eh identificado pelo apelido e guarda seu endereco
(ip, porta). A ordem do anel eh a ordenacao por (apelido, ip): assim todas as
maquinas, vendo o mesmo conjunto, calculam o MESMO sucessor para cada uma.

A controladora (responsavel por vigiar o token) eh, por convencao, a maquina de
menor apelido. Como a ordem eh total e igual em todos os nos, a eleicao eh
implicita: nao ha troca de mensagens para escolher quem controla.
"""
from __future__ import annotations


class Ring:
    """Conjunto de membros ativos e calculo de sucessor/controladora."""

    def __init__(self) -> None:
        # apelido -> (ip, porta)
        self._members = {}

    def update(self, apelido, ip, port) -> str:
        """Insere ou atualiza um membro.

        Retorna:
          - "new":     apelido ainda nao era conhecido;
          - "changed": apelido ja existia mas com (ip, porta) diferente
                       (possivel troca de IP ao reentrar, ou colisao de apelido);
          - "same":    ja existia com o mesmo endereco.
        """
        novo = (ip, port)
        atual = self._members.get(apelido)
        self._members[apelido] = novo
        if atual is None:
            return "new"
        if atual != novo:
            return "changed"
        return "same"

    def members(self) -> dict:
        """Copia do mapa apelido -> (ip, porta)."""
        return dict(self._members)

    def order(self) -> list:
        """Lista de (apelido, ip, porta) ordenada por (apelido, ip).

        A ordenacao por apelido define o sentido do anel; o ip eh apenas
        criterio de desempate determinístico caso dois apelidos coincidam.
        """
        return sorted(
            ((ap, ip, port) for ap, (ip, port) in self._members.items()),
            key=lambda e: (e[0], e[1]),
        )

    def successor(self, my_apelido):
        """Proximo membro no anel, de forma circular, a partir de ``my_apelido``.

        Com um unico membro, o sucessor eh ele mesmo. Retorna ``None`` se o
        apelido pedido nao estiver no anel.
        """
        ordem = self.order()
        for i, (ap, ip, port) in enumerate(ordem):
            if ap == my_apelido:
                prox = ordem[(i + 1) % len(ordem)]
                return prox
        return None

    def controller_apelido(self):
        """Apelido da controladora: o menor apelido (primeiro da ordem)."""
        ordem = self.order()
        if not ordem:
            return None
        return ordem[0][0]

    def is_controller(self, my_apelido) -> bool:
        """Se ``my_apelido`` eh a controladora (menor apelido) do anel."""
        return self.controller_apelido() == my_apelido

    def remove(self, apelido) -> None:
        """Remove um membro do anel, se presente (auxiliar)."""
        self._members.pop(apelido, None)
