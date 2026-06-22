"""Testes unitarios dos guards locais que nao dependem de timing de rede."""

from __future__ import annotations

import os
import sys
import unittest

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE not in sys.path:
    sys.path.insert(0, BASE)

from ring.core.token_logic import TokenLogicMixin  # noqa: E402


class _FakeNode(TokenLogicMixin):
    def __init__(self):
        self.apelido = "T"
        self.active_data_attempt = 2
        self.waiting_for_data_return = True
        self.dropped = 0
        self.completed = 0

    def _drop_head(self):
        self.dropped += 1

    def _complete_data_round(self):
        self.completed += 1


class TokenAttemptTests(unittest.TestCase):
    def test_timeout_antigo_nao_afeta_tentativa_atual(self):
        node = _FakeNode()
        node._on_timer_data_return_timeout(attempt=1)
        self.assertEqual(node.dropped, 0)
        self.assertEqual(node.completed, 0)
        self.assertTrue(node.waiting_for_data_return)

    def test_timeout_atual_finaliza_tentativa(self):
        node = _FakeNode()
        node._on_timer_data_return_timeout(attempt=2)
        self.assertEqual(node.dropped, 1)
        self.assertEqual(node.completed, 1)


if __name__ == "__main__":
    unittest.main()
