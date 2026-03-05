"""Configuracoes compartilhadas de testes."""

import pytest


def networkx_available() -> bool:
    """Verifica se networkx pode ser importado no ambiente atual."""
    try:
        import networkx  # noqa: F401
        return True
    except ImportError:
        return False


requires_networkx = pytest.mark.skipif(
    not networkx_available(),
    reason="networkx indisponivel (falta _bz2 no Python do sistema)",
)
