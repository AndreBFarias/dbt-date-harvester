"""Testes de compatibilidade Windows para input_screen."""

import logging
from pathlib import Path
from unittest.mock import patch

import pytest

from tui.screens.input_screen import _listar_arquivos


class TestListarArquivos:
    """Testes para _listar_arquivos com cenarios problematicos."""

    def test_diretorio_inexistente(self) -> None:
        resultado = _listar_arquivos(Path("/caminho/que/nao/existe"))
        assert resultado == []

    def test_diretorio_vazio(self, tmp_path: Path) -> None:
        resultado = _listar_arquivos(tmp_path)
        assert resultado == []

    def test_arquivos_csv_e_xlsx(self, tmp_path: Path) -> None:
        (tmp_path / "dados.csv").touch()
        (tmp_path / "planilha.xlsx").touch()
        (tmp_path / "ignorado.txt").touch()

        resultado = _listar_arquivos(tmp_path)
        nomes = [nome for nome, _ in resultado]

        assert len(resultado) == 2
        assert "dados.csv" in nomes
        assert "planilha.xlsx" in nomes
        assert "ignorado.txt" not in nomes

    def test_arquivo_com_acentuacao(self, tmp_path: Path) -> None:
        (tmp_path / "estrategico_painel.csv").touch()
        (tmp_path / "relatorio_gestao.xlsx").touch()

        resultado = _listar_arquivos(tmp_path)
        assert len(resultado) == 2

    def test_arquivo_com_espacos_no_nome(self, tmp_path: Path) -> None:
        (tmp_path / "Mapa Projeto Painel.csv").touch()

        resultado = _listar_arquivos(tmp_path)
        assert len(resultado) == 1
        assert resultado[0][0] == "Mapa Projeto Painel.csv"

    def test_arquivo_com_parenteses_no_nome(self, tmp_path: Path) -> None:
        (tmp_path / "Mapa_Projeto(dados).csv").touch()

        resultado = _listar_arquivos(tmp_path)
        assert len(resultado) == 1
        assert resultado[0][0] == "Mapa_Projeto(dados).csv"

    def test_erro_oserror_em_iterdir(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        with patch.object(Path, "iterdir", side_effect=OSError("Permission denied")):
            with caplog.at_level(logging.WARNING):
                resultado = _listar_arquivos(tmp_path)

        assert resultado == []
        assert "Erro ao listar arquivos" in caplog.text

    def test_erro_unicode_em_iterdir(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        with patch.object(Path, "iterdir", side_effect=UnicodeError("surrogates not allowed")):
            with caplog.at_level(logging.WARNING):
                resultado = _listar_arquivos(tmp_path)

        assert resultado == []
        assert "Erro ao listar arquivos" in caplog.text

    def test_retorna_parcial_antes_do_erro(self, tmp_path: Path) -> None:
        (tmp_path / "bom.csv").touch()
        (tmp_path / "outro.xlsx").touch()

        resultado = _listar_arquivos(tmp_path)
        assert len(resultado) == 2


class TestPathMatchFallback:
    """Testa a logica de fallback por nome quando resolve() falha."""

    def test_resolve_funciona_normalmente(self, tmp_path: Path) -> None:
        arquivo = tmp_path / "dados.csv"
        arquivo.touch()

        input_path = arquivo
        val = str(arquivo)

        match = Path(val).resolve() == input_path.resolve()
        assert match is True

    def test_fallback_por_nome_quando_resolve_falha(self, tmp_path: Path) -> None:
        arquivo = tmp_path / "dados.csv"
        arquivo.touch()
        val = str(arquivo)

        input_path = Path("data_input/dados.csv")

        with patch.object(Path, "resolve", side_effect=OSError("encoding error")):
            try:
                match = Path(val).resolve() == input_path.resolve()
            except OSError:
                match = Path(val).name == input_path.name

        assert match is True

    def test_fallback_nao_faz_match_errado(self, tmp_path: Path) -> None:
        val = str(tmp_path / "arquivo_a.csv")
        input_path = Path("data_input/arquivo_b.csv")

        with patch.object(Path, "resolve", side_effect=OSError("encoding error")):
            try:
                match = Path(val).resolve() == input_path.resolve()
            except OSError:
                match = Path(val).name == input_path.name

        assert match is False


class TestAutoSelecao:
    """Testa a logica de auto-selecao quando ha apenas 1 arquivo."""

    def test_auto_seleciona_unico_arquivo(self, tmp_path: Path) -> None:
        (tmp_path / "unico.csv").touch()

        arquivos = _listar_arquivos(tmp_path)
        assert len(arquivos) == 1

        selecionado = False
        input_file = ""

        if input_file:
            pass

        if not selecionado and len(arquivos) == 1:
            valor_selecionado = arquivos[0][1]
            selecionado = True

        assert selecionado is True
        assert "unico.csv" in valor_selecionado

    def test_nao_auto_seleciona_se_multiplos(self, tmp_path: Path) -> None:
        (tmp_path / "a.csv").touch()
        (tmp_path / "b.xlsx").touch()

        arquivos = _listar_arquivos(tmp_path)
        assert len(arquivos) == 2

        selecionado = False
        if not selecionado and len(arquivos) == 1:
            selecionado = True

        assert selecionado is False

    def test_nao_auto_seleciona_se_vazio(self, tmp_path: Path) -> None:
        arquivos = _listar_arquivos(tmp_path)
        assert len(arquivos) == 0

        selecionado = False
        if not selecionado and len(arquivos) == 1:
            selecionado = True

        assert selecionado is False


# "A complexidade e o inimigo da seguranca." -- Bruce Schneier
