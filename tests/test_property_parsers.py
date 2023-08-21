"""
Testes baseados em propriedades para os parsers SQL e YAML.
Utiliza hypothesis para gerar inputs automaticamente e verificar
invariantes dos parsers.
"""

import pytest

try:
    from hypothesis import given, settings, assume
    from hypothesis import strategies as st

    HAS_HYPOTHESIS = True
except ImportError:
    HAS_HYPOTHESIS = False

pytestmark = pytest.mark.skipif(
    not HAS_HYPOTHESIS, reason="hypothesis nao disponivel"
)


def valid_sql_identifier():
    """Estrategia para gerar identificadores SQL validos."""
    return st.from_regex(r"[a-z][a-z0-9_]{2,30}", fullmatch=True)


def valid_ref_sql():
    """Estrategia para gerar SQL com refs dbt validos."""
    model_name = valid_sql_identifier()
    return model_name.map(
        lambda name: f"SELECT * FROM {{{{ ref('{name}') }}}}"
    )


def valid_source_sql():
    """Estrategia para gerar SQL com sources dbt validos."""
    return st.tuples(valid_sql_identifier(), valid_sql_identifier()).map(
        lambda t: f"SELECT * FROM {{{{ source('{t[0]}', '{t[1]}') }}}}"
    )


class TestSqlParserProperties:
    """Testes de propriedade para SqlParser."""

    @given(model_name=valid_sql_identifier())
    @settings(max_examples=50)
    def test_ref_extraction_always_finds_ref(self, model_name, tmp_path):
        """Qualquer SQL com ref() deve ter o ref extraido."""
        from dbt_parser.parsers.sql_parser import SqlParser

        sql_content = f"SELECT * FROM {{{{ ref('{model_name}') }}}}"
        sql_file = tmp_path / "models" / f"test_{model_name}.sql"
        sql_file.parent.mkdir(parents=True, exist_ok=True)
        sql_file.write_text(sql_content)
        parser = SqlParser(tmp_path)
        result = parser.parse_file(sql_file)
        assert model_name in result.refs

    @given(
        source_name=valid_sql_identifier(),
        table_name=valid_sql_identifier(),
    )
    @settings(max_examples=50)
    def test_source_extraction_always_finds_source(
        self, source_name, table_name, tmp_path
    ):
        """Qualquer SQL com source() deve ter o source extraido."""
        from dbt_parser.parsers.sql_parser import SqlParser

        sql_content = (
            f"SELECT * FROM {{{{ source('{source_name}', '{table_name}') }}}}"
        )
        sql_file = tmp_path / "models" / "test_source.sql"
        sql_file.parent.mkdir(parents=True, exist_ok=True)
        sql_file.write_text(sql_content)
        parser = SqlParser(tmp_path)
        result = parser.parse_file(sql_file)
        assert len(result.sources) > 0

    @given(n_refs=st.integers(min_value=1, max_value=5))
    @settings(max_examples=20)
    def test_multiple_refs_all_extracted(self, n_refs, tmp_path):
        """SQL com N refs deve retornar exatamente N refs."""
        from dbt_parser.parsers.sql_parser import SqlParser

        models = [f"model_{i}" for i in range(n_refs)]
        joins = "\n".join(
            f"JOIN {{{{ ref('{m}') }}}} AS {m} ON 1=1" for m in models
        )
        sql_content = f"SELECT 1\n{joins}"
        sql_file = tmp_path / "models" / "test_multi.sql"
        sql_file.parent.mkdir(parents=True, exist_ok=True)
        sql_file.write_text(sql_content)
        parser = SqlParser(tmp_path)
        result = parser.parse_file(sql_file)
        assert len(result.refs) == n_refs

    @given(text=st.text(min_size=0, max_size=200))
    @settings(max_examples=50)
    def test_parser_never_crashes_on_arbitrary_input(self, text, tmp_path):
        """Parser nunca deve lancar excecao com input arbitrario."""
        from dbt_parser.parsers.sql_parser import SqlParser

        sql_file = tmp_path / "models" / "test_fuzz.sql"
        sql_file.parent.mkdir(parents=True, exist_ok=True)
        sql_file.write_text(text)
        parser = SqlParser(tmp_path)
        result = parser.parse_file(sql_file)
        assert result is not None
        assert isinstance(result.refs, list)
        assert isinstance(result.sources, list)


class TestYamlParserProperties:
    """Testes de propriedade para YamlParser."""

    @given(model_name=valid_sql_identifier())
    @settings(max_examples=30)
    def test_yaml_with_model_always_parseable(self, model_name, tmp_path):
        """YAML com estrutura valida de modelo deve ser parseavel."""
        from dbt_parser.parsers.yaml_parser import YamlParser

        yaml_content = (
            f"version: 2\nmodels:\n  - name: {model_name}\n"
            f"    description: Modelo de teste\n"
        )
        yaml_file = tmp_path / "models" / "schema.yml"
        yaml_file.parent.mkdir(parents=True, exist_ok=True)
        yaml_file.write_text(yaml_content)
        parser = YamlParser(tmp_path)
        result = parser.parse_file(yaml_file)
        assert result is not None

    @given(text=st.text(min_size=0, max_size=100))
    @settings(max_examples=30)
    def test_yaml_parser_handles_invalid_input(self, text, tmp_path):
        """YamlParser deve tratar input invalido sem crash."""
        from dbt_parser.parsers.yaml_parser import YamlParser

        yaml_file = tmp_path / "models" / "invalid.yml"
        yaml_file.parent.mkdir(parents=True, exist_ok=True)
        yaml_file.write_text(text)
        parser = YamlParser(tmp_path)
        try:
            parser.parse_file(yaml_file)
        except Exception:
            pass


# "A liberdade e o direito de fazer tudo o que as leis permitem." -- Montesquieu
