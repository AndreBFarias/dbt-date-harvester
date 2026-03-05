"""Testes para macro expander."""

import pytest
from pathlib import Path

from dbt_parser.parsers.macro_expander import MacroExpander, MacroDefinition


@pytest.fixture
def expander(tmp_path: Path) -> MacroExpander:
    macros_dir = tmp_path / "macros"
    macros_dir.mkdir()

    date_utils = """
{% macro format_date(date_column) %}
    to_char({{ date_column }}, 'YYYY-MM-DD')
{% endmacro %}

{% macro date_diff(start_date, end_date, unit='day') %}
    date_part('{{ unit }}', {{ end_date }} - {{ start_date }})
{% endmacro %}
"""
    (macros_dir / "date_utils.sql").write_text(date_utils)

    string_utils = """
{% macro clean_string(string_column) %}
    trim(lower({{ string_column }}))
{% endmacro %}
"""
    (macros_dir / "string_utils.sql").write_text(string_utils)

    exp = MacroExpander(tmp_path)
    exp.parse_all_macros()
    return exp


class TestMacroExpander:
    def test_find_macro_files(self, expander: MacroExpander) -> None:
        files = expander.find_macro_files()
        assert len(files) == 2

    def test_parse_all_macros(self, expander: MacroExpander) -> None:
        macros = expander.get_all_macros()
        assert len(macros) == 3

    def test_get_macro(self, expander: MacroExpander) -> None:
        macro = expander.get_macro("format_date")
        assert macro is not None
        assert macro.name == "format_date"
        assert "date_column" in macro.parameters

    def test_macro_parameters(self, expander: MacroExpander) -> None:
        macro = expander.get_macro("date_diff")
        assert macro is not None
        assert "start_date" in macro.parameters
        assert "end_date" in macro.parameters
        assert "unit" in macro.parameters

    def test_get_unused_macros(self, expander: MacroExpander) -> None:
        used = {"format_date", "clean_string"}
        unused = expander.get_unused_macros(used)
        assert "date_diff" in unused

    def test_macro_not_found(self, expander: MacroExpander) -> None:
        macro = expander.get_macro("nonexistent")
        assert macro is None

    def test_macro_summary(self, expander: MacroExpander) -> None:
        summary = expander.get_macro_summary()
        assert summary["total_macros"] == 3
        assert len(summary["macros_by_file"]) == 2

    def test_no_macros_dir(self, tmp_path: Path) -> None:
        exp = MacroExpander(tmp_path)
        files = exp.find_macro_files()
        assert files == []
