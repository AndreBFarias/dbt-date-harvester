"""Testes para jinja parser."""

import pytest

from dbt_parser.parsers.jinja_parser import JinjaParser, JinjaBlock, JinjaAnalysis


@pytest.fixture
def parser() -> JinjaParser:
    return JinjaParser()


SQL_CONTENT = """{{
  config(
    materialized='table',
    tags=['staging']
  )
}}

{# Comentario jinja #}

{% if var('is_test_run') %}
    {% set limit_amt = 100 %}
{% endif %}

with source as (
    select * from {{ source('raw', 'events') }}
    {% if var('filter_date') %}
    where event_date > '{{ var("filter_date") }}'
    {% endif %}
),

cleaned as (
    select
        event_id,
        {{ clean_string('event_name') }} as event_name,
        event_date | date
    from source
)

select * from cleaned
"""


class TestJinjaParser:
    def test_parse_expressions(self, parser: JinjaParser) -> None:
        analysis = parser.parse_content(SQL_CONTENT, "test.sql")
        expr_blocks = [b for b in analysis.blocks if b.block_type == "expression"]
        assert len(expr_blocks) > 0

    def test_parse_statements(self, parser: JinjaParser) -> None:
        analysis = parser.parse_content(SQL_CONTENT, "test.sql")
        stmt_blocks = [b for b in analysis.blocks if b.block_type == "statement"]
        assert len(stmt_blocks) > 0

    def test_parse_comments(self, parser: JinjaParser) -> None:
        analysis = parser.parse_content(SQL_CONTENT, "test.sql")
        comment_blocks = [b for b in analysis.blocks if b.block_type == "comment"]
        assert len(comment_blocks) == 1

    def test_extract_variables(self, parser: JinjaParser) -> None:
        analysis = parser.parse_content(SQL_CONTENT, "test.sql")
        assert "is_test_run" in analysis.variables or "filter_date" in analysis.variables

    def test_control_structures(self, parser: JinjaParser) -> None:
        analysis = parser.parse_content(SQL_CONTENT, "test.sql")
        assert "if" in analysis.control_structures
        assert "set" in analysis.control_structures

    def test_strip_jinja(self, parser: JinjaParser) -> None:
        result = parser.strip_jinja("select * from {{ ref('model') }}")
        assert "{{" not in result
        assert "ref" not in result

    def test_get_jinja_complexity(self, parser: JinjaParser) -> None:
        complexity = parser.get_jinja_complexity(SQL_CONTENT)
        assert complexity["expressions"] > 0
        assert complexity["statements"] > 0

    def test_empty_content(self, parser: JinjaParser) -> None:
        analysis = parser.parse_content("select 1", "simple.sql")
        assert len(analysis.blocks) == 0

    def test_get_all_variables(self, parser: JinjaParser) -> None:
        parser.parse_content(SQL_CONTENT, "test1.sql")
        parser.parse_content("select {{ var('other_var') }}", "test2.sql")
        all_vars = parser.get_all_variables()
        assert len(all_vars) >= 1
