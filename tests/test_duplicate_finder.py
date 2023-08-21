"""Testes para duplicate finder."""

import pytest
from pathlib import Path

from dbt_parser.parsers.sql_parser import SqlParser
from dbt_parser.analyzers.duplicate_finder import DuplicateFinder


@pytest.fixture
def finder(tmp_path: Path) -> DuplicateFinder:
    models_dir = tmp_path / "models"
    models_dir.mkdir()

    sql1 = """with source as (
    select * from {{ source('raw', 'events') }}
),
cleaned as (
    select event_id, event_name from source
)
select * from cleaned"""

    sql2 = """with source as (
    select * from {{ source('raw', 'dates') }}
),
cleaned as (
    select date_key, date_value from source
)
select * from cleaned"""

    sql3 = """select distinct event_type from {{ ref('stg_events') }}"""

    (models_dir / "model_a.sql").write_text(sql1)
    (models_dir / "model_b.sql").write_text(sql2)
    (models_dir / "model_c.sql").write_text(sql3)

    parser = SqlParser(tmp_path)
    parser.parse_all()
    return DuplicateFinder(parser)


class TestDuplicateFinder:
    def test_find_duplicate_ctes(self, finder: DuplicateFinder) -> None:
        dupes = finder.find_duplicate_ctes()
        assert "source" in dupes or "cleaned" in dupes

    def test_find_similar_models(self, finder: DuplicateFinder) -> None:
        similar = finder.find_similar_models(threshold=0.5)
        assert isinstance(similar, list)

    def test_find_duplicate_configs(self, finder: DuplicateFinder) -> None:
        dupes = finder.find_duplicate_configs()
        assert isinstance(dupes, dict)

    def test_get_summary(self, finder: DuplicateFinder) -> None:
        summary = finder.get_duplicate_summary()
        assert "duplicate_cte_names" in summary
