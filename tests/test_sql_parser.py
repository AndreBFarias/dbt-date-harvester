"""Testes para o parser SQL."""

import tempfile
from pathlib import Path

import pytest

from dbt_parser.parsers.sql_parser import SqlParser


@pytest.fixture
def tmp_dbt_project(tmp_path: Path) -> Path:
    """Cria projeto dbt com modelos SQL de teste."""
    models_dir = tmp_path / "models"
    staging_dir = models_dir / "staging"
    marts_dir = models_dir / "marts"
    staging_dir.mkdir(parents=True)
    marts_dir.mkdir(parents=True)

    stg_events_sql = """{{
  config(
    materialized='view',
    tags=['staging', 'events']
  )
}}

with source_data as (
    select * from {{ source('raw', 'events') }}
),

cleaned as (
    select
        event_id,
        trim(lower(event_name)) as event_name,
        event_date,
        event_type,
        created_at,
        updated_at
    from source_data
    where event_id is not null
)

select * from cleaned
"""
    (staging_dir / "stg_events.sql").write_text(stg_events_sql)

    fct_sql = """{{
  config(
    materialized='table',
    tags=['marts', 'fact']
  )
}}

with events as (
    select * from {{ ref('stg_events') }}
),

dates as (
    select * from {{ ref('stg_dates') }}
),

joined as (
    select
        e.event_id,
        e.event_name,
        d.date_key,
        d.year
    from events e
    left join dates d
        on e.event_date = d.date_value
)

select * from joined
"""
    (marts_dir / "fct_event_dates.sql").write_text(fct_sql)
    return tmp_path


class TestSqlParser:
    """Testes para SqlParser."""

    def test_find_sql_files(self, tmp_dbt_project: Path) -> None:
        parser = SqlParser(tmp_dbt_project)
        files = parser.find_sql_files()
        assert len(files) == 2

    def test_parse_refs(self, tmp_dbt_project: Path) -> None:
        parser = SqlParser(tmp_dbt_project)
        parser.parse_all()
        model = parser.get_model("fct_event_dates")
        assert model is not None
        assert "stg_events" in model.refs
        assert "stg_dates" in model.refs

    def test_parse_sources(self, tmp_dbt_project: Path) -> None:
        parser = SqlParser(tmp_dbt_project)
        parser.parse_all()
        model = parser.get_model("stg_events")
        assert model is not None
        assert ("raw", "events") in model.sources

    def test_parse_config(self, tmp_dbt_project: Path) -> None:
        parser = SqlParser(tmp_dbt_project)
        parser.parse_all()
        model = parser.get_model("stg_events")
        assert model is not None
        assert model.config.get("materialized") == "view"

    def test_parse_ctes(self, tmp_dbt_project: Path) -> None:
        parser = SqlParser(tmp_dbt_project)
        parser.parse_all()
        model = parser.get_model("fct_event_dates")
        assert model is not None
        assert "events" in model.ctes or "joined" in model.ctes

    def test_get_all_refs(self, tmp_dbt_project: Path) -> None:
        parser = SqlParser(tmp_dbt_project)
        parser.parse_all()
        all_refs = parser.get_all_refs()
        assert "fct_event_dates" in all_refs
        assert len(all_refs["fct_event_dates"]) == 2

    def test_get_all_sources(self, tmp_dbt_project: Path) -> None:
        parser = SqlParser(tmp_dbt_project)
        parser.parse_all()
        all_sources = parser.get_all_sources()
        assert "stg_events" in all_sources

    def test_nonexistent_model(self, tmp_dbt_project: Path) -> None:
        parser = SqlParser(tmp_dbt_project)
        model = parser.get_model("nonexistent")
        assert model is None

    def test_no_models_dir(self, tmp_path: Path) -> None:
        parser = SqlParser(tmp_path)
        files = parser.find_sql_files()
        assert files == []
