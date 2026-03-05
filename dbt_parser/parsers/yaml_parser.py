"""Parser para arquivos YAML de projetos dbt."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

class YamlParser:
    """Parser para arquivos YAML de projetos dbt.

    Suporta schema.yml, sources.yml e dbt_project.yml.
    """

    SUPPORTED_FILES = {"schema.yml", "sources.yml", "dbt_project.yml"}

    def __init__(self, project_dir: Path) -> None:
        self.project_dir = project_dir
        self._parsed_files: dict[str, dict[str, Any]] = {}

    def parse_file(self, filepath: Path) -> dict[str, Any]:
        """Faz parsing de um arquivo YAML individual."""
        if not filepath.exists():
            logger.warning("Arquivo nao encontrado: %s", filepath)
            return {}

        try:
            with open(filepath, "r", encoding="utf-8") as f:
                content = yaml.safe_load(f)
            if content is None:
                return {}
            self._parsed_files[str(filepath)] = content
            logger.info("Arquivo parsed com sucesso: %s", filepath)
            return content
        except yaml.YAMLError as exc:
            logger.error("Erro ao parsear YAML %s: %s", filepath, exc)
            raise

    def find_yaml_files(self) -> list[Path]:
        """Encontra todos os arquivos YAML no projeto dbt."""
        yaml_files = []
        for pattern in ("**/*.yml", "**/*.yaml"):
            yaml_files.extend(self.project_dir.glob(pattern))
        return sorted(yaml_files)

    def parse_all(self) -> dict[str, dict[str, Any]]:
        """Faz parsing de todos os arquivos YAML encontrados."""
        results: dict[str, dict[str, Any]] = {}
        for filepath in self.find_yaml_files():
            parsed = self.parse_file(filepath)
            if parsed:
                results[str(filepath)] = parsed
        return results

    def get_parsed_files(self) -> dict[str, dict[str, Any]]:
        """Retorna arquivos ja parseados."""
        return self._parsed_files.copy()

    def extract_version(self, content: dict[str, Any]) -> int | None:
        """Extrai versao do schema YAML."""
        return content.get("version")

    def extract_models_section(self, content: dict[str, Any]) -> list[dict[str, Any]]:
        """Extrai secao de modelos de um schema.yml."""
        return content.get("models", [])

    def extract_sources_section(self, content: dict[str, Any]) -> list[dict[str, Any]]:
        """Extrai secao de sources de um sources.yml."""
        return content.get("sources", [])

