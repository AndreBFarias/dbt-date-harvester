# -*- coding: utf-8 -*-
"""Parser para arquivos SQL de projetos dbt com suporte a Jinja."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

REF_PATTERN = re.compile(r"{{\s*ref\(\s*['\"](\w+)['\"]\s*\)\s*}}")
SOURCE_PATTERN = re.compile(
    r"{{\s*source\(\s*['\"](\w+)['\"]\s*,\s*['\"](\w+)['\"]\s*\)\s*}}"
)
CONFIG_PATTERN = re.compile(r"{{\s*config\((.*?)\)\s*}}", re.DOTALL)
MACRO_CALL_PATTERN = re.compile(r"{{\s*(\w+)\((.*?)\)\s*}}")

@dataclass
class SqlModelInfo:
    """Informacoes extraidas de um arquivo SQL dbt."""

    filepath: Path
    name: str
    refs: list[str] = field(default_factory=list)
    sources: list[tuple[str, str]] = field(default_factory=list)
    config: dict[str, str] = field(default_factory=dict)
    macro_calls: list[str] = field(default_factory=list)
    raw_sql: str = ""
    ctes: list[str] = field(default_factory=list)

class SqlParser:
    """Parser para arquivos SQL de projetos dbt."""

    CTE_PATTERN = re.compile(
        r"with\s+(\w+)\s+as\s*\(", re.IGNORECASE
    )
    CTE_NAMED_PATTERN = re.compile(
        r"(\w+)\s+as\s*\(", re.IGNORECASE
    )

    def __init__(self, project_dir: Path) -> None:
        """Inicializa o parser com o diretorio do projeto dbt."""
        self.project_dir = project_dir
        self._parsed_models: dict[str, SqlModelInfo] = {}

    def parse_file(self, filepath: Path) -> SqlModelInfo:
        """Faz parsing de um arquivo SQL individual."""
        if not filepath.exists():
            logger.warning("Arquivo SQL nao encontrado: %s", filepath)
            return SqlModelInfo(filepath=filepath, name=filepath.stem)

        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()

        model_name = filepath.stem
        refs = self._extract_refs(content)
        sources = self._extract_sources(content)
        config = self._extract_config(content)
        macro_calls = self._extract_macro_calls(content)
        ctes = self._extract_ctes(content)

        model_info = SqlModelInfo(
            filepath=filepath,
            name=model_name,
            refs=refs,
            sources=sources,
            config=config,
            macro_calls=macro_calls,
            raw_sql=content,
            ctes=ctes,
        )

        self._parsed_models[model_name] = model_info
        logger.info("SQL parsed: %s (refs=%d, sources=%d)", model_name, len(refs), len(sources))
        return model_info

    def find_sql_files(self) -> list[Path]:
        """Encontra todos os arquivos SQL no projeto."""
        models_dir = self.project_dir / "models"
        if not models_dir.exists():
            logger.warning("Diretorio models nao encontrado: %s", models_dir)
            return []
        return sorted(models_dir.glob("**/*.sql"))

    def parse_all(self) -> dict[str, SqlModelInfo]:
        """Faz parsing de todos os arquivos SQL."""
        for filepath in self.find_sql_files():
            self.parse_file(filepath)
        return self._parsed_models.copy()

    def _extract_refs(self, content: str) -> list[str]:
        """Extrai referencias ref() do SQL."""
        return REF_PATTERN.findall(content)

    def _extract_sources(self, content: str) -> list[tuple[str, str]]:
        """Extrai referencias source() do SQL."""
        return SOURCE_PATTERN.findall(content)

    def _extract_config(self, content: str) -> dict[str, str]:
        """Extrai configuracao do bloco config()."""
        match = CONFIG_PATTERN.search(content)
        if not match:
            return {}

        config_str = match.group(1)
        config: dict[str, str] = {}
        pairs = re.findall(r"(\w+)\s*=\s*['\"]?([^,'\"\)]+)['\"]?", config_str)
        for key, value in pairs:
            config[key.strip()] = value.strip()
        return config

    def _extract_macro_calls(self, content: str) -> list[str]:
        """Extrai chamadas de macros do SQL."""
        excluded = {"ref", "source", "config", "if", "else", "endif", "for", "endfor"}
        calls = MACRO_CALL_PATTERN.findall(content)
        return [name for name, _ in calls if name not in excluded]

    def _extract_ctes(self, content: str) -> list[str]:
        """Extrai nomes de CTEs do SQL."""
        clean_content = re.sub(r"/\*.*?\*/", "", content, flags=re.DOTALL)
        clean_content = re.sub(r"--.*$", "", clean_content, flags=re.MULTILINE)
        clean_content = re.sub(r"{{.*?}}", "", clean_content, flags=re.DOTALL)

        ctes: list[str] = []
        cte_blocks = re.split(r"\bwith\b", clean_content, flags=re.IGNORECASE)
        if len(cte_blocks) > 1:
            after_with = cte_blocks[1]
            names = self.CTE_NAMED_PATTERN.findall(after_with)
            ctes = [n for n in names if n.lower() not in ("select", "from", "where")]
        return ctes

    def get_model(self, name: str) -> SqlModelInfo | None:
        """Retorna modelo parseado por nome."""
        return self._parsed_models.get(name)

    def get_all_refs(self) -> dict[str, list[str]]:
        """Retorna mapa de refs por modelo."""
        return {name: model.refs for name, model in self._parsed_models.items()}

    def get_all_sources(self) -> dict[str, list[tuple[str, str]]]:
        """Retorna mapa de sources por modelo."""
        return {name: model.sources for name, model in self._parsed_models.items()}

