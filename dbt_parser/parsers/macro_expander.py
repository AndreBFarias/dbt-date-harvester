"""Expansor de macros dbt Jinja."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

MACRO_DEF_PATTERN = re.compile(
    r"{%\s*macro\s+(\w+)\s*\((.*?)\)\s*%}(.*?){%\s*endmacro\s*%}",
    re.DOTALL,
)

@dataclass
class MacroDefinition:
    """Definicao de uma macro dbt."""

    name: str
    parameters: list[str]
    body: str
    filepath: Path
    calls_macros: list[str] = field(default_factory=list)

class MacroExpander:
    """Gerencia e expande macros Jinja de projetos dbt."""

    def __init__(self, project_dir: Path) -> None:
        self.project_dir = project_dir
        self._macros: dict[str, MacroDefinition] = {}

    def find_macro_files(self) -> list[Path]:
        """Encontra todos os arquivos de macros."""
        macros_dir = self.project_dir / "macros"
        if not macros_dir.exists():
            return []
        return sorted(macros_dir.glob("**/*.sql"))

    def parse_macro_file(self, filepath: Path) -> list[MacroDefinition]:
        """Faz parsing de um arquivo de macros."""
        if not filepath.exists():
            return []

        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()

        macros: list[MacroDefinition] = []
        for match in MACRO_DEF_PATTERN.finditer(content):
            name = match.group(1)
            params_str = match.group(2).strip()
            body = match.group(3).strip()

            params = []
            if params_str:
                for param in params_str.split(","):
                    param = param.strip()
                    if "=" in param:
                        param = param.split("=")[0].strip()
                    if param:
                        params.append(param)

            calls = self._find_macro_calls(body)

            macro_def = MacroDefinition(
                name=name,
                parameters=params,
                body=body,
                filepath=filepath,
                calls_macros=calls,
            )
            macros.append(macro_def)
            self._macros[name] = macro_def
            logger.info("Macro encontrada: %s(%s)", name, ", ".join(params))

        return macros

    def parse_all_macros(self) -> dict[str, MacroDefinition]:
        """Faz parsing de todas as macros do projeto."""
        for filepath in self.find_macro_files():
            self.parse_macro_file(filepath)
        return self._macros.copy()

    def get_macro(self, name: str) -> MacroDefinition | None:
        """Retorna definicao de uma macro por nome."""
        return self._macros.get(name)

    def get_all_macros(self) -> dict[str, MacroDefinition]:
        """Retorna todas as macros registradas."""
        return self._macros.copy()

    def get_macro_dependencies(self, macro_name: str) -> set[str]:
        """Retorna macros das quais uma macro depende."""
        macro = self._macros.get(macro_name)
        if not macro:
            return set()
        return set(macro.calls_macros)

    def get_unused_macros(self, used_macros: set[str]) -> set[str]:
        """Retorna macros definidas mas nao utilizadas."""
        defined = set(self._macros.keys())
        return defined - used_macros

    def _find_macro_calls(self, content: str) -> list[str]:
        """Encontra chamadas de macros em conteudo Jinja."""
        excluded = {"ref", "source", "config", "if", "else", "endif", "for", "endfor", "set"}
        pattern = re.compile(r"{{\s*(\w+)\s*\(")
        calls = pattern.findall(content)
        return [c for c in calls if c not in excluded and c not in self._macros]

    def get_macro_summary(self) -> dict[str, Any]:
        """Retorna resumo de macros do projeto."""
        return {
            "total_macros": len(self._macros),
            "macros_by_file": self._group_by_file(),
            "macros_with_deps": [
                m.name for m in self._macros.values() if m.calls_macros
            ],
        }

    def _group_by_file(self) -> dict[str, list[str]]:
        """Agrupa macros por arquivo."""
        by_file: dict[str, list[str]] = {}
        for macro in self._macros.values():
            key = str(macro.filepath)
            if key not in by_file:
                by_file[key] = []
            by_file[key].append(macro.name)
        return by_file

