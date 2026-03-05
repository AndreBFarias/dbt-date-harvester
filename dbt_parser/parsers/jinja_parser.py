"""Parser para templates Jinja em projetos dbt."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

@dataclass
class JinjaBlock:
    """Bloco Jinja identificado no SQL."""

    block_type: str  # "expression", "statement", "comment"
    content: str
    start_pos: int
    end_pos: int

@dataclass
class JinjaAnalysis:
    """Resultado da analise Jinja de um arquivo SQL."""

    filepath: str
    blocks: list[JinjaBlock] = field(default_factory=list)
    variables: set[str] = field(default_factory=set)
    control_structures: list[str] = field(default_factory=list)
    filters_used: set[str] = field(default_factory=set)

class JinjaParser:
    """Parser para blocos Jinja em SQL dbt."""

    EXPRESSION_PATTERN = re.compile(r"{{(.*?)}}", re.DOTALL)
    STATEMENT_PATTERN = re.compile(r"{%(.*?)%}", re.DOTALL)
    COMMENT_PATTERN = re.compile(r"{#(.*?)#}", re.DOTALL)
    VARIABLE_PATTERN = re.compile(r"\bvar\(\s*['\"](\w+)['\"]\s*\)")
    FILTER_PATTERN = re.compile(r"\|\s*(\w+)")
    SET_PATTERN = re.compile(r"{%\s*set\s+(\w+)\s*=")
    IF_PATTERN = re.compile(r"{%\s*if\b")
    FOR_PATTERN = re.compile(r"{%\s*for\b")

    def __init__(self) -> None:
        self._analyses: dict[str, JinjaAnalysis] = {}

    def parse_content(self, content: str, filepath: str = "") -> JinjaAnalysis:
        """Analisa conteudo Jinja de um arquivo SQL."""
        analysis = JinjaAnalysis(filepath=filepath)

        for match in self.EXPRESSION_PATTERN.finditer(content):
            block = JinjaBlock(
                block_type="expression",
                content=match.group(1).strip(),
                start_pos=match.start(),
                end_pos=match.end(),
            )
            analysis.blocks.append(block)

        for match in self.STATEMENT_PATTERN.finditer(content):
            block = JinjaBlock(
                block_type="statement",
                content=match.group(1).strip(),
                start_pos=match.start(),
                end_pos=match.end(),
            )
            analysis.blocks.append(block)

        for match in self.COMMENT_PATTERN.finditer(content):
            block = JinjaBlock(
                block_type="comment",
                content=match.group(1).strip(),
                start_pos=match.start(),
                end_pos=match.end(),
            )
            analysis.blocks.append(block)

        analysis.variables = set(self.VARIABLE_PATTERN.findall(content))
        analysis.filters_used = set(self.FILTER_PATTERN.findall(content))
        analysis.control_structures = self._extract_control_structures(content)

        self._analyses[filepath] = analysis
        logger.info(
            "Jinja parsed: %s (%d blocos, %d vars)",
            filepath,
            len(analysis.blocks),
            len(analysis.variables),
        )
        return analysis

    def _extract_control_structures(self, content: str) -> list[str]:
        """Extrai estruturas de controle Jinja."""
        structures: list[str] = []
        if self.IF_PATTERN.search(content):
            structures.append("if")
        if self.FOR_PATTERN.search(content):
            structures.append("for")
        if self.SET_PATTERN.search(content):
            structures.append("set")
        return structures

    def strip_jinja(self, content: str) -> str:
        """Remove todos os blocos Jinja do SQL."""
        result = self.COMMENT_PATTERN.sub("", content)
        result = self.STATEMENT_PATTERN.sub("", result)
        result = self.EXPRESSION_PATTERN.sub("''", result)
        return result

    def get_analysis(self, filepath: str) -> JinjaAnalysis | None:
        """Retorna analise Jinja de um arquivo."""
        return self._analyses.get(filepath)

    def get_all_variables(self) -> set[str]:
        """Retorna todas as variaveis Jinja usadas no projeto."""
        variables: set[str] = set()
        for analysis in self._analyses.values():
            variables.update(analysis.variables)
        return variables

    def get_jinja_complexity(self, content: str) -> dict[str, int]:
        """Calcula complexidade Jinja de um arquivo."""
        return {
            "expressions": len(self.EXPRESSION_PATTERN.findall(content)),
            "statements": len(self.STATEMENT_PATTERN.findall(content)),
            "comments": len(self.COMMENT_PATTERN.findall(content)),
            "variables": len(self.VARIABLE_PATTERN.findall(content)),
            "filters": len(self.FILTER_PATTERN.findall(content)),
            "if_blocks": len(self.IF_PATTERN.findall(content)),
            "for_loops": len(self.FOR_PATTERN.findall(content)),
        }

