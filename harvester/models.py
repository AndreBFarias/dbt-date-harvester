from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class DateColumnClass(Enum):
    REFERENCIA = "referencia"
    ATUALIZACAO = "atualizacao"
    DADOS = "dados"
    DESCONHECIDO = "desconhecido"


@dataclass
class DateColumnInfo:
    nome: str
    tipo_bq: str
    classificacao: DateColumnClass = DateColumnClass.DESCONHECIDO
    confianca: float = 0.0


@dataclass
class TableInfo:
    nome_completo: str
    projeto: str = ""
    dataset: str = ""
    tabela: str = ""
    colunas_data: list[DateColumnInfo] = field(default_factory=list)
    existe_no_bq: bool = True

    def __post_init__(self) -> None:
        if self.nome_completo and not self.tabela:
            partes = self.nome_completo.split(".")
            if len(partes) == 3:
                self.projeto = partes[0]
                self.dataset = partes[1]
                self.tabela = partes[2]
            elif len(partes) == 2:
                self.dataset = partes[0]
                self.tabela = partes[1]


@dataclass
class DateResult:
    tabela: str
    tipo_data: DateColumnClass
    valor_antigo: str
    valor_novo: str
    coluna_origem: str
    sucesso: bool = True
    erro: str = ""
    confianca: float = 0.0
    pct_nulls: float = 0.0
    fuzzy_match: bool = False


@dataclass
class HarvestReport:
    resultados: list[DateResult] = field(default_factory=list)
    total_tabelas: int = 0
    tabelas_atualizadas: int = 0
    tabelas_erro: int = 0
    tabelas_sem_data: int = 0
    tabelas_inexistentes: int = 0
    timestamp: datetime = field(default_factory=datetime.now)
    arquivo_entrada: str = ""
    arquivo_saida: str = ""

    @property
    def resumo(self) -> str:
        return (
            f"Processadas: {self.total_tabelas} | "
            f"Atualizadas: {self.tabelas_atualizadas} | "
            f"Erros: {self.tabelas_erro} | "
            f"Sem data: {self.tabelas_sem_data} | "
            f"Inexistentes: {self.tabelas_inexistentes}"
        )
