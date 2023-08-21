"""Gerenciador de plugins para o dbt parser."""

from __future__ import annotations

import importlib
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Type

from dbt_parser.analyzers.graph_resolver import GraphResolver

logger = logging.getLogger(__name__)

class BasePlugin(ABC):
    """Classe base para plugins do dbt parser."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Nome unico do plugin."""

    @property
    @abstractmethod
    def version(self) -> str:
        """Versao do plugin."""

    @property
    def description(self) -> str:
        """Descricao do plugin."""
        return ""

    def on_load(self) -> None:
        """Chamado quando o plugin e carregado."""

    def on_unload(self) -> None:
        """Chamado quando o plugin e descarregado."""

    def on_parse_start(self, project_dir: Path) -> None:
        """Chamado antes do inicio do parsing."""

    def on_parse_complete(self, graph: GraphResolver) -> None:
        """Chamado apos conclusao do parsing."""

    def on_validate_start(self) -> None:
        """Chamado antes do inicio da validacao."""

    def on_validate_complete(self, results: list[Any]) -> None:
        """Chamado apos conclusao da validacao."""

    def register_validators(self) -> list[Callable]:
        """Retorna validadores customizados do plugin."""
        return []

    def register_exporters(self) -> dict[str, Callable]:
        """Retorna exportadores customizados do plugin."""
        return {}

@dataclass
class PluginInfo:
    """Informacoes de um plugin registrado."""

    name: str
    version: str
    description: str
    plugin_class: Type[BasePlugin]
    instance: BasePlugin | None = None
    enabled: bool = True

class PluginManager:
    """Gerencia ciclo de vida de plugins."""

    def __init__(self) -> None:
        self._plugins: dict[str, PluginInfo] = {}
        self._hooks: dict[str, list[Callable]] = {}

    def register(self, plugin_class: Type[BasePlugin]) -> None:
        """Registra um plugin."""
        instance = plugin_class()
        info = PluginInfo(
            name=instance.name,
            version=instance.version,
            description=instance.description,
            plugin_class=plugin_class,
            instance=instance,
        )
        self._plugins[instance.name] = info
        instance.on_load()
        logger.info("Plugin registrado: %s v%s", instance.name, instance.version)

    def unregister(self, plugin_name: str) -> bool:
        """Remove registro de um plugin."""
        info = self._plugins.pop(plugin_name, None)
        if info and info.instance:
            info.instance.on_unload()
            logger.info("Plugin removido: %s", plugin_name)
            return True
        return False

    def get_plugin(self, name: str) -> BasePlugin | None:
        """Retorna instancia de um plugin."""
        info = self._plugins.get(name)
        return info.instance if info else None

    def get_all_plugins(self) -> list[PluginInfo]:
        """Retorna todos os plugins registrados."""
        return list(self._plugins.values())

    def enable_plugin(self, name: str) -> bool:
        """Habilita um plugin."""
        if name in self._plugins:
            self._plugins[name].enabled = True
            return True
        return False

    def disable_plugin(self, name: str) -> bool:
        """Desabilita um plugin."""
        if name in self._plugins:
            self._plugins[name].enabled = False
            return True
        return False

    def emit(self, event: str, *args: Any, **kwargs: Any) -> None:
        """Emite evento para todos os plugins habilitados."""
        for info in self._plugins.values():
            if not info.enabled or not info.instance:
                continue
            handler = getattr(info.instance, event, None)
            if handler and callable(handler):
                try:
                    handler(*args, **kwargs)
                except Exception as exc:
                    logger.error(
                        "Erro no plugin %s ao processar %s: %s",
                        info.name,
                        event,
                        exc,
                    )

    def collect_validators(self) -> list[Callable]:
        """Coleta validadores de todos os plugins."""
        validators: list[Callable] = []
        for info in self._plugins.values():
            if info.enabled and info.instance:
                validators.extend(info.instance.register_validators())
        return validators

    def collect_exporters(self) -> dict[str, Callable]:
        """Coleta exportadores de todos os plugins."""
        exporters: dict[str, Callable] = {}
        for info in self._plugins.values():
            if info.enabled and info.instance:
                exporters.update(info.instance.register_exporters())
        return exporters

    def load_plugin_from_module(self, module_path: str) -> None:
        """Carrega plugin de um modulo Python."""
        try:
            module = importlib.import_module(module_path)
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if (
                    isinstance(attr, type)
                    and issubclass(attr, BasePlugin)
                    and attr is not BasePlugin
                ):
                    self.register(attr)
        except ImportError as exc:
            logger.error("Erro ao carregar modulo de plugin %s: %s", module_path, exc)

    def get_summary(self) -> dict[str, Any]:
        """Retorna resumo de plugins."""
        return {
            "total_plugins": len(self._plugins),
            "enabled": sum(1 for p in self._plugins.values() if p.enabled),
            "disabled": sum(1 for p in self._plugins.values() if not p.enabled),
            "plugins": [
                {"name": p.name, "version": p.version, "enabled": p.enabled}
                for p in self._plugins.values()
            ],
        }

