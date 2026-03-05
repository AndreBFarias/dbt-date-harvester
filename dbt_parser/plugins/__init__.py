"""Sistema de plugins para extensibilidade do dbt parser."""

from dbt_parser.plugins.plugin_manager import PluginManager, BasePlugin

__all__ = ["PluginManager", "BasePlugin"]
