"""Testes para sistema de plugins."""

import pytest
from pathlib import Path

from dbt_parser.plugins.plugin_manager import PluginManager, BasePlugin
from dbt_parser.analyzers.graph_resolver import GraphResolver
from tests.conftest import requires_networkx


class SamplePlugin(BasePlugin):
    def __init__(self) -> None:
        self.loaded = False
        self.parse_events: list = []

    @property
    def name(self) -> str:
        return "sample-plugin"

    @property
    def version(self) -> str:
        return "0.1.0"

    @property
    def description(self) -> str:
        return "Plugin de teste"

    def on_load(self) -> None:
        self.loaded = True

    def on_parse_complete(self, graph: GraphResolver) -> None:
        self.parse_events.append(graph.node_count())


class AnotherPlugin(BasePlugin):
    @property
    def name(self) -> str:
        return "another-plugin"

    @property
    def version(self) -> str:
        return "0.2.0"


class TestPluginManager:
    def test_register_plugin(self) -> None:
        manager = PluginManager()
        manager.register(SamplePlugin)
        assert manager.get_plugin("sample-plugin") is not None

    def test_plugin_loaded(self) -> None:
        manager = PluginManager()
        manager.register(SamplePlugin)
        plugin = manager.get_plugin("sample-plugin")
        assert plugin.loaded

    def test_unregister_plugin(self) -> None:
        manager = PluginManager()
        manager.register(SamplePlugin)
        assert manager.unregister("sample-plugin")
        assert manager.get_plugin("sample-plugin") is None

    @requires_networkx
    def test_emit_event(self) -> None:
        manager = PluginManager()
        manager.register(SamplePlugin)
        graph = GraphResolver()
        manager.emit("on_parse_complete", graph)
        plugin = manager.get_plugin("sample-plugin")
        assert len(plugin.parse_events) == 1

    @requires_networkx
    def test_disable_plugin(self) -> None:
        manager = PluginManager()
        manager.register(SamplePlugin)
        manager.disable_plugin("sample-plugin")
        graph = GraphResolver()
        manager.emit("on_parse_complete", graph)
        plugin = manager.get_plugin("sample-plugin")
        assert len(plugin.parse_events) == 0

    def test_enable_plugin(self) -> None:
        manager = PluginManager()
        manager.register(SamplePlugin)
        manager.disable_plugin("sample-plugin")
        manager.enable_plugin("sample-plugin")
        all_plugins = manager.get_all_plugins()
        assert all_plugins[0].enabled

    def test_multiple_plugins(self) -> None:
        manager = PluginManager()
        manager.register(SamplePlugin)
        manager.register(AnotherPlugin)
        assert len(manager.get_all_plugins()) == 2

    def test_get_summary(self) -> None:
        manager = PluginManager()
        manager.register(SamplePlugin)
        manager.register(AnotherPlugin)
        manager.disable_plugin("another-plugin")
        summary = manager.get_summary()
        assert summary["total_plugins"] == 2
        assert summary["enabled"] == 1
        assert summary["disabled"] == 1

    def test_unregister_nonexistent(self) -> None:
        manager = PluginManager()
        assert not manager.unregister("nonexistent")
