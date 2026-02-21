import pytest
from app.registry import PluginNotFoundError, PluginRegistry
from app.plugins.pork_erp import PorkErpPlugin
from app.plugins.beef_wms import BeefWmsPlugin
from app.plugins.poultry_mes import PoultryMesPlugin


def test_register_and_resolve():
    registry = PluginRegistry()
    registry.register(PorkErpPlugin)
    registry.register(BeefWmsPlugin)
    registry.register(PoultryMesPlugin)

    plugin = registry.resolve("PORK_ERP")
    assert plugin.source_system == "PORK_ERP"

    plugin2 = registry.resolve("BEEF_WMS")
    assert plugin2.source_system == "BEEF_WMS"

    plugin3 = registry.resolve("POULTRY_MES")
    assert plugin3.source_system == "POULTRY_MES"


def test_keys_sorted():
    registry = PluginRegistry()
    registry.register(PoultryMesPlugin)
    registry.register(PorkErpPlugin)
    registry.register(BeefWmsPlugin)
    assert registry.keys() == ["BEEF_WMS", "PORK_ERP", "POULTRY_MES"]


def test_unknown_raises_plugin_not_found_error():
    registry = PluginRegistry()
    with pytest.raises(PluginNotFoundError):
        registry.resolve("UNKNOWN_SYSTEM")
