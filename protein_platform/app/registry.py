class PluginNotFoundError(Exception):
    def __init__(self, source_system: str):
        super().__init__(f"No plugin registered for source_system: {source_system!r}")
        self.source_system = source_system


class PluginRegistry:
    def __init__(self):
        self._plugins: dict[str, object] = {}

    def register(self, plugin_cls) -> None:
        instance = plugin_cls()
        self._plugins[instance.source_system] = instance

    def resolve(self, source_system: str):
        if source_system not in self._plugins:
            raise PluginNotFoundError(source_system)
        return self._plugins[source_system]

    def keys(self) -> list[str]:
        return sorted(self._plugins.keys())
