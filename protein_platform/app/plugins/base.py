from abc import ABC, abstractmethod


class SourcePlugin(ABC):
    @property
    @abstractmethod
    def source_system(self) -> str:
        ...

    @abstractmethod
    def transform_payload(self, payload: dict) -> dict:
        ...
