from datetime import datetime

from app.plugins.base import SourcePlugin


class PorkErpPlugin(SourcePlugin):
    @property
    def source_system(self) -> str:
        return "PORK_ERP"

    def transform_payload(self, payload: dict) -> dict:
        raw_ts = payload.get("event_ts") or payload.get("event_time")
        if isinstance(raw_ts, str):
            raw_ts = datetime.fromisoformat(raw_ts)

        source_item_id = payload.get("source_item_id") or payload.get("item_id")
        source_item_desc = payload.get("source_item_desc") or payload.get("item_desc")

        return {
            "source_system": self.source_system,
            "source_event_id": payload["source_event_id"],
            "event_ts": raw_ts,
            "plant_code": payload["plant_code"],
            "source_item_id": source_item_id,
            "source_item_desc": source_item_desc,
            "qty": payload.get("qty", 0.0),
            "uom": payload.get("uom", "LB"),
            "scrap_qty": payload.get("scrap_qty", 0.0),
        }
