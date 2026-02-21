from datetime import datetime

from app.plugins.base import SourcePlugin


class BeefWmsPlugin(SourcePlugin):
    @property
    def source_system(self) -> str:
        return "BEEF_WMS"

    def transform_payload(self, payload: dict) -> dict:
        raw_ts = payload.get("event_ts") or payload.get("ts")
        if isinstance(raw_ts, str):
            raw_ts = datetime.fromisoformat(raw_ts)

        plant_code = payload.get("plant_code") or payload.get("warehouse")
        source_item_id = payload.get("source_item_id") or payload.get("sku")
        source_item_desc = payload.get("source_item_desc") or payload.get("sku_desc")
        qty = payload.get("qty") if payload.get("qty") is not None else payload.get("produced", 0.0)
        scrap_qty = payload.get("scrap_qty") if payload.get("scrap_qty") is not None else payload.get("scrap", 0.0)

        return {
            "source_system": self.source_system,
            "source_event_id": payload["source_event_id"],
            "event_ts": raw_ts,
            "plant_code": plant_code,
            "source_item_id": source_item_id,
            "source_item_desc": source_item_desc,
            "qty": qty,
            "uom": payload.get("uom", "LB"),
            "scrap_qty": scrap_qty,
        }
