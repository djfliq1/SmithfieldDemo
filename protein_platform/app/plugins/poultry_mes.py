from datetime import datetime

from app.plugins.base import SourcePlugin


class PoultryMesPlugin(SourcePlugin):
    @property
    def source_system(self) -> str:
        return "POULTRY_MES"

    def transform_payload(self, payload: dict) -> dict:
        raw_ts = payload.get("event_ts") or payload.get("event_time")
        if isinstance(raw_ts, str):
            raw_ts = datetime.fromisoformat(raw_ts)

        material = payload.get("material", {})
        quantities = payload.get("quantities", {})

        source_item_id = material.get("id") if material else payload.get("source_item_id")
        source_item_desc = material.get("desc") if material else payload.get("source_item_desc")
        qty = quantities.get("good") if quantities else payload.get("qty", 0.0)
        scrap_qty = quantities.get("scrap") if quantities else payload.get("scrap_qty", 0.0)
        uom = quantities.get("uom") if quantities else payload.get("uom", "LB")

        return {
            "source_system": self.source_system,
            "source_event_id": payload["source_event_id"],
            "event_ts": raw_ts,
            "plant_code": payload.get("plant_code"),
            "source_item_id": source_item_id,
            "source_item_desc": source_item_desc,
            "qty": qty,
            "uom": uom or "LB",
            "scrap_qty": scrap_qty,
        }
