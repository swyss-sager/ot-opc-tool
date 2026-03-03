from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from opcua import Client, ua


@dataclass
class AccessInfo:
    access_level: int | None
    user_access_level: int | None

    @staticmethod
    def _flags(v: int) -> dict[str, bool]:
        return {
            "CurrentRead": bool(v & 0x01),
            "CurrentWrite": bool(v & 0x02),
            "HistoryRead": bool(v & 0x04),
            "HistoryWrite": bool(v & 0x08),
            "SemanticChange": bool(v & 0x10),
        }

    def pretty(self) -> str:
        parts = []
        if self.access_level is not None:
            parts.append(f"AccessLevel={self.access_level} {self._flags(self.access_level)}")
        if self.user_access_level is not None:
            parts.append(f"UserAccessLevel={self.user_access_level} {self._flags(self.user_access_level)}")
        return " | ".join(parts) if parts else "n/a"


class WinCCOpcUaReader:
    def __init__(self, endpoint_url: str, timeout_s: int = 5):
        self.endpoint_url = endpoint_url
        self.client = Client(endpoint_url, timeout=timeout_s)

    def connect(self) -> None:
        self.client.connect()
        print(f"[OK] Verbunden mit: {self.endpoint_url}")

    def disconnect(self) -> None:
        try:
            self.client.disconnect()
            print("[OK] Verbindung getrennt.")
        except Exception as ex:
            print(f"[WARN] Fehler beim Trennen: {ex}")

    def _read_attr_int(self, node, attr_id: ua.AttributeIds) -> int | None:
        try:
            dv = node.get_attribute(attr_id)
            return dv.Value.Value
        except Exception:
            return None

    def read_access_info(self, node) -> AccessInfo:
        al = self._read_attr_int(node, ua.AttributeIds.AccessLevel)
        ual = self._read_attr_int(node, ua.AttributeIds.UserAccessLevel)
        return AccessInfo(al, ual)

    def read_current_value(self, node) -> None:
        dv = node.get_data_value()
        val = dv.Value.Value if dv.Value is not None else None
        print("\n--- Current Value ---")
        print(f"Value           : {val!r}")
        print(f"StatusCode      : {dv.StatusCode}")
        print(f"SourceTimestamp : {dv.SourceTimestamp}")
        print(f"ServerTimestamp : {dv.ServerTimestamp}")

    def read_history(self, node, minutes_back: int = 60, max_values: int = 20) -> None:
        # WinCC liefert Historie nur, wenn Archivierung/History für den Tag existiert und HistoryRead erlaubt ist.
        end = datetime.now(timezone.utc)
        start = end - timedelta(minutes=minutes_back)

        print(f"\n--- History (last {minutes_back} min, showing up to {max_values}) ---")
        # freeopcua: read_raw_history gibt eine Liste von DataValue zurück
        dvs = node.read_raw_history(starttime=start, endtime=end)

        if not dvs:
            print("Keine Historienwerte zurückgegeben.")
            return

        for dv in dvs[-max_values:]:
            val = dv.Value.Value if dv.Value is not None else None
            print(f"{dv.SourceTimestamp} | {dv.StatusCode} | {val!r}")

    def read_tag_wincc(self, node_id: str, history_minutes_back: int = 60) -> None:
        node = self.client.get_node(node_id)

        print("\n========== WINCC OPC UA TAG ==========")
        print(f"NodeId      : {node_id}")
        print(f"BrowseName  : {node.get_browse_name()}")
        print(f"DisplayName : {node.get_display_name()}")
        print(f"NodeClass   : {node.get_node_class()}")
        try:
            print(f"Description : {node.get_description()}")
        except Exception:
            pass

        # AccessLevel auswerten (entscheidend bei WinCC)
        access = self.read_access_info(node)
        print(f"\nAccess        : {access.pretty()}")

        # Entscheidung: Current oder History
        can_current = access.user_access_level is not None and (access.user_access_level & 0x01)
        can_history = access.user_access_level is not None and (access.user_access_level & 0x04)

        if can_current:
            self.read_current_value(node)
        else:
            print("\n[INFO] CurrentRead ist nicht erlaubt (bei dir typisch: UserAccessLevel=4).")
            if can_history:
                self.read_history(node, minutes_back=history_minutes_back)
            else:
                print("[ERROR] Weder CurrentRead noch HistoryRead erlaubt – Server/Tag-Rechte prüfen.")

        print("=====================================\n")



