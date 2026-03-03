# ---------------------------------------------------------------------------
# Read current value and raw history from a WinCC OPC UA server.
# Uses the synchronous 'opcua' (freeopcua) library — no asyncua.
# ---------------------------------------------------------------------------

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Optional

from opcua import Client, ua


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class AccessInfo:
    """Decoded OPC UA AccessLevel / UserAccessLevel bit fields."""
    access_level: Optional[int]
    user_access_level: Optional[int]

    @staticmethod
    def _flags(v: int) -> dict:
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
            parts.append(
                f"AccessLevel={self.access_level} "
                f"{self._flags(self.access_level)}"
            )
        if self.user_access_level is not None:
            parts.append(
                f"UserAccessLevel={self.user_access_level} "
                f"{self._flags(self.user_access_level)}"
            )
        return " | ".join(parts) if parts else "n/a"

    @property
    def can_current_read(self) -> bool:
        return bool(self.user_access_level and self.user_access_level & 0x01)

    @property
    def can_history_read(self) -> bool:
        return bool(self.user_access_level and self.user_access_level & 0x04)


# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------

class WinCCOpcUaReader:
    """Thin synchronous client for WinCC OPC UA nodes."""

    def __init__(self, endpoint_url: str, timeout_s: int = 5) -> None:
        self.endpoint_url = endpoint_url
        self.client = Client(endpoint_url, timeout=timeout_s)

    # -- Connection ----------------------------------------------------------

    def connect(self) -> None:
        self.client.connect()
        print(f"[connected]  {self.endpoint_url}")

    def disconnect(self) -> None:
        try:
            self.client.disconnect()
            print("[disconnected]")
        except Exception as ex:
            print(f"[warn] disconnect error: {ex}")

    # -- Node helpers --------------------------------------------------------

    @staticmethod
    def _read_int_attr(node, attr_id: ua.AttributeIds) -> Optional[int]:
        try:
            return node.get_attribute(attr_id).Value.Value
        except (Exception,):
            return None

    def _read_access_info(self, node) -> AccessInfo:
        return AccessInfo(
            access_level=self._read_int_attr(node, ua.AttributeIds.AccessLevel),
            user_access_level=self._read_int_attr(node, ua.AttributeIds.UserAccessLevel),
        )

    # -- Reads ---------------------------------------------------------------

    @staticmethod
    def _print_current_value(node) -> None:
        dv = node.get_data_value()
        val = dv.Value.Value if dv.Value is not None else None
        print("\n--- current value ---")
        print(f"  value            : {val!r}")
        print(f"  status_code      : {dv.StatusCode}")
        print(f"  source_timestamp : {dv.SourceTimestamp}")
        print(f"  server_timestamp : {dv.ServerTimestamp}")

    @staticmethod
    def _print_history(
            node,
            minutes_back: int = 60,
            max_values: int = 20,
    ) -> None:
        end = dt.datetime.now(dt.timezone.utc)
        start = end - dt.timedelta(minutes=minutes_back)
        dvs = node.read_raw_history(starttime=start, endtime=end)

        print(f"\n--- history (last {minutes_back} min, max {max_values}) ---")
        if not dvs:
            print("  no values returned")
            return
        for dv in dvs[-max_values:]:
            val = dv.Value.Value if dv.Value is not None else None
            print(f"  {dv.SourceTimestamp}  |  {dv.StatusCode}  |  {val!r}")

    # -- Public API ----------------------------------------------------------

    def read_tag_wincc(
            self,
            node_id: str,
            history_minutes_back: int = 60,
    ) -> None:
        node = self.client.get_node(node_id)
        access = self._read_access_info(node)

        print("\n===== WinCC OPC UA tag =====")
        print(f"  node_id      : {node_id}")
        print(f"  browse_name  : {node.get_browse_name()}")
        print(f"  display_name : {node.get_display_name()}")
        print(f"  node_class   : {node.get_node_class()}")
        try:
            print(f"  description  : {node.get_description()}")
        except (Exception,):
            pass
        print(f"  access       : {access.pretty()}")

        if access.can_current_read:
            self._print_current_value(node)
        elif access.can_history_read:
            print("\n[info] CurrentRead not allowed -> reading history")
            self._print_history(node, minutes_back=history_minutes_back)
        else:
            print("[error] Neither CurrentRead nor HistoryRead allowed.")

        print("============================\n")
