# ---------------------------------------------------------------------------
# Async OPC UA client for Siemens Process Historian with full history paging.
# Exposes a synchronous facade (ProcessHistorianReader) for use in runners.
# ---------------------------------------------------------------------------

import asyncio
import datetime as dt
from pathlib import Path
from typing import List, Optional, Tuple

from asyncua import Client, ua
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from src.app_opc_reader.logic.helper import (
    HistoryValue,
    aware_utc_now,
    ensure_aware_utc,
    to_naive_utc,
)


# ---------------------------------------------------------------------------
# Async OPC UA client
# ---------------------------------------------------------------------------

class SiemensHistorianOpcUaClient:
    """
    Async OPC UA client with PKI self-signed certificate handling
    and paginated raw history reads.
    """

    def __init__(
            self,
            endpoint_url: str,
            username: str,
            password: str,
            pki_dir: Path,
            application_uri: str,
            security_policy: str = "Basic128Rsa15",
            message_security_mode: str = "SignAndEncrypt",
            timeout_s: int = 30,
    ) -> None:
        if not application_uri:
            raise ValueError("application_uri must not be empty")

        self.endpoint_url = endpoint_url
        self.username = username
        self.password = password
        self.timeout_s = timeout_s
        self.pki_dir = Path(pki_dir).resolve()
        self.application_uri = application_uri

        self.client_cert_path = self.pki_dir / "own" / "certs" / "client_cert.der"
        self.client_key_path = self.pki_dir / "own" / "private" / "client_key.pem"
        self.security_string = (
            f"{security_policy},{message_security_mode},"
            f"{self.client_cert_path},{self.client_key_path}"
        )
        self._client: Optional[Client] = None

    # -- PKI -----------------------------------------------------------------

    def _ensure_pki_folders(self) -> None:
        for sub in (
                "own/certs", "own/private", "trusted/certs", "rejected/certs"
        ):
            (self.pki_dir / sub).mkdir(parents=True, exist_ok=True)

    def trusted_certs_dir(self) -> Path:
        self._ensure_pki_folders()
        return (self.pki_dir / "trusted" / "certs").resolve()

    @staticmethod
    def _cert_has_app_uri(cert_der: bytes, app_uri: str) -> bool:
        try:
            cert = x509.load_der_x509_certificate(cert_der)
            san = cert.extensions.get_extension_for_class(
                x509.SubjectAlternativeName).value
            return app_uri in san.get_values_for_type(
                x509.UniformResourceIdentifier)
        except (Exception,):
            return False

    def ensure_client_certificate(self) -> Tuple[Path, Path]:
        """Generate a self-signed client certificate if none exists or URI changed."""
        self._ensure_pki_folders()

        if self.client_cert_path.exists() and self.client_key_path.exists():
            if self._cert_has_app_uri(
                    self.client_cert_path.read_bytes(), self.application_uri):
                return self.client_cert_path, self.client_key_path
            self.client_cert_path.unlink(missing_ok=True)
            self.client_key_path.unlink(missing_ok=True)

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        name = x509.Name([
            x509.NameAttribute(NameOID.COMMON_NAME, "Python OPC UA Client"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Local"),
        ])
        now_naive = aware_utc_now().replace(tzinfo=None)

        cert = (
            x509.CertificateBuilder()
            .subject_name(name).issuer_name(name)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now_naive - dt.timedelta(minutes=5))
            .not_valid_after(now_naive + dt.timedelta(days=3650))
            .add_extension(
                x509.SubjectAlternativeName(
                    [x509.UniformResourceIdentifier(self.application_uri)]),
                critical=False)
            .add_extension(
                x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]),
                critical=False)
            .sign(key, hashes.SHA256())
        )

        self.client_key_path.write_bytes(key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        ))
        self.client_cert_path.write_bytes(cert.public_bytes(serialization.Encoding.DER))
        return self.client_cert_path, self.client_key_path

    # -- Connection ----------------------------------------------------------

    async def connect(self) -> None:
        self.ensure_client_certificate()
        client = Client(url=self.endpoint_url, timeout=self.timeout_s)
        client.application_uri = self.application_uri
        await client.set_security_string(self.security_string)
        client.set_user(self.username)
        client.set_password(self.password)
        await client.connect()
        self._client = client

    async def disconnect(self) -> None:
        if self._client is not None:
            try:
                await self._client.disconnect()
            finally:
                self._client = None

    # -- Debug ---------------------------------------------------------------

    async def debug_node(self, node_id: str) -> None:
        if self._client is None:
            raise RuntimeError("Not connected.")
        node = self._client.get_node(node_id)
        print(f"\n--- Node debug: {node_id} ---")
        try:
            dv = await node.read_attribute(ua.AttributeIds.Historizing)
            print(f"  Historizing : {dv.Value.Value}")
        except Exception as e:
            print(f"  Historizing : n/a ({e!r})")
        try:
            print(f"  Current val : {await node.read_value()}")
        except Exception as e:
            print(f"  Current val : n/a ({e!r})")
        print("--- end ---\n")

    # -- Single history page -------------------------------------------------

    async def _read_raw(
            self,
            node_id: str,
            start_utc: dt.datetime,
            end_utc: dt.datetime,
            num_values: int = 0,
            return_bounds: bool = False,
    ) -> List[HistoryValue]:
        if self._client is None:
            raise RuntimeError("Not connected.")
        node = self._client.get_node(node_id)
        dvs = await node.read_raw_history(
            starttime=to_naive_utc(start_utc),
            endtime=to_naive_utc(end_utc),
            numvalues=num_values,
            return_bounds=return_bounds,
        )
        return [
            HistoryValue(
                source_timestamp=dv.SourceTimestamp,
                server_timestamp=dv.ServerTimestamp,
                value=dv.Value.Value if dv.Value is not None else None,
                status_code=str(dv.StatusCode),
            )
            for dv in dvs
        ]

    # -- Paginated history ---------------------------------------------------

    async def read_history_paged(
            self,
            node_id: str,
            start_utc: dt.datetime,
            end_utc: dt.datetime,
            page_size: int = 10_000,
            max_pages: int = 100_000,
    ) -> List[HistoryValue]:
        """
        Fetch the full history for [start_utc, end_utc] using automatic paging.

        Termination conditions (in priority order):
          1. Server returns an empty batch.
          2. No timestamp progress between two consecutive pages (loop guard).
          3. current_start has advanced beyond end_utc.
          4. max_pages reached.

        Note: 'len(batch) < page_size' is intentionally NOT used as a stop
        condition — a server may return fewer items mid-stream.
        """
        start = ensure_aware_utc(start_utc)
        end = ensure_aware_utc(end_utc)

        all_values: List[HistoryValue] = []
        current_start: dt.datetime = start
        last_ts: Optional[dt.datetime] = None
        page_idx: int = 0

        while page_idx < max_pages:
            if current_start > end:
                break

            batch = await self._read_raw(
                node_id=node_id,
                start_utc=current_start,
                end_utc=end,
                num_values=page_size,
                return_bounds=(page_idx == 0),
            )

            if not batch:
                break

            # Drop already-seen timestamps to avoid duplicates at page boundaries.
            if last_ts is not None:
                batch = [
                    hv for hv in batch
                    if hv.source_timestamp is not None
                       and ensure_aware_utc(hv.source_timestamp) > last_ts
                ]
                if not batch:
                    break

            all_values.extend(batch)

            # Determine last timestamp in this batch.
            new_last_ts: Optional[dt.datetime] = None
            for hv in reversed(batch):
                if hv.source_timestamp is not None:
                    new_last_ts = ensure_aware_utc(hv.source_timestamp)
                    break

            if new_last_ts is None or (last_ts is not None and new_last_ts <= last_ts):
                break

            last_ts = new_last_ts
            current_start = last_ts
            page_idx += 1

            print(
                f"  [page {page_idx:>4}]  "
                f"batch={len(batch):>6}  "
                f"total={len(all_values):>8}  "
                f"up_to={last_ts.isoformat()}"
            )

        return all_values


# ---------------------------------------------------------------------------
# Async event-loop manager
# ---------------------------------------------------------------------------

class _LoopRunner:
    """Owns a dedicated asyncio event loop for synchronous callers."""

    def __init__(self) -> None:
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def start(self) -> None:
        if self._loop and not self._loop.is_closed():
            return
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

    def run(self, coro):
        if not self._loop or self._loop.is_closed():
            raise RuntimeError("Loop not started.")
        return self._loop.run_until_complete(coro)

    def stop(self) -> None:
        if self._loop is None:
            return
        try:
            pending = asyncio.all_tasks(self._loop)
            for t in pending:
                t.cancel()
            if pending:
                self._loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True))
        finally:
            self._loop.close()
            self._loop = None


# ---------------------------------------------------------------------------
# Synchronous facade
# ---------------------------------------------------------------------------

class ProcessHistorianReader:
    """
    Synchronous wrapper around SiemensHistorianOpcUaClient.
    One dedicated event loop per instance; use as context manager.
    """

    def __init__(
            self,
            endpoint_url: str,
            node_id: str,
            username: str,
            password: str,
            pki_dir: Path,
            application_uri: str,
            security_policy: str = "Basic128Rsa15",
            message_security_mode: str = "SignAndEncrypt",
            timeout_s: int = 30,
    ) -> None:
        self.node_id = node_id
        self._loop = _LoopRunner()
        self._connected = False
        self._client = SiemensHistorianOpcUaClient(
            endpoint_url=endpoint_url,
            username=username,
            password=password,
            pki_dir=pki_dir,
            application_uri=application_uri,
            security_policy=security_policy,
            message_security_mode=message_security_mode,
            timeout_s=timeout_s,
        )

    # -- Context manager -----------------------------------------------------

    def __enter__(self) -> "ProcessHistorianReader":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.disconnect()

    # -- Connection ----------------------------------------------------------

    def connect(self) -> None:
        if self._connected:
            return
        self._loop.start()
        self._loop.run(self._client.connect())
        self._connected = True

    def disconnect(self) -> None:
        if not self._connected:
            self._loop.stop()
            return
        try:
            self._loop.run(self._client.disconnect())
        finally:
            self._connected = False
            self._loop.stop()

    # -- Properties ----------------------------------------------------------

    @property
    def trusted_store(self) -> Path:
        return self._client.trusted_certs_dir()

    # -- Public API ----------------------------------------------------------

    def debug_node(self) -> None:
        self._loop.run(self._client.debug_node(self.node_id))

    def read_history_paged(
            self,
            start_utc: dt.datetime,
            end_utc: dt.datetime,
            page_size: int,
    ) -> List[HistoryValue]:
        return self._loop.run(
            self._client.read_history_paged(
                node_id=self.node_id,
                start_utc=start_utc,
                end_utc=end_utc,
                page_size=page_size,
            )
        )

    def probe_last_value(self, probe_minutes: int = 30) -> Optional[HistoryValue]:
        """
        Read a single recent value to determine where the historian
        has data (used by the fallback mechanism).
        """
        end_utc = aware_utc_now()
        start_utc = end_utc - dt.timedelta(minutes=probe_minutes)
        hist = self._loop.run(
            self._client._read_raw(
                node_id=self.node_id,
                start_utc=start_utc,
                end_utc=end_utc,
                num_values=0,
                return_bounds=True,
            )
        )
        return hist[0] if hist else None
