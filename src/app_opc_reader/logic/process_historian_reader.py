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


class SiemensHistorianOpcUaClient:
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
            raise ValueError("application_uri must not be empty/None")

        self.endpoint_url = endpoint_url
        self.username = username
        self.password = password
        self.timeout_s = timeout_s

        self.pki_dir = Path(pki_dir).resolve()
        self.application_uri = application_uri

        self.client_cert_path = self.pki_dir / "own" / "certs" / "client_cert.der"
        self.client_key_path  = self.pki_dir / "own" / "private" / "client_key.pem"

        self.security_string = (
            f"{security_policy},{message_security_mode},"
            f"{self.client_cert_path},{self.client_key_path}"
        )
        self._client: Optional[Client] = None

    # ── PKI ────────────────────────────────────────────────────────────────

    def _ensure_pki_folders(self) -> None:
        (self.pki_dir / "own"      / "certs"  ).mkdir(parents=True, exist_ok=True)
        (self.pki_dir / "own"      / "private").mkdir(parents=True, exist_ok=True)
        (self.pki_dir / "trusted"  / "certs"  ).mkdir(parents=True, exist_ok=True)
        (self.pki_dir / "rejected" / "certs"  ).mkdir(parents=True, exist_ok=True)

    def trusted_certs_dir(self) -> Path:
        self._ensure_pki_folders()
        return (self.pki_dir / "trusted" / "certs").resolve()

    @staticmethod
    def _cert_contains_app_uri(cert_der: bytes, app_uri: str) -> bool:
        try:
            cert = x509.load_der_x509_certificate(cert_der)
            try:
                san = cert.extensions.get_extension_for_class(
                    x509.SubjectAlternativeName).value
            except Exception:
                return False
            return app_uri in san.get_values_for_type(
                x509.UniformResourceIdentifier)
        except Exception:
            return False

    def ensure_client_certificate(
            self, common_name: str = "Python OPC UA Client"
    ) -> Tuple[Path, Path]:
        self._ensure_pki_folders()

        if self.client_cert_path.exists() and self.client_key_path.exists():
            if self._cert_contains_app_uri(
                    self.client_cert_path.read_bytes(), self.application_uri):
                return self.client_cert_path, self.client_key_path
            self.client_cert_path.unlink(missing_ok=True)
            self.client_key_path.unlink(missing_ok=True)

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = issuer = x509.Name([
            x509.NameAttribute(NameOID.COMMON_NAME,        common_name),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME,  "Local"),
        ])
        now_naive = aware_utc_now().replace(tzinfo=None)

        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now_naive - dt.timedelta(minutes=5))
            .not_valid_after (now_naive + dt.timedelta(days=3650))
            .add_extension(
                x509.SubjectAlternativeName(
                    [x509.UniformResourceIdentifier(self.application_uri)]),
                critical=False)
            .add_extension(
                x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]),
                critical=False)
            .sign(private_key=key, algorithm=hashes.SHA256())
        )

        self.client_key_path.write_bytes(key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        ))
        self.client_cert_path.write_bytes(
            cert.public_bytes(serialization.Encoding.DER))
        return self.client_cert_path, self.client_key_path

    # ── Connect / Disconnect ───────────────────────────────────────────────

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

    # ── Debug ──────────────────────────────────────────────────────────────

    async def debug_history_capabilities(self, node_id: str) -> None:
        if self._client is None:
            raise RuntimeError("Not connected.")
        node = self._client.get_node(node_id)
        print("\n--- DEBUG: Node capabilities ---")
        print("NodeId:", node_id)
        try:
            dv = await node.read_attribute(ua.AttributeIds.Historizing)
            print("Historizing attribute:", dv.Value.Value)
        except Exception as e:
            print("Could not read Historizing attribute:", repr(e))
        try:
            v = await node.read_value()
            print("Current value (read_value):", v)
        except Exception as e:
            print("Could not read current value:", repr(e))
        print("--- DEBUG: end ---\n")

    # ── Raw history (single call) ──────────────────────────────────────────

    async def read_history_raw(
            self,
            node_id: str,
            start_time_utc: dt.datetime,
            end_time_utc: dt.datetime,
            num_values: int = 0,
            return_bounds: bool = False,
    ) -> List[HistoryValue]:
        if self._client is None:
            raise RuntimeError("Not connected.")

        node = self._client.get_node(node_id)
        dvs  = await node.read_raw_history(
            starttime=to_naive_utc(start_time_utc),
            endtime=to_naive_utc(end_time_utc),
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

    # ── Paged history ──────────────────────────────────────────────────────

    async def read_history_raw_paged(
            self,
            node_id: str,
            start_time_utc: dt.datetime,
            end_time_utc: dt.datetime,
            page_size: int = 10000,
            return_bounds: bool = False,
            max_pages: int = 100_000,
    ) -> List[HistoryValue]:
        """
        Vollständiges, lückenfreies Paging über den gesamten Zeitbereich.

        Abbruchbedingungen (in Priorität):
          1. Leerer Batch vom Server
          2. Kein Fortschritt im Timestamp (Endlos-Loop-Schutz)
          3. current_start > end_utc
          4. max_pages erreicht

        ENTFERNT: `if len(batch) < page_size: break`
        → Der Server darf jederzeit weniger als page_size liefern,
          ohne dass das Ende des Datensatzes erreicht ist.
        """
        start = ensure_aware_utc(start_time_utc)
        end   = ensure_aware_utc(end_time_utc)

        all_values: List[HistoryValue] = []
        current_start = start
        last_ts: Optional[dt.datetime] = None
        page_idx = 0

        while page_idx < max_pages:
            if current_start > end:
                break

            batch = await self.read_history_raw(
                node_id=node_id,
                start_time_utc=current_start,
                end_time_utc=end,
                num_values=page_size,
                return_bounds=return_bounds if page_idx == 0 else False,
            )

            # ── Abbruch 1: Server liefert nichts mehr ─────────────────
            if not batch:
                break

            # ── Deduplizierung: alles <= last_ts verwerfen ─────────────
            if last_ts is not None:
                batch = [
                    hv for hv in batch
                    if hv.source_timestamp is None
                    or ensure_aware_utc(hv.source_timestamp) > last_ts
                ]
                # ── Abbruch 2: kein Fortschritt ───────────────────────
                if not batch:
                    break

            all_values.extend(batch)

            # ── Neuen last_ts bestimmen ────────────────────────────────
            new_last_ts: Optional[dt.datetime] = None
            for hv in reversed(batch):
                if hv.source_timestamp is not None:
                    new_last_ts = ensure_aware_utc(hv.source_timestamp)
                    break

            if new_last_ts is None:
                break  # Kein Timestamp → können nicht weiter paginieren

            # ── Abbruch 3: Timestamp-Stillstand ───────────────────────
            if last_ts is not None and new_last_ts <= last_ts:
                break

            last_ts       = new_last_ts
            current_start = last_ts   # bewusst ohne +epsilon
            page_idx     += 1

            print(
                f"  [Paging] Seite {page_idx:>4} | "
                f"Batch {len(batch):>6} | "
                f"Gesamt {len(all_values):>8} | "
                f"bis {last_ts.isoformat()}"
            )

        return all_values


# ── Event-Loop Helper ──────────────────────────────────────────────────────

class _LoopRunner:
    def __init__(self) -> None:
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def start(self) -> None:
        if self._loop is not None and not self._loop.is_closed():
            return
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

    def run(self, coro):
        if self._loop is None or self._loop.is_closed():
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


# ── Sync Facade ────────────────────────────────────────────────────────────

class ProcessHistorianReader:
    """Sync-Wrapper um SiemensHistorianOpcUaClient — ein Loop pro Instanz."""

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
        self.node_id    = node_id
        self._loop      = _LoopRunner()
        self._connected = False
        self._client    = SiemensHistorianOpcUaClient(
            endpoint_url=endpoint_url,
            username=username,
            password=password,
            pki_dir=pki_dir,
            application_uri=application_uri,
            security_policy=security_policy,
            message_security_mode=message_security_mode,
            timeout_s=timeout_s,
        )

    def __enter__(self) -> "ProcessHistorianReader":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.disconnect()

    @property
    def trusted_store(self) -> Path:
        return self._client.trusted_certs_dir()

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

    def debug_node(self) -> None:
        self._loop.run(self._client.debug_history_capabilities(self.node_id))

    def read_history_paged(
            self,
            start_utc: dt.datetime,
            end_utc: dt.datetime,
            page_size: int,
    ) -> List[HistoryValue]:
        return self._loop.run(
            self._client.read_history_raw_paged(
                node_id=self.node_id,
                start_time_utc=start_utc,
                end_time_utc=end_utc,
                page_size=page_size,
                return_bounds=False,
            )
        )

    def probe_bounds_last_value(
            self, probe_minutes: int = 30
    ) -> Optional[HistoryValue]:
        end_utc   = aware_utc_now()
        start_utc = end_utc - dt.timedelta(minutes=probe_minutes)
        hist = self._loop.run(
            self._client.read_history_raw(
                node_id=self.node_id,
                start_time_utc=start_utc,
                end_time_utc=end_utc,
                num_values=0,
                return_bounds=True,
            )
        )
        return hist[0] if hist else None
