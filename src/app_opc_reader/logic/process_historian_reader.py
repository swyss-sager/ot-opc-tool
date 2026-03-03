import asyncio
import datetime as dt
from pathlib import Path
from typing import Any, List, Optional, Tuple

from asyncua import Client, ua
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from src.app.logic.helper import (
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
        self.client_key_path = self.pki_dir / "own" / "private" / "client_key.pem"

        self.security_string = f"{security_policy},{message_security_mode},{self.client_cert_path},{self.client_key_path}"
        self._client: Optional[Client] = None

    def _ensure_pki_folders(self) -> None:
        (self.pki_dir / "own" / "certs").mkdir(parents=True, exist_ok=True)
        (self.pki_dir / "own" / "private").mkdir(parents=True, exist_ok=True)
        (self.pki_dir / "trusted" / "certs").mkdir(parents=True, exist_ok=True)
        (self.pki_dir / "rejected" / "certs").mkdir(parents=True, exist_ok=True)

    def trusted_certs_dir(self) -> Path:
        self._ensure_pki_folders()
        return (self.pki_dir / "trusted" / "certs").resolve()

    def _cert_contains_app_uri(self, cert_der: bytes, app_uri: str) -> bool:
        try:
            cert = x509.load_der_x509_certificate(cert_der)
            try:
                san = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
            except Exception:
                return False
            uris = san.get_values_for_type(x509.UniformResourceIdentifier)
            return app_uri in uris
        except Exception:
            return False

    def ensure_client_certificate(self, common_name: str = "Python OPC UA Client") -> Tuple[Path, Path]:
        """
        Wichtig: Wenn Zertifikat existiert, aber NICHT zur application_uri passt,
        wird es automatisch neu erzeugt (sonst BadCertificateUriInvalid).
        """
        self._ensure_pki_folders()

        if self.client_cert_path.exists() and self.client_key_path.exists():
            cert_der = self.client_cert_path.read_bytes()
            if self._cert_contains_app_uri(cert_der, self.application_uri):
                return (self.client_cert_path, self.client_key_path)

            # Regenerate if URI mismatch
            self.client_cert_path.unlink(missing_ok=True)
            self.client_key_path.unlink(missing_ok=True)

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

        subject = issuer = x509.Name(
            [
                x509.NameAttribute(NameOID.COMMON_NAME, common_name),
                x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Local"),
            ]
        )

        now_naive_utc = aware_utc_now().replace(tzinfo=None)

        cert_builder = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now_naive_utc - dt.timedelta(minutes=5))
            .not_valid_after(now_naive_utc + dt.timedelta(days=3650))
            .add_extension(
                x509.SubjectAlternativeName([x509.UniformResourceIdentifier(self.application_uri)]),
                critical=False,
            )
            .add_extension(
                x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]),
                critical=False,
            )
        )

        cert = cert_builder.sign(private_key=key, algorithm=hashes.SHA256())

        self.client_key_path.write_bytes(
            key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )
        self.client_cert_path.write_bytes(cert.public_bytes(serialization.Encoding.DER))
        return (self.client_cert_path, self.client_key_path)

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
        start_time_utc = to_naive_utc(start_time_utc)
        end_time_utc = to_naive_utc(end_time_utc)

        dvs = await node.read_raw_history(
            starttime=start_time_utc,
            endtime=end_time_utc,
            numvalues=num_values,
            return_bounds=return_bounds,
        )

        out: List[HistoryValue] = []
        for dv in dvs:
            out.append(
                HistoryValue(
                    source_timestamp=dv.SourceTimestamp,
                    server_timestamp=dv.ServerTimestamp,
                    value=dv.Value.Value if dv.Value is not None else None,
                    status_code=str(dv.StatusCode),
                )
            )
        return out


    async def read_history_raw_paged(
            self,
            node_id: str,
            start_time_utc: dt.datetime,
            end_time_utc: dt.datetime,
            page_size: int = 10000,
            return_bounds: bool = False,
            max_pages: int = 100000,
    ) -> List[HistoryValue]:
        """
        Lückenfreies Paging:
        - KEIN +1µs (das kann Werte mit identischem Timestamp überspringen)
        - Stattdessen: Start = last_ts und am Anfang der nächsten Seite alles <= last_ts verwerfen
        - Abbruch, wenn kein Fortschritt mehr möglich ist
        """
        start = ensure_aware_utc(start_time_utc)
        end = ensure_aware_utc(end_time_utc)

        all_values: List[HistoryValue] = []
        current_start = start
        last_ts: Optional[dt.datetime] = None

        for page_idx in range(max_pages):
            if current_start > end:
                break

            batch = await self.read_history_raw(
                node_id=node_id,
                start_time_utc=current_start,
                end_time_utc=end,
                num_values=page_size,
                return_bounds=return_bounds if page_idx == 0 else False,
            )

            if not batch:
                break

            # Wenn wir schon Daten haben: alles entfernen, was <= last_ts ist (Dedup/Progress)
            if last_ts is not None:
                filtered: List[HistoryValue] = []
                for hv in batch:
                    if hv.source_timestamp is None:
                        # selten, aber: ohne ts können wir nicht sauber paginieren
                        # -> behalten, sonst potentiell Datenverlust
                        filtered.append(hv)
                        continue
                    ts = ensure_aware_utc(hv.source_timestamp)
                    if ts > last_ts:
                        filtered.append(hv)

                batch = filtered
                if not batch:
                    # Kein Fortschritt => abbrechen, sonst Endlosschleife
                    break

            all_values.extend(batch)

            # neuen last_ts bestimmen (letzter gültiger SourceTimestamp)
            new_last_ts = None
            for hv in reversed(batch):
                if hv.source_timestamp is not None:
                    new_last_ts = ensure_aware_utc(hv.source_timestamp)
                    break

            if new_last_ts is None:
                # Ohne Timestamp können wir nicht weiter paginieren
                break

            # Fortschritt prüfen
            if last_ts is not None and new_last_ts <= last_ts:
                break

            last_ts = new_last_ts
            current_start = last_ts  # bewusst ohne +epsilon

            # Wenn Server weniger als page_size liefert, sind wir i.d.R. am Ende
            if len(batch) < page_size:
                break

        return all_values


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
                self._loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        finally:
            self._loop.close()
            self._loop = None


class ProcessHistorianReader:
    """
    Konsolidierter Sync-Facade:
      - ein Event-Loop pro Instanz
      - connect/debug/read/disconnect im selben Loop
      - Bounds-Probe vorhanden
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

    def read_history_paged(self, start_utc: dt.datetime, end_utc: dt.datetime, page_size: int) -> List[HistoryValue]:
        return self._loop.run(
            self._client.read_history_raw_paged(
                node_id=self.node_id,
                start_time_utc=start_utc,
                end_time_utc=end_utc,
                page_size=page_size,
                return_bounds=False,
            )
        )

    def probe_bounds_last_value(self, probe_minutes: int = 30) -> Optional[HistoryValue]:
        end_utc = aware_utc_now()
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
