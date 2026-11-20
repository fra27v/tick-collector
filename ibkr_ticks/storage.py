import hashlib
import json
import logging
import os
from pathlib import Path
import time
import uuid
from datetime import datetime, timezone

log = logging.getLogger(__name__)


class StoreLock:
    """OS lock released even after a crash; collect/compact cannot race recovery."""
    def __init__(self, root):
        self.root = Path(root)

    def __enter__(self):
        self.root.mkdir(parents=True, exist_ok=True)
        self.f = (self.root / ".lock").open("a+b")
        self.f.seek(0)
        self.f.write(b"0")
        self.f.flush()
        self.f.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self.f.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.f.close()
            raise RuntimeError("Storage already in use") from None
        return self

    def __exit__(self, *args):
        self.f.close()


def recover(root):
    for path in sorted(Path(root).glob("*.open.ndjson")):
        # Only a final incomplete line can be discarded. Validate complete lines.
        end = 0
        with path.open("rb") as f:
            for line in f:
                if not line.endswith(b"\n"):
                    log.warning("Recovery partial final line: %s (%s bytes)", path, len(line))
                    break
                json.loads(line)
                end += len(line)
        with path.open("r+b") as f:
            f.truncate(end)
            f.flush()
            os.fsync(f.fileno())
        path.rename(path.with_name(path.name.replace(".open.", ".closed.")))
        log.warning("Recovered %s complete bytes from %s", end, path)


class SegmentWriter:
    def __init__(self, config):
        self.options = config.storage
        self.root = Path(self.options["directory"]) / "segments"
        self.root.mkdir(parents=True, exist_ok=True)
        self.file = None

    def write(self, event):
        line = (json.dumps(event, separators=(",", ":"), allow_nan=False) + "\n").encode()
        now = time.monotonic()
        if self.file and (now - self.started >= self.options["segment_max_seconds"] or self.size + len(line) > self.options["segment_max_bytes"]):
            self.close()
        if not self.file:
            self.path = self.root / f"{time.time_ns()}-{uuid.uuid4().hex}.open.ndjson"
            self.file = self.path.open("xb")
            self.started = self.flushed = now
            self.size = 0
        self.file.write(line)
        self.size += len(line)
        if now - self.flushed >= self.options["flush_interval_seconds"]:
            self.flush()

    def flush(self):
        if self.file:
            self.file.flush()
            os.fsync(self.file.fileno())
            self.flushed = time.monotonic()

    def close(self):
        if self.file:
            self.flush()
            self.file.close()
            self.file = None
            self.path.rename(self.path.with_name(self.path.name.replace(".open.", ".closed.")))


def schema():
    import pyarrow as pa
    return pa.schema([
        ("contract", pa.struct([(k, pa.string()) for k in ("symbol", "sec_type", "currency", "exchange")])),
        ("instrument", pa.string()), ("source", pa.string()), ("ibkr_timestamp", pa.int64()),
        ("received_utc_ns", pa.int64()), ("run_id", pa.string()), ("sequence", pa.int64()),
        ("bid", pa.float64()), ("ask", pa.float64()), ("bid_size", pa.string()), ("ask_size", pa.string()),
        ("attributes", pa.struct([("bidPastLow", pa.bool_()), ("askPastHigh", pa.bool_())]))])


def compact(config, delete=False):
    import pyarrow as pa
    import pyarrow.parquet as pq
    root = Path(config.storage["directory"])
    count = 0
    with StoreLock(root):
        for source in sorted((root / "segments").glob("*.closed.ndjson")):
            digest = hashlib.sha256(source.read_bytes()).hexdigest()
            groups = {}
            with source.open(encoding="utf-8") as f:
                for line in f:
                    event = json.loads(line)
                    day = datetime.fromtimestamp(event["received_utc_ns"] // 1_000_000_000, timezone.utc).date().isoformat()
                    instrument = event["instrument"]
                    if not instrument or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-" for c in instrument):
                        raise ValueError("Unsafe instrument")
                    groups.setdefault((instrument, day), []).append(event)
            for (instrument, day), rows in groups.items():
                table = pa.Table.from_pylist(rows, schema=schema())
                if table.to_pylist() != rows:
                    raise ValueError(f"Schema conversion changed records: {source}")
                folder = root / "parquet" / f"instrument={instrument}" / f"date={day}"
                folder.mkdir(parents=True, exist_ok=True)
                identity = hashlib.sha256((source.name + digest).encode()).hexdigest()
                dest = folder / f"{identity}.parquet"
                def verify(path):
                    with path.open("rb") as handle:
                        result = pq.ParquetFile(handle).read()
                    if not result.schema.equals(schema()) or result.to_pylist() != rows:
                        raise ValueError(f"Parquet verification failed: {path}")
                if dest.exists():
                    verify(dest)
                else:
                    temp = dest.with_suffix(".parquet.tmp")
                    with temp.open("wb") as handle:
                        pq.write_table(table, handle, compression="zstd")
                    verify(temp)
                    with temp.open("r+b") as f:
                        os.fsync(f.fileno())
                    os.replace(temp, dest)
                count += len(rows)
            if delete:
                source.unlink()
            log.info("Verified segment=%s records=%d delete=%s", source, sum(map(len, groups.values())), delete)
    return count
