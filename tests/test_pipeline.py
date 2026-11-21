import json
from decimal import Decimal
from pathlib import Path
import threading
import time

import pyarrow.parquet as pq
import pytest

from ibkr_ticks.config import Config, load
from ibkr_ticks.collector import Collector
from ibkr_ticks.storage import SegmentWriter, compact, recover, StoreLock


def config(tmp_path):
    c = Config()
    c.storage.update(directory=str(tmp_path), minimum_free_bytes=0, segment_max_bytes=800,
                     status_interval_seconds=0.05)
    return c


def read_raw(root):
    return [json.loads(line) for p in sorted((root / "segments").glob("*.closed.ndjson")) for line in p.read_text().splitlines()]


def test_end_to_end_reconnect_and_compact(tmp_path):
    c = config(tmp_path)
    c.ibkr["tick_request_min_interval_seconds"] = 0
    adapters = []
    captured = []
    class Feed:
        def __init__(self, sink):
            self.sink = sink
            self.calls = 0
            adapters.append(self)
        def connect(self):
            self.sink.ready.set()
        def subscribe(self):
            self.calls += 1
            for size in ("100.000000000000000001", "101.25", "101.25"):
                self.sink.tick(1700000000, 1.1, 1.2, Decimal(size), Decimal("200.50"),
                               dict(bidPastLow=False, askPastHigh=True))
            if len(adapters) == 2:
                self.sink.stop.set()
        def close(self):
            pass
    collector = Collector(c, Feed)
    original = collector.queue.put_nowait
    def capture(event):
        captured.append(event)
        original(event)
    collector.queue.put_nowait = capture
    def steer():
        deadline = time.monotonic() + 5
        while collector.sequence < 3 and time.monotonic() < deadline:
            time.sleep(.01)
        collector.error(1100, "lost")
        collector.error(1102, "maintained")
        time.sleep(.15)
        assert adapters[0].calls == 1
        collector.error(1100, "lost")
        collector.error(1101, "lost data")
        collector.error(1101, "repeated")
        while collector.sequence < 6 and time.monotonic() < deadline:
            time.sleep(.01)
        collector.error(1101, "repeated after subscription")
        time.sleep(.15)
        collector.lost.set()
    worker = threading.Thread(target=steer)
    worker.start()
    collector.run()
    worker.join()
    assert [a.calls for a in adapters] == [2, 1]
    assert collector.written == 9
    assert read_raw(tmp_path) == captured
    assert captured[1]["bid_size"] == captured[2]["bid_size"]
    assert captured[0]["bid_size"] == "100.000000000000000001"
    assert compact(c) == 9
    paths = sorted(tmp_path.rglob("*.parquet"))
    assert compact(c) == 9
    assert sorted(tmp_path.rglob("*.parquet")) == paths
    actual = [row for p in paths for row in pq.ParquetFile(p).read().to_pylist()]
    assert sorted(actual, key=lambda x: x["sequence"]) == captured
    assert compact(c, delete=True) == 9
    assert not list((tmp_path / "segments").glob("*.ndjson"))
    assert compact(c) == 0


def test_recovery(tmp_path, caplog):
    c = config(tmp_path)
    collector = Collector(c, None)
    collector.tick(1, 1., 2., Decimal("1.23"), Decimal("2.34"), dict(bidPastLow=False, askPastHigh=False))
    event = collector.queue.get_nowait()
    folder = tmp_path / "segments"
    folder.mkdir()
    p = folder / "crash.open.ndjson"
    p.write_bytes((json.dumps(event) + "\n").encode() + b'{"broken":')
    recover(folder)
    assert "partial final line" in caplog.text
    assert read_raw(tmp_path) == [event]
    assert compact(c) == 1


def test_overflow_and_low_disk(tmp_path):
    c = config(tmp_path)
    c.storage["queue_capacity"] = 1
    collector = Collector(c, None)
    for _ in range(2):
        collector.tick(1, 1., 2., 1, 2, dict(bidPastLow=False, askPastHigh=False))
    assert collector.stop.is_set()
    assert "saturated" in collector.failure
    c.storage["minimum_free_bytes"] = 10**30
    with pytest.raises(RuntimeError, match="Low disk"):
        Collector(c, lambda _: pytest.fail("must not connect")).run()


def test_corrupt_output_prevents_deletion(tmp_path):
    c = config(tmp_path)
    collector = Collector(c, None)
    collector.tick(1, 1., 2., 1, 2, dict(bidPastLow=False, askPastHigh=False))
    writer = SegmentWriter(c)
    writer.write(collector.queue.get_nowait())
    writer.close()
    compact(c)
    next(tmp_path.rglob("*.parquet")).write_bytes(b"corrupt")
    with pytest.raises(Exception):
        compact(c, delete=True)
    assert read_raw(tmp_path)


def test_config_and_lock(tmp_path):
    p = tmp_path / "config.toml"
    p.write_text('[storage]\ndirectory="local"\n')
    assert load(p).storage["directory"] == str(tmp_path / "local")
    for text in ('[ibkr]\nport=true', '[storage]\nqueue_capacity=1.5', '[contract]\nsymbol="../bad"', '[storage]\nunknown=1'):
        p.write_text(text)
        with pytest.raises(ValueError):
            load(p)
    with StoreLock(tmp_path):
        with pytest.raises(RuntimeError, match="already in use"):
            with StoreLock(tmp_path):
                pass


def test_rotation_partitions_and_writer_error(tmp_path, monkeypatch):
    c = config(tmp_path)
    c.storage["segment_max_seconds"] = .001
    collector = Collector(c, None)
    writer = SegmentWriter(c)
    for day in (0, 86400):
        collector.tick(1, 1., 2., 1, 2, dict(bidPastLow=False, askPastHigh=False))
        event = collector.queue.get_nowait()
        event["received_utc_ns"] = day * 10**9
        writer.write(event)
        time.sleep(.01)
    writer.close()
    assert len(list((tmp_path / "segments").glob("*.closed.ndjson"))) == 2
    assert compact(c) == 2
    assert len(list((tmp_path / "parquet").glob("*/date=*"))) == 2
    def broken(self, event):
        raise OSError("disk unavailable")
    monkeypatch.setattr(SegmentWriter, "write", broken)
    class Feed:
        def __init__(self, sink): self.sink = sink
        def connect(self): self.sink.ready.set()
        def subscribe(self): self.sink.tick(1, 1., 2., 1, 2, {})
        def close(self): pass
    with pytest.raises(RuntimeError, match="disk unavailable"):
        Collector(c, Feed).run()
