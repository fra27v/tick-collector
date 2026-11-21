import logging
from pathlib import Path
import subprocess
import sys
import threading
import time

import pytest

from ibkr_ticks.config import Config, load
from ibkr_ticks.collector import Collector
from ibkr_ticks.storage import compact, StoreLock

EXAMPLE = Path(__file__).resolve().parents[1] / "config.example.toml"


def test_real_example_cli(tmp_path):
    assert load(EXAMPLE).ibkr["tick_request_min_interval_seconds"] == 15
    example = tmp_path / EXAMPLE.name
    example.write_bytes(EXAMPLE.read_bytes())
    result = subprocess.run([sys.executable, "-c", "from ibkr_ticks.cli import main; raise SystemExit(main())",
                             "compact", "--config", str(example)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert (tmp_path / "data" / ".converter.lock").exists()


@pytest.mark.parametrize("value", ["-1", "nan", "inf", "-inf", "true", '"15"'])
def test_invalid_interval(tmp_path, value):
    p = tmp_path / "config.toml"
    p.write_text(f"[ibkr]\ntick_request_min_interval_seconds={value}\n")
    with pytest.raises(ValueError, match="tick_request_min_interval_seconds"):
        load(p)


@pytest.mark.parametrize("value", [0, 0.25, 5, 30])
def test_valid_interval(tmp_path, value):
    p = tmp_path / "config.toml"
    p.write_text(f"[ibkr]\ntick_request_min_interval_seconds={value}\n")
    assert load(p).ibkr["tick_request_min_interval_seconds"] == value
    p.write_text("[ibkr]\n")
    assert load(p).ibkr["tick_request_min_interval_seconds"] == 15


@pytest.mark.parametrize("interval", [None, 5, 30])
@pytest.mark.parametrize("recovery", ["1101", "socket"])
@pytest.mark.parametrize("stop_pending", [False, True])
def test_pacing_clock(tmp_path, interval, recovery, stop_pending, monkeypatch):
    c = Config()
    c.storage.update(directory=str(tmp_path), minimum_free_bytes=0, status_interval_seconds=1)
    if interval is not None:
        c.ibkr["tick_request_min_interval_seconds"] = interval
    expected = 15 if interval is None else interval
    now = [0]
    calls, adapters, disk_checks = [], [], []
    import ibkr_ticks.collector as module
    original = module.shutil.disk_usage
    monkeypatch.setattr(module.shutil, "disk_usage", lambda root: (disk_checks.append(now[0]) or original(root)))
    class Feed:
        def __init__(self, sink):
            self.sink = sink
            adapters.append(self)
        def connect(self): self.sink.ready.set()
        def close(self): pass
        def subscribe(self):
            calls.append(now[0])
            if len(calls) == 2:
                self.sink.stop.set()
    def wait(_):
        now[0] += 1
        assert now[0] <= expected + 2, "pacing request never sent"
        if now[0] == 1:
            if recovery == "socket":
                collector.lost.set()
            else:
                collector.error(1100, "lost")
                collector.error(1101, "restored")
        if recovery == "1101" and now[0] == 3:
            collector.error(1101, "duplicate")
        if stop_pending and now[0] == expected - 1:
            collector.stop.set()
    collector = Collector(c, Feed, clock=lambda: now[0], wait=wait)
    collector.run()
    assert calls == ([0] if stop_pending else [0, expected])
    assert len(adapters) == (2 if recovery == "socket" else 1)
    assert len(disk_checks) >= expected - 1


@pytest.mark.parametrize("code", [2103, 2105, 2110, 200, 354])
def test_connectivity_vs_fatal(tmp_path, caplog, code):
    caplog.set_level(logging.INFO)
    c = Config()
    c.storage.update(directory=str(tmp_path), minimum_free_bytes=0)
    class Feed:
        def __init__(self, sink): self.sink = sink
        def connect(self): self.sink.ready.set()
        def close(self): pass
        def subscribe(self):
            self.sink.error(code, "test")
            if code in (2103, 2105, 2110):
                assert not self.sink.stop.is_set()
                self.sink.tick(1, 1., 2., 3, 4, dict(bidPastLow=False, askPastHigh=False))
                self.sink.stop.set()
    collector = Collector(c, Feed)
    if code in (200, 354):
        with pytest.raises(RuntimeError, match=str(code)):
            collector.run()
    else:
        collector.run()
        assert collector.written == 1
        assert "Recoverable connectivity" in caplog.text
        assert caplog.text.index("Sending BidAsk") < caplog.text.index("First BidAsk tick received")


def test_compact_during_collection(tmp_path):
    c = Config()
    c.storage.update(directory=str(tmp_path), minimum_free_bytes=0, segment_max_bytes=1)
    class Feed:
        def __init__(self, sink): self.sink = sink
        def connect(self): self.sink.ready.set()
        def close(self): pass
        def subscribe(self):
            for _ in range(2):
                self.sink.tick(1, 1., 2., 3, 4, dict(bidPastLow=False, askPastHigh=False))
    collector = Collector(c, Feed)
    errors = []
    def run():
        try: collector.run()
        except Exception as exc: errors.append(exc)
    thread = threading.Thread(target=run)
    thread.start()
    try:
        deadline = time.monotonic() + 5
        while collector.written < 2 and time.monotonic() < deadline:
            time.sleep(.01)
        assert collector.written == 2
        opened = next((tmp_path / "segments").glob("*.open.ndjson"))
        assert compact(c) == 1
        assert compact(c, delete=True) == 1
        assert opened.exists()
        with pytest.raises(RuntimeError, match="already in use"):
            with StoreLock(tmp_path, "writer"): pass
        with StoreLock(tmp_path, "converter"):
            with pytest.raises(RuntimeError, match="already in use"):
                compact(c)
    finally:
        collector.stop.set()
        thread.join(5)
    assert not thread.is_alive()
    assert not errors
    assert compact(c) == 1


def test_1102_cancels_pending_resubscription(tmp_path):
    c = Config()
    c.storage.update(directory=str(tmp_path), minimum_free_bytes=0)
    now, calls = [0], []
    class Feed:
        def __init__(self, sink): self.sink = sink
        def connect(self): self.sink.ready.set()
        def close(self): pass
        def subscribe(self): calls.append(now[0])
    def wait(_):
        now[0] += 1
        if now[0] == 1:
            collector.error(1100, "lost")
            collector.error(1101, "data lost")
        if now[0] == 2:
            collector.error(1102, "maintained")
            collector.error(1102, "duplicate")
        if now[0] == 20:
            collector.stop.set()
    collector = Collector(c, Feed, clock=lambda: now[0], wait=wait)
    collector.run()
    assert calls == [0]
