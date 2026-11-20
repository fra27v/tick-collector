import logging
from queue import Queue, Empty, Full
import shutil
import threading
import time
import uuid
from decimal import Decimal
from typing import Protocol
from pathlib import Path

from .storage import SegmentWriter, StoreLock, recover

log = logging.getLogger(__name__)


class Adapter(Protocol):
    def connect(self): ...
    def subscribe(self): ...
    def close(self): ...


class Collector:
    def __init__(self, config, adapter_factory):
        self.config = config
        self.factory = adapter_factory
        self.queue = Queue(config.storage["queue_capacity"])
        self.stop = threading.Event()
        self.ready = threading.Event()
        self.lost = threading.Event()
        self.resubscribe = threading.Event()
        self.run_id = str(uuid.uuid4())
        self.sequence = self.written = 0
        self.failure = None
        self.accepting = True
        self.recovery_processed = False
        self.callback_lock = threading.Lock()

    def fail(self, reason):
        self.failure = reason
        self.stop.set()

    def tick(self, timestamp, bid, ask, bid_size, ask_size, attributes):
        # Never wait for queue capacity or disk I/O in the SDK callback.
        with self.callback_lock:
            if not self.accepting:
                return
            self.sequence += 1
            event = dict(contract=dict(self.config.contract), instrument=self.config.instrument,
                source="IBKR.tickByTickBidAsk", ibkr_timestamp=timestamp,
                received_utc_ns=time.time_ns(), run_id=self.run_id, sequence=self.sequence,
                bid=bid, ask=ask, bid_size=str(Decimal(str(bid_size))),
                ask_size=str(Decimal(str(ask_size))), attributes=attributes)
            try:
                self.queue.put_nowait(event)
            except Full:
                self.accepting = False
                self.fail(f"Queue saturated: callback sequence={self.sequence} not persisted; acquisition incomplete")

    def error(self, code, message):
        log.warning("IBKR code=%s message=%s", code, message)
        if code == 1100:
            self.ready.clear()
            self.recovery_processed = False
        elif code == 1101:
            if not self.recovery_processed:
                self.resubscribe.set()
                self.recovery_processed = True
        elif code == 1102:
            self.recovery_processed = True
            self.ready.set()  # subscriptions maintained: do not submit again
        elif code in (502, 504, 1300):
            self.lost.set()
        elif code not in (2104, 2106, 2107, 2108, 2158):
            self.fail(f"IBKR error {code}: {message}")

    def _writer(self):
        writer = SegmentWriter(self.config)
        try:
            while not self.writer_done.is_set() or not self.queue.empty():
                try:
                    event = self.queue.get(timeout=0.1)
                except Empty:
                    if writer.file and time.monotonic() - writer.started >= self.config.storage["segment_max_seconds"]:
                        writer.close()
                    elif writer.file and time.monotonic() - writer.flushed >= self.config.storage["flush_interval_seconds"]:
                        writer.flush()
                    continue
                writer.write(event)
                self.written += 1
            writer.close()
        except Exception as exc:
            log.exception("Writer failure; segment left open for recovery")
            self.fail(f"Writer failure: {exc}")
            if writer.file:
                writer.file.close()

    def run(self):
        root = Path(self.config.storage["directory"])
        with StoreLock(root):
            recover(root / "segments")
            self.writer_done = threading.Event()
            thread = threading.Thread(target=self._writer, name="ndjson-writer")
            thread.start()
            adapter = None
            backoff = 1
            retry_at = 0
            deadline = 0
            subscribed = False
            status_at = 0
            try:
                while not self.stop.is_set():
                    now = time.monotonic()
                    if now >= status_at:
                        free = shutil.disk_usage(root).free
                        log.info("received=%d written=%d backlog=%d free_bytes=%d run=%s", self.sequence, self.written, self.queue.qsize(), free, self.run_id)
                        status_at = now + self.config.storage["status_interval_seconds"]
                        if free < self.config.storage["minimum_free_bytes"]:
                            self.fail(f"Low disk space: {free} bytes free")
                            break
                    if adapter and (self.lost.is_set() or (not subscribed and now > deadline)):
                        log.warning("Connection interrupted; retry in %ss", backoff)
                        adapter.close()
                        adapter = None
                        self.ready.clear()
                        retry_at = now + backoff
                        backoff = min(backoff * 2, 60)
                    if adapter is None and now >= retry_at:
                        self.ready.clear()
                        self.lost.clear()
                        self.resubscribe.clear()
                        self.recovery_processed = False
                        subscribed = False
                        adapter = self.factory(self)
                        deadline = now + 15
                        try:
                            adapter.connect()
                        except Exception:
                            log.exception("Connection attempt failed")
                            self.lost.set()
                    if adapter and self.resubscribe.is_set():
                        self.resubscribe.clear()
                        subscribed = False
                        self.ready.set()
                    if adapter and self.ready.is_set() and not subscribed and not self.lost.is_set():
                        adapter.subscribe()
                        subscribed = True
                        backoff = 1
                        log.info("BidAsk subscription active")
                    self.stop.wait(0.05)
            finally:
                try:
                    if adapter:
                        adapter.close()
                finally:
                    with self.callback_lock:
                        self.accepting = False
                    self.writer_done.set()
                    thread.join()
                log.info("Stopped received=%d written=%d backlog=%d reason=%s", self.sequence, self.written, self.queue.qsize(), self.failure or "signal/request")
            if self.failure:
                raise RuntimeError(self.failure)
