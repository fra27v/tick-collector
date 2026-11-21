from ibkr_ticks.collector import Collector
from ibkr_ticks.config import Config


def test_pacing_measured_after_slow_connect(tmp_path):
    config = Config()
    config.storage.update(directory=str(tmp_path), minimum_free_bytes=0)
    now, requests, adapters = [0], [], []

    class Feed:
        def __init__(self, sink):
            self.sink = sink
            adapters.append(self)

        def connect(self):
            if len(adapters) == 1:
                now[0] += 10
            self.sink.ready.set()

        def subscribe(self):
            requests.append(now[0])
            if len(requests) == 2:
                self.sink.stop.set()

        def close(self):
            pass

    def wait(_):
        now[0] += 1
        if now[0] == 11:
            collector.lost.set()
        assert now[0] <= 26, "second subscription never sent"

    collector = Collector(config, Feed, clock=lambda: now[0], wait=wait)
    collector.run()
    assert len(adapters) == 2
    assert requests == [10, 25]
    assert collector.last_request_at == 25


def test_latest_outage_suspends_pending_recovery(tmp_path):
    config = Config()
    config.storage.update(directory=str(tmp_path), minimum_free_bytes=0)
    now, requests = [0], []

    class Feed:
        def __init__(self, sink):
            self.sink = sink

        def connect(self):
            self.sink.ready.set()

        def subscribe(self):
            requests.append(now[0])

        def close(self):
            pass

    def wait(_):
        now[0] += 1
        if now[0] == 1:
            collector.error(1100, "first outage")
            collector.error(1101, "first recovery")
            collector.error(1100, "second outage before next iteration")
            assert collector.ready.is_set()  # socket handshake remains valid
            assert not collector.ibkr_connected
        if 1 < now[0] <= 20:
            assert requests == [0]
            # An active feed callback is preserved even while recovery waits.
            collector.tick(1, 1., 2., 3, 4, dict(bidPastLow=False, askPastHigh=False))
        if now[0] == 20:
            collector.error(1101, "second recovery")
            collector.error(1101, "duplicate recovery")
        if now[0] == 21:
            collector.error(1101, "duplicate after request")
        if now[0] == 40:
            collector.stop.set()

    collector = Collector(config, Feed, clock=lambda: now[0], wait=wait)
    collector.run()
    assert requests == [0, 20]
    assert collector.written == 19
