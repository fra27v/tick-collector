import sys
import types
from types import SimpleNamespace
from unittest.mock import Mock

from ibkr_ticks.adapter import IBAdapter
from ibkr_ticks.collector import Collector
from ibkr_ticks.config import Config


def test_official_sdk_boundary(monkeypatch):
    class Client:
        def __init__(self, wrapper):
            self.reqTickByTickData = Mock()
            self.disconnect = Mock()
    for name, attr, value in (("client", "EClient", Client), ("wrapper", "EWrapper", type("Wrapper", (), {})),
                              ("contract", "Contract", type("Contract", (), {}))):
        module = types.ModuleType(f"ibapi.{name}")
        setattr(module, attr, value)
        monkeypatch.setitem(sys.modules, f"ibapi.{name}", module)
    sink = Collector(Config(), None)
    adapter = IBAdapter(sink)
    assert not sink.ready.is_set()
    adapter.app.nextValidId(123)
    assert sink.ready.is_set()
    adapter.subscribe()
    adapter.app.reqTickByTickData.assert_called_once_with(1, adapter.contract, "BidAsk", 0, False)
    assert adapter.contract.secType == "CASH"
    adapter.app.tickByTickBidAsk(1, 123, 1., 2., "1.001", "2.002", SimpleNamespace(bidPastLow=True, askPastHigh=False))
    assert sink.queue.get_nowait()["bid_size"] == "1.001"
    adapter.app.error(1, 1100, "lost")
    assert sink.ready.is_set()
    assert not sink.ibkr_connected
    adapter.app.error(1, 1234567890, 1102, "restored", "")
    assert sink.ready.is_set()
    assert sink.ibkr_connected
