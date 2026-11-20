"""Only module importing the official SDK; no trading/account requests."""
import threading


class IBAdapter:
    def __init__(self, sink):
        from ibapi.client import EClient
        from ibapi.wrapper import EWrapper
        from ibapi.contract import Contract
        self.sink = sink

        class App(EWrapper, EClient):
            def __init__(self):
                EWrapper.__init__(self)
                EClient.__init__(self, self)

            def nextValidId(self, orderId):
                sink.ready.set()

            def connectionClosed(self):
                sink.lost.set()

            def error(self, reqId, *args):
                # SDK releases expose either code/message or time/code/message.
                if len(args) >= 3 and isinstance(args[1], int):
                    sink.error(args[1], args[2])
                elif len(args) >= 2:
                    sink.error(args[0], args[1])

            def tickByTickBidAsk(self, reqId, timestamp, bidPrice, askPrice, bidSize, askSize, tickAttribBidAsk):
                sink.tick(timestamp, bidPrice, askPrice, bidSize, askSize,
                    dict(bidPastLow=bool(tickAttribBidAsk.bidPastLow), askPastHigh=bool(tickAttribBidAsk.askPastHigh)))

        self.app = App()
        self.contract = Contract()
        for key, value in sink.config.contract.items():
            setattr(self.contract, "secType" if key == "sec_type" else key, value)
        self.thread = None

    def connect(self):
        c = self.sink.config.ibkr
        self.app.connect(c["host"], c["port"], c["client_id"])
        self.thread = threading.Thread(target=self.app.run, name="ibapi-reader", daemon=True)
        self.thread.start()

    def subscribe(self):
        self.app.reqTickByTickData(1, self.contract, "BidAsk", 0, False)

    def close(self):
        self.app.disconnect()
        if self.thread:
            self.thread.join(timeout=5)
            if self.thread.is_alive():
                raise RuntimeError("IBAPI reader did not stop")
