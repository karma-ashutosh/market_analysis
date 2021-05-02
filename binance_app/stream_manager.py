from kline_object import KLineEntity
from binance_trader import BinanceTrader
from general_util import setup_logger
import json

logger = setup_logger('binance_logger', './app.log', msg_only=True)


class StreamManager:
    def __init__(self, binance_trader: BinanceTrader):
        self.min_event_time = 0
        self.trader = binance_trader

    def consume(self, kline_event):
        kline = KLineEntity(kline_event)
        if self.__should_process(kline):
            self.__mark_even_update(kline)
            self.trader.consume(kline)
        logger.info(json.dumps(kline_event))

    def __should_process(self, kline: KLineEntity):
        # return True
        return kline.window_end_epoch_seconds >= self.min_event_time

    def __mark_even_update(self, event: KLineEntity):
        self.min_event_time = event.window_end_epoch_seconds + 60

