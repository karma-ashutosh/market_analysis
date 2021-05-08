from market_entity import MarketTickEntity
from binance_trader import BinanceTrader
from util_general import setup_logger
import json

logger = setup_logger('binance_logger', '../logs/binance_data.log', msg_only=True)


class StreamManager:
    def __init__(self, binance_trader: BinanceTrader):
        self.min_event_time = 0
        self.trader = binance_trader

    def consume(self, kline_event):
        kline = MarketTickEntity.map_from_binance_kline(kline_event)
        if self.__should_process(kline):
            self.__mark_even_update(kline)
            self.trader.consume(kline)
        logger.info(json.dumps(kline_event))

    def __should_process(self, kline: MarketTickEntity):
        # return True
        return kline.window_end_epoch_seconds >= self.min_event_time

    def __mark_even_update(self, event: MarketTickEntity):
        self.min_event_time = event.window_end_epoch_seconds + 60

