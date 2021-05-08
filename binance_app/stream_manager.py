from market_tick import MarketTickEntity
from market_trader import ProfessionalTrader
from util_general import setup_logger
import json

logger = setup_logger('binance_logger', '../logs/binance_data.log', msg_only=True)


class StreamManager:
    def __init__(self, binance_trader: ProfessionalTrader, event_transformer):
        self.min_event_time = 0
        self.trader = binance_trader
        self.mapper = event_transformer

    def consume(self, kline_event):
        market_tick = self.mapper(kline_event)
        if self.__should_process(market_tick):
            self.__mark_even_update(market_tick)
            self.trader.consume(market_tick)
        logger.info(json.dumps(kline_event))

    def __should_process(self, kline: MarketTickEntity):
        # return True
        return kline.window_end_epoch_seconds >= self.min_event_time

    def __mark_even_update(self, event: MarketTickEntity):
        self.min_event_time = event.window_end_epoch_seconds + 60

