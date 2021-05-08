from market_entity import MarketTickEntity
from opportunity_spotter import BinanceAnalyzer, Opportunity, IndicatorDirection, IndicatorIntensity
from binance_provider import TradeExecutor


class BinanceTrader:
    def __init__(self, trade_executor: TradeExecutor, analyser: BinanceAnalyzer):
        self.analyzer = analyser
        self.trade_executor: TradeExecutor = trade_executor
        self.holds_instrument = False

    def consume(self, event: MarketTickEntity):
        opportunity = self.analyzer.find_opportunity(event)

        if opportunity.direction is IndicatorDirection.NEGATIVE_SUSTAINED \
                or opportunity.direction is IndicatorDirection.POSITIVE_SUSTAINED:
            print("No opportunity made")
            return None

        result = {}
        trade_executed = False
        if opportunity.direction is IndicatorDirection.POSITIVE and not self.holds_instrument:
            result = self.trade_executor.buy(event.close)
            self.holds_instrument = True
            trade_executed = True
        if opportunity.direction is IndicatorDirection.NEGATIVE and self.holds_instrument:
            result = self.trade_executor.sell(event.close)
            self.holds_instrument = False
            trade_executed = True

        print("executed trade successfully: {}".format(trade_executed))
        print("returning result: {}".format(result))
        return result

