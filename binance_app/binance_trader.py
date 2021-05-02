from kline_object import KLineEntity
from binance_analyzer import BinanceAnalyzer
from binance_provider import TradeExecutor
from binance_analyzer import TradeOpportunity, OpportunityType


class BinanceTrader:
    def __init__(self, trade_executor: TradeExecutor, analyser: BinanceAnalyzer):
        self.analyzer = analyser
        self.trade_executor: TradeExecutor = trade_executor
        self.holds_instrument = False

    def consume(self, event: KLineEntity):
        opportunity = self.__analyze(event)

        if opportunity.opp_type is OpportunityType.SKIP:
            print("No opportunity made")
            return None

        result = {}
        trade_executed = False
        if opportunity.opp_type is OpportunityType.BUY and not self.holds_instrument:
            result = self.trade_executor.buy(event.close)
            self.holds_instrument = True
            trade_executed = True
        elif opportunity.opp_type is OpportunityType.SELL and self.holds_instrument:
            result = self.trade_executor.sell(event.close)
            self.holds_instrument = False
            trade_executed = True

        print("executed trade successfully: {}".format(trade_executed))
        print("returning result: {}".format(result))
        return result

    def __analyze(self, event) -> TradeOpportunity:
        return self.analyzer.find_opportunity(event)

    def __should_sell(self) -> bool:
        pass
