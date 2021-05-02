from kline_object import KLineEntity
from binance_analyzer import BinanceAnalyzer
from binance_provider import TradeExecutor
from binance_analyzer import TradeOpportunity, OpportunityType


class BinanceTrader:
    def __init__(self, trade_executor: TradeExecutor, analyser: BinanceAnalyzer):
        self.analyzer = analyser
        self.trade_executor: TradeExecutor = trade_executor
        self.net_shares = 0

    def consume(self, event: KLineEntity):
        opportunity = self.__analyze(event)

        if opportunity.opp_type is OpportunityType.SKIP:
            print("No opportunity made")
            return None

        result = {}
        if opportunity.opp_type is OpportunityType.BUY:
            result = self.trade_executor.buy(1)
            self.net_shares = self.net_shares + 1
        elif opportunity.opp_type is OpportunityType.SELL:
            if self.net_shares > 0:
                result = self.trade_executor.sell(1)
                self.net_shares = self.net_shares - 1
        print("returning result: {}".format(result))
        return result

    def __analyze(self, event) -> TradeOpportunity:
        return self.analyzer.find_opportunity(event)

    def __should_sell(self) -> bool:
        pass
