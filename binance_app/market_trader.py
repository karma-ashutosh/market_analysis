import abc

from market_tick import MarketTickEntity
from market_tick_analyzer import MarketTickConsolidatedOpportunityFinder, Opportunity, IndicatorDirection, \
    IndicatorIntensity
from provider import TradeExecutor
from util_general import app_logger


class MarketTrader:
    def __init__(self, trade_executor: TradeExecutor, opportunity_finder: MarketTickConsolidatedOpportunityFinder):
        self.opportunity_finder = opportunity_finder
        self.trade_executor: TradeExecutor = trade_executor

    @abc.abstractmethod
    def consume(self, event: MarketTickEntity):
        pass


class ProfessionalTrader(MarketTrader):
    def __init__(self, trade_executor: TradeExecutor, opportunity_finder: MarketTickConsolidatedOpportunityFinder):
        super().__init__(trade_executor, opportunity_finder)
        self.holds_instrument = False

    def consume(self, event: MarketTickEntity):
        opportunity = self.opportunity_finder.find_opportunity(event)

        if opportunity.direction is IndicatorDirection.NEGATIVE_SUSTAINED \
                or opportunity.direction is IndicatorDirection.POSITIVE_SUSTAINED \
                or opportunity.direction is IndicatorDirection.NOT_ANALYZED:
            app_logger.info("No opportunity made for event at time: {}".format(event.event_time))
            return None

        result = {}
        trade_executed = False
        if opportunity.direction is IndicatorDirection.POSITIVE and not self.holds_instrument:
            result = self.trade_executor.buy(event)
            self.holds_instrument = True
            trade_executed = True
        elif opportunity.direction is IndicatorDirection.NEGATIVE and self.holds_instrument:
            result = self.trade_executor.sell(event)
            self.holds_instrument = False
            trade_executed = True
        else:
            result = {'opp_type': opportunity.direction.name}

        app_logger.info("executed trade successfully: {}\nReturning result: {}".format(trade_executed, result))
        return result
