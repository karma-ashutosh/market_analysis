import abc

from trade_models import TradeResult
from market_tick import MarketTickEntity
from market_tick_analyzer import MarketTickConsolidatedOpportunityFinder, IndicatorDirection
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
    def __init__(self, trade_executor: TradeExecutor, opportunity_finder: MarketTickConsolidatedOpportunityFinder,
                 take_longs: bool = True, take_shorts: bool = False, profit_threshold=2, stoploss_threshold=2):
        super().__init__(trade_executor, opportunity_finder)
        self.current_long_pos: TradeResult = None
        self.current_short_pos: TradeResult = None
        self.take_longs = take_longs
        self.take_shorts = take_shorts

        self.profit_threshold = profit_threshold
        self.stoploss_threshold = stoploss_threshold
        # self.stoploss_threshold = 2
        self.prev_high = -1

    def __long_book_profit(self, evaluation_price: float):
        buying_price = self.current_long_pos.trade_price
        threshold_price = buying_price * (100 + self.profit_threshold) / 100
        return evaluation_price > threshold_price, threshold_price

    def __long_book_loss(self, evaluation_price: float):
        # buying_price = self.current_long_pos.trade_price
        buying_price = self.prev_high
        threshold_price = buying_price * (100 - self.stoploss_threshold) / 100
        should_book_loss = evaluation_price < threshold_price

        # if should_book_loss:
        #     print("recommending threshold_price of : {} for booking loss against buy_price: {}".format(threshold_price,
        #                                                                                                buying_price))
        return should_book_loss, threshold_price

    def consume(self, event: MarketTickEntity):
        opportunity = self.opportunity_finder.find_opportunity(event)

        trade_executed = False
        long_result = None

        if self.take_longs and self.current_long_pos:
            high, low = event.high, event.low
            should_book_loss, loss_threshold = self.__long_book_loss(low)
            should_book_profit, profit_threshold = self.__long_book_profit(high)
            # assuming worse case first then best case
            if should_book_loss:
                long_result = self.trade_executor.sell(event, opportunity, price=loss_threshold)
                self.current_long_pos = None
                trade_executed = True
            elif should_book_profit:
                long_result = self.trade_executor.sell(event, opportunity, price=profit_threshold)
                self.current_long_pos = None
                trade_executed = True
            else:
                self.prev_high = event.high if event.high > self.prev_high else self.prev_high

        if self.take_longs and not trade_executed:
            if opportunity.direction is IndicatorDirection.POSITIVE:
                if not self.current_long_pos:
                    # from technical_value_calculator import TechCalc
                    # obv_strength = sum(TechCalc.OBV(self.opportunity_finder.cur_df))
                    # print("obv_strength: {}".format(obv_strength))
                    # if obv_strength > 3:
                    long_result: TradeResult = self.trade_executor.buy(event, opportunity)
                    self.current_long_pos = long_result
                    trade_executed = True
                    self.prev_high = long_result.trade_price
            elif opportunity.direction is IndicatorDirection.NEGATIVE:
                if self.current_long_pos:
                    long_result = self.trade_executor.sell(event, opportunity)
                    self.current_long_pos = None
                    trade_executed = True
            else:
                long_result = {'opp_type': opportunity.direction.name}

        short_result = None
        if self.take_shorts:
            if opportunity.direction is IndicatorDirection.POSITIVE:
                short_result = self.trade_executor.square_short(event, opportunity)
                trade_executed = True
            elif opportunity.direction is IndicatorDirection.NEGATIVE:
                short_result = self.trade_executor.take_short(event, opportunity)
                trade_executed = True
            else:
                short_result = {'opp_type': opportunity.direction.name}

        if trade_executed:
            app_logger.info("executed trade successfully: {}\nReturning result long: {}, short: {}"
                            .format(trade_executed, long_result, short_result))
        else:
            app_logger.info("No opportunity made for event at time: {}".format(event.event_time))
        return long_result, short_result
