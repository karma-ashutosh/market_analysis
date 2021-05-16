from pandas import DataFrame

from analyzer_models import Opportunity, IndicatorDirection, IndicatorIntensity, PositionStrategy
from technical_opportunity_finder import OpportunityFinder, MovingAvgOpportunityFinder, MACDOpportunityFinder
from market_tick import MarketTickEntity
from util_general import app_logger


class MovingDF:
    def __init__(self, max_rows):
        self._df_cols = ['epoch_seconds', 'open', 'high', 'low', 'close', 'volume']
        self._cur_df: DataFrame = DataFrame([], columns=self._df_cols)
        self._max_rows = max_rows

    def generate_snapshot(self, tick: MarketTickEntity):
        df_row = self.__df_from_row(tick)
        cur_row_count = self._cur_df.shape[0]
        if cur_row_count < self._max_rows:
            app_logger.info("current row count is {} and min limit is {}".format(cur_row_count, self._max_rows))
            self._cur_df = self._cur_df.append(df_row)
        else:
            new_df = self._cur_df.append(df_row, ignore_index=True)
            # self._cur_df = new_df.iloc[1:].copy()
            self._cur_df = new_df.copy()

        return self._cur_df

    def __df_from_row(self, tick: MarketTickEntity) -> DataFrame:
        values = [[tick.window_end_epoch_seconds, tick.open, tick.high, tick.low, tick.close, tick.volume]]
        return DataFrame(values, columns=self._df_cols)


class MarketTickConsolidatedOpportunityFinder:
    def __init__(self, entry_strategy: PositionStrategy, exit_strategy: PositionStrategy, min_sample_window,
                 entry_params: dict, exit_params: dict):
        self.events = []
        self.window_length = min_sample_window
        self.moving_df = MovingDF(self.window_length)
        self.entry_strategy = entry_strategy
        self.exit_strategy = exit_strategy

        self.entry_params = entry_params
        self.exit_params = exit_params
        self.cur_df = None

    def find_opportunity(self, tick: MarketTickEntity) -> Opportunity:
        cur_df = self.moving_df.generate_snapshot(tick)
        self.cur_df = cur_df

        cur_row_count = cur_df.shape[0]
        if cur_row_count < self.window_length:
            app_logger.info("current row count is {} and min limit is {}".format(cur_row_count, self.window_length))
            return Opportunity(tick, IndicatorDirection.NOT_ANALYZED, IndicatorIntensity.ZERO)
        else:
            entry_opportunity = self.resolve_finder(self.entry_strategy, tick, cur_df, self.entry_params).cur_opportunity()
            exit_opportunity = self.resolve_finder(self.exit_strategy, tick, cur_df, self.exit_params).cur_opportunity()

            entry_positive = entry_opportunity.direction == IndicatorDirection.POSITIVE
            exit_negative = exit_opportunity.direction == IndicatorDirection.NEGATIVE

            if entry_positive and exit_negative:
                raise Exception("Positive and Negative both signals raised. Failing application")
            elif entry_positive:
                return entry_opportunity
            elif exit_negative:
                return exit_opportunity
            else:
                return Opportunity(tick, IndicatorDirection.NOT_ANALYZED, IndicatorIntensity.ZERO)

    @staticmethod
    def resolve_finder(strategy: PositionStrategy, cur_tick, cur_df, config) -> OpportunityFinder:
        if strategy == PositionStrategy.MovingAvg:
            return MovingAvgOpportunityFinder(cur_tick, cur_df, config)
        if strategy == PositionStrategy.MACD:
            return MACDOpportunityFinder(cur_tick, cur_df, config)

        raise Exception("Strategy {} not implemented".format(strategy.name))
