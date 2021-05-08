from pandas import DataFrame

from analyzer_models import Opportunity, IndicatorDirection, IndicatorIntensity
from opportunity_finder import MovingAvgOpportunityFinder, MACDOpportunityFinder
from market_entity import MarketTickEntity
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
            new_df = self._cur_df.append(df_row)
            self._cur_df = new_df.iloc[1:].copy()

        return self._cur_df

    def __df_from_row(self, tick: MarketTickEntity) -> DataFrame:
        values = [[tick.window_end_epoch_seconds, tick.open, tick.high, tick.low, tick.close, tick.volume]]
        return DataFrame(values, columns=self._df_cols)


class BinanceAnalyzer:
    def __init__(self, min_sample_window=30):
        self.events = []
        self.window_length = min_sample_window
        self.moving_df = MovingDF(self.window_length)

    def find_opportunity(self, tick: MarketTickEntity) -> Opportunity:
        cur_df = self.moving_df.generate_snapshot(tick)

        cur_row_count = cur_df.shape[0]
        if cur_row_count < self.window_length:
            app_logger.info("current row count is {} and min limit is {}".format(cur_row_count, self.window_length))
            opportunity = Opportunity(tick, IndicatorDirection.POSITIVE_SUSTAINED, IndicatorIntensity.ZERO)
        else:
            # opportunity = MovingAvgOpportunityFinder(tick, cur_df).cur_opportunity()
            opportunity = MACDOpportunityFinder(tick, cur_df).cur_opportunity()

        return opportunity
