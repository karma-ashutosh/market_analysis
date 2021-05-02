from pandas import DataFrame
from finta import TA

from kline_object import KLineEntity
from enum import Enum


class OpportunityType(Enum):
    BUY = 1,
    SELL = 2.
    SKIP = 3


class Stats:
    def __init__(self, small_window_avg, large_window_avg):
        self.large_window_avg = large_window_avg
        self.small_window_avg = small_window_avg
        self.diffs = [small_window_avg[i] - large_window_avg[i] for i in range(len(large_window_avg))]

    def small_prev(self):
        return self.small_window_avg[-2]

    def small_cur(self):
        return self.small_window_avg[-1]

    def large_prev(self):
        return self.large_window_avg[-2]

    def large_cur(self):
        return self.large_window_avg[-1]

    def diff_prev(self):
        return self.diffs[-2]

    def diff_cur(self):
        return self.diffs[-1]

    def opportunity_type(self) -> OpportunityType:
        if self.diff_cur() * self.diff_prev() >= 0:
            return OpportunityType.SKIP
        if self.diff_cur() > 0:
            return OpportunityType.BUY
        return OpportunityType.SELL


class TradeOpportunity:
    def __init__(self, opp_type: OpportunityType, event: KLineEntity, stats: Stats):
        self.event = event
        self.opp_type = opp_type
        self.stats = stats


class DataAnalyzer:
    def __init__(self):
        pass

    def give_cur_avgs(self, data_frame: DataFrame, windows: list) -> dict:
        pass


class FinTAAnalyzer(DataAnalyzer):
    def __init__(self):
        super().__init__()

    def give_cur_avgs(self, data_frame: DataFrame, windows: list):
        result = {}
        for window in windows:
            result[window] = TA.EMA(data_frame, window)
        return result


class BinanceAnalyzer:
    def __init__(self, small_window=1, large_window=5):
        self.events = []
        self.larger_window = large_window
        self.small_window = small_window
        self.window_length = self.larger_window + 2  # how much older data to keep to calculate moving avg
        self.df_cols = ['epoch_seconds', 'open', 'high', 'low', 'close', 'volume']
        self.cur_df: DataFrame = DataFrame([], columns=self.df_cols)
        self.analyzer: DataAnalyzer = FinTAAnalyzer()

    def find_opportunity(self, kline: KLineEntity) -> TradeOpportunity:
        opportunity: TradeOpportunity = None

        df_row = self.__df_from_row(kline)
        cur_row_count = self.cur_df.shape[0]
        if cur_row_count < self.window_length:
            print("current row count is {} and min limit is {}".format(cur_row_count, self.window_length))
            self.cur_df = self.cur_df.append(df_row)
            opportunity = TradeOpportunity(OpportunityType.SKIP, kline, None)
        else:
            new_df = self.cur_df.append(df_row)
            self.cur_df = new_df.iloc[1:].copy()

            moving_avgs = self.analyzer.give_cur_avgs(self.cur_df, [self.larger_window, self.small_window])
            small, large = moving_avgs[self.small_window].tolist(), moving_avgs[self.larger_window].tolist()
            stats = Stats(small, large)
            opportunity_type = stats.opportunity_type()

            print("generated opportunity of type: {} at event: {}".format(opportunity_type.name, kline.raw_json()))
            opportunity = TradeOpportunity(opportunity_type, kline, stats)

        return opportunity

    def __df_from_row(self, kline: KLineEntity) -> DataFrame:
        # vals = {
        #             'epoch_seconds': kline.window_end_epoch_seconds,
        #             'open': kline.open,
        #             'high': kline.high,
        #             'low': kline.low,
        #             'close': kline.close,
        #             'volume': kline.volume
        # }
        vals = [[kline.window_end_epoch_seconds, kline.open, kline.high, kline.low, kline.close, kline.volume]]
        return DataFrame(vals, columns=self.df_cols)
