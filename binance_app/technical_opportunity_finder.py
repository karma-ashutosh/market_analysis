import abc

from analyzer_models import Opportunity, IndicatorDirection, IndicatorIntensity
from strategy_config import MovingAvgParams, MACDParams
from technical_value_calculator import TechCalc


class OpportunityFinder:
    def __init__(self, cur_tick, cur_df):
        self.cur_df = cur_df
        self.cur_tick = cur_tick
        self.fixed_intensity = IndicatorIntensity.THREE

    def cur_opportunity(self) -> Opportunity:
        opp_type = self.opportunity_type()
        intensity = self.__opportunity_intensity()
        opportunity = Opportunity(self.cur_tick, opp_type, intensity, attrs=self.indicator_summary())
        return opportunity

    def __opportunity_intensity(self):
        return self.fixed_intensity

    @abc.abstractmethod
    def indicator_summary(self) -> dict:
        pass

    @abc.abstractmethod
    def opportunity_type(self) -> IndicatorDirection:
        pass


class DifferenceBasedOpportunityFinder(OpportunityFinder):
    def __init__(self, cur_tick, cur_df):
        super().__init__(cur_tick, cur_df)

    def opportunity_type(self) -> IndicatorDirection:
        result = None
        if self.diff_prev() < 0 and self.diff_cur() >= 0:
            result = IndicatorDirection.POSITIVE

        elif self.diff_prev() >= 0 and self.diff_cur() < 0:
            result = IndicatorDirection.NEGATIVE

        elif self.diff_prev() >= 0 and self.diff_cur() >= 0:
            result = IndicatorDirection.POSITIVE_SUSTAINED

        elif self.diff_prev() < 0 and self.diff_cur() < 0:
            result = IndicatorDirection.NEGATIVE_SUSTAINED

        return result

    @abc.abstractmethod
    def diff_prev(self):
        pass

    @abc.abstractmethod
    def diff_cur(self):
        pass


class MovingAvgOpportunityFinder(DifferenceBasedOpportunityFinder):

    def __init__(self, cur_tick, cur_df, config=MovingAvgParams.params):
        super().__init__(cur_tick, cur_df)

        consts = MovingAvgParams
        small_window, large_window = config[consts.SMALL], config[consts.LARGE]
        self.cur_tick = cur_tick

        self.large_window_avg = TechCalc.EMA(cur_df, large_window)
        self.small_window_avg = TechCalc.EMA(cur_df, small_window)
        self.diffs = [self.small_window_avg[i] - self.large_window_avg[i] for i in range(len(self.large_window_avg))]

    def indicator_summary(self) -> dict:
        pass

    def diff_prev(self):
        return self.diffs[-2]

    def diff_cur(self):
        return self.diffs[-1]


class MACDOpportunityFinder(DifferenceBasedOpportunityFinder):
    def __init__(self, cur_tick, cur_df, config=MACDParams.params):
        super().__init__(cur_tick, cur_df)

        const = MACDParams
        self.slow = config[const.SLOW]
        self.fast = config[const.FAST]
        self.signal = config[const.SIGNAL]
        df1, df2 = TechCalc.MACD(self.cur_df, self.fast, self.slow, self.signal)
        self.macd_values = df1.to_list()
        self.signal_values = df2.to_list()
        self.macd_diff = (df1 - df2).to_list()

    def indicator_summary(self) -> dict:
        return {
            "macd_prev": self.macd_values[-2],
            "macd_cur": self.macd_values[-1],
            "signal_prev": self.macd_values[-2],
            "signal_cur": self.macd_values[-1]
        }

    def diff_cur(self):
        return self.macd_diff[-1]

    def diff_prev(self):
        return self.macd_diff[-2]
