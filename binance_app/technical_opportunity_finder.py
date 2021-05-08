import abc

import strategy_config
from analyzer_models import Opportunity, IndicatorDirection, IndicatorIntensity, PositionStrategy
from technical_value_calculator import TechCalc


class OpportunityFinder:
    def __init__(self, cur_tick, cur_df):
        self.cur_df = cur_df
        self.cur_tick = cur_tick
        self.fixed_intensity = IndicatorIntensity.THREE

    def cur_opportunity(self) -> Opportunity:
        opp_type = self.opportunity_type()
        intensity = self.__opportunity_intensity()
        opportunity = Opportunity(self.cur_tick, opp_type, intensity)
        return opportunity

    def __opportunity_intensity(self):
        return self.fixed_intensity

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

    def __init__(self, cur_tick, cur_df):
        super().__init__(cur_tick, cur_df)

        config = strategy_config.MovingAvgParams.params
        consts = strategy_config.MovingAvgParams
        small_window, large_window = config[consts.SMALL], config[consts.LARGE]
        self.cur_tick = cur_tick

        self.large_window_avg = TechCalc.EMA(cur_df, large_window)
        self.small_window_avg = TechCalc.EMA(cur_df, small_window)
        self.diffs = [self.small_window_avg[i] - self.large_window_avg[i] for i in range(len(self.large_window_avg))]

    def diff_prev(self):
        return self.diffs[-2]

    def diff_cur(self):
        return self.diffs[-1]


class MACDOpportunityFinder(DifferenceBasedOpportunityFinder):
    def __init__(self, cur_tick, cur_df):
        super().__init__(cur_tick, cur_df)

        const = strategy_config.MACDParams
        config = strategy_config.MACDParams.params
        self.slow = config[const.SLOW]
        self.fast = config[const.FAST]
        self.signal = config[const.SIGNAL]
        self.macd_values = TechCalc.MACD(self.cur_df, self.fast, self.slow, self.signal)

    def diff_cur(self):
        return self.macd_values[-1]

    def diff_prev(self):
        return self.macd_values[-2]
