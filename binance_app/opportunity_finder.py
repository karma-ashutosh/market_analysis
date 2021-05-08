import strategy_config
from analyzer_models import Opportunity, IndicatorDirection, IndicatorIntensity
from technical_value_calculator import TechCalc
from util_general import app_logger


class OpportunityFinder:
    def __init__(self, cur_tick, cur_df):
        self.cur_df = cur_df
        self.cur_tick = cur_tick

    def cur_opportunity(self):
        pass


class MovingAvgOpportunityFinder(OpportunityFinder):

    def __init__(self, cur_tick, cur_df):
        super().__init__(cur_tick, cur_df)

        config = strategy_config.MovingAvgParams.params
        consts = strategy_config.MovingAvgParams
        small_window, large_window = config[consts.SMALL], config[consts.LARGE]
        self.cur_tick = cur_tick

        self.large_window_avg = TechCalc.EMA(cur_df, large_window).tolist()
        self.small_window_avg = TechCalc.EMA(cur_df, small_window).tolist()
        self.diffs = [self.small_window_avg[i] - self.large_window_avg[i] for i in range(len(self.large_window_avg))]
        self.fixed_intensity = IndicatorIntensity.THREE

    def cur_opportunity(self) -> Opportunity:
        opp_type = self.__opportunity_type()
        intensity = self.__opportunity_intensity()
        opportunity = Opportunity(self.cur_tick, opp_type, intensity)
        return opportunity

    def __opportunity_intensity(self):
        return self.fixed_intensity

    def __opportunity_type(self) -> IndicatorDirection:
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

    def diff_prev(self):
        return self.diffs[-2]

    def diff_cur(self):
        return self.diffs[-1]


class MACDOpportunityFinder(OpportunityFinder):
    def __init__(self, cur_tick, cur_df):
        super().__init__(cur_tick, cur_df)

        const = strategy_config.MACDParams
        config = strategy_config.MACDParams.params
        self.slow = config[const.SLOW]
        self.fast = config[const.FAST]
        self.signal = config[const.SIGNAL]

    def cur_opportunity(self) -> Opportunity:
        series = TechCalc.MACD(self.cur_df, self.fast, self.slow, self.signal)
        app_logger.info("This is the series {}".format(series))


