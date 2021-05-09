from finta import TA
from pandas import DataFrame, Series


class TechCalc:
    def __init__(self):
        raise Exception("Cannot initialize this class")

    @staticmethod
    def RSI(data_frame: DataFrame, window: int) -> list:
        return TA.RSI(data_frame, period=window).to_list()

    @staticmethod
    def EMA(data_frame: DataFrame, window: int) -> list:
        return TA.EMA(data_frame, window).to_list()

    @staticmethod
    def MACD(data_frame: DataFrame, fast, slow, signal) -> list:
        df = TA.MACD(data_frame, period_fast=fast, period_slow=slow, signal=signal)
        diff = df['MACD'] - df['SIGNAL']
        return diff.to_list()
