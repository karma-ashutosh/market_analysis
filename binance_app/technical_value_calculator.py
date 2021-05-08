from finta import TA
from pandas import DataFrame, Series


class TechCalc:
    def __init__(self):
        raise Exception("Cannot initialize this class")

    @staticmethod
    def EMA(data_frame: DataFrame, window: int) -> Series:
        return TA.EMA(data_frame, window)

    @staticmethod
    def MACD(data_frame: DataFrame, fast, slow, signal) -> Series:
        df = TA.MACD(data_frame, period_fast=fast, period_slow=slow, signal=signal)
        diff = df['MACD'] - df['SIGNAL']
        return diff
