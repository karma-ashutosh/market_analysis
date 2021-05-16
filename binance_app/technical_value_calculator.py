import pandas
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
    def MACD(data_frame: DataFrame, fast, slow, signal) -> tuple:
        df = TA.MACD(data_frame, period_fast=fast, period_slow=slow, signal=signal, column='open')
        return df['MACD'], df['SIGNAL']

    @staticmethod
    def obv(df: DataFrame) -> Series:
        """Calculate On-Balance Volume for given data.

        :param df: pandas.DataFrame
        :param n:
        :return: pandas.DataFrame
        """
        i = 0
        OBV = [0]
        rows = df.shape[0]
        while i < rows - 1:
            if df.loc[i + 1, 'close'] - df.loc[i, 'close'] > 0:
                OBV.append(df.loc[i + 1, 'volume'])
            if df.loc[i + 1, 'close'] - df.loc[i, 'close'] == 0:
                OBV.append(OBV[-1])
            if df.loc[i + 1, 'close'] - df.loc[i, 'close'] < 0:
                OBV.append(-df.loc[i + 1, 'volume'])
            i = i + 1
        OBV = pandas.Series(OBV)
        # OBV_ma = pd.Series(OBV.rolling(n, min_periods=n).mean(), name='OBV_' + str(n))
        # df = df.join(OBV_ma)
        return OBV

    @staticmethod
    def OBV(data_frame: DataFrame) -> list:
        # print(data_frame)
        obv = TechCalc.obv(data_frame).to_list()
        # print(obv)

        def n_day_obv(days):
            for i in range(days, 1, -1):
                if obv[-i] > obv[-(i - 1)]:
                    return False
            return True

        # return [(day, n_day_obv(day)) for day in (2, 3, 4, 5)]
        return [x > 0 for x in obv[-5:]]

