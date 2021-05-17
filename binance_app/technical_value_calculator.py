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

    @staticmethod
    def HA(df: DataFrame, observe=3) -> Series:
        try:
            df['HA_Close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4

            idx = df.index.name
            df.reset_index(inplace=True)

            for i in range(0, len(df)):
                if i == 0:
                    df.at[i, 'HA_Open'] = (df._get_value(i, 'open') + df._get_value(i, 'close')) / 2
                else:
                    df.at[i, 'HA_Open'] = (df._get_value(i - 1, 'HA_Open') + df._get_value(i - 1, 'HA_Close')) / 2

            if idx:
                df.set_index(idx, inplace=True)

            # df['HA_High'] = df[['HA_Open', 'HA_Close', 'High']].max(axis=1)
            # df['HA_Low'] = df[['HA_Open', 'HA_Close', 'Low']].min(axis=1)
            # high = df[['HA_Open', 'HA_Close', 'high']].max(axis=1)
            # low = df[['HA_Open', 'HA_Close', 'low']].min(axis=1)
            # return high - low
            # return df
            diff = df['HA_Open'] - df['HA_Close']
            # if all([x > 0 for x in diff]):
            #     return 1
            # if all([x < 0 for x in diff]):
            #     return -1
            # return 0
            # return sum([x > 0 for x in diff])
            return diff.to_list()[-15:]
        except:
            return -1
