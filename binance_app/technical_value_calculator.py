import pandas
from finta import TA
from pandas import DataFrame, Series
import pandas as pd


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
    def MACD_ORG(
            ohlc: DataFrame,
            period_fast: int = 12,
            period_slow: int = 26,
            signal: int = 9,
            column: str = "close",
            adjust: bool = True,
    ) -> tuple:
        EMA_fast = pd.Series(
            ohlc[column].ewm(span=period_fast, adjust=adjust).mean(),
            name="EMA_fast",
        )
        EMA_slow = pd.Series(
            ohlc[column].ewm(span=period_slow, adjust=adjust).mean(),
            name="EMA_slow",
        )

        MACD = pd.Series(EMA_fast - EMA_slow, name="MACD")

        MACD_signal = pd.Series(
            MACD.ewm(ignore_na=False, span=signal, adjust=adjust).mean(), name="SIGNAL"
        )

        return EMA_fast, EMA_slow, MACD, MACD_signal

    @staticmethod
    def MACD(data_frame: DataFrame, fast, slow, signal) -> tuple:
        df = TA.MACD(data_frame, period_fast=fast, period_slow=slow, signal=signal)
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


class MovingAvg:
    def __init__(self, initial_data: list, window_size: int):
        self.yesterday_avg = sum(initial_data) / window_size
        self.window_size = window_size

    def next(self, today_val):
        smoothing_factor = 2

        today_avg = (today_val * (smoothing_factor / (1 + self.window_size))) \
                    + self.yesterday_avg * (1 - (smoothing_factor / (1 + self.window_size)))
        self.yesterday_avg = today_avg
        return today_avg


class CustomMACD:
    def __init__(self, fast, slow, signal, data):
        self.fast = MovingAvg(data[:fast], fast)
        self.slow = MovingAvg(data[:slow], slow)
        self.signal = None
        self.signal_window = signal

        self.fast_avgs = [None] * fast
        self.slow_avgs = [None] * slow
        self.macd_vals = [None] * slow
        self.signal_vals = [None] * (slow + signal - 1)
        for index in range(fast, len(data)):
            fast_avg = self.fast.next(data[index])
            self.fast_avgs.append(fast_avg)
            if index >= slow:
                slow_avg = self.slow.next(data[index])
                self.slow_avgs.append(slow_avg)
                macd_val = fast_avg - slow_avg
                self.macd_vals.append(macd_val)
                if not self.signal:
                    self.__setup_signal()
                else:
                    self.signal_vals.append(self.signal.next(macd_val))

    def next(self, today_val):
        fast_avg = self.fast.next(today_val)
        slow_avg = self.slow.next(today_val)
        macd_val = fast_avg - slow_avg
        self.fast_avgs.append(fast_avg)
        self.slow_avgs.append(slow_avg)
        self.macd_vals.append(macd_val)
        self.__setup_signal()

        signal = self.signal.next(macd_val) if self.signal else None
        self.signal_vals.append(signal)
        return fast_avg, slow_avg, fast_avg - slow_avg, signal

    def __setup_signal(self):
        non_null_macd = list(filter(lambda v: v is not None, self.macd_vals))
        if len(non_null_macd) == self.signal_window:
            self.signal = MovingAvg(non_null_macd, self.signal_window)
