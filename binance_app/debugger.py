from market_tick import MarketTickEntity
from technical_value_calculator import TechCalc
from market_tick_analyzer import MovingDF
from util_json import json_arr_to_csv


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


class MACD:
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


def row_to_tick(d: dict):
    tick = MarketTickEntity()
    print(d)
    d, c = d['date'], float(d['close'])
    tick.window_end_epoch_seconds = d
    tick.close = c
    return tick


csv_file_path = "/home/karma/Downloads/macd_calc.csv"

headers = "date    close   12 Day EMA      26 Day EMA      MACD    Signal"
rows = [line.strip() for line in open(csv_file_path).readlines()[1:]]

ticks = []
result_map = []


def setup_result(vals: list):
    return {
        'source': {
            'date': vals[0] if len(vals) >= 1 else None,
            'close': vals[1] if len(vals) >= 2 else None,
            'ema_12': vals[2] if len(vals) >= 3 else None,
            'ema_26': vals[3] if len(vals) >= 4 else None,
            'macd': vals[4] if len(vals) >= 5 else None,
            'signal': vals[5] if len(vals) >= 6 else None
        }
    }


for row in rows:
    vals = row.split("\t")
    source = setup_result(vals)
    result_map.append(source)
    ticks.append(row_to_tick(source['source']))

window_len = 30
moving_df = MovingDF(window_len)
index = 0
cur_df = None
while index < window_len:
    cur_df = moving_df.generate_snapshot(ticks[index])
    index = index + 1

macd_calc = MACD(12, 26, 9, cur_df['close'].to_list())

while index < len(ticks):
    tick = ticks[index]
    df = moving_df.generate_snapshot(tick)
    # ema = TechCalc.MACD_ORG(df, 12, 26, 9, adjust=False)
    fast_avg, slow_avg, macd, signal = macd_calc.next(tick.close)
    result_map[index]['calculated'] = {
        'date': tick.window_end_epoch_seconds,
        'ema_12': fast_avg,
        'ema_26': slow_avg,
        'macd': macd,
        'signal': signal
    }
    index = index + 1

if __name__ == '__main__':
    json_arr_to_csv(result_map, "/tmp/macd_analysis.csv", seperator="\t")
