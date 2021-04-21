import json
from enum import Enum


class Direction(Enum):
    UP = 1,
    DOWN = 2


class CrossOver:
    def __init__(self, direction: Direction, price, date, last_small_avg, last_large_avg, cur_small_avg, cur_large_avg,
                 symbol=None):
        self.direction = direction
        self.price = price
        self.date = date
        self.last_small_avg = last_small_avg
        self.last_large_avg = last_large_avg
        self.cur_small_avg = cur_small_avg
        self.cur_large_avg = cur_large_avg
        self.symbol = symbol

    def set_symbol(self, symbol):
        self.symbol = symbol

    def json(self):
        return {
            "date": self.date,
            "direction": self.direction.name,
            "price": self.price,
            "symbol": self.symbol,
            "last_small_avg": self.last_small_avg,
            "last_large_avg": self.last_large_avg,
            "cur_small_avg": self.cur_small_avg,
            "cur_large_avg": self.cur_large_avg
        }


class CrossOverGenerator:
    def __init__(self, price_series: list, date_series: list, smaller_window: int, large_window: int):
        self.date_series = date_series
        self.price_series = price_series
        self.smaller_window = smaller_window
        self.large_window = large_window
        self.staring_index = large_window - 1
        if large_window <= smaller_window:
            raise Exception("larger window must be larger than smaller window")

    @staticmethod
    def avg_function(data_series, window_size):
        smoothing_factor = 2

        avg_series = [None] * window_size
        yesterday_avg = sum(data_series[:window_size]) / window_size
        for index in range(window_size, len(data_series)):
            today_val = data_series[index]
            today_avg = (today_val * (smoothing_factor / (1 + window_size))) \
                        + yesterday_avg * (1 - (smoothing_factor / (1 + window_size)))
            avg_series.append(today_avg)
            yesterday_avg = today_avg

        return avg_series

    def find_cross_overs(self):
        result = []
        moving_avg_diff, small_moving_avg, large_moving_avg = self.find_moving_avg_diff()
        for index in range(self.large_window + 1, len(moving_avg_diff)):
            prev_diff, prev_small, prev_large = moving_avg_diff[index - 1], small_moving_avg[index - 1], \
                                                large_moving_avg[index - 1]
            cur_diff, cur_small, cur_large = moving_avg_diff[index], small_moving_avg[index], large_moving_avg[index]
            if prev_diff > 0 and cur_diff > 0 or prev_diff < 0 and cur_diff < 0:
                continue
            direction = Direction.DOWN if prev_diff > 0 else Direction.UP
            result.append(CrossOver(direction,
                                    self.price_series[index], self.date_series[index],
                                    prev_small, prev_large,
                                    cur_small, cur_large))
        return result

    def find_moving_avg_diff(self) -> list:
        # return [(index, self.avg_diff_for_index(index)) for index in range(self.staring_index, len(self.data_series))]
        larger_moving_avg = self.avg_function(self.price_series, self.large_window)
        smaller_moving_avg = self.avg_function(self.price_series, self.smaller_window)
        result = [None] * self.large_window

        for index in range(self.large_window, len(self.price_series)):
            result.append(smaller_moving_avg[index] - larger_moving_avg[index])
        return result, smaller_moving_avg, larger_moving_avg

    def window_entries(self, end_pos):
        large_window = self.price_series[end_pos - (self.large_window - 1): end_pos]
        small_window = self.price_series[end_pos - (self.smaller_window - 1): end_pos]
        return small_window, large_window


class DataSeriesProvider:
    def __init__(self):
        pass

    def price_series(self):
        raise not NotImplementedError("Has to be implemented by base class")

    def date_series(self):
        raise not NotImplementedError("Has to be implemented by base class")


class MovingAvgTradeSimulator:
    def __init__(self, provider: DataSeriesProvider, smaller_window, larger_window):
        self.smaller_window = smaller_window
        self.larger_window = larger_window
        self.price_series = provider.price_series()
        self.date_series = provider.date_series()

    def get_cross_overs(self):
        cross_overs = CrossOverGenerator(self.price_series, self.date_series, self.smaller_window, self.larger_window) \
            .find_cross_overs()
        return cross_overs

    def print_cross_overs(self):
        cross_overs = self.get_cross_overs()
        for cross in cross_overs:
            direction = cross.direction
            print("{}: price: {} \t direction from {} to {}".format(
                cross.date, cross.price,
                Direction.UP if direction is Direction.DOWN else Direction.DOWN, direction))


class Trade:
    def __init__(self, symbol, buy_price, sell_price, total_stocks, total_profit, buy_date, sell_date):
        self.symbol = symbol
        self.sell_date = sell_date
        self.buy_date = buy_date
        self.total_stocks = total_stocks
        self.total_profit = total_profit
        self.sell_price = sell_price
        self.buy_price = buy_price

    def to_json(self):
        return {"symbol": self.symbol,
                "sell_date": self.sell_date,
                "buy_date": self.buy_date,
                "total_stocks": self.total_stocks,
                "total_profit": self.total_profit,
                "sell_price": self.sell_price,
                "buy_price": self.buy_price}

    def __str__(self):
        result = self.to_json()
        return json.dumps(result, indent=1)


class TradeSimulator:
    def __init__(self, cross_overs, money, symbol):
        self.cross_overs = cross_overs
        self.money = money
        self.cur_stocks = 0
        self.buy_price = 0
        self.buy_date = None
        self.symbol = symbol

    def execute_trades(self):
        trades = []
        for cross_over in self.cross_overs:
            direction = cross_over.direction
            price = cross_over.price
            date = cross_over.date
            if direction is Direction.UP:
                self.buy_price = price
                self.cur_stocks = int(self.money / cross_over.price)
                self.buy_date = date
            else:
                profit_per_stock = price - self.buy_price
                total_profit = profit_per_stock * self.cur_stocks
                trades.append(Trade(self.symbol, self.buy_price, price, self.cur_stocks, total_profit, self.buy_date,
                                    date))
                self.buy_price = 0
                self.cur_stocks = 0
                self.buy_date = None
        return trades
