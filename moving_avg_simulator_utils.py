import json
from enum import Enum


class Direction(Enum):
    UP = 1,
    DOWN = 2


class CrossOver:
    def __init__(self, direction: Direction, price, date, symbol=None):
        self.direction = direction
        self.price = price
        self.date = date
        self.symbol = None

    def json(self):
        return {
            "date": self.date,
            "direction": self.direction.name,
            "price": self.price,
            "symbol": self.symbol
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
        moving_avg_diff = self.find_moving_avg_diff()
        for index in range(self.large_window + 1, len(moving_avg_diff)):
            prev_diff = moving_avg_diff[index - 1]
            cur_diff = moving_avg_diff[index]
            if prev_diff > 0 and cur_diff > 0 or prev_diff < 0 and cur_diff < 0:
                continue
            elif prev_diff > 0:
                result.append(CrossOver(Direction.DOWN, self.price_series[index], self.date_series[index]))
            else:
                result.append(CrossOver(Direction.UP, self.price_series[index], self.date_series[index]))
        return result

    def find_moving_avg_diff(self) -> list:
        # return [(index, self.avg_diff_for_index(index)) for index in range(self.staring_index, len(self.data_series))]
        larger_moving_avg = self.avg_function(self.price_series, self.large_window)
        smaller_moving_avg = self.avg_function(self.price_series, self.smaller_window)
        result = [None] * self.large_window

        for index in range(self.large_window + 1, len(self.price_series)):
            result.append(smaller_moving_avg[index] - larger_moving_avg[index])
        return result

    def window_entries(self, end_pos):
        large_window = self.price_series[end_pos - (self.large_window - 1): end_pos]
        small_window = self.price_series[end_pos - (self.smaller_window - 1): end_pos]
        return small_window, large_window


class KiteOHLC:
    def __init__(self, kite_historical_api_data_point):
        self.date = kite_historical_api_data_point[0]
        self.open = kite_historical_api_data_point[1]
        self.high = kite_historical_api_data_point[2]
        self.low = kite_historical_api_data_point[3]
        self.close = kite_historical_api_data_point[4]
        self.volume = kite_historical_api_data_point[5]


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
