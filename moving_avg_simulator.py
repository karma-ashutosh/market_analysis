import json
from enum import Enum
from general_util import json_arr_to_csv

file_name_prefix = "/data/kite_websocket_data/historical/2020_21/"


class Direction(Enum):
    UP = 1,
    DOWN = 2


class CrossOver:
    def __init__(self, index, direction: Direction, price, date):
        self.index = index
        self.direction = direction
        self.price = price
        self.date = date


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
                result.append(CrossOver(index, Direction.DOWN, self.price_series[index], self.date_series[index]))
            else:
                result.append(CrossOver(index, Direction.UP, self.price_series[index], self.date_series[index]))
        return result

    def find_moving_avg_diff(self) -> list:
        # return [(index, self.avg_diff_for_index(index)) for index in range(self.staring_index, len(self.data_series))]
        larger_moving_avg = self.avg_function(self.price_series, self.large_window)
        smaller_moving_avg = self.avg_function(self.price_series, self.smaller_window)
        result = [None] * self.large_window

        for index in range(self.large_window + 1, len(self.price_series)):
            result.append(smaller_moving_avg[index] - larger_moving_avg[index])
        return result

    def avg_diff_for_index(self, index):
        small_window, large_window = self.window_entries(index)
        small_avg = self.avg_func(small_window)
        large_avg = self.avg_func(large_window)
        avg_diff = small_avg - large_avg
        return avg_diff

    def window_entries(self, end_pos):
        large_window = self.price_series[end_pos - (self.large_window - 1): end_pos]
        small_window = self.price_series[end_pos - (self.smaller_window - 1): end_pos]
        return small_window, large_window


class MovingAvgTradeSimulator:
    def __init__(self, file_name, smaller_window, larger_window):
        self.file_name = file_name
        self.smaller_window = smaller_window
        self.larger_window = larger_window

    def kite_series(self):
        with open(file_name_prefix + self.file_name) as handle:
            series = json.load(handle)
        return list(map(lambda tup: (tup[0], tup[1]), series))

    def get_cross_overs(self):
        date_open_series = self.kite_series()
        price_series = list(map(lambda tup: tup[1], date_open_series))
        date_series = list(map(lambda tup: tup[0], date_open_series))

        cross_overs = CrossOverGenerator(price_series, date_series, self.smaller_window, self.larger_window) \
            .find_cross_overs()
        return cross_overs

    def print_cross_overs(self):
        cross_overs = self.get_cross_overs()
        for cross in cross_overs:
            index = cross.index
            direction = cross.direction
            print("{}: price: {} \t direction from {} to {}".format(
                date_open_series[index][0], price_series[index],
                Direction.UP if direction is Direction.DOWN else Direction.DOWN, direction))


class Trade:

    def __init__(self, buy_price, sell_price, total_stocks, total_profit, buy_date, sell_date):
        self.sell_date = sell_date
        self.buy_date = buy_date
        self.total_stocks = total_stocks
        self.total_profit = total_profit
        self.sell_price = sell_price
        self.buy_price = buy_price

    def __str__(self):
        result = {"sell_date": self.sell_date,
                  "buy_date": self.buy_date,
                  "total_stocks": self.total_stocks,
                  "total_profit": self.total_profit,
                  "sell_price": self.sell_price,
                  "buy_pricee": self.buy_price}
        return json.dumps(result, indent=1)


class TradeSimulator:
    def __init__(self, cross_overs, money):
        self.cross_overs = cross_overs
        self.money = money
        self.cur_stocks = 0
        self.buy_price = 0
        self.buy_date = None

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
                trades.append(Trade(self.buy_price, price, self.cur_stocks, total_profit, self.buy_date, date))
                self.buy_price = 0
                self.cur_stocks = 0
                self.buy_date = None
        return trades


def run_trades_for_file(file_name):
    simulator = MovingAvgTradeSimulator(file_name, 5, 15)
    cross_overs = simulator.get_cross_overs()
    trade_simulator = TradeSimulator(cross_overs, 10000)
    trades = trade_simulator.execute_trades()
    net_profit = 0
    profitable_trades, loss_making_trades = 0, 0
    only_profit, only_loss = 0, 0
    for trade in trades:
        # print(trade)
        trade_profit = trade.total_profit
        net_profit = net_profit + trade_profit
        if trade_profit > 0:
            only_profit = only_profit + trade_profit
            profitable_trades = profitable_trades + 1
        else:
            only_loss = loss_making_trades - trade_profit
            loss_making_trades = loss_making_trades + 1
    # print("total profit earned {} in {} trades".format(net_profit, len(trades)))
    result = {
        "net_profit": int(net_profit),
        "profitable_trades": profitable_trades,
        "only_profit": int(only_profit),
        "loss_making_trades": loss_making_trades,
        "only_loss": int(only_loss)
    }
    return result


if __name__ == '__main__':
    file_names = ['ADANIPORTS_3861249.json', 'ASIANPAINT_60417.json', 'BAJAJ-AUTO_4267265.json', 'BAJAJFINSV_4268801.json',
     'BHARTIARTL_2714625.json', 'BPCL_134657.json', 'BRITANNIA_140033.json', 'CIPLA_177665.json',
     'DIVISLAB_2800641.json', 'DRREDDY_225537.json', 'EICHERMOT_232961.json', 'GRASIM_315393.json',
     'HCLTECH_1850625.json', 'HDFC_340481.json', 'HDFCBANK_341249.json', 'HEROMOTOCO_345089.json',
     'HINDPETRO_359937.json', 'HINDUNILVR_356865.json', 'INDUSINDBK_1346049.json', 'INFY_408065.json',
     'IOC_415745.json', 'ITC_424961.json', 'JSWSTEEL_3001089.json', 'KOTAKBANK_492033.json', 'LT_2939649.json',
     'MARUTI_2815745.json', 'M&M_519937.json', 'NESTLEIND_4598529.json', 'NTPC_2977281.json', 'ONGC_633601.json',
     'POWERGRID_3834113.json', 'RELIANCE_738561.json', 'SAIL_758529.json', 'SBILIFE_5582849.json', 'SBIN_779521.json',
     'SHREECEM_794369.json', 'SUNPHARMA_857857.json', 'TATACONSUM_878593.json', 'TATAMOTORS_884737.json',
     'TECHM_3465729.json', 'TITAN_897537.json', 'ULTRACEMCO_2952193.json', 'UPL_2889473.json', 'WIPRO_969473.json']

    j_arr = []
    for name in file_names:
        result = run_trades_for_file(name)
        result['file_name'] = name
        j_arr.append(result)
    json_arr_to_csv(j_arr, "/tmp/output_20.csv")


