from constants import TextFileConstants
from general_util import json_arr_to_csv, save_csv_and_json_output
from moving_avg_simulator_utils import MovingAvgTradeSimulator, TradeSimulator


class StockPnLAnalyzer:
    def __init__(self, symbol, file_name, smaller_window=5, larger_window=15, debug=False):
        self.__symbol = symbol
        self.__debug = debug
        self.__larger_window = larger_window
        self.__smaller_window = smaller_window
        self.__file_name = file_name
        self.trades = None
        self.profit_loss = None

    def analyze(self):
        self.trades = self.__simulated_trades(self.__file_name, self.__smaller_window, self.__larger_window)
        self.profit_loss = self.__profit_loss_analysis(self.trades)
        return self

    def __simulated_trades(self, file_name, smaller_window, larger_window):
        simulator = MovingAvgTradeSimulator(file_name_prefix + file_name, smaller_window, larger_window)
        cross_overs = simulator.get_cross_overs()
        trade_simulator = TradeSimulator(cross_overs, 10000, self.__symbol)
        trades = trade_simulator.execute_trades()
        return trades

    def __profit_loss_analysis(self, trades):
        net_profit = 0
        profitable_trades, loss_making_trades = 0, 0
        only_profit, only_loss = 0, 0
        for trade in trades:
            trade_profit = trade.total_profit
            net_profit = net_profit + trade_profit
            if trade_profit > 0:
                only_profit = only_profit + trade_profit
                profitable_trades = profitable_trades + 1
            else:
                only_loss = loss_making_trades - trade_profit
                loss_making_trades = loss_making_trades + 1
        result = {
            "symbol": self.__symbol,
            "net_profit": int(net_profit),
            "profitable_trades": profitable_trades,
            "only_profit": int(only_profit),
            "loss_making_trades": loss_making_trades,
            "only_loss": int(only_loss)
        }
        return result


class CombinedCrossOverGenerator:
    def __init__(self, base_file_path, file_name, small_window=5, large_window=15):
        self.symbol = file_name.replace(".json", "")
        self.file_name = base_file_path + file_name
        self.small_window = small_window
        self.large_window = large_window
        self.simulator = MovingAvgTradeSimulator(self.file_name, small_window, large_window)

    def get_cross_over_jarr(self):
        cross_overs = self.simulator.get_cross_overs()
        for cross_over in cross_overs:
            cross_over.symbol = self.symbol
        return cross_overs

    @staticmethod
    def get_combined_cross_overs(base_file_path, small_window=5, large_winodow=15):
        result = []
        for file_name in TextFileConstants.NIFTY_50_DATA_FILE_NAMES:
            result.extend(CombinedCrossOverGenerator(base_file_path, file_name, small_window, large_winodow)
                          .get_cross_over_jarr())
        return result


def __get_trade_summary_for_all_stocks(larger_window, smaller_window):
    summary = []
    all_trades = []
    for name in TextFileConstants.NIFTY_50_DATA_FILE_NAMES:
        stock_symbol = name.replace(".json", "")
        trade_analyzer = StockPnLAnalyzer(stock_symbol, name, smaller_window, larger_window).analyze()
        all_trades.extend(map(lambda trade: trade.to_json(), trade_analyzer.trades))
        summary.append(trade_analyzer.profit_loss)
    return all_trades, summary


def save_predicted_trades_and_summary(all_trades_path_wo_ext, summary_path_wo_ext, smaller_window, larger_window):
    all_trades, all_trade_summary = __get_trade_summary_for_all_stocks(larger_window, smaller_window)

    save_csv_and_json_output(all_trades, all_trades_path_wo_ext)
    save_csv_and_json_output(all_trade_summary, summary_path_wo_ext)


def generate_matrix(min_ranges: list, max_ranges: list, output_file_wo_ext):
    result = []
    for larger_window in max_ranges:
        for smaller_window in min_ranges:
            _, summary = __get_trade_summary_for_all_stocks(larger_window, smaller_window)
            for e in summary:
                result.append({
                    "symbol": e["symbol"],
                    "profit": e["net_profit"],
                    "smaller_window": smaller_window,
                    "larger_window": larger_window
                })
    result = list(sorted(result, key=lambda d: d['symbol']))
    # with open(output_file_wo_ext + ".json", 'w') as handle:
    #     json.dump(result, handle, indent=2)
    json_arr_to_csv(result, output_file_wo_ext + ".csv")


if __name__ == '__main__':

    # run simulation with profit loss analysis
    for year in ("2015_16", "2016_17", "2017_18", "2018_19", "2019_20", "2020_21"):
        file_name_prefix = TextFileConstants.KITE_HISTORICAL_BASE_DIR + "{}/".format(year)
        save_predicted_trades_and_summary("/tmp/bullet/all_trades_1_3_{}".format(year),
                                          "/tmp/bullet/trading_summary_1_3_{}".format(year), 1, 3)

    # generate cross over points with emwa to bet for coming times
    # file_name_prefix = "/data/kite_websocket_data/historical/2021/"
    # cross_overs = CombinedCrossOverGenerator.get_combined_cross_overs(file_name_prefix)
    # result = [cross_over.json() for cross_over in cross_overs]
    # save_csv_and_json_output(result, target_file_wo_ext)
    # "/tmp/bullet/cross_overs_till_apr_11")

    # generate data log for identifying stock wise window size
    # for year in ("2015_16", "2016_17", "2017_18", "2018_19", "2019_20", "2020_21"):
    #     file_name_prefix = "/data/kite_websocket_data/historical/{}/".format(year)
    #     generate_matrix([1, 2, 3, 4, 5], [15, 21, 25, 28, 35], "/tmp/bullet/multi_window/simulation_{}".format(year))
