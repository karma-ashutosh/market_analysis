import json

from general_util import json_arr_to_csv
from moving_avg_simulator_utils import MovingAvgTradeSimulator, TradeSimulator


class Constants:
    FILE_NAMES = ['ADANIPORTS_3861249.json', 'ASIANPAINT_60417.json', 'BAJAJ-AUTO_4267265.json',
                  'BAJAJFINSV_4268801.json',
                  'BHARTIARTL_2714625.json', 'BPCL_134657.json', 'BRITANNIA_140033.json', 'CIPLA_177665.json',
                  'DIVISLAB_2800641.json', 'DRREDDY_225537.json', 'EICHERMOT_232961.json', 'GRASIM_315393.json',
                  'HCLTECH_1850625.json', 'HDFC_340481.json', 'HDFCBANK_341249.json', 'HEROMOTOCO_345089.json',
                  'HINDPETRO_359937.json', 'HINDUNILVR_356865.json', 'INDUSINDBK_1346049.json', 'INFY_408065.json',
                  'IOC_415745.json', 'ITC_424961.json', 'JSWSTEEL_3001089.json', 'KOTAKBANK_492033.json',
                  'LT_2939649.json',
                  'MARUTI_2815745.json', 'M&M_519937.json', 'NESTLEIND_4598529.json', 'NTPC_2977281.json',
                  'ONGC_633601.json',
                  'POWERGRID_3834113.json', 'RELIANCE_738561.json', 'SAIL_758529.json', 'SBILIFE_5582849.json',
                  'SBIN_779521.json',
                  'SHREECEM_794369.json', 'SUNPHARMA_857857.json', 'TATACONSUM_878593.json', 'TATAMOTORS_884737.json',
                  'TECHM_3465729.json', 'TITAN_897537.json', 'ULTRACEMCO_2952193.json', 'UPL_2889473.json',
                  'WIPRO_969473.json']


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
    def __init__(self, file_name, small_window=5, large_window=15):
        self.file_name = file_name_prefix + file_name
        self.small_window = small_window
        self.large_window = large_window
        self.simulator = MovingAvgTradeSimulator(self.file_name, small_window, large_window)

    def get_cross_over_jarr(self):
        cross_overs = self.simulator.get_cross_overs()
        j_arr = []
        for cross_over in cross_overs:
            d = cross_over.json()
            d["symbol"] = self.file_name.replace(".json", "")
            j_arr.append(d)
        return j_arr

    @staticmethod
    def generate_cross_over_data_for_all_files(target_file_wo_ext):
        result = []
        for file_name in Constants.FILE_NAMES:
            result.extend(CombinedCrossOverGenerator(file_name, 5, 15).get_cross_over_jarr())
        with open(target_file_wo_ext + ".json", 'w') as handle:
            json.dump(result, handle)
        json_arr_to_csv(result, target_file_wo_ext + '.csv')


def __get_trade_summary_for_all_stocks(larger_window, smaller_window):
    summary = []
    all_trades = []
    for name in Constants.FILE_NAMES:
        stock_symbol = name.replace(".json", "")
        trade_analyzer = StockPnLAnalyzer(stock_symbol, name, smaller_window, larger_window).analyze()
        all_trades.extend(map(lambda trade: trade.to_json(), trade_analyzer.trades))
        summary.append(trade_analyzer.profit_loss)
    return all_trades, summary


def save_predicted_trades_and_summary(all_trades_path_wo_ext, summary_path_wo_ext, smaller_window, larger_window):
    all_trades, all_trade_summary = __get_trade_summary_for_all_stocks(larger_window, smaller_window)

    def save_csv_and_json_output(values, path):
        json_arr_to_csv(values, path + ".csv")
        with open(path + ".json", 'w') as handle:
            json.dump(values, handle, indent=1)
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
    # for year in ("2015_16", "2016_17", "2017_18", "2018_19", "2019_20", "2020_21"):
    #     file_name_prefix = "/data/kite_websocket_data/historical/{}/".format(year)
    #     save_predicted_trades_and_summary("/tmp/bullet/all_trades_{}".format(year), "/tmp/bullet/trading_summary_{}".format(year), 5,
    #                                       15)

    # generate cross over points with emwa to bet for coming times
    # file_name_prefix = "/data/kite_websocket_data/historical/2021/"
    # CombinedCrossOverGenerator.generate_cross_over_data_for_all_files("/tmp/bullet/cross_overs_till_apr_11")

    # generate data log for identifying stock wise window size
    for year in ("2015_16", "2016_17", "2017_18", "2018_19", "2019_20", "2020_21"):
        file_name_prefix = "/data/kite_websocket_data/historical/{}/".format(year)
        generate_matrix([1, 2, 3, 4, 5], [15, 21, 25, 28, 35], "/tmp/bullet/multi_window/simulation_{}".format(year))
