from constants import TextFileConstants
from general_util import json_arr_to_csv, save_csv_and_json_output
from kite_data_downloader import KiteFileHistoricalDataProvider, Nify50LastNDaysDownloader
from moving_avg_simulator import StockPnLAnalyzer, CombinedCrossOverGenerator


def __get_trade_summary_for_all_stocks(larger_window, smaller_window):
    summary = []
    all_trades = []
    for name in TextFileConstants.NIFTY_50_DATA_FILE_NAMES:
        stock_symbol = name.replace(".json", "")
        provider = KiteFileHistoricalDataProvider(file_name_prefix + name)
        trade_analyzer = StockPnLAnalyzer(stock_symbol, provider, smaller_window, larger_window).analyze()
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


def get_combined_cross_overs(base_file_path, small_window=5, large_winodow=15):
    result = []
    for file_name in TextFileConstants.NIFTY_50_DATA_FILE_NAMES:
        symbol = file_name.replace(".json", "")
        provider = KiteFileHistoricalDataProvider(base_file_path + file_name)
        result.extend(CombinedCrossOverGenerator(symbol, provider, small_window, large_winodow)
                      .get_cross_over_jarr())
    return result


class DailyMovingAvgIndicator:
    def __init__(self, past_days, smaller_window, larger_window):
        self.larger_window = larger_window
        self.smaller_window = smaller_window
        self.past_days = past_days
        self.file_name, self.filter_func = self.__variables()
        self.result_cross_overs = None

    def __variables(self):
        today = datetime.today()
        start_date = today - timedelta(days=self.past_days)
        filter_smaller_than = str(start_date.date())
        file_name = "cross_overs_{}_to_{}".format(filter_smaller_than, str(today.date()))

        def filter_func(cross_over):
            return cross_over.date >= filter_smaller_than

        return file_name, filter_func

    def generate_indicators(self):
        combined_cross_overs = get_combined_cross_overs(TextFileConstants.KITE_CURRENT_DATA,
                                                        small_window=self.smaller_window,
                                                        large_winodow=self.larger_window)
        required_cross_overs = list(filter(self.filter_func, combined_cross_overs))
        self.result_cross_overs = [cross_over.json() for cross_over in required_cross_overs]
        return self

    def flush_indicators(self):
        save_csv_and_json_output(self.result_cross_overs, TextFileConstants.KITE_DATA_BASE_DIR + self.file_name)


if __name__ == '__main__':

    # run simulation with profit loss analysis
    # for year in ("2015_16", "2016_17", "2017_18", "2018_19", "2019_20", "2020_21"):
    #     file_name_prefix = TextFileConstants.KITE_HISTORICAL_BASE_DIR + "{}/".format(year)
    #     save_predicted_trades_and_summary("/tmp/bullet/all_trades_1_3_{}".format(year),
    #                                       "/tmp/bullet/trading_summary_1_3_{}".format(year), 1, 3)

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

    # for giving triggers on based on current data
    Nify50LastNDaysDownloader(number_of_days=60).download()
    DailyMovingAvgIndicator(past_days=0, smaller_window=1, larger_window=5).generate_indicators().flush_indicators()
