import json
from datetime import timedelta, datetime

from constants import TextFileConstants
from general_util import json_arr_to_csv, save_csv_and_json_output
from kite_data_downloader import Nify50LastNDaysDownloader
from moving_avg_simulator import StockPnLAnalyzer, CombinedCrossOverGenerator
from moving_avg_simulator_utils import DataSeriesProvider


class FilePaths:
    def __init__(self, read_base_directory, write_base_directory, file_names, symbols):
        self.read_base_directory = read_base_directory
        self.write_base_path = write_base_directory
        self.file_names = file_names
        self.symbols = symbols


class DailyMovingAvgIndicator:
    def __init__(self, past_days, smaller_window, larger_window, file_paths: FilePaths, file_to_provider_func):
        self.larger_window = larger_window
        self.smaller_window = smaller_window
        self.past_days = past_days
        self.file_name, self.filter_func = self.__variables()
        self.result_cross_overs = None
        self.file_paths = file_paths
        self.file_to_provider_func = file_to_provider_func

    def __variables(self):
        today = datetime.today()
        start_date = today - timedelta(days=self.past_days)
        filter_smaller_than = str(start_date.date())
        file_name = "cross_overs_{}_to_{}".format(filter_smaller_than, str(today.date()))

        def filter_func(cross_over):
            return cross_over.date >= filter_smaller_than

        return file_name, filter_func

    def generate_indicators(self):
        combined_cross_overs = get_combined_cross_overs(self.file_paths,
                                                        small_window=self.smaller_window,
                                                        large_window=self.larger_window,
                                                        file_to_provider_func=self.file_to_provider_func)
        required_cross_overs = list(filter(self.filter_func, combined_cross_overs))
        self.result_cross_overs = [cross_over.json() for cross_over in required_cross_overs]
        return self

    def flush_indicators(self):
        save_csv_and_json_output(self.result_cross_overs, self.file_paths.write_base_path + self.file_name)


def __get_trade_summary_for_all_stocks(larger_window, smaller_window, file_paths: FilePaths, path_to_provider_func):
    summary = []
    all_trades = []
    for name, stock_symbol in zip(file_paths.file_names, file_paths.symbols):
        provider = path_to_provider_func(file_paths.read_base_directory + name)
        trade_analyzer = StockPnLAnalyzer(stock_symbol, provider, smaller_window, larger_window).analyze()
        all_trades.extend(map(lambda trade: trade.to_json(), trade_analyzer.trades))
        summary.append(trade_analyzer.profit_loss)
    return all_trades, summary


def save_predicted_trades_and_summary(file_paths: FilePaths, year,
                                      smaller_window, larger_window, file_to_provider_func):
    all_trades, all_trade_summary = __get_trade_summary_for_all_stocks(larger_window, smaller_window, file_paths,
                                                                       file_to_provider_func)
    all_trades_file = file_paths.write_base_path + "all_trades_{}_{}_{}".format(smaller_window, larger_window, year)
    summary_file = file_paths.write_base_path + "trading_summary_{}_{}_{}".format(smaller_window, larger_window, year)
    save_csv_and_json_output(all_trades, all_trades_file)
    save_csv_and_json_output(all_trade_summary, summary_file)


def generate_matrix(min_ranges: list, max_ranges: list, output_file_wo_ext, file_paths, file_to_provider_func):
    result = []
    for larger_window in max_ranges:
        for smaller_window in min_ranges:
            _, summary = __get_trade_summary_for_all_stocks(larger_window, smaller_window, file_paths,
                                                            file_to_provider_func)
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


def get_combined_cross_overs(file_paths: FilePaths, small_window, large_window, file_to_provider_func):
    result = []
    for symbol, name in zip(file_paths.symbols, file_paths.file_names):
        provider = file_to_provider_func(file_paths.read_base_directory + name)
        result.extend(CombinedCrossOverGenerator(symbol, provider, small_window, large_window)
                      .get_cross_over_jarr())
    return result


class KiteOHLC:
    def __init__(self, kite_historical_api_data_point):
        self.date = kite_historical_api_data_point[0]
        self.open = kite_historical_api_data_point[1]
        self.high = kite_historical_api_data_point[2]
        self.low = kite_historical_api_data_point[3]
        self.close = kite_historical_api_data_point[4]
        self.volume = kite_historical_api_data_point[5]


class KiteFileHistoricalDataProvider(DataSeriesProvider):
    def __init__(self, file_path):
        super().__init__()
        self.file_path = file_path
        self.kite_ohlc_series = self._kite_series()

    def _kite_series(self):
        with open(self.file_path) as handle:
            series = json.load(handle)
        return list(map(lambda tup: KiteOHLC(tup), series))

    def date_series(self):
        return list(map(lambda kite_ohlc: kite_ohlc.date, self.kite_ohlc_series))

    def price_series(self):
        return list(map(lambda kite_holc: kite_holc.open, self.kite_ohlc_series))


if __name__ == '__main__':
    def file_to_provider_func(file_path):
        return KiteFileHistoricalDataProvider(file_path)


    # run simulation with profit loss analysis
    # for year in ("2015_16", "2016_17", "2017_18", "2018_19", "2019_20", "2020_21"):
    #     read_dir = TextFileConstants.KITE_HISTORICAL_BASE_DIR + "{}/".format(year)
    #     write_dir = "/tmp/bullet1/"
    #     file_paths = FilePaths(read_dir, write_dir, TextFileConstants.NIFTY_50_DATA_FILE_NAMES,
    #                            TextFileConstants.NIFTY_50_SYMBOLS)
    #     save_predicted_trades_and_summary(file_paths, year, 1, 3, file_to_provider_func)

    # generate data log for identifying stock wise window size
    # for year in ("2015_16", "2016_17", "2017_18", "2018_19", "2019_20", "2020_21"):
    #     read_dir = "/data/kite_websocket_data/historical/{}/".format(year)
    #     file_paths = FilePaths(read_dir, None, TextFileConstants.NIFTY_50_DATA_FILE_NAMES,
    #                            TextFileConstants.NIFTY_50_SYMBOLS)
    #     generate_matrix([1, 2, 3, 4, 5], [15, 21, 25, 28, 35], "/tmp/bullet/multi_window/simulation_{}".format(year),
    #                     file_paths, file_to_provider_func)

    # for giving triggers on based on current data
    Nify50LastNDaysDownloader(number_of_days=20).download()
    file_paths = FilePaths(TextFileConstants.KITE_CURRENT_DATA, TextFileConstants.KITE_DATA_BASE_DIR,
                           TextFileConstants.NIFTY_50_DATA_FILE_NAMES, TextFileConstants.NIFTY_50_SYMBOLS)
    DailyMovingAvgIndicator(past_days=0, smaller_window=1, larger_window=5, file_paths=file_paths,
                            file_to_provider_func=file_to_provider_func).generate_indicators().flush_indicators()
