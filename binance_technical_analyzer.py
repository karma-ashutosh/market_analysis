from binance_local_data_handler import BinanceFileDataProvider
from constants import BINANCE
from kite_technical_analyzer import save_predicted_trades_and_summary
from kite_technical_analyzer import FilePaths, DailyMovingAvgIndicator

if __name__ == '__main__':
    def file_to_provider_func(file_path):
        return BinanceFileDataProvider(file_path)

    read_file_path = BINANCE.DATA_FILE_READ_BASE_PATH
    write_file_path = BINANCE.DATA_FILE_WRITE_BASE_PATH
    file_paths = FilePaths(read_file_path, write_file_path, [BINANCE.SYMBOL + ".json"], [BINANCE.SYMBOL])
    save_predicted_trades_and_summary(file_paths, 2021, 1, 3, file_to_provider_func)

    # daily_indicatory = DailyMovingAvgIndicator(0, 1, 3, file_paths, file_to_provider_func)
    # daily_indicatory.filter_func = lambda _: True
    # daily_indicatory.generate_indicators()
    # daily_indicatory.flush_indicators()
