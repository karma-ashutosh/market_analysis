import json

from binance.client import Client
from binance_client import InstrumentBinanceClient, BinanceDepth
from constants import BINANCE
from moving_avg_simulator_utils import DataSeriesProvider


class BinanceFileDataProvider(DataSeriesProvider):
    def __init__(self, file_path):
        super().__init__()
        self.file_path = file_path
        data_series = self.__data_series()
        self._price_series = [depth.open for depth in data_series]
        self._date_series = [depth.open_time for depth in data_series]

    def __data_series(self):
        with open(self.file_path) as handle:
            series = json.load(handle)
        return [BinanceDepth(row) for row in series]

    def price_series(self):
        return self._price_series

    def date_series(self):
        return self._date_series


class LocalDataHandler:
    def __init__(self, symbol):
        self.symbol = symbol
        self.data_file_path = BINANCE.DATA_FILE_READ_BASE_PATH + symbol + ".json"
        client = Client(BINANCE.API_KEY, BINANCE.SECRET_KEU)
        self.instrument_client = InstrumentBinanceClient(client, self.symbol)

    def download_historical_data(self, number_of_days):
        result = self.instrument_client.historical_minute_wise(number_of_days)
        with open(self.data_file_path, 'w') as handle:
            json.dump(result, handle, indent=1)

    def load_historical_data(self):
        with open(self.data_file_path) as handle:
            j = json.load(handle)
        return [BinanceDepth(row) for row in j]


if __name__ == '__main__':
    symbol = 'BNBBTC'
    data_handler = LocalDataHandler(symbol)
    data_handler.download_historical_data(7)
