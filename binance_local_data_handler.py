import json
from datetime import datetime, timedelta

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

    def download_historical_data(self, number_of_days=1):
        if number_of_days < 1:
            raise ValueError("Number of days has to be >= 1")

        result = []

        to_date = datetime.now()
        while number_of_days > 0:
            from_date = to_date - timedelta(days=1)
            from_timestamp, to_timestamp = int(from_date.timestamp()) * 1000, int(to_date.timestamp()) * 1000
            result.extend(self.instrument_client.from_to_data(from_timestamp, to_timestamp))
            to_date = from_date
            number_of_days = number_of_days - 1

        result = sorted(result, key=lambda j_elem: j_elem[0], reverse=False)

        with open(self.data_file_path, 'w') as handle:
            json.dump(result, handle, indent=1)

    def load_historical_data(self):
        with open(self.data_file_path) as handle:
            j = json.load(handle)
        return [BinanceDepth(row) for row in j]


if __name__ == '__main__':
    data_handler = LocalDataHandler(BINANCE.SYMBOL)
    data_handler.download_historical_data(7)
