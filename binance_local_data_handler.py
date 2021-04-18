import json

from binance.client import Client
from binance_client import InstrumentBinanceClient, BinanceDepth
from constants import BINANCE


class LocalDataHandler:
    def __init__(self, symbol):
        self.symbol = symbol
        self.data_file_path = BINANCE.DATA_FILE_BASE_PATH + symbol + ".json"
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
    download_historical_data()
