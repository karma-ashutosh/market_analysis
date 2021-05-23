import json
from datetime import datetime, timedelta

from binance.client import Client
from constants import BINANCE


class InstrumentBinanceClient:
    def __init__(self, binance_client: Client, instrument_symbol: str):
        self.binance_client = binance_client
        self.instrument_symbol = instrument_symbol

    def depth(self):
        return self.binance_client.get_order_book(symbol=self.instrument_symbol)

    def buy_market_order(self, quantity):
        order = self.binance_client.create_test_order(symbol=self.instrument_symbol, side=Client.SIDE_BUY,
                                                      type=Client.ORDER_TYPE_MARKET, quantity=quantity)
        return order

    def sell_market_order(self, quantity):
        order = self.binance_client.create_test_order(symbol=self.instrument_symbol, side=Client.SIDE_SELL,
                                                      type=Client.ORDER_TYPE_MARKET, quantity=quantity)
        return order

    def stream(self, consumer):
        bm = BinanceSocketManager(self.binance_client)
        bm.start_aggtrade_socket(self.instrument_symbol, consumer)
        bm.start()

    def from_to_data(self, from_date, to_date):
        return self.binance_client.get_historical_klines(self.instrument_symbol, Client.KLINE_INTERVAL_1MINUTE,
                                                         from_date, to_date)

    def last_n_days_data(self, num_of_days=10):
        return self.binance_client.get_historical_klines(self.instrument_symbol, Client.KLINE_INTERVAL_1MINUTE,
                                                         "{} day ago UTC".format(num_of_days))


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
    data_handler.download_historical_data(2)
