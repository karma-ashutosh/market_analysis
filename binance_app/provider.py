import json

from binance.client import Client
from binance.websockets import BinanceSocketManager

from trade_executor_client import BinanceTradeExecutor, AcademicTradeExecutor, TradeExecutor
from constants import BINANCE


class Factory:
    # https://github.com/binance/binance-public-data/
    def __init__(self, test_mode: bool = True):
        self.client = Client(BINANCE.API_KEY, BINANCE.SECRET_KEU)

    @staticmethod
    def open_file_kline_connection(processor=lambda msg: print(msg), symbol=BINANCE.SYMBOL):
        with open(BINANCE.DATA_FILE_READ_PATH) as handle:
            events = json.load(handle)
        for event in events:
            processor(event)

    def open_kline_connection(self, processor=lambda msg: print(msg), symbol=BINANCE.SYMBOL):
        bm = BinanceSocketManager(self.client)
        bm.start_kline_socket(symbol, processor, interval=Client.KLINE_INTERVAL_1MINUTE)
        bm.start()

    def binance_trade_executor(self, symbol) -> BinanceTradeExecutor:
        return BinanceTradeExecutor(self.client, symbol)

    @staticmethod
    def analytical_trade_executor(symbol) -> AcademicTradeExecutor:
        return AcademicTradeExecutor(symbol)


if __name__ == '__main__':
    provider = Factory()
    provider.binance_trade_executor(BINANCE.SYMBOL).sell(.38)
