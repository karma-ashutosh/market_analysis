from binance.client import Client
from binance.websockets import BinanceSocketManager

from constants import BINANCE


class TradeExecutor:
    def __init__(self, client: Client, symbol):
        self.client = client
        self.symbol = symbol

    def buy(self, quantity):
        print("buying {} for {}".format(quantity, self.symbol))
        return self.client.create_test_order(symbol=self.symbol,
                                             side=Client.SIDE_BUY,
                                             type=Client.ORDER_TYPE_MARKET,
                                             quantity=quantity)

    def sell(self, quantity):
        print("selling {} for {}".format(quantity, self.symbol))
        return self.client.create_test_order(symbol=self.symbol,
                                             side=Client.SIDE_SELL,
                                             type=Client.ORDER_TYPE_MARKET,
                                             quantity=quantity)


class Factory:
    def __init__(self):
        self.client = Client(BINANCE.API_KEY, BINANCE.SECRET_KEU)

    def open_kline_connection(self, processor=lambda msg: print(msg), symbol=BINANCE.SYMBOL):
        bm = BinanceSocketManager(self.client)
        bm.start_kline_socket(symbol, processor, interval=Client.KLINE_INTERVAL_1MINUTE)
        bm.start()

    def trade_executor(self, symbol) -> TradeExecutor:
        return TradeExecutor(self.client, symbol)
