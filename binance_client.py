from constants import BINANCE
from binance.client import Client
from binance.websockets import BinanceSocketManager, BinanceClientFactory


class BinanceDepth:
    def __init__(self, binance_depth_arr):
        _ignore = binance_depth_arr[11]
        self.taker_by_quote_asset_vol = binance_depth_arr[10]
        self.taker_by_base_asset_vol = binance_depth_arr[9]
        self.trade_count = binance_depth_arr[8]
        self.quote_asset_vol = binance_depth_arr[7]
        self.close_time = binance_depth_arr[6]
        self.volume = binance_depth_arr[5]
        self.close = binance_depth_arr[4]
        self.low = binance_depth_arr[3]
        self.high = binance_depth_arr[2]
        self.open = binance_depth_arr[1]
        self.open_time = binance_depth_arr[0]


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

    def historical_minute_wise(self, num_of_days=10):
        return self.binance_client.get_historical_klines(self.instrument_symbol, Client.KLINE_INTERVAL_1MINUTE,
                                                         "{} day ago UTC".format(num_of_days))


if __name__ == '__main__':
    client = Client(BINANCE.API_KEY, BINANCE.SECRET_KEU)
    instrument_client = InstrumentBinanceClient(client, 'BNBBTC')
    result = instrument_client.historical_minute_wise(1)
    result = [BinanceDepth(r) for r in result]
    x = len(result)
