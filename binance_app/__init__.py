from binance_analyzer import BinanceAnalyzer
from binance_provider import Factory
from binance_trader import BinanceTrader
from stream_manager import StreamManager
from kline_object import map_historical_k_line
from constants import BINANCE


if __name__ == '__main__':
    provider = Factory()

    small_window, large_window = 5, 15
    client = provider.client
    historical_data = client.get_historical_klines(symbol=BINANCE.SYMBOL, interval=client.KLINE_INTERVAL_1MINUTE,
                                               start_str='{} min ago UTC'.format(large_window * 2))
    historical_klines = list(map(map_historical_k_line, historical_data))

    analyzer = BinanceAnalyzer(small_window=small_window, large_window=large_window)
    for kline in historical_klines:
        analyzer.find_opportunity(kline)

    trader = BinanceTrader(provider.trade_executor(BINANCE.SYMBOL), analyzer)
    manager = StreamManager(trader)
    provider.open_kline_connection(processor=lambda event: manager.consume(event), symbol=BINANCE.SYMBOL)


