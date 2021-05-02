from binance_analyzer import BinanceAnalyzer
from binance_provider import Factory
from binance_trader import BinanceTrader
from stream_manager import  StreamManager
from constants import BINANCE

if __name__ == '__main__':
    provider = Factory()
    analyzer = BinanceAnalyzer()
    trader = BinanceTrader(provider.trade_executor(BINANCE.SYMBOL), analyzer)
    manager = StreamManager(trader)
    provider.open_kline_connection(processor=lambda event: manager.consume(event), symbol=BINANCE.SYMBOL)


